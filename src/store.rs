use crate::error::AppError;
use rusqlite::{params, Connection};
use std::path::Path;
use std::sync::Mutex;

pub struct Store {
    conn: Mutex<Connection>,
}

impl Store {
    pub fn new(path: &Path) -> Result<Self, AppError> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            use std::os::unix::fs::PermissionsExt;

            // Atomically create file with 0600 permissions if it does not exist
            let _ = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .mode(0o600)
                .open(path)
                .map_err(|e| {
                    AppError::Store(format!(
                        "Failed to create SQLite file with mode 0600 at {}: {e}",
                        path.display()
                    ))
                })?;

            // Ensure permissions are 0600 even if file pre-existed, and propagate failure
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).map_err(
                |e| {
                    AppError::Store(format!(
                        "Failed to set 0600 permissions on SQLite database at {}: {e}",
                        path.display()
                    ))
                },
            )?;
        }

        let conn = Connection::open(path).map_err(|e| {
            AppError::Store(format!(
                "Failed to open SQLite database at {}: {e}",
                path.display()
            ))
        })?;
        let store = Self {
            conn: Mutex::new(conn),
        };
        store.init_schema()?;
        Ok(store)
    }

    pub fn new_in_memory() -> Result<Self, AppError> {
        let conn = Connection::open_in_memory().map_err(|e| {
            AppError::Store(format!("Failed to open in-memory SQLite database: {e}"))
        })?;
        let store = Self {
            conn: Mutex::new(conn),
        };
        store.init_schema()?;
        Ok(store)
    }

    fn init_schema(&self) -> Result<(), AppError> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS threads (
                thread_root_id TEXT PRIMARY KEY,
                message_ids_json TEXT NOT NULL,
                updated_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS rate_limits (
                user_mxid TEXT NOT NULL,
                timestamp INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_rate_limits_user ON rate_limits(user_mxid, timestamp);
            CREATE TABLE IF NOT EXISTS bot_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS dm_rooms (
                user_mxid TEXT PRIMARY KEY,
                room_id TEXT NOT NULL,
                created_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS thread_askers (
                thread_root_id TEXT PRIMARY KEY,
                asker_mxid TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS bot_messages (
                event_id TEXT PRIMARY KEY,
                thread_root_id TEXT NOT NULL,
                timestamp INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_bot_messages_thread ON bot_messages(thread_root_id);
            CREATE TABLE IF NOT EXISTS control_events (
                message_id TEXT NOT NULL,
                user_mxid TEXT NOT NULL,
                control TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                PRIMARY KEY (message_id, user_mxid, control)
            );
            CREATE TABLE IF NOT EXISTS relayed_events (
                event_id TEXT PRIMARY KEY,
                timestamp INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS thread_cursors (
                thread_root_id TEXT PRIMARY KEY,
                cursor_event_id TEXT NOT NULL,
                forwarded_at INTEGER NOT NULL,
                session_id TEXT
            );",
        )
        .map_err(|e| AppError::Store(format!("Failed to initialize SQLite schema: {e}")))?;

        // Migration: add session_id column to thread_cursors if not present
        let _ = conn.execute("ALTER TABLE thread_cursors ADD COLUMN session_id TEXT;", []);

        Ok(())
    }

    /// Records an xmsg message_id associated with a Matrix thread root event ID.
    pub fn record_thread_message(
        &self,
        thread_root_id: &str,
        message_id: &str,
        now: i64,
    ) -> Result<(), AppError> {
        let conn = self.conn.lock().unwrap();
        let mut existing_ids = Vec::<String>::new();

        let mut stmt = conn
            .prepare("SELECT message_ids_json FROM threads WHERE thread_root_id = ?1")
            .map_err(|e| AppError::Store(e.to_string()))?;

        let res = stmt.query_row(params![thread_root_id], |row| {
            let json_str: String = row.get(0)?;
            Ok(json_str)
        });

        if let Ok(json_str) = res {
            if let Ok(ids) = serde_json::from_str::<Vec<String>>(&json_str) {
                existing_ids = ids;
            }
        }

        if !existing_ids.contains(&message_id.to_string()) {
            existing_ids.push(message_id.to_string());
        }

        let new_json = serde_json::to_string(&existing_ids)
            .map_err(|e| AppError::Store(format!("Serialization error: {e}")))?;

        conn.execute(
            "INSERT INTO threads (thread_root_id, message_ids_json, updated_at)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(thread_root_id) DO UPDATE SET message_ids_json = ?2, updated_at = ?3",
            params![thread_root_id, new_json, now],
        )
        .map_err(|e| AppError::Store(e.to_string()))?;

        Ok(())
    }

    /// Retrieves all xmsg message_ids associated with a thread root event ID.
    pub fn get_thread_messages(&self, thread_root_id: &str) -> Result<Vec<String>, AppError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT message_ids_json FROM threads WHERE thread_root_id = ?1")
            .map_err(|e| AppError::Store(e.to_string()))?;

        let res = stmt.query_row(params![thread_root_id], |row| {
            let json_str: String = row.get(0)?;
            Ok(json_str)
        });

        match res {
            Ok(json_str) => serde_json::from_str(&json_str)
                .map_err(|e| AppError::Store(format!("Failed to parse message IDs: {e}"))),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(Vec::new()),
            Err(e) => Err(AppError::Store(e.to_string())),
        }
    }

    /// Checks the user's rate limit against a sliding window and records the current timestamp if permitted.
    pub fn check_and_record_rate_limit(
        &self,
        user_mxid: &str,
        max_count: usize,
        window_secs: u64,
        now: i64,
    ) -> Result<(), AppError> {
        let conn = self.conn.lock().unwrap();
        let cutoff = now - (window_secs as i64);

        // Purge expired records for this user
        let _ = conn.execute(
            "DELETE FROM rate_limits WHERE user_mxid = ?1 AND timestamp < ?2",
            params![user_mxid, cutoff],
        );

        let count: usize = conn
            .query_row(
                "SELECT COUNT(*) FROM rate_limits WHERE user_mxid = ?1 AND timestamp >= ?2",
                params![user_mxid, cutoff],
                |row| row.get(0),
            )
            .map_err(|e| AppError::Store(e.to_string()))?;

        if count >= max_count {
            return Err(AppError::RateLimitExceeded(user_mxid.to_string()));
        }

        conn.execute(
            "INSERT INTO rate_limits (user_mxid, timestamp) VALUES (?1, ?2)",
            params![user_mxid, now],
        )
        .map_err(|e| AppError::Store(e.to_string()))?;

        Ok(())
    }

    /// Retrieves the persisted sync token (next_batch) if available.
    pub fn get_sync_token(&self) -> Result<Option<String>, AppError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT value FROM bot_meta WHERE key = 'sync_token'")
            .map_err(|e| AppError::Store(e.to_string()))?;

        let res = stmt.query_row([], |row| row.get(0));
        match res {
            Ok(token) => Ok(Some(token)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(AppError::Store(e.to_string())),
        }
    }

    /// Persists the Matrix sync token (next_batch) into the bot_meta table.
    pub fn set_sync_token(&self, token: &str) -> Result<(), AppError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO bot_meta (key, value) VALUES ('sync_token', ?1)
             ON CONFLICT(key) DO UPDATE SET value = ?1",
            params![token],
        )
        .map_err(|e| AppError::Store(e.to_string()))?;
        Ok(())
    }

    /// Looks up a cached DM room ID for an escalated user/owner.
    pub fn get_dm_room(&self, user_mxid: &str) -> Result<Option<String>, AppError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT room_id FROM dm_rooms WHERE user_mxid = ?1")
            .map_err(|e| AppError::Store(e.to_string()))?;

        let res = stmt.query_row(params![user_mxid], |row| row.get(0));
        match res {
            Ok(room_id) => Ok(Some(room_id)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(AppError::Store(e.to_string())),
        }
    }

    /// Records a cached DM room ID for an escalated user/owner.
    pub fn set_dm_room(&self, user_mxid: &str, room_id: &str, now: i64) -> Result<(), AppError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO dm_rooms (user_mxid, room_id, created_at) VALUES (?1, ?2, ?3)
             ON CONFLICT(user_mxid) DO UPDATE SET room_id = ?2, created_at = ?3",
            params![user_mxid, room_id, now],
        )
        .map_err(|e| AppError::Store(e.to_string()))?;
        Ok(())
    }

    /// Records the original asking user of a thread root event ID.
    /// Preserves the original asker (INSERT OR IGNORE).
    pub fn record_thread_asker(
        &self,
        thread_root_id: &str,
        asker_mxid: &str,
    ) -> Result<(), AppError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO thread_askers (thread_root_id, asker_mxid) VALUES (?1, ?2)",
            params![thread_root_id, asker_mxid],
        )
        .map_err(|e| AppError::Store(format!("Failed to record thread asker: {e}")))?;
        Ok(())
    }

    /// Retrieves the original asking user of a thread root event ID.
    pub fn get_thread_asker(&self, thread_root_id: &str) -> Result<Option<String>, AppError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT asker_mxid FROM thread_askers WHERE thread_root_id = ?1")
            .map_err(|e| AppError::Store(e.to_string()))?;
        let res = stmt.query_row(params![thread_root_id], |row| row.get(0));
        match res {
            Ok(asker) => Ok(Some(asker)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(AppError::Store(e.to_string())),
        }
    }

    /// Records an event_id posted by the bot in a given thread.
    pub fn record_bot_message(
        &self,
        event_id: &str,
        thread_root_id: &str,
        now: i64,
    ) -> Result<(), AppError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO bot_messages (event_id, thread_root_id, timestamp) VALUES (?1, ?2, ?3)",
            params![event_id, thread_root_id, now],
        )
        .map_err(|e| AppError::Store(format!("Failed to record bot message: {e}")))?;
        Ok(())
    }

    /// Retrieves the thread_root_id for a message posted by the bot.
    pub fn get_bot_message_thread(&self, event_id: &str) -> Result<Option<String>, AppError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT thread_root_id FROM bot_messages WHERE event_id = ?1")
            .map_err(|e| AppError::Store(e.to_string()))?;
        let res = stmt.query_row(params![event_id], |row| row.get(0));
        match res {
            Ok(thread) => Ok(Some(thread)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(AppError::Store(e.to_string())),
        }
    }

    /// Retrieves the latest bot message event_id in a thread.
    pub fn get_latest_bot_message_in_thread(
        &self,
        thread_root_id: &str,
    ) -> Result<Option<String>, AppError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT event_id FROM bot_messages WHERE thread_root_id = ?1 ORDER BY timestamp DESC LIMIT 1")
            .map_err(|e| AppError::Store(e.to_string()))?;
        let res = stmt.query_row(params![thread_root_id], |row| row.get(0));
        match res {
            Ok(ev) => Ok(Some(ev)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(AppError::Store(e.to_string())),
        }
    }

    /// Returns true if the thread is engaged (has a recorded asker or bot message).
    pub fn is_thread_engaged(&self, thread_root_id: &str) -> Result<bool, AppError> {
        if self.get_thread_asker(thread_root_id)?.is_some() {
            return Ok(true);
        }
        if self
            .get_latest_bot_message_in_thread(thread_root_id)?
            .is_some()
        {
            return Ok(true);
        }
        Ok(false)
    }

    /// Records a control event if it has not already been recorded for this (message_id, user_mxid, control).
    /// Returns Ok(true) if newly inserted, or Ok(false) if debounced (duplicate).
    pub fn record_control_if_new(
        &self,
        message_id: &str,
        user_mxid: &str,
        control: &str,
        now: i64,
    ) -> Result<bool, AppError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT 1 FROM control_events WHERE message_id = ?1 AND user_mxid = ?2 AND control = ?3")
            .map_err(|e| AppError::Store(e.to_string()))?;
        let exists = stmt
            .exists(params![message_id, user_mxid, control])
            .map_err(|e| AppError::Store(e.to_string()))?;
        if exists {
            return Ok(false);
        }
        conn.execute(
            "INSERT INTO control_events (message_id, user_mxid, control, timestamp) VALUES (?1, ?2, ?3, ?4)",
            params![message_id, user_mxid, control, now],
        )
        .map_err(|e| AppError::Store(format!("Failed to record control event: {e}")))?;
        Ok(true)
    }

    /// Checks whether an event ID has already been relayed.
    pub fn is_event_relayed(&self, event_id: &str) -> Result<bool, AppError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT 1 FROM relayed_events WHERE event_id = ?1")
            .map_err(|e| AppError::Store(e.to_string()))?;
        let exists = stmt
            .exists(params![event_id])
            .map_err(|e| AppError::Store(e.to_string()))?;
        Ok(exists)
    }

    /// Records an event ID as relayed.
    pub fn record_event_relayed(&self, event_id: &str, now: i64) -> Result<(), AppError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO relayed_events (event_id, timestamp) VALUES (?1, ?2)",
            params![event_id, now],
        )
        .map_err(|e| AppError::Store(e.to_string()))?;
        Ok(())
    }

    /// Retrieves the thread cursor (last forwarded event ID, timestamp, and optional session ID) for a thread root.
    pub fn get_thread_cursor(
        &self,
        thread_root_id: &str,
    ) -> Result<Option<ThreadCursor>, AppError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT cursor_event_id, forwarded_at, session_id FROM thread_cursors WHERE thread_root_id = ?1")
            .map_err(|e| AppError::Store(e.to_string()))?;
        let res = stmt.query_row(params![thread_root_id], |row| {
            Ok(ThreadCursor {
                cursor_event_id: row.get(0)?,
                forwarded_at: row.get(1)?,
                session_id: row.get(2)?,
            })
        });
        match res {
            Ok(cursor) => Ok(Some(cursor)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(AppError::Store(e.to_string())),
        }
    }

    /// Sets or updates the thread cursor for a thread root.
    pub fn set_thread_cursor(
        &self,
        thread_root_id: &str,
        cursor_event_id: &str,
        now: i64,
        session_id: Option<&str>,
    ) -> Result<(), AppError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO thread_cursors (thread_root_id, cursor_event_id, forwarded_at, session_id) VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(thread_root_id) DO UPDATE SET cursor_event_id = ?2, forwarded_at = ?3, session_id = ?4",
            params![thread_root_id, cursor_event_id, now, session_id],
        )
        .map_err(|e| AppError::Store(e.to_string()))?;
        Ok(())
    }

    /// Returns the count of rows across all tables in the store.
    pub fn total_row_count(&self) -> Result<usize, AppError> {
        let conn = self.conn.lock().unwrap();
        let tables = [
            "threads",
            "rate_limits",
            "bot_meta",
            "dm_rooms",
            "thread_askers",
            "bot_messages",
            "control_events",
            "relayed_events",
            "thread_cursors",
        ];
        let mut total = 0;
        for table in tables {
            let query = format!("SELECT COUNT(*) FROM {}", table);
            let count: usize = conn
                .query_row(&query, [], |row| row.get(0))
                .map_err(|e| AppError::Store(e.to_string()))?;
            total += count;
        }
        Ok(total)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadCursor {
    pub cursor_event_id: String,
    pub forwarded_at: i64,
    pub session_id: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_thread_message_storage() {
        let store = Store::new_in_memory().unwrap();
        store
            .record_thread_message("$root_1", "msg_100", 1000)
            .unwrap();
        store
            .record_thread_message("$root_1", "msg_101", 1010)
            .unwrap();

        let msgs = store.get_thread_messages("$root_1").unwrap();
        assert_eq!(msgs, vec!["msg_100", "msg_101"]);

        let empty = store.get_thread_messages("$nonexistent").unwrap();
        assert!(empty.is_empty());
    }

    #[test]
    fn test_rate_limiting_sliding_window() {
        let store = Store::new_in_memory().unwrap();
        let user = "@alice:example.org";

        for t in 0..5 {
            store.check_and_record_rate_limit(user, 5, 60, t).unwrap();
        }

        // 6th attempt within window fails
        let err = store.check_and_record_rate_limit(user, 5, 60, 10);
        assert!(matches!(err, Err(AppError::RateLimitExceeded(_))));

        // Attempt after window (t = 61) succeeds (since cutoff = 1, t = 0 expired)
        assert!(store.check_and_record_rate_limit(user, 5, 60, 61).is_ok());
    }

    #[test]
    fn test_sync_token_persistence() {
        let store = Store::new_in_memory().unwrap();
        assert_eq!(store.get_sync_token().unwrap(), None);

        store.set_sync_token("syt_batch_001").unwrap();
        assert_eq!(
            store.get_sync_token().unwrap(),
            Some("syt_batch_001".to_string())
        );

        store.set_sync_token("syt_batch_002").unwrap();
        assert_eq!(
            store.get_sync_token().unwrap(),
            Some("syt_batch_002".to_string())
        );
    }

    #[test]
    fn test_dm_room_caching() {
        let store = Store::new_in_memory().unwrap();
        assert_eq!(store.get_dm_room("@owner:example.org").unwrap(), None);

        store
            .set_dm_room("@owner:example.org", "!dm_123:example.org", 1000)
            .unwrap();
        assert_eq!(
            store.get_dm_room("@owner:example.org").unwrap(),
            Some("!dm_123:example.org".to_string())
        );

        store
            .set_dm_room("@owner:example.org", "!dm_456:example.org", 2000)
            .unwrap();
        assert_eq!(
            store.get_dm_room("@owner:example.org").unwrap(),
            Some("!dm_456:example.org".to_string())
        );
    }

    #[test]
    #[cfg(unix)]
    fn test_store_file_permissions_0600() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("test_perms.db");
        let _store = Store::new(&db_path).unwrap();
        let mode = std::fs::metadata(&db_path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "Created SQLite database must have 0600 mode");
    }

    #[test]
    fn test_relayed_events_tracking() {
        let store = Store::new_in_memory().unwrap();
        assert!(!store.is_event_relayed("$ev_1").unwrap());

        store.record_event_relayed("$ev_1", 1000).unwrap();
        assert!(store.is_event_relayed("$ev_1").unwrap());

        // Repeated record is idempotent
        store.record_event_relayed("$ev_1", 2000).unwrap();
        assert!(store.is_event_relayed("$ev_1").unwrap());
        assert!(!store.is_event_relayed("$ev_2").unwrap());
    }

    #[test]
    fn test_total_row_count() {
        let store = Store::new_in_memory().unwrap();
        assert_eq!(store.total_row_count().unwrap(), 0);
        store.record_event_relayed("$ev_1", 1000).unwrap();
        assert_eq!(store.total_row_count().unwrap(), 1);
    }

    #[test]
    fn test_thread_cursor_storage() {
        let store = Store::new_in_memory().unwrap();
        assert_eq!(store.get_thread_cursor("$root_1").unwrap(), None);

        store
            .set_thread_cursor("$root_1", "$ev_1", 1000, Some("sess_1"))
            .unwrap();
        let cursor = store.get_thread_cursor("$root_1").unwrap().unwrap();
        assert_eq!(cursor.cursor_event_id, "$ev_1");
        assert_eq!(cursor.forwarded_at, 1000);
        assert_eq!(cursor.session_id.as_deref(), Some("sess_1"));

        // Update cursor with new session
        store
            .set_thread_cursor("$root_1", "$ev_2", 2000, Some("sess_2"))
            .unwrap();
        let updated = store.get_thread_cursor("$root_1").unwrap().unwrap();
        assert_eq!(updated.cursor_event_id, "$ev_2");
        assert_eq!(updated.forwarded_at, 2000);
        assert_eq!(updated.session_id.as_deref(), Some("sess_2"));
    }

    #[test]
    fn test_thread_cursor_migration_from_unversioned() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        // Create table without session_id column (pre-M16)
        conn.execute_batch(
            "CREATE TABLE thread_cursors (
                thread_root_id TEXT PRIMARY KEY,
                cursor_event_id TEXT NOT NULL,
                forwarded_at INTEGER NOT NULL
            );
            INSERT INTO thread_cursors VALUES ('$root_old', '$ev_old', 500);",
        )
        .unwrap();

        let store = Store {
            conn: Mutex::new(conn),
        };
        store.init_schema().unwrap();

        let cursor = store.get_thread_cursor("$root_old").unwrap().unwrap();
        assert_eq!(cursor.cursor_event_id, "$ev_old");
        assert_eq!(cursor.forwarded_at, 500);
        assert_eq!(cursor.session_id, None);
    }
}
