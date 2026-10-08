use crate::error::AppError;
use rusqlite::{params, Connection};
use std::path::Path;
use std::sync::Mutex;

pub struct Store {
    conn: Mutex<Connection>,
}

impl Store {
    pub fn new(path: &Path) -> Result<Self, AppError> {
        let conn = Connection::open(path).map_err(|e| {
            AppError::Store(format!(
                "Failed to open SQLite database at {}: {e}",
                path.display()
            ))
        })?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
        }
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
            );",
        )
        .map_err(|e| AppError::Store(format!("Failed to initialize SQLite schema: {e}")))?;
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
}
