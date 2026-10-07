use crate::error::AppError;
use async_trait::async_trait;
use std::sync::Mutex;

#[async_trait]
pub trait MatrixClient: Send + Sync {
    /// Posts a message as m.notice into a room, optionally threaded and mentioning the asking user.
    async fn send_notice(
        &self,
        room_id: &str,
        thread_root_id: Option<&str>,
        body: &str,
        mention_user: Option<&str>,
    ) -> Result<String, AppError>;

    /// Sends a direct escalation message to the owner MXID.
    async fn send_dm(&self, user_id: &str, body: &str) -> Result<String, AppError>;
}

/// Checks whether the incoming Matrix event mentions the bot.
pub fn is_bot_mentioned(
    event_body: &str,
    formatted_body: Option<&str>,
    mentions: Option<&[String]>,
    bot_mxid: &str,
) -> bool {
    // 1. Explicit m.mentions.user_ids array
    if let Some(user_ids) = mentions {
        if user_ids.iter().any(|u| u == bot_mxid) {
            return true;
        }
    }

    // 2. Matrix pill in formatted_body (e.g. <a href="https://matrix.to/#/@genie:example.org">)
    if let Some(html) = formatted_body {
        let pill_link = format!("matrix.to/#/{bot_mxid}");
        if html.contains(&pill_link) {
            return true;
        }
    }

    // 3. Plain text exact MXID mention
    if event_body.contains(bot_mxid) {
        return true;
    }

    // 4. Localpart mention (e.g. "@genie" if bot_mxid is "@genie:example.org")
    if let Some(localpart) = bot_mxid.split(':').next() {
        if !localpart.is_empty() && event_body.contains(localpart) {
            return true;
        }
    }

    false
}

#[derive(Debug, Clone)]
pub struct SentNotice {
    pub room_id: String,
    pub thread_root_id: Option<String>,
    pub body: String,
    pub mention_user: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SentDm {
    pub user_id: String,
    pub body: String,
}

#[derive(Default)]
pub struct MockMatrixClient {
    pub sent_notices: Mutex<Vec<SentNotice>>,
    pub sent_dms: Mutex<Vec<SentDm>>,
}

#[async_trait]
impl MatrixClient for MockMatrixClient {
    async fn send_notice(
        &self,
        room_id: &str,
        thread_root_id: Option<&str>,
        body: &str,
        mention_user: Option<&str>,
    ) -> Result<String, AppError> {
        let mut list = self.sent_notices.lock().unwrap();
        list.push(SentNotice {
            room_id: room_id.to_string(),
            thread_root_id: thread_root_id.map(|s| s.to_string()),
            body: body.to_string(),
            mention_user: mention_user.map(|s| s.to_string()),
        });
        Ok(format!("$mock_event_{}", list.len()))
    }

    async fn send_dm(&self, user_id: &str, body: &str) -> Result<String, AppError> {
        let mut list = self.sent_dms.lock().unwrap();
        list.push(SentDm {
            user_id: user_id.to_string(),
            body: body.to_string(),
        });
        Ok(format!("$mock_dm_{}", list.len()))
    }
}
