use crate::context::EventMessage;
use crate::error::AppError;
use async_trait::async_trait;
use matrix_sdk::Client;
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

    /// Fetches recent message history for a room in chronological order.
    async fn fetch_history(
        &self,
        room_id: &str,
        limit: usize,
    ) -> Result<Vec<EventMessage>, AppError>;
}

/// Real Matrix client backed by `matrix-sdk`.
pub struct MatrixSdkClient {
    client: Client,
}

impl MatrixSdkClient {
    pub async fn new(
        homeserver_url: &str,
        bot_mxid: &str,
        access_token: &str,
    ) -> Result<Self, AppError> {
        let client = Client::builder()
            .homeserver_url(homeserver_url)
            .build()
            .await
            .map_err(|e| AppError::Matrix(format!("Failed to build Matrix client: {e}")))?;

        let user_id = <&matrix_sdk::ruma::UserId>::try_from(bot_mxid)
            .map_err(|e| AppError::Config(format!("Invalid bot_mxid '{bot_mxid}': {e}")))?;

        let meta = matrix_sdk::SessionMeta {
            user_id: user_id.to_owned(),
            device_id: matrix_sdk::ruma::owned_device_id!("BOT"),
        };
        let tokens = matrix_sdk::SessionTokens {
            access_token: access_token.to_string(),
            refresh_token: None,
        };
        let session = matrix_sdk::authentication::matrix::MatrixSession { meta, tokens };

        client
            .matrix_auth()
            .restore_session(session, Default::default())
            .await
            .map_err(|e| AppError::Matrix(format!("Failed to restore session: {e}")))?;

        Ok(Self { client })
    }

    pub fn inner(&self) -> &Client {
        &self.client
    }
}

pub fn parse_timeline_event(
    event: matrix_sdk::ruma::events::AnySyncTimelineEvent,
) -> Option<EventMessage> {
    use matrix_sdk::ruma::events::room::message::Relation;
    use matrix_sdk::ruma::events::{
        AnySyncMessageLikeEvent, AnySyncTimelineEvent, SyncMessageLikeEvent,
    };

    match event {
        AnySyncTimelineEvent::MessageLike(AnySyncMessageLikeEvent::RoomMessage(
            SyncMessageLikeEvent::Original(orig),
        )) => {
            let thread_root_id = match &orig.content.relates_to {
                Some(Relation::Thread(t)) => Some(t.event_id.to_string()),
                _ => None,
            };
            Some(EventMessage {
                event_id: orig.event_id.to_string(),
                sender_mxid: orig.sender.to_string(),
                timestamp_ms: u64::from(orig.origin_server_ts.0) as i64,
                body: orig.content.body().to_string(),
                thread_root_id,
            })
        }
        _ => None,
    }
}

#[async_trait]
impl MatrixClient for MatrixSdkClient {
    async fn send_notice(
        &self,
        room_id: &str,
        thread_root_id: Option<&str>,
        body: &str,
        mention_user: Option<&str>,
    ) -> Result<String, AppError> {
        use matrix_sdk::ruma::events::relation::Thread;
        use matrix_sdk::ruma::events::room::message::{Relation, RoomMessageEventContent};
        use matrix_sdk::ruma::events::Mentions;
        use matrix_sdk::ruma::{EventId, RoomId, UserId};

        let r_id = <&RoomId>::try_from(room_id)
            .map_err(|e| AppError::Matrix(format!("Invalid room ID '{room_id}': {e}")))?;

        let room = self.client.get_room(r_id).ok_or_else(|| {
            AppError::Matrix(format!("Room not found in client state: {room_id}"))
        })?;

        let mut content = RoomMessageEventContent::notice_markdown(body);

        if let Some(user_mxid) = mention_user {
            if let Ok(u_id) = <&UserId>::try_from(user_mxid) {
                content.mentions = Some(Mentions::with_user_ids([u_id.to_owned()]));
            }
        }

        if let Some(root_id_str) = thread_root_id {
            let root_id = <&EventId>::try_from(root_id_str).map_err(|e| {
                AppError::Matrix(format!("Invalid thread root ID '{root_id_str}': {e}"))
            })?;
            content.relates_to = Some(Relation::Thread(Thread::plain(
                root_id.to_owned(),
                root_id.to_owned(),
            )));
        }

        let resp = room
            .send(content)
            .await
            .map_err(|e| AppError::Matrix(format!("Failed to send notice: {e}")))?;

        Ok(resp.response.event_id.to_string())
    }

    async fn send_dm(&self, user_id: &str, body: &str) -> Result<String, AppError> {
        use matrix_sdk::ruma::api::client::room::create_room::v3::Request as CreateRoomRequest;
        use matrix_sdk::ruma::events::room::message::RoomMessageEventContent;
        use matrix_sdk::ruma::UserId;

        let u_id = <&UserId>::try_from(user_id)
            .map_err(|e| AppError::Matrix(format!("Invalid owner user ID '{user_id}': {e}")))?;

        let mut req = CreateRoomRequest::new();
        req.is_direct = true;
        req.invite = vec![u_id.to_owned()];

        let room = self
            .client
            .create_room(req)
            .await
            .map_err(|e| AppError::Matrix(format!("Failed to create DM room: {e}")))?;

        let content = RoomMessageEventContent::notice_markdown(body);
        let send_resp = room
            .send(content)
            .await
            .map_err(|e| AppError::Matrix(format!("Failed to send DM message: {e}")))?;

        Ok(send_resp.response.event_id.to_string())
    }

    async fn fetch_history(
        &self,
        room_id: &str,
        limit: usize,
    ) -> Result<Vec<EventMessage>, AppError> {
        use matrix_sdk::room::MessagesOptions;
        use matrix_sdk::ruma::RoomId;

        let r_id = <&RoomId>::try_from(room_id)
            .map_err(|e| AppError::Matrix(format!("Invalid room ID '{room_id}': {e}")))?;

        let room = match self.client.get_room(r_id) {
            Some(r) => r,
            None => return Ok(Vec::new()),
        };

        let mut opts = MessagesOptions::backward();
        opts.limit = u16::try_from(limit).unwrap_or(50).into();

        let res = room
            .messages(opts)
            .await
            .map_err(|e| AppError::Matrix(format!("Failed to fetch room messages: {e}")))?;

        let mut messages = Vec::new();
        for ev in res.chunk {
            if let Ok(timeline_event) = ev.raw().deserialize() {
                if let Some(msg) = parse_timeline_event(timeline_event) {
                    messages.push(msg);
                }
            }
        }
        messages.reverse();
        Ok(messages)
    }
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
    pub canned_history: Mutex<Vec<EventMessage>>,
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

    async fn fetch_history(
        &self,
        _room_id: &str,
        _limit: usize,
    ) -> Result<Vec<EventMessage>, AppError> {
        let list = self.canned_history.lock().unwrap();
        Ok(list.clone())
    }
}
