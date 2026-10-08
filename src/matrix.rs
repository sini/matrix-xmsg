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

    /// Attaches the SQLite Store for DM room caching and state persistence.
    fn set_store(&self, _store: std::sync::Arc<crate::store::Store>) {}
}

/// Real Matrix client backed by `matrix-sdk`.
pub struct MatrixSdkClient {
    client: Client,
    dm_rooms: Mutex<std::collections::HashMap<String, String>>,
    store: Mutex<Option<std::sync::Arc<crate::store::Store>>>,
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

        Ok(Self {
            client,
            dm_rooms: Mutex::new(std::collections::HashMap::new()),
            store: Mutex::new(None),
        })
    }

    pub fn set_store(&self, store: std::sync::Arc<crate::store::Store>) {
        let mut guard = self.store.lock().unwrap();
        *guard = Some(store);
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

async fn is_owner_member_of_room(
    client: &matrix_sdk::Client,
    room_id: &matrix_sdk::ruma::RoomId,
    user_id: &matrix_sdk::ruma::UserId,
) -> bool {
    use matrix_sdk::ruma::api::client::state::get_state_event_for_key::v3::Request as GetStateEventRequest;
    use matrix_sdk::ruma::events::StateEventType;

    let req = GetStateEventRequest::new(
        room_id.to_owned(),
        StateEventType::RoomMember,
        user_id.to_string(),
    );

    if let Ok(resp) = client.send(req).await {
        if let Ok(val) = serde_json::from_str::<serde_json::Value>(resp.event_or_content.get()) {
            let direct_m = val.get("membership").and_then(|m| m.as_str());
            let content_m = val
                .get("content")
                .and_then(|c| c.get("membership"))
                .and_then(|m| m.as_str());
            let chunk_m = val
                .get("chunk")
                .and_then(|arr| arr.as_array())
                .and_then(|list| {
                    list.iter()
                        .find(|ev| {
                            ev.get("state_key").and_then(|k| k.as_str()) == Some(user_id.as_str())
                        })
                        .and_then(|ev| {
                            ev.get("content")
                                .and_then(|c| c.get("membership"))
                                .and_then(|m| m.as_str())
                        })
                });
            let membership = direct_m.or(content_m).or(chunk_m);
            return matches!(membership, Some("join") | Some("invite"));
        }
    }
    false
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
        use matrix_sdk::ruma::{RoomId, UserId};

        let u_id = <&UserId>::try_from(user_id)
            .map_err(|e| AppError::Matrix(format!("Invalid owner user ID '{user_id}': {e}")))?;

        // F6 & N2: Reuse existing DM room for this user/owner if cached and valid
        let cached_room_id: Option<String> = {
            let in_memory = self.dm_rooms.lock().unwrap().get(user_id).cloned();
            if in_memory.is_some() {
                in_memory
            } else {
                let store_guard = self.store.lock().unwrap();
                if let Some(store) = store_guard.as_ref() {
                    store.get_dm_room(user_id).ok().flatten()
                } else {
                    None
                }
            }
        };

        let content = RoomMessageEventContent::notice_markdown(body);

        if let Some(ref r_id_str) = cached_room_id {
            if let Ok(r_id) = <&RoomId>::try_from(r_id_str.as_str()) {
                // R1: Query the server for the owner's membership of the cached DM room.
                // Reuse only if the owner has joined or been invited; otherwise create a new room.
                if is_owner_member_of_room(&self.client, r_id, u_id).await {
                    if let Some(room) = self.client.get_room(r_id) {
                        // Arm A: room in SDK state
                        if let Ok(send_resp) = room.send(content.clone()).await {
                            self.dm_rooms
                                .lock()
                                .unwrap()
                                .insert(user_id.to_string(), r_id_str.clone());
                            return Ok(send_resp.response.event_id.to_string());
                        }
                    } else {
                        // Arm B: room not in SDK state (e.g. after restart)
                        use matrix_sdk::ruma::api::client::message::send_message_event::v3::Request as SendMessageEventRequest;
                        use matrix_sdk::ruma::events::MessageLikeEventType;
                        use matrix_sdk::ruma::TransactionId;

                        if let Ok(raw_content) = serde_json::to_string(&content) {
                            if let Ok(raw_val) =
                                matrix_sdk::ruma::serde::Raw::from_json_string(raw_content)
                            {
                                let req = SendMessageEventRequest::new_raw(
                                    r_id.to_owned(),
                                    TransactionId::new(),
                                    MessageLikeEventType::RoomMessage,
                                    raw_val,
                                );
                                if let Ok(resp) = self.client.send(req).await {
                                    self.dm_rooms
                                        .lock()
                                        .unwrap()
                                        .insert(user_id.to_string(), r_id_str.clone());
                                    return Ok(resp.event_id.to_string());
                                }
                            }
                        }
                    }
                }
            }
        }

        let mut req = CreateRoomRequest::new();
        req.is_direct = true;
        req.invite = vec![u_id.to_owned()];

        let created = self
            .client
            .create_room(req)
            .await
            .map_err(|e| AppError::Matrix(format!("Failed to create DM room: {e}")))?;

        let new_room_id = created.room_id().to_string();
        self.dm_rooms
            .lock()
            .unwrap()
            .insert(user_id.to_string(), new_room_id.clone());

        {
            let store_guard = self.store.lock().unwrap();
            if let Some(store) = store_guard.as_ref() {
                let now = chrono::Utc::now().timestamp();
                let _ = store.set_dm_room(user_id, &new_room_id, now);
            }
        }

        let send_resp = created
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

    fn set_store(&self, store: std::sync::Arc<crate::store::Store>) {
        self.set_store(store);
    }
}

fn is_full_mxid_mentioned(event_body: &str, bot_mxid: &str) -> bool {
    let mut start = 0;
    while let Some(pos) = event_body[start..].find(bot_mxid) {
        let idx = start + pos;
        let after_idx = idx + bot_mxid.len();

        let boundary_before = idx == 0
            || event_body[..idx]
                .chars()
                .last()
                .is_none_or(|c| !c.is_alphanumeric() && c != '@');

        let boundary_after = if after_idx == event_body.len() {
            true
        } else {
            let remainder = &event_body[after_idx..];
            let next_char = remainder.chars().next().unwrap();
            if next_char.is_alphanumeric() || next_char == '-' || next_char == '_' {
                false
            } else if next_char == '.' || next_char == '/' {
                remainder[1..]
                    .chars()
                    .next()
                    .is_none_or(|c| !c.is_alphanumeric() && c != '-' && c != '_')
            } else if next_char == ':' {
                remainder[1..]
                    .chars()
                    .next()
                    .is_none_or(|c| c.is_whitespace())
            } else {
                true
            }
        };

        if boundary_before && boundary_after {
            return true;
        }
        start = idx + bot_mxid.len();
    }
    false
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

    // 3. Plain text exact MXID mention with token boundary checks
    if is_full_mxid_mentioned(event_body, bot_mxid) {
        return true;
    }

    // 4. Localpart mention with boundary checks (e.g. "@genie" if bot_mxid is "@genie:example.org")
    if let Some(localpart) = bot_mxid.split(':').next() {
        if !localpart.is_empty() {
            let mut start = 0;
            while let Some(pos) = event_body[start..].find(localpart) {
                let idx = start + pos;
                let after_idx = idx + localpart.len();
                let boundary_before = idx == 0
                    || event_body[..idx]
                        .chars()
                        .last()
                        .is_none_or(|c| !c.is_alphanumeric() && c != '@');
                let boundary_after = if after_idx == event_body.len() {
                    true
                } else {
                    let remainder = &event_body[after_idx..];
                    let next_char = remainder.chars().next().unwrap();
                    if next_char.is_alphanumeric() || next_char == '-' || next_char == '_' {
                        false
                    } else if next_char == ':' {
                        remainder[1..]
                            .chars()
                            .next()
                            .is_none_or(|c| c.is_whitespace())
                    } else if next_char == '.' || next_char == '/' {
                        remainder[1..]
                            .chars()
                            .next()
                            .is_none_or(|c| !c.is_alphanumeric() && c != '-' && c != '_')
                    } else {
                        true
                    }
                };
                if boundary_before && boundary_after {
                    return true;
                }
                start = idx + localpart.len();
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_bot_mentioned_boundary_checks() {
        let bot_mxid = "@genie:example.org";

        // Positive matches
        assert!(is_bot_mentioned("@genie help", None, None, bot_mxid));
        assert!(is_bot_mentioned(
            "Hey @genie, what's up?",
            None,
            None,
            bot_mxid
        ));
        assert!(is_bot_mentioned("Hello @genie!", None, None, bot_mxid));
        assert!(is_bot_mentioned(
            "Contact @genie:example.org",
            None,
            None,
            bot_mxid
        ));
        assert!(is_bot_mentioned(
            "can you help @genie",
            None,
            None,
            bot_mxid
        ));

        // Negative matches: substrings that should NOT trigger bot
        assert!(!is_bot_mentioned(
            "Hey @genies how are you?",
            None,
            None,
            bot_mxid
        ));
        assert!(!is_bot_mentioned(
            "Contact @genie-x:evil.org",
            None,
            None,
            bot_mxid
        ));
        assert!(!is_bot_mentioned(
            "email user@genie.org",
            None,
            None,
            bot_mxid
        ));
        assert!(!is_bot_mentioned("Look at @@genie", None, None, bot_mxid));
        assert!(is_bot_mentioned("hey @genie", None, None, bot_mxid));
        assert!(is_bot_mentioned("@genie: help", None, None, bot_mxid));
        assert!(is_bot_mentioned("ask @genie.", None, None, bot_mxid));
        assert!(!is_bot_mentioned(
            "ask @genie:evil.org",
            None,
            None,
            bot_mxid
        ));
        assert!(!is_bot_mentioned("@genie.bot hi", None, None, bot_mxid));
        assert!(!is_bot_mentioned("@genie/x", None, None, bot_mxid));
    }
}
