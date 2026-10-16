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

    /// Sends a reaction (m.reaction) to an event in a room.
    async fn send_reaction(
        &self,
        room_id: &str,
        event_id: &str,
        key: &str,
    ) -> Result<String, AppError>;

    /// Redacts an event in a room.
    async fn redact_event(
        &self,
        room_id: &str,
        event_id: &str,
        reason: Option<&str>,
    ) -> Result<(), AppError>;

    /// Edits an existing bot event in a room using an `m.replace` replacement relation.
    async fn edit_notice(
        &self,
        _room_id: &str,
        _target_event_id: &str,
        _new_body: &str,
    ) -> Result<String, AppError> {
        Ok("$mock_edit_event".to_string())
    }

    /// Fetches a single event by ID in a room.
    async fn fetch_event(
        &self,
        _room_id: &str,
        _event_id: &str,
    ) -> Result<Option<EventMessage>, AppError> {
        Ok(None)
    }

    /// Fetches a single page of thread relations for a given thread root.
    /// Returns (events_in_page, next_batch_token).
    async fn fetch_thread_relations_page(
        &self,
        _room_id: &str,
        _thread_root_id: &str,
        _from_token: Option<&str>,
        _limit: usize,
    ) -> Result<(Vec<EventMessage>, Option<String>), AppError> {
        Ok((Vec::new(), None))
    }

    /// Fetches the full history of a thread from its root, paged.
    async fn fetch_thread_history(
        &self,
        _room_id: &str,
        _thread_root_id: &str,
    ) -> Result<Vec<EventMessage>, AppError> {
        Ok(Vec::new())
    }

    /// Attaches the SQLite Store for DM room caching and state persistence.
    fn set_store(&self, _store: std::sync::Arc<crate::store::Store>) {}

    /// Resolves configured rooms in the client state (e.g. at startup before serving the inbox).
    async fn resolve_rooms(&self, _rooms: &[String]) -> Result<(), AppError> {
        Ok(())
    }
}

/// Real Matrix client backed by `matrix-sdk`.
pub struct MatrixSdkClient {
    client: Client,
    dm_rooms: Mutex<std::collections::HashMap<String, String>>,
    store: Mutex<Option<std::sync::Arc<crate::store::Store>>>,
    configured_rooms: Mutex<std::collections::HashSet<String>>,
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
            configured_rooms: Mutex::new(std::collections::HashSet::new()),
        })
    }

    pub fn set_store(&self, store: std::sync::Arc<crate::store::Store>) {
        let mut guard = self.store.lock().unwrap();
        *guard = Some(store);
    }

    pub fn set_configured_rooms(&self, rooms: &[String]) {
        let mut conf = self.configured_rooms.lock().unwrap();
        conf.clear();
        for r in rooms {
            conf.insert(r.clone());
        }
    }

    pub fn is_room_configured(&self, room_id: &str) -> bool {
        self.configured_rooms.lock().unwrap().contains(room_id)
    }

    pub fn inner(&self) -> &Client {
        &self.client
    }

    pub async fn resolve_rooms(&self, rooms: &[String]) -> Result<(), AppError> {
        use matrix_sdk::ruma::RoomId;
        {
            let mut conf = self.configured_rooms.lock().unwrap();
            for r in rooms {
                conf.insert(r.clone());
            }
        }
        for room_str in rooms {
            if let Ok(r_id) = <&RoomId>::try_from(room_str.as_str()) {
                if self.client.get_room(r_id).is_none() {
                    tracing::info!("Resolving room {room_str} into client state");
                    if let Err(e) = self.client.join_room_by_id(r_id).await {
                        tracing::warn!("Failed to resolve/join configured room {room_str}: {e}");
                    }
                }
            }
        }
        Ok(())
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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IncomingMatrixEvent {
    pub room_id: String,
    pub event_id: String,
    pub sender_mxid: String,
    pub body: String,
    pub formatted_body: Option<String>,
    pub mentions: Option<Vec<String>>,
    pub timestamp_ms: i64,
    pub thread_root_id: Option<String>,
    pub replaces_event_id: Option<String>,
    pub in_reply_to_event_id: Option<String>,
    pub is_falling_back: bool,
}

/// Extracts an `IncomingMatrixEvent` from a Matrix SDK `SyncRoomMessageEvent`.
pub fn extract_incoming_event(
    event: &matrix_sdk::ruma::events::room::message::SyncRoomMessageEvent,
    room_id: &matrix_sdk::ruma::RoomId,
) -> Option<IncomingMatrixEvent> {
    use matrix_sdk::ruma::events::room::message::{MessageType, Relation, SyncRoomMessageEvent};

    if let SyncRoomMessageEvent::Original(orig) = event {
        if let Some(Relation::Replacement(repl)) = &orig.content.relates_to {
            let formatted_body = match &repl.new_content.msgtype {
                MessageType::Text(t) => t.formatted.as_ref().map(|f| f.body.clone()),
                MessageType::Notice(n) => n.formatted.as_ref().map(|f| f.body.clone()),
                _ => None,
            };
            let mentions = repl
                .new_content
                .mentions
                .as_ref()
                .map(|m| m.user_ids.iter().map(|u| u.to_string()).collect::<Vec<_>>());
            let body = repl.new_content.msgtype.body().to_string();

            return Some(IncomingMatrixEvent {
                room_id: room_id.to_string(),
                event_id: orig.event_id.to_string(),
                sender_mxid: orig.sender.to_string(),
                body,
                formatted_body,
                mentions,
                timestamp_ms: u64::from(orig.origin_server_ts.0) as i64,
                thread_root_id: None,
                replaces_event_id: Some(repl.event_id.to_string()),
                in_reply_to_event_id: None,
                is_falling_back: false,
            });
        }

        let formatted_body = match &orig.content.msgtype {
            MessageType::Text(t) => t.formatted.as_ref().map(|f| f.body.clone()),
            MessageType::Notice(n) => n.formatted.as_ref().map(|f| f.body.clone()),
            _ => None,
        };
        let mentions = orig
            .content
            .mentions
            .as_ref()
            .map(|m| m.user_ids.iter().map(|u| u.to_string()).collect::<Vec<_>>());

        let (thread_root_id, in_reply_to_event_id, is_falling_back) = match &orig.content.relates_to
        {
            Some(Relation::Thread(t)) => (
                Some(t.event_id.to_string()),
                t.in_reply_to.as_ref().map(|r| r.event_id.to_string()),
                t.is_falling_back,
            ),
            Some(Relation::Reply(rep)) => (None, Some(rep.in_reply_to.event_id.to_string()), false),
            _ => (None, None, false),
        };

        Some(IncomingMatrixEvent {
            room_id: room_id.to_string(),
            event_id: orig.event_id.to_string(),
            sender_mxid: orig.sender.to_string(),
            body: orig.content.body().to_string(),
            formatted_body,
            mentions,
            timestamp_ms: u64::from(orig.origin_server_ts.0) as i64,
            thread_root_id,
            replaces_event_id: None,
            in_reply_to_event_id,
            is_falling_back,
        })
    } else {
        None
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

/// Extracts matrix.to user mention links, validates MXIDs, deduplicates them,
/// refuses room mentions, and transforms bare matrix.to URLs into Markdown links
/// so that the Markdown renderer produces HTML pills (<a href="...">display</a>).
pub fn parse_mentions_and_render_pills(text: &str) -> (String, Vec<matrix_sdk::ruma::OwnedUserId>) {
    use matrix_sdk::ruma::UserId;
    use regex::Regex;
    use std::sync::OnceLock;

    static LINK_RE: OnceLock<Regex> = OnceLock::new();
    let re = LINK_RE.get_or_init(|| {
        Regex::new(r"(?:\[([^\]]+)\]\((https://matrix\.to/#/(@[^\s\)]+))\)|<https://matrix\.to/#/(@[^\s>]+)>|(https://matrix\.to/#/(@[^\s\)]+)))").unwrap()
    });

    let mut user_ids = Vec::new();
    let mut seen = std::collections::HashSet::new();

    let mut output = String::with_capacity(text.len());
    let mut last_end = 0;

    for caps in re.captures_iter(text) {
        let whole_match = caps.get(0).unwrap();
        output.push_str(&text[last_end..whole_match.start()]);

        if let Some(_disp_match) = caps.get(1) {
            // Case 1: [display_name](https://matrix.to/#/@...)
            let raw_mxid = caps.get(3).unwrap().as_str();
            let clean_mxid = raw_mxid
                .split('?')
                .next()
                .unwrap_or(raw_mxid)
                .trim_end_matches(['.', ',', ';', ':', '!', '?', '/']);
            if let Ok(uid) = <&UserId>::try_from(clean_mxid) {
                let owned = uid.to_owned();
                if seen.insert(owned.clone()) {
                    user_ids.push(owned);
                }
            }
            output.push_str(whole_match.as_str());
        } else if let Some(bracket_mxid) = caps.get(4) {
            // Case 2: <https://matrix.to/#/@...>
            let raw_mxid = bracket_mxid.as_str();
            let clean_mxid = raw_mxid
                .split('?')
                .next()
                .unwrap_or(raw_mxid)
                .trim_end_matches(['.', ',', ';', ':', '!', '?', '/']);
            if let Ok(uid) = <&UserId>::try_from(clean_mxid) {
                let owned = uid.to_owned();
                if seen.insert(owned.clone()) {
                    user_ids.push(owned);
                }
                output.push_str(&format!("[{clean_mxid}](https://matrix.to/#/{clean_mxid})"));
            } else {
                output.push_str(whole_match.as_str());
            }
        } else if let Some(_bare_url) = caps.get(5) {
            // Case 3: bare https://matrix.to/#/@...
            let raw_mxid = caps.get(6).unwrap().as_str();
            let clean_mxid = raw_mxid
                .split('?')
                .next()
                .unwrap_or(raw_mxid)
                .trim_end_matches(['.', ',', ';', ':', '!', '?', '/']);
            let trailing = &raw_mxid[clean_mxid.len()..];
            if let Ok(uid) = <&UserId>::try_from(clean_mxid) {
                let owned = uid.to_owned();
                if seen.insert(owned.clone()) {
                    user_ids.push(owned);
                }
                output.push_str(&format!(
                    "[{clean_mxid}](https://matrix.to/#/{clean_mxid}){trailing}"
                ));
            } else {
                output.push_str(whole_match.as_str());
            }
        }

        last_end = whole_match.end();
    }

    output.push_str(&text[last_end..]);

    if text.contains("@room") {
        tracing::debug!("Explicit @room mention in text was refused by default");
    }

    (output, user_ids)
}

/// Constructs a RoomMessageEventContent with Markdown formatting and MSC3952 mentions.
/// Validates and deduplicates matrix.to user links, renders them as pills,
/// and unconditionally refuses @room room mentions.
pub fn build_notice_content(
    body: &str,
    direct_mention_user: Option<&str>,
) -> matrix_sdk::ruma::events::room::message::RoomMessageEventContent {
    use matrix_sdk::ruma::events::room::message::RoomMessageEventContent;
    use matrix_sdk::ruma::events::Mentions;
    use matrix_sdk::ruma::UserId;

    let (processed_body, mut user_ids) = parse_mentions_and_render_pills(body);

    if let Some(direct_user) = direct_mention_user {
        if let Ok(uid) = <&UserId>::try_from(direct_user) {
            let owned = uid.to_owned();
            if !user_ids.contains(&owned) {
                user_ids.push(owned);
            }
        }
    }

    let mut content = RoomMessageEventContent::notice_markdown(&processed_body);

    if user_ids.is_empty() {
        // MSC3952: An empty m.mentions object signals "mentions nobody",
        // which disables legacy body-based pings in compliant clients.
        content.mentions = Some(Mentions::new());
    } else {
        // Intentional mentions for the extracted user IDs.
        // Room mentions are refused (Mentions::with_user_ids sets room: false/omitted).
        content.mentions = Some(Mentions::with_user_ids(user_ids));
    }

    content
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
        use matrix_sdk::ruma::events::room::message::Relation;
        use matrix_sdk::ruma::{EventId, RoomId};

        let r_id = <&RoomId>::try_from(room_id)
            .map_err(|e| AppError::Matrix(format!("Invalid room ID '{room_id}': {e}")))?;

        let mut content = build_notice_content(body, mention_user);

        if let Some(root_id_str) = thread_root_id {
            let root_id = <&EventId>::try_from(root_id_str).map_err(|e| {
                AppError::Matrix(format!("Invalid thread root ID '{root_id_str}': {e}"))
            })?;
            content.relates_to = Some(Relation::Thread(Thread::plain(
                root_id.to_owned(),
                root_id.to_owned(),
            )));
        }

        if let Some(room) = self.client.get_room(r_id) {
            let resp = room
                .send(content)
                .await
                .map_err(|e| AppError::Matrix(format!("Failed to send notice: {e}")))?;
            return Ok(resp.response.event_id.to_string());
        }

        // Cold room fallback: attempt to join/resolve into client state (restricted to configured rooms)
        let is_configured = self.configured_rooms.lock().unwrap().contains(room_id);
        if is_configured {
            if let Ok(room) = self.client.join_room_by_id(r_id).await {
                let resp = room
                    .send(content)
                    .await
                    .map_err(|e| AppError::Matrix(format!("Failed to send notice: {e}")))?;
                return Ok(resp.response.event_id.to_string());
            }
        }

        // Direct HTTP send fallback (matching send_dm Arm B)
        use matrix_sdk::ruma::api::client::message::send_message_event::v3::Request as SendMessageEventRequest;
        use matrix_sdk::ruma::events::MessageLikeEventType;
        use matrix_sdk::ruma::TransactionId;

        if let Ok(raw_content) = serde_json::to_string(&content) {
            if let Ok(raw_val) = matrix_sdk::ruma::serde::Raw::from_json_string(raw_content) {
                let req = SendMessageEventRequest::new_raw(
                    r_id.to_owned(),
                    TransactionId::new(),
                    MessageLikeEventType::RoomMessage,
                    raw_val,
                );
                if let Ok(resp) = self.client.send(req).await {
                    return Ok(resp.event_id.to_string());
                }
            }
        }

        Err(AppError::Matrix(format!(
            "Room not found in client state: {room_id}"
        )))
    }

    async fn send_dm(&self, user_id: &str, body: &str) -> Result<String, AppError> {
        use matrix_sdk::ruma::api::client::room::create_room::v3::Request as CreateRoomRequest;
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

        let content = build_notice_content(body, None);

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

    async fn send_reaction(
        &self,
        room_id: &str,
        event_id: &str,
        key: &str,
    ) -> Result<String, AppError> {
        use matrix_sdk::ruma::events::reaction::ReactionEventContent;
        use matrix_sdk::ruma::events::relation::Annotation;
        use matrix_sdk::ruma::{EventId, RoomId};

        let r_id = <&RoomId>::try_from(room_id)
            .map_err(|e| AppError::Matrix(format!("Invalid room ID '{room_id}': {e}")))?;
        let e_id = <&EventId>::try_from(event_id)
            .map_err(|e| AppError::Matrix(format!("Invalid event ID '{event_id}': {e}")))?;

        let room = self.client.get_room(r_id).ok_or_else(|| {
            AppError::Matrix(format!("Room not found in client state: {room_id}"))
        })?;

        let content = ReactionEventContent::new(Annotation::new(e_id.to_owned(), key.to_string()));
        let resp = room
            .send(content)
            .await
            .map_err(|e| AppError::Matrix(format!("Failed to send reaction: {e}")))?;

        Ok(resp.response.event_id.to_string())
    }

    async fn redact_event(
        &self,
        room_id: &str,
        event_id: &str,
        reason: Option<&str>,
    ) -> Result<(), AppError> {
        use matrix_sdk::ruma::{EventId, RoomId};

        let r_id = <&RoomId>::try_from(room_id)
            .map_err(|e| AppError::Matrix(format!("Invalid room ID '{room_id}': {e}")))?;
        let e_id = <&EventId>::try_from(event_id)
            .map_err(|e| AppError::Matrix(format!("Invalid event ID '{event_id}': {e}")))?;

        if let Some(room) = self.client.get_room(r_id) {
            room.redact(e_id, reason, None)
                .await
                .map_err(|e| AppError::Matrix(format!("Failed to redact event: {e}")))?;
            return Ok(());
        }

        // Cold room fallback: attempt to join/resolve into client state (restricted to configured rooms)
        let is_configured = self.configured_rooms.lock().unwrap().contains(room_id);
        if is_configured {
            if let Ok(room) = self.client.join_room_by_id(r_id).await {
                room.redact(e_id, reason, None)
                    .await
                    .map_err(|e| AppError::Matrix(format!("Failed to redact event: {e}")))?;
                return Ok(());
            }
        }

        // Direct HTTP redact fallback (e.g. after bot restart before room is synced into SDK state)
        use matrix_sdk::ruma::api::client::redact::redact_event::v3::Request as RedactEventRequest;
        use matrix_sdk::ruma::TransactionId;

        let mut req =
            RedactEventRequest::new(r_id.to_owned(), e_id.to_owned(), TransactionId::new());
        req.reason = reason.map(str::to_string);
        self.client
            .send(req)
            .await
            .map_err(|e| AppError::Matrix(format!("Failed to redact event: {e}")))?;

        Ok(())
    }

    async fn edit_notice(
        &self,
        room_id: &str,
        target_event_id: &str,
        new_body: &str,
    ) -> Result<String, AppError> {
        use matrix_sdk::ruma::events::room::message::ReplacementMetadata;
        use matrix_sdk::ruma::{EventId, RoomId};

        let r_id = <&RoomId>::try_from(room_id)
            .map_err(|e| AppError::Matrix(format!("Invalid room ID '{room_id}': {e}")))?;
        let target_id = <&EventId>::try_from(target_event_id).map_err(|e| {
            AppError::Matrix(format!("Invalid target event ID '{target_event_id}': {e}"))
        })?;

        let content = build_notice_content(new_body, None)
            .make_replacement(ReplacementMetadata::new(target_id.to_owned(), None));

        if let Some(room) = self.client.get_room(r_id) {
            let resp = room
                .send(content)
                .await
                .map_err(|e| AppError::Matrix(format!("Failed to send edit notice: {e}")))?;
            return Ok(resp.response.event_id.to_string());
        }

        let is_configured = self.configured_rooms.lock().unwrap().contains(room_id);
        if is_configured {
            if let Ok(room) = self.client.join_room_by_id(r_id).await {
                let resp = room
                    .send(content)
                    .await
                    .map_err(|e| AppError::Matrix(format!("Failed to send edit notice: {e}")))?;
                return Ok(resp.response.event_id.to_string());
            }
        }

        use matrix_sdk::ruma::api::client::message::send_message_event::v3::Request as SendMessageEventRequest;
        use matrix_sdk::ruma::events::MessageLikeEventType;
        use matrix_sdk::ruma::TransactionId;

        if let Ok(raw_content) = serde_json::to_string(&content) {
            if let Ok(raw_val) = matrix_sdk::ruma::serde::Raw::from_json_string(raw_content) {
                let req = SendMessageEventRequest::new_raw(
                    r_id.to_owned(),
                    TransactionId::new(),
                    MessageLikeEventType::RoomMessage,
                    raw_val,
                );
                if let Ok(resp) = self.client.send(req).await {
                    return Ok(resp.event_id.to_string());
                }
            }
        }

        Err(AppError::Matrix(format!(
            "Room not found in client state: {room_id}"
        )))
    }

    async fn fetch_event(
        &self,
        room_id: &str,
        event_id: &str,
    ) -> Result<Option<EventMessage>, AppError> {
        use matrix_sdk::ruma::{EventId, RoomId};

        let r_id = <&RoomId>::try_from(room_id)
            .map_err(|e| AppError::Matrix(format!("Invalid room ID '{room_id}': {e}")))?;
        let e_id = <&EventId>::try_from(event_id)
            .map_err(|e| AppError::Matrix(format!("Invalid event ID '{event_id}': {e}")))?;

        let room = match self.client.get_room(r_id) {
            Some(r) => r,
            None => return Ok(None),
        };

        match room.event(e_id, None).await {
            Ok(timeline_event) => {
                if let Ok(sync_event) = timeline_event.raw().deserialize() {
                    Ok(parse_timeline_event(sync_event))
                } else {
                    Ok(None)
                }
            }
            Err(e) => {
                tracing::debug!(
                    "Failed to fetch event {} in room {}: {}",
                    event_id,
                    room_id,
                    e
                );
                Ok(None)
            }
        }
    }

    async fn fetch_thread_relations_page(
        &self,
        room_id: &str,
        thread_root_id: &str,
        from_token: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<EventMessage>, Option<String>), AppError> {
        use matrix_sdk::room::{IncludeRelations, RelationsOptions};
        use matrix_sdk::ruma::api::Direction;
        use matrix_sdk::ruma::events::relation::RelationType;
        use matrix_sdk::ruma::{EventId, RoomId, UInt};

        let r_id = <&RoomId>::try_from(room_id)
            .map_err(|e| AppError::Matrix(format!("Invalid room ID '{room_id}': {e}")))?;
        let e_id = <&EventId>::try_from(thread_root_id).map_err(|e| {
            AppError::Matrix(format!("Invalid thread root ID '{thread_root_id}': {e}"))
        })?;

        let room = match self.client.get_room(r_id) {
            Some(r) => r,
            None => return Ok((Vec::new(), None)),
        };

        let opts = RelationsOptions {
            from: from_token.map(|s| s.to_string()),
            dir: Direction::Forward,
            limit: Some(UInt::new(limit as u64).unwrap_or(UInt::MAX)),
            include_relations: IncludeRelations::RelationsOfType(RelationType::Thread),
            recurse: false,
        };

        let relations = room
            .relations(e_id.to_owned(), opts)
            .await
            .map_err(|e| AppError::Matrix(format!("Failed to fetch thread relations page: {e}")))?;

        let mut events = Vec::new();
        for timeline_event in relations.chunk {
            if let Ok(sync_event) = timeline_event.raw().deserialize() {
                if let Some(msg) = parse_timeline_event(sync_event) {
                    events.push(msg);
                }
            }
        }

        let next_batch = relations.next_batch_token.or(relations.prev_batch_token);
        Ok((events, next_batch))
    }

    async fn fetch_thread_history(
        &self,
        room_id: &str,
        thread_root_id: &str,
    ) -> Result<Vec<EventMessage>, AppError> {
        fetch_paged_thread_history(self, room_id, thread_root_id).await
    }

    fn set_store(&self, store: std::sync::Arc<crate::store::Store>) {
        self.set_store(store);
    }

    async fn resolve_rooms(&self, rooms: &[String]) -> Result<(), AppError> {
        self.resolve_rooms(rooms).await
    }
}

/// Paged thread fetch helper: fetches root event and queries thread relations pages
/// until all pages are retrieved.
pub async fn fetch_paged_thread_history(
    matrix: &dyn MatrixClient,
    room_id: &str,
    thread_root_id: &str,
) -> Result<Vec<EventMessage>, AppError> {
    let mut thread_events = Vec::new();

    // 1. Fetch root event
    if let Some(root_msg) = matrix.fetch_event(room_id, thread_root_id).await? {
        thread_events.push(root_msg);
    }

    // 2. Page through relations
    let mut from_token: Option<String> = None;
    let page_limit = 20;

    loop {
        let (page, next_token) = matrix
            .fetch_thread_relations_page(room_id, thread_root_id, from_token.as_deref(), page_limit)
            .await?;

        thread_events.extend(page);

        match next_token {
            Some(token) if from_token.as_deref() != Some(&token) => {
                from_token = Some(token);
            }
            _ => break,
        }
    }

    Ok(thread_events)
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
    pub mentions: Option<Vec<String>>,
    pub formatted_body: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SentDm {
    pub user_id: String,
    pub body: String,
}

#[derive(Debug, Clone)]
pub struct SentReaction {
    pub room_id: String,
    pub event_id: String,
    pub key: String,
}

#[derive(Debug, Clone)]
pub struct RedactedEvent {
    pub room_id: String,
    pub event_id: String,
    pub reason: Option<String>,
}

#[derive(Debug, Clone)]
pub struct EditedNotice {
    pub room_id: String,
    pub target_event_id: String,
    pub new_body: String,
}

#[derive(Default)]
pub struct MockMatrixClient {
    pub sent_notices: Mutex<Vec<SentNotice>>,
    pub sent_dms: Mutex<Vec<SentDm>>,
    pub sent_reactions: Mutex<Vec<SentReaction>>,
    pub redacted_events: Mutex<Vec<RedactedEvent>>,
    pub edited_notices: Mutex<Vec<EditedNotice>>,
    pub canned_history: Mutex<Vec<EventMessage>>,
    pub canned_events: Mutex<std::collections::HashMap<String, EventMessage>>,
    pub canned_thread_relations: Mutex<std::collections::HashMap<String, Vec<EventMessage>>>,
    pub canned_thread_page_size: Mutex<Option<usize>>,
    pub known_rooms: Mutex<std::collections::HashSet<String>>,
    pub configured_rooms: Mutex<std::collections::HashSet<String>>,
    pub joined_rooms: Mutex<Vec<String>>,
    pub require_known_rooms: std::sync::atomic::AtomicBool,
    pub fail_send_notice: std::sync::atomic::AtomicBool,
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
        if self
            .fail_send_notice
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(AppError::Matrix(
                "Send notice failed: network error".to_string(),
            ));
        }

        if self
            .require_known_rooms
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            let known = self.known_rooms.lock().unwrap();
            if !known.contains(room_id) {
                return Err(AppError::Matrix(format!(
                    "Room not found in client state: {room_id}"
                )));
            }
        }

        let (processed_body, extracted_user_ids) = parse_mentions_and_render_pills(body);
        let mut final_mentions = Vec::new();
        if let Some(user) = mention_user {
            final_mentions.push(user.to_string());
        }
        for uid in extracted_user_ids {
            let s = uid.to_string();
            if !final_mentions.contains(&s) {
                final_mentions.push(s);
            }
        }

        let mention_user_record = mention_user
            .map(|s| s.to_string())
            .or_else(|| final_mentions.first().cloned());

        let notice_content =
            matrix_sdk::ruma::events::room::message::RoomMessageEventContent::notice_markdown(
                &processed_body,
            );
        let formatted = match &notice_content.msgtype {
            matrix_sdk::ruma::events::room::message::MessageType::Notice(n) => {
                n.formatted.as_ref().map(|f| f.body.clone())
            }
            _ => None,
        };

        let mut list = self.sent_notices.lock().unwrap();
        list.push(SentNotice {
            room_id: room_id.to_string(),
            thread_root_id: thread_root_id.map(|s| s.to_string()),
            body: body.to_string(),
            mention_user: mention_user_record,
            mentions: if final_mentions.is_empty() {
                None
            } else {
                Some(final_mentions)
            },
            formatted_body: formatted,
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

    async fn send_reaction(
        &self,
        room_id: &str,
        event_id: &str,
        key: &str,
    ) -> Result<String, AppError> {
        let mut list = self.sent_reactions.lock().unwrap();
        list.push(SentReaction {
            room_id: room_id.to_string(),
            event_id: event_id.to_string(),
            key: key.to_string(),
        });
        Ok(format!("$mock_reaction_{}", list.len()))
    }

    async fn redact_event(
        &self,
        room_id: &str,
        event_id: &str,
        reason: Option<&str>,
    ) -> Result<(), AppError> {
        let mut list = self.redacted_events.lock().unwrap();
        list.push(RedactedEvent {
            room_id: room_id.to_string(),
            event_id: event_id.to_string(),
            reason: reason.map(|s| s.to_string()),
        });
        Ok(())
    }

    async fn edit_notice(
        &self,
        room_id: &str,
        target_event_id: &str,
        new_body: &str,
    ) -> Result<String, AppError> {
        let mut list = self.edited_notices.lock().unwrap();
        list.push(EditedNotice {
            room_id: room_id.to_string(),
            target_event_id: target_event_id.to_string(),
            new_body: new_body.to_string(),
        });
        Ok(format!("$mock_edit_{}", list.len()))
    }

    async fn fetch_history(
        &self,
        _room_id: &str,
        _limit: usize,
    ) -> Result<Vec<EventMessage>, AppError> {
        let list = self.canned_history.lock().unwrap();
        Ok(list.clone())
    }

    async fn fetch_event(
        &self,
        _room_id: &str,
        event_id: &str,
    ) -> Result<Option<EventMessage>, AppError> {
        let history = self.canned_history.lock().unwrap();
        if let Some(msg) = history.iter().find(|m| m.event_id == event_id) {
            return Ok(Some(msg.clone()));
        }
        let events = self.canned_events.lock().unwrap();
        Ok(events.get(event_id).cloned())
    }

    async fn fetch_thread_relations_page(
        &self,
        _room_id: &str,
        thread_root_id: &str,
        from_token: Option<&str>,
        default_limit: usize,
    ) -> Result<(Vec<EventMessage>, Option<String>), AppError> {
        let relations = self.canned_thread_relations.lock().unwrap();
        let list = match relations.get(thread_root_id) {
            Some(l) => l,
            None => return Ok((Vec::new(), None)),
        };

        let page_size = self
            .canned_thread_page_size
            .lock()
            .unwrap()
            .unwrap_or(default_limit);
        let start: usize = from_token.and_then(|t| t.parse().ok()).unwrap_or(0);
        if start >= list.len() {
            return Ok((Vec::new(), None));
        }

        let end = (start + page_size).min(list.len());
        let page = list[start..end].to_vec();
        let next_token = if end < list.len() {
            Some(end.to_string())
        } else {
            None
        };

        Ok((page, next_token))
    }

    async fn fetch_thread_history(
        &self,
        room_id: &str,
        thread_root_id: &str,
    ) -> Result<Vec<EventMessage>, AppError> {
        fetch_paged_thread_history(self, room_id, thread_root_id).await
    }

    async fn resolve_rooms(&self, rooms: &[String]) -> Result<(), AppError> {
        let mut known = self.known_rooms.lock().unwrap();
        let mut configured = self.configured_rooms.lock().unwrap();
        for r in rooms {
            known.insert(r.clone());
            configured.insert(r.clone());
        }
        Ok(())
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

    #[test]
    fn test_parse_mentions_and_pills() {
        use super::*;

        // 1. Markdown link [vic](https://matrix.to/#/@vborja:matrix.org)
        let (p1, u1) =
            parse_mentions_and_render_pills("Hello [vic](https://matrix.to/#/@vborja:matrix.org)!");
        assert_eq!(p1, "Hello [vic](https://matrix.to/#/@vborja:matrix.org)!");
        assert_eq!(u1.len(), 1);
        assert_eq!(u1[0].as_str(), "@vborja:matrix.org");

        // 2. Bare URL
        let (p2, u2) =
            parse_mentions_and_render_pills("Ask https://matrix.to/#/@vborja:matrix.org for help");
        assert_eq!(
            p2,
            "Ask [@vborja:matrix.org](https://matrix.to/#/@vborja:matrix.org) for help"
        );
        assert_eq!(u2.len(), 1);
        assert_eq!(u2[0].as_str(), "@vborja:matrix.org");

        // 3. Autolink with angle brackets <https://...>
        let (p3, u3) =
            parse_mentions_and_render_pills("See <https://matrix.to/#/@vborja:matrix.org>");
        assert_eq!(
            p3,
            "See [@vborja:matrix.org](https://matrix.to/#/@vborja:matrix.org)"
        );
        assert_eq!(u3.len(), 1);
        assert_eq!(u3[0].as_str(), "@vborja:matrix.org");

        // 4. Deduplication
        let (_p4, u4) = parse_mentions_and_render_pills(
            "[vic](https://matrix.to/#/@vborja:matrix.org) and [vic again](https://matrix.to/#/@vborja:matrix.org)"
        );
        assert_eq!(u4.len(), 1);
        assert_eq!(u4[0].as_str(), "@vborja:matrix.org");

        // 5. Invalid MXID
        let (_p5, u5) = parse_mentions_and_render_pills("[bad](https://matrix.to/#/@invalid)");
        assert!(u5.is_empty());

        // 6. Plain text without links
        let (p6, u6) =
            parse_mentions_and_render_pills("Just regular text with @vborja:matrix.org plain name");
        assert_eq!(p6, "Just regular text with @vborja:matrix.org plain name");
        assert!(u6.is_empty());
    }

    #[test]
    fn test_build_notice_content_mentions() {
        use super::*;

        // With mention
        let c1 = build_notice_content("Hello [vic](https://matrix.to/#/@vborja:matrix.org)", None);
        let val1 = serde_json::to_value(&c1).unwrap();
        assert_eq!(val1["m.mentions"]["user_ids"][0], "@vborja:matrix.org");
        assert!(val1["formatted_body"]
            .as_str()
            .unwrap()
            .contains("<a href=\"https://matrix.to/#/@vborja:matrix.org\">vic</a>"));
        assert!(val1["m.mentions"]["room"].is_null() || val1["m.mentions"]["room"] == false);

        // Without mention
        let c2 = build_notice_content("Just a plain answer", None);
        let val2 = serde_json::to_value(&c2).unwrap();
        assert!(val2["m.mentions"]["user_ids"].is_null()); // empty object {}
        assert_eq!(val2["m.mentions"], serde_json::json!({}));

        // With @room
        let c3 = build_notice_content("Attention @room!", None);
        let val3 = serde_json::to_value(&c3).unwrap();
        assert!(val3["m.mentions"]["room"].is_null() || val3["m.mentions"]["room"] == false);
    }
}
