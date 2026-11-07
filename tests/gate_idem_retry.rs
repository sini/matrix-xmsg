//! A forward send that xmsg accepts after `FORWARD_SEND_TIMEOUT` is retried once under the same
//! idempotency key, recorded under the first send's message ID, and its reply is posted once.
//!
//! The mock xmsg keeps xmsg's keyed semantics (README, Idempotency): one key is one message,
//! a repeat of a key answers the first send's ID without delivering, and a repeat with another
//! body is a conflict. Its first send is delivered at once but answers only after the bound,
//! as an xmsg whose delivery outlives its caller's timeout does. Time is paused, so the bound
//! elapses without waiting for it.

use async_trait::async_trait;
use matrix_xmsg::bot::{
    handle_inbox_delivery_with_xmsg, handle_incoming_event_with_guard, BotOutcome,
    IncomingMatrixEvent,
};
use matrix_xmsg::config::{Admission, Config};
use matrix_xmsg::error::AppError;
use matrix_xmsg::matrix::MockMatrixClient;
use matrix_xmsg::store::Store;
use matrix_xmsg::xmsg::{MockGuardClient, SendResponse, SvcDelivery, SvcInbox, XmsgClient};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

const ROOM: &str = "!support:example.org";
const REPLY: &str = "late answer from expert";
const EVENT_MS: i64 = 1_000_000;

fn config() -> Config {
    Config {
        homeserver_url: "https://matrix.example.org".to_string(),
        bot_mxid: "@genie:example.org".to_string(),
        access_token_file: PathBuf::from("/nonexistent/token"),
        rooms: vec![ROOM.to_string()],
        trusted_mxids: vec!["@alice:example.org".to_string()],
        owner_mxid: "@owner:example.org".to_string(),
        admission: Admission::Trusted,
        xmsg_socket: PathBuf::from("/nonexistent/xmsg"),
        expert_ref: "claude".to_string(),
        expert_refs: None,
        room_routes: std::collections::BTreeMap::new(),
        open_thread_nodes: vec![],
        history_n: 5,
        history_byte_cap: 1024,
        rate_limit_count: 10,
        rate_limit_window_secs: 60,
        size_cap_bytes: 1024,
        answer_timeout_secs: 10,
        session_live_secs: 3600,
        resync_byte_cap: 65536,
        resync_page_bytes: 8192,
        context_max_lines: 20,
        context_max_bytes: 8192,
        sender_window_secs: 900,
        context_rate_limit_count: 6,
        context_rate_limit_window_secs: 600,
        guard_ref: "genie-guard".to_string(),
        guard_timeout_secs: 30,
        guard_retry_budget: 3,
        screen_mentions: false,
        screen_solicited: true,
        guard_failure_mode: matrix_xmsg::config::GuardFailureMode::Closed,
        guard_outage_dm_window_secs: 3600,
        inbox_retry_budget: 10,
        store_passphrase_file: None,
        service_name: "matrix-xmsg".to_string(),
        undecryptable_wait_secs: 10,
        db_path: PathBuf::from(":memory:"),
    }
}

/// An xmsg whose send number `late` (0-based) is delivered at once and answers after the bound.
struct LateAccept {
    late: usize,
    calls: Mutex<usize>,
    /// key => (message ID, body)
    keys: Mutex<HashMap<String, (String, String)>>,
    /// Message IDs delivered, in order.
    delivered: Mutex<Vec<String>>,
}

impl LateAccept {
    fn new(late: usize) -> Self {
        Self {
            late,
            calls: Mutex::new(0),
            keys: Mutex::new(HashMap::new()),
            delivered: Mutex::new(Vec::new()),
        }
    }

    fn delivered(&self) -> Vec<String> {
        self.delivered.lock().unwrap().clone()
    }

    async fn send(&self, text: &str, key: Option<&str>) -> Result<SendResponse, AppError> {
        let call = {
            let mut c = self.calls.lock().unwrap();
            *c += 1;
            *c - 1
        };
        if let Some(k) = key {
            if let Some((id, body)) = self.keys.lock().unwrap().get(k) {
                if body != text {
                    return Err(AppError::Xmsg(format!("conflict on key {k}")));
                }
                // A repeat while the first is in flight: `pending`, address "".
                return Ok(SendResponse::new(id.clone(), None));
            }
        }
        let id = format!("01LATE{call:010}");
        if let Some(k) = key {
            self.keys
                .lock()
                .unwrap()
                .insert(k.to_string(), (id.clone(), text.to_string()));
        }
        self.delivered.lock().unwrap().push(id.clone());
        if call == self.late {
            tokio::time::sleep(Duration::from_secs(600)).await;
        }
        Ok(SendResponse::new(id, Some("sess".to_string())))
    }
}

#[async_trait]
impl XmsgClient for LateAccept {
    async fn send_message(&self, _: &str, _: &str, text: &str) -> Result<SendResponse, AppError> {
        self.send(text, None).await
    }
    async fn send_message_keyed(
        &self,
        _: &str,
        _: &str,
        text: &str,
        key: &str,
    ) -> Result<SendResponse, AppError> {
        self.send(text, Some(key)).await
    }
    async fn reply_message(&self, _: &str, text: &str) -> Result<ReplyOutcome, AppError> {
        async { self.send(text, None).await }
            .await
            .map(replies::pushed)
    }
    async fn reply_message_keyed(
        &self,
        _: &str,
        text: &str,
        key: &str,
    ) -> Result<ReplyOutcome, AppError> {
        async { self.send(text, Some(key)).await }
            .await
            .map(replies::pushed)
    }
}

#[derive(Default)]
struct AckInbox;

#[async_trait]
impl SvcInbox for AckInbox {
    async fn poll(&mut self, _wait_secs: u64) -> Result<Option<SvcDelivery>, AppError> {
        Ok(None)
    }
    async fn ack(&mut self, _message_id: &str) -> Result<(), AppError> {
        Ok(())
    }
}

/// The reply path's xmsg, which a plain expert reply never calls.
struct NoXmsg;

#[async_trait]
impl XmsgClient for NoXmsg {
    async fn send_message(&self, _: &str, _: &str, _: &str) -> Result<SendResponse, AppError> {
        Err(AppError::Xmsg("unexpected send".to_string()))
    }
}

fn mention(root: &str) -> IncomingMatrixEvent {
    IncomingMatrixEvent {
        room_id: ROOM.to_string(),
        event_id: "$ev_late".to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        body: "@genie ping".to_string(),
        formatted_body: None,
        mentions: Some(vec!["@genie:example.org".to_string()]),
        timestamp_ms: EVENT_MS,
        thread_root_id: Some(root.to_string()),
        replaces_event_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
    }
}

/// Forwards one mention in `root` through `xmsg`, then answers delivered message `answered`
/// and returns how often the answer was posted to the thread.
async fn forward_then_answer(
    root: &str,
    store: &Store,
    xmsg: &LateAccept,
    answered: usize,
) -> usize {
    let config = config();
    let matrix = MockMatrixClient::default();
    let outcome = handle_incoming_event_with_guard(
        &mention(root),
        &[],
        &config,
        &matrix,
        xmsg,
        &MockGuardClient::new(),
        store,
        None,
    )
    .await
    .expect("the forward survives a send answered after the bound");
    assert_eq!(outcome, BotOutcome::Forwarded);

    let delivered = xmsg.delivered();
    let mut reply = SvcDelivery::new(
        "01LATEREPLY000001",
        "expert · sess@sini.test",
        REPLY,
        "",
        None,
    );
    reply.in_reply_to = Some(delivered[answered].clone());
    let outcome = handle_inbox_delivery_with_xmsg(
        &reply,
        &config,
        &matrix,
        store,
        &NoXmsg,
        &mut AckInbox,
        EVENT_MS / 1000 + 5,
    )
    .await
    .expect("reply routing");
    assert_eq!(
        outcome,
        BotOutcome::Replied,
        "the late send's reply was not posted"
    );
    let notices = matrix.sent_notices.lock().unwrap();
    notices
        .iter()
        .filter(|n| n.thread_root_id.as_deref() == Some(root) && n.body.contains(REPLY))
        .count()
}

// The expert forward's send answers after the bound: the retry with its key returns the first
// send's ID, one message is delivered, and the reply to it is posted once.
// Mutants: no retry => RED (the forward fails); a fresh key on the retry => RED (two
// deliveries).
#[tokio::test(start_paused = true)]
async fn a_late_expert_forward_is_retried_under_its_key() {
    let store = Store::new_in_memory().unwrap();
    let xmsg = LateAccept::new(0);
    let posted = forward_then_answer("$root_fwd", &store, &xmsg, 0).await;
    assert_eq!(
        xmsg.delivered().len(),
        1,
        "the retry delivered a second message"
    );
    assert_eq!(posted, 1);
}

// The rebootstrap send answers after the bound: both sends are recorded and the reply to the
// rebootstrap is posted once. Mutant: rebootstrap keyed as the delta => RED (conflict).
#[tokio::test(start_paused = true)]
async fn a_late_rebootstrap_is_retried_under_its_own_key() {
    let store = Store::new_in_memory().unwrap();
    // A cursor with no session id: the next forward is a delta followed by a rebootstrap.
    store
        .set_thread_cursor("$root_boot", "$ev_old", EVENT_MS / 1000 - 10, None)
        .unwrap();
    let xmsg = LateAccept::new(1);
    let posted = forward_then_answer("$root_boot", &store, &xmsg, 1).await;
    let delivered = xmsg.delivered();
    assert_eq!(delivered.len(), 2, "a delta and one rebootstrap");
    assert!(store
        .get_forwarded_message(&delivered[0])
        .unwrap()
        .is_some());
    assert_eq!(posted, 1);
}

// The relay into a solicited thread answers after the bound: the retry with its key returns
// the first reply's ID, one reply is pushed, and the session's answer is posted once.
// Mutants: no retry => RED (the relay is recorded under a dead-session id and the answer is
// dropped); a fresh key on the retry => RED (two pushes).
#[tokio::test(start_paused = true)]
async fn a_late_relay_reply_is_retried_under_its_key() {
    let store = Store::new_in_memory().unwrap();
    store
        .bind_solicited_thread("$root_rel", ROOM, "01LATEREQUEST001", 900)
        .unwrap();
    let xmsg = LateAccept::new(0);
    let posted = forward_then_answer("$root_rel", &store, &xmsg, 0).await;
    assert_eq!(xmsg.delivered().len(), 1, "the retry pushed a second reply");
    assert_eq!(posted, 1);
}

struct StoredRelay;

#[async_trait]
impl XmsgClient for StoredRelay {
    async fn send_message(&self, _: &str, _: &str, _: &str) -> Result<SendResponse, AppError> {
        panic!("a solicited relay is a reply, not a new send");
    }

    async fn reply_message_keyed(
        &self,
        message_id: &str,
        text: &str,
        key: &str,
    ) -> Result<matrix_xmsg::xmsg::ReplyOutcome, AppError> {
        assert_eq!(message_id, "01STOREDREQUEST");
        assert!(!key.is_empty());
        Ok(matrix_xmsg::xmsg::ReplyOutcome::Stored {
            record: xmsg::client::ReplyRecord {
                seq: 7,
                message_id: message_id.into(),
                created_at: 1,
                replier_session_id: "matrix-xmsg".into(),
                text: text.into(),
                push_outcome: Some("stored".into()),
                pushed_message_id: Some("01STOREDRELAY".into()),
            },
        })
    }
}

// Mutants: use the request ID as the new reply ID, or omit the Stored association.
#[tokio::test]
async fn a_reply_to_a_stored_relay_routes_to_its_matrix_thread() {
    use matrix_xmsg::store::ForwardedMessageRecord;
    let store = Store::new_in_memory().unwrap();
    store
        .bind_solicited_thread("$stored-root", ROOM, "01STOREDREQUEST", 900)
        .unwrap();
    store
        .record_forwarded_message(&ForwardedMessageRecord {
            message_id: "01STOREDREQUEST".into(),
            room_id: ROOM.into(),
            thread_root_id: "$stored-root".into(),
            event_id: "$original".into(),
            sender_mxid: "@alice:example.org".into(),
            addressed: true,
            sent_at: 900,
            ack_reaction_id: None,
            timeout_dm_sent: false,
            reply_count: 0,
        })
        .unwrap();
    store
        .set_forward_session("01STOREDREQUEST", Some("expert@fixture"))
        .unwrap();
    let config = config();
    let matrix = MockMatrixClient::default();
    let event = mention("$stored-root");
    let outcome = handle_incoming_event_with_guard(
        &event,
        &[],
        &config,
        &matrix,
        &StoredRelay,
        &MockGuardClient::new(),
        &store,
        None,
    )
    .await
    .unwrap();
    assert_eq!(outcome, BotOutcome::Forwarded);
    let cursor = store.get_thread_cursor("$stored-root").unwrap().unwrap();
    assert_eq!(cursor.session_id.as_deref(), Some("expert@fixture"));
    assert_eq!(
        store
            .get_forwarded_message("01STOREDRELAY")
            .unwrap()
            .unwrap()
            .thread_root_id,
        "$stored-root"
    );
    assert_eq!(
        store
            .get_forwarded_message("01STOREDREQUEST")
            .unwrap()
            .unwrap()
            .event_id,
        "$original"
    );
    let mut answer = SvcDelivery::new(
        "01FOLLOWUP",
        "expert@fixture",
        REPLY,
        "fixture reply",
        Some(serde_json::json!({"service":"expert","node":"fixture"})),
    );
    answer.in_reply_to = Some("01STOREDRELAY".into());
    handle_inbox_delivery_with_xmsg(
        &answer,
        &config,
        &matrix,
        &store,
        &NoXmsg,
        &mut AckInbox,
        1001,
    )
    .await
    .unwrap();
    assert_eq!(
        matrix
            .sent_notices
            .lock()
            .unwrap()
            .iter()
            .filter(
                |m| m.thread_root_id.as_deref() == Some("$stored-root") && m.body.contains(REPLY)
            )
            .count(),
        1
    );
}

/// An `agent.sock` that answers each frame with `answer` and records the frame.
async fn agent_sock_answering(
    dir: &std::path::Path,
    answer: serde_json::Value,
) -> std::sync::Arc<Mutex<Vec<serde_json::Value>>> {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
    let listener = tokio::net::UnixListener::bind(dir.join("agent.sock")).unwrap();
    let frames = std::sync::Arc::new(Mutex::new(Vec::new()));
    let seen = frames.clone();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let (r, mut w) = stream.into_split();
            let mut lines = tokio::io::BufReader::new(r).lines();
            if let Ok(Some(line)) = lines.next_line().await {
                let frame: serde_json::Value = serde_json::from_str(&line).unwrap();
                let response = &answer[frame["action"].as_str().unwrap()];
                seen.lock().unwrap().push(frame);
                let _ = w.write_all(format!("{response}\n").as_bytes()).await;
            }
        }
    });
    frames
}

// On the wire: a keyed send and a keyed reply carry `idempotency_key`, and a `pending` repeat
// (address "") is read as the first send's ID with no session. Mutants: drop the key from
// either frame => RED; read the empty address as a session => RED.
#[tokio::test]
async fn keyed_frames_carry_the_key_and_read_pending() {
    let tmp = tempfile::tempdir().unwrap();
    let frames = agent_sock_answering(
        tmp.path(),
        serde_json::json!({
            "send": {"status":"ok", "message_id":"01FIRST", "address":"", "outcome":"pending"},
            "reply": {"status":"ok", "reply": {"seq":0,"messageId":"01REQ","createdAt":1,
                "replierSessionId":"matrix-xmsg","text":"hi","pushOutcome":"pending","pushedMessageId":"01FIRSTREPLY"}},
        }),
    )
    .await;
    let registration = transport_fixture::register_for_agent(tmp.path().join("agent.sock")).await;
    let client = registration.client();

    let sent = client
        .send_message_keyed("claude", "matrix_alice", "hi", "k-send")
        .await
        .unwrap();
    assert_eq!(sent.message_id, "01FIRST");
    assert_eq!(sent.session_id, None);
    assert_eq!(sent.outcome.as_deref(), Some("pending"));
    let replied = client
        .reply_message_keyed("01REQ", "hi", "k-reply")
        .await
        .unwrap();
    match replied {
        ReplyOutcome::Pushed { response, record } => {
            assert_eq!(response.message_id, "01FIRSTREPLY");
            assert_eq!(record.push_outcome.as_deref(), Some("pending"));
        }
        ReplyOutcome::Stored { .. } => panic!("pending receipt carries its reserved identity"),
    }

    let frames = frames.lock().unwrap().clone();
    assert_eq!(
        frames,
        vec![
            serde_json::json!({"action": "send", "schema_version":2, "to": "claude", "text": "hi", "idempotency_key": "k-send"}),
            serde_json::json!({"action": "reply", "schema_version":2, "message_id": "01REQ", "text": "hi", "idempotency_key": "k-reply"}),
        ]
    );
}

const ROOM_OTHER: &str = "!other:example.org";

struct TransientExpertXmsg {
    /// For ROOM ("claude"): list of scripted outcomes per call; once exhausted, uses `default_outcome`.
    scripted: Mutex<std::collections::VecDeque<Result<SendResponse, String>>>,
    default_refusal_code: Mutex<Option<String>>,
    attempts: Mutex<Vec<(String, Option<String>)>>,
    keys: Mutex<HashMap<String, (String, String)>>,
    delivered: Mutex<Vec<(String, String)>>,
}

impl TransientExpertXmsg {
    fn new_refusing(code: &str) -> Self {
        Self {
            scripted: Mutex::new(std::collections::VecDeque::new()),
            default_refusal_code: Mutex::new(Some(code.to_string())),
            attempts: Mutex::new(Vec::new()),
            keys: Mutex::new(HashMap::new()),
            delivered: Mutex::new(Vec::new()),
        }
    }

    fn register_expert(&self) {
        *self.default_refusal_code.lock().unwrap() = None;
    }

    fn set_refusal_code(&self, code: &str) {
        *self.default_refusal_code.lock().unwrap() = Some(code.to_string());
    }

    fn attempts_for(&self, expert_ref: &str) -> usize {
        self.attempts
            .lock()
            .unwrap()
            .iter()
            .filter(|(r, _)| r == expert_ref)
            .count()
    }

    fn delivered_for(&self, expert_ref: &str) -> Vec<String> {
        self.delivered
            .lock()
            .unwrap()
            .iter()
            .filter(|(r, _)| r == expert_ref)
            .map(|(_, id)| id.clone())
            .collect()
    }
}

#[async_trait]
impl XmsgClient for TransientExpertXmsg {
    async fn send_message(
        &self,
        expert_ref: &str,
        _from: &str,
        text: &str,
    ) -> Result<SendResponse, AppError> {
        self.send_message_keyed(expert_ref, _from, text, "").await
    }

    async fn send_message_keyed(
        &self,
        expert_ref: &str,
        _from: &str,
        text: &str,
        key: &str,
    ) -> Result<SendResponse, AppError> {
        self.attempts.lock().unwrap().push((
            expert_ref.to_string(),
            (!key.is_empty()).then(|| key.to_string()),
        ));

        if expert_ref == "other-expert" {
            let id = format!("01OTHER{:06}", self.delivered.lock().unwrap().len() + 1);
            self.delivered
                .lock()
                .unwrap()
                .push((expert_ref.to_string(), id.clone()));
            return Ok(SendResponse::new(
                id,
                Some("other-expert@sini.test".to_string()),
            ));
        }

        if let Some(step) = self.scripted.lock().unwrap().pop_front() {
            return step.map_err(|code| {
                AppError::XmsgTransport(::xmsg::client::ClientError::Refused { code })
            });
        }

        if let Some(code) = self.default_refusal_code.lock().unwrap().clone() {
            return Err(AppError::XmsgTransport(
                ::xmsg::client::ClientError::Refused { code },
            ));
        }

        if !key.is_empty() {
            if let Some((existing_id, existing_body)) = self.keys.lock().unwrap().get(key).cloned()
            {
                if existing_body != text {
                    return Err(AppError::Xmsg(format!("conflict on key {key}")));
                }
                return Ok(SendResponse::new(
                    existing_id,
                    Some("claude@sini.test".to_string()),
                ));
            }
        }

        let id = format!("01RETRY{:06}", self.delivered.lock().unwrap().len() + 1);
        if !key.is_empty() {
            self.keys
                .lock()
                .unwrap()
                .insert(key.to_string(), (id.clone(), text.to_string()));
        }
        self.delivered
            .lock()
            .unwrap()
            .push((expert_ref.to_string(), id.clone()));
        Ok(SendResponse::new(id, Some("claude@sini.test".to_string())))
    }
}

// XM4 Cell 1 (D-1, D-2): Expert absent at mention time (`not_found`, then `unavailable`) and
// registering within the 10-minute window => `handle_incoming_event_with_guard` returns
// `BotOutcome::Pending` immediately without blocking another room's mention, and the background
// retry worker delivers the message once with zero notices.
// Mutant `transient-not-found`: classify `not_found` as permanent => RED.
#[tokio::test(start_paused = true)]
async fn transient_expert_absence_retries_without_blocking_other_rooms_and_delivers_once() {
    use matrix_xmsg::bot::run_forward_retry_worker;
    use matrix_xmsg::config::RoomRoute;
    use std::sync::Arc;

    let mut cfg = config();
    cfg.rooms.push(ROOM_OTHER.to_string());
    cfg.room_routes.insert(
        ROOM_OTHER.to_string(),
        RoomRoute {
            expert_ref: Some("other-expert".to_string()),
            expert_refs: None,
            screen_mentions: Some(false),
            screen_solicited: Some(false),
        },
    );
    let cfg = Arc::new(cfg);
    let store = Arc::new(Store::new_in_memory().unwrap());
    let matrix = Arc::new(MockMatrixClient::default());
    let xmsg = Arc::new(TransientExpertXmsg::new_refusing("not_found"));

    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);
    let worker = tokio::spawn(run_forward_retry_worker(
        cfg.clone(),
        matrix.clone(),
        xmsg.clone(),
        store.clone(),
        shutdown_rx,
    ));

    // 1. Mention in ROOM while expert is absent (`not_found`) returns `Pending` immediately.
    let ev_room1 = IncomingMatrixEvent {
        room_id: ROOM.to_string(),
        event_id: "$ev_xm4_transient".to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        body: "@genie help with transient retry".to_string(),
        formatted_body: None,
        mentions: Some(vec!["@genie:example.org".to_string()]),
        timestamp_ms: EVENT_MS,
        thread_root_id: Some("$root_xm4_1".to_string()),
        replaces_event_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
    };
    let outcome1 = handle_incoming_event_with_guard(
        &ev_room1,
        &[],
        &cfg,
        matrix.as_ref(),
        xmsg.as_ref(),
        &MockGuardClient::new(),
        &store,
        None,
    )
    .await
    .unwrap();
    assert_eq!(outcome1, BotOutcome::Pending);
    assert_eq!(xmsg.attempts_for("claude"), 1);
    assert!(xmsg.delivered_for("claude").is_empty());
    assert!(matrix.sent_notices.lock().unwrap().is_empty());

    // 2. D-1: While ROOM's retry is pending, an event in ROOM_OTHER is handled immediately.
    let ev_other = IncomingMatrixEvent {
        room_id: ROOM_OTHER.to_string(),
        event_id: "$ev_xm4_other_room".to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        body: "@genie other room question".to_string(),
        formatted_body: None,
        mentions: Some(vec!["@genie:example.org".to_string()]),
        timestamp_ms: EVENT_MS + 100,
        thread_root_id: Some("$root_other".to_string()),
        replaces_event_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
    };
    let outcome_other = handle_incoming_event_with_guard(
        &ev_other,
        &[],
        &cfg,
        matrix.as_ref(),
        xmsg.as_ref(),
        &MockGuardClient::new(),
        &store,
        None,
    )
    .await
    .unwrap();
    assert_eq!(outcome_other, BotOutcome::Forwarded);
    assert_eq!(xmsg.delivered_for("other-expert").len(), 1);

    // 3. Switch refusal to `unavailable` before the first 2s retry, then register expert before the 4s retry.
    xmsg.set_refusal_code("unavailable");
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        xmsg.attempts_for("claude"),
        2,
        "2s backoff retry must have fired"
    );
    assert!(xmsg.delivered_for("claude").is_empty());

    xmsg.register_expert();
    tokio::time::sleep(Duration::from_secs(4)).await;

    assert_eq!(
        xmsg.delivered_for("claude").len(),
        1,
        "expert must receive the forward exactly once after registering within the 10-minute window"
    );
    assert!(
        matrix.sent_notices.lock().unwrap().is_empty(),
        "zero failure notices when retry succeeds within the window"
    );
    assert!(store.is_event_relayed("$ev_xm4_transient").unwrap());
    assert!(store
        .get_forward_retry("$ev_xm4_transient")
        .unwrap()
        .is_none());

    let _ = shutdown_tx.send(());
    worker.await.unwrap().unwrap();
}

// XM4 Cell 2 (D-2, D-3, D-4): Expert absent past the 10-minute window => retries with 2s..60s
// backoff, then gives up at 600s and posts ONE user-friendly notice with no internal IDs/codes;
// permanent refusal (`ambiguous`) posts the notice straight away without retrying.
// Mutant `record-before-send` (D-3): record `record_event_relayed` before the send => RED (the
// give-up cell sees no retry).
#[tokio::test(start_paused = true)]
async fn expert_absent_past_window_posts_one_notice_and_permanent_refusal_does_not_retry() {
    use matrix_xmsg::bot::{run_forward_retry_worker, FORWARD_FAILURE_NOTICE};
    use std::sync::Arc;

    let cfg = Arc::new(config());
    let store = Arc::new(Store::new_in_memory().unwrap());
    let matrix = Arc::new(MockMatrixClient::default());
    let xmsg = Arc::new(TransientExpertXmsg::new_refusing("unavailable"));

    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);
    let worker = tokio::spawn(run_forward_retry_worker(
        cfg.clone(),
        matrix.clone(),
        xmsg.clone(),
        store.clone(),
        shutdown_rx,
    ));

    let ev_giveup = IncomingMatrixEvent {
        room_id: ROOM.to_string(),
        event_id: "$ev_xm4_giveup".to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        body: "@genie still absent".to_string(),
        formatted_body: None,
        mentions: Some(vec!["@genie:example.org".to_string()]),
        timestamp_ms: EVENT_MS,
        thread_root_id: Some("$root_xm4_giveup".to_string()),
        replaces_event_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
    };
    let outcome = handle_incoming_event_with_guard(
        &ev_giveup,
        &[],
        &cfg,
        matrix.as_ref(),
        xmsg.as_ref(),
        &MockGuardClient::new(),
        &store,
        None,
    )
    .await
    .unwrap();
    assert_eq!(outcome, BotOutcome::Pending);

    // Advance 590s (still within the 600s window): multiple backoff retries must have run, 0 notices yet.
    tokio::time::sleep(Duration::from_secs(590)).await;
    assert!(
        xmsg.attempts_for("claude") >= 10,
        "expected backoff retries across the 10-minute window, got {}",
        xmsg.attempts_for("claude")
    );
    assert!(
        matrix.sent_notices.lock().unwrap().is_empty(),
        "no notice may be posted before the 10-minute retry window expires"
    );

    // Advance past the 600s deadline: worker gives up and posts exactly one notice.
    tokio::time::sleep(Duration::from_secs(15)).await;
    let attempts_at_giveup = xmsg.attempts_for("claude");
    {
        let notices = matrix.sent_notices.lock().unwrap();
        assert_eq!(notices.len(), 1, "exactly one notice must be posted at give-up");
        assert_eq!(notices[0].thread_root_id.as_deref(), Some("$root_xm4_giveup"));
        assert_eq!(notices[0].body, FORWARD_FAILURE_NOTICE);
        assert_eq!(
            notices[0].mention_user.as_deref(),
            Some("@alice:example.org")
        );
        assert!(!notices[0].body.contains("unavailable"));
        assert!(!notices[0].body.contains("not_found"));
        assert!(!notices[0].body.contains("$ev_xm4_giveup"));
    }
    assert_eq!(
        matrix.sent_dms.lock().unwrap().len(),
        1,
        "owner DM must be sent once on give-up (D-4)"
    );
    assert!(xmsg.delivered_for("claude").is_empty());

    // Advance another 120s: no further retries or notices after give-up.
    tokio::time::sleep(Duration::from_secs(120)).await;
    assert_eq!(xmsg.attempts_for("claude"), attempts_at_giveup);
    assert_eq!(matrix.sent_notices.lock().unwrap().len(), 1);

    // Sub-cell B: Permanent refusal (`ambiguous`) posts the notice immediately with 0 retries.
    xmsg.set_refusal_code("ambiguous");
    let ev_perm = IncomingMatrixEvent {
        room_id: ROOM.to_string(),
        event_id: "$ev_xm4_perm".to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        body: "@genie ambiguous route".to_string(),
        formatted_body: None,
        mentions: Some(vec!["@genie:example.org".to_string()]),
        timestamp_ms: EVENT_MS + 800_000,
        thread_root_id: Some("$root_xm4_perm".to_string()),
        replaces_event_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
    };
    let perm_outcome = handle_incoming_event_with_guard(
        &ev_perm,
        &[],
        &cfg,
        matrix.as_ref(),
        xmsg.as_ref(),
        &MockGuardClient::new(),
        &store,
        None,
    )
    .await
    .unwrap();
    assert_eq!(perm_outcome, BotOutcome::ForwardFailed);
    assert_eq!(xmsg.attempts_for("claude"), attempts_at_giveup + 1);
    tokio::time::sleep(Duration::from_secs(30)).await;
    assert_eq!(
        xmsg.attempts_for("claude"),
        attempts_at_giveup + 1,
        "permanent refusal must never be retried"
    );
    assert_eq!(matrix.sent_notices.lock().unwrap().len(), 2);

    let _ = shutdown_tx.send(());
    worker.await.unwrap().unwrap();
}

// XM4 Cell 3: Redelivered event after give-up (or permanent refusal) produces no second notice
// and no second forward attempt.
#[tokio::test(start_paused = true)]
async fn redelivered_event_after_give_up_posts_no_second_notice() {
    use matrix_xmsg::bot::sweep_due_forward_retries;

    let cfg = config();
    let store = Store::new_in_memory().unwrap();
    let matrix = MockMatrixClient::default();
    let xmsg = TransientExpertXmsg::new_refusing("not_found");

    let ev = IncomingMatrixEvent {
        room_id: ROOM.to_string(),
        event_id: "$ev_xm4_redeliver".to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        body: "@genie redelivery check".to_string(),
        formatted_body: None,
        mentions: Some(vec!["@genie:example.org".to_string()]),
        timestamp_ms: EVENT_MS,
        thread_root_id: Some("$root_xm4_redeliver".to_string()),
        replaces_event_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
    };
    let now_secs = EVENT_MS / 1000;
    assert_eq!(
        handle_incoming_event_with_guard(
            &ev,
            &[],
            &cfg,
            &matrix,
            &xmsg,
            &MockGuardClient::new(),
            &store,
            None,
        )
        .await
        .unwrap(),
        BotOutcome::Pending
    );

    // Redelivery while retry is still pending returns Pending without a second initial attempt.
    assert_eq!(
        handle_incoming_event_with_guard(
            &ev,
            &[],
            &cfg,
            &matrix,
            &xmsg,
            &MockGuardClient::new(),
            &store,
            None,
        )
        .await
        .unwrap(),
        BotOutcome::Pending
    );
    assert_eq!(xmsg.attempts_for("claude"), 1);

    // Drive past the 600s retry window so the retry gives up and posts the single notice.
    sweep_due_forward_retries(&cfg, &matrix, &xmsg, &store, now_secs + 600)
        .await
        .unwrap();
    assert_eq!(matrix.sent_notices.lock().unwrap().len(), 1);
    let attempts_after_giveup = xmsg.attempts_for("claude");

    // Redeliver the event after give-up (even if expert is now registered): must be ignored with no second notice.
    xmsg.register_expert();
    let redeliver_outcome = handle_incoming_event_with_guard(
        &ev,
        &[],
        &cfg,
        &matrix,
        &xmsg,
        &MockGuardClient::new(),
        &store,
        None,
    )
    .await
    .unwrap();
    assert_eq!(redeliver_outcome, BotOutcome::IgnoredDuplicate);
    assert_eq!(
        matrix.sent_notices.lock().unwrap().len(),
        1,
        "redelivered event must not post a second notice"
    );
    assert_eq!(xmsg.attempts_for("claude"), attempts_after_giveup);
    assert!(xmsg.delivered_for("claude").is_empty());
}

// XM4 Cell 4 (D-3): Crash/disconnect between `xmsg` accepting the forward send and `store.record_event_relayed`
// => on restart, recovery replays the deterministic forward key (`matrix-xmsg:forward:<claim_id>`),
// `xmsg` deduplicates within its idempotency TTL, and the message is delivered once.
#[tokio::test(start_paused = true)]
async fn crash_between_send_and_record_event_relayed_delivers_once_on_restart() {
    use matrix_xmsg::bot::sweep_due_forward_retries;

    struct CrashWindowXmsg {
        crash_first_return: Mutex<bool>,
        keys: Mutex<HashMap<String, (String, String)>>,
        delivered: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl XmsgClient for CrashWindowXmsg {
        async fn send_message(
            &self,
            _: &str,
            _: &str,
            text: &str,
        ) -> Result<SendResponse, AppError> {
            self.send_message_keyed("claude", "", text, "").await
        }

        async fn send_message_keyed(
            &self,
            _: &str,
            _: &str,
            text: &str,
            key: &str,
        ) -> Result<SendResponse, AppError> {
            let existing = self.keys.lock().unwrap().get(key).cloned();
            let id = if let Some((existing_id, existing_body)) = existing {
                assert_eq!(existing_body, text, "deterministic key must carry identical body");
                existing_id
            } else {
                let fresh = format!("01CRASH{:06}", self.delivered.lock().unwrap().len() + 1);
                self.keys
                    .lock()
                    .unwrap()
                    .insert(key.to_string(), (fresh.clone(), text.to_string()));
                self.delivered.lock().unwrap().push(fresh.clone());
                fresh
            };

            let should_crash_after_accept = {
                let mut flag = self.crash_first_return.lock().unwrap();
                let cur = *flag;
                *flag = false;
                cur
            };
            if should_crash_after_accept {
                return Err(AppError::XmsgTransport(
                    ::xmsg::client::ClientError::Disconnected,
                ));
            }
            Ok(SendResponse::new(id, Some("claude@sini.test".to_string())))
        }
    }

    let cfg = config();
    let store = Store::new_in_memory().unwrap();
    let matrix = MockMatrixClient::default();
    let xmsg = CrashWindowXmsg {
        crash_first_return: Mutex::new(true),
        keys: Mutex::new(HashMap::new()),
        delivered: Mutex::new(Vec::new()),
    };

    let ev = IncomingMatrixEvent {
        room_id: ROOM.to_string(),
        event_id: "$ev_xm4_crash".to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        body: "@genie crash window test".to_string(),
        formatted_body: None,
        mentions: Some(vec!["@genie:example.org".to_string()]),
        timestamp_ms: EVENT_MS,
        thread_root_id: Some("$root_xm4_crash".to_string()),
        replaces_event_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
    };

    // First attempt: `xmsg` accepts the send under the deterministic forward key, then connection/process drops
    // before `store.record_event_relayed` runs.
    let first_outcome = handle_incoming_event_with_guard(
        &ev,
        &[],
        &cfg,
        &matrix,
        &xmsg,
        &MockGuardClient::new(),
        &store,
        None,
    )
    .await
    .unwrap();
    assert_eq!(first_outcome, BotOutcome::Pending);

    assert_eq!(
        xmsg.delivered.lock().unwrap().len(),
        1,
        "xmsg accepted the send before the crash"
    );
    assert!(
        !store.is_event_relayed("$ev_xm4_crash").unwrap(),
        "crash occurred before record_event_relayed committed"
    );
    assert!(
        store.get_forward_retry("$ev_xm4_crash").unwrap().is_some(),
        "pending forward row must be durable in SQLite across the crash window"
    );

    // Restart recovery: retry worker sweep + Matrix sync redelivery both deduplicate on the forward key.
    let retry_now = EVENT_MS / 1000 + 2;
    let swept = sweep_due_forward_retries(&cfg, &matrix, &xmsg, &store, retry_now)
        .await
        .unwrap();
    assert_eq!(swept, 1);
    assert!(store.is_event_relayed("$ev_xm4_crash").unwrap());
    assert_eq!(
        xmsg.delivered.lock().unwrap().len(),
        1,
        "restart recovery must deduplicate under the deterministic forward key (one delivery)"
    );

    let redeliver = handle_incoming_event_with_guard(
        &ev,
        &[],
        &cfg,
        &matrix,
        &xmsg,
        &MockGuardClient::new(),
        &store,
        None,
    )
    .await
    .unwrap();
    assert_eq!(redeliver, BotOutcome::IgnoredDuplicate);
    assert_eq!(xmsg.delivered.lock().unwrap().len(), 1);
}

#[path = "common/replies.rs"]
mod replies;
use matrix_xmsg::xmsg::ReplyOutcome;

#[path = "common/transport.rs"]
mod transport_fixture;
