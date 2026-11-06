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

#[path = "common/replies.rs"]
mod replies;
use matrix_xmsg::xmsg::ReplyOutcome;

#[path = "common/transport.rs"]
mod transport_fixture;
