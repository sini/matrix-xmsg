use async_trait::async_trait;
use matrix_xmsg::bot::{handle_incoming_event, IncomingMatrixEvent};
use matrix_xmsg::config::{Admission, Config};
use matrix_xmsg::error::AppError;
use matrix_xmsg::matrix::MockMatrixClient;
use matrix_xmsg::store::Store;
use matrix_xmsg::xmsg::{SendResponse, XmsgClient};
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

struct MockSessionClient {
    sent_payloads: Mutex<Vec<(String, String, String)>>,
    responses: Mutex<VecDeque<Result<SendResponse, AppError>>>,
    default_session_id: Mutex<Option<String>>,
    send_counter: AtomicUsize,
}

impl MockSessionClient {
    fn new(default_session: Option<&str>) -> Self {
        Self {
            sent_payloads: Mutex::new(Vec::new()),
            responses: Mutex::new(VecDeque::new()),
            default_session_id: Mutex::new(default_session.map(|s| s.to_string())),
            send_counter: AtomicUsize::new(0),
        }
    }

    fn queue_response(&self, resp: Result<SendResponse, AppError>) {
        self.responses.lock().unwrap().push_back(resp);
    }
}

#[async_trait]
impl XmsgClient for MockSessionClient {
    async fn send_message(
        &self,
        expert_ref: &str,
        from: &str,
        text: &str,
    ) -> Result<SendResponse, AppError> {
        let mut list = self.sent_payloads.lock().unwrap();
        list.push((expert_ref.to_string(), from.to_string(), text.to_string()));

        let mut responses = self.responses.lock().unwrap();
        if let Some(resp) = responses.pop_front() {
            return resp;
        }

        let num = self.send_counter.fetch_add(1, Ordering::SeqCst) + 1;
        let sess = self.default_session_id.lock().unwrap().clone();
        Ok(SendResponse {
            message_id: format!("01M16MSG{num:06}"),
            session_id: sess,
        })
    }
}

fn test_config() -> Config {
    Config {
        homeserver_url: "https://matrix.example.org".to_string(),
        bot_mxid: "@genie:example.org".to_string(),
        access_token_file: PathBuf::from("/nonexistent/token"),
        rooms: vec!["!support:example.org".to_string()],
        trusted_mxids: vec![
            "@alice:example.org".to_string(),
            "@trusted:example.org".to_string(),
        ],
        owner_mxid: "@owner:example.org".to_string(),
        admission: Admission::Trusted,
        xmsg_socket: PathBuf::from("/run/user/1000/xmsg"),
        expert_ref: "claude".to_string(),
        history_n: 5,
        history_byte_cap: 1024,
        rate_limit_count: 10,
        rate_limit_window_secs: 60,
        size_cap_bytes: 1024,
        answer_timeout_secs: 10,
        session_live_secs: 3600,
        resync_byte_cap: 65536,
        db_path: PathBuf::from(":memory:"),
    }
}

fn make_event(id: &str, thread_root: Option<&str>, body: &str, ts: i64) -> IncomingMatrixEvent {
    IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: id.to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        body: body.to_string(),
        formatted_body: None,
        mentions: Some(vec!["@genie:example.org".to_string()]),
        timestamp_ms: ts,
        thread_root_id: thread_root.map(|s| s.to_string()),
        replaces_event_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
    }
}

// ---------------------------------------------------------------------------
// Oracle 1:
// Two forwards to the same session within the live window => bootstrap then delta
// (M11 behaviour kept).
// Mutant: always re-bootstrap => RED.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_1_two_forwards_same_session_bootstrap_then_delta() {
    let config = test_config();
    let matrix = MockMatrixClient::default();
    let store = Store::new_in_memory().unwrap();
    let xmsg = MockSessionClient::new(Some("sess_alpha"));

    let ev1 = make_event("$ev_1", Some("$root_1"), "@genie question 1", 1000);
    handle_incoming_event(&ev1, &[], &config, &matrix, &xmsg, &store)
        .await
        .expect("forward 1 must succeed");

    {
        let sent = xmsg.sent_payloads.lock().unwrap();
        assert_eq!(
            sent.len(),
            1,
            "First forward must send exactly 1 message (bootstrap)"
        );
        assert!(
            sent[0].2.contains("context=\"bootstrap\""),
            "First forward must carry context=\"bootstrap\""
        );
    }

    let cursor1 = store.get_thread_cursor("$root_1").unwrap().unwrap();
    assert_eq!(cursor1.cursor_event_id, "$ev_1");
    assert_eq!(cursor1.session_id.as_deref(), Some("sess_alpha"));

    // Second forward 50s later (within 3600s live window) to the same session
    let ev2 = make_event("$ev_2", Some("$root_1"), "@genie question 2", 1050);
    handle_incoming_event(&ev2, &[], &config, &matrix, &xmsg, &store)
        .await
        .expect("forward 2 must succeed");

    {
        let sent = xmsg.sent_payloads.lock().unwrap();
        assert_eq!(
            sent.len(),
            2,
            "Second forward to same session must send exactly 1 delta (total 2 sends)"
        );
        assert!(
            sent[1].2.contains("context=\"delta\""),
            "Second forward must carry context=\"delta\""
        );
        assert!(
            !sent[1].2.contains("context=\"bootstrap\""),
            "Second forward must NOT carry context=\"bootstrap\""
        );
    }

    let cursor2 = store.get_thread_cursor("$root_1").unwrap().unwrap();
    assert_eq!(cursor2.cursor_event_id, "$ev_2");
    assert_eq!(cursor2.session_id.as_deref(), Some("sess_alpha"));
}

// ---------------------------------------------------------------------------
// Oracle 2:
// A forward whose response names a different session than the cursor => followed by
// a bootstrap to the same ref, and the cursor then records the new session.
// Mutant: ignore the session id => RED.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_2_session_change_triggers_immediate_bootstrap() {
    let config = test_config();
    let matrix = MockMatrixClient::default();
    let store = Store::new_in_memory().unwrap();
    let xmsg = MockSessionClient::new(None);

    // Initial forward lands on sess_alpha
    xmsg.queue_response(Ok(SendResponse {
        message_id: "01M16MSG000001".to_string(),
        session_id: Some("sess_alpha".to_string()),
    }));

    let ev1 = make_event("$ev_1", Some("$root_2"), "@genie initial question", 1000);
    handle_incoming_event(&ev1, &[], &config, &matrix, &xmsg, &store)
        .await
        .expect("forward 1 must succeed");

    let cursor1 = store.get_thread_cursor("$root_2").unwrap().unwrap();
    assert_eq!(cursor1.session_id.as_deref(), Some("sess_alpha"));

    // Next forward arrives at t=1050 (within live window).
    // The initial delta lands on a restarted session: sess_beta!
    xmsg.queue_response(Ok(SendResponse {
        message_id: "01M16MSG000002".to_string(),
        session_id: Some("sess_beta".to_string()),
    }));
    // The immediate bootstrap also lands on sess_beta:
    xmsg.queue_response(Ok(SendResponse {
        message_id: "01M16MSG000003".to_string(),
        session_id: Some("sess_beta".to_string()),
    }));

    let ev2 = make_event(
        "$ev_2",
        Some("$root_2"),
        "@genie question after restart",
        1050,
    );
    handle_incoming_event(&ev2, &[], &config, &matrix, &xmsg, &store)
        .await
        .expect("forward 2 must succeed");

    {
        let sent = xmsg.sent_payloads.lock().unwrap();
        assert_eq!(
            sent.len(),
            3,
            "Expected 3 sends total: initial forward (bootstrap), forward 2 (delta), then re-bootstrap"
        );
        // Send 1 (ev1): bootstrap
        assert!(sent[0].2.contains("context=\"bootstrap\""));
        // Send 2 (ev2 delta): delta
        assert!(sent[1].2.contains("context=\"delta\""));
        // Send 3 (re-bootstrap to same ref): bootstrap
        assert_eq!(sent[2].0, config.expert_ref);
        assert!(
            sent[2].2.contains("context=\"bootstrap\""),
            "Follow-up send must carry context=\"bootstrap\""
        );
        assert!(
            sent[2].2.contains("supersedes=\"01M16MSG000002\""),
            "Follow-up bootstrap must carry supersedes attribute naming delta message id"
        );
    }

    let cursor2 = store.get_thread_cursor("$root_2").unwrap().unwrap();
    assert_eq!(cursor2.cursor_event_id, "$ev_2");
    assert_eq!(
        cursor2.session_id.as_deref(),
        Some("sess_beta"),
        "Cursor must record the new session id"
    );

    // Both message IDs must be recorded against the thread
    let thread_msgs = store.get_thread_messages("$root_2").unwrap();
    assert!(
        thread_msgs.contains(&"01M16MSG000002".to_string()),
        "Thread messages must contain delta message id"
    );
    assert!(
        thread_msgs.contains(&"01M16MSG000003".to_string()),
        "Thread messages must contain bootstrap message id"
    );
}

// ---------------------------------------------------------------------------
// Oracle 3:
// A cursor row with no session id (pre-migration) => the next forward re-bootstraps
// once, then deltas.
// Mutant: treat missing as equal => RED.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_3_pre_migration_missing_session_rebootstraps_once_then_deltas() {
    let config = test_config();
    let matrix = MockMatrixClient::default();
    let store = Store::new_in_memory().unwrap();
    let xmsg = MockSessionClient::new(Some("sess_gamma"));

    // Pre-migration row with None session_id
    store
        .set_thread_cursor("$root_3", "$ev_old", 1000, None)
        .unwrap();

    let pre_cursor = store.get_thread_cursor("$root_3").unwrap().unwrap();
    assert_eq!(
        pre_cursor.session_id, None,
        "Pre-migration row has no session_id"
    );

    // Forward 1: arrives at 1050 (within live window). Missing session_id triggers re-bootstrap!
    let ev1 = make_event(
        "$ev_new1",
        Some("$root_3"),
        "@genie question after migration",
        1050,
    );
    handle_incoming_event(&ev1, &[], &config, &matrix, &xmsg, &store)
        .await
        .expect("forward 1 must succeed");

    {
        let sent = xmsg.sent_payloads.lock().unwrap();
        assert_eq!(
            sent.len(),
            2,
            "Expected delta followed by immediate bootstrap for pre-migration row"
        );
        assert!(sent[0].2.contains("context=\"delta\""));
        assert!(sent[1].2.contains("context=\"bootstrap\""));
    }

    let cursor1 = store.get_thread_cursor("$root_3").unwrap().unwrap();
    assert_eq!(cursor1.session_id.as_deref(), Some("sess_gamma"));

    // Forward 2: arrives at 1100 (within live window) to same sess_gamma => DELTA ONLY!
    let ev2 = make_event(
        "$ev_new2",
        Some("$root_3"),
        "@genie subsequent question",
        1100,
    );
    handle_incoming_event(&ev2, &[], &config, &matrix, &xmsg, &store)
        .await
        .expect("forward 2 must succeed");

    {
        let sent = xmsg.sent_payloads.lock().unwrap();
        assert_eq!(
            sent.len(),
            3,
            "Subsequent forward to same session must send only delta (1 more send, total 3)"
        );
        assert!(sent[2].2.contains("context=\"delta\""));
        assert!(!sent[2].2.contains("context=\"bootstrap\""));
    }
}

// ---------------------------------------------------------------------------
// Oracle 4:
// A failed re-bootstrap send leaves the cursor's session unchanged (the next forward
// tries again).
// Mutant: record the new session before the send => RED.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_4_failed_rebootstrap_leaves_cursor_session_unchanged() {
    let config = test_config();
    let matrix = MockMatrixClient::default();
    let store = Store::new_in_memory().unwrap();
    let xmsg = MockSessionClient::new(None);

    // Initial forward recorded with sess_old
    store
        .set_thread_cursor("$root_4", "$ev_init", 1000, Some("sess_old"))
        .unwrap();

    // Next forward at t=1050:
    // Initial delta send succeeds on new session sess_new:
    xmsg.queue_response(Ok(SendResponse {
        message_id: "01M16MSG000001".to_string(),
        session_id: Some("sess_new".to_string()),
    }));
    // Re-bootstrap send FAILS:
    xmsg.queue_response(Err(AppError::Xmsg(
        "simulated network failure on bootstrap".to_string(),
    )));

    let ev_fail = make_event("$ev_attempt1", Some("$root_4"), "@genie question", 1050);
    let res = handle_incoming_event(&ev_fail, &[], &config, &matrix, &xmsg, &store).await;
    assert!(res.is_err(), "Forward must fail when re-bootstrap fails");

    // The cursor's session MUST REMAIN UNCHANGED (still sess_old)
    let cursor_after_fail = store.get_thread_cursor("$root_4").unwrap().unwrap();
    assert_eq!(
        cursor_after_fail.session_id.as_deref(),
        Some("sess_old"),
        "Failed re-bootstrap must leave cursor session_id unchanged"
    );
    assert_eq!(cursor_after_fail.cursor_event_id, "$ev_init");

    // Next forward at t=1060 (tries again):
    // Initial delta send returns sess_new:
    xmsg.queue_response(Ok(SendResponse {
        message_id: "01M16MSG000002".to_string(),
        session_id: Some("sess_new".to_string()),
    }));
    // Re-bootstrap send SUCCEEDS this time:
    xmsg.queue_response(Ok(SendResponse {
        message_id: "01M16MSG000003".to_string(),
        session_id: Some("sess_new".to_string()),
    }));

    let ev_retry = make_event(
        "$ev_attempt2",
        Some("$root_4"),
        "@genie question retry",
        1060,
    );
    handle_incoming_event(&ev_retry, &[], &config, &matrix, &xmsg, &store)
        .await
        .expect("Retry forward must succeed");

    let cursor_after_success = store.get_thread_cursor("$root_4").unwrap().unwrap();
    assert_eq!(
        cursor_after_success.session_id.as_deref(),
        Some("sess_new"),
        "Cursor must advance with sess_new after successful re-bootstrap"
    );
    assert_eq!(cursor_after_success.cursor_event_id, "$ev_attempt2");
}

// ---------------------------------------------------------------------------
// Oracle 5:
// On a session change, the bootstrap envelope carries supersedes equal to the
// delta's message id, and both message ids are recorded against the thread.
// Mutant: omit supersedes attribute => RED.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_5_session_change_rebootstrap_carries_supersedes_delta_message_id() {
    let config = test_config();
    let matrix = MockMatrixClient::default();
    let store = Store::new_in_memory().unwrap();
    let xmsg = MockSessionClient::new(None);

    // Initial forward lands on sess_initial
    xmsg.queue_response(Ok(SendResponse {
        message_id: "01M16DELTA000001".to_string(),
        session_id: Some("sess_initial".to_string()),
    }));

    let ev1 = make_event("$ev_init", Some("$root_5"), "@genie question 1", 1000);
    handle_incoming_event(&ev1, &[], &config, &matrix, &xmsg, &store)
        .await
        .expect("initial forward must succeed");

    // Second forward lands on sess_changed
    let delta_msg_id = "01M16DELTA000002";
    xmsg.queue_response(Ok(SendResponse {
        message_id: delta_msg_id.to_string(),
        session_id: Some("sess_changed".to_string()),
    }));
    // Re-bootstrap succeeds
    xmsg.queue_response(Ok(SendResponse {
        message_id: "01M16BOOT000003".to_string(),
        session_id: Some("sess_changed".to_string()),
    }));

    let ev2 = make_event("$ev_second", Some("$root_5"), "@genie question 2", 1050);
    handle_incoming_event(&ev2, &[], &config, &matrix, &xmsg, &store)
        .await
        .expect("second forward must succeed");

    let sent = xmsg.sent_payloads.lock().unwrap();
    assert_eq!(sent.len(), 3);
    let rebootstrap_envelope = &sent[2].2;
    assert!(
        rebootstrap_envelope.contains("context=\"bootstrap\""),
        "Follow-up must be bootstrap"
    );
    assert!(
        rebootstrap_envelope.contains(&format!("supersedes=\"{delta_msg_id}\"")),
        "Bootstrap envelope must carry supersedes attribute equal to delta message id, got: {rebootstrap_envelope}"
    );

    // Bot records both message ids against the thread
    let thread_msgs = store.get_thread_messages("$root_5").unwrap();
    assert!(
        thread_msgs.contains(&delta_msg_id.to_string()),
        "Thread messages must contain delta message id"
    );
    assert!(
        thread_msgs.contains(&"01M16BOOT000003".to_string()),
        "Thread messages must contain bootstrap message id"
    );
}
