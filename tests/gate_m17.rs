use async_trait::async_trait;
use matrix_xmsg::bot::{
    handle_inbox_delivery_with_xmsg, handle_incoming_event, BotOutcome, IncomingMatrixEvent,
};
use matrix_xmsg::config::{Admission, Config};
use matrix_xmsg::context::EventMessage;
use matrix_xmsg::error::AppError;
use matrix_xmsg::matrix::MockMatrixClient;
use matrix_xmsg::store::Store;
use matrix_xmsg::xmsg::{SendResponse, SvcDelivery, SvcInbox, XmsgClient};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

#[derive(Default)]
struct MockResyncXmsgClient {
    pub sent_messages: Mutex<Vec<(String, String, String)>>, // (expert_ref, from, text)
    pub sent_replies: Mutex<Vec<(String, String)>>,          // (message_id, text)
    pub session_id: Mutex<Option<String>>,
    send_counter: AtomicUsize,
}

impl MockResyncXmsgClient {
    fn new(session_id: Option<&str>) -> Self {
        Self {
            sent_messages: Mutex::new(Vec::new()),
            sent_replies: Mutex::new(Vec::new()),
            session_id: Mutex::new(session_id.map(|s| s.to_string())),
            send_counter: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl XmsgClient for MockResyncXmsgClient {
    async fn send_message(
        &self,
        expert_ref: &str,
        from: &str,
        text: &str,
    ) -> Result<SendResponse, AppError> {
        self.sent_messages.lock().unwrap().push((
            expert_ref.to_string(),
            from.to_string(),
            text.to_string(),
        ));
        let num = self.send_counter.fetch_add(1, Ordering::SeqCst) + 1;
        let sess = self.session_id.lock().unwrap().clone();
        Ok(SendResponse {
            message_id: format!("01M17FWD{num:06}"),
            session_id: sess,
        })
    }

    async fn reply_message(&self, message_id: &str, text: &str) -> Result<SendResponse, AppError> {
        self.sent_replies
            .lock()
            .unwrap()
            .push((message_id.to_string(), text.to_string()));
        let num = self.send_counter.fetch_add(1, Ordering::SeqCst) + 1;
        let sess = self.session_id.lock().unwrap().clone();
        Ok(SendResponse {
            message_id: format!("01M17REP{num:06}"),
            session_id: sess,
        })
    }
}

struct MockTestInbox {
    pub acks: Mutex<Vec<String>>,
}

impl MockTestInbox {
    fn new() -> Self {
        Self {
            acks: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl SvcInbox for MockTestInbox {
    async fn poll(&mut self, _wait_secs: u64) -> Result<Option<SvcDelivery>, AppError> {
        Ok(None)
    }

    async fn ack(&mut self, message_id: &str) -> Result<(), AppError> {
        self.acks.lock().unwrap().push(message_id.to_string());
        Ok(())
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
        guard_ref: "svc:genie-guard".to_string(),
        guard_timeout_secs: 30,
        guard_retry_budget: 3,
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

fn make_event_msg(id: &str, thread_root: Option<&str>, body: &str, ts: i64) -> EventMessage {
    EventMessage {
        event_id: id.to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        timestamp_ms: ts,
        body: body.to_string(),
        thread_root_id: thread_root.map(|s| s.to_string()),
    }
}

// ---------------------------------------------------------------------------
// Oracle 1:
// resync reply produces exactly one transcript reply to asker containing
// every thread line from root; nothing posted to room.
// Mutant: post in room -> RED.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_1_resync_reply_produces_transcript_reply_and_nothing_in_room() {
    let config = test_config();
    let matrix = MockMatrixClient::default();
    let store = Store::new_in_memory().unwrap();
    let xmsg = MockResyncXmsgClient::new(Some("session-oracle1"));

    // Prepare timeline and thread history
    let root_msg = make_event_msg("$root_1", None, "@genie help with root topic", 1000);
    let reply1 = make_event_msg("$rep_1_1", Some("$root_1"), "thread reply 1", 1010);
    let reply2 = make_event_msg("$rep_1_2", Some("$root_1"), "thread reply 2", 1020);
    let bg_msg = make_event_msg("$bg_1", None, "earlier room message", 900);

    matrix
        .canned_events
        .lock()
        .unwrap()
        .insert("$root_1".to_string(), root_msg.clone());
    matrix
        .canned_history
        .lock()
        .unwrap()
        .extend(vec![bg_msg, root_msg]);
    matrix
        .canned_thread_relations
        .lock()
        .unwrap()
        .insert("$root_1".to_string(), vec![reply1, reply2]);

    // Initial forward of root message
    let ev = make_event("$root_1", None, "@genie help with root topic", 1000);
    let outcome = handle_incoming_event(&ev, &[], &config, &matrix, &xmsg, &store)
        .await
        .expect("handle_incoming_event must succeed");
    assert_eq!(outcome, BotOutcome::Forwarded);

    let fwd_msg_id = xmsg.sent_messages.lock().unwrap()[0].2.clone();
    assert!(!fwd_msg_id.is_empty());
    // Get the forwarded message id generated by xmsg
    let last_fwd_id = "01M17FWD000001";

    // Asker sends {"resync": true}
    let delivery = SvcDelivery {
        message_id: "01RESYNC00001".to_string(),
        from_name: "session-oracle1".to_string(),
        origin: None,
        text: r#"{"resync": true}"#.to_string(),
        envelope: format!(
            "[xmsg] reply to message_id={last_fwd_id} — message_id=01RESYNC00001\n{{\"resync\": true}}"
        ),
    };

    let mut inbox = MockTestInbox::new();
    let resync_outcome = handle_inbox_delivery_with_xmsg(
        &delivery, &config, &matrix, &store, &xmsg, &mut inbox, 1030,
    )
    .await
    .expect("handle_inbox_delivery_with_xmsg must succeed");

    assert_eq!(resync_outcome, BotOutcome::Resynced);

    // Assert: NOTHING posted in room
    let sent_notices = matrix.sent_notices.lock().unwrap().clone();
    assert!(
        sent_notices.is_empty(),
        "Resync control must NEVER post anything to the Matrix room, found: {:?}",
        sent_notices
    );

    // Assert: exactly one reply to the asker
    let replies = xmsg.sent_replies.lock().unwrap().clone();
    assert_eq!(
        replies.len(),
        1,
        "Exactly one transcript reply must be sent"
    );
    assert_eq!(replies[0].0, "01RESYNC00001");

    let transcript = &replies[0].1;
    // Envelope format matches bootstrap
    assert!(
        transcript.contains(r#"context="bootstrap""#),
        "Transcript must carry context=\"bootstrap\""
    );
    assert!(
        transcript.contains(r#"role="background""#),
        "Transcript must carry background block"
    );
    assert!(
        transcript.contains(r#"role="thread""#),
        "Transcript must carry thread block"
    );
    // Contains every thread line from root
    assert!(
        transcript.contains("help with root topic"),
        "Transcript must contain root message"
    );
    assert!(
        transcript.contains("thread reply 1"),
        "Transcript must contain reply 1"
    );
    assert!(
        transcript.contains("thread reply 2"),
        "Transcript must contain reply 2"
    );
    assert!(
        transcript.contains("earlier room message"),
        "Transcript must contain room background"
    );

    // Assert: delivery was acknowledged
    let acks = inbox.acks.lock().unwrap().clone();
    assert_eq!(acks, vec!["01RESYNC00001".to_string()]);
}

// ---------------------------------------------------------------------------
// Oracle 2:
// thread longer than history_n returned in full (paged fetch).
// Mutant: fetch one page only -> RED.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_2_thread_longer_than_history_n_returned_in_full_paged() {
    let mut config = test_config();
    config.history_n = 5; // small history window

    let matrix = MockMatrixClient::default();
    let store = Store::new_in_memory().unwrap();
    let xmsg = MockResyncXmsgClient::new(Some("session-oracle2"));

    // Set page size to 3 so fetching 12 replies requires 4 separate pages
    *matrix.canned_thread_page_size.lock().unwrap() = Some(3);

    let root_msg = make_event_msg("$root_2", None, "@genie thread root", 2000);
    matrix
        .canned_events
        .lock()
        .unwrap()
        .insert("$root_2".to_string(), root_msg.clone());
    matrix.canned_history.lock().unwrap().push(root_msg);

    // 12 replies in the thread (> history_n = 5)
    let mut replies = Vec::new();
    for i in 1..=12 {
        replies.push(make_event_msg(
            &format!("$rep_2_{i}"),
            Some("$root_2"),
            &format!("detailed thread message number {i}"),
            2000 + i as i64 * 10,
        ));
    }
    matrix
        .canned_thread_relations
        .lock()
        .unwrap()
        .insert("$root_2".to_string(), replies);

    // Initial forward
    let ev = make_event("$root_2", None, "@genie thread root", 2000);
    handle_incoming_event(&ev, &[], &config, &matrix, &xmsg, &store)
        .await
        .unwrap();

    let fwd_id = "01M17FWD000001";
    let delivery = SvcDelivery {
        message_id: "01RESYNC00002".to_string(),
        from_name: "session-oracle2".to_string(),
        origin: None,
        text: r#"{"resync": true}"#.to_string(),
        envelope: format!(
            "[xmsg] reply to message_id={fwd_id} — message_id=01RESYNC00002\n{{\"resync\": true}}"
        ),
    };

    let mut inbox = MockTestInbox::new();
    let resync_outcome = handle_inbox_delivery_with_xmsg(
        &delivery, &config, &matrix, &store, &xmsg, &mut inbox, 2200,
    )
    .await
    .unwrap();

    assert_eq!(resync_outcome, BotOutcome::Resynced);

    let sent_replies = xmsg.sent_replies.lock().unwrap().clone();
    assert_eq!(sent_replies.len(), 1);
    let transcript = &sent_replies[0].1;

    // Must contain root and ALL 12 thread replies
    assert!(transcript.contains("thread root"));
    for i in 1..=12 {
        assert!(
            transcript.contains(&format!("detailed thread message number {i}")),
            "Transcript must contain reply {i} even across multiple pages"
        );
    }
}

// ---------------------------------------------------------------------------
// Oracle 3:
// over resync_byte_cap, background lines go first and envelope says truncated.
// Mutant: drop thread lines first -> RED.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_3_over_byte_cap_background_lines_dropped_first_and_envelope_truncated() {
    let mut config = test_config();
    // Set a restrictive byte cap: fits the envelope + thread lines, but not room background
    config.resync_byte_cap = 900;

    let matrix = MockMatrixClient::default();
    let store = Store::new_in_memory().unwrap();
    let xmsg = MockResyncXmsgClient::new(Some("session-oracle3"));

    let root_msg = make_event_msg("$root_3", None, "@genie root topic message", 3000);
    let rep1 = make_event_msg(
        "$rep_3_1",
        Some("$root_3"),
        "important thread response alpha",
        3010,
    );
    let rep2 = make_event_msg(
        "$rep_3_2",
        Some("$root_3"),
        "important thread response beta",
        3020,
    );

    // Multiple long background messages
    let bg1 = make_event_msg(
        "$bg_3_1",
        None,
        "long background history message line 1 with lots of padding content",
        2900,
    );
    let bg2 = make_event_msg(
        "$bg_3_2",
        None,
        "long background history message line 2 with lots of padding content",
        2910,
    );
    let bg3 = make_event_msg(
        "$bg_3_3",
        None,
        "long background history message line 3 with lots of padding content",
        2920,
    );

    matrix
        .canned_events
        .lock()
        .unwrap()
        .insert("$root_3".to_string(), root_msg.clone());
    matrix
        .canned_history
        .lock()
        .unwrap()
        .extend(vec![bg1, bg2, bg3, root_msg]);
    matrix
        .canned_thread_relations
        .lock()
        .unwrap()
        .insert("$root_3".to_string(), vec![rep1, rep2]);

    let ev = make_event("$root_3", None, "@genie root topic message", 3000);
    handle_incoming_event(&ev, &[], &config, &matrix, &xmsg, &store)
        .await
        .unwrap();

    let fwd_id = "01M17FWD000001";
    let delivery = SvcDelivery {
        message_id: "01RESYNC00003".to_string(),
        from_name: "session-oracle3".to_string(),
        origin: None,
        text: r#"{"resync": true}"#.to_string(),
        envelope: format!(
            "[xmsg] reply to message_id={fwd_id} — message_id=01RESYNC00003\n{{\"resync\": true}}"
        ),
    };

    let mut inbox = MockTestInbox::new();
    let resync_outcome = handle_inbox_delivery_with_xmsg(
        &delivery, &config, &matrix, &store, &xmsg, &mut inbox, 3030,
    )
    .await
    .unwrap();

    assert_eq!(resync_outcome, BotOutcome::Resynced);

    let sent_replies = xmsg.sent_replies.lock().unwrap().clone();
    assert_eq!(sent_replies.len(), 1);
    let transcript = &sent_replies[0].1;

    // Envelope must state truncated="true"
    assert!(
        transcript.contains(r#"truncated="true""#),
        "Envelope must indicate truncated=\"true\" when lines are dropped; got: {transcript}"
    );

    // Thread lines MUST be preserved
    assert!(
        transcript.contains("important thread response alpha"),
        "Thread lines must be preserved before room background"
    );
    assert!(
        transcript.contains("important thread response beta"),
        "Thread lines must be preserved before room background"
    );

    // Oldest background line MUST have been dropped
    assert!(
        !transcript.contains("long background history message line 1"),
        "Oldest background lines must be dropped first to satisfy resync_byte_cap"
    );
}

// ---------------------------------------------------------------------------
// Oracle 4:
// after resync, next forward is delta from newest line sent.
// Mutant: leave cursor -> RED.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_4_after_resync_next_forward_is_delta_from_newest_line_sent() {
    let config = test_config();
    let matrix = MockMatrixClient::default();
    let store = Store::new_in_memory().unwrap();
    let xmsg = MockResyncXmsgClient::new(Some("session-oracle4"));

    let root_msg = make_event_msg("$root_4", None, "@genie initial topic", 4000);
    let t1 = make_event_msg("$t_4_1", Some("$root_4"), "thread message 1", 4010);
    let t2 = make_event_msg("$t_4_2", Some("$root_4"), "thread message 2 newest", 4020);

    matrix
        .canned_events
        .lock()
        .unwrap()
        .insert("$root_4".to_string(), root_msg.clone());
    matrix.canned_history.lock().unwrap().push(root_msg);
    matrix
        .canned_thread_relations
        .lock()
        .unwrap()
        .insert("$root_4".to_string(), vec![t1.clone(), t2.clone()]);

    // Initial forward at t=4000
    let ev_root = make_event("$root_4", None, "@genie initial topic", 4000);
    handle_incoming_event(&ev_root, &[], &config, &matrix, &xmsg, &store)
        .await
        .unwrap();

    let fwd_id = "01M17FWD000001";

    // Resync requested by session at t=4030
    let delivery = SvcDelivery {
        message_id: "01RESYNC00004".to_string(),
        from_name: "session-oracle4".to_string(),
        origin: None,
        text: r#"{"resync": true}"#.to_string(),
        envelope: format!(
            "[xmsg] reply to message_id={fwd_id} — message_id=01RESYNC00004\n{{\"resync\": true}}"
        ),
    };

    let mut inbox = MockTestInbox::new();
    handle_inbox_delivery_with_xmsg(&delivery, &config, &matrix, &store, &xmsg, &mut inbox, 4030)
        .await
        .unwrap();

    // Verify cursor updated in store to $t_4_2
    let cursor = store
        .get_thread_cursor("$root_4")
        .unwrap()
        .expect("cursor must exist");
    assert_eq!(cursor.cursor_event_id, "$t_4_2");
    assert_eq!(cursor.session_id.as_deref(), Some("session-oracle4"));

    // A new message in thread arrives at t=4040: $t_4_3
    let t3 = make_event_msg("$t_4_3", Some("$root_4"), "@genie what about part 3", 4040);
    let thread_history = vec![t1, t2, t3];

    let ev_new = make_event("$t_4_3", Some("$root_4"), "@genie what about part 3", 4040);
    let fwd_outcome =
        handle_incoming_event(&ev_new, &thread_history, &config, &matrix, &xmsg, &store)
            .await
            .expect("handle_incoming_event must succeed");

    assert_eq!(fwd_outcome, BotOutcome::Forwarded);

    // The forward must be a DELTA starting after $t_4_2 (newest line sent during resync)
    let sends = xmsg.sent_messages.lock().unwrap().clone();
    assert_eq!(sends.len(), 2, "Must have forwarded root and then t3");

    let second_payload = &sends[1].2;
    assert!(
        second_payload.contains(r#"context="delta""#),
        "Subsequent forward must be a delta context, got: {second_payload}"
    );
    assert!(
        !second_payload.contains("thread message 1"),
        "Delta forward must NOT contain lines prior to the cursor ($t_4_1)"
    );
    assert!(
        !second_payload.contains("thread message 2 newest"),
        "Delta forward must NOT contain lines prior to the cursor ($t_4_2)"
    );
    assert!(
        second_payload.contains("what about part 3"),
        "Delta forward must contain the new message ($t_4_3)"
    );
}

// ---------------------------------------------------------------------------
// Oracle 5:
// second resync within 60s is throttled.
// Mutant: no throttle -> RED.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_5_second_resync_within_60s_is_throttled() {
    let config = test_config();
    let matrix = MockMatrixClient::default();
    let store = Store::new_in_memory().unwrap();
    let xmsg = MockResyncXmsgClient::new(Some("session-oracle5"));

    let root_msg = make_event_msg("$root_5", None, "@genie throttle test", 5000);
    matrix
        .canned_events
        .lock()
        .unwrap()
        .insert("$root_5".to_string(), root_msg.clone());
    matrix.canned_history.lock().unwrap().push(root_msg);

    let ev = make_event("$root_5", None, "@genie throttle test", 5000);
    handle_incoming_event(&ev, &[], &config, &matrix, &xmsg, &store)
        .await
        .unwrap();

    let fwd_id = "01M17FWD000001";

    // Resync 1 at t=5000 => Success
    let delivery1 = SvcDelivery {
        message_id: "01RESYNC00005_1".to_string(),
        from_name: "session-oracle5".to_string(),
        origin: None,
        text: r#"{"resync": true}"#.to_string(),
        envelope: format!(
            "[xmsg] reply to message_id={fwd_id} — message_id=01RESYNC00005_1\n{{\"resync\": true}}"
        ),
    };
    let mut inbox1 = MockTestInbox::new();
    let outcome1 = handle_inbox_delivery_with_xmsg(
        &delivery1,
        &config,
        &matrix,
        &store,
        &xmsg,
        &mut inbox1,
        5000,
    )
    .await
    .unwrap();
    assert_eq!(outcome1, BotOutcome::Resynced);

    // Resync 2 at t=5035 (35 seconds later < 60s) => Throttled
    let delivery2 = SvcDelivery {
        message_id: "01RESYNC00005_2".to_string(),
        from_name: "session-oracle5".to_string(),
        origin: None,
        text: r#"{"resync": true}"#.to_string(),
        envelope: format!(
            "[xmsg] reply to message_id={fwd_id} — message_id=01RESYNC00005_2\n{{\"resync\": true}}"
        ),
    };
    let mut inbox2 = MockTestInbox::new();
    let outcome2 = handle_inbox_delivery_with_xmsg(
        &delivery2,
        &config,
        &matrix,
        &store,
        &xmsg,
        &mut inbox2,
        5035,
    )
    .await
    .unwrap();
    assert_eq!(outcome2, BotOutcome::ResyncThrottled);

    // Verify reply body for throttled delivery
    let replies = xmsg.sent_replies.lock().unwrap().clone();
    assert_eq!(replies.len(), 2);
    let throttled_reply_json: serde_json::Value =
        serde_json::from_str(&replies[1].1).expect("Throttled reply must be valid JSON");
    assert_eq!(
        throttled_reply_json.get("resync").and_then(|v| v.as_str()),
        Some("throttled")
    );
    assert_eq!(
        throttled_reply_json
            .get("retry_after")
            .and_then(|v| v.as_i64()),
        Some(25) // 60 - 35 = 25
    );

    // Resync 3 at t=5065 (65 seconds after first resync > 60s) => Success
    let delivery3 = SvcDelivery {
        message_id: "01RESYNC00005_3".to_string(),
        from_name: "session-oracle5".to_string(),
        origin: None,
        text: r#"{"resync": true}"#.to_string(),
        envelope: format!(
            "[xmsg] reply to message_id={fwd_id} — message_id=01RESYNC00005_3\n{{\"resync\": true}}"
        ),
    };
    let mut inbox3 = MockTestInbox::new();
    let outcome3 = handle_inbox_delivery_with_xmsg(
        &delivery3,
        &config,
        &matrix,
        &store,
        &xmsg,
        &mut inbox3,
        5065,
    )
    .await
    .unwrap();
    assert_eq!(outcome3, BotOutcome::Resynced);
}

// ---------------------------------------------------------------------------
// Unknown message resync test:
// Resync referencing unknown message id responds with {"resync": "unknown"}
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_resync_unknown_forward_message_responds_unknown() {
    let config = test_config();
    let matrix = MockMatrixClient::default();
    let store = Store::new_in_memory().unwrap();
    let xmsg = MockResyncXmsgClient::new(Some("session-unknown"));

    let delivery = SvcDelivery {
        message_id: "01RESYNC_UNK".to_string(),
        from_name: "session-unknown".to_string(),
        origin: None,
        text: r#"{"resync": true}"#.to_string(),
        envelope:
            "[xmsg] reply to message_id=01NONEXISTENT — message_id=01RESYNC_UNK\n{\"resync\": true}"
                .to_string(),
    };

    let mut inbox = MockTestInbox::new();
    let outcome = handle_inbox_delivery_with_xmsg(
        &delivery, &config, &matrix, &store, &xmsg, &mut inbox, 6000,
    )
    .await
    .unwrap();

    assert_eq!(outcome, BotOutcome::ResyncUnknown);

    let replies = xmsg.sent_replies.lock().unwrap().clone();
    assert_eq!(replies.len(), 1);
    assert_eq!(replies[0].1, r#"{"resync": "unknown"}"#);
    assert_eq!(
        inbox.acks.lock().unwrap().clone(),
        vec!["01RESYNC_UNK".to_string()]
    );
}
