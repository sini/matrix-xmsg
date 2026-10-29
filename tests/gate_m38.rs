use async_trait::async_trait;
use matrix_xmsg::bot::{
    handle_inbox_delivery_with_guard, handle_inbox_delivery_with_xmsg,
    handle_incoming_event_with_guard, run_inbox_loop, BotOutcome, IncomingMatrixEvent,
};
use matrix_xmsg::config::{Admission, Config, GuardFailureMode};
use matrix_xmsg::context::EventMessage;
use matrix_xmsg::error::AppError;
use matrix_xmsg::matrix::{MatrixClient, MockMatrixClient};
use matrix_xmsg::store::Store;
use matrix_xmsg::xmsg::{
    route_guard_reply, MockGuardClient, MockSvcInbox, SendResponse, SvcDelivery, SvcInbox,
    XmsgClient,
};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::net::UnixListener;

pub struct MockXmsgClient {
    pub sends: Arc<Mutex<Vec<(String, String, String)>>>, // (expert_ref, from, text)
    pub replies: Arc<Mutex<Vec<(String, String)>>>,       // (message_id, text)
    pub session_id: Mutex<Option<String>>,
    pub counter: AtomicUsize,
}

impl Default for MockXmsgClient {
    fn default() -> Self {
        Self {
            sends: Arc::new(Mutex::new(Vec::new())),
            replies: Arc::new(Mutex::new(Vec::new())),
            session_id: Mutex::new(Some("session-m38@sini.test".to_string())),
            counter: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl XmsgClient for MockXmsgClient {
    async fn send_message(
        &self,
        expert_ref: &str,
        from: &str,
        text: &str,
    ) -> Result<SendResponse, AppError> {
        self.sends.lock().unwrap().push((
            expert_ref.to_string(),
            from.to_string(),
            text.to_string(),
        ));
        let id = self.counter.fetch_add(1, Ordering::SeqCst) + 1;
        let sess = self.session_id.lock().unwrap().clone();
        Ok(SendResponse::new(format!("01M38MSG{id:06}"), sess))
    }

    async fn reply_message(&self, message_id: &str, text: &str) -> Result<SendResponse, AppError> {
        self.replies
            .lock()
            .unwrap()
            .push((message_id.to_string(), text.to_string()));
        let id = self.counter.fetch_add(1, Ordering::SeqCst) + 1;
        let sess = self.session_id.lock().unwrap().clone();
        Ok(SendResponse::new(format!("01M38REPLY{id:06}"), sess))
    }
}

fn test_config() -> Config {
    Config {
        homeserver_url: "https://matrix.example.org".to_string(),
        bot_mxid: "@genie:example.org".to_string(),
        access_token_file: PathBuf::from("/nonexistent/token"),
        rooms: vec!["!support:example.org".to_string()],
        trusted_mxids: vec!["@trusted:example.org".to_string()],
        owner_mxid: "@owner:example.org".to_string(),
        admission: Admission::Public,
        xmsg_socket: PathBuf::from("/run/user/1000/xmsg"),
        expert_ref: "claude".to_string(),
        expert_refs: None,
        room_routes: std::collections::BTreeMap::new(),
        open_thread_nodes: vec![],
        guard_ref: "genie-guard".to_string(),
        guard_timeout_secs: 2,
        guard_retry_budget: 3,
        screen_mentions: true,
        screen_solicited: true,
        guard_failure_mode: GuardFailureMode::Closed,
        guard_outage_dm_window_secs: 3600,
        inbox_retry_budget: 10,
        store_passphrase_file: None,
        undecryptable_wait_secs: 10,
        history_n: 10,
        history_byte_cap: 12 * 1024,
        resync_byte_cap: 65536,
        resync_page_bytes: 8192,
        context_max_lines: 20,
        context_max_bytes: 8192,
        sender_window_secs: 900,
        context_rate_limit_count: 6,
        context_rate_limit_window_secs: 600,
        rate_limit_count: 50,
        rate_limit_window_secs: 600,
        size_cap_bytes: 2000,
        answer_timeout_secs: 300,
        session_live_secs: 3600,
        db_path: PathBuf::from(":memory:"),
    }
}

fn make_event(
    event_id: &str,
    sender: &str,
    body: &str,
    mentioned: bool,
    thread_root: Option<&str>,
    ts_secs: i64,
) -> IncomingMatrixEvent {
    IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: event_id.to_string(),
        sender_mxid: sender.to_string(),
        body: body.to_string(),
        formatted_body: None,
        mentions: if mentioned {
            Some(vec!["@genie:example.org".to_string()])
        } else {
            None
        },
        in_reply_to_event_id: None,
        thread_root_id: thread_root.map(String::from),
        replaces_event_id: None,
        timestamp_ms: ts_secs * 1000,
        is_falling_back: false,
    }
}

// -------------------------------------------------------------------------------------------------
// Oracle 1: A `context` request that needs a guard verdict (which arrives via the inbox) completes
// and returns the screened `<context>` transcript.
// Mutant: handle it inline on the loop => times out => RED.
// -------------------------------------------------------------------------------------------------
#[tokio::test]
async fn oracle_1_context_request_requiring_inbox_guard_verdict_completes() {
    let tmp = tempfile::tempdir().unwrap();
    let agent_sock_path = tmp.path().join("agent.sock");
    let agent_listener = UnixListener::bind(&agent_sock_path).unwrap();

    let inbox = MockSvcInbox::default();
    let inbox_for_guard = inbox.clone();

    // Mock agent.sock for UnixGuardClient: when check_guard sends to genie-guard, push the guard
    // verdict into the bot's SvcInbox so ONLY run_inbox_loop polling the inbox can route it via
    // route_guard_reply.
    let guard_counter = Arc::new(AtomicUsize::new(1));
    tokio::spawn(async move {
        while let Ok((stream, _)) = agent_listener.accept().await {
            let (r, mut w) = stream.into_split();
            let mut lines = tokio::io::BufReader::new(r).lines();
            let inbox_clone = inbox_for_guard.clone();
            let counter = Arc::clone(&guard_counter);
            tokio::spawn(async move {
                while let Ok(Some(line)) = lines.next_line().await {
                    if let Ok(val) = serde_json::from_str::<Value>(&line) {
                        if val.get("action").and_then(|a| a.as_str()) == Some("send") {
                            let n = counter.fetch_add(1, Ordering::SeqCst);
                            let guard_req_id = format!("01M38GUARDREQ{n:06}");
                            let resp = json!({
                                "status": "ok",
                                "message_id": guard_req_id,
                            });
                            let _ = w.write_all(format!("{resp}\n").as_bytes()).await;
                            let _ = w.flush().await;

                            // Push the guard verdict into the inbox after send returns
                            let verdict_id = format!("01M38GUARDVERDICT{n:06}");
                            inbox_clone.push(SvcDelivery {
                                message_id: verdict_id,
                                from_name: "genie-guard@sini.test".to_string(),
                                text: json!({
                                    "verdict": "allow",
                                    "reason": "clean room context",
                                    "cleaned_text": "",
                                })
                                .to_string(),
                                envelope: String::new(),
                                origin: None,
                                in_reply_to: Some(guard_req_id),
                            });
                        }
                    }
                }
            });
        }
    });

    let mut cfg = test_config();
    cfg.xmsg_socket = tmp.path().to_path_buf();
    let config = Arc::new(cfg);

    let matrix = Arc::new(MockMatrixClient::default());
    // Seed room history with an untrusted user line so context request requires guard screening
    matrix.canned_room_history.lock().unwrap().insert(
        "!support:example.org".to_string(),
        vec![EventMessage {
            event_id: "$room_untrusted_1".to_string(),
            sender_mxid: "@stranger:example.org".to_string(),
            timestamp_ms: 1_000_000,
            body: "Untrusted room question that must be screened".to_string(),
            thread_root_id: None,
        }],
    );

    let store = Arc::new(Store::new_in_memory().unwrap());
    let xmsg = Arc::new(MockXmsgClient::default());

    // Seed a forwarded mention so 01M38MSG000001 is a known forward in session-m38
    let trigger = make_event(
        "$trig_1",
        "@trusted:example.org",
        "@genie:example.org help",
        true,
        None,
        1005,
    );
    let mock_guard = MockGuardClient::new();
    handle_incoming_event_with_guard(
        &trigger,
        &[],
        &config,
        matrix.as_ref(),
        xmsg.as_ref(),
        &mock_guard,
        &store,
        None,
    )
    .await
    .unwrap();

    // Push a context request from session-m38 replying to 01M38MSG000001
    inbox.push(SvcDelivery {
        message_id: "01M38CTX_REQ_01".to_string(),
        from_name: "claude · session-m38@sini.test".to_string(),
        text: json!({"context": {"scope": "room", "limit": 5}}).to_string(),
        envelope: String::new(),
        origin: None,
        in_reply_to: Some("01M38MSG000001".to_string()),
    });

    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let loop_handle = tokio::spawn({
        let inbox = inbox.clone();
        let config = Arc::clone(&config);
        let matrix: Arc<dyn MatrixClient> = matrix.clone();
        let store = Arc::clone(&store);
        let xmsg: Arc<dyn XmsgClient> = xmsg.clone();
        async move {
            let _ = run_inbox_loop(inbox, config, matrix, store, xmsg, shutdown_rx).await;
        }
    });

    // Wait up to 1.5s for the context reply to be sent and the delivery acked
    let completed = tokio::time::timeout(Duration::from_millis(1500), async {
        loop {
            let has_reply = !xmsg.replies.lock().unwrap().is_empty();
            let has_ack = inbox
                .acks
                .lock()
                .unwrap()
                .contains(&"01M38CTX_REQ_01".to_string());
            if has_reply && has_ack {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;

    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(Duration::from_millis(200), loop_handle).await;

    assert!(
        completed.is_ok(),
        "Context request requiring inbox guard verdict timed out (inbox self-deadlock)"
    );

    let replies = xmsg.replies.lock().unwrap().clone();
    assert_eq!(replies.len(), 1);
    assert_eq!(replies[0].0, "01M38CTX_REQ_01");
    assert!(
        replies[0].1.contains("<context kind=\"channel\""),
        "Expected screened <context> reply, got: {}",
        replies[0].1
    );
    assert!(
        !replies[0].1.contains("screened=\"false\""),
        "Expected screened context (not unscreened), got: {}",
        replies[0].1
    );
    assert!(
        replies[0]
            .1
            .contains("Untrusted room question that must be screened"),
        "Expected room line in <context> reply, got: {}",
        replies[0].1
    );
    assert!(
        inbox
            .acks
            .lock()
            .unwrap()
            .contains(&"01M38GUARDVERDICT000001".to_string()),
        "Expected guard verdict delivery to be routed and acked on the inbox"
    );
    assert!(
        inbox.queue.lock().unwrap().is_empty(),
        "Expected acked deliveries to be removed from the shared inbox queue"
    );
}

// -------------------------------------------------------------------------------------------------
// Oracle 2 (XM2): The other half of Oracle 1 — a handler that awaits a second delivery while the
// first is unacked deadlocks against the pop-free real model unless M38's routing is present.
// Mutant: make the shared mock (`MockSvcInbox::poll_now`) pop on poll again => the unacked head
// delivery is popped on the first poll, the second poll returns the guard verdict even without
// M38's routing, and the handler completes instead of deadlocking => RED.
// -------------------------------------------------------------------------------------------------
#[tokio::test]
async fn oracle_2_unacked_head_delivery_deadlocks_awaiting_second_delivery_without_m38_routing() {
    let tmp = tempfile::tempdir().unwrap();
    let agent_sock_path = tmp.path().join("agent.sock");
    let agent_listener = UnixListener::bind(&agent_sock_path).unwrap();

    let inbox = MockSvcInbox::default();
    let inbox_for_guard = inbox.clone();

    let guard_counter = Arc::new(AtomicUsize::new(1));
    tokio::spawn(async move {
        while let Ok((stream, _)) = agent_listener.accept().await {
            let (r, mut w) = stream.into_split();
            let mut lines = tokio::io::BufReader::new(r).lines();
            let inbox_clone = inbox_for_guard.clone();
            let counter = Arc::clone(&guard_counter);
            tokio::spawn(async move {
                while let Ok(Some(line)) = lines.next_line().await {
                    if let Ok(val) = serde_json::from_str::<Value>(&line) {
                        if val.get("action").and_then(|a| a.as_str()) == Some("send") {
                            let n = counter.fetch_add(1, Ordering::SeqCst);
                            let guard_req_id = format!("01M38GUARDREQ2_{n:06}");
                            let resp = json!({
                                "status": "ok",
                                "message_id": guard_req_id,
                            });
                            let _ = w.write_all(format!("{resp}\n").as_bytes()).await;
                            let _ = w.flush().await;

                            let verdict_id = format!("01M38GUARDVERDICT2_{n:06}");
                            inbox_clone.push(SvcDelivery {
                                message_id: verdict_id,
                                from_name: "genie-guard@sini.test".to_string(),
                                text: json!({
                                    "verdict": "allow",
                                    "reason": "clean room context",
                                    "cleaned_text": "",
                                })
                                .to_string(),
                                envelope: String::new(),
                                origin: None,
                                in_reply_to: Some(guard_req_id),
                            });
                        }
                    }
                }
            });
        }
    });

    let mut cfg = test_config();
    cfg.xmsg_socket = tmp.path().to_path_buf();
    cfg.guard_timeout_secs = 1;
    let config = Arc::new(cfg);

    let matrix = Arc::new(MockMatrixClient::default());
    matrix.canned_room_history.lock().unwrap().insert(
        "!support:example.org".to_string(),
        vec![EventMessage {
            event_id: "$room_untrusted_2".to_string(),
            sender_mxid: "@stranger:example.org".to_string(),
            timestamp_ms: 1_000_000,
            body: "Untrusted room question that must be screened".to_string(),
            thread_root_id: None,
        }],
    );

    let store = Arc::new(Store::new_in_memory().unwrap());
    let xmsg = Arc::new(MockXmsgClient::default());

    let trigger = make_event(
        "$trig_2",
        "@trusted:example.org",
        "@genie:example.org help",
        true,
        None,
        1005,
    );
    let mock_guard = MockGuardClient::new();
    handle_incoming_event_with_guard(
        &trigger,
        &[],
        &config,
        matrix.as_ref(),
        xmsg.as_ref(),
        &mock_guard,
        &store,
        None,
    )
    .await
    .unwrap();

    // Push the first delivery (`01M38CTX_REQ_02`), whose handler awaits a second delivery
    // (`01M38GUARDVERDICT2_000001`) while `01M38CTX_REQ_02` remains unacked.
    inbox.push(SvcDelivery {
        message_id: "01M38CTX_REQ_02".to_string(),
        from_name: "claude · session-m38@sini.test".to_string(),
        text: json!({"context": {"scope": "room", "limit": 5}}).to_string(),
        envelope: String::new(),
        origin: None,
        in_reply_to: Some("01M38MSG000001".to_string()),
    });

    // Poll the first delivery on a consumer without M38's single-connection inbox routing:
    // the handler runs while `01M38CTX_REQ_02` is unacked, and a second reader (or reconnected
    // poll without XF6/M38 routing) polls `inbox` for the second delivery.
    let mut handler_inbox = inbox.clone();
    let d1 = handler_inbox
        .poll(1)
        .await
        .unwrap()
        .expect("must poll first delivery");
    assert_eq!(d1.message_id, "01M38CTX_REQ_02");

    // Without M38's routing + XF6 in-flight tracking, polling for the second delivery while the
    // first is unacked on a pop-free inbox re-serves the unacked head delivery `d1` until `d1`
    // is acked.
    let mut second_poller = inbox.new_connection().without_in_flight();
    let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel::<()>();
    let poller_handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut stop_rx => break,
                res = second_poller.poll(0) => {
                    if let Ok(Some(d)) = res {
                        if d.message_id != "01M38CTX_REQ_02" {
                            if let Some(ref reply_to) = d.in_reply_to {
                                if route_guard_reply(reply_to, &d.text).await {
                                    let _ = second_poller.ack(&d.message_id).await;
                                    break;
                                }
                            }
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }
    });

    let completed = tokio::time::timeout(
        Duration::from_millis(350),
        handle_inbox_delivery_with_xmsg(
            &d1,
            &config,
            matrix.as_ref(),
            store.as_ref(),
            xmsg.as_ref(),
            &mut handler_inbox,
            1010,
        ),
    )
    .await;

    let _ = stop_tx.send(());
    let _ = poller_handle.await;

    assert!(
        completed.is_err(),
        "Handler awaiting a second delivery while the first is unacked must deadlock against the pop-free inbox model without M38 routing (did MockSvcInbox pop on poll?)"
    );
    assert!(
        xmsg.replies.lock().unwrap().is_empty(),
        "Deadlocked context handler must not have produced a <context> reply"
    );
}

// -------------------------------------------------------------------------------------------------
// Oracle 3: When `guard_retry_budget` is exhausted under `GuardFailureMode::Closed`:
// - on `context` (and `resync`), the requester receives `{"error":"screening unavailable; try again later"}`,
//   the delivery is acked, and the owner receives a DM;
// - on a mention, the trigger gets a `⚠️` reaction, the owner receives a DM, and retrying stops.
// -------------------------------------------------------------------------------------------------
#[tokio::test]
async fn oracle_3_fail_closed_budget_exhaustion_replies_on_context_and_reacts_on_mention() {
    let config = test_config(); // guard_retry_budget = 3, Closed
    let matrix = MockMatrixClient::default();
    let store = Store::new_in_memory().unwrap();
    let xmsg = MockXmsgClient::default();
    let guard = MockGuardClient::new();

    // Seed untrusted room & thread history so context/resync require guard screening
    let untrusted_room_msg = EventMessage {
        event_id: "$untrusted_room_1".to_string(),
        sender_mxid: "@untrusted:example.org".to_string(),
        timestamp_ms: 1_000_000,
        body: "Untrusted room line".to_string(),
        thread_root_id: None,
    };
    let untrusted_thread_msg = EventMessage {
        event_id: "$untrusted_thread_1".to_string(),
        sender_mxid: "@untrusted:example.org".to_string(),
        timestamp_ms: 1_000_000,
        body: "Untrusted thread line".to_string(),
        thread_root_id: Some("$trig_ok".to_string()),
    };
    matrix
        .canned_room_history
        .lock()
        .unwrap()
        .insert("!support:example.org".to_string(), vec![untrusted_room_msg]);
    matrix
        .canned_thread_relations
        .lock()
        .unwrap()
        .insert("$trig_ok".to_string(), vec![untrusted_thread_msg]);

    // 1. Seed a valid forward ($trig_ok -> 01M38MSG000001)
    let trig_ok = make_event(
        "$trig_ok",
        "@trusted:example.org",
        "@genie:example.org hello",
        true,
        None,
        1005,
    );
    handle_incoming_event_with_guard(&trig_ok, &[], &config, &matrix, &xmsg, &guard, &store, None)
        .await
        .unwrap();

    // 2. Test `context` request exhausting guard_retry_budget (3 attempts)
    let mut inbox = MockSvcInbox::default();
    let ctx_delivery = SvcDelivery {
        message_id: "01M38CTX_FAIL".to_string(),
        from_name: "claude · session-m38@sini.test".to_string(),
        text: json!({"context": {"scope": "room", "limit": 5}}).to_string(),
        envelope: String::new(),
        origin: None,
        in_reply_to: Some("01M38MSG000001".to_string()),
    };

    for attempt in 1..=2 {
        guard.add_error("guard timeout");
        let res = handle_inbox_delivery_with_guard(
            &ctx_delivery,
            &config,
            &matrix,
            &store,
            &xmsg,
            &guard,
            &mut inbox,
            1010,
        )
        .await
        .unwrap();
        assert_eq!(
            res,
            BotOutcome::GuardFailed,
            "Attempt {attempt} should return GuardFailed to retry"
        );
        assert!(
            inbox.acks.lock().unwrap().is_empty(),
            "Attempt {attempt} must not ack delivery before budget exhausted"
        );
        assert!(
            xmsg.replies.lock().unwrap().is_empty(),
            "Attempt {attempt} must not reply before budget exhausted"
        );
    }

    // Attempt 3 exhausts guard_retry_budget: must DM owner, reply error JSON, ack delivery, and clear retries
    guard.add_error("guard timeout");
    let res3 = handle_inbox_delivery_with_guard(
        &ctx_delivery,
        &config,
        &matrix,
        &store,
        &xmsg,
        &guard,
        &mut inbox,
        1010,
    )
    .await
    .unwrap();
    assert_eq!(res3, BotOutcome::GuardFailed);
    assert_eq!(
        inbox.acks.lock().unwrap().as_slice(),
        &["01M38CTX_FAIL".to_string()],
        "Context delivery must be acked when guard_retry_budget is exhausted"
    );
    let replies = xmsg.replies.lock().unwrap().clone();
    assert_eq!(replies.len(), 1);
    assert_eq!(replies[0].0, "01M38CTX_FAIL");
    assert_eq!(
        serde_json::from_str::<Value>(&replies[0].1).unwrap(),
        json!({"error": "screening unavailable; try again later"})
    );
    assert_eq!(
        store.get_guard_retries("01M38CTX_FAIL").unwrap(),
        None,
        "guard_retries must be cleared after budget exhaustion"
    );
    assert_eq!(
        matrix.sent_dms.lock().unwrap().len(),
        1,
        "Owner must receive a fail-closed DM"
    );

    // 3. Test `resync` request exhausting guard_retry_budget (3 attempts)
    xmsg.replies.lock().unwrap().clear();
    inbox.acks.lock().unwrap().clear();
    let resync_delivery = SvcDelivery {
        message_id: "01M38RESYNC_FAIL".to_string(),
        from_name: "claude · session-m38@sini.test".to_string(),
        text: json!({"resync": true}).to_string(),
        envelope: String::new(),
        origin: None,
        in_reply_to: Some("01M38MSG000001".to_string()),
    };
    for _ in 1..=2 {
        guard.add_error("guard timeout");
        let out = handle_inbox_delivery_with_guard(
            &resync_delivery,
            &config,
            &matrix,
            &store,
            &xmsg,
            &guard,
            &mut inbox,
            1015,
        )
        .await
        .unwrap();
        assert_eq!(out, BotOutcome::GuardFailed);
    }
    guard.add_error("guard timeout");
    let out_resync_3 = handle_inbox_delivery_with_guard(
        &resync_delivery,
        &config,
        &matrix,
        &store,
        &xmsg,
        &guard,
        &mut inbox,
        1015,
    )
    .await
    .unwrap();
    assert_eq!(out_resync_3, BotOutcome::GuardFailed);
    assert_eq!(
        inbox.acks.lock().unwrap().as_slice(),
        &["01M38RESYNC_FAIL".to_string()]
    );
    let resync_replies = xmsg.replies.lock().unwrap().clone();
    assert_eq!(resync_replies.len(), 1);
    assert_eq!(
        serde_json::from_str::<Value>(&resync_replies[0].1).unwrap(),
        json!({"error": "screening unavailable; try again later"})
    );

    // 4. Test mention exhausting guard_retry_budget: sends owner DM, reacts with ⚠️, and stops retrying
    matrix.sent_dms.lock().unwrap().clear();
    matrix.sent_reactions.lock().unwrap().clear();
    let bad_mention = make_event(
        "$trig_mention_fail",
        "@untrusted:example.org",
        "@genie:example.org help me",
        true,
        None,
        2000,
    );

    for attempt in 1..=2 {
        guard.add_error("guard timeout");
        let out = handle_incoming_event_with_guard(
            &bad_mention,
            &[],
            &config,
            &matrix,
            &xmsg,
            &guard,
            &store,
            None,
        )
        .await
        .unwrap();
        assert_eq!(out, BotOutcome::GuardFailed);
        assert!(
            matrix.sent_reactions.lock().unwrap().is_empty(),
            "Attempt {attempt} must not react with ⚠️ before budget exhausted"
        );
        assert!(!store
            .has_guard_failure_dm_sent("$trig_mention_fail")
            .unwrap());
    }

    // Attempt 3: reaches guard_retry_budget
    guard.add_error("guard timeout");
    let calls_before_3 = guard.calls.lock().unwrap().len();
    let out3 = handle_incoming_event_with_guard(
        &bad_mention,
        &[],
        &config,
        &matrix,
        &xmsg,
        &guard,
        &store,
        None,
    )
    .await
    .unwrap();
    assert_eq!(out3, BotOutcome::GuardFailed);
    assert_eq!(guard.calls.lock().unwrap().len(), calls_before_3 + 1);
    assert!(store
        .has_guard_failure_dm_sent("$trig_mention_fail")
        .unwrap());
    assert_eq!(matrix.sent_dms.lock().unwrap().len(), 1);
    {
        let reactions = matrix.sent_reactions.lock().unwrap();
        assert_eq!(reactions.len(), 1);
        assert_eq!(reactions[0].room_id, "!support:example.org");
        assert_eq!(reactions[0].event_id, "$trig_mention_fail");
        assert_eq!(reactions[0].key, "⚠️");
    }

    // Attempt 4: subsequent call must NOT call guard again or send duplicate DM/reaction
    guard.add_error("guard timeout");
    let calls_before_4 = guard.calls.lock().unwrap().len();
    let out4 = handle_incoming_event_with_guard(
        &bad_mention,
        &[],
        &config,
        &matrix,
        &xmsg,
        &guard,
        &store,
        None,
    )
    .await
    .unwrap();
    assert_eq!(out4, BotOutcome::GuardFailed);
    assert_eq!(
        guard.calls.lock().unwrap().len(),
        calls_before_4,
        "Must stop calling guard once guard_retry_budget is exhausted"
    );
    assert_eq!(matrix.sent_dms.lock().unwrap().len(), 1);
    assert_eq!(matrix.sent_reactions.lock().unwrap().len(), 1);
}

// -------------------------------------------------------------------------------------------------
// Oracle 4: A stuck delivery in thread A does not delay a delivery in thread B.
// Mutant: serial inbox loop => RED.
// -------------------------------------------------------------------------------------------------
struct BlockingThreadAMatrixClient {
    inner: MockMatrixClient,
    thread_a_root: String,
    unblock_thread_a: Arc<tokio::sync::Notify>,
    thread_a_entered: Arc<tokio::sync::Notify>,
}

#[async_trait]
impl MatrixClient for BlockingThreadAMatrixClient {
    async fn send_notice(
        &self,
        room_id: &str,
        thread_root_id: Option<&str>,
        body: &str,
        mention_user: Option<&str>,
    ) -> Result<String, AppError> {
        if thread_root_id == Some(self.thread_a_root.as_str()) {
            self.thread_a_entered.notify_waiters();
            self.unblock_thread_a.notified().await;
        }
        self.inner
            .send_notice(room_id, thread_root_id, body, mention_user)
            .await
    }

    async fn send_dm(&self, user_id: &str, body: &str) -> Result<String, AppError> {
        self.inner.send_dm(user_id, body).await
    }

    async fn fetch_history(
        &self,
        room_id: &str,
        limit: usize,
    ) -> Result<Vec<EventMessage>, AppError> {
        self.inner.fetch_history(room_id, limit).await
    }

    async fn send_reaction(
        &self,
        room_id: &str,
        event_id: &str,
        key: &str,
    ) -> Result<String, AppError> {
        self.inner.send_reaction(room_id, event_id, key).await
    }

    async fn redact_event(
        &self,
        room_id: &str,
        event_id: &str,
        reason: Option<&str>,
    ) -> Result<(), AppError> {
        self.inner.redact_event(room_id, event_id, reason).await
    }
}

#[tokio::test]
async fn oracle_4_stuck_delivery_in_thread_a_does_not_delay_thread_b() {
    let config = Arc::new(test_config());
    let unblock_a = Arc::new(tokio::sync::Notify::new());
    let entered_a = Arc::new(tokio::sync::Notify::new());
    let matrix = Arc::new(BlockingThreadAMatrixClient {
        inner: MockMatrixClient::default(),
        thread_a_root: "$thread_a".to_string(),
        unblock_thread_a: Arc::clone(&unblock_a),
        thread_a_entered: Arc::clone(&entered_a),
    });
    let store = Arc::new(Store::new_in_memory().unwrap());
    let xmsg = Arc::new(MockXmsgClient::default());
    let guard = Arc::new(MockGuardClient::new());

    // Forward mention in thread A -> 01M38MSG000001
    let trig_a = make_event(
        "$thread_a",
        "@trusted:example.org",
        "@genie:example.org question A",
        true,
        None,
        1000,
    );
    handle_incoming_event_with_guard(
        &trig_a,
        &[],
        &config,
        matrix.as_ref(),
        xmsg.as_ref(),
        guard.as_ref(),
        &store,
        None,
    )
    .await
    .unwrap();

    // Forward mention in thread B -> 01M38MSG000002
    let trig_b = make_event(
        "$thread_b",
        "@trusted:example.org",
        "@genie:example.org question B",
        true,
        None,
        1001,
    );
    handle_incoming_event_with_guard(
        &trig_b,
        &[],
        &config,
        matrix.as_ref(),
        xmsg.as_ref(),
        guard.as_ref(),
        &store,
        None,
    )
    .await
    .unwrap();

    let inbox = MockSvcInbox::default();
    // Queue stuck delivery for thread A first, followed by fast delivery for thread B
    inbox.push(SvcDelivery {
        message_id: "01M38_DELIV_A".to_string(),
        from_name: "claude · session-m38@sini.test".to_string(),
        text: "slow answer A".to_string(),
        envelope: String::new(),
        origin: None,
        in_reply_to: Some("01M38MSG000001".to_string()),
    });
    inbox.push(SvcDelivery {
        message_id: "01M38_DELIV_B".to_string(),
        from_name: "claude · session-m38@sini.test".to_string(),
        text: "fast answer B".to_string(),
        envelope: String::new(),
        origin: None,
        in_reply_to: Some("01M38MSG000002".to_string()),
    });

    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let loop_handle = tokio::spawn({
        let inbox = inbox.clone();
        let config = Arc::clone(&config);
        let matrix: Arc<dyn MatrixClient> = matrix.clone();
        let store = Arc::clone(&store);
        let xmsg: Arc<dyn XmsgClient> = xmsg.clone();
        async move {
            let _ = run_inbox_loop(inbox, config, matrix, store, xmsg, shutdown_rx).await;
        }
    });

    // Wait for thread A to enter send_notice (where it blocks)
    tokio::time::timeout(Duration::from_millis(500), entered_a.notified())
        .await
        .expect("Thread A should start processing");

    // While thread A is still blocked, thread B must complete and ack within 500ms!
    let b_completed = tokio::time::timeout(Duration::from_millis(500), async {
        loop {
            let acked_b = inbox
                .acks
                .lock()
                .unwrap()
                .contains(&"01M38_DELIV_B".to_string());
            let posted_b = matrix.inner.sent_notices.lock().unwrap().iter().any(|r| {
                r.thread_root_id.as_deref() == Some("$thread_b") && r.body.contains("fast answer B")
            });
            if acked_b && posted_b {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;

    assert!(
        b_completed.is_ok(),
        "Thread B delivery was blocked by stuck delivery in Thread A!"
    );
    assert!(
        !inbox
            .acks
            .lock()
            .unwrap()
            .contains(&"01M38_DELIV_A".to_string()),
        "Thread A should still be blocked while Thread B completed"
    );

    // Unblock thread A and shut down cleanly
    unblock_a.notify_waiters();
    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(Duration::from_millis(200), loop_handle).await;
}

// -------------------------------------------------------------------------------------------------
// Oracle 5: When >= 17 concurrent context requests in distinct threads saturate the 16-permit
// handler semaphore while waiting for guard verdicts, the inbox loop's verdict fast path still
// routes every guard verdict and all 17 context requests complete.
// Mutant: `if false && route_guard_reply(...)` at src/bot.rs:3302 => verdicts routed through
// worker tasks starve behind the 16 blocked context handlers and time out (RED).
// -------------------------------------------------------------------------------------------------
#[tokio::test]
async fn oracle_5_seventeen_concurrent_context_requests_complete_via_verdict_fast_path() {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::UnixListener;

    let tmp = tempfile::tempdir().unwrap();
    let agent_sock = tmp.path().join("agent.sock");
    let listener = UnixListener::bind(&agent_sock).unwrap();

    let inbox = MockSvcInbox::default();
    let inbox_clone = inbox.clone();

    // Stub agent.sock: accept guard sends from UnixGuardClient::check_guard, reply with a
    // message_id, and push the guard verdict delivery into `inbox`.
    tokio::spawn(async move {
        let mut seq = 0u32;
        while let Ok((mut stream, _)) = listener.accept().await {
            let inbox_clone = inbox_clone.clone();
            seq += 1;
            let n = seq;
            tokio::spawn(async move {
                let (r, mut w) = stream.split();
                let mut reader = BufReader::new(r);
                let mut line = String::new();
                while reader.read_line(&mut line).await.unwrap_or(0) > 0 {
                    if let Ok(val) = serde_json::from_str::<serde_json::Value>(line.trim()) {
                        if val.get("action").and_then(|v| v.as_str()) == Some("send") {
                            let guard_req_id = format!("01M38GUARDREQ5_{n:06}");
                            let resp = json!({
                                "status": "ok",
                                "message_id": guard_req_id,
                            });
                            let _ = w.write_all(format!("{resp}\n").as_bytes()).await;
                            let _ = w.flush().await;

                            let verdict_id = format!("01M38GUARDVERDICT5_{n:06}");
                            inbox_clone.push(SvcDelivery {
                                message_id: verdict_id,
                                from_name: "genie-guard@sini.test".to_string(),
                                text: json!({
                                    "verdict": "allow",
                                    "reason": "clean room context",
                                    "cleaned_text": "",
                                })
                                .to_string(),
                                envelope: String::new(),
                                origin: None,
                                in_reply_to: Some(guard_req_id),
                            });
                        }
                    }
                    line.clear();
                }
            });
        }
    });

    let mut cfg = test_config();
    cfg.xmsg_socket = tmp.path().to_path_buf();
    cfg.context_rate_limit_count = 100;
    cfg.rate_limit_count = 100;
    let config = Arc::new(cfg);

    let matrix = Arc::new(MockMatrixClient::default());
    matrix.canned_room_history.lock().unwrap().insert(
        "!support:example.org".to_string(),
        vec![EventMessage {
            event_id: "$room_untrusted_5".to_string(),
            sender_mxid: "@stranger:example.org".to_string(),
            timestamp_ms: 1_000_000,
            body: "Untrusted room question that must be screened".to_string(),
            thread_root_id: None,
        }],
    );

    let store = Arc::new(Store::new_in_memory().unwrap());
    let xmsg = Arc::new(MockXmsgClient::default());
    let mock_guard = MockGuardClient::new();

    const N_REQUESTS: usize = 17;
    for i in 1..=N_REQUESTS {
        let trig = make_event(
            &format!("$trig_5_{i}"),
            "@trusted:example.org",
            &format!("@genie:example.org help {i}"),
            true,
            None,
            1000 + i as i64,
        );
        handle_incoming_event_with_guard(
            &trig,
            &[],
            &config,
            matrix.as_ref(),
            xmsg.as_ref(),
            &mock_guard,
            &store,
            None,
        )
        .await
        .unwrap();

        inbox.push(SvcDelivery {
            message_id: format!("01M38CTX5_REQ_{i:02}"),
            from_name: "claude · session-m38@sini.test".to_string(),
            text: json!({"context": {"scope": "room", "limit": 5}}).to_string(),
            envelope: String::new(),
            origin: None,
            in_reply_to: Some(format!("01M38MSG{i:06}")),
        });
    }

    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let loop_handle = tokio::spawn({
        let inbox = inbox.clone();
        let config = Arc::clone(&config);
        let matrix: Arc<dyn MatrixClient> = matrix.clone();
        let store = Arc::clone(&store);
        let xmsg: Arc<dyn XmsgClient> = xmsg.clone();
        async move {
            let _ = run_inbox_loop(inbox, config, matrix, store, xmsg, shutdown_rx).await;
        }
    });

    let completed = tokio::time::timeout(Duration::from_millis(2500), async {
        loop {
            let replies_len = xmsg.replies.lock().unwrap().len();
            let acks = inbox.acks.lock().unwrap().clone();
            let all_req_acked =
                (1..=N_REQUESTS).all(|i| acks.contains(&format!("01M38CTX5_REQ_{i:02}")));
            if replies_len == N_REQUESTS && all_req_acked {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;

    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(Duration::from_millis(200), loop_handle).await;

    assert!(
        completed.is_ok(),
        "M38 Oracle 5 timed out — 17 concurrent context requests exhausted the 16-permit worker pool and verdicts could not be routed without the inbox fast path"
    );

    let replies = xmsg.replies.lock().unwrap().clone();
    assert_eq!(replies.len(), N_REQUESTS);
    for (_, body) in &replies {
        assert!(
            body.contains("<context kind=\"channel\""),
            "Expected screened <context> reply, got: {body}"
        );
    }
}
