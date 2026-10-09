use matrix_xmsg::bot::{
    handle_inbox_delivery, handle_incoming_event, sweep_answer_timeouts, BotOutcome,
    IncomingMatrixEvent,
};
use matrix_xmsg::config::{Admission, Config};
use matrix_xmsg::context::EventMessage;
use matrix_xmsg::error::AppError;
use matrix_xmsg::matrix::{MatrixClient, MockMatrixClient};
use matrix_xmsg::store::Store;
use matrix_xmsg::xmsg::{create_xmsg_client, register_svc, SvcInbox};
use serde_json::Value;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::net::UnixListener;
use tokio::sync::Mutex;

pub struct MockXmsgSockets {
    pub dir: PathBuf,
    pub recorded_sends: Arc<Mutex<Vec<Value>>>,
    pub recorded_acks: Arc<Mutex<Vec<String>>>,
    pub queued_deliveries: Arc<Mutex<VecDeque<Value>>>,
    pub send_counter: Arc<AtomicUsize>,
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl MockXmsgSockets {
    pub async fn start(dir: &Path) -> Self {
        let register_sock_path = dir.join("register.sock");
        let agent_sock_path = dir.join("agent.sock");
        let _ = std::fs::remove_file(&register_sock_path);
        let _ = std::fs::remove_file(&agent_sock_path);

        let reg_listener = UnixListener::bind(&register_sock_path).expect("bind register.sock");
        let agent_listener = UnixListener::bind(&agent_sock_path).expect("bind agent.sock");

        let recorded_sends = Arc::new(Mutex::new(Vec::new()));
        let recorded_acks = Arc::new(Mutex::new(Vec::new()));
        let queued_deliveries = Arc::new(Mutex::new(VecDeque::new()));
        let send_counter = Arc::new(AtomicUsize::new(1));

        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        let sends_clone = Arc::clone(&recorded_sends);
        let acks_clone = Arc::clone(&recorded_acks);
        let deliveries_clone = Arc::clone(&queued_deliveries);
        let counter_clone = Arc::clone(&send_counter);

        tokio::spawn(async move {
            tokio::select! {
                _ = &mut shutdown_rx => {}
                _ = async {
                    let agent_task = {
                        let sends = Arc::clone(&sends_clone);
                        let counter = Arc::clone(&counter_clone);
                        tokio::spawn(async move {
                            while let Ok((stream, _)) = agent_listener.accept().await {
                                let (r, mut w) = stream.into_split();
                                let mut lines = tokio::io::BufReader::new(r).lines();
                                let sends = Arc::clone(&sends);
                                let counter = Arc::clone(&counter);
                                tokio::spawn(async move {
                                    while let Ok(Some(line)) = lines.next_line().await {
                                        if let Ok(val) = serde_json::from_str::<Value>(&line) {
                                            sends.lock().await.push(val.clone());
                                            let action = val.get("action").and_then(|a| a.as_str()).unwrap_or("");
                                            if action == "send" {
                                                let idx = counter.fetch_add(1, Ordering::SeqCst);
                                                let msg_id = format!("01M13MSG{:016X}", idx);
                                                let resp = serde_json::json!({
                                                    "status": "ok",
                                                    "message_id": msg_id,
                                                });
                                                let _ = w.write_all(format!("{resp}\n").as_bytes()).await;
                                                let _ = w.flush().await;
                                            }
                                        }
                                    }
                                });
                            }
                        })
                    };

                    let reg_task = {
                        let acks = Arc::clone(&acks_clone);
                        let deliveries = Arc::clone(&deliveries_clone);
                        tokio::spawn(async move {
                            while let Ok((stream, _)) = reg_listener.accept().await {
                                let (r, mut w) = stream.into_split();
                                let mut lines = tokio::io::BufReader::new(r).lines();
                                let acks = Arc::clone(&acks);
                                let deliveries = Arc::clone(&deliveries);
                                tokio::spawn(async move {
                                    // First line is register
                                    if let Ok(Some(line)) = lines.next_line().await {
                                        if let Ok(val) = serde_json::from_str::<Value>(&line) {
                                            if val.get("harness").and_then(|h| h.as_str()) == Some("svc") {
                                                let resp = serde_json::json!({
                                                    "status": "ok",
                                                    "sessionId": "matrix-xmsg",
                                                });
                                                let _ = w.write_all(format!("{resp}\n").as_bytes()).await;
                                                let _ = w.flush().await;
                                            }
                                        }
                                    }

                                    // Next lines are poll / ack
                                    while let Ok(Some(line)) = lines.next_line().await {
                                        if let Ok(val) = serde_json::from_str::<Value>(&line) {
                                            let action = val.get("action").and_then(|a| a.as_str()).unwrap_or("");
                                            if action == "poll" {
                                                let delivery = deliveries.lock().await.pop_front();
                                                let resp = match delivery {
                                                    Some(d) => d,
                                                    None => serde_json::json!({
                                                        "status": "ok",
                                                        "action": "timeout",
                                                    }),
                                                };
                                                let _ = w.write_all(format!("{resp}\n").as_bytes()).await;
                                                let _ = w.flush().await;
                                            } else if action == "ack" {
                                                if let Some(msg_id) = val.get("messageId").and_then(|m| m.as_str()) {
                                                    acks.lock().await.push(msg_id.to_string());
                                                }
                                                let resp = serde_json::json!({
                                                    "status": "ok",
                                                });
                                                let _ = w.write_all(format!("{resp}\n").as_bytes()).await;
                                                let _ = w.flush().await;
                                            }
                                        }
                                    }
                                });
                            }
                        })
                    };

                    let _ = tokio::join!(agent_task, reg_task);
                } => {}
            }
        });

        Self {
            dir: dir.to_path_buf(),
            recorded_sends,
            recorded_acks,
            queued_deliveries,
            send_counter,
            shutdown_tx: Some(shutdown_tx),
        }
    }

    pub async fn queue_delivery(&self, delivery: Value) {
        self.queued_deliveries.lock().await.push_back(delivery);
    }
}

impl Drop for MockXmsgSockets {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        let _ = std::fs::remove_file(self.dir.join("register.sock"));
        let _ = std::fs::remove_file(self.dir.join("agent.sock"));
    }
}

fn test_config(socket_dir: &Path) -> Config {
    Config {
        homeserver_url: "https://matrix.example.org".to_string(),
        bot_mxid: "@genie:example.org".to_string(),
        access_token_file: PathBuf::from("/nonexistent/token"),
        rooms: vec!["!support:example.org".to_string()],
        trusted_mxids: vec![
            "@trusted:example.org".to_string(),
            "@alice:example.org".to_string(),
        ],
        owner_mxid: "@owner:example.org".to_string(),
        admission: Admission::Trusted,
        xmsg_socket: socket_dir.to_path_buf(),
        expert_ref: "claude".to_string(),
        history_n: 5,
        history_byte_cap: 1024,
        rate_limit_count: 5,
        rate_limit_window_secs: 60,
        size_cap_bytes: 2048,
        answer_timeout_secs: 10,
        session_live_secs: 3600,
        db_path: PathBuf::from(":memory:"),
    }
}

struct FailingMatrixClient {
    pub inner: MockMatrixClient,
    pub fail_notice: AtomicBool,
}

impl FailingMatrixClient {
    fn new() -> Self {
        Self {
            inner: MockMatrixClient::default(),
            fail_notice: AtomicBool::new(false),
        }
    }
}

#[async_trait::async_trait]
impl MatrixClient for FailingMatrixClient {
    async fn send_notice(
        &self,
        room_id: &str,
        thread_root_id: Option<&str>,
        body: &str,
        mention_user: Option<&str>,
    ) -> Result<String, AppError> {
        if self.fail_notice.load(Ordering::SeqCst) {
            return Err(AppError::Matrix(
                "simulated notice post failure".to_string(),
            ));
        }
        self.inner
            .send_notice(room_id, thread_root_id, body, mention_user)
            .await
    }

    async fn send_dm(&self, user_id: &str, body: &str) -> Result<String, AppError> {
        self.inner.send_dm(user_id, body).await
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

    async fn fetch_history(
        &self,
        room_id: &str,
        limit: usize,
    ) -> Result<Vec<EventMessage>, AppError> {
        self.inner.fetch_history(room_id, limit).await
    }

    async fn fetch_event(
        &self,
        room_id: &str,
        event_id: &str,
    ) -> Result<Option<EventMessage>, AppError> {
        self.inner.fetch_event(room_id, event_id).await
    }
}

// ---------------------------------------------------------------------------
// Oracle 1:
// Two replies to one forwarded message => two posts in the thread, in order.
// Mutant: post only the first => RED.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_1_two_replies_to_one_forward_yield_two_posts_in_order() {
    let tmp = tempfile::tempdir().unwrap();
    let server = MockXmsgSockets::start(tmp.path()).await;
    let config = test_config(tmp.path());
    let xmsg = create_xmsg_client(&config);
    let matrix = MockMatrixClient::default();
    let store = Store::new_in_memory().unwrap();

    let event = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$ev_ask1".to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        body: "@genie Tell me about thread replies".to_string(),
        formatted_body: None,
        mentions: Some(vec!["@genie:example.org".to_string()]),
        timestamp_ms: 1000000,
        thread_root_id: None,
        replaces_event_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
    };

    let outcome = handle_incoming_event(&event, &[], &config, &matrix, xmsg.as_ref(), &store)
        .await
        .expect("handle_incoming_event must succeed");
    assert_eq!(outcome, BotOutcome::Forwarded);

    let sends = server.recorded_sends.lock().await.clone();
    assert_eq!(sends.len(), 1);
    let fwd_msg_id = "01M13MSG0000000000000001";

    let mut inbox = register_svc(&config.xmsg_register_socket(), "matrix-xmsg")
        .await
        .expect("register_svc must succeed");

    // Reply 1
    let delivery1 = serde_json::json!({
        "status": "ok",
        "action": "deliver",
        "messageId": "01M13REPLY0000001",
        "fromName": "claude",
        "text": "First answer part",
        "envelope": format!("[xmsg] reply to message_id={fwd_msg_id} — message_id=01M13REPLY0000001\nFirst answer part")
    });
    server.queue_delivery(delivery1).await;
    let d1 = inbox.poll(1).await.unwrap().unwrap();
    let res1 = handle_inbox_delivery(&d1, &config, &matrix, &store, &mut inbox, 1001).await;
    assert_eq!(res1.unwrap(), BotOutcome::Replied);

    // Reply 2
    let delivery2 = serde_json::json!({
        "status": "ok",
        "action": "deliver",
        "messageId": "01M13REPLY0000002",
        "fromName": "claude",
        "text": "Second follow-up answer",
        "envelope": format!("[xmsg] reply to message_id={fwd_msg_id} — message_id=01M13REPLY0000002\nSecond follow-up answer")
    });
    server.queue_delivery(delivery2).await;
    let d2 = inbox.poll(1).await.unwrap().unwrap();
    let res2 = handle_inbox_delivery(&d2, &config, &matrix, &store, &mut inbox, 1002).await;
    assert_eq!(res2.unwrap(), BotOutcome::Replied);

    // Assert: two posts in the thread, in order
    let notices = matrix.sent_notices.lock().unwrap().clone();
    assert_eq!(
        notices.len(),
        2,
        "Two replies to one forward must yield two posts in the room"
    );
    assert!(
        notices[0].body.contains("First answer part"),
        "Notice 0 must contain first answer"
    );
    assert!(
        notices[1].body.contains("Second follow-up answer"),
        "Notice 1 must contain second answer"
    );

    // Both deliveries acked
    let acks = server.recorded_acks.lock().await.clone();
    assert_eq!(
        acks,
        vec![
            "01M13REPLY0000001".to_string(),
            "01M13REPLY0000002".to_string()
        ]
    );
}

// ---------------------------------------------------------------------------
// Oracle 2:
// A reply arriving long after any former deadline (clock advanced past 1 h) => posted.
// Mutant: restore a deadline => RED.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_2_reply_arriving_past_former_deadline_is_posted() {
    let tmp = tempfile::tempdir().unwrap();
    let server = MockXmsgSockets::start(tmp.path()).await;
    let config = test_config(tmp.path());
    let xmsg = create_xmsg_client(&config);
    let matrix = MockMatrixClient::default();
    let store = Store::new_in_memory().unwrap();

    let event = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$ev_ask2".to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        body: "@genie Will you reply eventually?".to_string(),
        formatted_body: None,
        mentions: Some(vec!["@genie:example.org".to_string()]),
        timestamp_ms: 1000000, // t=1000s
        thread_root_id: None,
        replaces_event_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
    };

    handle_incoming_event(&event, &[], &config, &matrix, xmsg.as_ref(), &store)
        .await
        .expect("handle_incoming_event must succeed");

    let fwd_msg_id = "01M13MSG0000000000000001";
    let mut inbox = register_svc(&config.xmsg_register_socket(), "matrix-xmsg")
        .await
        .expect("register_svc must succeed");

    // Clock advanced past 1 hour: t = 1000 + 4000 = 5000s
    let late_time_secs = 5000;
    server
        .queue_delivery(serde_json::json!({
            "status": "ok",
            "action": "deliver",
            "messageId": "01M13LATEReply",
            "fromName": "claude",
            "text": "Answer arriving after 4000 seconds with no deadline",
            "envelope": format!("[xmsg] reply to message_id={fwd_msg_id} — message_id=01M13LATEReply\nAnswer arriving after 4000 seconds with no deadline")
        }))
        .await;

    let delivery = inbox.poll(1).await.unwrap().unwrap();
    let res = handle_inbox_delivery(
        &delivery,
        &config,
        &matrix,
        &store,
        &mut inbox,
        late_time_secs,
    )
    .await;
    assert_eq!(
        res.unwrap(),
        BotOutcome::Replied,
        "Late reply must be accepted and posted with no deadline"
    );

    let notices = matrix.sent_notices.lock().unwrap().clone();
    assert_eq!(
        notices.len(),
        1,
        "Expected notice to be posted despite 4000s elapsed"
    );
    assert!(notices[0]
        .body
        .contains("Answer arriving after 4000 seconds with no deadline"));
}

// ---------------------------------------------------------------------------
// Oracle 3:
// A reply whose post fails is not acked and is posted on re-delivery.
// Mutant: ack before posting => RED.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_3_reply_post_failure_not_acked_and_posted_on_redelivery() {
    let tmp = tempfile::tempdir().unwrap();
    let server = MockXmsgSockets::start(tmp.path()).await;
    let config = test_config(tmp.path());
    let xmsg = create_xmsg_client(&config);
    let matrix = FailingMatrixClient::new();
    let store = Store::new_in_memory().unwrap();

    let event = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$ev_ask3".to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        body: "@genie Question before failure".to_string(),
        formatted_body: None,
        mentions: Some(vec!["@genie:example.org".to_string()]),
        timestamp_ms: 1000000,
        thread_root_id: None,
        replaces_event_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
    };

    handle_incoming_event(&event, &[], &config, &matrix, xmsg.as_ref(), &store)
        .await
        .expect("handle_incoming_event must succeed");

    let fwd_msg_id = "01M13MSG0000000000000001";
    let mut inbox = register_svc(&config.xmsg_register_socket(), "matrix-xmsg")
        .await
        .expect("register_svc must succeed");

    let delivery_json = serde_json::json!({
        "status": "ok",
        "action": "deliver",
        "messageId": "01M13RETRYMSG",
        "fromName": "claude",
        "text": "Answer to be retried",
        "envelope": format!("[xmsg] reply to message_id={fwd_msg_id} — message_id=01M13RETRYMSG\nAnswer to be retried")
    });

    // Attempt 1: Matrix send_notice fails
    matrix.fail_notice.store(true, Ordering::SeqCst);
    server.queue_delivery(delivery_json.clone()).await;
    let d1 = inbox.poll(1).await.unwrap().unwrap();
    let res1 = handle_inbox_delivery(&d1, &config, &matrix, &store, &mut inbox, 1001).await;
    assert!(
        res1.is_err(),
        "handle_inbox_delivery must return error when Matrix post fails"
    );

    // Verify NOT acked on failure
    let acks_after_fail = server.recorded_acks.lock().await.clone();
    assert!(
        acks_after_fail.is_empty(),
        "Failed delivery must NOT be acknowledged"
    );

    // Attempt 2: Re-delivery succeeds
    matrix.fail_notice.store(false, Ordering::SeqCst);
    server.queue_delivery(delivery_json).await;
    let d2 = inbox.poll(1).await.unwrap().unwrap();
    let res2 = handle_inbox_delivery(&d2, &config, &matrix, &store, &mut inbox, 1002).await;
    assert_eq!(
        res2.unwrap(),
        BotOutcome::Replied,
        "Re-delivered reply must succeed"
    );

    // Verify now posted and acked
    let notices = matrix.inner.sent_notices.lock().unwrap().clone();
    assert_eq!(notices.len(), 1, "Must post exactly one notice on retry");
    assert!(notices[0].body.contains("Answer to be retried"));

    let acks_after_success = server.recorded_acks.lock().await.clone();
    assert_eq!(
        acks_after_success,
        vec!["01M13RETRYMSG".to_string()],
        "Delivery must be acked after Matrix post succeeds"
    );
}

// ---------------------------------------------------------------------------
// Oracle 4:
// Sends carry push_replies:true on agent.sock.
// Mutant: send without push_replies => RED.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_4_sends_carry_push_replies_true_on_agent_sock() {
    let tmp = tempfile::tempdir().unwrap();
    let server = MockXmsgSockets::start(tmp.path()).await;
    let config = test_config(tmp.path());
    let xmsg = create_xmsg_client(&config);
    let matrix = MockMatrixClient::default();
    let store = Store::new_in_memory().unwrap();

    let event = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$ev_ask4".to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        body: "@genie Question verifying push_replies".to_string(),
        formatted_body: None,
        mentions: Some(vec!["@genie:example.org".to_string()]),
        timestamp_ms: 1000000,
        thread_root_id: None,
        replaces_event_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
    };

    handle_incoming_event(&event, &[], &config, &matrix, xmsg.as_ref(), &store)
        .await
        .expect("handle_incoming_event must succeed");

    let sends = server.recorded_sends.lock().await.clone();
    assert_eq!(
        sends.len(),
        1,
        "Expected exactly 1 request to agent.sock stub"
    );
    let req = &sends[0];
    assert_eq!(req["action"], "send");
    assert_eq!(req["ref"], "claude");
    assert_eq!(
        req["push_replies"], true,
        "Sends on agent.sock must carry push_replies:true"
    );
}

// ---------------------------------------------------------------------------
// Oracle 5:
// The owner DM fires once at answer_timeout_secs when no reply has come, and survives a bot restart (store-backed).
// Mutant: in-memory timer => RED.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_5_owner_dm_fires_once_at_timeout_and_survives_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let _server = MockXmsgSockets::start(tmp.path()).await;
    let db_path = tmp.path().join("bot.db");

    let mut config = test_config(tmp.path());
    config.db_path = db_path.clone();
    config.answer_timeout_secs = 10;

    let xmsg = create_xmsg_client(&config);
    let matrix = MockMatrixClient::default();
    let store = Store::new(&db_path).unwrap();

    // Forward an addressed question at t=1000
    let event = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$ev_ask5".to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        body: "@genie Question that will timeout".to_string(),
        formatted_body: None,
        mentions: Some(vec!["@genie:example.org".to_string()]),
        timestamp_ms: 1000000, // t=1000s
        thread_root_id: None,
        replaces_event_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
    };

    handle_incoming_event(&event, &[], &config, &matrix, xmsg.as_ref(), &store)
        .await
        .expect("handle_incoming_event must succeed");

    // Before timeout at t=1005 (5s elapsed < 10s): no DM
    let swept = sweep_answer_timeouts(&config, &matrix, &store, 1005)
        .await
        .unwrap();
    assert_eq!(swept, 0);
    assert!(matrix.sent_dms.lock().unwrap().is_empty());

    // Drop store to simulate bot shutdown/restart
    drop(store);

    // Bot restarts: create fresh store connected to same SQLite db_path
    let restarted_store = Store::new(&db_path).unwrap();

    // At t=1015 (15s elapsed > 10s): DM fires once
    let swept_after_restart = sweep_answer_timeouts(&config, &matrix, &restarted_store, 1015)
        .await
        .unwrap();
    assert_eq!(
        swept_after_restart, 1,
        "Sweeper must find 1 timed out message after restart"
    );

    {
        let dms = matrix.sent_dms.lock().unwrap().clone();
        assert_eq!(dms.len(), 1, "Owner DM must fire exactly once");
        assert_eq!(dms[0].user_id, config.owner_mxid);
        assert!(dms[0].body.contains("Answer timeout"));
    }

    // Later at t=1030: subsequent sweep fires zero DMs (does not repeat)
    let swept_later = sweep_answer_timeouts(&config, &matrix, &restarted_store, 1030)
        .await
        .unwrap();
    assert_eq!(swept_later, 0, "Sweeper must not duplicate DM");
    assert_eq!(
        matrix.sent_dms.lock().unwrap().len(),
        1,
        "No duplicate DMs allowed"
    );
}

// ---------------------------------------------------------------------------
// Oracle 6:
// A first {"silent": true} swaps 👀 for 🫡 and posts nothing (M9 cell, still green). Name it.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_6_silent_decline_swaps_eye_for_salute_and_posts_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    let server = MockXmsgSockets::start(tmp.path()).await;
    let config = test_config(tmp.path());
    let xmsg = create_xmsg_client(&config);
    let matrix = MockMatrixClient::default();
    let store = Store::new_in_memory().unwrap();

    let event = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$ev_ask6".to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        body: "@genie Please decline this request".to_string(),
        formatted_body: None,
        mentions: Some(vec!["@genie:example.org".to_string()]),
        timestamp_ms: 1000000,
        thread_root_id: None,
        replaces_event_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
    };

    handle_incoming_event(&event, &[], &config, &matrix, xmsg.as_ref(), &store)
        .await
        .expect("handle_incoming_event must succeed");

    // The bot sent reaction 👀 on receipt
    {
        let rxns = matrix.sent_reactions.lock().unwrap().clone();
        assert_eq!(rxns.len(), 1);
        assert_eq!(rxns[0].key, "👀");
    }

    let fwd_msg_id = "01M13MSG0000000000000001";
    let mut inbox = register_svc(&config.xmsg_register_socket(), "matrix-xmsg")
        .await
        .expect("register_svc must succeed");

    // Expert replies with {"silent": true}
    server
        .queue_delivery(serde_json::json!({
            "status": "ok",
            "action": "deliver",
            "messageId": "01M13DECLINE001",
            "fromName": "claude",
            "text": "{\"silent\": true}",
            "envelope": format!("[xmsg] reply to message_id={fwd_msg_id} — message_id=01M13DECLINE001\n{{\"silent\": true}}")
        }))
        .await;

    let delivery = inbox.poll(1).await.unwrap().unwrap();
    let outcome =
        handle_inbox_delivery(&delivery, &config, &matrix, &store, &mut inbox, 1005).await;
    assert_eq!(
        outcome.unwrap(),
        BotOutcome::Declined,
        "Decline reply must yield BotOutcome::Declined"
    );

    // 1. Redacted original eye reaction
    let redacted = matrix.redacted_events.lock().unwrap().clone();
    assert_eq!(redacted.len(), 1, "Must redact the original eye reaction");
    assert_eq!(redacted[0].event_id, "$mock_reaction_1");

    // 2. Added salute reaction
    let rxns = matrix.sent_reactions.lock().unwrap().clone();
    assert_eq!(rxns.len(), 2, "Expected initial eye + follow-up salute");
    assert_eq!(rxns[1].key, "🫡", "Must send salute reaction on decline");

    // 3. Posted NO notice to the room
    assert!(
        matrix.sent_notices.lock().unwrap().is_empty(),
        "Must post nothing to the room on silent decline"
    );

    // 4. Delivery was acked
    let acks = server.recorded_acks.lock().await.clone();
    assert_eq!(acks, vec!["01M13DECLINE001".to_string()]);
}

// ---------------------------------------------------------------------------
// Oracle 7 (M13 + M16 integration):
// M13 attributes a reply to EITHER recorded message id of a thread:
// - A reply to the delta message id is attributed to the thread and posted.
// - A reply to the re-bootstrap message id is attributed to the thread and posted.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_7_reply_attributed_to_either_delta_or_bootstrap_message_id() {
    let tmp = tempfile::tempdir().unwrap();
    let server = MockXmsgSockets::start(tmp.path()).await;
    let config = test_config(tmp.path());
    let xmsg = create_xmsg_client(&config);
    let matrix = MockMatrixClient::default();
    let store = Store::new_in_memory().unwrap();

    let ev1 = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$ev_init".to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        body: "@genie initial question".to_string(),
        formatted_body: None,
        mentions: Some(vec!["@genie:example.org".to_string()]),
        timestamp_ms: 1000000,
        thread_root_id: Some("$root_m13".to_string()),
        replaces_event_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
    };
    handle_incoming_event(&ev1, &[], &config, &matrix, xmsg.as_ref(), &store)
        .await
        .unwrap();

    let ev2 = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$ev_second".to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        body: "@genie follow-up question".to_string(),
        formatted_body: None,
        mentions: Some(vec!["@genie:example.org".to_string()]),
        timestamp_ms: 1050000,
        thread_root_id: Some("$root_m13".to_string()),
        replaces_event_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
    };
    handle_incoming_event(&ev2, &[], &config, &matrix, xmsg.as_ref(), &store)
        .await
        .unwrap();

    let mut inbox = register_svc(&config.xmsg_register_socket(), "matrix-xmsg")
        .await
        .expect("register_svc must succeed");

    // Reply referencing first forward's message ID (01M13MSG0000000000000001)
    server
        .queue_delivery(serde_json::json!({
            "status": "ok",
            "action": "deliver",
            "messageId": "01M13REP_A",
            "fromName": "claude",
            "text": "Answer referencing first message id",
            "envelope": "[xmsg] reply to message_id=01M13MSG0000000000000001 — message_id=01M13REP_A\nAnswer referencing first message id"
        }))
        .await;

    let d_a = inbox.poll(1).await.unwrap().unwrap();
    let res_a = handle_inbox_delivery(&d_a, &config, &matrix, &store, &mut inbox, 1060).await;
    assert_eq!(res_a.unwrap(), BotOutcome::Replied);

    // Reply referencing second forward's message ID (01M13MSG0000000000000002)
    server
        .queue_delivery(serde_json::json!({
            "status": "ok",
            "action": "deliver",
            "messageId": "01M13REP_B",
            "fromName": "claude",
            "text": "Answer referencing second message id",
            "envelope": "[xmsg] reply to message_id=01M13MSG0000000000000002 — message_id=01M13REP_B\nAnswer referencing second message id"
        }))
        .await;

    let d_b = inbox.poll(1).await.unwrap().unwrap();
    let res_b = handle_inbox_delivery(&d_b, &config, &matrix, &store, &mut inbox, 1061).await;
    assert_eq!(res_b.unwrap(), BotOutcome::Replied);

    let notices = matrix.sent_notices.lock().unwrap().clone();
    assert_eq!(notices.len(), 2);
    assert_eq!(notices[0].thread_root_id.as_deref(), Some("$root_m13"));
    assert_eq!(notices[1].thread_root_id.as_deref(), Some("$root_m13"));
    assert!(notices[0]
        .body
        .contains("Answer referencing first message id"));
    assert!(notices[1]
        .body
        .contains("Answer referencing second message id"));
}
