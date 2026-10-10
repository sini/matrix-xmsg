use matrix_xmsg::config::{Admission, Config};
use matrix_xmsg::error::AppError;
use matrix_xmsg::matrix::{MatrixClient, MockMatrixClient};
use matrix_xmsg::store::Store;
use matrix_xmsg::xmsg::{create_xmsg_client, ReconnectingSvcInbox};
use serde_json::Value;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::net::UnixListener;
use tokio::sync::Mutex;

pub struct MockXmsgSockets {
    pub dir: PathBuf,
    pub recorded_sends: Arc<Mutex<Vec<Value>>>,
    pub recorded_acks: Arc<Mutex<Vec<String>>>,
    pub queued_deliveries: Arc<Mutex<VecDeque<Value>>>,
    pub in_flight: Arc<Mutex<VecDeque<Value>>>,
    pub send_counter: Arc<AtomicUsize>,
    pub registration_count: Arc<AtomicUsize>,
    pub refuse_registration: Arc<AtomicBool>,
    pub active_client_abort: Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
    reg_shutdown_tx: Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl MockXmsgSockets {
    pub async fn start(dir: &Path) -> Self {
        let agent_sock_path = dir.join("agent.sock");
        let _ = std::fs::remove_file(&agent_sock_path);

        let agent_listener = UnixListener::bind(&agent_sock_path).expect("bind agent.sock");

        let recorded_sends = Arc::new(Mutex::new(Vec::new()));
        let recorded_acks = Arc::new(Mutex::new(Vec::new()));
        let queued_deliveries = Arc::new(Mutex::new(VecDeque::new()));
        let in_flight = Arc::new(Mutex::new(VecDeque::new()));
        let send_counter = Arc::new(AtomicUsize::new(1));
        let registration_count = Arc::new(AtomicUsize::new(0));
        let refuse_registration = Arc::new(AtomicBool::new(false));
        let active_client_abort = Arc::new(Mutex::new(None));
        let reg_shutdown_tx = Arc::new(Mutex::new(None));

        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        let sends_clone = Arc::clone(&recorded_sends);
        let counter_clone = Arc::clone(&send_counter);

        tokio::spawn(async move {
            tokio::select! {
                _ = &mut shutdown_rx => {}
                _ = async {
                    while let Ok((stream, _)) = agent_listener.accept().await {
                        let (r, mut w) = stream.into_split();
                        let mut lines = tokio::io::BufReader::new(r).lines();
                        let sends = Arc::clone(&sends_clone);
                        let counter = Arc::clone(&counter_clone);
                        tokio::spawn(async move {
                            while let Ok(Some(line)) = lines.next_line().await {
                                if let Ok(val) = serde_json::from_str::<Value>(&line) {
                                    sends.lock().await.push(val.clone());
                                    let action = val.get("action").and_then(|a| a.as_str()).unwrap_or("");
                                    if action == "send" {
                                        let idx = counter.fetch_add(1, Ordering::SeqCst);
                                        let msg_id = format!("01M18MSG{:016X}", idx);
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
                } => {}
            }
        });

        let mock = Self {
            dir: dir.to_path_buf(),
            recorded_sends,
            recorded_acks,
            queued_deliveries,
            in_flight,
            send_counter,
            registration_count,
            refuse_registration,
            active_client_abort,
            reg_shutdown_tx,
            shutdown_tx: Some(shutdown_tx),
        };

        mock.start_server().await;
        mock
    }

    pub async fn queue_delivery(&self, delivery: Value) {
        self.queued_deliveries.lock().await.push_back(delivery);
    }

    pub async fn drop_active_registration(&self) {
        if let Some(tx) = self.active_client_abort.lock().await.take() {
            let _ = tx.send(());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    pub async fn stop_server(&self) {
        self.drop_active_registration().await;
        if let Some(tx) = self.reg_shutdown_tx.lock().await.take() {
            let _ = tx.send(());
        }
        let reg_sock = self.dir.join("register.sock");
        let _ = std::fs::remove_file(&reg_sock);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    pub async fn start_server(&self) {
        let reg_sock = self.dir.join("register.sock");
        let _ = std::fs::remove_file(&reg_sock);
        let reg_listener = UnixListener::bind(&reg_sock).expect("bind register.sock");

        let (reg_tx, mut reg_rx) = tokio::sync::oneshot::channel::<()>();
        *self.reg_shutdown_tx.lock().await = Some(reg_tx);

        let acks = Arc::clone(&self.recorded_acks);
        let deliveries = Arc::clone(&self.queued_deliveries);
        let in_flight = Arc::clone(&self.in_flight);
        let registration_count = Arc::clone(&self.registration_count);
        let refuse_registration = Arc::clone(&self.refuse_registration);
        let active_client_abort = Arc::clone(&self.active_client_abort);

        tokio::spawn(async move {
            tokio::select! {
                _ = &mut reg_rx => {}
                _ = async {
                    while let Ok((stream, _)) = reg_listener.accept().await {
                        let (r, mut w) = stream.into_split();
                        let mut lines = tokio::io::BufReader::new(r).lines();
                        let acks = Arc::clone(&acks);
                        let deliveries = Arc::clone(&deliveries);
                        let in_flight = Arc::clone(&in_flight);
                        let registration_count = Arc::clone(&registration_count);
                        let refuse_registration = Arc::clone(&refuse_registration);

                        let (client_abort_tx, mut client_abort_rx) = tokio::sync::oneshot::channel::<()>();
                        *active_client_abort.lock().await = Some(client_abort_tx);

                        tokio::spawn(async move {
                            tokio::select! {
                                _ = &mut client_abort_rx => {
                                    // Connection aborted: return unacked in_flight to front of queue
                                    let mut inflight = in_flight.lock().await;
                                    let mut q = deliveries.lock().await;
                                    while let Some(d) = inflight.pop_back() {
                                        q.push_front(d);
                                    }
                                }
                                _ = async {
                                    // 1. Registration line
                                    if let Ok(Some(line)) = lines.next_line().await {
                                        if let Ok(val) = serde_json::from_str::<Value>(&line) {
                                            if val.get("harness").and_then(|h| h.as_str()) == Some("svc") {
                                                if refuse_registration.load(Ordering::SeqCst) {
                                                    let resp = serde_json::json!({
                                                        "status": "error",
                                                        "detail": "service name already registered"
                                                    });
                                                    let _ = w.write_all(format!("{resp}\n").as_bytes()).await;
                                                    let _ = w.flush().await;
                                                    return;
                                                }
                                                registration_count.fetch_add(1, Ordering::SeqCst);
                                                let resp = serde_json::json!({
                                                    "status": "ok",
                                                    "sessionId": "matrix-xmsg",
                                                });
                                                let _ = w.write_all(format!("{resp}\n").as_bytes()).await;
                                                let _ = w.flush().await;
                                            }
                                        }
                                    }

                                    // 2. Poll / ack loop
                                    while let Ok(Some(line)) = lines.next_line().await {
                                        if let Ok(val) = serde_json::from_str::<Value>(&line) {
                                            let action = val.get("action").and_then(|a| a.as_str()).unwrap_or("");
                                            if action == "poll" {
                                                let delivery = deliveries.lock().await.pop_front();
                                                if let Some(ref d) = delivery {
                                                    in_flight.lock().await.push_back(d.clone());
                                                }
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
                                                    in_flight.lock().await.retain(|d| {
                                                        let id = d.get("messageId").or_else(|| d.get("message_id")).and_then(|v| v.as_str()).unwrap_or("");
                                                        id != msg_id
                                                    });
                                                }
                                                let resp = serde_json::json!({
                                                    "status": "ok",
                                                });
                                                let _ = w.write_all(format!("{resp}\n").as_bytes()).await;
                                                let _ = w.flush().await;
                                            }
                                        }
                                    }

                                    // Connection closed or EOF: return unacked in_flight to front of queue
                                    let mut inflight = in_flight.lock().await;
                                    let mut q = deliveries.lock().await;
                                    while let Some(d) = inflight.pop_back() {
                                        q.push_front(d);
                                    }
                                } => {}
                            }
                        });
                    }
                } => {}
            }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
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
        resync_byte_cap: 65536,
        guard_ref: "svc:genie-guard".to_string(),
        guard_timeout_secs: 30,
        guard_retry_budget: 3,
        db_path: PathBuf::from(":memory:"),
    }
}

// ---------------------------------------------------------------------------
// Oracle 1:
// Mock server drops registration connection and comes back => bot re-registers
// within bound, and reply pushed after that is posted.
// Mutant: no reconnect => RED.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_1_mock_server_drops_connection_and_bot_reregisters() {
    let tmp = tempfile::tempdir().unwrap();
    let server = MockXmsgSockets::start(tmp.path()).await;
    let config = Arc::new(test_config(tmp.path()));
    let xmsg = create_xmsg_client(&config);
    let matrix = Arc::new(MockMatrixClient::default());
    let store = Arc::new(Store::new_in_memory().unwrap());

    let orig_msg_id = "01M18ORIG000000000000001";
    store
        .record_forwarded_message(&matrix_xmsg::store::ForwardedMessageRecord {
            message_id: orig_msg_id.to_string(),
            room_id: "!support:example.org".to_string(),
            thread_root_id: "$root1".to_string(),
            event_id: "$ev1".to_string(),
            sender_mxid: "@alice:example.org".to_string(),
            addressed: true,
            sent_at: 1000,
            ack_reaction_id: None,
            timeout_dm_sent: false,
            reply_count: 0,
        })
        .unwrap();

    let inbox = ReconnectingSvcInbox::new(config.xmsg_register_socket(), "matrix-xmsg")
        .with_backoff(Duration::from_millis(50), Duration::from_millis(200));

    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);

    let inbox_config = config.clone();
    let inbox_matrix = matrix.clone();
    let inbox_store = store.clone();
    let inbox_xmsg = xmsg.clone();

    let loop_handle = tokio::spawn(async move {
        matrix_xmsg::bot::run_inbox_loop(
            inbox,
            inbox_config,
            inbox_matrix,
            inbox_store,
            inbox_xmsg,
            shutdown_rx,
        )
        .await
    });

    // Wait until initial registration succeeds
    let start = tokio::time::Instant::now();
    while server.registration_count.load(Ordering::SeqCst) < 1 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if start.elapsed() > Duration::from_secs(5) {
            panic!("Initial registration timed out");
        }
    }
    assert_eq!(server.registration_count.load(Ordering::SeqCst), 1);

    // Drop active registration connection
    server.drop_active_registration().await;

    // Queue reply delivery
    server
        .queue_delivery(serde_json::json!({
            "status": "ok",
            "action": "deliver",
            "messageId": "01M18REPLY00000000000001",
            "fromName": "claude",
            "text": "Answer after reconnect",
            "envelope": format!("[xmsg] reply to message_id={orig_msg_id} — message_id=01M18REPLY00000000000001\nAnswer after reconnect")
        }))
        .await;

    // Wait for the bot to re-register (registration_count >= 2) and post the reply
    let start = tokio::time::Instant::now();
    loop {
        let count = server.registration_count.load(Ordering::SeqCst);
        let notices = matrix.sent_notices.lock().unwrap().clone();
        if count >= 2 && !notices.is_empty() {
            assert!(
                notices[0].body.contains("Answer after reconnect"),
                "Notice must contain the reply text"
            );
            break;
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
        if start.elapsed() > Duration::from_secs(5) {
            panic!(
                "Reconnection or notice posting timed out: reg_count={}, notices={}",
                count,
                notices.len()
            );
        }
    }

    // Verify delivery was acked
    let acks = server.recorded_acks.lock().await.clone();
    assert_eq!(acks, vec!["01M18REPLY00000000000001".to_string()]);

    let _ = shutdown_tx.send(());
    let _ = loop_handle.await;
}

// ---------------------------------------------------------------------------
// Oracle 2:
// A delivery the bot received but had not acked before the drop is redelivered
// and posted exactly once.
// Mutant: ack on receipt => RED (double or zero post).
// ---------------------------------------------------------------------------
struct DroppingMatrixClient {
    pub inner: MockMatrixClient,
    pub server_to_drop: Arc<MockXmsgSockets>,
    pub drop_on_first_send: AtomicBool,
    pub send_attempts: AtomicUsize,
}

#[async_trait::async_trait]
impl MatrixClient for DroppingMatrixClient {
    async fn send_notice(
        &self,
        room_id: &str,
        thread_root_id: Option<&str>,
        body: &str,
        in_reply_to_sender: Option<&str>,
    ) -> Result<String, AppError> {
        let attempt = self.send_attempts.fetch_add(1, Ordering::SeqCst);
        if attempt == 0 && self.drop_on_first_send.load(Ordering::SeqCst) {
            // Drop registration connection before ack can occur, and simulate send failure
            self.server_to_drop.drop_active_registration().await;
            return Err(AppError::Matrix("simulated failure before ack".into()));
        }
        self.inner
            .send_notice(room_id, thread_root_id, body, in_reply_to_sender)
            .await
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

    async fn send_dm(&self, user_mxid: &str, body: &str) -> Result<String, AppError> {
        self.inner.send_dm(user_mxid, body).await
    }

    async fn fetch_history(
        &self,
        room_id: &str,
        limit: usize,
    ) -> Result<Vec<matrix_xmsg::context::EventMessage>, AppError> {
        self.inner.fetch_history(room_id, limit).await
    }

    async fn fetch_thread_history(
        &self,
        room_id: &str,
        thread_root_id: &str,
    ) -> Result<Vec<matrix_xmsg::context::EventMessage>, AppError> {
        self.inner
            .fetch_thread_history(room_id, thread_root_id)
            .await
    }
}

#[tokio::test]
async fn oracle_2_unacked_delivery_before_drop_is_redelivered_and_posted_once() {
    let tmp = tempfile::tempdir().unwrap();
    let server = Arc::new(MockXmsgSockets::start(tmp.path()).await);
    let config = Arc::new(test_config(tmp.path()));
    let xmsg = create_xmsg_client(&config);
    let matrix = Arc::new(DroppingMatrixClient {
        inner: MockMatrixClient::default(),
        server_to_drop: server.clone(),
        drop_on_first_send: AtomicBool::new(true),
        send_attempts: AtomicUsize::new(0),
    });
    let store = Arc::new(Store::new_in_memory().unwrap());

    let orig_msg_id = "01M18ORIG000000000000002";
    store
        .record_forwarded_message(&matrix_xmsg::store::ForwardedMessageRecord {
            message_id: orig_msg_id.to_string(),
            room_id: "!support:example.org".to_string(),
            thread_root_id: "$root2".to_string(),
            event_id: "$ev2".to_string(),
            sender_mxid: "@alice:example.org".to_string(),
            addressed: true,
            sent_at: 1000,
            ack_reaction_id: None,
            timeout_dm_sent: false,
            reply_count: 0,
        })
        .unwrap();

    let inbox = ReconnectingSvcInbox::new(config.xmsg_register_socket(), "matrix-xmsg")
        .with_backoff(Duration::from_millis(50), Duration::from_millis(200));

    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);

    let inbox_config = config.clone();
    let inbox_matrix = matrix.clone();
    let inbox_store = store.clone();
    let inbox_xmsg = xmsg.clone();

    let loop_handle = tokio::spawn(async move {
        matrix_xmsg::bot::run_inbox_loop(
            inbox,
            inbox_config,
            inbox_matrix,
            inbox_store,
            inbox_xmsg,
            shutdown_rx,
        )
        .await
    });

    // Wait until initial registration succeeds
    let start = tokio::time::Instant::now();
    while server.registration_count.load(Ordering::SeqCst) < 1 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if start.elapsed() > Duration::from_secs(5) {
            panic!("Initial registration timed out");
        }
    }

    // Queue delivery
    server
        .queue_delivery(serde_json::json!({
            "status": "ok",
            "action": "deliver",
            "messageId": "01M18DELIVERY0000000002",
            "fromName": "claude",
            "text": "Answer redelivered and posted once",
            "envelope": format!("[xmsg] reply to message_id={orig_msg_id} — message_id=01M18DELIVERY0000000002\nAnswer redelivered and posted once")
        }))
        .await;

    // The first attempt will drop connection before ack.
    // The bot will reconnect, the unacked message will be redelivered, and then posted once.
    let start = tokio::time::Instant::now();
    loop {
        let notices = matrix.inner.sent_notices.lock().unwrap().clone();
        if notices.len() == 1 {
            assert!(
                notices[0]
                    .body
                    .contains("Answer redelivered and posted once"),
                "Notice must contain reply text"
            );
            break;
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
        if start.elapsed() > Duration::from_secs(5) {
            panic!(
                "Posting timed out: notices={}, attempts={}",
                notices.len(),
                matrix.send_attempts.load(Ordering::SeqCst)
            );
        }
    }

    // Give a brief window to confirm no second duplicate post occurs
    tokio::time::sleep(Duration::from_millis(150)).await;
    let notices = matrix.inner.sent_notices.lock().unwrap().clone();
    assert_eq!(notices.len(), 1, "Must be posted exactly once");

    let acks = server.recorded_acks.lock().await.clone();
    assert_eq!(
        acks,
        vec!["01M18DELIVERY0000000002".to_string()],
        "Must be acknowledged on redelivery"
    );

    let _ = shutdown_tx.send(());
    let _ = loop_handle.await;
}

// ---------------------------------------------------------------------------
// Oracle 3:
// While the server is down, the bot keeps running (the process does not exit).
// Mutant: exit on connection loss => RED.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_3_bot_keeps_running_while_server_is_down() {
    let tmp = tempfile::tempdir().unwrap();
    let server = MockXmsgSockets::start(tmp.path()).await;
    let config = Arc::new(test_config(tmp.path()));
    let xmsg = create_xmsg_client(&config);
    let matrix = Arc::new(MockMatrixClient::default());
    let store = Arc::new(Store::new_in_memory().unwrap());

    let orig_msg_id = "01M18ORIG000000000000003";
    store
        .record_forwarded_message(&matrix_xmsg::store::ForwardedMessageRecord {
            message_id: orig_msg_id.to_string(),
            room_id: "!support:example.org".to_string(),
            thread_root_id: "$root3".to_string(),
            event_id: "$ev3".to_string(),
            sender_mxid: "@alice:example.org".to_string(),
            addressed: true,
            sent_at: 1000,
            ack_reaction_id: None,
            timeout_dm_sent: false,
            reply_count: 0,
        })
        .unwrap();

    let inbox = ReconnectingSvcInbox::new(config.xmsg_register_socket(), "matrix-xmsg")
        .with_backoff(Duration::from_millis(50), Duration::from_millis(200));

    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);

    let inbox_config = config.clone();
    let inbox_matrix = matrix.clone();
    let inbox_store = store.clone();
    let inbox_xmsg = xmsg.clone();

    let loop_handle = tokio::spawn(async move {
        matrix_xmsg::bot::run_inbox_loop(
            inbox,
            inbox_config,
            inbox_matrix,
            inbox_store,
            inbox_xmsg,
            shutdown_rx,
        )
        .await
    });

    // Wait until initial registration succeeds
    let start = tokio::time::Instant::now();
    while server.registration_count.load(Ordering::SeqCst) < 1 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if start.elapsed() > Duration::from_secs(5) {
            panic!("Initial registration timed out");
        }
    }

    // Stop server completely: removes register.sock and stops accepting
    server.stop_server().await;

    // Sleep for 1.5 seconds while server is down. The bot must keep running!
    tokio::time::sleep(Duration::from_millis(1500)).await;

    assert!(
        !loop_handle.is_finished(),
        "Inbox loop task must remain running while server is down"
    );

    // Restart the server
    server.start_server().await;

    // Queue a delivery
    server
        .queue_delivery(serde_json::json!({
            "status": "ok",
            "action": "deliver",
            "messageId": "01M18DELIVERY0000000003",
            "fromName": "claude",
            "text": "Answer after full server restart",
            "envelope": format!("[xmsg] reply to message_id={orig_msg_id} — message_id=01M18DELIVERY0000000003\nAnswer after full server restart")
        }))
        .await;

    // Wait for the bot to reconnect and post the reply
    let start = tokio::time::Instant::now();
    loop {
        let notices = matrix.sent_notices.lock().unwrap().clone();
        if !notices.is_empty() {
            assert!(
                notices[0].body.contains("Answer after full server restart"),
                "Notice must contain reply text"
            );
            break;
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
        if start.elapsed() > Duration::from_secs(5) {
            panic!("Notice posting timed out after server restart");
        }
    }

    let _ = shutdown_tx.send(());
    let _ = loop_handle.await;
}
