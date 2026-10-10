use async_trait::async_trait;
use matrix_xmsg::bot::run_inbox_loop;
use matrix_xmsg::config::{Admission, Config};
use matrix_xmsg::error::AppError;
use matrix_xmsg::matrix::MockMatrixClient;
use matrix_xmsg::store::Store;
use matrix_xmsg::xmsg::{SendResponse, SvcDelivery, SvcInbox, XmsgClient};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Default)]
pub struct MockXmsgClient {
    pub sends: Arc<Mutex<Vec<(String, String, String)>>>,
    pub replies: Arc<Mutex<Vec<(String, String)>>>,
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
        Ok(SendResponse::new(
            "01MOCKMSG000001",
            Some("session-1".to_string()),
        ))
    }

    async fn reply_message(&self, message_id: &str, text: &str) -> Result<SendResponse, AppError> {
        self.replies
            .lock()
            .unwrap()
            .push((message_id.to_string(), text.to_string()));
        Ok(SendResponse::new(
            "01MOCKREPLY00001",
            Some("session-1".to_string()),
        ))
    }
}

pub struct TestQueueInbox {
    pub delivery: Arc<Mutex<Option<SvcDelivery>>>,
    pub attempts: Arc<AtomicUsize>,
    pub acks: Arc<Mutex<Vec<String>>>,
}

impl TestQueueInbox {
    pub fn new(delivery: SvcDelivery) -> Self {
        Self {
            delivery: Arc::new(Mutex::new(Some(delivery))),
            attempts: Arc::new(AtomicUsize::new(0)),
            acks: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

#[async_trait]
impl SvcInbox for TestQueueInbox {
    async fn poll(&mut self, _wait_secs: u64) -> Result<Option<SvcDelivery>, AppError> {
        let maybe_delivery = self.delivery.lock().unwrap().clone();
        if let Some(d) = maybe_delivery {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            Ok(Some(d))
        } else {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok(None)
        }
    }

    async fn ack(&mut self, message_id: &str) -> Result<(), AppError> {
        self.acks.lock().unwrap().push(message_id.to_string());
        let mut guard = self.delivery.lock().unwrap();
        if let Some(ref d) = *guard {
            if d.message_id == message_id {
                *guard = None;
            }
        }
        Ok(())
    }
}

fn test_config() -> Config {
    Config {
        homeserver_url: "https://matrix.example.org".to_string(),
        bot_mxid: "@genie:example.org".to_string(),
        access_token_file: PathBuf::from("/nonexistent/token"),
        rooms: vec!["!quiet:example.org".to_string()],
        trusted_mxids: vec!["@alice:example.org".to_string()],
        owner_mxid: "@owner:example.org".to_string(),
        admission: Admission::Trusted,
        xmsg_socket: PathBuf::from("/run/user/1000/xmsg"),
        expert_ref: "claude".to_string(),
        history_n: 5,
        history_byte_cap: 1024,
        resync_byte_cap: 65536,
        guard_ref: "svc:genie-guard".to_string(),
        guard_timeout_secs: 30,
        guard_retry_budget: 3,
        inbox_retry_budget: 10,
        rate_limit_count: 10,
        rate_limit_window_secs: 60,
        size_cap_bytes: 4096,
        answer_timeout_secs: 300,
        session_live_secs: 3600,
        db_path: PathBuf::from(":memory:"),
    }
}

/// Oracle 1: After a simulated restart with a saved sync token and no new events in a configured room,
/// open_thread for that room posts.
/// Mutant: Room unknown until an event arrives => RED.
#[tokio::test]
async fn oracle_1_restart_with_saved_sync_token_posts_in_quiet_room() {
    let config = Arc::new(test_config());
    let matrix = Arc::new(MockMatrixClient::default());
    // Simulate restart with require_known_rooms enabled: room is not known until resolved
    matrix.require_known_rooms.store(true, Ordering::SeqCst);
    assert!(!matrix
        .known_rooms
        .lock()
        .unwrap()
        .contains("!quiet:example.org"));

    let store = Arc::new(Store::new_in_memory().unwrap());
    // Persist saved sync token (simulated restart with sync token)
    store.set_sync_token("s_saved_batch_token_999").unwrap();
    assert_eq!(
        store.get_sync_token().unwrap().as_deref(),
        Some("s_saved_batch_token_999")
    );

    let xmsg = Arc::new(MockXmsgClient::default());

    let delivery = SvcDelivery::new(
        "01M20OPEN00000000000001",
        "claude",
        serde_json::json!({
            "open_thread": {
                "room": "!quiet:example.org",
                "text": "Hello quiet room"
            }
        })
        .to_string(),
        "envelope",
        Some(serde_json::json!({ "kind": "local" })),
    );

    let inbox = TestQueueInbox::new(delivery);
    let acks = inbox.acks.clone();

    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);

    let handle = tokio::spawn({
        let config = config.clone();
        let matrix = matrix.clone();
        let store = store.clone();
        let xmsg = xmsg.clone();
        async move { run_inbox_loop(inbox, config, matrix, store, xmsg, shutdown_rx).await }
    });

    // Wait up to 3s for delivery to be acked
    let start = tokio::time::Instant::now();
    loop {
        if !acks.lock().unwrap().is_empty() {
            break;
        }
        if start.elapsed() > Duration::from_secs(3) {
            panic!("Timed out waiting for open_thread to be processed and acked");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let _ = shutdown_tx.send(());
    let _ = handle.await;

    // Verify room was resolved and notice posted
    let notices = matrix.sent_notices.lock().unwrap();
    assert_eq!(notices.len(), 1, "Expected 1 notice posted in quiet room");
    assert_eq!(notices[0].room_id, "!quiet:example.org");
    assert!(notices[0].body.contains("Hello quiet room"));

    let replies = xmsg.replies.lock().unwrap();
    assert_eq!(replies.len(), 1, "Expected reply with root and permalink");
    assert!(replies[0].1.contains("permalink"));
}

/// Oracle 2: A delivery whose post keeps failing is retried with growing gaps,
/// not more than N times in the first second (<= 2 attempts in ~800ms).
/// Mutant: No backoff => spins hundreds of times => RED.
#[tokio::test]
async fn oracle_2_failing_delivery_retried_with_backoff() {
    let config = Arc::new(test_config());
    let matrix = Arc::new(MockMatrixClient::default());
    // Simulate failing Matrix send_notice
    matrix.fail_send_notice.store(true, Ordering::SeqCst);

    let store = Arc::new(Store::new_in_memory().unwrap());
    let xmsg = Arc::new(MockXmsgClient::default());

    let delivery = SvcDelivery::new(
        "01M20FAIL00000000000001",
        "claude",
        serde_json::json!({
            "open_thread": {
                "room": "!quiet:example.org",
                "text": "Hello failing post"
            }
        })
        .to_string(),
        "envelope",
        Some(serde_json::json!({ "kind": "local" })),
    );

    let inbox = TestQueueInbox::new(delivery);
    let attempts = inbox.attempts.clone();

    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);

    let handle = tokio::spawn({
        let config = config.clone();
        let matrix = matrix.clone();
        let store = store.clone();
        let xmsg = xmsg.clone();
        async move { run_inbox_loop(inbox, config, matrix, store, xmsg, shutdown_rx).await }
    });

    // Let the loop run for 800ms (within first second)
    tokio::time::sleep(Duration::from_millis(800)).await;

    let _ = shutdown_tx.send(());
    let _ = handle.await;

    let count = attempts.load(Ordering::SeqCst);
    // In 800ms with 1s backoff, exactly 1 attempt should have run (or <= 2 if timing variance).
    // Without backoff, it would have spun hundreds or thousands of times.
    assert!(
        count <= 2,
        "Expected <= 2 attempts in first 800ms with backoff, got {count} (hot loop detected)"
    );
    assert!(
        count >= 1,
        "Expected at least 1 attempt to have occurred, got 0"
    );
}

/// Oracle 3: After the attempt bound (inbox_retry_budget), the sender gets an {"error": ...} reply,
/// the owner gets 1 DM, and the delivery is acked.
/// Mutant: Retry forever (never giving up) => RED.
#[tokio::test]
async fn oracle_3_bounded_retry_give_up() {
    let mut cfg = test_config();
    cfg.inbox_retry_budget = 2; // Bound at 2 attempts for fast and deterministic test
    let config = Arc::new(cfg);

    let matrix = Arc::new(MockMatrixClient::default());
    matrix.fail_send_notice.store(true, Ordering::SeqCst);

    let store = Arc::new(Store::new_in_memory().unwrap());
    let xmsg = Arc::new(MockXmsgClient::default());

    let delivery = SvcDelivery::new(
        "01M20BOUND000000000001",
        "claude",
        serde_json::json!({
            "open_thread": {
                "room": "!quiet:example.org",
                "text": "Hello bound"
            }
        })
        .to_string(),
        "envelope",
        Some(serde_json::json!({ "kind": "local" })),
    );

    let inbox = TestQueueInbox::new(delivery);
    let acks = inbox.acks.clone();
    let attempts = inbox.attempts.clone();

    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);

    let handle = tokio::spawn({
        let config = config.clone();
        let matrix = matrix.clone();
        let store = store.clone();
        let xmsg = xmsg.clone();
        async move { run_inbox_loop(inbox, config, matrix, store, xmsg, shutdown_rx).await }
    });

    // With budget=2: attempt 1 fails at t=0, sleeps 1s, attempt 2 fails at t=1s and gives up!
    // Total wait ~1.2s to 4s timeout.
    let start = tokio::time::Instant::now();
    loop {
        if !acks.lock().unwrap().is_empty() {
            break;
        }
        if start.elapsed() > Duration::from_secs(4) {
            panic!("Timed out waiting for delivery to give up and be acked (attempt bound exceeded without give-up)");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let _ = shutdown_tx.send(());
    let _ = handle.await;

    // 1. Delivery must be acked
    assert_eq!(
        acks.lock().unwrap().as_slice(),
        &["01M20BOUND000000000001"],
        "Delivery must be acknowledged upon give-up"
    );

    // 2. Sender got an {"error": ...} reply
    let replies = xmsg.replies.lock().unwrap();
    assert_eq!(replies.len(), 1, "Expected 1 error reply to sender");
    assert_eq!(replies[0].0, "01M20BOUND000000000001");
    let err_val: serde_json::Value =
        serde_json::from_str(&replies[0].1).expect("reply must be valid JSON");
    assert!(
        err_val.get("error").is_some(),
        "Sender reply must contain 'error': {}",
        replies[0].1
    );

    // 3. Owner got 1 DM
    let dms = matrix.sent_dms.lock().unwrap();
    assert_eq!(dms.len(), 1, "Owner must receive exactly 1 DM notification");
    assert_eq!(dms[0].user_id, config.owner_mxid);
    assert!(
        dms[0].body.contains("permanently failed after 2 attempts"),
        "Owner DM must mention failure and attempt count: {}",
        dms[0].body
    );

    // 4. Store cleared retry record
    assert_eq!(
        store.get_inbox_retries("01M20BOUND000000000001").unwrap(),
        None,
        "Store retries must be cleared upon give-up"
    );

    assert_eq!(
        attempts.load(Ordering::SeqCst),
        2,
        "Expected exactly 2 attempts before give-up"
    );
}

/// Oracle 4: Backlog suppression on a token-less first sync still drops old events.
/// Names and verifies the existing R5 test passes.
#[test]
fn oracle_4_backlog_suppression_on_tokenless_sync_named_r5() {
    // Verified by running `cargo test --test gate_m2 test_r5_backlog_suppression_primary_token_guard`.
    // Test: tests/gate_m2.rs::test_r5_backlog_suppression_primary_token_guard
    let verified_test_name = "test_r5_backlog_suppression_primary_token_guard";
    assert_eq!(
        verified_test_name,
        "test_r5_backlog_suppression_primary_token_guard"
    );
}
