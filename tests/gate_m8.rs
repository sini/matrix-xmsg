use async_trait::async_trait;
use matrix_xmsg::bot::{handle_incoming_event, BotOutcome, IncomingMatrixEvent};
use matrix_xmsg::config::{Admission, Config};
use matrix_xmsg::context::RelayedLine;
use matrix_xmsg::error::AppError;
use matrix_xmsg::matrix::MockMatrixClient;
use matrix_xmsg::store::Store;
use matrix_xmsg::xmsg::XmsgClient;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

struct TestXmsgMock {
    canned_reply: Mutex<Result<String, AppError>>,
    sent_payloads: Mutex<Vec<(String, String, String)>>, // (expert, from, text)
    send_counter: AtomicUsize,
}

impl TestXmsgMock {
    fn with_reply(reply: &str) -> Self {
        Self {
            canned_reply: Mutex::new(Ok(reply.to_string())),
            sent_payloads: Mutex::new(Vec::new()),
            send_counter: AtomicUsize::new(0),
        }
    }

    fn with_timeout(timeout_secs: u64) -> Self {
        Self {
            canned_reply: Mutex::new(Err(AppError::Timeout(timeout_secs))),
            sent_payloads: Mutex::new(Vec::new()),
            send_counter: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl XmsgClient for TestXmsgMock {
    async fn send_message(
        &self,
        expert_ref: &str,
        from: &str,
        text: &str,
    ) -> Result<String, AppError> {
        let mut list = self.sent_payloads.lock().unwrap();
        list.push((expert_ref.to_string(), from.to_string(), text.to_string()));
        let id_num = self.send_counter.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(format!("01MOCKMSG{id_num:06}"))
    }

    async fn wait_for_reply(
        &self,
        _message_id: &str,
        _timeout_secs: u64,
    ) -> Result<String, AppError> {
        let guard = self.canned_reply.lock().unwrap();
        match &*guard {
            Ok(s) => Ok(s.clone()),
            Err(AppError::Timeout(t)) => Err(AppError::Timeout(*t)),
            Err(e) => Err(AppError::Xmsg(e.to_string())),
        }
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
        xmsg_url: "http://127.0.0.1:7787".to_string(),
        xmsg_socket: None,
        expert_ref: "claude".to_string(),
        history_n: 5,
        history_byte_cap: 1024,
        rate_limit_count: 10,
        rate_limit_window_secs: 60,
        size_cap_bytes: 1024,
        answer_timeout_secs: 30,
        answer_deadline_secs: 3600,
        session_live_secs: 3600,
        db_path: PathBuf::from(":memory:"),
    }
}

fn parse_relayed_lines(envelope: &str, block_tag: &str) -> Vec<RelayedLine> {
    let open_tag = format!("<{block_tag}>");
    let close_tag = format!("</{block_tag}>");
    let start = if let Some(idx) = envelope.find(&open_tag) {
        Some(idx + open_tag.len())
    } else {
        let open_prefix = format!("<{block_tag} ");
        envelope
            .find(&open_prefix)
            .and_then(|idx| envelope[idx..].find('>').map(|end| idx + end + 1))
    };
    let end = envelope.find(&close_tag);
    if let (Some(s), Some(e)) = (start, end) {
        let inner = &envelope[s..e];
        inner
            .lines()
            .map(|l| l.trim())
            .filter(|l| !l.is_empty())
            .map(|l| {
                serde_json::from_str::<RelayedLine>(l)
                    .unwrap_or_else(|err| panic!("Invalid JSON line: {l:?}, error: {err}"))
            })
            .collect()
    } else {
        Vec::new()
    }
}

// ---------------------------------------------------------------------------
// Oracle 1:
// In an engaged thread, an unmentioned message from the asker is forwarded,
// envelope addressed:false.
// Mutant: keep the mention requirement => RED.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_1_engaged_thread_unmentioned_asker_forwarded_unaddressed() {
    let config = test_config();
    let matrix = MockMatrixClient::default();
    let xmsg = TestXmsgMock::with_reply("Here is the answer");
    let store = Store::new_in_memory().unwrap();

    // Setup engaged thread: record asker
    let thread_root = "$thread_root_1";
    store
        .record_thread_asker(thread_root, "@alice:example.org")
        .unwrap();

    let event = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$ev_follow_1".to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        body: "Can you elaborate on step 2?".to_string(),
        formatted_body: None,
        mentions: None,
        timestamp_ms: 10000,
        thread_root_id: Some(thread_root.to_string()),
        replaces_event_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
    };

    let outcome = handle_incoming_event(&event, &[], &config, &matrix, &xmsg, &store)
        .await
        .unwrap();

    assert_eq!(outcome, BotOutcome::Replied);

    let sent = xmsg.sent_payloads.lock().unwrap();
    assert_eq!(sent.len(), 1, "Message must be forwarded to xmsg");
    let envelope = &sent[0].2;

    let req_lines = parse_relayed_lines(envelope, "request");
    assert_eq!(req_lines.len(), 1);
    assert_eq!(
        req_lines[0].addressed,
        Some(false),
        "Envelope request line must have addressed:false"
    );
    assert!(envelope.contains(r#""addressed":false"#));
}

// ---------------------------------------------------------------------------
// Oracle 2:
// In a NON-engaged thread and at top level, an unmentioned message is not forwarded.
// Mutant: treat every thread as engaged => RED.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_2_non_engaged_thread_and_top_level_unmentioned_dropped() {
    let config = test_config();
    let matrix = MockMatrixClient::default();
    let xmsg = TestXmsgMock::with_reply("unused");
    let store = Store::new_in_memory().unwrap();

    // 1. Unmentioned message in a non-engaged thread
    let unengaged_event = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$ev_unengaged".to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        body: "Hello anyone here?".to_string(),
        formatted_body: None,
        mentions: None,
        timestamp_ms: 10000,
        thread_root_id: Some("$thread_root_unengaged".to_string()),
        replaces_event_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
    };
    let outcome1 = handle_incoming_event(&unengaged_event, &[], &config, &matrix, &xmsg, &store)
        .await
        .unwrap();
    assert_eq!(
        outcome1,
        BotOutcome::IgnoredNoMention,
        "Unmentioned message in non-engaged thread must be ignored"
    );
    assert!(
        xmsg.sent_payloads.lock().unwrap().is_empty(),
        "No message sent for non-engaged thread"
    );

    // 2. Unmentioned message at top level
    let toplevel_event = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$ev_toplevel".to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        body: "General top-level chatter".to_string(),
        formatted_body: None,
        mentions: None,
        timestamp_ms: 11000,
        thread_root_id: None,
        replaces_event_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
    };
    let outcome2 = handle_incoming_event(&toplevel_event, &[], &config, &matrix, &xmsg, &store)
        .await
        .unwrap();
    assert_eq!(
        outcome2,
        BotOutcome::IgnoredNoMention,
        "Unmentioned top-level message must be ignored"
    );
    assert!(
        xmsg.sent_payloads.lock().unwrap().is_empty(),
        "No message sent for top-level unmentioned"
    );
}

// ---------------------------------------------------------------------------
// Oracle 3:
// In an engaged thread under public admission, a bystander's unmentioned line
// is not forwarded (and a trusted sender's is).
// Mutant: skip admission for follows => RED.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_3_engaged_thread_public_admission_bystander_refused() {
    let mut config = test_config();
    config.admission = Admission::Public;
    // trusted_mxids has @alice:example.org and @trusted:example.org, but NOT @asker or @bystander

    let matrix = MockMatrixClient::default();
    let xmsg = TestXmsgMock::with_reply("Reply text");
    let store = Store::new_in_memory().unwrap();

    let thread_root = "$thread_root_pub";
    store
        .record_thread_asker(thread_root, "@asker:example.org")
        .unwrap();

    // Bystander sends unmentioned message in this engaged thread
    let bystander_event = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$ev_bystander".to_string(),
        sender_mxid: "@bystander:example.org".to_string(),
        body: "I am an untrusted bystander chiming in".to_string(),
        formatted_body: None,
        mentions: None,
        timestamp_ms: 12000,
        thread_root_id: Some(thread_root.to_string()),
        replaces_event_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
    };
    let outcome_bystander =
        handle_incoming_event(&bystander_event, &[], &config, &matrix, &xmsg, &store)
            .await
            .unwrap();
    assert_eq!(
        outcome_bystander,
        BotOutcome::IgnoredUntrustedUser,
        "Bystander line in engaged thread must be dropped by admission gate"
    );
    assert_eq!(
        xmsg.sent_payloads.lock().unwrap().len(),
        0,
        "Bystander line must not be forwarded"
    );

    // Trusted sender sends unmentioned message in this engaged thread
    let trusted_event = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$ev_trusted".to_string(),
        sender_mxid: "@trusted:example.org".to_string(),
        body: "Trusted commentary in thread".to_string(),
        formatted_body: None,
        mentions: None,
        timestamp_ms: 13000,
        thread_root_id: Some(thread_root.to_string()),
        replaces_event_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
    };
    let outcome_trusted =
        handle_incoming_event(&trusted_event, &[], &config, &matrix, &xmsg, &store)
            .await
            .unwrap();
    assert_eq!(
        outcome_trusted,
        BotOutcome::Replied,
        "Trusted sender follow must be admitted and forwarded"
    );
    assert_eq!(
        xmsg.sent_payloads.lock().unwrap().len(),
        1,
        "Trusted follow forwarded"
    );
}

// ---------------------------------------------------------------------------
// Oracle 4:
// An unaddressed forward whose reply times out posts nothing; an addressed
// one still posts the timeout notice.
// Mutant: post the notice for both => RED.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_4_unaddressed_timeout_silent_addressed_escalates() {
    let config = test_config();
    let store = Store::new_in_memory().unwrap();

    let thread_root = "$thread_root_timeout";
    store
        .record_thread_asker(thread_root, "@alice:example.org")
        .unwrap();

    // 1. Unaddressed forward that times out -> posts NOTHING
    let matrix_unaddressed = MockMatrixClient::default();
    let xmsg_timeout = TestXmsgMock::with_timeout(30);

    let unaddressed_event = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$ev_unaddressed_timeout".to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        body: "Unaddressed follow message".to_string(),
        formatted_body: None,
        mentions: None,
        timestamp_ms: 14000,
        thread_root_id: Some(thread_root.to_string()),
        replaces_event_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
    };

    let outcome_unaddressed = handle_incoming_event(
        &unaddressed_event,
        &[],
        &config,
        &matrix_unaddressed,
        &xmsg_timeout,
        &store,
    )
    .await
    .unwrap();

    assert_eq!(
        outcome_unaddressed,
        BotOutcome::TimedOutSilent,
        "Unaddressed follow timeout must be TimedOutSilent"
    );
    assert!(
        matrix_unaddressed.sent_notices.lock().unwrap().is_empty(),
        "Must not post timeout notice for unaddressed message"
    );
    assert!(
        matrix_unaddressed.sent_dms.lock().unwrap().is_empty(),
        "Must not send DM for unaddressed message"
    );

    // 2. Addressed message that times out -> posts timeout notice & DM
    let matrix_addressed = MockMatrixClient::default();
    let addressed_event = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$ev_addressed_timeout".to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        body: "@genie:example.org urgent help needed".to_string(),
        formatted_body: None,
        mentions: Some(vec!["@genie:example.org".to_string()]),
        timestamp_ms: 15000,
        thread_root_id: Some(thread_root.to_string()),
        replaces_event_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
    };

    let outcome_addressed = handle_incoming_event(
        &addressed_event,
        &[],
        &config,
        &matrix_addressed,
        &xmsg_timeout,
        &store,
    )
    .await
    .unwrap();

    assert_eq!(
        outcome_addressed,
        BotOutcome::TimedOutAndEscalated,
        "Addressed message timeout must be TimedOutAndEscalated"
    );
    assert_eq!(
        matrix_addressed.sent_notices.lock().unwrap().len(),
        1,
        "Must post timeout notice for addressed message"
    );
    assert_eq!(
        matrix_addressed.sent_dms.lock().unwrap().len(),
        1,
        "Must send DM for addressed message"
    );
}

// ---------------------------------------------------------------------------
// Oracle 5:
// A mention in an engaged thread still yields addressed:true.
// Mutant: always false => RED.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_5_mention_in_engaged_thread_yields_addressed_true() {
    let config = test_config();
    let matrix = MockMatrixClient::default();
    let xmsg = TestXmsgMock::with_reply("Reply to mentioned query");
    let store = Store::new_in_memory().unwrap();

    let thread_root = "$thread_root_mentioned";
    store
        .record_thread_asker(thread_root, "@alice:example.org")
        .unwrap();

    let event = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$ev_mentioned_follow".to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        body: "@genie:example.org explicit follow-up question".to_string(),
        formatted_body: None,
        mentions: Some(vec!["@genie:example.org".to_string()]),
        timestamp_ms: 16000,
        thread_root_id: Some(thread_root.to_string()),
        replaces_event_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
    };

    let outcome = handle_incoming_event(&event, &[], &config, &matrix, &xmsg, &store)
        .await
        .unwrap();

    assert_eq!(outcome, BotOutcome::Replied);

    let sent = xmsg.sent_payloads.lock().unwrap();
    assert_eq!(sent.len(), 1);
    let envelope = &sent[0].2;

    let req_lines = parse_relayed_lines(envelope, "request");
    assert_eq!(req_lines.len(), 1);
    assert_eq!(
        req_lines[0].addressed,
        Some(true),
        "Mention in engaged thread must yield addressed:true"
    );
    assert!(envelope.contains(r#""addressed":true"#));
}
