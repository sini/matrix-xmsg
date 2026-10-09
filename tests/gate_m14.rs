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
// In a thread, a genuine reply (is_falling_back absent/false) to the bot's
// message with no mention => forwarded with addressed: true.
// Mutant: ignore m.in_reply_to (is_reply_to_bot = false) => RED.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_1_genuine_reply_in_thread_to_bot_message_addresses_bot() {
    let config = test_config();
    let matrix = MockMatrixClient::default();
    let xmsg = TestXmsgMock::with_reply("Reply from expert");
    let store = Store::new_in_memory().unwrap();

    let thread_root = "$thread_root_1";
    let bot_msg_id = "$bot_msg_1";
    store
        .record_bot_message(bot_msg_id, thread_root, 1000)
        .unwrap();

    let event = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$reply_1".to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        body: "Can you elaborate on step 2?".to_string(),
        formatted_body: None,
        mentions: None,
        timestamp_ms: 2000,
        thread_root_id: Some(thread_root.to_string()),
        replaces_event_id: None,
        in_reply_to_event_id: Some(bot_msg_id.to_string()),
        is_falling_back: false,
    };

    let outcome = handle_incoming_event(&event, &[], &config, &matrix, &xmsg, &store)
        .await
        .unwrap();

    assert_eq!(outcome, BotOutcome::Replied);

    let sent = xmsg.sent_payloads.lock().unwrap();
    assert_eq!(sent.len(), 1, "Must forward genuine reply to xmsg");
    let envelope = &sent[0].2;
    let req_lines = parse_relayed_lines(envelope, "request");
    assert_eq!(req_lines.len(), 1);
    assert_eq!(
        req_lines[0].addressed,
        Some(true),
        "Genuine reply to bot message must have addressed: true"
    );
    assert!(envelope.contains(r#""addressed":true"#));
}

// ---------------------------------------------------------------------------
// Oracle 2:
// A thread message whose m.in_reply_to is a fallback (is_falling_back: true)
// pointing at the bot's message, no mention => not addressed:
// - In a non-engaged thread: not forwarded (IgnoredNoMention).
// - In an engaged thread: forwarded with addressed: false.
// Mutant: ignore is_falling_back => RED.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_2_fallback_reply_in_thread_does_not_address_bot() {
    let config = test_config();
    let matrix = MockMatrixClient::default();
    let xmsg = TestXmsgMock::with_reply("unused");
    let store = Store::new_in_memory().unwrap();

    let bot_msg_outside = "$bot_msg_outside";
    let outside_root = "$thread_root_outside";
    store
        .record_bot_message(bot_msg_outside, outside_root, 1000)
        .unwrap();

    // 1. Non-engaged thread: fallback reply pointing to bot's message is NOT addressed,
    // so dropped with IgnoredNoMention.
    let unengaged_root = "$thread_root_unengaged";
    let fallback_unengaged = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$ev_fallback_1".to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        body: "Thread fallback message in unengaged thread".to_string(),
        formatted_body: None,
        mentions: None,
        timestamp_ms: 2000,
        thread_root_id: Some(unengaged_root.to_string()),
        replaces_event_id: None,
        in_reply_to_event_id: Some(bot_msg_outside.to_string()),
        is_falling_back: true,
    };
    let outcome1 = handle_incoming_event(&fallback_unengaged, &[], &config, &matrix, &xmsg, &store)
        .await
        .unwrap();
    assert_eq!(
        outcome1,
        BotOutcome::IgnoredNoMention,
        "Fallback in non-engaged thread must be ignored"
    );

    // 2. Engaged thread: fallback reply to bot is forwarded as unaddressed follow
    let engaged_root = "$thread_root_engaged";
    let bot_msg_engaged = "$bot_msg_engaged";
    store
        .record_thread_asker(engaged_root, "@alice:example.org")
        .unwrap();
    store
        .record_bot_message(bot_msg_engaged, engaged_root, 1000)
        .unwrap();

    let fallback_engaged = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$ev_fallback_2".to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        body: "Followup message with thread fallback".to_string(),
        formatted_body: None,
        mentions: None,
        timestamp_ms: 3000,
        thread_root_id: Some(engaged_root.to_string()),
        replaces_event_id: None,
        in_reply_to_event_id: Some(bot_msg_engaged.to_string()),
        is_falling_back: true,
    };
    let outcome2 = handle_incoming_event(&fallback_engaged, &[], &config, &matrix, &xmsg, &store)
        .await
        .unwrap();
    assert_eq!(outcome2, BotOutcome::Replied);

    let sent = xmsg.sent_payloads.lock().unwrap();
    assert_eq!(sent.len(), 1);
    let envelope = &sent[0].2;
    let req_lines = parse_relayed_lines(envelope, "request");
    assert_eq!(req_lines.len(), 1);
    assert_eq!(
        req_lines[0].addressed,
        Some(false),
        "Fallback in engaged thread must have addressed: false"
    );
    assert!(envelope.contains(r#""addressed":false"#));
}

// ---------------------------------------------------------------------------
// Oracle 3:
// A top-level reply to the bot's message => addressed (addressed: true).
// Mutant: only honour replies inside threads => RED.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_3_top_level_reply_to_bot_message_addresses_bot() {
    let config = test_config();
    let matrix = MockMatrixClient::default();
    let xmsg = TestXmsgMock::with_reply("Top level reply answer");
    let store = Store::new_in_memory().unwrap();

    let bot_msg_top = "$bot_msg_top";
    store
        .record_bot_message(bot_msg_top, bot_msg_top, 1000)
        .unwrap();

    let event = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$reply_top_1".to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        body: "Replying directly to your top-level post".to_string(),
        formatted_body: None,
        mentions: None,
        timestamp_ms: 2000,
        thread_root_id: None,
        replaces_event_id: None,
        in_reply_to_event_id: Some(bot_msg_top.to_string()),
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
        "Top-level reply to bot message must have addressed: true"
    );
    assert!(envelope.contains(r#""addressed":true"#));
}

// ---------------------------------------------------------------------------
// Oracle 4:
// A reply to another user's message => not addressed (dropped in unengaged thread).
// Mutant: treat any reply as addressed => RED.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_4_reply_to_another_users_message_not_addressed() {
    let config = test_config();
    let matrix = MockMatrixClient::default();
    let xmsg = TestXmsgMock::with_reply("unused");
    let store = Store::new_in_memory().unwrap();

    let other_user_msg = "$other_user_msg_4";
    let unengaged_thread = "$thread_root_unengaged_4";

    let event = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$reply_to_user_4".to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        body: "Replying to another human user".to_string(),
        formatted_body: None,
        mentions: None,
        timestamp_ms: 2000,
        thread_root_id: Some(unengaged_thread.to_string()),
        replaces_event_id: None,
        in_reply_to_event_id: Some(other_user_msg.to_string()),
        is_falling_back: false,
    };

    let outcome = handle_incoming_event(&event, &[], &config, &matrix, &xmsg, &store)
        .await
        .unwrap();

    assert_eq!(
        outcome,
        BotOutcome::IgnoredNoMention,
        "Reply to another user's message in unengaged thread must be ignored"
    );
    assert!(
        xmsg.sent_payloads.lock().unwrap().is_empty(),
        "Must not forward message to xmsg"
    );
}

// ---------------------------------------------------------------------------
// Oracle 5:
// A reply to the bot from a sender who fails admission => dropped.
// Mutant: bypass admission for replies => RED.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_5_reply_failing_admission_is_dropped() {
    let config = test_config();
    let matrix = MockMatrixClient::default();
    let xmsg = TestXmsgMock::with_reply("unused");
    let store = Store::new_in_memory().unwrap();

    let bot_msg_id = "$bot_msg_5";
    let thread_root = "$thread_root_5";
    store
        .record_bot_message(bot_msg_id, thread_root, 1000)
        .unwrap();

    // Untrusted sender @stranger:example.org sends reply under Admission::Trusted
    let event = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$reply_stranger_5".to_string(),
        sender_mxid: "@stranger:example.org".to_string(),
        body: "Stranger replying to bot message".to_string(),
        formatted_body: None,
        mentions: None,
        timestamp_ms: 2000,
        thread_root_id: Some(thread_root.to_string()),
        replaces_event_id: None,
        in_reply_to_event_id: Some(bot_msg_id.to_string()),
        is_falling_back: false,
    };

    let outcome = handle_incoming_event(&event, &[], &config, &matrix, &xmsg, &store)
        .await
        .unwrap();

    assert_eq!(
        outcome,
        BotOutcome::IgnoredUntrustedUser,
        "Reply from untrusted sender must be dropped under Admission::Trusted"
    );
    assert!(
        xmsg.sent_payloads.lock().unwrap().is_empty(),
        "Must not relay reply from untrusted sender"
    );
}
