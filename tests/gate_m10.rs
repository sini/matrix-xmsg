use async_trait::async_trait;
use matrix_xmsg::bot::{handle_incoming_event, BotOutcome, IncomingMatrixEvent};
use matrix_xmsg::config::{Admission, Config};
use matrix_xmsg::context::EventMessage;
use matrix_xmsg::error::AppError;
use matrix_xmsg::matrix::MockMatrixClient;
use matrix_xmsg::store::Store;
use matrix_xmsg::xmsg::XmsgClient;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

struct TestXmsgMock {
    canned_replies: Mutex<VecDeque<Result<String, AppError>>>,
    sent_payloads: Mutex<Vec<(String, String, String)>>,
    send_counter: AtomicUsize,
    poll_calls: Mutex<Vec<(String, u64)>>,
}

impl TestXmsgMock {
    fn new(replies: Vec<Result<String, AppError>>) -> Self {
        Self {
            canned_replies: Mutex::new(VecDeque::from(replies)),
            sent_payloads: Mutex::new(Vec::new()),
            send_counter: AtomicUsize::new(0),
            poll_calls: Mutex::new(Vec::new()),
        }
    }

    fn with_reply(reply: &str) -> Self {
        Self::new(vec![Ok(reply.to_string())])
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
        message_id: &str,
        timeout_secs: u64,
    ) -> Result<String, AppError> {
        self.poll_calls
            .lock()
            .unwrap()
            .push((message_id.to_string(), timeout_secs));
        let mut guard = self.canned_replies.lock().unwrap();
        if let Some(res) = guard.pop_front() {
            res
        } else {
            Err(AppError::Timeout(timeout_secs))
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
        answer_timeout_secs: 10,
        answer_deadline_secs: 60,
        session_live_secs: 3600,
        db_path: PathBuf::from(":memory:"),
    }
}

// ---------------------------------------------------------------------------
// Oracle 1:
// Original "@gini …" (no mention) then an edit to "@genie …" by the same sender
// => relayed once, with the edited text.
// M9's 👀 reaction goes on the ORIGINAL event ID ($orig_1).
// Mutant: drop every edit => RED.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_1_typo_fix_edit_triggers_once_with_edited_text() {
    let config = test_config();
    let matrix = MockMatrixClient::default();
    let xmsg = TestXmsgMock::with_reply("Fixed typo reply");
    let store = Store::new_in_memory().unwrap();

    // The homeserver holds the original event message
    let orig_msg = EventMessage {
        event_id: "$orig_1".to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        timestamp_ms: 1000,
        body: "@gini how do I configure flakes?".to_string(),
        thread_root_id: None,
    };
    matrix
        .canned_events
        .lock()
        .unwrap()
        .insert("$orig_1".to_string(), orig_msg);

    // 1. Original event: "@gini how do I configure flakes?" (no bot mention)
    let orig_event = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$orig_1".to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        body: "@gini how do I configure flakes?".to_string(),
        formatted_body: None,
        mentions: None,
        timestamp_ms: 1000,
        thread_root_id: None,
        replaces_event_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
    };

    let orig_outcome = handle_incoming_event(&orig_event, &[], &config, &matrix, &xmsg, &store)
        .await
        .unwrap();
    assert_eq!(orig_outcome, BotOutcome::IgnoredNoMention);
    assert_eq!(xmsg.sent_payloads.lock().unwrap().len(), 0);

    // Assertion: a plain non-edit message writes nothing to the store beyond what 285d1b9 wrote
    assert_eq!(
        store.total_row_count().unwrap(),
        0,
        "A plain non-edit message writes nothing to the store"
    );
    assert!(!store.is_event_relayed("$orig_1").unwrap());

    // 2. Edit event: fixes typo to "@genie:example.org how do I configure flakes?"
    let edit_event = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$edit_1".to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        body: "@genie:example.org how do I configure flakes?".to_string(),
        formatted_body: None,
        mentions: Some(vec!["@genie:example.org".to_string()]),
        timestamp_ms: 2000,
        thread_root_id: None,
        replaces_event_id: Some("$orig_1".to_string()),
        in_reply_to_event_id: None,
        is_falling_back: false,
    };

    let edit_outcome = handle_incoming_event(&edit_event, &[], &config, &matrix, &xmsg, &store)
        .await
        .unwrap();
    assert_eq!(edit_outcome, BotOutcome::Replied);

    let sent = xmsg.sent_payloads.lock().unwrap();
    assert_eq!(sent.len(), 1, "Must be relayed exactly once");
    assert!(
        sent[0]
            .2
            .contains("@genie:example.org how do I configure flakes?"),
        "Payload must contain edited text"
    );
    assert!(
        !sent[0].2.contains("@gini how do I configure flakes?"),
        "Payload must not use old unedited text as trigger message"
    );

    // M9's 👀 must be on the ORIGINAL event ID ($orig_1)
    let reactions = matrix.sent_reactions.lock().unwrap();
    assert_eq!(reactions.len(), 1, "Exactly one reaction must be sent");
    assert_eq!(
        reactions[0].event_id, "$orig_1",
        "Reaction must target original event ID"
    );
    assert_eq!(reactions[0].key, "👀");
}

// ---------------------------------------------------------------------------
// Oracle 2:
// An original that already mentioned genie (relayed), then an edit => nothing relayed.
// Mutant: relay every edit that mentions => RED.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_2_edit_to_already_relayed_event_is_ignored() {
    let config = test_config();
    let matrix = MockMatrixClient::default();
    let xmsg = TestXmsgMock::with_reply("Original reply");
    let store = Store::new_in_memory().unwrap();

    // 1. Original event mentions bot and is relayed
    let orig_event = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$orig_2".to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        body: "@genie:example.org Initial question".to_string(),
        formatted_body: None,
        mentions: Some(vec!["@genie:example.org".to_string()]),
        timestamp_ms: 1000,
        thread_root_id: None,
        replaces_event_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
    };

    let orig_outcome = handle_incoming_event(&orig_event, &[], &config, &matrix, &xmsg, &store)
        .await
        .unwrap();
    assert_eq!(orig_outcome, BotOutcome::Replied);
    assert_eq!(xmsg.sent_payloads.lock().unwrap().len(), 1);

    // Clear sent payloads to observe edit outcome cleanly
    xmsg.sent_payloads.lock().unwrap().clear();

    // 2. Edit event targeting $orig_2
    let edit_event = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$edit_2".to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        body: "@genie:example.org Initial question (clarified)".to_string(),
        formatted_body: None,
        mentions: Some(vec!["@genie:example.org".to_string()]),
        timestamp_ms: 2000,
        thread_root_id: None,
        replaces_event_id: Some("$orig_2".to_string()),
        in_reply_to_event_id: None,
        is_falling_back: false,
    };

    let edit_outcome = handle_incoming_event(&edit_event, &[], &config, &matrix, &xmsg, &store)
        .await
        .unwrap();
    assert_eq!(edit_outcome, BotOutcome::IgnoredEdit);
    assert_eq!(
        xmsg.sent_payloads.lock().unwrap().len(),
        0,
        "Edit of already-relayed event must not trigger any relay"
    );
}

// ---------------------------------------------------------------------------
// Oracle 3:
// Two successive edits that both mention, original unmentioned => relayed exactly once.
// Mutant: key the claim on the edit's event id => RED.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_3_two_successive_mention_edits_relayed_exactly_once() {
    let config = test_config();
    let matrix = MockMatrixClient::default();
    let xmsg = TestXmsgMock::new(vec![
        Ok("Reply to first edit".to_string()),
        Ok("Reply to second edit".to_string()),
    ]);
    let store = Store::new_in_memory().unwrap();

    let orig_msg = EventMessage {
        event_id: "$orig_3".to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        timestamp_ms: 1000,
        body: "@gini hello".to_string(),
        thread_root_id: None,
    };
    matrix
        .canned_events
        .lock()
        .unwrap()
        .insert("$orig_3".to_string(), orig_msg);

    // 1. Original event: no mention
    let orig_event = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$orig_3".to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        body: "@gini hello".to_string(),
        formatted_body: None,
        mentions: None,
        timestamp_ms: 1000,
        thread_root_id: None,
        replaces_event_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
    };
    let orig_outcome = handle_incoming_event(&orig_event, &[], &config, &matrix, &xmsg, &store)
        .await
        .unwrap();
    assert_eq!(orig_outcome, BotOutcome::IgnoredNoMention);
    assert_eq!(
        store.total_row_count().unwrap(),
        0,
        "Plain non-edit message writes nothing to the store"
    );

    // 2. First edit: mentions bot
    let edit_1 = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$edit_3a".to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        body: "@genie:example.org hello v1".to_string(),
        formatted_body: None,
        mentions: Some(vec!["@genie:example.org".to_string()]),
        timestamp_ms: 2000,
        thread_root_id: None,
        replaces_event_id: Some("$orig_3".to_string()),
        in_reply_to_event_id: None,
        is_falling_back: false,
    };
    let outcome_1 = handle_incoming_event(&edit_1, &[], &config, &matrix, &xmsg, &store)
        .await
        .unwrap();
    assert_eq!(outcome_1, BotOutcome::Replied);

    // 3. Second edit: also mentions bot
    let edit_2 = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$edit_3b".to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        body: "@genie:example.org hello v2".to_string(),
        formatted_body: None,
        mentions: Some(vec!["@genie:example.org".to_string()]),
        timestamp_ms: 3000,
        thread_root_id: None,
        replaces_event_id: Some("$orig_3".to_string()),
        in_reply_to_event_id: None,
        is_falling_back: false,
    };
    let outcome_2 = handle_incoming_event(&edit_2, &[], &config, &matrix, &xmsg, &store)
        .await
        .unwrap();
    assert_eq!(outcome_2, BotOutcome::IgnoredEdit);

    let sent = xmsg.sent_payloads.lock().unwrap();
    assert_eq!(
        sent.len(),
        1,
        "Exactly one relay must occur across both edits"
    );
    assert!(sent[0].2.contains("@genie:example.org hello v1"));
}

// ---------------------------------------------------------------------------
// Oracle 4:
// An edit whose sender differs from the original's => dropped.
// Mutant: skip the same-sender check => RED.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_4_edit_with_different_sender_is_dropped() {
    let mut config = test_config();
    config.trusted_mxids.push("@eve:example.org".to_string());

    let matrix = MockMatrixClient::default();
    let xmsg = TestXmsgMock::with_reply("unused");
    let store = Store::new_in_memory().unwrap();

    let orig_msg = EventMessage {
        event_id: "$orig_4".to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        timestamp_ms: 1000,
        body: "@gini Alice message".to_string(),
        thread_root_id: None,
    };
    matrix
        .canned_events
        .lock()
        .unwrap()
        .insert("$orig_4".to_string(), orig_msg);

    // 1. Original event sent by @alice (unmentioned)
    let orig_event = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$orig_4".to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        body: "@gini Alice message".to_string(),
        formatted_body: None,
        mentions: None,
        timestamp_ms: 1000,
        thread_root_id: None,
        replaces_event_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
    };
    let orig_outcome = handle_incoming_event(&orig_event, &[], &config, &matrix, &xmsg, &store)
        .await
        .unwrap();
    assert_eq!(orig_outcome, BotOutcome::IgnoredNoMention);

    // 2. Edit event sent by @eve targeting @alice's message
    let edit_event = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$edit_4".to_string(),
        sender_mxid: "@eve:example.org".to_string(),
        body: "@genie:example.org Eve malicious edit".to_string(),
        formatted_body: None,
        mentions: Some(vec!["@genie:example.org".to_string()]),
        timestamp_ms: 2000,
        thread_root_id: None,
        replaces_event_id: Some("$orig_4".to_string()),
        in_reply_to_event_id: None,
        is_falling_back: false,
    };

    let edit_outcome = handle_incoming_event(&edit_event, &[], &config, &matrix, &xmsg, &store)
        .await
        .unwrap();
    assert_eq!(edit_outcome, BotOutcome::IgnoredUntrustedUser);
    assert_eq!(
        xmsg.sent_payloads.lock().unwrap().len(),
        0,
        "Edit by different sender must not be relayed"
    );
}

// ---------------------------------------------------------------------------
// Oracle 5:
// An edit that adds the mention but fails admission (untrusted sender in trusted mode) => dropped.
// Mutant: bypass admission for edits => RED.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_5_edit_failing_admission_is_dropped() {
    let config = test_config();
    let matrix = MockMatrixClient::default();
    let xmsg = TestXmsgMock::with_reply("unused");
    let store = Store::new_in_memory().unwrap();

    let orig_msg = EventMessage {
        event_id: "$orig_5".to_string(),
        sender_mxid: "@stranger:example.org".to_string(),
        timestamp_ms: 1000,
        body: "@gini Stranger message".to_string(),
        thread_root_id: None,
    };
    matrix
        .canned_events
        .lock()
        .unwrap()
        .insert("$orig_5".to_string(), orig_msg);

    // 1. Original event sent by untrusted stranger @stranger:example.org (unmentioned)
    let orig_event = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$orig_5".to_string(),
        sender_mxid: "@stranger:example.org".to_string(),
        body: "@gini Stranger message".to_string(),
        formatted_body: None,
        mentions: None,
        timestamp_ms: 1000,
        thread_root_id: None,
        replaces_event_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
    };
    let orig_outcome = handle_incoming_event(&orig_event, &[], &config, &matrix, &xmsg, &store)
        .await
        .unwrap();
    assert_eq!(orig_outcome, BotOutcome::IgnoredNoMention);

    // 2. Stranger edits to add mention
    let edit_event = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$edit_5".to_string(),
        sender_mxid: "@stranger:example.org".to_string(),
        body: "@genie:example.org Stranger edit with mention".to_string(),
        formatted_body: None,
        mentions: Some(vec!["@genie:example.org".to_string()]),
        timestamp_ms: 2000,
        thread_root_id: None,
        replaces_event_id: Some("$orig_5".to_string()),
        in_reply_to_event_id: None,
        is_falling_back: false,
    };

    let edit_outcome = handle_incoming_event(&edit_event, &[], &config, &matrix, &xmsg, &store)
        .await
        .unwrap();
    assert_eq!(edit_outcome, BotOutcome::IgnoredUntrustedUser);
    assert_eq!(
        xmsg.sent_payloads.lock().unwrap().len(),
        0,
        "Edit by untrusted sender under Admission::Trusted must be dropped"
    );
}

// ---------------------------------------------------------------------------
// Oracle 6:
// Original event fetched via Matrix client if not in local store/history.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_6_original_event_fetched_from_matrix_when_not_in_store() {
    let config = test_config();
    let matrix = MockMatrixClient::default();
    let xmsg = TestXmsgMock::with_reply("Fetched original reply");
    let store = Store::new_in_memory().unwrap();

    let orig_msg = EventMessage {
        event_id: "$orig_fetch".to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        timestamp_ms: 1000,
        body: "@gini fetched question".to_string(),
        thread_root_id: None,
    };
    matrix
        .canned_events
        .lock()
        .unwrap()
        .insert("$orig_fetch".to_string(), orig_msg);

    // Edit arrives
    let edit_event = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$edit_fetch".to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        body: "@genie:example.org fetched question".to_string(),
        formatted_body: None,
        mentions: Some(vec!["@genie:example.org".to_string()]),
        timestamp_ms: 2000,
        thread_root_id: None,
        replaces_event_id: Some("$orig_fetch".to_string()),
        in_reply_to_event_id: None,
        is_falling_back: false,
    };

    let edit_outcome = handle_incoming_event(&edit_event, &[], &config, &matrix, &xmsg, &store)
        .await
        .unwrap();
    assert_eq!(edit_outcome, BotOutcome::Replied);
    assert_eq!(xmsg.sent_payloads.lock().unwrap().len(), 1);
}

// ---------------------------------------------------------------------------
// Oracle 7:
// A plain non-edit message writes nothing to the store (cached_events removed,
// ensuring no room message retention on disk).
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_7_plain_non_edit_message_writes_nothing_to_store() {
    let config = test_config();
    let matrix = MockMatrixClient::default();
    let xmsg = TestXmsgMock::with_reply("unused");
    let store = Store::new_in_memory().unwrap();

    let initial_rows = store.total_row_count().unwrap();
    assert_eq!(initial_rows, 0);

    let event = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$plain_msg_1".to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        body: "just regular public conversation with no bot mention".to_string(),
        formatted_body: None,
        mentions: None,
        timestamp_ms: 1000,
        thread_root_id: None,
        replaces_event_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
    };

    let outcome = handle_incoming_event(&event, &[], &config, &matrix, &xmsg, &store)
        .await
        .unwrap();
    assert_eq!(outcome, BotOutcome::IgnoredNoMention);

    let final_rows = store.total_row_count().unwrap();
    assert_eq!(
        final_rows, initial_rows,
        "Plain non-edit message must write 0 rows to the store"
    );
    assert!(
        !store.is_event_relayed("$plain_msg_1").unwrap(),
        "Plain message must not be recorded as relayed"
    );
}
