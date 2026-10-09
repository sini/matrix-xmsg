use async_trait::async_trait;
use matrix_xmsg::bot::{
    format_expert_reply, handle_incoming_event, handle_incoming_reaction, BotOutcome,
    IncomingMatrixEvent, IncomingReactionEvent,
};
use matrix_xmsg::config::{Admission, Config};
use matrix_xmsg::error::AppError;
use matrix_xmsg::matrix::MockMatrixClient;
use matrix_xmsg::store::Store;
use matrix_xmsg::xmsg::{SendResponse, XmsgClient};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

#[derive(Default)]
struct TestXmsgMock {
    sent_payloads: Mutex<Vec<(String, String, String)>>,
    send_counter: AtomicUsize,
}

impl TestXmsgMock {
    fn with_reply(_reply: &str) -> Self {
        Self::default()
    }
}

#[async_trait]
impl XmsgClient for TestXmsgMock {
    async fn send_message(
        &self,
        expert_ref: &str,
        from: &str,
        text: &str,
    ) -> Result<SendResponse, AppError> {
        let mut list = self.sent_payloads.lock().unwrap();
        list.push((expert_ref.to_string(), from.to_string(), text.to_string()));
        let id_num = self.send_counter.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(format!("01MOCKMSG{id_num:06}").into())
    }
}

#[derive(Default)]
struct TestInboxMock {
    acked: Vec<String>,
}

#[async_trait]
impl matrix_xmsg::xmsg::SvcInbox for TestInboxMock {
    async fn poll(
        &mut self,
        _wait_secs: u64,
    ) -> Result<Option<matrix_xmsg::xmsg::SvcDelivery>, AppError> {
        Ok(None)
    }

    async fn ack(&mut self, message_id: &str) -> Result<(), AppError> {
        self.acked.push(message_id.to_string());
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
        resync_byte_cap: 65536,
        rate_limit_count: 10,
        rate_limit_window_secs: 60,
        size_cap_bytes: 1024,
        answer_timeout_secs: 10,
        session_live_secs: 3600,
        db_path: PathBuf::from(":memory:"),
    }
}

// ---------------------------------------------------------------------------
// Oracle 1:
// An addressed message accepted by xmsg gets exactly one 👀 reaction on its event.
// Mutant: no reaction => RED.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_1_addressed_accepted_gets_eye_reaction() {
    let config = test_config();
    let matrix = MockMatrixClient::default();
    let xmsg = TestXmsgMock::with_reply("Here is the solution to your issue.");
    let store = Store::new_in_memory().unwrap();

    let event = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$ev_ask_1".to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        body: "@genie:example.org How do I configure flakes?".to_string(),
        formatted_body: None,
        mentions: Some(vec!["@genie:example.org".to_string()]),
        timestamp_ms: 1000,
        thread_root_id: None,
        replaces_event_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
    };

    let outcome = handle_incoming_event(&event, &[], &config, &matrix, &xmsg, &store)
        .await
        .unwrap();

    assert_eq!(outcome, BotOutcome::Forwarded);

    let reactions = matrix.sent_reactions.lock().unwrap();
    assert_eq!(
        reactions.len(),
        1,
        "must have exactly 1 reaction on accepted addressed message"
    );
    assert_eq!(reactions[0].room_id, "!support:example.org");
    assert_eq!(reactions[0].event_id, "$ev_ask_1");
    assert_eq!(reactions[0].key, "👀");
}

// ---------------------------------------------------------------------------
// Oracle 2:
// An unaddressed follow, and a message dropped by admission, get no reaction.
// Mutant: react on every relay attempt => RED.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_2_unaddressed_and_dropped_get_no_reaction() {
    let config = test_config();
    let matrix = MockMatrixClient::default();
    let xmsg = TestXmsgMock::default();
    let store = Store::new_in_memory().unwrap();

    // 1. Establish engaged thread via initial addressed question
    let init_event = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$root_1".to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        body: "@genie:example.org initial question".to_string(),
        formatted_body: None,
        mentions: Some(vec!["@genie:example.org".to_string()]),
        timestamp_ms: 1000,
        thread_root_id: None,
        replaces_event_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
    };
    let init_outcome = handle_incoming_event(&init_event, &[], &config, &matrix, &xmsg, &store)
        .await
        .unwrap();
    assert_eq!(init_outcome, BotOutcome::Forwarded);

    // Clear reactions from initial question
    matrix.sent_reactions.lock().unwrap().clear();

    // 2. Unaddressed follow in engaged thread (relayed to xmsg with addressed: false)
    let follow_event = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$follow_1".to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        body: "unaddressed follow-up in thread".to_string(),
        formatted_body: None,
        mentions: None,
        timestamp_ms: 2000,
        thread_root_id: Some("$root_1".to_string()),
        replaces_event_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
    };
    let follow_outcome = handle_incoming_event(&follow_event, &[], &config, &matrix, &xmsg, &store)
        .await
        .unwrap();
    assert_eq!(follow_outcome, BotOutcome::Forwarded);

    let reactions_after_follow = matrix.sent_reactions.lock().unwrap().clone();
    assert_eq!(
        reactions_after_follow.len(),
        0,
        "unaddressed follow in engaged thread must get NO reaction"
    );

    // 3. Message dropped by admission (stranger under Admission::Trusted)
    let stranger_event = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$stranger_1".to_string(),
        sender_mxid: "@stranger:example.org".to_string(),
        body: "@genie:example.org question from untrusted user".to_string(),
        formatted_body: None,
        mentions: Some(vec!["@genie:example.org".to_string()]),
        timestamp_ms: 3000,
        thread_root_id: None,
        replaces_event_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
    };
    let drop_outcome = handle_incoming_event(&stranger_event, &[], &config, &matrix, &xmsg, &store)
        .await
        .unwrap();
    assert_eq!(drop_outcome, BotOutcome::IgnoredUntrustedUser);

    let reactions_after_drop = matrix.sent_reactions.lock().unwrap().clone();
    assert_eq!(
        reactions_after_drop.len(),
        0,
        "message dropped by admission gate must get NO reaction"
    );
}

// ---------------------------------------------------------------------------
// Oracle 3:
// A reply arriving after answer_timeout_secs is posted in the thread,
// and the owner DM was still sent at the timeout.
// Mutant: stop waiting at the timeout (today) => RED.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_3_late_answer_posted_and_owner_dmed() {
    let mut config = test_config();
    config.answer_timeout_secs = 5;

    let matrix = MockMatrixClient::default();
    let xmsg = TestXmsgMock::default();
    let store = Store::new_in_memory().unwrap();

    let event = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$ev_late_1".to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        body: "@genie:example.org question with late answer".to_string(),
        formatted_body: None,
        mentions: Some(vec!["@genie:example.org".to_string()]),
        timestamp_ms: 1000,
        thread_root_id: None,
        replaces_event_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
    };

    let outcome = handle_incoming_event(&event, &[], &config, &matrix, &xmsg, &store)
        .await
        .unwrap();

    assert_eq!(outcome, BotOutcome::Forwarded);

    // Timeout sweep sends owner DM at answer_timeout_secs
    let swept = matrix_xmsg::bot::sweep_answer_timeouts(&config, &matrix, &store, 1 + 10)
        .await
        .unwrap();
    assert_eq!(swept, 1);

    // Verify owner DM was sent at timeout
    {
        let dms = matrix.sent_dms.lock().unwrap();
        assert_eq!(dms.len(), 1, "owner DM must be sent at answer_timeout_secs");
        assert_eq!(dms[0].user_id, "@owner:example.org");
        assert!(
            dms[0]
                .body
                .contains("Answer timeout in room !support:example.org"),
            "DM body must indicate answer timeout: {}",
            dms[0].body
        );
    }

    // Late answer arrives in inbox and is posted in thread
    let delivery = matrix_xmsg::xmsg::SvcDelivery {
        message_id: "01M4LATE".to_string(),
        from_name: "claude".to_string(),
        text: "Late answer arrived before deadline.".to_string(),
        envelope: "[xmsg] reply to message_id=01MOCKMSG000001 — message_id=01M4LATE; reply with the xmsg reply tool\n\nLate answer arrived before deadline.".to_string(),
    };
    let mut inbox = TestInboxMock::default();
    let reply_outcome = matrix_xmsg::bot::handle_inbox_delivery(
        &delivery,
        &config,
        &matrix,
        &store,
        &mut inbox,
        1 + 20,
    )
    .await
    .unwrap();
    assert_eq!(reply_outcome, BotOutcome::Replied);

    // Verify the late answer was posted in the thread
    let notices = matrix.sent_notices.lock().unwrap();
    assert_eq!(notices.len(), 1, "late reply must be posted in thread");
    assert_eq!(notices[0].room_id, "!support:example.org");
    assert_eq!(notices[0].thread_root_id.as_deref(), Some("$ev_late_1"));
    assert_eq!(notices[0].body, "Late answer arrived before deadline.");
}

// ---------------------------------------------------------------------------
// Oracle 4:
// In M13, deadline is retired. Late replies are always posted.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_4_late_reply_always_posted_no_deadline() {
    let mut config = test_config();
    config.answer_timeout_secs = 5;

    let matrix = MockMatrixClient::default();
    let xmsg = TestXmsgMock::default();
    let store = Store::new_in_memory().unwrap();

    let event = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$ev_late_2".to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        body: "@genie:example.org question that completely times out".to_string(),
        formatted_body: None,
        mentions: Some(vec!["@genie:example.org".to_string()]),
        timestamp_ms: 1000,
        thread_root_id: None,
        replaces_event_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
    };

    let outcome = handle_incoming_event(&event, &[], &config, &matrix, &xmsg, &store)
        .await
        .unwrap();
    assert_eq!(outcome, BotOutcome::Forwarded);

    // Timeout sweep at +10s sends owner DM
    let swept = matrix_xmsg::bot::sweep_answer_timeouts(&config, &matrix, &store, 1 + 10)
        .await
        .unwrap();
    assert_eq!(swept, 1);

    // Reply arrives long after former deadline (+7200s)
    let delivery = matrix_xmsg::xmsg::SvcDelivery {
        message_id: "01M4VERYLATE".to_string(),
        from_name: "claude".to_string(),
        text: "Late reply is posted with no deadline.".to_string(),
        envelope: "[xmsg] reply to message_id=01MOCKMSG000001 — message_id=01M4VERYLATE; reply with the xmsg reply tool\n\nLate reply is posted with no deadline.".to_string(),
    };
    let mut inbox = TestInboxMock::default();
    let reply_outcome = matrix_xmsg::bot::handle_inbox_delivery(
        &delivery,
        &config,
        &matrix,
        &store,
        &mut inbox,
        1 + 7200,
    )
    .await
    .unwrap();
    assert_eq!(reply_outcome, BotOutcome::Replied);

    let notices = matrix.sent_notices.lock().unwrap();
    assert_eq!(notices.len(), 1, "reply must be posted with no deadline");
    assert_eq!(notices[0].body, "Late reply is posted with no deadline.");
}

// ---------------------------------------------------------------------------
// Oracle 5:
// A {"silent": true} reply to an addressed message posts no message in the room,
// sends no owner DM, redacts the 👀 and reacts 🫡.
// Mutant 1: render it as a normal reply => RED.
// Mutant 2: keep the 👀 => RED.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_5_silent_decline_redacts_eye_and_salutes() {
    let config = test_config();
    let matrix = MockMatrixClient::default();
    let xmsg = TestXmsgMock::default();
    let store = Store::new_in_memory().unwrap();

    let event = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$ev_decline_1".to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        body: "@genie:example.org casual mention that gets declined".to_string(),
        formatted_body: None,
        mentions: Some(vec!["@genie:example.org".to_string()]),
        timestamp_ms: 1000,
        thread_root_id: None,
        replaces_event_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
    };

    let outcome = handle_incoming_event(&event, &[], &config, &matrix, &xmsg, &store)
        .await
        .unwrap();

    assert_eq!(outcome, BotOutcome::Forwarded);

    // Initial reaction is 👀
    {
        let reactions = matrix.sent_reactions.lock().unwrap();
        assert_eq!(reactions.len(), 1);
        assert_eq!(reactions[0].key, "👀");
        assert_eq!(reactions[0].event_id, "$ev_decline_1");
    }

    // Inbox delivery with silent: true arrives
    let delivery = matrix_xmsg::xmsg::SvcDelivery {
        message_id: "01M4DECLINE".to_string(),
        from_name: "claude".to_string(),
        text: r#"{"silent": true}"#.to_string(),
        envelope: r#"[xmsg] reply to message_id=01MOCKMSG000001 — message_id=01M4DECLINE; reply with the xmsg reply tool

{"silent": true}"#.to_string(),
    };
    let mut inbox = TestInboxMock::default();
    let decline_outcome = matrix_xmsg::bot::handle_inbox_delivery(
        &delivery,
        &config,
        &matrix,
        &store,
        &mut inbox,
        1000 / 1000 + 1,
    )
    .await
    .unwrap();

    assert_eq!(decline_outcome, BotOutcome::Declined);

    // 1. Posts no message in the room
    assert_eq!(
        matrix.sent_notices.lock().unwrap().len(),
        0,
        "silent decline must post no notice in room"
    );

    // 2. Sends no owner DM
    assert_eq!(
        matrix.sent_dms.lock().unwrap().len(),
        0,
        "silent decline must send no owner DM"
    );

    // 3. Reacted 👀 first, then 🫡
    let reactions = matrix.sent_reactions.lock().unwrap();
    assert_eq!(reactions.len(), 2, "must send 👀 then 🫡");
    assert_eq!(reactions[0].key, "👀");
    assert_eq!(reactions[0].event_id, "$ev_decline_1");
    assert_eq!(reactions[1].key, "🫡");
    assert_eq!(reactions[1].event_id, "$ev_decline_1");

    // 4. Redacted the initial 👀 reaction
    let redacted = matrix.redacted_events.lock().unwrap();
    assert_eq!(
        redacted.len(),
        1,
        "must redact exactly the initial 👀 reaction"
    );
    assert_eq!(
        redacted[0].event_id, "$mock_reaction_1",
        "redacted event must match the ack reaction id"
    );

    // Direct check of format_expert_reply contract
    assert!(
        format_expert_reply(r#"{"silent": true}"#).is_none(),
        "format_expert_reply must return None for silent: true"
    );
}

// ---------------------------------------------------------------------------
// Oracle 6:
// A reaction sent by the bot itself (👀, 🫡) is never treated as a control.
// Mutant: drop the self-sender guard on the reaction path => RED.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_6_bot_own_reaction_ignored() {
    let config = test_config();
    let xmsg = TestXmsgMock::with_reply("ack");
    let store = Store::new_in_memory().unwrap();

    // Reaction sent by the bot itself with 👀
    let rx_eye = IncomingReactionEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$rx_1".to_string(),
        sender_mxid: config.bot_mxid.clone(),
        relates_to_event_id: "$ev_target".to_string(),
        key: "👀".to_string(),
        timestamp_ms: 1000,
    };
    let outcome_eye = handle_incoming_reaction(&rx_eye, &config, &xmsg, &store)
        .await
        .unwrap();
    assert_eq!(
        outcome_eye,
        BotOutcome::IgnoredSelf,
        "bot's own 👀 reaction must be IgnoredSelf"
    );

    // Reaction sent by the bot itself with 🫡
    let rx_salute = IncomingReactionEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$rx_2".to_string(),
        sender_mxid: config.bot_mxid.clone(),
        relates_to_event_id: "$ev_target".to_string(),
        key: "🫡".to_string(),
        timestamp_ms: 1001,
    };
    let outcome_salute = handle_incoming_reaction(&rx_salute, &config, &xmsg, &store)
        .await
        .unwrap();
    assert_eq!(
        outcome_salute,
        BotOutcome::IgnoredSelf,
        "bot's own 🫡 reaction must be IgnoredSelf"
    );

    // Reaction sent by the bot itself with ✅ (control emoji)
    let rx_check = IncomingReactionEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$rx_3".to_string(),
        sender_mxid: config.bot_mxid.clone(),
        relates_to_event_id: "$ev_target".to_string(),
        key: "✅".to_string(),
        timestamp_ms: 1002,
    };
    let outcome_check = handle_incoming_reaction(&rx_check, &config, &xmsg, &store)
        .await
        .unwrap();
    assert_eq!(
        outcome_check,
        BotOutcome::IgnoredSelf,
        "bot's own control reaction must be IgnoredSelf"
    );
}
