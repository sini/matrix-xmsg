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

struct TestXmsgMock {
    canned_reply: Mutex<Result<String, AppError>>,
    sent_payloads: Mutex<Vec<(String, String, String)>>, // (expert, from, text)
    send_counter: AtomicUsize,
}

impl TestXmsgMock {
    fn new() -> Self {
        Self {
            canned_reply: Mutex::new(Ok("canned".to_string())),
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
    ) -> Result<SendResponse, AppError> {
        let mut list = self.sent_payloads.lock().unwrap();
        list.push((expert_ref.to_string(), from.to_string(), text.to_string()));
        let id_num = self.send_counter.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(format!("01MOCKMSG{id_num:06}").into())
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
            "@trusted:example.org".to_string(),
            "@admin:example.org".to_string(),
        ],
        owner_mxid: "@owner:example.org".to_string(),
        admission: Admission::Trusted,
        xmsg_url: "http://127.0.0.1:7787".to_string(),
        xmsg_socket: None,
        expert_ref: "claude".to_string(),
        history_n: 5,
        history_byte_cap: 1024,
        rate_limit_count: 5,
        rate_limit_window_secs: 60,
        size_cap_bytes: 500,
        answer_timeout_secs: 30,
        answer_deadline_secs: 3600,
        session_live_secs: 3600,
        db_path: PathBuf::from(":memory:"),
    }
}

// ---------------------------------------------------------------------------
// Oracle 1:
// 🔍 from a stranger => 0 events; from the asker => 1; from a trusted MXID => 1.
// Mutant: accept anyone.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_1_control_authorization() {
    let config = test_config();
    let xmsg = TestXmsgMock::new();
    let store = Store::new_in_memory().unwrap();
    let matrix = MockMatrixClient::default();

    let thread_id = "$thread_100";
    let bot_msg_id = "$bot_msg_100";

    // Set up recorded thread with asker and a bot message
    store
        .record_thread_asker(thread_id, "@asker:example.org")
        .unwrap();
    store
        .record_bot_message(bot_msg_id, thread_id, 1000)
        .unwrap();

    // 1A. 🔍 from a stranger => 0 events
    let reaction_stranger = IncomingReactionEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$react_stranger".to_string(),
        sender_mxid: "@stranger:example.org".to_string(),
        relates_to_event_id: bot_msg_id.to_string(),
        key: "🔍".to_string(),
        timestamp_ms: 1000,
    };
    let outcome = handle_incoming_reaction(&reaction_stranger, &config, &xmsg, &store)
        .await
        .unwrap();
    assert_eq!(outcome, BotOutcome::IgnoredUntrustedUser);
    assert_eq!(
        xmsg.sent_payloads.lock().unwrap().len(),
        0,
        "stranger reaction must emit 0 events"
    );

    // 1B. 🔍 from the original asker => 1 event
    let reaction_asker = IncomingReactionEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$react_asker".to_string(),
        sender_mxid: "@asker:example.org".to_string(),
        relates_to_event_id: bot_msg_id.to_string(),
        key: "🔍".to_string(),
        timestamp_ms: 2000,
    };
    let outcome = handle_incoming_reaction(&reaction_asker, &config, &xmsg, &store)
        .await
        .unwrap();
    assert_eq!(outcome, BotOutcome::ControlEmitted);
    {
        let sent = xmsg.sent_payloads.lock().unwrap();
        assert_eq!(sent.len(), 1, "asker reaction must emit 1 event");
        let payload: serde_json::Value = serde_json::from_str(&sent[0].2).unwrap();
        assert_eq!(payload["thread_id"], thread_id);
        assert_eq!(payload["control"], "deeper");
        assert_eq!(payload["by"], "@asker:example.org");
    }

    // 1C. 🔍 from a trusted MXID => 1 event (total 2)
    let reaction_trusted = IncomingReactionEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$react_trusted".to_string(),
        sender_mxid: "@trusted:example.org".to_string(),
        relates_to_event_id: bot_msg_id.to_string(),
        key: "🔍".to_string(),
        timestamp_ms: 3000,
    };
    let outcome = handle_incoming_reaction(&reaction_trusted, &config, &xmsg, &store)
        .await
        .unwrap();
    assert_eq!(outcome, BotOutcome::ControlEmitted);
    {
        let sent = xmsg.sent_payloads.lock().unwrap();
        assert_eq!(
            sent.len(),
            2,
            "trusted MXID reaction must emit 1 event (total 2)"
        );
        let payload: serde_json::Value = serde_json::from_str(&sent[1].2).unwrap();
        assert_eq!(payload["thread_id"], thread_id);
        assert_eq!(payload["control"], "deeper");
        assert_eq!(payload["by"], "@trusted:example.org");
    }

    // 1D. Also verify text fallback `!deeper` in the thread:
    // Stranger !deeper => 0 new events
    let deeper_stranger = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$msg_deeper_stranger".to_string(),
        sender_mxid: "@stranger:example.org".to_string(),
        body: "!deeper".to_string(),
        formatted_body: None,
        mentions: None,
        timestamp_ms: 4000,
        thread_root_id: Some(thread_id.to_string()),
        replaces_event_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
    };
    let outcome = handle_incoming_event(&deeper_stranger, &[], &config, &matrix, &xmsg, &store)
        .await
        .unwrap();
    assert_eq!(outcome, BotOutcome::IgnoredUntrustedUser);
    assert_eq!(
        xmsg.sent_payloads.lock().unwrap().len(),
        2,
        "stranger !deeper must emit 0 events"
    );
}

// ---------------------------------------------------------------------------
// Oracle 2:
// A duplicate ✅ from the same user => 1 event.
// Mutant: no debounce.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_2_duplicate_debounce() {
    let config = test_config();
    let xmsg = TestXmsgMock::new();
    let store = Store::new_in_memory().unwrap();

    let thread_id = "$thread_200";
    let bot_msg_id = "$bot_msg_200";

    store
        .record_thread_asker(thread_id, "@asker:example.org")
        .unwrap();
    store
        .record_bot_message(bot_msg_id, thread_id, 1000)
        .unwrap();

    // First ✅ reaction from asker
    let reaction1 = IncomingReactionEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$react_accept_1".to_string(),
        sender_mxid: "@asker:example.org".to_string(),
        relates_to_event_id: bot_msg_id.to_string(),
        key: "✅".to_string(),
        timestamp_ms: 1000,
    };
    let outcome1 = handle_incoming_reaction(&reaction1, &config, &xmsg, &store)
        .await
        .unwrap();
    assert_eq!(outcome1, BotOutcome::ControlEmitted);
    assert_eq!(
        xmsg.sent_payloads.lock().unwrap().len(),
        1,
        "first reaction must emit 1 event"
    );

    // Duplicate ✅ reaction from same asker on same message
    let reaction2 = IncomingReactionEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$react_accept_2".to_string(),
        sender_mxid: "@asker:example.org".to_string(),
        relates_to_event_id: bot_msg_id.to_string(),
        key: "✅".to_string(),
        timestamp_ms: 2000,
    };
    let outcome2 = handle_incoming_reaction(&reaction2, &config, &xmsg, &store)
        .await
        .unwrap();
    assert_eq!(outcome2, BotOutcome::ControlDebounced);
    assert_eq!(
        xmsg.sent_payloads.lock().unwrap().len(),
        1,
        "duplicate reaction must not emit a second event"
    );
}

// ---------------------------------------------------------------------------
// Oracle 3:
// Render snapshot with confidence and gaps.
// Mutant: drop the confidence line.
// ---------------------------------------------------------------------------
#[test]
fn oracle_3_render_snapshot_confidence_and_gaps() {
    let reply_json = serde_json::json!({
        "answer": "Nix evaluation succeeded with 0 warnings.",
        "confidence": 0.72,
        "gaps": ["missing nixpkgs check", "untested on darwin"],
        "tier": 1
    })
    .to_string();

    let (rendered, is_escalate) = format_expert_reply(&reply_json).unwrap();
    assert!(!is_escalate);

    let expected = "\
Nix evaluation succeeded with 0 warnings.

confidence 0.72 · gaps: missing nixpkgs check, untested on darwin
Controls: ✅ accept · 🔍 deeper · !deeper";

    assert_eq!(
        rendered, expected,
        "rendered output must match snapshot exactly"
    );

    // Also verify tier: expert rendering
    let expert_json = serde_json::json!({
        "answer": "Expert analysis confirms derivation correctness.",
        "confidence": 0.95,
        "gaps": [],
        "tier": "expert"
    })
    .to_string();

    let (expert_rendered, _) = format_expert_reply(&expert_json).unwrap();
    let expected_expert = "\
[expert review]
Expert analysis confirms derivation correctness.

expert review · confidence 0.95 · gaps: none
Controls: ✅ accept · 🔍 deeper · !deeper";

    assert_eq!(
        expert_rendered, expected_expert,
        "expert review must match snapshot"
    );
}

// ---------------------------------------------------------------------------
// Oracle 4:
// A plain-text reply renders byte-identical to the pre-M5 output.
// Mutant: always add the confidence line.
// ---------------------------------------------------------------------------
#[test]
fn oracle_4_plaintext_reply_byte_identical() {
    let pre_m5_sample_1 = "This is a direct answer to your question about Nix overlays.";
    let (rendered_1, is_esc_1) = format_expert_reply(pre_m5_sample_1).unwrap();
    assert_eq!(
        rendered_1, pre_m5_sample_1,
        "plain-text reply must be byte-identical to pre-M5 output"
    );
    assert!(!is_esc_1);

    let pre_m5_sample_2 = "Partial investigation completed. [escalate]";
    let (rendered_2, is_esc_2) = format_expert_reply(pre_m5_sample_2).unwrap();
    assert_eq!(
        rendered_2, "Partial investigation completed.",
        "clean plain-text reply must strip [escalate] and match pre-M5"
    );
    assert!(is_esc_2);
}

// ---------------------------------------------------------------------------
// Oracle 5:
// Under default admission ("trusted"), a stranger's top-level question => 0 relays.
// Under admission = "public", a stranger's top-level question => 1 relay.
// Mutant: ignore the admission option (always require trusted allowlist).
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_5_admission_policy() {
    let stranger_mxid = "@stranger:example.org";
    let stranger_question = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$ev_stranger_question".to_string(),
        sender_mxid: stranger_mxid.to_string(),
        body: "@genie How do I configure flakes?".to_string(),
        formatted_body: None,
        mentions: Some(vec!["@genie:example.org".to_string()]),
        timestamp_ms: 1000,
        thread_root_id: None,
        replaces_event_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
    };

    // 5A. Under default admission (trusted): 0 relays
    {
        let config_default = test_config(); // admission is Trusted
        let xmsg = TestXmsgMock::new();
        let store = Store::new_in_memory().unwrap();
        let matrix = MockMatrixClient::default();

        let outcome = handle_incoming_event(
            &stranger_question,
            &[],
            &config_default,
            &matrix,
            &xmsg,
            &store,
        )
        .await
        .unwrap();

        assert_eq!(outcome, BotOutcome::IgnoredUntrustedUser);
        assert_eq!(
            xmsg.sent_payloads.lock().unwrap().len(),
            0,
            "stranger question under default (trusted) admission must emit 0 relays"
        );
    }

    // 5B. Under admission = "public": 1 relay
    {
        let mut config_public = test_config();
        config_public.admission = Admission::Public;
        let xmsg = TestXmsgMock::new();
        let store = Store::new_in_memory().unwrap();
        let matrix = MockMatrixClient::default();

        let outcome = handle_incoming_event(
            &stranger_question,
            &[],
            &config_public,
            &matrix,
            &xmsg,
            &store,
        )
        .await
        .unwrap();

        assert_eq!(outcome, BotOutcome::Replied);
        assert_eq!(
            xmsg.sent_payloads.lock().unwrap().len(),
            1,
            "stranger question under public admission must emit 1 relay"
        );
        let sent = xmsg.sent_payloads.lock().unwrap();
        assert_eq!(sent[0].0, "claude");
        assert!(sent[0].2.contains("How do I configure flakes?"));
    }
}
