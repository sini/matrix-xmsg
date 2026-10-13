use async_trait::async_trait;
use matrix_xmsg::bot::{handle_incoming_event, BotOutcome, IncomingMatrixEvent};
use matrix_xmsg::config::{Admission, Config};
use matrix_xmsg::context::{EventMessage, RelayedLine};
use matrix_xmsg::error::AppError;
use matrix_xmsg::matrix::MockMatrixClient;
use matrix_xmsg::sender_map::map_sender_mxid;
use matrix_xmsg::store::Store;
use matrix_xmsg::xmsg::{SendResponse, XmsgClient};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

#[derive(Default)]
struct TestXmsgMock {
    sent_payloads: Mutex<Vec<(String, String, String)>>, // (expert, from, text)
    send_counter: AtomicUsize,
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

fn test_config() -> Config {
    Config {
        homeserver_url: "https://matrix.example.org".to_string(),
        bot_mxid: "@genie:example.org".to_string(),
        access_token_file: PathBuf::from("/nonexistent/token"),
        rooms: vec!["!support:example.org".to_string()],
        trusted_mxids: vec![
            "@alice:example.org".to_string(),
            "@alice🚀:example.org".to_string(),
            "@trusted:example.org".to_string(),
        ],
        owner_mxid: "@owner:example.org".to_string(),
        admission: Admission::Public,
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
        size_cap_bytes: 1024,
        answer_timeout_secs: 30,
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
// Two MXIDs that map to the same display name stay distinct on their RelayedLines.
// Mutant: sender_id = mapped_name => RED.
// Producer check: delete sender_id at producer => RED.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_relayed_line_distinct_sender_id_for_colliding_mapped_names() {
    let config = test_config();
    let matrix = MockMatrixClient::default();
    let xmsg = TestXmsgMock::default();
    let store = Store::new_in_memory().unwrap();

    let mxid1 = "@alice:example.org";
    let mxid2 = "@alice🚀:example.org";

    // Both MXIDs must map to the same lossy display name
    assert_eq!(
        map_sender_mxid(mxid1),
        map_sender_mxid(mxid2),
        "Precondition: both MXIDs must collide in map_sender_mxid"
    );
    assert_eq!(map_sender_mxid(mxid1), "matrix alice at example.org");

    // 1. First event from mxid1 opens a thread
    let event1 = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$ev_root_1".to_string(),
        sender_mxid: mxid1.to_string(),
        body: "@genie:example.org question from alice 1".to_string(),
        formatted_body: None,
        mentions: None,
        thread_root_id: None,
        in_reply_to_event_id: None,
        replaces_event_id: None,
        timestamp_ms: 1000,
        is_falling_back: false,
    };

    let outcome1 = handle_incoming_event(&event1, &[], &config, &matrix, &xmsg, &store)
        .await
        .unwrap();
    assert_eq!(outcome1, BotOutcome::Forwarded);

    // 2. Second event from mxid2 in the same thread
    let event2 = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$ev_reply_2".to_string(),
        sender_mxid: mxid2.to_string(),
        body: "@genie:example.org reply from alice 2".to_string(),
        formatted_body: None,
        mentions: None,
        thread_root_id: Some("$ev_root_1".to_string()),
        in_reply_to_event_id: None,
        replaces_event_id: None,
        timestamp_ms: 2000,
        is_falling_back: false,
    };

    let history = vec![EventMessage {
        event_id: "$ev_root_1".to_string(),
        sender_mxid: mxid1.to_string(),
        body: "@genie:example.org question from alice 1".to_string(),
        thread_root_id: None,
        timestamp_ms: 1000,
    }];

    let outcome2 = handle_incoming_event(&event2, &history, &config, &matrix, &xmsg, &store)
        .await
        .unwrap();
    assert_eq!(outcome2, BotOutcome::Forwarded);

    let payloads = xmsg.sent_payloads.lock().unwrap();
    assert_eq!(payloads.len(), 2, "Expected 2 forwarded messages");

    // Parse request lines from both envelopes
    let req1 = parse_relayed_lines(&payloads[0].2, "request");
    let req2 = parse_relayed_lines(&payloads[1].2, "request");
    assert_eq!(req1.len(), 1);
    assert_eq!(req2.len(), 1);

    // Display names MUST collide
    assert_eq!(req1[0].sender, req2[0].sender);
    assert_eq!(req1[0].sender, "matrix alice at example.org");

    // Injective sender_ids MUST stay distinct (raw MXIDs)
    assert_ne!(
        req1[0].sender_id, req2[0].sender_id,
        "Oracle violation: colliding display names must not collide in sender_id"
    );
    assert_eq!(req1[0].sender_id, "@alice:example.org");
    assert_eq!(req2[0].sender_id, "@alice🚀:example.org");
}

// ---------------------------------------------------------------------------
// Oracle 2:
// Two MXIDs that map to the same display name stay distinct on the `!trust` control.
// Mutant: sender_id = mapped_name => RED.
// Producer check: delete sender_id at producer => RED.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_trust_control_distinct_sender_id_for_colliding_mapped_names() {
    let config = test_config();
    let matrix = MockMatrixClient::default();
    let xmsg = TestXmsgMock::default();
    let store = Store::new_in_memory().unwrap();

    let mxid1 = "@alice:example.org";
    let mxid2 = "@alice🚀:example.org";
    assert_eq!(map_sender_mxid(mxid1), map_sender_mxid(mxid2));

    // 1. Trusted user 1 issues !trust in thread
    let trust_event_1 = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$ev_trust_1".to_string(),
        sender_mxid: mxid1.to_string(),
        body: "!trust".to_string(),
        formatted_body: None,
        mentions: None,
        thread_root_id: Some("$thread_root".to_string()),
        in_reply_to_event_id: None,
        replaces_event_id: None,
        timestamp_ms: 1000,
        is_falling_back: false,
    };

    let outcome1 = handle_incoming_event(&trust_event_1, &[], &config, &matrix, &xmsg, &store)
        .await
        .unwrap();
    assert_eq!(outcome1, BotOutcome::ControlEmitted);

    // Verify 🔒 reaction sent on event 1
    let reactions = matrix.sent_reactions.lock().unwrap();
    assert_eq!(reactions.len(), 1);
    assert_eq!(reactions[0].room_id, "!support:example.org");
    assert_eq!(reactions[0].event_id, "$ev_trust_1");
    assert_eq!(reactions[0].key, "🔒");
    drop(reactions);

    // 2. Trusted user 2 issues !trust in the same thread (new event)
    let trust_event_2 = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$ev_trust_2".to_string(),
        sender_mxid: mxid2.to_string(),
        body: "!trust".to_string(),
        formatted_body: None,
        mentions: None,
        thread_root_id: Some("$thread_root".to_string()),
        in_reply_to_event_id: None,
        replaces_event_id: None,
        timestamp_ms: 2000,
        is_falling_back: false,
    };

    let outcome2 = handle_incoming_event(&trust_event_2, &[], &config, &matrix, &xmsg, &store)
        .await
        .unwrap();
    assert_eq!(outcome2, BotOutcome::ControlEmitted);

    // Verify 🔒 reaction sent on event 2
    let reactions = matrix.sent_reactions.lock().unwrap();
    assert_eq!(reactions.len(), 2);
    assert_eq!(reactions[1].event_id, "$ev_trust_2");
    assert_eq!(reactions[1].key, "🔒");
    drop(reactions);

    // Inspect xmsg payloads for both !trust controls
    let payloads = xmsg.sent_payloads.lock().unwrap();
    assert_eq!(payloads.len(), 2);

    let ctrl1: serde_json::Value = serde_json::from_str(&payloads[0].2).unwrap();
    let ctrl2: serde_json::Value = serde_json::from_str(&payloads[1].2).unwrap();

    assert_eq!(ctrl1["control"], "trust");
    assert_eq!(ctrl1["tier"], "trusted");
    assert_eq!(ctrl1["thread_id"], "$thread_root");
    assert_eq!(ctrl1["event_id"], "$ev_trust_1");

    assert_eq!(ctrl2["control"], "trust");
    assert_eq!(ctrl2["tier"], "trusted");
    assert_eq!(ctrl2["thread_id"], "$thread_root");
    assert_eq!(ctrl2["event_id"], "$ev_trust_2");

    // Display names collide
    assert_eq!(ctrl1["sender"], ctrl2["sender"]);
    assert_eq!(ctrl1["sender"], "matrix alice at example.org");

    // Injective sender_ids MUST stay distinct
    assert_ne!(
        ctrl1["sender_id"], ctrl2["sender_id"],
        "Oracle violation: !trust sender_ids must stay distinct despite display name collision"
    );
    assert_eq!(ctrl1["sender_id"], "@alice:example.org");
    assert_eq!(ctrl2["sender_id"], "@alice🚀:example.org");
}

// ---------------------------------------------------------------------------
// Oracle 3:
// Public / untrusted user cannot issue !trust (dropped silently).
// Mutant: accept !trust from any user => RED.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_trust_control_untrusted_sender_dropped() {
    let config = test_config();
    let matrix = MockMatrixClient::default();
    let xmsg = TestXmsgMock::default();
    let store = Store::new_in_memory().unwrap();

    let untrusted_event = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$ev_untrusted_trust".to_string(),
        sender_mxid: "@mallory:evil.org".to_string(),
        body: "!trust".to_string(),
        formatted_body: None,
        mentions: None,
        thread_root_id: Some("$thread_root".to_string()),
        in_reply_to_event_id: None,
        replaces_event_id: None,
        timestamp_ms: 1000,
        is_falling_back: false,
    };

    let outcome = handle_incoming_event(&untrusted_event, &[], &config, &matrix, &xmsg, &store)
        .await
        .unwrap();

    assert_eq!(
        outcome,
        BotOutcome::IgnoredUntrustedUser,
        "Untrusted !trust must be dropped with IgnoredUntrustedUser"
    );

    // No xmsg payload must be emitted
    assert!(xmsg.sent_payloads.lock().unwrap().is_empty());
    // No lock reaction must be sent
    assert!(matrix.sent_reactions.lock().unwrap().is_empty());
}

// ---------------------------------------------------------------------------
// Oracle 4:
// Duplicate !trust event is debounced.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_trust_control_debounced() {
    let config = test_config();
    let matrix = MockMatrixClient::default();
    let xmsg = TestXmsgMock::default();
    let store = Store::new_in_memory().unwrap();

    let trust_event = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$ev_trust_dup".to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        body: "!trust".to_string(),
        formatted_body: None,
        mentions: None,
        thread_root_id: Some("$thread_root".to_string()),
        in_reply_to_event_id: None,
        replaces_event_id: None,
        timestamp_ms: 1000,
        is_falling_back: false,
    };

    let outcome1 = handle_incoming_event(&trust_event, &[], &config, &matrix, &xmsg, &store)
        .await
        .unwrap();
    assert_eq!(outcome1, BotOutcome::ControlEmitted);

    let outcome2 = handle_incoming_event(&trust_event, &[], &config, &matrix, &xmsg, &store)
        .await
        .unwrap();
    assert_eq!(
        outcome2,
        BotOutcome::ControlDebounced,
        "Duplicate !trust event must be debounced"
    );

    // Exactly 1 message and 1 reaction sent
    assert_eq!(xmsg.sent_payloads.lock().unwrap().len(), 1);
    assert_eq!(matrix.sent_reactions.lock().unwrap().len(), 1);
}

// ---------------------------------------------------------------------------
// Oracle 5:
// Quoted history context lines also carry the injective sender_id.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_context_history_line_carries_injective_sender_id() {
    let config = test_config();
    let matrix = MockMatrixClient::default();
    let xmsg = TestXmsgMock::default();
    let store = Store::new_in_memory().unwrap();

    let mxid = "@alice🚀:example.org";

    let trigger = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$ev_reply".to_string(),
        sender_mxid: "@trusted:example.org".to_string(),
        body: "@genie:example.org current query".to_string(),
        formatted_body: None,
        mentions: None,
        thread_root_id: Some("$ev_root".to_string()),
        in_reply_to_event_id: None,
        replaces_event_id: None,
        timestamp_ms: 2000,
        is_falling_back: false,
    };

    let history = vec![EventMessage {
        event_id: "$ev_root".to_string(),
        sender_mxid: mxid.to_string(),
        body: "prior chat from unicode alice".to_string(),
        thread_root_id: None,
        timestamp_ms: 1000,
    }];

    let outcome = handle_incoming_event(&trigger, &history, &config, &matrix, &xmsg, &store)
        .await
        .unwrap();
    assert_eq!(outcome, BotOutcome::Forwarded);

    let payloads = xmsg.sent_payloads.lock().unwrap();
    let ctx_lines = parse_relayed_lines(&payloads[0].2, "context");
    assert_eq!(ctx_lines.len(), 1);
    assert_eq!(ctx_lines[0].sender, "matrix alice at example.org");
    assert_eq!(
        ctx_lines[0].sender_id, mxid,
        "Context line must carry the injective raw MXID"
    );
}
