use async_trait::async_trait;
use matrix_sdk::config::SyncSettings;
use matrix_xmsg::bot::{
    handle_incoming_event_with_guard, register_event_handlers_with_startup_ts, BotOutcome,
    IncomingMatrixEvent,
};
use matrix_xmsg::config::{Admission, Config, GuardFailureMode};
use matrix_xmsg::context::EventMessage;
use matrix_xmsg::error::AppError;
use matrix_xmsg::matrix::{MatrixClient, MatrixSdkClient, MockMatrixClient};
use matrix_xmsg::store::Store;
use matrix_xmsg::xmsg::{MockGuardClient, SendResponse, XmsgClient};
use serde_json::json;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

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
            session_id: Mutex::new(Some("session-m39".to_string())),
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
        Ok(SendResponse::new(format!("01M39MSG{id:06}"), sess))
    }

    async fn reply_message(&self, message_id: &str, text: &str) -> Result<SendResponse, AppError> {
        self.replies
            .lock()
            .unwrap()
            .push((message_id.to_string(), text.to_string()));
        let id = self.counter.fetch_add(1, Ordering::SeqCst) + 1;
        let sess = self.session_id.lock().unwrap().clone();
        Ok(SendResponse::new(format!("01M39REPLY{id:06}"), sess))
    }
}

fn test_config(admission: Admission) -> Config {
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
        admission,
        xmsg_socket: PathBuf::from("/run/user/1000/xmsg"),
        expert_ref: "claude".to_string(),
        expert_refs: None,
        room_routes: std::collections::BTreeMap::new(),
        open_thread_nodes: vec![],
        guard_ref: "genie-guard".to_string(),
        guard_timeout_secs: 30,
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

#[allow(clippy::too_many_arguments)]
fn make_event(
    event_id: &str,
    sender: &str,
    body: &str,
    mentioned: bool,
    in_reply_to: Option<&str>,
    thread_root: Option<&str>,
    replaces: Option<&str>,
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
        in_reply_to_event_id: in_reply_to.map(String::from),
        thread_root_id: thread_root.map(String::from),
        replaces_event_id: replaces.map(String::from),
        timestamp_ms: ts_secs * 1000,
        is_falling_back: false,
    }
}

// -------------------------------------------------------------------------------------------------
// O1: Untrusted U posts root. Trusted T replies in thread mentioning genie -> forwarded, recorded
// asker is U. U's follow-up without mention is admitted, forwarded with `addressed: true`, screened,
// reacted 👀.
// Mutants:
//   - drop `is_escalation_asker` from `Admission::Trusted` -> RED
//   - drop `is_escalation_asker` from `is_addressed` -> RED
// -------------------------------------------------------------------------------------------------
#[tokio::test]
async fn o1_escalation_admits_root_author_follow_up_and_marks_addressed() {
    let config = test_config(Admission::Trusted);
    let matrix = MockMatrixClient::default();
    let store = Store::new_in_memory().unwrap();
    let xmsg = MockXmsgClient::default();
    let guard = MockGuardClient::new();

    let root_msg = EventMessage {
        event_id: "$root_o1".to_string(),
        sender_mxid: "@untrusted:example.org".to_string(),
        timestamp_ms: 1_000_000,
        body: "My audio service crashes after suspend".to_string(),
        thread_root_id: None,
    };
    let history = vec![root_msg.clone()];

    // T replies in U's thread mentioning @genie:example.org
    let t_mention = make_event(
        "$t_mention_o1",
        "@trusted:example.org",
        "@genie:example.org please help @untrusted:example.org with this crash",
        true,
        Some("$root_o1"),
        Some("$root_o1"),
        None,
        1_010,
    );
    let out1 = handle_incoming_event_with_guard(
        &t_mention, &history, &config, &matrix, &xmsg, &guard, &store, None,
    )
    .await
    .unwrap();
    assert_eq!(out1, BotOutcome::Forwarded);
    assert_eq!(
        store.get_thread_asker("$root_o1").unwrap().as_deref(),
        Some("@untrusted:example.org"),
        "O1: escalated thread asker in thread_askers must be root author U"
    );
    assert_eq!(
        store
            .get_thread_escalation_asker("$root_o1")
            .unwrap()
            .as_deref(),
        Some("@untrusted:example.org"),
        "O1: thread_escalations must record root author U as asker"
    );
    let esc = store
        .get_thread_escalation("$root_o1")
        .unwrap()
        .expect("O1: thread_escalations row must exist");
    assert_eq!(esc.asker_mxid, "@untrusted:example.org");
    assert_eq!(esc.escalated_by, "@trusted:example.org");

    let guard_calls_after_t = guard.calls.lock().unwrap().len();
    assert_eq!(
        guard_calls_after_t, 1,
        "O1: T's mention with untrusted root in history must be screened"
    );

    // U posts an unmentioned follow-up in the same thread
    let u_followup = make_event(
        "$u_followup_o1",
        "@untrusted:example.org",
        "Here is the journalctl output from right before the crash",
        false,
        Some("$t_mention_o1"),
        Some("$root_o1"),
        None,
        1_020,
    );
    let history2 = vec![
        root_msg,
        EventMessage {
            event_id: "$t_mention_o1".to_string(),
            sender_mxid: "@trusted:example.org".to_string(),
            timestamp_ms: 1_010_000,
            body: "@genie:example.org please help @untrusted:example.org with this crash"
                .to_string(),
            thread_root_id: Some("$root_o1".to_string()),
        },
    ];
    let out2 = handle_incoming_event_with_guard(
        &u_followup,
        &history2,
        &config,
        &matrix,
        &xmsg,
        &guard,
        &store,
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        out2,
        BotOutcome::Forwarded,
        "O1: escalated root author U must be admitted under Admission::Trusted without a mention"
    );

    let sends = xmsg.sends.lock().unwrap();
    assert_eq!(sends.len(), 2);
    let u_envelope = &sends[1].2;
    assert!(
        u_envelope.contains("\"addressed\":true"),
        "O1: U's unmentioned follow-up in an escalated thread must carry \"addressed\":true, got:\n{u_envelope}"
    );
    assert_eq!(
        guard.calls.lock().unwrap().len(),
        2,
        "O1: U's follow-up must be screened by the guard"
    );
    let reactions = matrix.sent_reactions.lock().unwrap();
    assert!(
        reactions
            .iter()
            .any(|r| r.event_id == "$u_followup_o1" && r.key == "👀"),
        "O1: bot must react 👀 on U's follow-up"
    );
}

// -------------------------------------------------------------------------------------------------
// O2: Bystander B posts in the same escalated thread -> IgnoredUntrustedUser, no forward, no reaction.
// Mutant: admit any sender when `get_thread_escalation_asker(root).is_some()` -> RED.
// -------------------------------------------------------------------------------------------------
#[tokio::test]
async fn o2_bystander_in_escalated_thread_is_ignored() {
    let config = test_config(Admission::Trusted);
    let matrix = MockMatrixClient::default();
    let store = Store::new_in_memory().unwrap();
    let xmsg = MockXmsgClient::default();
    let guard = MockGuardClient::new();

    let root_msg = EventMessage {
        event_id: "$root_o2".to_string(),
        sender_mxid: "@untrusted:example.org".to_string(),
        timestamp_ms: 1_000_000,
        body: "Root question from U".to_string(),
        thread_root_id: None,
    };
    let t_mention = make_event(
        "$t_mention_o2",
        "@trusted:example.org",
        "@genie:example.org take a look",
        true,
        Some("$root_o2"),
        Some("$root_o2"),
        None,
        1_010,
    );
    let out1 = handle_incoming_event_with_guard(
        &t_mention,
        std::slice::from_ref(&root_msg),
        &config,
        &matrix,
        &xmsg,
        &guard,
        &store,
        None,
    )
    .await
    .unwrap();
    assert_eq!(out1, BotOutcome::Forwarded);

    // Bystander B posts in the escalated thread (even mentioning genie)
    let b_msg = make_event(
        "$b_msg_o2",
        "@bystander:example.org",
        "@genie:example.org me too!",
        true,
        Some("$t_mention_o2"),
        Some("$root_o2"),
        None,
        1_020,
    );
    let out_b = handle_incoming_event_with_guard(
        &b_msg,
        std::slice::from_ref(&root_msg),
        &config,
        &matrix,
        &xmsg,
        &guard,
        &store,
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        out_b,
        BotOutcome::IgnoredUntrustedUser,
        "O2: untrusted bystander B must be ignored in an escalated thread"
    );
    assert_eq!(
        xmsg.sends.lock().unwrap().len(),
        1,
        "O2: bystander message must not be forwarded"
    );
    assert!(
        !matrix
            .sent_reactions
            .lock()
            .unwrap()
            .iter()
            .any(|r| r.event_id == "$b_msg_o2"),
        "O2: bystander message must receive no reaction"
    );
}

// -------------------------------------------------------------------------------------------------
// O3: U's forwarded line carries `"tier":"public"`, not `"trusted"`.
// Mutant: fold `is_escalation_asker` into `compute_sender_tier` / `effective_thread_tier` -> RED.
// -------------------------------------------------------------------------------------------------
#[tokio::test]
async fn o3_escalated_asker_relayed_line_tier_is_public() {
    let config = test_config(Admission::Trusted);
    let matrix = MockMatrixClient::default();
    let store = Store::new_in_memory().unwrap();
    let xmsg = MockXmsgClient::default();
    let guard = MockGuardClient::new();

    let root_msg = EventMessage {
        event_id: "$root_o3".to_string(),
        sender_mxid: "@untrusted:example.org".to_string(),
        timestamp_ms: 1_000_000,
        body: "Initial question from U".to_string(),
        thread_root_id: None,
    };
    let t_mention = make_event(
        "$t_mention_o3",
        "@trusted:example.org",
        "@genie:example.org please assist",
        true,
        Some("$root_o3"),
        Some("$root_o3"),
        None,
        1_010,
    );
    handle_incoming_event_with_guard(
        &t_mention,
        std::slice::from_ref(&root_msg),
        &config,
        &matrix,
        &xmsg,
        &guard,
        &store,
        None,
    )
    .await
    .unwrap();

    let u_followup = make_event(
        "$u_followup_o3",
        "@untrusted:example.org",
        "Follow-up details from U",
        false,
        Some("$t_mention_o3"),
        Some("$root_o3"),
        None,
        1_020,
    );
    let out = handle_incoming_event_with_guard(
        &u_followup,
        std::slice::from_ref(&root_msg),
        &config,
        &matrix,
        &xmsg,
        &guard,
        &store,
        None,
    )
    .await
    .unwrap();
    assert_eq!(out, BotOutcome::Forwarded);

    let sends = xmsg.sends.lock().unwrap();
    let u_envelope = &sends[1].2;
    assert!(
        u_envelope.contains("user=matrix untrusted at example.org (public)"),
        "O3: envelope header must carry (public) for escalated asker U, got:\n{u_envelope}"
    );
    let req_start = u_envelope.find("<request").unwrap();
    let req_end = u_envelope.find("</request>").unwrap();
    let req_block = &u_envelope[req_start..req_end];
    assert!(
        req_block.contains("\"tier\":\"public\""),
        "O3: U's RelayedLine in <request> must carry \"tier\":\"public\", got:\n{req_block}"
    );
    assert!(
        !req_block.contains("\"tier\":\"trusted\""),
        "O3: U's RelayedLine in <request> must NOT carry \"tier\":\"trusted\", got:\n{req_block}"
    );
}

// -------------------------------------------------------------------------------------------------
// O4: O1 under `admission = "public"` behaves the same, and B is still ignored.
// Mutant (Amendment 1): skip escalation recording under `admission = "public"` -> RED.
// -------------------------------------------------------------------------------------------------
#[tokio::test]
async fn o4_escalation_under_public_admission_admits_root_author_and_ignores_bystander() {
    let config = test_config(Admission::Public);
    let matrix = MockMatrixClient::default();
    let store = Store::new_in_memory().unwrap();
    let xmsg = MockXmsgClient::default();
    let guard = MockGuardClient::new();

    // U posts a top-level message WITHOUT mentioning genie (so no asker is recorded yet)
    let root_msg = EventMessage {
        event_id: "$root_o4".to_string(),
        sender_mxid: "@untrusted:example.org".to_string(),
        timestamp_ms: 1_000_000,
        body: "Anyone seen this linker error before?".to_string(),
        thread_root_id: None,
    };

    // T replies in thread mentioning genie
    let t_mention = make_event(
        "$t_mention_o4",
        "@trusted:example.org",
        "@genie:example.org can you answer this?",
        true,
        Some("$root_o4"),
        Some("$root_o4"),
        None,
        1_010,
    );
    let out1 = handle_incoming_event_with_guard(
        &t_mention,
        std::slice::from_ref(&root_msg),
        &config,
        &matrix,
        &xmsg,
        &guard,
        &store,
        None,
    )
    .await
    .unwrap();
    assert_eq!(out1, BotOutcome::Forwarded);
    assert_eq!(
        store.get_thread_asker("$root_o4").unwrap().as_deref(),
        Some("@untrusted:example.org"),
        "O4: under Admission::Public, escalation must record U as thread asker"
    );
    assert_eq!(
        store
            .get_thread_escalation_asker("$root_o4")
            .unwrap()
            .as_deref(),
        Some("@untrusted:example.org"),
        "O4: under Admission::Public, thread_escalations must record U"
    );

    // U's unmentioned follow-up in thread is admitted and marked addressed="true"
    let u_followup = make_event(
        "$u_followup_o4",
        "@untrusted:example.org",
        "It happens on x86_64-linux",
        false,
        Some("$t_mention_o4"),
        Some("$root_o4"),
        None,
        1_020,
    );
    let out_u = handle_incoming_event_with_guard(
        &u_followup,
        std::slice::from_ref(&root_msg),
        &config,
        &matrix,
        &xmsg,
        &guard,
        &store,
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        out_u,
        BotOutcome::Forwarded,
        "O4: U's follow-up under Admission::Public must be admitted and forwarded"
    );
    assert!(
        xmsg.sends.lock().unwrap()[1]
            .2
            .contains("\"addressed\":true"),
        "O4: U's follow-up under Admission::Public must carry \"addressed\":true"
    );

    // Bystander B is still ignored
    let b_followup = make_event(
        "$b_followup_o4",
        "@bystander:example.org",
        "I also want to know",
        false,
        Some("$t_mention_o4"),
        Some("$root_o4"),
        None,
        1_030,
    );
    let out_b = handle_incoming_event_with_guard(
        &b_followup,
        std::slice::from_ref(&root_msg),
        &config,
        &matrix,
        &xmsg,
        &guard,
        &store,
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        out_b,
        BotOutcome::IgnoredUntrustedUser,
        "O4: bystander B in escalated thread under Admission::Public must be ignored"
    );
}

// -------------------------------------------------------------------------------------------------
// O5: No escalation recorded when root is from a trusted user or from `bot_mxid`: asker = T,
// `get_thread_escalation` is `None`, and U (not the root author) is ignored.
// Mutant: drop the `!is_trusted && != bot_mxid` guard on escalation recording -> RED.
// -------------------------------------------------------------------------------------------------
#[tokio::test]
async fn o5_no_escalation_when_root_author_is_trusted_or_bot() {
    let config = test_config(Admission::Trusted);
    let matrix = MockMatrixClient::default();
    let store = Store::new_in_memory().unwrap();
    let xmsg = MockXmsgClient::default();
    let guard = MockGuardClient::new();

    // Case 1: Root authored by trusted user @alice:example.org
    let trusted_root = EventMessage {
        event_id: "$root_trusted_o5".to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        timestamp_ms: 1_000_000,
        body: "Trusted root post".to_string(),
        thread_root_id: None,
    };
    let t_mention_1 = make_event(
        "$t_mention_o5_1",
        "@trusted:example.org",
        "@genie:example.org thoughts?",
        true,
        Some("$root_trusted_o5"),
        Some("$root_trusted_o5"),
        None,
        1_010,
    );
    let out1 = handle_incoming_event_with_guard(
        &t_mention_1,
        std::slice::from_ref(&trusted_root),
        &config,
        &matrix,
        &xmsg,
        &guard,
        &store,
        None,
    )
    .await
    .unwrap();
    assert_eq!(out1, BotOutcome::Forwarded);
    assert_eq!(
        store
            .get_thread_asker("$root_trusted_o5")
            .unwrap()
            .as_deref(),
        Some("@trusted:example.org"),
        "O5: when root author is trusted, thread_askers must record the mentioning sender T"
    );
    assert!(
        store
            .get_thread_escalation("$root_trusted_o5")
            .unwrap()
            .is_none(),
        "O5: when root author is trusted, no thread_escalations row may be recorded"
    );

    let u_msg_1 = make_event(
        "$u_msg_o5_1",
        "@untrusted:example.org",
        "Untrusted chime-in",
        false,
        Some("$t_mention_o5_1"),
        Some("$root_trusted_o5"),
        None,
        1_020,
    );
    let out_u1 = handle_incoming_event_with_guard(
        &u_msg_1,
        std::slice::from_ref(&trusted_root),
        &config,
        &matrix,
        &xmsg,
        &guard,
        &store,
        None,
    )
    .await
    .unwrap();
    assert_eq!(out_u1, BotOutcome::IgnoredUntrustedUser);

    // Case 2: Root authored by bot_mxid (@genie:example.org)
    let bot_root = EventMessage {
        event_id: "$root_bot_o5".to_string(),
        sender_mxid: "@genie:example.org".to_string(),
        timestamp_ms: 2_000_000,
        body: "Bot opening post".to_string(),
        thread_root_id: None,
    };
    let t_mention_2 = make_event(
        "$t_mention_o5_2",
        "@trusted:example.org",
        "@genie:example.org follow up on bot post",
        true,
        Some("$root_bot_o5"),
        Some("$root_bot_o5"),
        None,
        2_010,
    );
    let out2 = handle_incoming_event_with_guard(
        &t_mention_2,
        std::slice::from_ref(&bot_root),
        &config,
        &matrix,
        &xmsg,
        &guard,
        &store,
        None,
    )
    .await
    .unwrap();
    assert_eq!(out2, BotOutcome::Forwarded);
    assert_eq!(
        store.get_thread_asker("$root_bot_o5").unwrap().as_deref(),
        Some("@trusted:example.org"),
        "O5: when root author is bot_mxid, thread_askers must record T"
    );
    assert!(
        store
            .get_thread_escalation("$root_bot_o5")
            .unwrap()
            .is_none(),
        "O5: when root author is bot_mxid, no thread_escalations row may be recorded"
    );
}

// -------------------------------------------------------------------------------------------------
// O6: T posts bare `!release` in the escalated thread: bot reacts `✅`, `thread_escalations` row is
// deleted, `thread_askers` is reset to `escalated_by`, and U's next follow-up is ignored. Before
// T's release, a `!release` from U returns `IgnoredReleaseNotOwner` with no `✅`, the escalation
// row still present, and no forward. Repeat under `admission = "public"`: U is ignored and U's
// `!deeper` is refused (`IgnoredUntrustedUser`).
// Mutant: make `!release` a no-op on `thread_escalations` (or leave `thread_askers` as U) -> RED.
// -------------------------------------------------------------------------------------------------
#[tokio::test]
async fn o6_release_in_escalated_thread_revokes_escalation_under_trusted_and_public() {
    // Part 1: Admission::Trusted
    {
        let config = test_config(Admission::Trusted);
        let matrix = MockMatrixClient::default();
        let store = Store::new_in_memory().unwrap();
        let xmsg = MockXmsgClient::default();
        let guard = MockGuardClient::new();

        let root_msg = EventMessage {
            event_id: "$root_o6_trusted".to_string(),
            sender_mxid: "@untrusted:example.org".to_string(),
            timestamp_ms: 1_000_000,
            body: "Help with nix build".to_string(),
            thread_root_id: None,
        };
        let t_mention = make_event(
            "$t_mention_o6",
            "@trusted:example.org",
            "@genie:example.org help U here",
            true,
            Some("$root_o6_trusted"),
            Some("$root_o6_trusted"),
            None,
            1_010,
        );
        handle_incoming_event_with_guard(
            &t_mention,
            std::slice::from_ref(&root_msg),
            &config,
            &matrix,
            &xmsg,
            &guard,
            &store,
            None,
        )
        .await
        .unwrap();

        let sends_before = xmsg.sends.lock().unwrap().len();

        // Untrusted U attempts `!release` before T's release
        let u_release = make_event(
            "$u_release_o6",
            "@untrusted:example.org",
            "!release",
            false,
            Some("$t_mention_o6"),
            Some("$root_o6_trusted"),
            None,
            1_015,
        );
        let out_u_rel = handle_incoming_event_with_guard(
            &u_release,
            std::slice::from_ref(&root_msg),
            &config,
            &matrix,
            &xmsg,
            &guard,
            &store,
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            out_u_rel,
            BotOutcome::IgnoredReleaseNotOwner,
            "O6: untrusted !release in escalated thread must return IgnoredReleaseNotOwner"
        );
        assert!(
            !matrix
                .sent_reactions
                .lock()
                .unwrap()
                .iter()
                .any(|r| r.event_id == "$u_release_o6"),
            "O6: untrusted !release must not receive ✅ reaction"
        );
        assert!(
            store
                .get_thread_escalation("$root_o6_trusted")
                .unwrap()
                .is_some(),
            "O6: escalation row must remain present after untrusted !release"
        );
        assert_eq!(
            xmsg.sends.lock().unwrap().len(),
            sends_before,
            "O6: untrusted !release must not be forwarded"
        );

        // Trusted T posts bare `!release`
        let t_release = make_event(
            "$t_release_o6",
            "@trusted:example.org",
            "!release",
            false,
            Some("$t_mention_o6"),
            Some("$root_o6_trusted"),
            None,
            1_020,
        );
        let out_t_rel = handle_incoming_event_with_guard(
            &t_release,
            std::slice::from_ref(&root_msg),
            &config,
            &matrix,
            &xmsg,
            &guard,
            &store,
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            out_t_rel,
            BotOutcome::Unbound,
            "O6: trusted !release in escalated thread must return BotOutcome::Unbound"
        );
        assert!(
            matrix
                .sent_reactions
                .lock()
                .unwrap()
                .iter()
                .any(|r| r.event_id == "$t_release_o6" && r.key == "✅"),
            "O6: trusted !release must react ✅"
        );
        assert!(
            store
                .get_thread_escalation("$root_o6_trusted")
                .unwrap()
                .is_none(),
            "O6: trusted !release must delete thread_escalations row"
        );
        assert_eq!(
            store
                .get_thread_asker("$root_o6_trusted")
                .unwrap()
                .as_deref(),
            Some("@trusted:example.org"),
            "O6: trusted !release must reset thread_askers to escalated_by"
        );

        // U's next follow-up is ignored
        let u_after = make_event(
            "$u_after_o6",
            "@untrusted:example.org",
            "Still having issues",
            false,
            Some("$t_release_o6"),
            Some("$root_o6_trusted"),
            None,
            1_030,
        );
        let out_u_after = handle_incoming_event_with_guard(
            &u_after,
            std::slice::from_ref(&root_msg),
            &config,
            &matrix,
            &xmsg,
            &guard,
            &store,
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            out_u_after,
            BotOutcome::IgnoredUntrustedUser,
            "O6: U's follow-up after trusted !release must be ignored under Admission::Trusted"
        );
    }

    // Part 2: Repeat under Admission::Public (Amendment 2)
    {
        let config = test_config(Admission::Public);
        let matrix = MockMatrixClient::default();
        let store = Store::new_in_memory().unwrap();
        let xmsg = MockXmsgClient::default();
        let guard = MockGuardClient::new();

        let root_msg = EventMessage {
            event_id: "$root_o6_public".to_string(),
            sender_mxid: "@untrusted:example.org".to_string(),
            timestamp_ms: 2_000_000,
            body: "Unmentioned root in public room".to_string(),
            thread_root_id: None,
        };
        let t_mention = make_event(
            "$t_mention_o6_pub",
            "@trusted:example.org",
            "@genie:example.org check this thread",
            true,
            Some("$root_o6_public"),
            Some("$root_o6_public"),
            None,
            2_010,
        );
        handle_incoming_event_with_guard(
            &t_mention,
            std::slice::from_ref(&root_msg),
            &config,
            &matrix,
            &xmsg,
            &guard,
            &store,
            None,
        )
        .await
        .unwrap();

        let t_release = make_event(
            "$t_release_o6_pub",
            "@alice:example.org",
            "!release",
            false,
            Some("$t_mention_o6_pub"),
            Some("$root_o6_public"),
            None,
            2_020,
        );
        let out_rel = handle_incoming_event_with_guard(
            &t_release,
            std::slice::from_ref(&root_msg),
            &config,
            &matrix,
            &xmsg,
            &guard,
            &store,
            None,
        )
        .await
        .unwrap();
        assert_eq!(out_rel, BotOutcome::Unbound);
        assert_eq!(
            store
                .get_thread_asker("$root_o6_public")
                .unwrap()
                .as_deref(),
            Some("@trusted:example.org"),
            "O6 (public): trusted !release must reset thread_askers to escalated_by (@trusted:example.org)"
        );

        // Under Admission::Public, U's follow-up is now ignored because asker was reset to T
        let u_after_pub = make_event(
            "$u_after_o6_pub",
            "@untrusted:example.org",
            "Follow-up after release in public mode",
            false,
            Some("$t_release_o6_pub"),
            Some("$root_o6_public"),
            None,
            2_030,
        );
        let out_u_pub = handle_incoming_event_with_guard(
            &u_after_pub,
            std::slice::from_ref(&root_msg),
            &config,
            &matrix,
            &xmsg,
            &guard,
            &store,
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            out_u_pub,
            BotOutcome::IgnoredUntrustedUser,
            "O6 (public): U's follow-up after !release under Admission::Public must return IgnoredUntrustedUser"
        );

        // And U's `!deeper` is refused under Admission::Public
        let u_deeper_pub = make_event(
            "$u_deeper_o6_pub",
            "@untrusted:example.org",
            "!deeper",
            false,
            Some("$t_release_o6_pub"),
            Some("$root_o6_public"),
            None,
            2_040,
        );
        let out_deeper_pub = handle_incoming_event_with_guard(
            &u_deeper_pub,
            std::slice::from_ref(&root_msg),
            &config,
            &matrix,
            &xmsg,
            &guard,
            &store,
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            out_deeper_pub,
            BotOutcome::IgnoredUntrustedUser,
            "O6 (public): U's !deeper after !release under Admission::Public must be refused (IgnoredUntrustedUser)"
        );
    }
}

// -------------------------------------------------------------------------------------------------
// O7: When `fetch_event` returns `None` (or `Err`), recording falls back to today's behaviour
// (asker = T, no `thread_escalations` row), and the forward still goes out.
// Mutant: propagate `None` / error from `fetch_event` as a failed forward -> RED.
// -------------------------------------------------------------------------------------------------
#[tokio::test]
async fn o7_unfetchable_root_falls_back_to_trusted_sender_as_asker() {
    let config = test_config(Admission::Trusted);
    let matrix = MockMatrixClient::default();
    let store = Store::new_in_memory().unwrap();
    let xmsg = MockXmsgClient::default();
    let guard = MockGuardClient::new();

    // $root_missing_o7 is neither in history nor in matrix.canned_events -> fetch_event returns Ok(None)
    let t_mention = make_event(
        "$t_mention_o7",
        "@trusted:example.org",
        "@genie:example.org help on missing root thread",
        true,
        Some("$root_missing_o7"),
        Some("$root_missing_o7"),
        None,
        1_010,
    );
    let out = handle_incoming_event_with_guard(
        &t_mention,
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
    assert_eq!(
        out,
        BotOutcome::Forwarded,
        "O7: forward must still go out when fetch_event returns None"
    );
    assert_eq!(
        store
            .get_thread_asker("$root_missing_o7")
            .unwrap()
            .as_deref(),
        Some("@trusted:example.org"),
        "O7: when root cannot be fetched, thread_askers must fall back to T"
    );
    assert!(
        store
            .get_thread_escalation("$root_missing_o7")
            .unwrap()
            .is_none(),
        "O7: when root cannot be fetched, no thread_escalations row may be recorded"
    );
}

// -------------------------------------------------------------------------------------------------
// O8: Trusted `!trust` in a thread that has no asker and no bot message yet: bot reacts `🔒` and
// `promoted_threads` has the row.
// Mutant: drop `is_thread_trust` from `is_addressed` -> RED.
// -------------------------------------------------------------------------------------------------
#[tokio::test]
async fn o8_trust_command_in_unengaged_thread_promotes_thread() {
    let config = test_config(Admission::Trusted);
    let matrix = MockMatrixClient::default();
    let store = Store::new_in_memory().unwrap();
    let xmsg = MockXmsgClient::default();
    let guard = MockGuardClient::new();

    assert!(!store.is_thread_engaged("$root_o8").unwrap());
    assert!(store.get_thread_asker("$root_o8").unwrap().is_none());

    let t_trust = make_event(
        "$t_trust_o8",
        "@trusted:example.org",
        "!trust",
        false,
        Some("$root_o8"),
        Some("$root_o8"),
        None,
        1_010,
    );
    let out = handle_incoming_event_with_guard(
        &t_trust,
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
    assert_eq!(
        out,
        BotOutcome::ControlEmitted,
        "O8: trusted !trust in an unengaged thread must emit control"
    );
    assert!(
        store.is_thread_promoted("$root_o8").unwrap(),
        "O8: promoted_threads must have the row for $root_o8"
    );
    assert!(
        matrix
            .sent_reactions
            .lock()
            .unwrap()
            .iter()
            .any(|r| r.event_id == "$t_trust_o8" && r.key == "🔒"),
        "O8: bot must react 🔒 on the !trust event"
    );
}

// -------------------------------------------------------------------------------------------------
// O9: Sync-path per-thread worker serializes back-to-back events in the same thread with
// `fetch_history` delayed:
//   Case 1 (Amendment 5): `!trust` then an untrusted message mentioning genie in the same thread,
//     delivered back-to-back via sync (`register_event_handlers_with_startup_ts`): the untrusted
//     message is admitted and forwarded.
//   Case 2 (Amendment 5): an escalating mention from T followed back-to-back by the untrusted root
//     author U's unmentioned follow-up in the same thread with `fetch_history` delayed: U's
//     follow-up is admitted and forwarded.
// Mutant: evaluate Gate 2 / Gate 3 before enqueueing (or spawn without per-thread queue) -> RED.
// -------------------------------------------------------------------------------------------------
struct DelayedHistoryMatrixClient {
    inner: MockMatrixClient,
    delay: Duration,
}

#[async_trait]
impl MatrixClient for DelayedHistoryMatrixClient {
    async fn send_notice(
        &self,
        room_id: &str,
        thread_root_id: Option<&str>,
        body: &str,
        mention_user: Option<&str>,
    ) -> Result<String, AppError> {
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
        tokio::time::sleep(self.delay).await;
        self.inner.fetch_history(room_id, limit).await
    }

    async fn fetch_event(
        &self,
        room_id: &str,
        event_id: &str,
    ) -> Result<Option<EventMessage>, AppError> {
        self.inner.fetch_event(room_id, event_id).await
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
async fn o9_sync_per_thread_worker_serializes_back_to_back_thread_events() {
    let mock_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/_matrix/client/versions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "versions": ["v1.1", "v1.2", "v1.3", "v1.4", "v1.5", "v1.6"]
        })))
        .mount(&mock_server)
        .await;

    let room_id = "!support:example.org";
    let now_ts = chrono::Utc::now().timestamp_millis() + 5_000;

    let sync_response = json!({
        "next_batch": "nb_o9_1",
        "rooms": {
            "join": {
                room_id: {
                    "timeline": {
                        "events": [
                            // Case 1: !trust from T followed back-to-back by untrusted U mentioning genie
                            {
                                "type": "m.room.message",
                                "sender": "@trusted:example.org",
                                "content": {
                                    "msgtype": "m.text",
                                    "body": "!trust",
                                    "m.relates_to": {
                                        "rel_type": "m.thread",
                                        "event_id": "$root_o9_case1"
                                    }
                                },
                                "event_id": "$o9_case1_trust",
                                "origin_server_ts": now_ts
                            },
                            {
                                "type": "m.room.message",
                                "sender": "@untrusted:example.org",
                                "content": {
                                    "msgtype": "m.text",
                                    "body": "@genie:example.org question in promoted thread",
                                    "m.mentions": {
                                        "user_ids": ["@genie:example.org"]
                                    },
                                    "m.relates_to": {
                                        "rel_type": "m.thread",
                                        "event_id": "$root_o9_case1"
                                    }
                                },
                                "event_id": "$o9_case1_u_msg",
                                "origin_server_ts": now_ts + 1
                            },
                            // Case 2: escalating mention from T followed back-to-back by root author U's unmentioned follow-up
                            {
                                "type": "m.room.message",
                                "sender": "@trusted:example.org",
                                "content": {
                                    "msgtype": "m.text",
                                    "body": "@genie:example.org please help U in case 2",
                                    "m.mentions": {
                                        "user_ids": ["@genie:example.org"]
                                    },
                                    "m.relates_to": {
                                        "rel_type": "m.thread",
                                        "event_id": "$root_o9_case2"
                                    }
                                },
                                "event_id": "$o9_case2_t_escalate",
                                "origin_server_ts": now_ts + 2
                            },
                            {
                                "type": "m.room.message",
                                "sender": "@untrusted:example.org",
                                "content": {
                                    "msgtype": "m.text",
                                    "body": "Unmentioned follow-up from root author in case 2",
                                    "m.relates_to": {
                                        "rel_type": "m.thread",
                                        "event_id": "$root_o9_case2"
                                    }
                                },
                                "event_id": "$o9_case2_u_followup",
                                "origin_server_ts": now_ts + 3
                            }
                        ]
                    }
                }
            }
        }
    });

    Mock::given(method("GET"))
        .and(path("/_matrix/client/v3/sync"))
        .respond_with(ResponseTemplate::new(200).set_body_json(sync_response))
        .mount(&mock_server)
        .await;

    let mut cfg = test_config(Admission::Trusted);
    cfg.homeserver_url = mock_server.uri();
    // Disable guard screening so handle_incoming_event_with_claim doesn't need a live unix socket guard
    cfg.screen_mentions = false;
    cfg.screen_solicited = false;
    let config = Arc::new(cfg);

    let matrix = Arc::new(DelayedHistoryMatrixClient {
        inner: MockMatrixClient::default(),
        delay: Duration::from_millis(80),
    });
    matrix.inner.canned_events.lock().unwrap().insert(
        "$root_o9_case2".to_string(),
        EventMessage {
            event_id: "$root_o9_case2".to_string(),
            sender_mxid: "@untrusted:example.org".to_string(),
            timestamp_ms: now_ts - 10_000,
            body: "Untrusted root in case 2".to_string(),
            thread_root_id: None,
        },
    );

    let xmsg = Arc::new(MockXmsgClient::default());
    let store = Arc::new(Store::new_in_memory().unwrap());

    let sdk_client = MatrixSdkClient::new(&config.homeserver_url, &config.bot_mxid, "syt_token")
        .await
        .unwrap();

    let tracker = register_event_handlers_with_startup_ts(
        sdk_client.inner(),
        config.clone(),
        matrix.clone(),
        xmsg.clone(),
        store.clone(),
        0,
    );

    sdk_client
        .inner()
        .sync_once(SyncSettings::default())
        .await
        .unwrap();

    tracker
        .drain_or_notify(matrix.as_ref(), Duration::from_secs(5))
        .await;

    let sends = xmsg.sends.lock().unwrap();
    assert!(
        sends
            .iter()
            .any(|(_, _, text)| text.contains("question in promoted thread")),
        "O9 Case 1: untrusted message mentioning genie right after !trust must be admitted and forwarded, sends: {sends:?}"
    );
    assert!(
        sends.iter().any(|(_, _, text)| {
            text.contains("Unmentioned follow-up from root author in case 2")
                && text.contains("\"addressed\":true")
        }),
        "O9 Case 2: root author U's unmentioned follow-up right after T's escalating mention must be admitted and forwarded with \"addressed\":true, sends: {sends:?}"
    );
}

// -------------------------------------------------------------------------------------------------
// O10: T mentions genie again in the escalated thread, and a second trusted user mentions genie in
// a thread whose asker is already set. Neither call reaches `fetch_event`, and no
// `thread_escalations` row is added or changed.
// Mutant (Amendment 6): record the escalation on every trusted forward -> RED.
// -------------------------------------------------------------------------------------------------
struct CountingMatrixClient {
    inner: MockMatrixClient,
    fetch_event_calls: AtomicUsize,
}

impl Default for CountingMatrixClient {
    fn default() -> Self {
        Self {
            inner: MockMatrixClient::default(),
            fetch_event_calls: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl MatrixClient for CountingMatrixClient {
    async fn send_notice(
        &self,
        room_id: &str,
        thread_root_id: Option<&str>,
        body: &str,
        mention_user: Option<&str>,
    ) -> Result<String, AppError> {
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

    async fn fetch_event(
        &self,
        room_id: &str,
        event_id: &str,
    ) -> Result<Option<EventMessage>, AppError> {
        self.fetch_event_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.fetch_event(room_id, event_id).await
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
async fn o10_subsequent_trusted_mentions_skip_fetch_event_and_preserve_escalation() {
    let config = test_config(Admission::Trusted);
    let matrix = CountingMatrixClient::default();
    let store = Store::new_in_memory().unwrap();
    let xmsg = MockXmsgClient::default();
    let guard = MockGuardClient::new();

    matrix.inner.canned_events.lock().unwrap().insert(
        "$root_o10_1".to_string(),
        EventMessage {
            event_id: "$root_o10_1".to_string(),
            sender_mxid: "@untrusted:example.org".to_string(),
            timestamp_ms: 1_000_000,
            body: "Untrusted root 1".to_string(),
            thread_root_id: None,
        },
    );
    matrix.inner.canned_events.lock().unwrap().insert(
        "$root_o10_2".to_string(),
        EventMessage {
            event_id: "$root_o10_2".to_string(),
            sender_mxid: "@untrusted:example.org".to_string(),
            timestamp_ms: 1_000_000,
            body: "Untrusted root 2".to_string(),
            thread_root_id: None,
        },
    );

    // 1. T escalates $root_o10_1 (not in room_history, so fetch_event is called once)
    let t_mention_1 = make_event(
        "$t_mention_o10_1",
        "@trusted:example.org",
        "@genie:example.org first escalation",
        true,
        None,
        Some("$root_o10_1"),
        None,
        1_010,
    );
    handle_incoming_event_with_guard(
        &t_mention_1,
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
    assert_eq!(matrix.fetch_event_calls.load(Ordering::SeqCst), 1);

    // Reset counter before testing subsequent mentions
    matrix.fetch_event_calls.store(0, Ordering::SeqCst);

    // 2. T mentions genie again in the escalated thread $root_o10_1
    let t_mention_again = make_event(
        "$t_mention_o10_2",
        "@trusted:example.org",
        "@genie:example.org second mention from T",
        true,
        None,
        Some("$root_o10_1"),
        None,
        1_020,
    );
    handle_incoming_event_with_guard(
        &t_mention_again,
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
    assert_eq!(
        matrix.fetch_event_calls.load(Ordering::SeqCst),
        0,
        "O10: subsequent mention by T in already-escalated thread must not call fetch_event"
    );

    // 3. Second trusted user @alice:example.org mentions genie in $root_o10_1 (and in $root_o10_2 whose asker is already set)
    let alice_in_o10_1 = make_event(
        "$alice_mention_o10_1",
        "@alice:example.org",
        "@genie:example.org alice chiming in",
        true,
        None,
        Some("$root_o10_1"),
        None,
        1_030,
    );
    handle_incoming_event_with_guard(
        &alice_in_o10_1,
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
    assert_eq!(
        matrix.fetch_event_calls.load(Ordering::SeqCst),
        0,
        "O10: second trusted user mentioning genie in escalated thread must not call fetch_event"
    );
    let esc = store
        .get_thread_escalation("$root_o10_1")
        .unwrap()
        .expect("O10: escalation row must still exist");
    assert_eq!(
        esc.escalated_by, "@trusted:example.org",
        "O10: thread_escalations.escalated_by must not be overwritten by second trusted user"
    );

    // Pre-existing thread $root_o10_2 whose asker is already set to @trusted:example.org (without escalation)
    store
        .record_thread_asker("$root_o10_2", "@trusted:example.org")
        .unwrap();
    let alice_in_o10_2 = make_event(
        "$alice_mention_o10_2",
        "@alice:example.org",
        "@genie:example.org alice in thread with existing asker",
        true,
        None,
        Some("$root_o10_2"),
        None,
        1_040,
    );
    handle_incoming_event_with_guard(
        &alice_in_o10_2,
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
    assert_eq!(
        matrix.fetch_event_calls.load(Ordering::SeqCst),
        0,
        "O10: second trusted user mentioning genie in a thread whose asker is already set must not call fetch_event"
    );
    assert!(
        store
            .get_thread_escalation("$root_o10_2")
            .unwrap()
            .is_none(),
        "O10: no thread_escalations row may be added when thread asker was already set"
    );
    assert_eq!(
        store.get_thread_asker("$root_o10_2").unwrap().as_deref(),
        Some("@trusted:example.org"),
        "O10: existing thread_askers row must remain unchanged"
    );
}
