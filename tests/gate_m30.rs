use async_trait::async_trait;
use matrix_xmsg::bot::{
    handle_inbox_delivery_with_guard, handle_incoming_event_with_guard, BotOutcome,
    IncomingMatrixEvent,
};
use matrix_xmsg::config::{Admission, Config, GuardFailureMode, RoomRoute};
use matrix_xmsg::context::{
    EventMessage, DEFAULT_CONTEXT_NOTE, UNSCREENED_CONFIG_NOTE, UNSCREENED_WARNING_NOTE,
};
use matrix_xmsg::error::AppError;
use matrix_xmsg::matrix::MockMatrixClient;
use matrix_xmsg::store::Store;
use matrix_xmsg::xmsg::{MockGuardClient, SendResponse, SvcDelivery, SvcInbox, XmsgClient};
use serde_json::json;
use std::collections::{BTreeMap, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

const R1: &str = "!r1:example.org";
const R2: &str = "!r2:example.org";

fn base_config() -> Config {
    Config {
        homeserver_url: "https://matrix.example.org".to_string(),
        bot_mxid: "@genie:example.org".to_string(),
        access_token_file: PathBuf::from("/tmp/token"),
        rooms: vec![R1.to_string(), R2.to_string()],
        trusted_mxids: vec!["@trusted:example.org".to_string()],
        owner_mxid: "@owner:example.org".to_string(),
        admission: Admission::Public,
        xmsg_socket: PathBuf::from("/tmp/xmsg"),
        expert_ref: "claude".to_string(),
        expert_refs: None,
        room_routes: BTreeMap::new(),
        open_thread_nodes: vec![],
        guard_ref: "genie-guard".to_string(),
        guard_timeout_secs: 30,
        guard_retry_budget: 3,
        screen_mentions: true,
        screen_solicited: true,
        guard_failure_mode: GuardFailureMode::Closed,
        guard_outage_dm_window_secs: 3600,
        inbox_retry_budget: 10,
        history_n: 30,
        history_byte_cap: 12288,
        resync_byte_cap: 65536,
        resync_page_bytes: 8192,
        context_max_lines: 20,
        context_max_bytes: 8192,
        sender_window_secs: 900,
        context_rate_limit_count: 6,
        context_rate_limit_window_secs: 600,
        rate_limit_count: 100,
        rate_limit_window_secs: 600,
        size_cap_bytes: 4096,
        answer_timeout_secs: 600,
        session_live_secs: 3600,
        db_path: PathBuf::from(":memory:"),
    }
}

#[derive(Clone, Default)]
struct RecordingXmsg {
    sent: Arc<Mutex<Vec<(String, String, String)>>>,
    replies: Arc<Mutex<Vec<(String, String)>>>,
    sessions: Arc<Mutex<VecDeque<Option<String>>>>,
    counter: Arc<Mutex<u64>>,
}

impl RecordingXmsg {
    fn queue_session(&self, sess: Option<&str>) {
        self.sessions
            .lock()
            .unwrap()
            .push_back(sess.map(|s| s.to_string()));
    }
}

#[async_trait]
impl XmsgClient for RecordingXmsg {
    async fn send_message(
        &self,
        to: &str,
        sender: &str,
        text: &str,
    ) -> Result<SendResponse, AppError> {
        let mut c = self.counter.lock().unwrap();
        *c += 1;
        let id = format!("01M30MSG{:06}", *c);
        drop(c);
        self.sent
            .lock()
            .unwrap()
            .push((to.to_string(), sender.to_string(), text.to_string()));
        let session_id = self
            .sessions
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| Some(format!("{to}@sini.bitstream")));
        Ok(SendResponse {
            message_id: id,
            session_id,
        })
    }

    async fn reply_message(&self, message_id: &str, text: &str) -> Result<SendResponse, AppError> {
        let mut c = self.counter.lock().unwrap();
        *c += 1;
        let id = format!("01M30REP{:06}", *c);
        drop(c);
        self.replies
            .lock()
            .unwrap()
            .push((message_id.to_string(), text.to_string()));
        let session_id = self
            .sessions
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| Some("reply@sini.bitstream".to_string()));
        Ok(SendResponse {
            message_id: id,
            session_id,
        })
    }
}

struct MockInbox {
    own_node: Option<String>,
    acked: Vec<String>,
}

impl MockInbox {
    fn new(node: &str) -> Self {
        Self {
            own_node: Some(node.to_string()),
            acked: Vec::new(),
        }
    }
}

#[async_trait]
impl SvcInbox for MockInbox {
    async fn poll(&mut self, _wait_secs: u64) -> Result<Option<SvcDelivery>, AppError> {
        Ok(None)
    }

    async fn ack(&mut self, id: &str) -> Result<(), AppError> {
        self.acked.push(id.to_string());
        Ok(())
    }

    fn node(&self) -> Option<String> {
        self.own_node.clone()
    }
}

/// Strips the off-mode screening attributes (`screened="false"`, `"screened":false`, and
/// `UNSCREENED_CONFIG_NOTE`) so an off-mode envelope can be compared byte-for-byte with
/// the corresponding on-mode `allow` envelope.
fn normalize_off_mode_envelope(off_env: &str) -> String {
    off_env
        .replace(
            &format!("<request screened=\"false\" note=\"{UNSCREENED_CONFIG_NOTE}\">"),
            "<request>",
        )
        .replace(
            &format!(" screened=\"false\" note=\"{UNSCREENED_CONFIG_NOTE}\""),
            &format!(" note=\"{DEFAULT_CONTEXT_NOTE}\""),
        )
        .replace(",\"screened\":false", "")
}

// =================================================================================================
// Oracle 1: Switches (`screen_mentions`, `screen_solicited`) & optional `guard_ref` validation.
// =================================================================================================
#[test]
fn oracle_1_switches_and_guard_ref_validation() {
    // 1. Both switches default to true when guard_ref is set
    let default_toml = r#"
        homeserver_url = "https://matrix.example.org"
        bot_mxid = "@genie:example.org"
        access_token_file = "/tmp/token"
        rooms = ["!r1:example.org"]
        trusted_mxids = []
        owner_mxid = "@owner:example.org"
        xmsg_socket = "/tmp/xmsg"
        guard_ref = "genie-guard"
    "#;
    let cfg = Config::from_toml_str_with_env(default_toml, None).unwrap();
    assert!(cfg.screen_mentions);
    assert!(cfg.screen_solicited);

    // 2. Both switches false and no guard_ref loads cleanly
    let both_off_toml = r#"
        homeserver_url = "https://matrix.example.org"
        bot_mxid = "@genie:example.org"
        access_token_file = "/tmp/token"
        rooms = ["!r1:example.org"]
        trusted_mxids = []
        owner_mxid = "@owner:example.org"
        xmsg_socket = "/tmp/xmsg"
        screen_mentions = false
        screen_solicited = false
    "#;
    let cfg_off = Config::from_toml_str_with_env(both_off_toml, None).unwrap();
    assert!(!cfg_off.screen_mentions);
    assert!(!cfg_off.screen_solicited);
    assert!(cfg_off.guard_ref.is_empty());

    // 3. screen_mentions = true with no guard_ref fails naming both guard_ref and screen_mentions
    let mentions_on_no_guard = r#"
        homeserver_url = "https://matrix.example.org"
        bot_mxid = "@genie:example.org"
        access_token_file = "/tmp/token"
        rooms = ["!r1:example.org"]
        trusted_mxids = []
        owner_mxid = "@owner:example.org"
        xmsg_socket = "/tmp/xmsg"
        screen_mentions = true
        screen_solicited = false
    "#;
    let err1 = Config::from_toml_str_with_env(mentions_on_no_guard, None)
        .unwrap_err()
        .to_string();
    assert!(
        err1.contains("guard_ref"),
        "error must name guard_ref: {err1}"
    );
    assert!(
        err1.contains("screen_mentions"),
        "error must name screen_mentions: {err1}"
    );

    // 4. screen_solicited = true with no guard_ref fails naming both guard_ref and screen_solicited
    let solicited_on_no_guard = r#"
        homeserver_url = "https://matrix.example.org"
        bot_mxid = "@genie:example.org"
        access_token_file = "/tmp/token"
        rooms = ["!r1:example.org"]
        trusted_mxids = []
        owner_mxid = "@owner:example.org"
        xmsg_socket = "/tmp/xmsg"
        screen_mentions = false
        screen_solicited = true
    "#;
    let err2 = Config::from_toml_str_with_env(solicited_on_no_guard, None)
        .unwrap_err()
        .to_string();
    assert!(
        err2.contains("guard_ref"),
        "error must name guard_ref: {err2}"
    );
    assert!(
        err2.contains("screen_solicited"),
        "error must name screen_solicited: {err2}"
    );

    // 5. Global switches false, but a room_routes entry enables screen_solicited = true with no guard_ref
    let room_solicited_on_no_guard = r#"
        homeserver_url = "https://matrix.example.org"
        bot_mxid = "@genie:example.org"
        access_token_file = "/tmp/token"
        rooms = ["!r1:example.org"]
        trusted_mxids = []
        owner_mxid = "@owner:example.org"
        xmsg_socket = "/tmp/xmsg"
        screen_mentions = false
        screen_solicited = false

        [room_routes."!r1:example.org"]
        screen_solicited = true
    "#;
    let err3 = Config::from_toml_str_with_env(room_solicited_on_no_guard, None)
        .unwrap_err()
        .to_string();
    assert!(
        err3.contains("guard_ref"),
        "error must name guard_ref: {err3}"
    );
    assert!(
        err3.contains("screen_solicited"),
        "error must name screen_solicited: {err3}"
    );
}

// =================================================================================================
// Oracle 2: M32 × off-mode interaction (2 cells):
// - Cell A: all-trusted envelope has NO unscreened mark in either on-mode or off-mode (0 guard calls).
// - Cell B: envelope with >= 1 untrusted line gets screened in on-mode, and in off-mode gets
//   0 guard calls + `screened="false"` with `UNSCREENED_CONFIG_NOTE` (distinct from fail-open).
// =================================================================================================
#[tokio::test]
async fn oracle_2_m32_x_off_mode_two_cells() {
    // --- Cell A: All-trusted envelope in on-mode vs off-mode ---
    for screening_on in [true, false] {
        let mut config = base_config();
        config.screen_mentions = screening_on;
        config.screen_solicited = screening_on;
        if !screening_on {
            config.guard_ref = String::new();
        }
        let store = Store::new_in_memory().unwrap();
        let matrix = MockMatrixClient::default();
        let xmsg = RecordingXmsg::default();
        let guard = MockGuardClient::default();

        let trusted_history = vec![EventMessage {
            event_id: "$trusted_prior".to_string(),
            sender_mxid: "@trusted:example.org".to_string(),
            timestamp_ms: 995_000,
            body: "Trusted background line".to_string(),
            thread_root_id: None,
        }];
        let ev_trusted = IncomingMatrixEvent {
            room_id: R1.to_string(),
            event_id: "$trusted_trig".to_string(),
            sender_mxid: "@trusted:example.org".to_string(),
            body: "@genie:example.org trusted question".to_string(),
            formatted_body: None,
            mentions: None,
            thread_root_id: None,
            in_reply_to_event_id: None,
            is_falling_back: false,
            replaces_event_id: None,
            timestamp_ms: 1_000_000,
        };
        let out = handle_incoming_event_with_guard(
            &ev_trusted,
            &trusted_history,
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
        assert_eq!(
            guard.calls.lock().unwrap().len(),
            0,
            "Cell A (screening_on={screening_on}): all-trusted forward must make 0 guard calls"
        );
        let env = xmsg.sent.lock().unwrap()[0].2.clone();
        assert!(
            !env.contains("screened=\"false\"") && !env.contains("\"screened\":false"),
            "Cell A (screening_on={screening_on}): all-trusted envelope must carry no unscreened mark: {env}"
        );
        assert!(
            env.contains(DEFAULT_CONTEXT_NOTE),
            "Cell A (screening_on={screening_on}): all-trusted envelope must carry DEFAULT_CONTEXT_NOTE: {env}"
        );

        // Also check resync & context on all-trusted history in off-mode
        let fwd_id = "01M30MSG000001".to_string();
        let root_msg = EventMessage {
            event_id: "$trusted_trig".to_string(),
            sender_mxid: "@trusted:example.org".to_string(),
            timestamp_ms: 1_000_000,
            body: "@genie:example.org trusted question".to_string(),
            thread_root_id: None,
        };
        matrix
            .canned_events
            .lock()
            .unwrap()
            .insert("$trusted_trig".to_string(), root_msg.clone());
        matrix
            .canned_history
            .lock()
            .unwrap()
            .extend(vec![trusted_history[0].clone(), root_msg]);

        let mut inbox = MockInbox::new("sini.bitstream");
        let resync_del = SvcDelivery {
            message_id: "01M30CELLA_RESYNC".to_string(),
            from_name: "claude@sini.bitstream".to_string(),
            text: json!({ "resync": true }).to_string(),
            envelope: String::new(),
            in_reply_to: Some(fwd_id.clone()),
            origin: Some(json!({ "service": "claude", "node": "sini.bitstream" })),
        };
        handle_inbox_delivery_with_guard(
            &resync_del,
            &config,
            &matrix,
            &store,
            &xmsg,
            &guard,
            &mut inbox,
            1_010,
        )
        .await
        .unwrap();
        let resync_env = xmsg.replies.lock().unwrap()[0].1.clone();
        assert!(
            !resync_env.contains("screened=\"false\"") && !resync_env.contains("\"screened\":false"),
            "Cell A (screening_on={screening_on}): all-trusted resync must carry no unscreened mark: {resync_env}"
        );
    }

    // --- Cell B: Envelope with >= 1 untrusted line (trusted trigger + untrusted thread history) ---
    let mut config_off = base_config();
    config_off.screen_mentions = false;
    config_off.screen_solicited = false;
    config_off.guard_ref = String::new();
    let store = Store::new_in_memory().unwrap();
    let matrix = MockMatrixClient::default();
    let xmsg = RecordingXmsg::default();
    let guard = MockGuardClient::default();

    store
        .record_thread_asker("$root_b", "@public:example.org")
        .unwrap();
    let mixed_history = vec![
        EventMessage {
            event_id: "$root_b".to_string(),
            sender_mxid: "@public:example.org".to_string(),
            timestamp_ms: 990_000,
            body: "@genie:example.org untrusted root".to_string(),
            thread_root_id: None,
        },
        EventMessage {
            event_id: "$trusted_reply".to_string(),
            sender_mxid: "@trusted:example.org".to_string(),
            timestamp_ms: 1_000_000,
            body: "Trusted follow-up in thread with untrusted root".to_string(),
            thread_root_id: Some("$root_b".to_string()),
        },
    ];
    let ev_mixed = IncomingMatrixEvent {
        room_id: R1.to_string(),
        event_id: "$trusted_reply".to_string(),
        sender_mxid: "@trusted:example.org".to_string(),
        body: "Trusted follow-up in thread with untrusted root".to_string(),
        formatted_body: None,
        mentions: None,
        thread_root_id: Some("$root_b".to_string()),
        in_reply_to_event_id: None,
        is_falling_back: false,
        replaces_event_id: None,
        timestamp_ms: 1_000_000,
    };
    let out_b = handle_incoming_event_with_guard(
        &ev_mixed,
        &mixed_history,
        &config_off,
        &matrix,
        &xmsg,
        &guard,
        &store,
        None,
    )
    .await
    .unwrap();
    assert_eq!(out_b, BotOutcome::Forwarded);
    assert_eq!(guard.calls.lock().unwrap().len(), 0);
    let env_b = xmsg.sent.lock().unwrap()[0].2.clone();
    assert!(
        env_b.contains(&format!(
            "screened=\"false\" note=\"{UNSCREENED_CONFIG_NOTE}\""
        )),
        "Cell B: off-mode envelope with untrusted history line must carry UNSCREENED_CONFIG_NOTE: {env_b}"
    );
    assert!(
        !env_b.contains(UNSCREENED_WARNING_NOTE),
        "Cell B: off-mode envelope must not carry fail-open UNSCREENED_WARNING_NOTE: {env_b}"
    );
    assert!(env_b.contains("\"screened\":false"));

    // Next turn in $root_b: trusted sender posts a second reply while session changes ->
    // Delta has only trusted lines (Cell A: no mark), while M16 Rebootstrap includes $root_b (Cell B: marked unscreened).
    xmsg.queue_session(Some("new-sess@sini.bitstream"));
    xmsg.queue_session(Some("new-sess@sini.bitstream"));
    let mut mixed_history_2 = mixed_history.clone();
    mixed_history_2.push(EventMessage {
        event_id: "$trusted_reply_2".to_string(),
        sender_mxid: "@trusted:example.org".to_string(),
        timestamp_ms: 1_005_000,
        body: "Second trusted follow-up on session change".to_string(),
        thread_root_id: Some("$root_b".to_string()),
    });
    let ev_mixed_2 = IncomingMatrixEvent {
        room_id: R1.to_string(),
        event_id: "$trusted_reply_2".to_string(),
        sender_mxid: "@trusted:example.org".to_string(),
        body: "Second trusted follow-up on session change".to_string(),
        formatted_body: None,
        mentions: None,
        thread_root_id: Some("$root_b".to_string()),
        in_reply_to_event_id: None,
        is_falling_back: false,
        replaces_event_id: None,
        timestamp_ms: 1_005_000,
    };
    handle_incoming_event_with_guard(
        &ev_mixed_2,
        &mixed_history_2,
        &config_off,
        &matrix,
        &xmsg,
        &guard,
        &store,
        None,
    )
    .await
    .unwrap();
    assert_eq!(guard.calls.lock().unwrap().len(), 0);
    let sent = xmsg.sent.lock().unwrap();
    assert_eq!(sent.len(), 3);
    let delta_env = &sent[1].2;
    let reboot_env = &sent[2].2;
    assert!(
        !delta_env.contains("screened=\"false\"") && !delta_env.contains("\"screened\":false"),
        "All-trusted Delta in off-mode must have no unscreened mark: {delta_env}"
    );
    assert!(
        reboot_env.contains(&format!(
            "screened=\"false\" note=\"{UNSCREENED_CONFIG_NOTE}\""
        )),
        "M16 Rebootstrap carrying pre-cursor untrusted root in off-mode must be marked unscreened: {reboot_env}"
    );
}

// =================================================================================================
// Oracle 3: Parameterized forward-path fidelity over on-mode (`true`) and off-mode (`false`)
// across all 8 paths:
// (a) plain mention, (b) reply-to-bot, (c) edit-into-mention, (d) engaged-thread bootstrap,
// (e) engaged-thread delta, (f) M16 session-change rebootstrap, (g) solicited thread (`open_thread`
// with global and per-room `screen_solicited`), (h) `resync` and `context` (`room`, `event`, `thread`).
// =================================================================================================
async fn run_all_paths(screening_on: bool) -> (Vec<String>, usize) {
    let mut config = base_config();
    config.screen_mentions = screening_on;
    config.screen_solicited = screening_on;
    if !screening_on {
        config.guard_ref = String::new();
    }
    // Also exercise per-room `room_routes` on R2 matching `screening_on`
    config.room_routes.insert(
        R2.to_string(),
        RoomRoute {
            expert_ref: Some("claude".to_string()),
            expert_refs: None,
            screen_mentions: Some(screening_on),
            screen_solicited: Some(screening_on),
        },
    );

    let store = Store::new_in_memory().unwrap();
    let matrix = MockMatrixClient::default();
    let xmsg = RecordingXmsg::default();
    let guard = MockGuardClient::default();
    let mut envelopes = Vec::new();

    // --- (a) Plain mention with sender's own recent top-level line ---
    let hist_a = vec![
        EventMessage {
            event_id: "$a_prior".to_string(),
            sender_mxid: "@public:example.org".to_string(),
            timestamp_ms: 990_000,
            body: "My earlier context line".to_string(),
            thread_root_id: None,
        },
        EventMessage {
            event_id: "$a_trig".to_string(),
            sender_mxid: "@public:example.org".to_string(),
            timestamp_ms: 1_000_000,
            body: "@genie:example.org plain mention question".to_string(),
            thread_root_id: None,
        },
    ];
    let ev_a = IncomingMatrixEvent {
        room_id: R1.to_string(),
        event_id: "$a_trig".to_string(),
        sender_mxid: "@public:example.org".to_string(),
        body: "@genie:example.org plain mention question".to_string(),
        formatted_body: None,
        mentions: None,
        thread_root_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
        replaces_event_id: None,
        timestamp_ms: 1_000_000,
    };
    handle_incoming_event_with_guard(
        &ev_a, &hist_a, &config, &matrix, &xmsg, &guard, &store, None,
    )
    .await
    .unwrap();
    envelopes.push(xmsg.sent.lock().unwrap().last().unwrap().2.clone());

    // --- (b) Reply-to-bot (m.in_reply_to) ---
    store
        .record_bot_message("$bot_post_b", "$bot_post_b", 1_000)
        .unwrap();
    let bot_msg_b = EventMessage {
        event_id: "$bot_post_b".to_string(),
        sender_mxid: "@genie:example.org".to_string(),
        timestamp_ms: 1_000_000,
        body: "Bot announcement".to_string(),
        thread_root_id: None,
    };
    matrix
        .canned_events
        .lock()
        .unwrap()
        .insert("$bot_post_b".to_string(), bot_msg_b.clone());
    let ev_b = IncomingMatrixEvent {
        room_id: R1.to_string(),
        event_id: "$b_reply".to_string(),
        sender_mxid: "@public:example.org".to_string(),
        body: "Reply to bot post".to_string(),
        formatted_body: None,
        mentions: None,
        thread_root_id: None,
        in_reply_to_event_id: Some("$bot_post_b".to_string()),
        is_falling_back: false,
        replaces_event_id: None,
        timestamp_ms: 1_010_000,
    };
    handle_incoming_event_with_guard(
        &ev_b,
        &[bot_msg_b],
        &config,
        &matrix,
        &xmsg,
        &guard,
        &store,
        None,
    )
    .await
    .unwrap();
    envelopes.push(xmsg.sent.lock().unwrap().last().unwrap().2.clone());

    // --- (c) Edit-into-mention (m.replace) ---
    let orig_c = EventMessage {
        event_id: "$c_orig".to_string(),
        sender_mxid: "@public:example.org".to_string(),
        timestamp_ms: 1_020_000,
        body: "forgot to tag bot".to_string(),
        thread_root_id: None,
    };
    matrix
        .canned_events
        .lock()
        .unwrap()
        .insert("$c_orig".to_string(), orig_c.clone());
    let ev_c = IncomingMatrixEvent {
        room_id: R1.to_string(),
        event_id: "$c_edit".to_string(),
        sender_mxid: "@public:example.org".to_string(),
        body: "@genie:example.org edited to mention bot".to_string(),
        formatted_body: None,
        mentions: None,
        thread_root_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
        replaces_event_id: Some("$c_orig".to_string()),
        timestamp_ms: 1_025_000,
    };
    handle_incoming_event_with_guard(
        &ev_c,
        &[orig_c],
        &config,
        &matrix,
        &xmsg,
        &guard,
        &store,
        None,
    )
    .await
    .unwrap();
    envelopes.push(xmsg.sent.lock().unwrap().last().unwrap().2.clone());

    // --- (d) Engaged-thread bootstrap + (e) Engaged-thread delta + (f) M16 session-change rebootstrap ---
    store
        .record_thread_asker("$d_root", "@public:example.org")
        .unwrap();
    let d_root_msg = EventMessage {
        event_id: "$d_root".to_string(),
        sender_mxid: "@public:example.org".to_string(),
        timestamp_ms: 2_000_000,
        body: "@genie:example.org thread root question".to_string(),
        thread_root_id: None,
    };
    let d_msg1 = EventMessage {
        event_id: "$d_msg1".to_string(),
        sender_mxid: "@public:example.org".to_string(),
        timestamp_ms: 2_005_000,
        body: "First engaged thread follow-up (bootstrap)".to_string(),
        thread_root_id: Some("$d_root".to_string()),
    };
    let hist_d1 = vec![d_root_msg.clone(), d_msg1.clone()];
    let ev_d1 = IncomingMatrixEvent {
        room_id: R1.to_string(),
        event_id: "$d_msg1".to_string(),
        sender_mxid: "@public:example.org".to_string(),
        body: d_msg1.body.clone(),
        formatted_body: None,
        mentions: None,
        thread_root_id: Some("$d_root".to_string()),
        in_reply_to_event_id: None,
        is_falling_back: false,
        replaces_event_id: None,
        timestamp_ms: 2_005_000,
    };
    xmsg.queue_session(Some("sess-1@sini.bitstream"));
    handle_incoming_event_with_guard(
        &ev_d1, &hist_d1, &config, &matrix, &xmsg, &guard, &store, None,
    )
    .await
    .unwrap();
    envelopes.push(xmsg.sent.lock().unwrap().last().unwrap().2.clone());

    // (e) Engaged-thread delta (same session "sess-1@sini.bitstream")
    let d_msg2 = EventMessage {
        event_id: "$d_msg2".to_string(),
        sender_mxid: "@public:example.org".to_string(),
        timestamp_ms: 2_010_000,
        body: "Second engaged thread follow-up (delta)".to_string(),
        thread_root_id: Some("$d_root".to_string()),
    };
    let hist_d2 = vec![d_root_msg.clone(), d_msg1.clone(), d_msg2.clone()];
    let ev_d2 = IncomingMatrixEvent {
        room_id: R1.to_string(),
        event_id: "$d_msg2".to_string(),
        sender_mxid: "@public:example.org".to_string(),
        body: d_msg2.body.clone(),
        formatted_body: None,
        mentions: None,
        thread_root_id: Some("$d_root".to_string()),
        in_reply_to_event_id: None,
        is_falling_back: false,
        replaces_event_id: None,
        timestamp_ms: 2_010_000,
    };
    xmsg.queue_session(Some("sess-1@sini.bitstream"));
    handle_incoming_event_with_guard(
        &ev_d2, &hist_d2, &config, &matrix, &xmsg, &guard, &store, None,
    )
    .await
    .unwrap();
    envelopes.push(xmsg.sent.lock().unwrap().last().unwrap().2.clone());

    // (f) M16 session-change rebootstrap (new session "claude@sini.bitstream" on delta triggers immediate rebootstrap)
    let d_msg3 = EventMessage {
        event_id: "$d_msg3".to_string(),
        sender_mxid: "@public:example.org".to_string(),
        timestamp_ms: 2_015_000,
        body: "Third follow-up causing M16 rebootstrap".to_string(),
        thread_root_id: Some("$d_root".to_string()),
    };
    let hist_d3 = vec![
        d_root_msg.clone(),
        d_msg1.clone(),
        d_msg2.clone(),
        d_msg3.clone(),
    ];
    let ev_d3 = IncomingMatrixEvent {
        room_id: R1.to_string(),
        event_id: "$d_msg3".to_string(),
        sender_mxid: "@public:example.org".to_string(),
        body: d_msg3.body.clone(),
        formatted_body: None,
        mentions: None,
        thread_root_id: Some("$d_root".to_string()),
        in_reply_to_event_id: None,
        is_falling_back: false,
        replaces_event_id: None,
        timestamp_ms: 2_015_000,
    };
    xmsg.queue_session(Some("claude@sini.bitstream"));
    xmsg.queue_session(Some("claude@sini.bitstream"));
    handle_incoming_event_with_guard(
        &ev_d3, &hist_d3, &config, &matrix, &xmsg, &guard, &store, None,
    )
    .await
    .unwrap();
    {
        let sent = xmsg.sent.lock().unwrap();
        let len = sent.len();
        // Both the delta and the M16 rebootstrap envelope
        envelopes.push(sent[len - 2].2.clone());
        envelopes.push(sent[len - 1].2.clone());
    }

    // --- (g) Solicited thread (`open_thread` in R2, governed by `screen_solicited`) ---
    store
        .bind_solicited_thread("$sol_root", R2, "01M30SOLREQ", 3_000)
        .unwrap();
    store
        .record_bot_message("$sol_root", "$sol_root", 3_000)
        .unwrap();
    let sol_msg = EventMessage {
        event_id: "$sol_reply".to_string(),
        sender_mxid: "@public:example.org".to_string(),
        timestamp_ms: 3_005_000,
        body: "Solicited thread reply in R2".to_string(),
        thread_root_id: Some("$sol_root".to_string()),
    };
    let ev_sol = IncomingMatrixEvent {
        room_id: R2.to_string(),
        event_id: "$sol_reply".to_string(),
        sender_mxid: "@public:example.org".to_string(),
        body: sol_msg.body.clone(),
        formatted_body: None,
        mentions: None,
        thread_root_id: Some("$sol_root".to_string()),
        in_reply_to_event_id: Some("$sol_root".to_string()),
        is_falling_back: false,
        replaces_event_id: None,
        timestamp_ms: 3_005_000,
    };
    handle_incoming_event_with_guard(
        &ev_sol,
        &[sol_msg.clone()],
        &config,
        &matrix,
        &xmsg,
        &guard,
        &store,
        None,
    )
    .await
    .unwrap();
    envelopes.push(xmsg.replies.lock().unwrap().last().unwrap().1.clone());

    // --- (h) Thread `resync` and M33 `context` (`room`, `event`, `thread`) ---
    matrix
        .canned_events
        .lock()
        .unwrap()
        .insert("$d_root".to_string(), d_root_msg.clone());
    matrix
        .canned_events
        .lock()
        .unwrap()
        .insert("$d_msg1".to_string(), d_msg1.clone());
    matrix.canned_history.lock().unwrap().clear();
    matrix.canned_history.lock().unwrap().extend(vec![
        hist_a[0].clone(),
        d_root_msg,
        d_msg1,
        d_msg2,
        d_msg3,
    ]);

    let mut inbox = MockInbox::new("sini.bitstream");
    // Forwarded message id for $d_msg3 ("01M30MSG000006")
    let d_fwd_id = "01M30MSG000006".to_string();

    // h.1: resync
    let resync_del = SvcDelivery {
        message_id: "01M30RESYNC".to_string(),
        from_name: "claude@sini.bitstream".to_string(),
        text: json!({ "resync": true }).to_string(),
        envelope: String::new(),
        in_reply_to: Some(d_fwd_id.clone()),
        origin: Some(json!({ "service": "claude", "node": "sini.bitstream" })),
    };
    let out_resync = handle_inbox_delivery_with_guard(
        &resync_del,
        &config,
        &matrix,
        &store,
        &xmsg,
        &guard,
        &mut inbox,
        2_020,
    )
    .await
    .unwrap();
    assert_eq!(out_resync, BotOutcome::Resynced);
    envelopes.push(xmsg.replies.lock().unwrap().last().unwrap().1.clone());

    // h.2: context (room scope)
    let ctx_room_del = SvcDelivery {
        message_id: "01M30CTX_ROOM".to_string(),
        from_name: "claude@sini.bitstream".to_string(),
        text: json!({ "context": { "scope": "room", "limit": 5 } }).to_string(),
        envelope: String::new(),
        in_reply_to: Some(d_fwd_id.clone()),
        origin: Some(json!({ "service": "claude", "node": "sini.bitstream" })),
    };
    let out_ctx_room = handle_inbox_delivery_with_guard(
        &ctx_room_del,
        &config,
        &matrix,
        &store,
        &xmsg,
        &guard,
        &mut inbox,
        2_021,
    )
    .await
    .unwrap();
    assert_eq!(out_ctx_room, BotOutcome::ContextAnswered);
    envelopes.push(xmsg.replies.lock().unwrap().last().unwrap().1.clone());

    // h.3: context (event scope)
    let ctx_event_del = SvcDelivery {
        message_id: "01M30CTX_EVENT".to_string(),
        from_name: "claude@sini.bitstream".to_string(),
        text: json!({ "context": { "scope": "event", "event": "$d_msg1" } }).to_string(),
        envelope: String::new(),
        in_reply_to: Some(d_fwd_id.clone()),
        origin: Some(json!({ "service": "claude", "node": "sini.bitstream" })),
    };
    let out_ctx_event = handle_inbox_delivery_with_guard(
        &ctx_event_del,
        &config,
        &matrix,
        &store,
        &xmsg,
        &guard,
        &mut inbox,
        2_022,
    )
    .await
    .unwrap();
    assert_eq!(out_ctx_event, BotOutcome::ContextAnswered);
    envelopes.push(xmsg.replies.lock().unwrap().last().unwrap().1.clone());

    // h.4: context (thread scope)
    let ctx_thread_del = SvcDelivery {
        message_id: "01M30CTX_THREAD".to_string(),
        from_name: "claude@sini.bitstream".to_string(),
        text: json!({ "context": { "scope": "thread", "thread": "$d_root" } }).to_string(),
        envelope: String::new(),
        in_reply_to: Some(d_fwd_id),
        origin: Some(json!({ "service": "claude", "node": "sini.bitstream" })),
    };
    let out_ctx_thread = handle_inbox_delivery_with_guard(
        &ctx_thread_del,
        &config,
        &matrix,
        &store,
        &xmsg,
        &guard,
        &mut inbox,
        2_023,
    )
    .await
    .unwrap();
    assert_eq!(out_ctx_thread, BotOutcome::ContextAnswered);
    envelopes.push(xmsg.replies.lock().unwrap().last().unwrap().1.clone());

    let guard_count = guard.calls.lock().unwrap().len();
    (envelopes, guard_count)
}

#[tokio::test]
async fn oracle_3_parameterized_on_off_forward_fidelity_and_zero_guard_calls() {
    let (on_envelopes, on_guard_calls) = run_all_paths(true).await;
    let (off_envelopes, off_guard_calls) = run_all_paths(false).await;

    assert!(
        on_guard_calls >= 11,
        "on-mode must call the guard across all paths, got {on_guard_calls}"
    );
    assert_eq!(
        off_guard_calls, 0,
        "off-mode must make 0 guard requests across all paths"
    );
    assert_eq!(on_envelopes.len(), off_envelopes.len());

    for (idx, (on_env, off_env)) in on_envelopes.iter().zip(off_envelopes.iter()).enumerate() {
        assert!(
            off_env.contains(UNSCREENED_CONFIG_NOTE),
            "path index {idx}: off-mode envelope must carry UNSCREENED_CONFIG_NOTE:\n{off_env}"
        );
        assert!(
            off_env.contains("screened=\"false\"") && off_env.contains("\"screened\":false"),
            "path index {idx}: off-mode envelope must carry screened=false attributes:\n{off_env}"
        );
        let normalized = normalize_off_mode_envelope(off_env);
        assert_eq!(
            &normalized, on_env,
            "path index {idx}: off-mode envelope must equal on-mode allow envelope byte-for-byte apart from screening attributes"
        );
    }
}
