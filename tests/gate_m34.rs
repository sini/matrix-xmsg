use async_trait::async_trait;
use matrix_xmsg::bot::{
    handle_inbox_delivery_with_guard, handle_incoming_event_with_guard, handle_incoming_reaction,
    BotOutcome, IncomingMatrixEvent, IncomingReactionEvent,
};
use matrix_xmsg::config::{route_for, Admission, Config, GuardFailureMode, RoomRoute};
use matrix_xmsg::context::EventMessage;
use matrix_xmsg::error::AppError;
use matrix_xmsg::matrix::MockMatrixClient;
use matrix_xmsg::store::Store;
use matrix_xmsg::xmsg::{MockGuardClient, SendResponse, SvcDelivery, SvcInbox, XmsgClient};
use serde_json::json;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
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
        open_thread_nodes: vec!["sini.bitstream".to_string()],
        guard_ref: "genie-guard".to_string(),
        guard_timeout_secs: 30,
        guard_retry_budget: 3,
        screen_mentions: true,
        screen_solicited: true,
        guard_failure_mode: GuardFailureMode::Closed,
        guard_outage_dm_window_secs: 3600,
        inbox_retry_budget: 10,
        history_n: 30,
        history_byte_cap: 12 * 1024,
        resync_byte_cap: 65536,
        resync_page_bytes: 8192,
        context_max_lines: 20,
        context_max_bytes: 8192,
        sender_window_secs: 900,
        context_rate_limit_count: 6,
        context_rate_limit_window_secs: 600,
        rate_limit_count: 20,
        rate_limit_window_secs: 600,
        size_cap_bytes: 4096,
        answer_timeout_secs: 300,
        session_live_secs: 3600,
        db_path: PathBuf::from(":memory:"),
    }
}

#[derive(Default)]
struct RecordingXmsg {
    sent: Arc<Mutex<Vec<(String, String, String)>>>,
    replies: Arc<Mutex<Vec<(String, String)>>>,
    counter: AtomicUsize,
}

#[async_trait]
impl XmsgClient for RecordingXmsg {
    async fn send_message(
        &self,
        expert_ref: &str,
        from: &str,
        text: &str,
    ) -> Result<SendResponse, AppError> {
        self.sent.lock().unwrap().push((
            expert_ref.to_string(),
            from.to_string(),
            text.to_string(),
        ));
        let idx = self.counter.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(SendResponse::new(
            format!("01M34MSG{idx:06}"),
            Some(format!("{expert_ref}@sini.bitstream")),
        ))
    }

    async fn reply_message(&self, message_id: &str, text: &str) -> Result<SendResponse, AppError> {
        self.replies
            .lock()
            .unwrap()
            .push((message_id.to_string(), text.to_string()));
        let idx = self.counter.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(SendResponse::new(
            format!("01M34REPLY{idx:06}"),
            Some("reply@sini.bitstream".to_string()),
        ))
    }
}

struct MockInbox {
    own_node: Option<String>,
    acked: Vec<String>,
}

impl MockInbox {
    fn new(own_node: &str) -> Self {
        Self {
            own_node: Some(own_node.to_string()),
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

/// Oracle (a): Two rooms, with R2 routed to `genie` and `screen_mentions = false`, and R1 left
/// unrouted:
/// - a mention in R1 goes to the global ref (`claude`) and is screened (1 guard call);
/// - a mention in R2 goes to `genie` with 0 guard calls, and its envelope carries the M26
///   `screened="false"` unscreened mark (including on `resync` and `context` pages).
#[tokio::test]
async fn oracle_a_two_rooms_routing_and_per_room_screening() {
    let mut config = base_config();
    config.room_routes.insert(
        R2.to_string(),
        RoomRoute {
            expert_ref: Some("genie".to_string()),
            expert_refs: None,
            screen_mentions: Some(false),
            screen_solicited: None,
        },
    );

    let store = Store::new_in_memory().unwrap();
    let matrix = MockMatrixClient::default();
    let xmsg = RecordingXmsg::default();
    let guard = MockGuardClient::new();

    // 1. Mention in R1 (unrouted) from untrusted sender -> global `claude`, screened (1 guard call)
    let ev_r1 = IncomingMatrixEvent {
        room_id: R1.to_string(),
        event_id: "$r1_root".to_string(),
        sender_mxid: "@stranger:other.org".to_string(),
        body: "@genie:example.org question in R1".to_string(),
        formatted_body: None,
        mentions: None,
        thread_root_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
        replaces_event_id: None,
        timestamp_ms: 1_000_000,
    };
    let out_r1 = handle_incoming_event_with_guard(
        &ev_r1,
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
    assert_eq!(out_r1, BotOutcome::Forwarded);
    assert_eq!(
        guard.calls.lock().unwrap().len(),
        1,
        "Unrouted R1 must use global screen_mentions=true (1 guard call)"
    );
    {
        let sent = xmsg.sent.lock().unwrap();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].0, "claude", "R1 must route to global expert_ref");
        assert!(
            !sent[0].2.contains(r#"screened="false""#),
            "Screened R1 envelope must not carry screened=\"false\": {}",
            sent[0].2
        );
    }

    // 2. Mention in R2 (routed to `genie`, screen_mentions = false) -> `genie`, 0 additional guard calls,
    //    and envelope carries M26 `screened="false"` mark.
    let prior_r2 = EventMessage {
        event_id: "$r2_prior".to_string(),
        sender_mxid: "@stranger:other.org".to_string(),
        timestamp_ms: 1_004_000,
        body: "prior line in R2".to_string(),
        thread_root_id: None,
    };
    let ev_r2 = IncomingMatrixEvent {
        room_id: R2.to_string(),
        event_id: "$r2_root".to_string(),
        sender_mxid: "@stranger:other.org".to_string(),
        body: "@genie:example.org question in R2".to_string(),
        formatted_body: None,
        mentions: None,
        thread_root_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
        replaces_event_id: None,
        timestamp_ms: 1_005_000,
    };
    let out_r2 = handle_incoming_event_with_guard(
        &ev_r2,
        std::slice::from_ref(&prior_r2),
        &config,
        &matrix,
        &xmsg,
        &guard,
        &store,
        None,
    )
    .await
    .unwrap();
    assert_eq!(out_r2, BotOutcome::Forwarded);
    assert_eq!(
        guard.calls.lock().unwrap().len(),
        1,
        "R2 with screen_mentions=false must make 0 guard calls"
    );
    let r2_fwd_id = {
        let sent = xmsg.sent.lock().unwrap();
        assert_eq!(sent.len(), 2);
        assert_eq!(
            sent[1].0, "genie",
            "R2 must route to per-room expert_ref `genie`"
        );
        assert!(
            sent[1].2.contains(r#"<request screened="false""#),
            "Unscreened R2 request tag must carry screened=\"false\": {}",
            sent[1].2
        );
        assert!(
            sent[1].2.contains(r#"screened="false""#) && sent[1].2.contains(r#""screened":false"#),
            "Unscreened R2 envelope must carry M26 screened=\"false\" mark: {}",
            sent[1].2
        );
        "01M34MSG000002".to_string()
    };

    // 3. Verify `resync` and `context` on R2 also go through `route_for(R2).screen_mentions` (0 guard calls)
    let r2_root_msg = EventMessage {
        event_id: "$r2_root".to_string(),
        sender_mxid: "@stranger:other.org".to_string(),
        timestamp_ms: 1_005_000,
        body: "@genie:example.org question in R2".to_string(),
        thread_root_id: None,
    };
    matrix
        .canned_events
        .lock()
        .unwrap()
        .insert("$r2_root".to_string(), r2_root_msg.clone());
    matrix
        .canned_history
        .lock()
        .unwrap()
        .extend(vec![prior_r2, r2_root_msg]);

    let mut inbox = MockInbox::new("sini.bitstream");
    let resync_delivery = SvcDelivery {
        message_id: "01M34RESYNC_R2".to_string(),
        from_name: "genie@sini.bitstream".to_string(),
        text: json!({ "resync": true }).to_string(),
        envelope: String::new(),
        in_reply_to: Some(r2_fwd_id.clone()),
        origin: Some(json!({
            "service": "genie",
            "node": "sini.bitstream"
        })),
    };
    let out_resync = handle_inbox_delivery_with_guard(
        &resync_delivery,
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
    assert_eq!(out_resync, BotOutcome::Resynced);
    assert_eq!(
        guard.calls.lock().unwrap().len(),
        1,
        "Resync in R2 (screen_mentions=false) must make 0 guard calls"
    );

    let ctx_delivery = SvcDelivery {
        message_id: "01M34CTX_R2".to_string(),
        from_name: "genie@sini.bitstream".to_string(),
        text: json!({ "context": { "scope": "room", "limit": 5 } }).to_string(),
        envelope: String::new(),
        in_reply_to: Some(r2_fwd_id),
        origin: Some(json!({
            "service": "genie",
            "node": "sini.bitstream"
        })),
    };
    let out_ctx = handle_inbox_delivery_with_guard(
        &ctx_delivery,
        &config,
        &matrix,
        &store,
        &xmsg,
        &guard,
        &mut inbox,
        1_015,
    )
    .await
    .unwrap();
    assert_eq!(out_ctx, BotOutcome::ContextAnswered);
    assert_eq!(
        guard.calls.lock().unwrap().len(),
        1,
        "Context in R2 (screen_mentions=false) must make 0 guard calls"
    );
}

/// Oracle (b): Per-room `expert_refs`: R2's public and trusted tiers resolve to R2's refs,
/// and R1 keeps the global ones. Also verifies an entry with neither `expert_ref` nor
/// `expert_refs` set is legal and overrides only `screen_mentions`.
#[tokio::test]
async fn oracle_b_per_room_expert_refs_and_screen_only_override() {
    let toml_str = r#"
        homeserver_url = "https://matrix.example.org"
        bot_mxid = "@genie:example.org"
        access_token_file = "/tmp/token"
        rooms = ["!r1:example.org", "!r2:example.org", "!r3:example.org"]
        trusted_mxids = ["@trusted:example.org"]
        owner_mxid = "@owner:example.org"
        admission = "public"
        xmsg_socket = "/tmp/xmsg"
        guard_ref = "genie-guard"
        screen_mentions = true

        [expert_refs]
        public = "global-public@bitstream"
        trusted = "global-trusted@bitstream"

        [room_routes."!r2:example.org"]
        screen_mentions = false
        [room_routes."!r2:example.org".expert_refs]
        public = "r2-public@bitstream"
        trusted = "r2-trusted@bitstream"

        [room_routes."!r3:example.org"]
        screen_mentions = false
    "#;
    let config = Config::from_toml_str_with_env(toml_str, None).unwrap();

    // Direct `route_for` checks
    let r1_route = route_for(&config, R1);
    assert_eq!(r1_route.ref_for_tier("public"), "global-public@bitstream");
    assert_eq!(r1_route.ref_for_tier("trusted"), "global-trusted@bitstream");
    assert!(r1_route.screen_mentions);

    let r2_route = route_for(&config, R2);
    assert_eq!(r2_route.ref_for_tier("public"), "r2-public@bitstream");
    assert_eq!(r2_route.ref_for_tier("trusted"), "r2-trusted@bitstream");
    assert!(!r2_route.screen_mentions);

    // R3 overrides only screen_mentions; expert_refs falls back to global
    let r3_route = route_for(&config, "!r3:example.org");
    assert_eq!(r3_route.ref_for_tier("public"), "global-public@bitstream");
    assert_eq!(r3_route.ref_for_tier("trusted"), "global-trusted@bitstream");
    assert!(!r3_route.screen_mentions);

    // End-to-end forward checks for R1 vs R2 across public and trusted tiers
    let store = Store::new_in_memory().unwrap();
    let matrix = MockMatrixClient::default();
    let xmsg = RecordingXmsg::default();
    let guard = MockGuardClient::new();

    for (idx, (room, sender, expected_ref)) in [
        (R1, "@stranger:other.org", "global-public@bitstream"),
        (R1, "@trusted:example.org", "global-trusted@bitstream"),
        (R2, "@stranger:other.org", "r2-public@bitstream"),
        (R2, "@trusted:example.org", "r2-trusted@bitstream"),
    ]
    .into_iter()
    .enumerate()
    {
        let ev = IncomingMatrixEvent {
            room_id: room.to_string(),
            event_id: format!("$ev_{idx}"),
            sender_mxid: sender.to_string(),
            body: "@genie:example.org tier check".to_string(),
            formatted_body: None,
            mentions: None,
            thread_root_id: None,
            in_reply_to_event_id: None,
            is_falling_back: false,
            replaces_event_id: None,
            timestamp_ms: 2_000_000 + (idx as i64) * 5_000,
        };
        let out = handle_incoming_event_with_guard(
            &ev,
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
        assert_eq!(out, BotOutcome::Forwarded);
        let sent = xmsg.sent.lock().unwrap();
        assert_eq!(sent[idx].0, expected_ref, "Mismatch at step {idx}");
    }
}

/// Oracle (c): Config errors:
/// 1. A route keyed to a room not in `rooms` -> load error;
/// 2. An ambiguous alias/id entry (key in `rooms` under another spelling, or mixed alias/id) -> load error.
#[test]
fn oracle_c_config_load_rejects_unlisted_room_and_ambiguous_alias_id() {
    // 1. Route keyed to a room not in `rooms` -> load error
    let not_in_rooms_toml = r##"
        homeserver_url = "https://matrix.example.org"
        bot_mxid = "@genie:example.org"
        access_token_file = "/tmp/token"
        rooms = ["!r1:example.org"]
        trusted_mxids = []
        owner_mxid = "@owner:example.org"
        xmsg_socket = "/tmp/xmsg"

        [room_routes."!r2:example.org"]
        expert_ref = "genie"
    "##;
    let err_unlisted = Config::from_toml_str_with_env(not_in_rooms_toml, None)
        .expect_err("Route keyed to a room not in `rooms` must fail at config load")
        .to_string();
    assert!(
        err_unlisted.contains("!r2:example.org") && err_unlisted.contains("rooms"),
        "Unexpected error for unlisted room route: {err_unlisted}"
    );

    // 2a. Route keyed by alias `#r1:example.org` when `rooms` has `!r1:example.org` -> load error
    let alias_vs_id_toml = r##"
        homeserver_url = "https://matrix.example.org"
        bot_mxid = "@genie:example.org"
        access_token_file = "/tmp/token"
        rooms = ["!r1:example.org"]
        trusted_mxids = []
        owner_mxid = "@owner:example.org"
        xmsg_socket = "/tmp/xmsg"

        [room_routes."#r1:example.org"]
        expert_ref = "genie"
    "##;
    let err_alias_vs_id = Config::from_toml_str_with_env(alias_vs_id_toml, None)
        .expect_err("Route keyed by alias when `rooms` has canonical room ID must fail")
        .to_string();
    assert!(
        err_alias_vs_id.contains("#r1:example.org"),
        "Unexpected error for alias-vs-id route: {err_alias_vs_id}"
    );

    // 2b. Ambiguous alias + id both present in `rooms` and keyed in `room_routes` -> load error
    let both_in_rooms_toml = r##"
        homeserver_url = "https://matrix.example.org"
        bot_mxid = "@genie:example.org"
        access_token_file = "/tmp/token"
        rooms = ["!r1:example.org", "#r1:example.org"]
        trusted_mxids = []
        owner_mxid = "@owner:example.org"
        xmsg_socket = "/tmp/xmsg"

        [room_routes."!r1:example.org"]
        expert_ref = "genie"
    "##;
    let err_both = Config::from_toml_str_with_env(both_in_rooms_toml, None)
        .expect_err("Ambiguous alias and room id in `rooms`/`room_routes` must fail")
        .to_string();
    assert!(
        err_both.contains("!r1:example.org") || err_both.contains("#r1:example.org"),
        "Unexpected error for ambiguous alias/id entry: {err_both}"
    );
}

/// Oracle (d): Changing R2's route on a live thread -> the M28 target-change Bootstrap fires
/// (`supersedes`), and the old target gets nothing further. Also verifies controls, `open_thread`,
/// `redact`, and `edit` in R2.
#[tokio::test]
async fn oracle_d_changing_route_on_live_thread_fires_bootstrap_with_supersedes() {
    let mut config = base_config();
    config.room_routes.insert(
        R2.to_string(),
        RoomRoute {
            expert_ref: Some("genie-v1".to_string()),
            expert_refs: None,
            screen_mentions: Some(false),
            screen_solicited: None,
        },
    );

    let store = Store::new_in_memory().unwrap();
    let matrix = MockMatrixClient::default();
    let xmsg = RecordingXmsg::default();
    let guard = MockGuardClient::new();

    // Turn 1: Start thread in R2 -> Bootstrap to `genie-v1`
    let ev1 = IncomingMatrixEvent {
        room_id: R2.to_string(),
        event_id: "$r2_thread_root".to_string(),
        sender_mxid: "@trusted:example.org".to_string(),
        body: "@genie:example.org turn 1 in R2".to_string(),
        formatted_body: None,
        mentions: None,
        thread_root_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
        replaces_event_id: None,
        timestamp_ms: 3_000_000,
    };
    handle_incoming_event_with_guard(&ev1, &[], &config, &matrix, &xmsg, &guard, &store, None)
        .await
        .unwrap();

    // Turn 2 (10s later, same route `genie-v1`): Delta to `genie-v1`
    let ev2 = IncomingMatrixEvent {
        room_id: R2.to_string(),
        event_id: "$r2_msg2".to_string(),
        sender_mxid: "@trusted:example.org".to_string(),
        body: "@genie:example.org turn 2 in R2".to_string(),
        formatted_body: None,
        mentions: None,
        thread_root_id: Some("$r2_thread_root".to_string()),
        in_reply_to_event_id: None,
        is_falling_back: false,
        replaces_event_id: None,
        timestamp_ms: 3_010_000,
    };
    handle_incoming_event_with_guard(&ev2, &[], &config, &matrix, &xmsg, &guard, &store, None)
        .await
        .unwrap();
    {
        let sent = xmsg.sent.lock().unwrap();
        assert_eq!(sent.len(), 2);
        assert_eq!(sent[0].0, "genie-v1");
        assert_eq!(sent[1].0, "genie-v1");
        assert!(
            sent[1].2.contains(r#"context="delta""#),
            "Turn 2 with unchanged route must be Delta, got: {}",
            sent[1].2
        );
    }

    // Now change R2's route to `genie-v2` while the thread is still live:
    config.room_routes.insert(
        R2.to_string(),
        RoomRoute {
            expert_ref: Some("genie-v2".to_string()),
            expert_refs: None,
            screen_mentions: Some(false),
            screen_solicited: None,
        },
    );

    // Before a new forward arrives, a reaction control on an existing bot message in that thread
    // still uses the thread's recorded `thread_cursors.target_ref` (`genie-v1`).
    store
        .record_bot_message("$bot_reply_v1", "$r2_thread_root", 3_012)
        .unwrap();
    let reaction = IncomingReactionEvent {
        room_id: R2.to_string(),
        event_id: "$react_accept".to_string(),
        sender_mxid: "@trusted:example.org".to_string(),
        relates_to_event_id: "$bot_reply_v1".to_string(),
        key: "✅".to_string(),
        timestamp_ms: 3_015_000,
    };
    let out_react = handle_incoming_reaction(&reaction, &config, &xmsg, &store)
        .await
        .unwrap();
    assert_eq!(out_react, BotOutcome::ControlEmitted);
    {
        let sent = xmsg.sent.lock().unwrap();
        assert_eq!(sent.len(), 3);
        assert_eq!(
            sent[2].0, "genie-v1",
            "Thread control before re-bootstrap must address recorded thread_cursors.target_ref"
        );
    }

    // Turn 3 (20s after Turn 1, still within session_live_secs, after R2 route changed to `genie-v2`):
    // M28 target-change Bootstrap fires to `genie-v2` with `supersedes="01M34MSG000002"`,
    // and `genie-v1` (the old target) gets nothing further.
    let ev3 = IncomingMatrixEvent {
        room_id: R2.to_string(),
        event_id: "$r2_msg3".to_string(),
        sender_mxid: "@trusted:example.org".to_string(),
        body: "@genie:example.org turn 3 after route change".to_string(),
        formatted_body: None,
        mentions: None,
        thread_root_id: Some("$r2_thread_root".to_string()),
        in_reply_to_event_id: None,
        is_falling_back: false,
        replaces_event_id: None,
        timestamp_ms: 3_020_000,
    };
    handle_incoming_event_with_guard(&ev3, &[], &config, &matrix, &xmsg, &guard, &store, None)
        .await
        .unwrap();
    {
        let sent = xmsg.sent.lock().unwrap();
        assert_eq!(
            sent.len(),
            4,
            "Exactly one forward must be sent on Turn 3 (old target gets nothing further)"
        );
        assert_eq!(
            sent[3].0, "genie-v2",
            "Turn 3 must route to new target `genie-v2`"
        );
        assert!(
            sent[3].2.contains(r#"context="bootstrap""#)
                && sent[3].2.contains(r#"supersedes="01M34MSG000002""#),
            "Turn 3 after route change must be a Bootstrap carrying supersedes, got: {}",
            sent[3].2
        );
        // Verify old target `genie-v1` got zero messages after index 2
        assert!(
            sent[3..].iter().all(|(target, _, _)| target != "genie-v1"),
            "Old target `genie-v1` must receive nothing further"
        );
    }
}

/// Oracle (e): The module eval renders `room_routes` identically from NixOS and Home Manager.
#[test]
fn oracle_e_nixos_and_hm_modules_render_room_routes_identically() {
    let which_nix = Command::new("which")
        .arg("nix")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);

    if !which_nix {
        eprintln!("Nix binary not found in PATH; skipping nix-eval test");
        return;
    }

    let nix_expr = r#"
    let
      pkgs = import <nixpkgs> {};
      lib = pkgs.lib;
      options = import ./nix/options.nix;
      baseCfg = {
        enable = true;
        homeserverUrl = "https://matrix.example.org";
        botMxid = "@genie:example.org";
        accessTokenFile = "/run/secrets/matrix-token";
        rooms = [ "!r1:example.org" "!r2:example.org" "!r3:example.org" ];
        trustedMxids = [ "@trusted:example.org" ];
        ownerMxid = "@owner:example.org";
        admission = "public";
        stateDir = "/var/lib/matrix-xmsg";
        xmsgSocket = "/run/xmsg";
        expertRef = "claude";
        expertRefs = null;
        roomRoutes = {
          "!r2:example.org" = {
            expertRef = "genie";
            expertRefs = null;
            screenMentions = false;
          };
          "!r3:example.org" = {
            expertRef = null;
            expertRefs = {
              public = "r3-public@bitstream";
              trusted = "r3-trusted@bitstream";
            };
            screenMentions = true;
          };
        };
        openThreadNodes = [ "sini.bitstream" ];
        guardRef = "genie-guard";
        guardTimeoutSecs = 150;
        guardRetryBudget = 3;
        screenMentions = true;
        guardFailureMode = "closed";
        guardOutageDmWindowSecs = 3600;
        historyN = 30;
        historyByteCap = 12288;
        resyncByteCap = 65536;
        resyncPageBytes = 8192;
        contextMaxLines = 20;
        contextMaxBytes = 8192;
        senderWindowSecs = 900;
        contextRateLimitCount = 6;
        contextRateLimitWindowSecs = 600;
        rateLimitCount = 10;
        rateLimitWindowSecs = 600;
        sizeCapBytes = 4096;
        answerTimeoutSecs = 600;
        sessionLiveSecs = 3600;
        inboxRetryBudget = 10;
        dbPath = "/var/lib/matrix-xmsg/matrix-xmsg.db";
      };
      nixosMod = lib.evalModules {
        modules = [
          {
            options = {
              assertions = lib.mkOption { type = lib.types.listOf lib.types.attrs; default = []; };
              users.users = lib.mkOption { type = lib.types.attrs; default = {}; };
              users.groups = lib.mkOption { type = lib.types.attrs; default = {}; };
              systemd.services = lib.mkOption { type = lib.types.attrs; default = {}; };
            };
          }
          { _module.args.pkgs = pkgs; }
          (import ./nix/module.nix {
            self = { packages.${pkgs.stdenv.hostPlatform.system}.default = pkgs.hello; };
          })
          {
            services.matrix-xmsg = {
              enable = true;
              homeserverUrl = baseCfg.homeserverUrl;
              botMxid = baseCfg.botMxid;
              accessTokenFile = baseCfg.accessTokenFile;
              rooms = baseCfg.rooms;
              trustedMxids = baseCfg.trustedMxids;
              ownerMxid = baseCfg.ownerMxid;
              admission = baseCfg.admission;
              xmsgSocket = baseCfg.xmsgSocket;
              expertRef = baseCfg.expertRef;
              roomRoutes = {
                "!r2:example.org" = {
                  expertRef = "genie";
                  screenMentions = false;
                };
                "!r3:example.org" = {
                  expertRefs = {
                    public = "r3-public@bitstream";
                    trusted = "r3-trusted@bitstream";
                  };
                  screenMentions = true;
                };
              };
            };
          }
        ];
      };
      hmMod = lib.evalModules {
        modules = [
          {
            options = {
              assertions = lib.mkOption { type = lib.types.listOf lib.types.attrs; default = []; };
              warnings = lib.mkOption { type = lib.types.listOf lib.types.str; default = []; };
              home.homeDirectory = lib.mkOption { type = lib.types.str; default = "/home/alice"; };
              xdg.stateHome = lib.mkOption { type = lib.types.str; default = "/home/alice/.local/state"; };
              systemd.user.services = lib.mkOption { type = lib.types.attrs; default = {}; };
              systemd.user.startServices = lib.mkOption { type = lib.types.bool; default = true; };
              launchd.agents = lib.mkOption { type = lib.types.attrs; default = {}; };
            };
          }
          { _module.args.pkgs = pkgs; }
          (import ./nix/home-manager.nix {
            self = { packages.${pkgs.stdenv.hostPlatform.system}.default = pkgs.hello; };
          })
          {
            services.matrix-xmsg = {
              enable = true;
              homeserverUrl = baseCfg.homeserverUrl;
              botMxid = baseCfg.botMxid;
              accessTokenFile = baseCfg.accessTokenFile;
              rooms = baseCfg.rooms;
              trustedMxids = baseCfg.trustedMxids;
              ownerMxid = baseCfg.ownerMxid;
              admission = baseCfg.admission;
              xmsgSocket = baseCfg.xmsgSocket;
              expertRef = baseCfg.expertRef;
              roomRoutes = {
                "!r2:example.org" = {
                  expertRef = "genie";
                  screenMentions = false;
                };
                "!r3:example.org" = {
                  expertRefs = {
                    public = "r3-public@bitstream";
                    trusted = "r3-trusted@bitstream";
                  };
                  screenMentions = true;
                };
              };
            };
          }
        ];
      };
      nixosAttrs = options.mkConfigAttrs { inherit lib; } nixosMod.config.services.matrix-xmsg baseCfg.accessTokenFile;
      hmAttrs = options.mkConfigAttrs { inherit lib; } hmMod.config.services.matrix-xmsg baseCfg.accessTokenFile;
      tomlFile = options.mkConfigToml { inherit lib pkgs; } nixosMod.config.services.matrix-xmsg baseCfg.accessTokenFile;
    in {
      nixosRoomRoutes = nixosAttrs.room_routes;
      hmRoomRoutes = hmAttrs.room_routes;
      tomlText = builtins.readFile tomlFile;
    }
    "#;

    let output = Command::new("nix")
        .args(["eval", "--impure", "--json", "--expr", nix_expr])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("Failed to execute nix eval");

    assert!(
        output.status.success(),
        "nix eval failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let val: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("Failed to parse nix eval JSON");
    assert_eq!(
        val["nixosRoomRoutes"], val["hmRoomRoutes"],
        "NixOS and HM modules must render identical room_routes"
    );
    assert_eq!(
        val["nixosRoomRoutes"]["!r2:example.org"],
        json!({
            "expert_ref": "genie",
            "screen_mentions": false
        })
    );
    assert_eq!(
        val["nixosRoomRoutes"]["!r3:example.org"],
        json!({
            "expert_refs": {
                "public": "r3-public@bitstream",
                "trusted": "r3-trusted@bitstream"
            },
            "screen_mentions": true
        })
    );

    // Also verify the rendered TOML parses cleanly into Rust `Config`
    let toml_text = val["tomlText"].as_str().expect("tomlText string");
    let parsed = Config::from_toml_str_with_env(toml_text, None)
        .expect("Rendered TOML from Nix module must load cleanly into Rust Config");
    assert_eq!(route_for(&parsed, R2).ref_for_tier("public"), "genie");
    assert!(!route_for(&parsed, R2).screen_mentions);
    assert_eq!(
        route_for(&parsed, "!r3:example.org").ref_for_tier("public"),
        "r3-public@bitstream"
    );
    assert_eq!(
        route_for(&parsed, "!r3:example.org").ref_for_tier("trusted"),
        "r3-trusted@bitstream"
    );
}
