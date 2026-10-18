use async_trait::async_trait;
use matrix_xmsg::bot::{
    handle_inbox_delivery_with_xmsg, handle_incoming_event, BotOutcome, IncomingMatrixEvent,
};
use matrix_xmsg::config::{Admission, Config, ExpertRefs, GuardFailureMode};
use matrix_xmsg::error::AppError;
use matrix_xmsg::matrix::MockMatrixClient;
use matrix_xmsg::store::Store;
use matrix_xmsg::xmsg::{SendResponse, SvcDelivery, SvcInbox, XmsgClient};
use serde_json::json;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

fn test_config() -> Config {
    Config {
        homeserver_url: "https://matrix.example.org".to_string(),
        bot_mxid: "@genie:example.org".to_string(),
        access_token_file: PathBuf::from("/tmp/token"),
        rooms: vec!["!room:example.org".to_string()],
        trusted_mxids: vec!["@trusted:example.org".to_string()],
        owner_mxid: "@owner:example.org".to_string(),
        admission: Admission::Public,
        xmsg_socket: PathBuf::from("/tmp/xmsg"),
        expert_ref: "genie-expert".to_string(),
        expert_refs: Some(ExpertRefs {
            public: "genie-expert@genie-public.bitstream".to_string(),
            trusted: "genie-expert@genie-trusted.bitstream".to_string(),
        }),
        open_thread_nodes: vec!["sini.bitstream".to_string()],
        guard_ref: "genie-guard".to_string(),
        guard_timeout_secs: 150,
        guard_retry_budget: 3,
        screen_mentions: false,
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
        rate_limit_count: 10,
        rate_limit_window_secs: 600,
        size_cap_bytes: 4096,
        answer_timeout_secs: 300,
        session_live_secs: 3600,
        db_path: PathBuf::from(":memory:"),
    }
}

#[derive(Default)]
struct MockXmsg {
    sent: Arc<Mutex<Vec<(String, String, String)>>>,
    replies: Arc<Mutex<Vec<(String, String)>>>,
    counter: AtomicUsize,
}

#[async_trait]
impl XmsgClient for MockXmsg {
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
            format!("01M28MSG{idx:06}"),
            Some(format!("sess:{expert_ref}")),
        ))
    }

    async fn reply_message(&self, message_id: &str, text: &str) -> Result<SendResponse, AppError> {
        self.replies
            .lock()
            .unwrap()
            .push((message_id.to_string(), text.to_string()));
        let idx = self.counter.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(SendResponse::new(
            format!("01M28REPLY{idx:06}"),
            Some("sess:reply".to_string()),
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

/// Oracle 1: A forward from a public sender addresses the `public` expert ref;
/// a forward from a trusted sender (and an owner sender, and a thread after `!trust`)
/// addresses the `trusted` expert ref.
#[tokio::test]
async fn test_oracle1_tier_routing_and_trust_promotion() {
    let config = test_config();
    let store = Store::new_in_memory().unwrap();
    let matrix = MockMatrixClient::default();
    let xmsg = MockXmsg::default();

    // 1. Public sender top-level mention -> routes to expert_refs.public
    let ev_public = IncomingMatrixEvent {
        room_id: "!room:example.org".to_string(),
        event_id: "$pub_root".to_string(),
        sender_mxid: "@stranger:other.org".to_string(),
        body: "@genie:example.org hello from public".to_string(),
        formatted_body: None,
        mentions: None,
        thread_root_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
        replaces_event_id: None,
        timestamp_ms: 1_000_000,
    };
    let out1 = handle_incoming_event(&ev_public, &[], &config, &matrix, &xmsg, &store)
        .await
        .unwrap();
    assert_eq!(out1, BotOutcome::Forwarded);
    {
        let sent = xmsg.sent.lock().unwrap();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].0, "genie-expert@genie-public.bitstream");
        assert!(
            sent[0].2.contains(r#"context="bootstrap""#),
            "First forward must be Bootstrap: {}",
            sent[0].2
        );
    }

    // 2. Trusted sender top-level mention -> routes to expert_refs.trusted
    let ev_trusted = IncomingMatrixEvent {
        room_id: "!room:example.org".to_string(),
        event_id: "$trusted_root".to_string(),
        sender_mxid: "@trusted:example.org".to_string(),
        body: "@genie:example.org hello from trusted".to_string(),
        formatted_body: None,
        mentions: None,
        thread_root_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
        replaces_event_id: None,
        timestamp_ms: 1_005_000,
    };
    let out2 = handle_incoming_event(&ev_trusted, &[], &config, &matrix, &xmsg, &store)
        .await
        .unwrap();
    assert_eq!(out2, BotOutcome::Forwarded);
    {
        let sent = xmsg.sent.lock().unwrap();
        assert_eq!(sent.len(), 2);
        assert_eq!(sent[1].0, "genie-expert@genie-trusted.bitstream");
    }

    // 3. Owner sender top-level mention -> also routes to expert_refs.trusted (two tiers only)
    let ev_owner = IncomingMatrixEvent {
        room_id: "!room:example.org".to_string(),
        event_id: "$owner_root".to_string(),
        sender_mxid: "@owner:example.org".to_string(),
        body: "@genie:example.org hello from owner".to_string(),
        formatted_body: None,
        mentions: None,
        thread_root_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
        replaces_event_id: None,
        timestamp_ms: 1_010_000,
    };
    let out3 = handle_incoming_event(&ev_owner, &[], &config, &matrix, &xmsg, &store)
        .await
        .unwrap();
    assert_eq!(out3, BotOutcome::Forwarded);
    {
        let sent = xmsg.sent.lock().unwrap();
        assert_eq!(sent.len(), 3);
        assert_eq!(sent[2].0, "genie-expert@genie-trusted.bitstream");
    }

    // 4. Owner posts `!trust` in the public thread ($pub_root) -> control emitted to trusted ref,
    //    and subsequent message in $pub_root from the public sender routes to expert_refs.trusted
    let ev_trust_cmd = IncomingMatrixEvent {
        room_id: "!room:example.org".to_string(),
        event_id: "$trust_cmd".to_string(),
        sender_mxid: "@owner:example.org".to_string(),
        body: "!trust".to_string(),
        formatted_body: None,
        mentions: None,
        thread_root_id: Some("$pub_root".to_string()),
        in_reply_to_event_id: None,
        is_falling_back: false,
        replaces_event_id: None,
        timestamp_ms: 1_015_000,
    };
    let out_trust = handle_incoming_event(&ev_trust_cmd, &[], &config, &matrix, &xmsg, &store)
        .await
        .unwrap();
    assert_eq!(out_trust, BotOutcome::ControlEmitted);
    assert!(store.is_thread_promoted("$pub_root").unwrap());
    {
        let sent = xmsg.sent.lock().unwrap();
        assert_eq!(sent.len(), 4);
        assert_eq!(sent[3].0, "genie-expert@genie-trusted.bitstream");
    }

    // 5. Public sender follows up in the promoted thread ($pub_root) within session_live_secs
    let ev_pub_followup = IncomingMatrixEvent {
        room_id: "!room:example.org".to_string(),
        event_id: "$pub_followup".to_string(),
        sender_mxid: "@stranger:other.org".to_string(),
        body: "@genie:example.org follow-up after !trust".to_string(),
        formatted_body: None,
        mentions: None,
        thread_root_id: Some("$pub_root".to_string()),
        in_reply_to_event_id: None,
        is_falling_back: false,
        replaces_event_id: None,
        timestamp_ms: 1_020_000,
    };
    let out_followup =
        handle_incoming_event(&ev_pub_followup, &[], &config, &matrix, &xmsg, &store)
            .await
            .unwrap();
    assert_eq!(out_followup, BotOutcome::Forwarded);
    {
        let sent = xmsg.sent.lock().unwrap();
        assert_eq!(sent.len(), 5);
        assert_eq!(
            sent[4].0, "genie-expert@genie-trusted.bitstream",
            "Promoted thread must route subsequent forwards to trusted expert ref"
        );
        // Because target_ref changed from public -> trusted on a Delta-eligible thread,
        // the forward must be Bootstrap (`context="bootstrap"`).
        assert!(
            sent[4].2.contains(r#"context="bootstrap""#),
            "Target-ref change on Delta-eligible thread must force Bootstrap, got: {}",
            sent[4].2
        );
    }
}

/// Oracle 2 (Ruling #3): A target ref that changed on a Delta-eligible thread makes the forward
/// `Bootstrap` (generic rule comparing cursor `target_ref` against current target ref).
#[tokio::test]
async fn test_oracle2_target_ref_change_forces_bootstrap_on_delta_eligible_thread() {
    let mut config = test_config();
    let store = Store::new_in_memory().unwrap();
    let matrix = MockMatrixClient::default();
    let xmsg = MockXmsg::default();

    // Turn 1: Trusted user starts thread -> Bootstrap to trusted ref ("genie-expert@genie-trusted.bitstream")
    let ev1 = IncomingMatrixEvent {
        room_id: "!room:example.org".to_string(),
        event_id: "$t_root".to_string(),
        sender_mxid: "@trusted:example.org".to_string(),
        body: "@genie:example.org first turn".to_string(),
        formatted_body: None,
        mentions: None,
        thread_root_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
        replaces_event_id: None,
        timestamp_ms: 2_000_000,
    };
    handle_incoming_event(&ev1, &[], &config, &matrix, &xmsg, &store)
        .await
        .unwrap();

    // Turn 2 (10s later, same target ref): must be Delta (`context="delta"`)
    let ev2 = IncomingMatrixEvent {
        room_id: "!room:example.org".to_string(),
        event_id: "$t_msg2".to_string(),
        sender_mxid: "@trusted:example.org".to_string(),
        body: "@genie:example.org second turn same target".to_string(),
        formatted_body: None,
        mentions: None,
        thread_root_id: Some("$t_root".to_string()),
        in_reply_to_event_id: None,
        is_falling_back: false,
        replaces_event_id: None,
        timestamp_ms: 2_010_000,
    };
    handle_incoming_event(&ev2, &[], &config, &matrix, &xmsg, &store)
        .await
        .unwrap();
    {
        let sent = xmsg.sent.lock().unwrap();
        assert_eq!(sent.len(), 2);
        assert!(
            sent[1].2.contains(r#"context="delta""#),
            "Unchanged target ref within session_live_secs must be Delta, got: {}",
            sent[1].2
        );
    }

    // Turn 3 (10s later, still within session_live_secs, but target ref changed via config):
    // must be Bootstrap (`context="bootstrap"`), NOT Delta!
    config.expert_refs = Some(ExpertRefs {
        public: "genie-expert@genie-public.bitstream".to_string(),
        trusted: "genie-expert@genie-trusted-v2.bitstream".to_string(),
    });
    let ev3 = IncomingMatrixEvent {
        room_id: "!room:example.org".to_string(),
        event_id: "$t_msg3".to_string(),
        sender_mxid: "@trusted:example.org".to_string(),
        body: "@genie:example.org third turn after target ref change".to_string(),
        formatted_body: None,
        mentions: None,
        thread_root_id: Some("$t_root".to_string()),
        in_reply_to_event_id: None,
        is_falling_back: false,
        replaces_event_id: None,
        timestamp_ms: 2_020_000,
    };
    handle_incoming_event(&ev3, &[], &config, &matrix, &xmsg, &store)
        .await
        .unwrap();
    {
        let sent = xmsg.sent.lock().unwrap();
        assert_eq!(sent.len(), 3);
        assert_eq!(sent[2].0, "genie-expert@genie-trusted-v2.bitstream");
        assert!(
            sent[2].2.contains(r#"context="bootstrap""#),
            "Changed target ref on Delta-eligible thread must force Bootstrap, got: {}",
            sent[2].2
        );
    }
}

/// Oracle 3: An `open_thread` delivery from a node in `open_thread_nodes` succeeds;
/// one from a node not in the list is rejected and does not post to Matrix.
#[tokio::test]
async fn test_oracle3_open_thread_nodes_allowlist() {
    let config = test_config(); // open_thread_nodes = ["sini.bitstream"]
    let store = Store::new_in_memory().unwrap();
    let matrix = MockMatrixClient::default();
    let xmsg = MockXmsg::default();
    let mut inbox = MockInbox::new("matrix-xmsg.bitstream");

    // 1. Delivery from `genie-public.bitstream` (not in open_thread_nodes) -> rejected, no Matrix post
    let untrusted_delivery = SvcDelivery {
        message_id: "01M28OPEN_BAD".to_string(),
        from_name: "genie-expert@genie-public.bitstream".to_string(),
        text: json!({
            "open_thread": {
                "room": "!room:example.org",
                "text": "Should not be posted"
            }
        })
        .to_string(),
        envelope: String::new(),
        in_reply_to: None,
        origin: Some(json!({
            "id": "01EXPERT",
            "node": "genie-public.bitstream",
            "label": "genie-expert"
        })),
    };
    let out_bad = handle_inbox_delivery_with_xmsg(
        &untrusted_delivery,
        &config,
        &matrix,
        &store,
        &xmsg,
        &mut inbox,
        3_000,
    )
    .await
    .unwrap();
    assert_eq!(out_bad, BotOutcome::OpenThreadRefusal);
    assert!(
        matrix.sent_notices.lock().unwrap().is_empty(),
        "Rejected open_thread must not post to Matrix"
    );

    // 2. Delivery from `sini.bitstream` (in open_thread_nodes, even though inbox own_node is `matrix-xmsg.bitstream`) -> succeeds
    let allowed_delivery = SvcDelivery {
        message_id: "01M28OPEN_OK".to_string(),
        from_name: "owner-agent@sini.bitstream".to_string(),
        text: json!({
            "open_thread": {
                "room": "!room:example.org",
                "text": "Hello from owner node"
            }
        })
        .to_string(),
        envelope: String::new(),
        in_reply_to: None,
        origin: Some(json!({
            "id": "01OWNER",
            "node": "sini.bitstream",
            "label": "owner-agent"
        })),
    };
    let out_ok = handle_inbox_delivery_with_xmsg(
        &allowed_delivery,
        &config,
        &matrix,
        &store,
        &xmsg,
        &mut inbox,
        3_010,
    )
    .await
    .unwrap();
    assert_eq!(out_ok, BotOutcome::OpenThreadSuccess);
    assert_eq!(matrix.sent_notices.lock().unwrap().len(), 1);
}

/// Oracle 4: Neither `xmsg_socket` nor `XDG_RUNTIME_DIR` set fails with an error naming both.
#[test]
fn test_oracle4_missing_xmsg_socket_and_xdg_runtime_dir_fails_naming_both() {
    let toml_without_socket = r#"
        homeserver_url = "https://matrix.example.org"
        bot_mxid = "@genie:example.org"
        access_token_file = "/tmp/token"
        rooms = ["!room:example.org"]
        trusted_mxids = []
        owner_mxid = "@owner:example.org"
    "#;
    let err = Config::from_toml_str_with_env(toml_without_socket, None)
        .expect_err("Config without xmsg_socket or XDG_RUNTIME_DIR must fail")
        .to_string();
    assert!(
        err.contains("xmsg_socket") && err.contains("XDG_RUNTIME_DIR"),
        "Error must name both `xmsg_socket` and `XDG_RUNTIME_DIR`, got: {err}"
    );
}
