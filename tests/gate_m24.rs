//! M24 gating oracles: redact and edit of the bot's own posts (`matrix-xmsg`).
//!
//! Tested against a mock Matrix homeserver (`wiremock` + `MatrixSdkClient`), as in `tests/gate_m2.rs`:
//! 1. Redact from the authoring principal produces one redaction of the right event in the right room
//!    (tested both as a fresh `send` to `matrix-xmsg` and as a `reply`).
//! 2. Redact or edit from a different principal (or missing origin, or a principal forging the author's
//!    display label) is refused, and the homeserver sees no request.
//! 3. Edit from the authoring principal produces `m.replace` on the original event with the new body
//!    (tested both as a fresh `send` and as a `reply`).
//! 4. An unknown ID is refused with an error reply, and the homeserver sees no request.
//! 5. The ID -> (room, event, author) record survives a bot restart (on-disk SQLite + cold `MatrixSdkClient`).
//! 6. `requested_by` authorization:
//!    (a) A trusted user's request (`owner_mxid` or `trusted_mxids`) redacts, even from a different agent session.
//!    (b) The asker's request (`answered_mxid`) redacts, even when untrusted and from a different agent session.
//!    (c) An untrusted non-asker's request is refused with zero homeserver requests (even if the agent supplies forged sender fields or is the authoring agent).
//!    (d) A `requested_by` forwarded to a different agent (or unknown `requested_by`, or `requested_by` on `edit`) is refused with zero homeserver requests.

use async_trait::async_trait;
use matrix_sdk::config::SyncSettings;
use matrix_xmsg::bot::{handle_inbox_delivery_with_xmsg, BotOutcome};
use matrix_xmsg::config::{Admission, Config};
use matrix_xmsg::error::AppError;
use matrix_xmsg::matrix::MatrixSdkClient;
use matrix_xmsg::store::{ForwardedMessageRecord, Store};
use matrix_xmsg::xmsg::{SendResponse, SvcDelivery, SvcInbox, XmsgClient};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use tempfile::tempdir;
use wiremock::matchers::{method, path, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[derive(Default)]
struct RecordingXmsgClient {
    sent_messages: Mutex<Vec<(String, String, String)>>,
    replies: Mutex<Vec<(String, String)>>,
    counter: AtomicUsize,
}

#[async_trait]
impl XmsgClient for RecordingXmsgClient {
    async fn send_message(
        &self,
        expert_ref: &str,
        from: &str,
        text: &str,
    ) -> Result<SendResponse, AppError> {
        self.sent_messages.lock().unwrap().push((
            expert_ref.to_string(),
            from.to_string(),
            text.to_string(),
        ));
        let n = self.counter.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(SendResponse::new(
            format!("01M24FWD{n:04}"),
            Some("author-id-1@sini.cortex".to_string()),
        ))
    }

    async fn reply_message(&self, message_id: &str, text: &str) -> Result<SendResponse, AppError> {
        self.replies
            .lock()
            .unwrap()
            .push((message_id.to_string(), text.to_string()));
        let n = self.counter.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(SendResponse::new(format!("01M24BOTREPLY{n:04}"), None))
    }
}

#[derive(Default)]
struct RecordingInbox {
    acked: Vec<String>,
}

#[async_trait]
impl SvcInbox for RecordingInbox {
    async fn poll(&mut self, _wait_secs: u64) -> Result<Option<SvcDelivery>, AppError> {
        Ok(None)
    }

    async fn ack(&mut self, message_id: &str) -> Result<(), AppError> {
        self.acked.push(message_id.to_string());
        Ok(())
    }

    fn node(&self) -> Option<String> {
        Some("sini.cortex".to_string())
    }
}

fn test_config(homeserver_url: &str, db_path: PathBuf) -> Config {
    Config {
        homeserver_url: homeserver_url.to_string(),
        bot_mxid: "@genie:example.org".to_string(),
        access_token_file: PathBuf::from("/nonexistent/token"),
        rooms: vec![
            "!public:example.org".to_string(),
            "!second:example.org".to_string(),
        ],
        trusted_mxids: vec!["@alice:example.org".to_string()],
        owner_mxid: "@owner:example.org".to_string(),
        admission: Admission::Trusted,
        xmsg_socket: PathBuf::from("/run/user/1000/xmsg"),
        expert_ref: "claude".to_string(),
        expert_refs: None,
        room_routes: std::collections::BTreeMap::new(),
        open_thread_nodes: vec![],
        history_n: 5,
        history_byte_cap: 1024,
        resync_byte_cap: 65536,
        resync_page_bytes: 8192,
        context_max_lines: 20,
        context_max_bytes: 8192,
        sender_window_secs: 900,
        context_rate_limit_count: 6,
        context_rate_limit_window_secs: 600,
        guard_ref: "genie-guard".to_string(),
        guard_timeout_secs: 30,
        guard_retry_budget: 3,
        screen_mentions: false,
        guard_failure_mode: matrix_xmsg::config::GuardFailureMode::Closed,
        guard_outage_dm_window_secs: 3600,
        inbox_retry_budget: 10,
        rate_limit_count: 10,
        rate_limit_window_secs: 60,
        size_cap_bytes: 2048,
        answer_timeout_secs: 300,
        session_live_secs: 3600,
        db_path,
    }
}

async fn setup_homeserver_with_rooms(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/_matrix/client/versions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "versions": ["v1.1", "v1.2", "v1.3", "v1.4", "v1.5", "v1.6", "v1.7", "v1.8", "v1.9", "v1.10", "v1.11"]
        })))
        .mount(server)
        .await;

    let sync_response = json!({
        "next_batch": "batch_m24_1",
        "rooms": {
            "join": {
                "!public:example.org": {
                    "timeline": { "events": [] }
                },
                "!second:example.org": {
                    "timeline": { "events": [] }
                }
            }
        }
    });

    Mock::given(method("GET"))
        .and(path("/_matrix/client/v3/sync"))
        .respond_with(ResponseTemplate::new(200).set_body_json(sync_response))
        .mount(server)
        .await;
}

async fn synced_sdk_client(config: &Config) -> MatrixSdkClient {
    let sdk = MatrixSdkClient::new(&config.homeserver_url, &config.bot_mxid, "syt_m24_token")
        .await
        .expect("MatrixSdkClient::new");
    sdk.set_configured_rooms(&config.rooms);
    sdk.inner()
        .sync_once(SyncSettings::default())
        .await
        .expect("initial sync_once");
    sdk
}

fn author_origin() -> Value {
    json!({
        "id": "author-id-1",
        "node": "sini.cortex",
        "label": "claude"
    })
}

fn other_origin_forging_author_label() -> Value {
    json!({
        "id": "imposter-id-2",
        "node": "sini.cortex",
        "label": "claude"
    })
}

fn record_forward(store: &Store, fwd_id: &str, room_id: &str, root_id: &str) {
    store
        .record_forwarded_message(&ForwardedMessageRecord {
            message_id: fwd_id.to_string(),
            room_id: room_id.to_string(),
            thread_root_id: root_id.to_string(),
            event_id: root_id.to_string(),
            sender_mxid: "@alice:example.org".to_string(),
            addressed: true,
            sent_at: 1000,
            ack_reaction_id: None,
            timeout_dm_sent: false,
            reply_count: 0,
        })
        .unwrap();
}

// =============================================================================
// Oracle 1: A redact from the authoring principal produces one redaction of the
// right event in the right room (both via a fresh `send` and via a `reply`).
// Mutant: wrong lookup => RED.
// =============================================================================
#[tokio::test]
async fn oracle_1_author_redact_redacts_right_event_in_right_room_both_arrival_paths() {
    let server = MockServer::start().await;
    setup_homeserver_with_rooms(&server).await;

    let config = test_config(&server.uri(), PathBuf::from(":memory:"));
    let store = Store::new_in_memory().unwrap();
    let xmsg = RecordingXmsgClient::default();
    let mut inbox = RecordingInbox::default();
    let sdk = synced_sdk_client(&config).await;

    record_forward(&store, "01FWD_PUBLIC", "!public:example.org", "$root_pub");
    record_forward(&store, "01FWD_SECOND", "!second:example.org", "$root_sec");

    // Mount two distinct room send endpoints returning distinct Matrix event IDs.
    Mock::given(method("PUT"))
        .and(path_regex(
            r"^/_matrix/client/v3/rooms/!public(%3A|:)example\.org/send/m\.room\.message/.*$",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "event_id": "$bot_post_pub_1"
        })))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("PUT"))
        .and(path_regex(
            r"^/_matrix/client/v3/rooms/!second(%3A|:)example\.org/send/m\.room\.message/.*$",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "event_id": "$bot_post_sec_2"
        })))
        .expect(1)
        .mount(&server)
        .await;

    // 1. Author posts two replies: 01REPLY_PUB_1 and 01REPLY_SEC_2.
    let reply_pub = SvcDelivery {
        message_id: "01REPLY_PUB_1".to_string(),
        from_name: "claude · author-id-1@sini.cortex".to_string(),
        text: "First answer containing accidental config snippet".to_string(),
        envelope: "".to_string(),
        origin: Some(author_origin()),
        in_reply_to: Some("01FWD_PUBLIC".to_string()),
    };
    let out1 =
        handle_inbox_delivery_with_xmsg(&reply_pub, &config, &sdk, &store, &xmsg, &mut inbox, 1010)
            .await
            .unwrap();
    assert_eq!(out1, BotOutcome::Replied);

    let reply_sec = SvcDelivery {
        message_id: "01REPLY_SEC_2".to_string(),
        from_name: "claude · author-id-1@sini.cortex".to_string(),
        text: "Second answer in second room".to_string(),
        envelope: "".to_string(),
        origin: Some(author_origin()),
        in_reply_to: Some("01FWD_SECOND".to_string()),
    };
    let out2 =
        handle_inbox_delivery_with_xmsg(&reply_sec, &config, &sdk, &store, &xmsg, &mut inbox, 1020)
            .await
            .unwrap();
    assert_eq!(out2, BotOutcome::Replied);

    // No extra acknowledgement reply is sent when posting normal replies.
    assert!(xmsg.replies.lock().unwrap().is_empty());

    // Expect 1 redaction of $bot_post_pub_1 in !public:example.org and 1 of $bot_post_sec_2 in !second:example.org.
    Mock::given(method("PUT"))
        .and(path_regex(
            r"^/_matrix/client/v3/rooms/!public(%3A|:)example\.org/redact/\$bot_post_pub_1/.*$",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "event_id": "$redact_ev_1"
        })))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("PUT"))
        .and(path_regex(
            r"^/_matrix/client/v3/rooms/!second(%3A|:)example\.org/redact/\$bot_post_sec_2/.*$",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "event_id": "$redact_ev_2"
        })))
        .expect(1)
        .mount(&server)
        .await;

    // Cell 1a (Arrival path A: fresh `send` to `matrix-xmsg`, `in_reply_to: None`):
    let redact_via_send = SvcDelivery {
        message_id: "01REDACT_SEND_1".to_string(),
        from_name: "claude · author-id-1@sini.cortex".to_string(),
        text: json!({
            "redact": "01REPLY_PUB_1",
            "reason": "accidental private config"
        })
        .to_string(),
        envelope: "".to_string(),
        origin: Some(author_origin()),
        in_reply_to: None,
    };
    let out_redact_send = handle_inbox_delivery_with_xmsg(
        &redact_via_send,
        &config,
        &sdk,
        &store,
        &xmsg,
        &mut inbox,
        1030,
    )
    .await
    .unwrap();
    assert_eq!(out_redact_send, BotOutcome::Redacted);

    // Cell 1b (Arrival path B: `reply` to an arbitrary message, `in_reply_to: Some(...)`):
    let redact_via_reply = SvcDelivery {
        message_id: "01REDACT_REPLY_2".to_string(),
        from_name: "claude · author-id-1@sini.cortex".to_string(),
        text: json!({
            "redact": "01REPLY_SEC_2"
        })
        .to_string(),
        envelope: "[xmsg] reply to message_id=01UNRELATED — message_id=01REDACT_REPLY_2"
            .to_string(),
        origin: Some(author_origin()),
        in_reply_to: Some("01UNRELATED".to_string()),
    };
    let out_redact_reply = handle_inbox_delivery_with_xmsg(
        &redact_via_reply,
        &config,
        &sdk,
        &store,
        &xmsg,
        &mut inbox,
        1040,
    )
    .await
    .unwrap();
    assert_eq!(out_redact_reply, BotOutcome::Redacted);

    // Verify the redact request body on !public:example.org carried the optional reason.
    let reqs = server.received_requests().await.unwrap();
    let pub_redact_req = reqs
        .iter()
        .find(|r| r.url.path().contains("/redact/"))
        .expect("redact request must be received");
    let pub_redact_json: Value = serde_json::from_slice(&pub_redact_req.body).unwrap();
    assert_eq!(pub_redact_json["reason"], "accidental private config");
}

// =============================================================================
// Oracle 2: A redact or edit from a different principal (or with no origin, even
// if `from_name` or `origin.label` matches) is refused, and the homeserver sees
// no request.
// Mutant: the authorization check is skipped => RED.
// =============================================================================
#[tokio::test]
async fn oracle_2_different_principal_or_missing_origin_refused_and_homeserver_untouched() {
    let server = MockServer::start().await;
    setup_homeserver_with_rooms(&server).await;

    let config = test_config(&server.uri(), PathBuf::from(":memory:"));
    let store = Store::new_in_memory().unwrap();
    let xmsg = RecordingXmsgClient::default();
    let mut inbox = RecordingInbox::default();
    let sdk = synced_sdk_client(&config).await;

    record_forward(&store, "01FWD_O2", "!public:example.org", "$root_o2");

    // Expect exactly 1 send_notice (for the initial author reply) and 0 redactions / 0 further sends.
    Mock::given(method("PUT"))
        .and(path_regex(
            r"^/_matrix/client/v3/rooms/.*/send/m\.room\.message/.*$",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "event_id": "$bot_post_o2"
        })))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("PUT"))
        .and(path_regex(r"^/_matrix/client/v3/rooms/.*/redact/.*$"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "event_id": "$never_redact"
        })))
        .expect(0)
        .mount(&server)
        .await;

    let author_reply = SvcDelivery {
        message_id: "01REPLY_O2".to_string(),
        from_name: "claude · author-id-1@sini.cortex".to_string(),
        text: "Original author post".to_string(),
        envelope: "".to_string(),
        origin: Some(author_origin()),
        in_reply_to: Some("01FWD_O2".to_string()),
    };
    let out = handle_inbox_delivery_with_xmsg(
        &author_reply,
        &config,
        &sdk,
        &store,
        &xmsg,
        &mut inbox,
        1000,
    )
    .await
    .unwrap();
    assert_eq!(out, BotOutcome::Replied);

    // 1. Different principal (even with identical self-chosen label "claude"): redact refused.
    let bad_redact = SvcDelivery {
        message_id: "01BAD_REDACT_1".to_string(),
        from_name: "claude · author-id-1@sini.cortex".to_string(),
        text: json!({ "redact": "01REPLY_O2", "reason": "malicious delete" }).to_string(),
        envelope: "".to_string(),
        origin: Some(other_origin_forging_author_label()),
        in_reply_to: None,
    };
    let out_bad_redact = handle_inbox_delivery_with_xmsg(
        &bad_redact,
        &config,
        &sdk,
        &store,
        &xmsg,
        &mut inbox,
        1010,
    )
    .await
    .unwrap();
    assert_eq!(out_bad_redact, BotOutcome::RedactRefused);

    // 2. Different principal: edit refused.
    let bad_edit = SvcDelivery {
        message_id: "01BAD_EDIT_2".to_string(),
        from_name: "claude · author-id-1@sini.cortex".to_string(),
        text: json!({ "edit": "01REPLY_O2", "answer": "malicious overwrite" }).to_string(),
        envelope: "".to_string(),
        origin: Some(other_origin_forging_author_label()),
        in_reply_to: Some("01FWD_O2".to_string()),
    };
    let out_bad_edit =
        handle_inbox_delivery_with_xmsg(&bad_edit, &config, &sdk, &store, &xmsg, &mut inbox, 1020)
            .await
            .unwrap();
    assert_eq!(out_bad_edit, BotOutcome::EditRefused);

    // 3. Missing origin (`origin: None`), even when `from_name` matches the author's display string: refused!
    let no_origin_redact = SvcDelivery {
        message_id: "01NO_ORIGIN_3".to_string(),
        from_name: "claude · author-id-1@sini.cortex".to_string(),
        text: json!({ "redact": "01REPLY_O2" }).to_string(),
        envelope: "".to_string(),
        origin: None,
        in_reply_to: None,
    };
    let out_no_origin = handle_inbox_delivery_with_xmsg(
        &no_origin_redact,
        &config,
        &sdk,
        &store,
        &xmsg,
        &mut inbox,
        1030,
    )
    .await
    .unwrap();
    assert_eq!(out_no_origin, BotOutcome::RedactRefused);

    // All three received error replies naming the refusal reason.
    let replies = xmsg.replies.lock().unwrap();
    assert_eq!(replies.len(), 3);
    for (_, err_body) in replies.iter() {
        let v: Value = serde_json::from_str(err_body).unwrap();
        assert!(
            v["error"]
                .as_str()
                .unwrap_or("")
                .contains("unauthorized: only the authoring principal"),
            "refusal must name the authorization error: {err_body}"
        );
    }
}

// =============================================================================
// Oracle 3: An edit produces `m.replace` on the original event with the new body,
// rendered like a normal reply (both via a fresh `send` and via a `reply`).
// Mutant: send a plain thread notice instead of `m.replace` => RED.
// =============================================================================
#[tokio::test]
async fn oracle_3_edit_produces_m_replace_on_original_event_both_arrival_paths() {
    let server = MockServer::start().await;
    setup_homeserver_with_rooms(&server).await;

    let config = test_config(&server.uri(), PathBuf::from(":memory:"));
    let store = Store::new_in_memory().unwrap();
    let xmsg = RecordingXmsgClient::default();
    let mut inbox = RecordingInbox::default();
    let sdk = synced_sdk_client(&config).await;

    record_forward(&store, "01FWD_O3", "!public:example.org", "$root_o3");

    // 1 initial notice + 2 edit notices = 3 PUT /send/m.room.message requests.
    Mock::given(method("PUT"))
        .and(path_regex(
            r"^/_matrix/client/v3/rooms/!public(%3A|:)example\.org/send/m\.room\.message/.*$",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "event_id": "$bot_post_o3"
        })))
        .expect(3)
        .mount(&server)
        .await;

    let author_reply = SvcDelivery {
        message_id: "01REPLY_O3".to_string(),
        from_name: "claude · author-id-1@sini.cortex".to_string(),
        text: "Initial draft answer".to_string(),
        envelope: "".to_string(),
        origin: Some(author_origin()),
        in_reply_to: Some("01FWD_O3".to_string()),
    };
    let out = handle_inbox_delivery_with_xmsg(
        &author_reply,
        &config,
        &sdk,
        &store,
        &xmsg,
        &mut inbox,
        1000,
    )
    .await
    .unwrap();
    assert_eq!(out, BotOutcome::Replied);

    // Cell 3a: Edit arriving as a fresh `send` (`in_reply_to: None`)
    let edit_via_send = SvcDelivery {
        message_id: "01EDIT_SEND_1".to_string(),
        from_name: "claude · author-id-1@sini.cortex".to_string(),
        text: json!({
            "edit": "01REPLY_O3",
            "answer": "Updated answer with [Alice](https://matrix.to/#/@alice:example.org)"
        })
        .to_string(),
        envelope: "".to_string(),
        origin: Some(author_origin()),
        in_reply_to: None,
    };
    let out_edit_1 = handle_inbox_delivery_with_xmsg(
        &edit_via_send,
        &config,
        &sdk,
        &store,
        &xmsg,
        &mut inbox,
        1010,
    )
    .await
    .unwrap();
    assert_eq!(out_edit_1, BotOutcome::Edited);

    // Cell 3b: Edit arriving as a `reply` (`in_reply_to: Some(...)`)
    let edit_via_reply = SvcDelivery {
        message_id: "01EDIT_REPLY_2".to_string(),
        from_name: "claude · author-id-1@sini.cortex".to_string(),
        text: json!({
            "edit": "01REPLY_O3",
            "answer": "Second revision via reply"
        })
        .to_string(),
        envelope: "[xmsg] reply to message_id=01FWD_O3 — message_id=01EDIT_REPLY_2".to_string(),
        origin: Some(author_origin()),
        in_reply_to: Some("01FWD_O3".to_string()),
    };
    let out_edit_2 = handle_inbox_delivery_with_xmsg(
        &edit_via_reply,
        &config,
        &sdk,
        &store,
        &xmsg,
        &mut inbox,
        1020,
    )
    .await
    .unwrap();
    assert_eq!(out_edit_2, BotOutcome::Edited);

    // Inspect the two replacement requests sent to the homeserver.
    let reqs = server.received_requests().await.unwrap();
    let msg_reqs: Vec<_> = reqs
        .iter()
        .filter(|r| r.url.path().contains("/send/m.room.message/"))
        .collect();
    assert_eq!(msg_reqs.len(), 3);

    let edit1_body: Value = serde_json::from_slice(&msg_reqs[1].body).unwrap();
    assert_eq!(edit1_body["m.relates_to"]["rel_type"], "m.replace");
    assert_eq!(edit1_body["m.relates_to"]["event_id"], "$bot_post_o3");
    let new_content_1 = &edit1_body["m.new_content"];
    assert_eq!(new_content_1["msgtype"], "m.notice");
    assert!(new_content_1["body"]
        .as_str()
        .unwrap_or("")
        .contains("Updated answer with [Alice](https://matrix.to/#/@alice:example.org)"));
    assert!(new_content_1["body"]
        .as_str()
        .unwrap_or("")
        .contains("Controls: ✅ accept · 🔍 deeper · !deeper"));
    assert_eq!(
        new_content_1["m.mentions"]["user_ids"][0],
        "@alice:example.org"
    );

    let edit2_body: Value = serde_json::from_slice(&msg_reqs[2].body).unwrap();
    assert_eq!(edit2_body["m.relates_to"]["rel_type"], "m.replace");
    assert_eq!(edit2_body["m.relates_to"]["event_id"], "$bot_post_o3");
    assert!(edit2_body["m.new_content"]["body"]
        .as_str()
        .unwrap_or("")
        .contains("Second revision via reply"));
}

// =============================================================================
// Oracle 4: An unknown ID is refused with an error reply, and the homeserver
// sees no request.
// Mutant: omit the check or drop silently => RED.
// =============================================================================
#[tokio::test]
async fn oracle_4_unknown_id_refused_with_error_reply() {
    let server = MockServer::start().await;
    setup_homeserver_with_rooms(&server).await;

    let config = test_config(&server.uri(), PathBuf::from(":memory:"));
    let store = Store::new_in_memory().unwrap();
    let xmsg = RecordingXmsgClient::default();
    let mut inbox = RecordingInbox::default();
    let sdk = synced_sdk_client(&config).await;

    Mock::given(method("PUT"))
        .and(path_regex(
            r"^/_matrix/client/v3/rooms/.*/(send|redact)/.*$",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "event_id": "$never"
        })))
        .expect(0)
        .mount(&server)
        .await;

    let unknown_redact = SvcDelivery {
        message_id: "01UNKNOWN_REDACT".to_string(),
        from_name: "claude · author-id-1@sini.cortex".to_string(),
        text: json!({ "redact": "01NONEXISTENT_ID" }).to_string(),
        envelope: "".to_string(),
        origin: Some(author_origin()),
        in_reply_to: None,
    };
    let out1 = handle_inbox_delivery_with_xmsg(
        &unknown_redact,
        &config,
        &sdk,
        &store,
        &xmsg,
        &mut inbox,
        1000,
    )
    .await
    .unwrap();
    assert_eq!(out1, BotOutcome::RedactRefused);

    let unknown_edit = SvcDelivery {
        message_id: "01UNKNOWN_EDIT".to_string(),
        from_name: "claude · author-id-1@sini.cortex".to_string(),
        text: json!({ "edit": "01NONEXISTENT_ID", "answer": "new text" }).to_string(),
        envelope: "".to_string(),
        origin: Some(author_origin()),
        in_reply_to: None,
    };
    let out2 = handle_inbox_delivery_with_xmsg(
        &unknown_edit,
        &config,
        &sdk,
        &store,
        &xmsg,
        &mut inbox,
        1010,
    )
    .await
    .unwrap();
    assert_eq!(out2, BotOutcome::EditRefused);

    let replies = xmsg.replies.lock().unwrap();
    assert_eq!(replies.len(), 2);
    for (_, err_body) in replies.iter() {
        let v: Value = serde_json::from_str(err_body).unwrap();
        assert!(
            v["error"]
                .as_str()
                .unwrap_or("")
                .contains("unknown reply message id '01NONEXISTENT_ID'"),
            "refusal must name unknown reply message id: {err_body}"
        );
    }
}

// =============================================================================
// Oracle 5: The ID -> event record survives a bot restart (and still works a
// day later without expiring).
// Mutant: keep the map in memory or expire after session_live_secs => RED.
// =============================================================================
#[tokio::test]
async fn oracle_5_id_to_event_record_survives_bot_restart_and_does_not_expire() {
    let server = MockServer::start().await;
    setup_homeserver_with_rooms(&server).await;

    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("bot.db");
    let config = test_config(&server.uri(), db_path.clone());

    Mock::given(method("PUT"))
        .and(path_regex(
            r"^/_matrix/client/v3/rooms/!public(%3A|:)example\.org/send/m\.room\.message/.*$",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "event_id": "$bot_post_persist"
        })))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("PUT"))
        .and(path_regex(
            r"^/_matrix/client/v3/rooms/!public(%3A|:)example\.org/redact/\$bot_post_persist/.*$",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "event_id": "$redact_after_restart"
        })))
        .expect(1)
        .mount(&server)
        .await;

    // Bot instance 1: posts the reply and records it in the on-disk SQLite store.
    {
        let store1 = Store::new(&db_path).unwrap();
        let xmsg1 = RecordingXmsgClient::default();
        let mut inbox1 = RecordingInbox::default();
        let sdk1 = synced_sdk_client(&config).await;

        record_forward(&store1, "01FWD_O5", "!public:example.org", "$root_o5");

        let reply_delivery = SvcDelivery {
            message_id: "01REPLY_PERSIST".to_string(),
            from_name: "claude · author-id-1@sini.cortex".to_string(),
            text: "Post before bot restart".to_string(),
            envelope: "".to_string(),
            origin: Some(author_origin()),
            in_reply_to: Some("01FWD_O5".to_string()),
        };
        let out = handle_inbox_delivery_with_xmsg(
            &reply_delivery,
            &config,
            &sdk1,
            &store1,
            &xmsg1,
            &mut inbox1,
            1000,
        )
        .await
        .unwrap();
        assert_eq!(out, BotOutcome::Replied);
    }

    // Bot instance 2 (after restart, and 24 hours = 86_400s later, well past session_live_secs = 3600s,
    // with a cold MatrixSdkClient that hasn't even synced the room yet):
    {
        let store2 = Store::new(&db_path).unwrap();
        let xmsg2 = RecordingXmsgClient::default();
        let mut inbox2 = RecordingInbox::default();
        let sdk2 = MatrixSdkClient::new(&config.homeserver_url, &config.bot_mxid, "syt_m24_token")
            .await
            .unwrap();

        let redact_delivery = SvcDelivery {
            message_id: "01REDACT_AFTER_RESTART".to_string(),
            from_name: "claude · author-id-1@sini.cortex".to_string(),
            text: json!({
                "redact": "01REPLY_PERSIST",
                "reason": "spotted leak next day after restart"
            })
            .to_string(),
            envelope: "".to_string(),
            origin: Some(author_origin()),
            in_reply_to: None,
        };
        let out = handle_inbox_delivery_with_xmsg(
            &redact_delivery,
            &config,
            &sdk2,
            &store2,
            &xmsg2,
            &mut inbox2,
            1000 + 86_400,
        )
        .await
        .unwrap();
        assert_eq!(out, BotOutcome::Redacted);
    }
}

fn record_forward_with_sender_and_session(
    store: &Store,
    fwd_id: &str,
    room_id: &str,
    root_id: &str,
    sender_mxid: &str,
    session_id: &str,
) {
    store
        .record_forwarded_message(&ForwardedMessageRecord {
            message_id: fwd_id.to_string(),
            room_id: room_id.to_string(),
            thread_root_id: root_id.to_string(),
            event_id: root_id.to_string(),
            sender_mxid: sender_mxid.to_string(),
            addressed: true,
            sent_at: 1000,
            ack_reaction_id: None,
            timeout_dm_sent: false,
            reply_count: 0,
        })
        .unwrap();
    store.set_forward_session(fwd_id, Some(session_id)).unwrap();
}

// =============================================================================
// Oracle 6a: A trusted user's request (`requested_by`) redacts a reply posted by
// a different agent session, even when the original post answered an untrusted
// user.
// Mutant: `is_user_trusted` skipped / returns false => RED.
// =============================================================================
#[tokio::test]
async fn oracle_6a_requested_by_trusted_user_redacts_cross_session() {
    let server = MockServer::start().await;
    setup_homeserver_with_rooms(&server).await;

    let config = test_config(&server.uri(), PathBuf::from(":memory:"));
    let store = Store::new_in_memory().unwrap();
    let xmsg = RecordingXmsgClient::default();
    let mut inbox = RecordingInbox::default();
    let sdk = synced_sdk_client(&config).await;

    // @bob:example.org is NOT in trusted_mxids and is NOT owner_mxid.
    record_forward_with_sender_and_session(
        &store,
        "01FWD_BOB_ASK",
        "!public:example.org",
        "$root_6a",
        "@bob:example.org",
        "author-id-1@sini.cortex",
    );

    Mock::given(method("PUT"))
        .and(path_regex(
            r"^/_matrix/client/v3/rooms/!public(%3A|:)example\.org/send/m\.room\.message/.*$",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "event_id": "$bot_post_6a"
        })))
        .expect(1)
        .mount(&server)
        .await;

    // Agent 1 (`author-id-1@sini.cortex`) posts a reply answering @bob:example.org.
    let reply_delivery = SvcDelivery {
        message_id: "01REPLY_6A".to_string(),
        from_name: "claude · author-id-1@sini.cortex".to_string(),
        text: "Answer to Bob that Alice later asks to remove".to_string(),
        envelope: "".to_string(),
        origin: Some(author_origin()),
        in_reply_to: Some("01FWD_BOB_ASK".to_string()),
    };
    let out1 = handle_inbox_delivery_with_xmsg(
        &reply_delivery,
        &config,
        &sdk,
        &store,
        &xmsg,
        &mut inbox,
        1010,
    )
    .await
    .unwrap();
    assert_eq!(out1, BotOutcome::Replied);

    // Later, @alice:example.org (in `trusted_mxids`) asks in the thread to remove the post,
    // and that forward is delivered to a new session (`agent-2@sini.cortex`).
    record_forward_with_sender_and_session(
        &store,
        "01FWD_ALICE_REQ",
        "!public:example.org",
        "$root_6a",
        "@alice:example.org",
        "agent-2@sini.cortex",
    );

    Mock::given(method("PUT"))
        .and(path_regex(
            r"^/_matrix/client/v3/rooms/!public(%3A|:)example\.org/redact/\$bot_post_6a/.*$",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "event_id": "$redact_6a"
        })))
        .expect(1)
        .mount(&server)
        .await;

    let agent_2_origin = json!({
        "id": "agent-2",
        "node": "sini.cortex",
        "label": "claude"
    });
    let redact_delivery = SvcDelivery {
        message_id: "01REDACT_6A".to_string(),
        from_name: "claude · agent-2@sini.cortex".to_string(),
        text: json!({
            "redact": "01REPLY_6A",
            "requested_by": "01FWD_ALICE_REQ",
            "reason": "trusted user requested removal"
        })
        .to_string(),
        envelope: "".to_string(),
        origin: Some(agent_2_origin),
        in_reply_to: Some("01FWD_ALICE_REQ".to_string()),
    };
    let out2 = handle_inbox_delivery_with_xmsg(
        &redact_delivery,
        &config,
        &sdk,
        &store,
        &xmsg,
        &mut inbox,
        1020,
    )
    .await
    .unwrap();
    assert_eq!(out2, BotOutcome::Redacted);
    assert!(xmsg.replies.lock().unwrap().is_empty());
}

// =============================================================================
// Oracle 6b: The asker's request (`requested_by`) redacts the bot's reply to
// them, even when the asker is NOT in `trusted_mxids` / `owner_mxid` and the
// request comes from a different agent session.
// Mutant: `answered_mxid` check omitted => RED.
// =============================================================================
#[tokio::test]
async fn oracle_6b_requested_by_asker_redacts_even_when_untrusted_and_cross_session() {
    let server = MockServer::start().await;
    setup_homeserver_with_rooms(&server).await;

    let config = test_config(&server.uri(), PathBuf::from(":memory:"));
    let store = Store::new_in_memory().unwrap();
    let xmsg = RecordingXmsgClient::default();
    let mut inbox = RecordingInbox::default();
    let sdk = synced_sdk_client(&config).await;

    // @bob:example.org is untrusted (only @alice:example.org and @owner:example.org are trusted).
    assert!(!config.is_user_trusted("@bob:example.org"));
    record_forward_with_sender_and_session(
        &store,
        "01FWD_BOB_QUESTION",
        "!public:example.org",
        "$root_6b",
        "@bob:example.org",
        "author-id-1@sini.cortex",
    );

    Mock::given(method("PUT"))
        .and(path_regex(
            r"^/_matrix/client/v3/rooms/!public(%3A|:)example\.org/send/m\.room\.message/.*$",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "event_id": "$bot_post_6b"
        })))
        .expect(1)
        .mount(&server)
        .await;

    // Agent 1 answers Bob's question.
    let reply_delivery = SvcDelivery {
        message_id: "01REPLY_6B".to_string(),
        from_name: "claude · author-id-1@sini.cortex".to_string(),
        text: "Answer to Bob's question".to_string(),
        envelope: "".to_string(),
        origin: Some(author_origin()),
        in_reply_to: Some("01FWD_BOB_QUESTION".to_string()),
    };
    let out1 = handle_inbox_delivery_with_xmsg(
        &reply_delivery,
        &config,
        &sdk,
        &store,
        &xmsg,
        &mut inbox,
        1010,
    )
    .await
    .unwrap();
    assert_eq!(out1, BotOutcome::Replied);

    // Later, Bob himself asks for the bot's answer to him to be removed; delivered to `agent-2@sini.cortex`.
    record_forward_with_sender_and_session(
        &store,
        "01FWD_BOB_REMOVE",
        "!public:example.org",
        "$root_6b",
        "@bob:example.org",
        "agent-2@sini.cortex",
    );

    Mock::given(method("PUT"))
        .and(path_regex(
            r"^/_matrix/client/v3/rooms/!public(%3A|:)example\.org/redact/\$bot_post_6b/.*$",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "event_id": "$redact_6b"
        })))
        .expect(1)
        .mount(&server)
        .await;

    let agent_2_origin = json!({
        "id": "agent-2",
        "node": "sini.cortex",
        "label": "claude"
    });
    let redact_delivery = SvcDelivery {
        message_id: "01REDACT_6B".to_string(),
        from_name: "claude · agent-2@sini.cortex".to_string(),
        text: json!({
            "redact": "01REPLY_6B",
            "requested_by": "01FWD_BOB_REMOVE",
            "reason": "asker requested removal"
        })
        .to_string(),
        envelope: "".to_string(),
        origin: Some(agent_2_origin),
        in_reply_to: Some("01FWD_BOB_REMOVE".to_string()),
    };
    let out2 = handle_inbox_delivery_with_xmsg(
        &redact_delivery,
        &config,
        &sdk,
        &store,
        &xmsg,
        &mut inbox,
        1020,
    )
    .await
    .unwrap();
    assert_eq!(out2, BotOutcome::Redacted);
    assert!(xmsg.replies.lock().unwrap().is_empty());
}

// =============================================================================
// Oracle 6c: An untrusted non-asker's request is refused with zero homeserver
// requests — even if the agent supplies a forged `sender`/`sender_mxid` field
// claiming to be `@owner:example.org`, or even if the requesting agent is the
// authoring agent itself.
// Mutant 1: `is_user_trusted` skipped so any sender passes => RED.
// Mutant 2: sender taken from an agent-supplied JSON field => RED.
// =============================================================================
#[tokio::test]
async fn oracle_6c_requested_by_untrusted_non_asker_refused_with_zero_homeserver_requests() {
    let server = MockServer::start().await;
    setup_homeserver_with_rooms(&server).await;

    let config = test_config(&server.uri(), PathBuf::from(":memory:"));
    let store = Store::new_in_memory().unwrap();
    let xmsg = RecordingXmsgClient::default();
    let mut inbox = RecordingInbox::default();
    let sdk = synced_sdk_client(&config).await;

    // Original question was from @bob:example.org.
    record_forward_with_sender_and_session(
        &store,
        "01FWD_BOB_Q",
        "!public:example.org",
        "$root_6c",
        "@bob:example.org",
        "author-id-1@sini.cortex",
    );

    Mock::given(method("PUT"))
        .and(path_regex(
            r"^/_matrix/client/v3/rooms/!public(%3A|:)example\.org/send/m\.room\.message/.*$",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "event_id": "$bot_post_6c"
        })))
        .expect(1)
        .mount(&server)
        .await;

    let reply_delivery = SvcDelivery {
        message_id: "01REPLY_6C".to_string(),
        from_name: "claude · author-id-1@sini.cortex".to_string(),
        text: "Answer to Bob that Mallory tries to get deleted".to_string(),
        envelope: "".to_string(),
        origin: Some(author_origin()),
        in_reply_to: Some("01FWD_BOB_Q".to_string()),
    };
    let out1 = handle_inbox_delivery_with_xmsg(
        &reply_delivery,
        &config,
        &sdk,
        &store,
        &xmsg,
        &mut inbox,
        1010,
    )
    .await
    .unwrap();
    assert_eq!(out1, BotOutcome::Replied);

    // Zero redact requests may reach the homeserver.
    Mock::given(method("PUT"))
        .and(path_regex(r"^/_matrix/client/v3/rooms/.*/redact/.*$"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "event_id": "$should_never_redact_6c"
        })))
        .expect(0)
        .mount(&server)
        .await;

    // @mallory:example.org (untrusted non-asker) asks in the thread for the post to be removed.
    assert!(!config.is_user_trusted("@mallory:example.org"));
    record_forward_with_sender_and_session(
        &store,
        "01FWD_MALLORY_REQ1",
        "!public:example.org",
        "$root_6c",
        "@mallory:example.org",
        "agent-2@sini.cortex",
    );
    record_forward_with_sender_and_session(
        &store,
        "01FWD_MALLORY_REQ2",
        "!public:example.org",
        "$root_6c",
        "@mallory:example.org",
        "author-id-1@sini.cortex",
    );

    // Attempt 1: `agent-2` sends `redact` with `requested_by: "01FWD_MALLORY_REQ1"` AND forged
    // agent-supplied sender fields claiming `@owner:example.org` / `@alice:example.org`.
    let agent_2_origin = json!({
        "id": "agent-2",
        "node": "sini.cortex",
        "label": "claude"
    });
    let forged_redact = SvcDelivery {
        message_id: "01REDACT_6C_1".to_string(),
        from_name: "claude · agent-2@sini.cortex".to_string(),
        text: json!({
            "redact": "01REPLY_6C",
            "requested_by": "01FWD_MALLORY_REQ1",
            "sender": "@owner:example.org",
            "sender_mxid": "@alice:example.org",
            "reason": "Mallory asked to delete Bob's answer"
        })
        .to_string(),
        envelope: "".to_string(),
        origin: Some(agent_2_origin),
        in_reply_to: Some("01FWD_MALLORY_REQ1".to_string()),
    };
    let out2 = handle_inbox_delivery_with_xmsg(
        &forged_redact,
        &config,
        &sdk,
        &store,
        &xmsg,
        &mut inbox,
        1020,
    )
    .await
    .unwrap();
    assert_eq!(out2, BotOutcome::RedactRefused);

    // Attempt 2: Even when the authoring agent (`author-id-1@sini.cortex`) relays Mallory's request via
    // `requested_by`, the bot sees that `requested_by` came from `@mallory:example.org` and refuses.
    let author_relay_mallory = SvcDelivery {
        message_id: "01REDACT_6C_2".to_string(),
        from_name: "claude · author-id-1@sini.cortex".to_string(),
        text: json!({
            "redact": "01REPLY_6C",
            "requested_by": "01FWD_MALLORY_REQ2",
            "sender": "@owner:example.org",
            "sender_mxid": "@bob:example.org"
        })
        .to_string(),
        envelope: "".to_string(),
        origin: Some(author_origin()),
        in_reply_to: Some("01FWD_MALLORY_REQ2".to_string()),
    };
    let out3 = handle_inbox_delivery_with_xmsg(
        &author_relay_mallory,
        &config,
        &sdk,
        &store,
        &xmsg,
        &mut inbox,
        1030,
    )
    .await
    .unwrap();
    assert_eq!(out3, BotOutcome::RedactRefused);

    let replies = xmsg.replies.lock().unwrap();
    assert_eq!(replies.len(), 2);
    for (_, err_body) in replies.iter() {
        let v: Value = serde_json::from_str(err_body).unwrap();
        assert!(
            v["error"]
                .as_str()
                .unwrap_or("")
                .contains("unauthorized: sender '@mallory:example.org'"),
            "refusal must explain untrusted non-asker: {err_body}"
        );
    }
}

// =============================================================================
// Oracle 6d: A `requested_by` forwarded to a different agent is refused with
// zero homeserver requests — including when the caller has the SAME id/service
// name on a DIFFERENT node (`agent-2@other.node` vs `agent-2@sini.cortex`),
// plus unknown `requested_by` refused, and `requested_by` on `edit` refused.
// Mutant 1: skip `forward_received_by_caller` check => RED.
// Mutant 2: compare only bare `id` instead of full `id@node` => RED.
// =============================================================================
#[tokio::test]
async fn oracle_6d_requested_by_forwarded_to_different_agent_or_other_node_or_edit_refused() {
    let server = MockServer::start().await;
    setup_homeserver_with_rooms(&server).await;

    let config = test_config(&server.uri(), PathBuf::from(":memory:"));
    let store = Store::new_in_memory().unwrap();
    let xmsg = RecordingXmsgClient::default();
    let mut inbox = RecordingInbox::default();
    let sdk = synced_sdk_client(&config).await;

    record_forward_with_sender_and_session(
        &store,
        "01FWD_BOB_6D",
        "!public:example.org",
        "$root_6d",
        "@bob:example.org",
        "author-id-1@sini.cortex",
    );

    Mock::given(method("PUT"))
        .and(path_regex(
            r"^/_matrix/client/v3/rooms/!public(%3A|:)example\.org/send/m\.room\.message/.*$",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "event_id": "$bot_post_6d"
        })))
        .expect(1)
        .mount(&server)
        .await;

    let reply_delivery = SvcDelivery {
        message_id: "01REPLY_6D".to_string(),
        from_name: "claude · author-id-1@sini.cortex".to_string(),
        text: "Post in 6d".to_string(),
        envelope: "".to_string(),
        origin: Some(author_origin()),
        in_reply_to: Some("01FWD_BOB_6D".to_string()),
    };
    let out1 = handle_inbox_delivery_with_xmsg(
        &reply_delivery,
        &config,
        &sdk,
        &store,
        &xmsg,
        &mut inbox,
        1010,
    )
    .await
    .unwrap();
    assert_eq!(out1, BotOutcome::Replied);

    // Zero subsequent send to !public:example.org or any redact requests may reach the homeserver.
    Mock::given(method("PUT"))
        .and(path_regex(
            r"^/_matrix/client/v3/rooms/(!public(%3A|:)example\.org/send/m\.room\.message|.*/redact)/.*$",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "event_id": "$should_never_happen_6d"
        })))
        .expect(0)
        .mount(&server)
        .await;

    // @alice:example.org (trusted) sends a forward that is delivered to `agent-2@sini.cortex`.
    record_forward_with_sender_and_session(
        &store,
        "01FWD_ALICE_TO_AGENT2",
        "!public:example.org",
        "$root_6d",
        "@alice:example.org",
        "agent-2@sini.cortex",
    );

    let agent_3_origin = json!({
        "id": "agent-3",
        "node": "sini.cortex",
        "label": "claude"
    });

    // 1. `agent-3@sini.cortex` tries to redact citing `01FWD_ALICE_TO_AGENT2` (forwarded to `agent-2@sini.cortex`).
    let wrong_agent_redact = SvcDelivery {
        message_id: "01REDACT_6D_WRONG_AGENT".to_string(),
        from_name: "claude · agent-3@sini.cortex".to_string(),
        text: json!({
            "redact": "01REPLY_6D",
            "requested_by": "01FWD_ALICE_TO_AGENT2"
        })
        .to_string(),
        envelope: "".to_string(),
        origin: Some(agent_3_origin.clone()),
        in_reply_to: None,
    };
    let out2 = handle_inbox_delivery_with_xmsg(
        &wrong_agent_redact,
        &config,
        &sdk,
        &store,
        &xmsg,
        &mut inbox,
        1020,
    )
    .await
    .unwrap();
    assert_eq!(out2, BotOutcome::RedactRefused);

    // 2. Same id/service `agent-2` on ANOTHER node (`agent-2@other.node`) tries to redact citing
    // `01FWD_ALICE_TO_AGENT2` (which was forwarded to `agent-2@sini.cortex`). Must be refused!
    let same_id_other_node_origin = json!({
        "id": "agent-2",
        "node": "other.node",
        "label": "claude"
    });
    let same_id_other_node_redact = SvcDelivery {
        message_id: "01REDACT_6D_SAME_ID_OTHER_NODE".to_string(),
        from_name: "claude · agent-2@other.node".to_string(),
        text: json!({
            "redact": "01REPLY_6D",
            "requested_by": "01FWD_ALICE_TO_AGENT2"
        })
        .to_string(),
        envelope: "".to_string(),
        origin: Some(same_id_other_node_origin),
        in_reply_to: None,
    };
    let out_other_node = handle_inbox_delivery_with_xmsg(
        &same_id_other_node_redact,
        &config,
        &sdk,
        &store,
        &xmsg,
        &mut inbox,
        1022,
    )
    .await
    .unwrap();
    assert_eq!(out_other_node, BotOutcome::RedactRefused);

    // 3. Unknown `requested_by` message ID is refused.
    let unknown_req_by = SvcDelivery {
        message_id: "01REDACT_6D_UNKNOWN_REQ_BY".to_string(),
        from_name: "claude · agent-3@sini.cortex".to_string(),
        text: json!({
            "redact": "01REPLY_6D",
            "requested_by": "01FWD_DOES_NOT_EXIST"
        })
        .to_string(),
        envelope: "".to_string(),
        origin: Some(agent_3_origin),
        in_reply_to: None,
    };
    let out3 = handle_inbox_delivery_with_xmsg(
        &unknown_req_by,
        &config,
        &sdk,
        &store,
        &xmsg,
        &mut inbox,
        1025,
    )
    .await
    .unwrap();
    assert_eq!(out3, BotOutcome::RedactRefused);

    // 4. `requested_by` on an `edit` is refused (even when sent by `agent-2@sini.cortex` who received Alice's forward,
    // or by the authoring agent itself).
    let agent_2_origin = json!({
        "id": "agent-2",
        "node": "sini.cortex",
        "label": "claude"
    });
    let edit_with_req_by = SvcDelivery {
        message_id: "01EDIT_6D_REQ_BY".to_string(),
        from_name: "claude · agent-2@sini.cortex".to_string(),
        text: json!({
            "edit": "01REPLY_6D",
            "body": "Trying to edit via requested_by",
            "requested_by": "01FWD_ALICE_TO_AGENT2"
        })
        .to_string(),
        envelope: "".to_string(),
        origin: Some(agent_2_origin),
        in_reply_to: Some("01FWD_ALICE_TO_AGENT2".to_string()),
    };
    let out4 = handle_inbox_delivery_with_xmsg(
        &edit_with_req_by,
        &config,
        &sdk,
        &store,
        &xmsg,
        &mut inbox,
        1030,
    )
    .await
    .unwrap();
    assert_eq!(out4, BotOutcome::EditRefused);

    {
        let replies = xmsg.replies.lock().unwrap();
        assert_eq!(replies.len(), 4);
        assert!(replies[0]
            .1
            .contains("was not received by the requesting agent"));
        assert!(replies[1]
            .1
            .contains("was not received by the requesting agent"));
        assert!(replies[2]
            .1
            .contains("unknown requested_by forward message id"));
        assert!(replies[3]
            .1
            .contains("edit is author-only and does not accept 'requested_by'"));
    }

    // 5. Verify `open_thread` records `id@node` in `forwarded_messages.session_id`, so a same-named
    // service on another node cannot cite the open_thread message ID in `requested_by`.
    Mock::given(method("PUT"))
        .and(path_regex(
            r"^/_matrix/client/v3/rooms/!second(%3A|:)example\.org/send/m\.room\.message/.*$",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "event_id": "$root_open_6d"
        })))
        .expect(1)
        .mount(&server)
        .await;

    let open_delivery = SvcDelivery {
        message_id: "01OPEN_THREAD_6D".to_string(),
        from_name: "svc-opener@sini.cortex".to_string(),
        text: json!({
            "open_thread": {
                "room": "!second:example.org",
                "text": "Opening thread"
            }
        })
        .to_string(),
        envelope: "".to_string(),
        origin: Some(json!({
            "service": "svc-opener",
            "node": "sini.cortex"
        })),
        in_reply_to: None,
    };
    let out_open = handle_inbox_delivery_with_xmsg(
        &open_delivery,
        &config,
        &sdk,
        &store,
        &xmsg,
        &mut inbox,
        1040,
    )
    .await
    .unwrap();
    assert_eq!(out_open, BotOutcome::OpenThreadSuccess);
    assert_eq!(
        store
            .get_forward_session("01OPEN_THREAD_6D")
            .unwrap()
            .as_deref(),
        Some("svc-opener@sini.cortex")
    );

    // Same-named service `svc-opener` on `other.node` tries to redact citing `01OPEN_THREAD_6D` -> refused.
    let other_node_svc_redact = SvcDelivery {
        message_id: "01REDACT_6D_OTHER_NODE_SVC".to_string(),
        from_name: "svc-opener@other.node".to_string(),
        text: json!({
            "redact": "01REPLY_6D",
            "requested_by": "01OPEN_THREAD_6D"
        })
        .to_string(),
        envelope: "".to_string(),
        origin: Some(json!({
            "service": "svc-opener",
            "node": "other.node"
        })),
        in_reply_to: None,
    };
    let out_other_svc = handle_inbox_delivery_with_xmsg(
        &other_node_svc_redact,
        &config,
        &sdk,
        &store,
        &xmsg,
        &mut inbox,
        1045,
    )
    .await
    .unwrap();
    assert_eq!(out_other_svc, BotOutcome::RedactRefused);
}
