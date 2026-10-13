use async_trait::async_trait;
use matrix_xmsg::bot::{handle_inbox_delivery, register_event_handlers, BotOutcome};
use matrix_xmsg::config::{Admission, Config};
use matrix_xmsg::error::AppError;
use matrix_xmsg::matrix::{build_notice_content, parse_mentions_and_render_pills, MatrixSdkClient};
use matrix_xmsg::store::Store;
use matrix_xmsg::xmsg::{SendResponse, SvcDelivery, SvcInbox, XmsgClient};
use serde_json::json;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use wiremock::matchers::{method, path, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn live_ts() -> i64 {
    chrono::Utc::now().timestamp_millis() + 5000
}

#[derive(Default)]
struct TestXmsgMock {
    sent_payloads: Mutex<Vec<(String, String, String)>>,
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

#[derive(Default)]
struct TestInboxMock {
    acked: Vec<String>,
}

#[async_trait]
impl SvcInbox for TestInboxMock {
    async fn poll(&mut self, _wait_secs: u64) -> Result<Option<SvcDelivery>, AppError> {
        Ok(None)
    }

    async fn ack(&mut self, message_id: &str) -> Result<(), AppError> {
        self.acked.push(message_id.to_string());
        Ok(())
    }
}

fn test_config(homeserver_url: &str) -> Config {
    Config {
        homeserver_url: homeserver_url.to_string(),
        bot_mxid: "@genie:example.org".to_string(),
        access_token_file: PathBuf::from("/nonexistent/token"),
        rooms: vec!["!public:example.org".to_string()],
        trusted_mxids: vec![
            "@alice:example.org".to_string(),
            "@charlie:example.org".to_string(),
            "@vborja:matrix.org".to_string(),
        ],
        owner_mxid: "@owner:example.org".to_string(),
        admission: Admission::Trusted,
        xmsg_socket: PathBuf::from("/run/user/1000/xmsg"),
        expert_ref: "claude".to_string(),
        history_n: 5,
        history_byte_cap: 1024,
        resync_byte_cap: 65536,
        guard_ref: "genie-guard".to_string(),
        guard_timeout_secs: 30,
        guard_retry_budget: 3,
        inbox_retry_budget: 10,
        rate_limit_count: 5,
        rate_limit_window_secs: 60,
        size_cap_bytes: 2048,
        answer_timeout_secs: 30,
        session_live_secs: 3600,
        db_path: PathBuf::from(":memory:"),
    }
}

async fn setup_versions_mock(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/_matrix/client/versions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "versions": ["v1.1", "v1.2", "v1.3", "v1.4", "v1.5", "v1.6", "v1.7", "v1.8", "v1.9", "v1.10", "v1.11"]
        })))
        .mount(server)
        .await;
}

fn mention_event(event_id: &str, sender: &str, ts: i64) -> serde_json::Value {
    json!({
        "type": "m.room.message",
        "sender": sender,
        "content": {
            "msgtype": "m.text",
            "body": "Hello @genie:example.org help me",
            "m.mentions": {
                "user_ids": ["@genie:example.org"]
            }
        },
        "event_id": event_id,
        "origin_server_ts": ts
    })
}

// -------------------------------------------------------------------------------------------------
// Oracle 1:
// A reply containing `[vic](https://matrix.to/#/@vborja:matrix.org)` produces an event whose
// `m.mentions.user_ids` is exactly `["@vborja:matrix.org"]`, and whose `formatted_body` has the pill.
// Mutant: no `m.mentions` written => RED.
// -------------------------------------------------------------------------------------------------
#[tokio::test]
async fn oracle_1_reply_with_matrix_to_link_produces_mentions_and_pill() {
    let mock_server = MockServer::start().await;
    setup_versions_mock(&mock_server).await;

    let sync_response = json!({
        "next_batch": "nb1",
        "rooms": {
            "join": {
                "!public:example.org": {
                    "timeline": {
                        "events": [mention_event("$trig", "@vborja:matrix.org", live_ts())]
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

    Mock::given(method("GET"))
        .and(path_regex(r"^/_matrix/client/v3/rooms/.*/messages$"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"start":"s","end":"e","chunk":[]})),
        )
        .mount(&mock_server)
        .await;

    Mock::given(method("PUT"))
        .and(path_regex(
            r"^/_matrix/client/v3/rooms/.*/send/m\.room\.message/.*$",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"event_id":"$r"})))
        .expect(1)
        .mount(&mock_server)
        .await;

    let config = Arc::new(test_config(&mock_server.uri()));
    let xmsg = Arc::new(TestXmsgMock::default());
    let store = Arc::new(Store::new_in_memory().unwrap());

    let client = Arc::new(
        MatrixSdkClient::new(&config.homeserver_url, &config.bot_mxid, "syt_valid_token")
            .await
            .unwrap(),
    );
    register_event_handlers(
        client.inner(),
        config.clone(),
        client.clone(),
        xmsg.clone(),
        store.clone(),
    );

    client
        .inner()
        .sync_once(matrix_sdk::config::SyncSettings::default())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Delivery containing [vic](https://matrix.to/#/@vborja:matrix.org)
    let reply_text = "[vic](https://matrix.to/#/@vborja:matrix.org) here is the solution";
    let delivery = SvcDelivery {
        in_reply_to: None,
        message_id: "01DELIVERY_M22_1".to_string(),
        envelope: format!("[xmsg] reply to message_id=01MOCKMSG000001 — message_id=01DELIVERY_M22_1; reply with the xmsg reply tool\n\n{reply_text}"),
        text: reply_text.to_string(),
        from_name: "claude".to_string(),
        origin: None,
    };
    let mut inbox = TestInboxMock::default();
    let outcome = handle_inbox_delivery(
        &delivery,
        &config,
        client.as_ref(),
        &store,
        &mut inbox,
        live_ts() / 1000 + 10,
    )
    .await
    .unwrap();

    assert_eq!(outcome, BotOutcome::Replied);

    let reqs = mock_server.received_requests().await.unwrap();
    let puts: Vec<serde_json::Value> = reqs
        .iter()
        .filter(|r| r.method.as_str() == "PUT" && r.url.path().contains("/m.room.message/"))
        .map(|r| serde_json::from_slice(&r.body).unwrap())
        .collect();

    assert_eq!(
        puts.len(),
        1,
        "Must have exactly 1 PUT request for the reply"
    );
    let put_body = &puts[0];

    // Oracle 1 assertion 1: m.mentions.user_ids is exactly ["@vborja:matrix.org"]
    assert_eq!(
        put_body["m.mentions"]["user_ids"],
        json!(["@vborja:matrix.org"]),
        "m.mentions.user_ids must be exactly ['@vborja:matrix.org']"
    );

    // Oracle 1 assertion 2: formatted_body has the pill (<a href="https://matrix.to/#/@vborja:matrix.org">vic</a>)
    let formatted_body = put_body["formatted_body"]
        .as_str()
        .expect("formatted_body must be present");
    assert!(
        formatted_body.contains("<a href=\"https://matrix.to/#/@vborja:matrix.org\">vic</a>"),
        "formatted_body must render the pill: {formatted_body}"
    );

    // Companion assertion 3: Deduplication and bare URL pill rendering
    let (bare_processed, bare_uids) = parse_mentions_and_render_pills(
        "See https://matrix.to/#/@vborja:matrix.org and [vic](https://matrix.to/#/@vborja:matrix.org)"
    );
    assert_eq!(
        bare_uids.len(),
        1,
        "Deduplication must yield exactly 1 user_id"
    );
    assert_eq!(bare_uids[0].as_str(), "@vborja:matrix.org");
    let bare_notice = build_notice_content(&bare_processed, None);
    let bare_val = serde_json::to_value(&bare_notice).unwrap();
    assert_eq!(
        bare_val["m.mentions"]["user_ids"],
        json!(["@vborja:matrix.org"])
    );
    assert!(bare_val["formatted_body"]
        .as_str()
        .unwrap()
        .contains("<a href=\"https://matrix.to/#/@vborja:matrix.org\">@vborja:matrix.org</a>"));
}

// -------------------------------------------------------------------------------------------------
// Oracle 2:
// A plain reply mentions only the asker (@alice:example.org).
// Any other thread participant (e.g. a second public sender @bob:example.org in the thread) is never added.
// Mutant: add every thread participant => RED.
// -------------------------------------------------------------------------------------------------
#[tokio::test]
async fn oracle_2_plain_reply_mentions_only_asker_and_no_other_thread_participants() {
    let mock_server = MockServer::start().await;
    setup_versions_mock(&mock_server).await;

    let sync_response = json!({
        "next_batch": "nb1",
        "rooms": {
            "join": {
                "!public:example.org": {
                    "timeline": {
                        "events": [
                            mention_event("$trig2", "@alice:example.org", live_ts()),
                            json!({
                                "type": "m.room.message",
                                "sender": "@bob:example.org",
                                "content": {
                                    "msgtype": "m.text",
                                    "body": "I have this issue too",
                                    "m.relates_to": {
                                        "rel_type": "m.thread",
                                        "event_id": "$trig2"
                                    }
                                },
                                "event_id": "$bob_msg",
                                "origin_server_ts": live_ts() + 10
                            })
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

    Mock::given(method("GET"))
        .and(path_regex(r"^/_matrix/client/v3/rooms/.*/messages$"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"start":"s","end":"e","chunk":[]})),
        )
        .mount(&mock_server)
        .await;

    Mock::given(method("PUT"))
        .and(path_regex(
            r"^/_matrix/client/v3/rooms/.*/send/m\.room\.message/.*$",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"event_id":"$r2"})))
        .expect(1)
        .mount(&mock_server)
        .await;

    let config = Arc::new(test_config(&mock_server.uri()));
    let xmsg = Arc::new(TestXmsgMock::default());
    let store = Arc::new(Store::new_in_memory().unwrap());

    let client = Arc::new(
        MatrixSdkClient::new(&config.homeserver_url, &config.bot_mxid, "syt_valid_token")
            .await
            .unwrap(),
    );
    register_event_handlers(
        client.inner(),
        config.clone(),
        client.clone(),
        xmsg.clone(),
        store.clone(),
    );

    client
        .inner()
        .sync_once(matrix_sdk::config::SyncSettings::default())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Delivery with NO matrix.to link
    let reply_text = "Here is an answer without mentioning anyone.";
    let delivery = SvcDelivery {
        in_reply_to: None,
        message_id: "01DELIVERY_M22_2".to_string(),
        envelope: format!("[xmsg] reply to message_id=01MOCKMSG000001 — message_id=01DELIVERY_M22_2; reply with the xmsg reply tool\n\n{reply_text}"),
        text: reply_text.to_string(),
        from_name: "claude".to_string(),
        origin: None,
    };
    let mut inbox = TestInboxMock::default();
    let outcome = handle_inbox_delivery(
        &delivery,
        &config,
        client.as_ref(),
        &store,
        &mut inbox,
        live_ts() / 1000 + 10,
    )
    .await
    .unwrap();

    assert_eq!(outcome, BotOutcome::Replied);

    let reqs = mock_server.received_requests().await.unwrap();
    let puts: Vec<serde_json::Value> = reqs
        .iter()
        .filter(|r| r.method.as_str() == "PUT" && r.url.path().contains("/m.room.message/"))
        .map(|r| serde_json::from_slice(&r.body).unwrap())
        .collect();

    assert_eq!(
        puts.len(),
        1,
        "Must have exactly 1 PUT request for the reply"
    );
    let put_body = &puts[0];

    // Oracle 2 assertion: a plain reply mentions only the asker (@alice:example.org)
    assert_eq!(
        put_body["m.mentions"]["user_ids"],
        json!(["@alice:example.org"]),
        "A plain reply must mention only the asker (@alice:example.org)"
    );
    let uids = put_body["m.mentions"]["user_ids"].as_array().unwrap();
    assert!(
        !uids.contains(&json!("@bob:example.org")),
        "Other thread participant @bob:example.org must NOT be added"
    );

    // Companion cell: where the reply links the asker too, user_ids holds the asker once (deduplicated)
    let reply_with_asker_link =
        "Hello [alice](https://matrix.to/#/@alice:example.org), here is the answer";
    let notice_with_asker = build_notice_content(reply_with_asker_link, Some("@alice:example.org"));
    let notice_val = serde_json::to_value(&notice_with_asker).unwrap();
    assert_eq!(
        notice_val["m.mentions"]["user_ids"],
        json!(["@alice:example.org"]),
        "When reply links the asker too, user_ids must hold the asker exactly once"
    );
}

// -------------------------------------------------------------------------------------------------
// Oracle 3:
// `@room` in the text sets no room mention.
// Mutant: `room: true` on any text containing `@room` => RED.
// -------------------------------------------------------------------------------------------------
#[tokio::test]
async fn oracle_3_at_room_in_text_sets_no_room_mention() {
    let mock_server = MockServer::start().await;
    setup_versions_mock(&mock_server).await;

    let sync_response = json!({
        "next_batch": "nb1",
        "rooms": {
            "join": {
                "!public:example.org": {
                    "timeline": {
                        "events": [mention_event("$trig3", "@alice:example.org", live_ts())]
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

    Mock::given(method("GET"))
        .and(path_regex(r"^/_matrix/client/v3/rooms/.*/messages$"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"start":"s","end":"e","chunk":[]})),
        )
        .mount(&mock_server)
        .await;

    Mock::given(method("PUT"))
        .and(path_regex(
            r"^/_matrix/client/v3/rooms/.*/send/m\.room\.message/.*$",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"event_id":"$r3"})))
        .expect(1)
        .mount(&mock_server)
        .await;

    let config = Arc::new(test_config(&mock_server.uri()));
    let xmsg = Arc::new(TestXmsgMock::default());
    let store = Arc::new(Store::new_in_memory().unwrap());

    let client = Arc::new(
        MatrixSdkClient::new(&config.homeserver_url, &config.bot_mxid, "syt_valid_token")
            .await
            .unwrap(),
    );
    register_event_handlers(
        client.inner(),
        config.clone(),
        client.clone(),
        xmsg.clone(),
        store.clone(),
    );

    client
        .inner()
        .sync_once(matrix_sdk::config::SyncSettings::default())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Delivery containing `@room`
    let reply_text = "@room Please be advised that service maintenance is scheduled tonight.";
    let delivery = SvcDelivery {
        in_reply_to: None,
        message_id: "01DELIVERY_M22_3".to_string(),
        envelope: format!("[xmsg] reply to message_id=01MOCKMSG000001 — message_id=01DELIVERY_M22_3; reply with the xmsg reply tool\n\n{reply_text}"),
        text: reply_text.to_string(),
        from_name: "claude".to_string(),
        origin: None,
    };
    let mut inbox = TestInboxMock::default();
    let outcome = handle_inbox_delivery(
        &delivery,
        &config,
        client.as_ref(),
        &store,
        &mut inbox,
        live_ts() / 1000 + 10,
    )
    .await
    .unwrap();

    assert_eq!(outcome, BotOutcome::Replied);

    let reqs = mock_server.received_requests().await.unwrap();
    let puts: Vec<serde_json::Value> = reqs
        .iter()
        .filter(|r| r.method.as_str() == "PUT" && r.url.path().contains("/m.room.message/"))
        .map(|r| serde_json::from_slice(&r.body).unwrap())
        .collect();

    assert_eq!(
        puts.len(),
        1,
        "Must have exactly 1 PUT request for the reply"
    );
    let put_body = &puts[0];

    // Oracle 3 assertion: room: true is NOT set (refused by default)
    assert_ne!(
        put_body["m.mentions"]["room"], true,
        "@room in text must NOT set room: true"
    );
    assert!(
        put_body["m.mentions"]["room"].is_null() || put_body["m.mentions"]["room"] == false,
        "m.mentions.room must be omitted or false"
    );
}
