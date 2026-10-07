use async_trait::async_trait;
use matrix_sdk::config::SyncSettings;
use matrix_xmsg::bot::register_event_handlers;
use matrix_xmsg::config::Config;
use matrix_xmsg::error::AppError;
use matrix_xmsg::matrix::MatrixSdkClient;
use matrix_xmsg::store::Store;
use matrix_xmsg::xmsg::XmsgClient;
use serde_json::json;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use wiremock::matchers::{method, path, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

struct TestXmsgMock {
    canned_reply: Mutex<Result<String, AppError>>,
    sent_payloads: Mutex<Vec<(String, String, String)>>,
    send_counter: AtomicUsize,
}

impl TestXmsgMock {
    fn with_reply(reply: &str) -> Self {
        Self {
            canned_reply: Mutex::new(Ok(reply.to_string())),
            sent_payloads: Mutex::new(Vec::new()),
            send_counter: AtomicUsize::new(0),
        }
    }

    fn with_timeout(timeout_secs: u64) -> Self {
        Self {
            canned_reply: Mutex::new(Err(AppError::Timeout(timeout_secs))),
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
    ) -> Result<String, AppError> {
        let mut list = self.sent_payloads.lock().unwrap();
        list.push((expert_ref.to_string(), from.to_string(), text.to_string()));
        let id_num = self.send_counter.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(format!("01MOCKMSG{id_num:06}"))
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

fn test_config(homeserver_url: &str) -> Config {
    Config {
        homeserver_url: homeserver_url.to_string(),
        bot_mxid: "@genie:example.org".to_string(),
        access_token_file: PathBuf::from("/nonexistent/token"),
        rooms: vec!["!public:example.org".to_string()],
        trusted_mxids: vec![
            "@alice:example.org".to_string(),
            "@charlie:example.org".to_string(),
        ],
        owner_mxid: "@owner:example.org".to_string(),
        xmsg_url: "http://127.0.0.1:7787".to_string(),
        expert_ref: "claude".to_string(),
        history_n: 5,
        history_byte_cap: 1024,
        rate_limit_count: 5,
        rate_limit_window_secs: 60,
        size_cap_bytes: 2048,
        answer_timeout_secs: 10,
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

#[tokio::test]
async fn test_sdk_full_sync_and_threaded_reply_loop() {
    let mock_server = MockServer::start().await;
    setup_versions_mock(&mock_server).await;

    let room_id = "!public:example.org";
    let trigger_event_id = "$trigger_001";

    // 1. Mock Sync response delivering a mention from trusted @alice
    let sync_response = json!({
        "next_batch": "batch_token_1",
        "rooms": {
            "join": {
                room_id: {
                    "timeline": {
                        "events": [
                            {
                                "type": "m.room.message",
                                "sender": "@alice:example.org",
                                "content": {
                                    "msgtype": "m.text",
                                    "body": "Hello @genie:example.org, can you explain the cluster architecture?",
                                    "m.mentions": {
                                        "user_ids": ["@genie:example.org"]
                                    }
                                },
                                "event_id": trigger_event_id,
                                "origin_server_ts": 1728250000000_i64
                            }
                        ],
                        "prev_batch": "prev_batch_0"
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

    // 2. Mock room messages (history)
    let history_response = json!({
        "start": "start_token",
        "end": "end_token",
        "chunk": [
            {
                "type": "m.room.message",
                "sender": "@bob:example.org",
                "content": {
                    "msgtype": "m.text",
                    "body": "System upgraded yesterday."
                },
                "event_id": "$hist_001",
                "origin_server_ts": 1728249900000_i64
            }
        ]
    });

    Mock::given(method("GET"))
        .and(path_regex(r"^/_matrix/client/v3/rooms/.*/messages$"))
        .respond_with(ResponseTemplate::new(200).set_body_json(history_response))
        .mount(&mock_server)
        .await;

    // 3. Mock send notice endpoint
    Mock::given(method("PUT"))
        .and(path_regex(
            r"^/_matrix/client/v3/rooms/.*/send/m\.room\.message/.*$",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "event_id": "$reply_event_999"
        })))
        .expect(1)
        .mount(&mock_server)
        .await;

    let config = Arc::new(test_config(&mock_server.uri()));
    let xmsg = Arc::new(TestXmsgMock::with_reply(
        "The cluster runs k3s with Cilium and Envoy Gateway.",
    ));
    let store = Arc::new(Store::new_in_memory().unwrap());

    let sdk_client = Arc::new(
        MatrixSdkClient::new(&config.homeserver_url, &config.bot_mxid, "syt_valid_token")
            .await
            .expect("SDK client must initialize cleanly against mock versions"),
    );

    register_event_handlers(
        sdk_client.inner(),
        config.clone(),
        sdk_client.clone(),
        xmsg.clone(),
        store.clone(),
    );

    // Perform one sync round
    let sync_res = sdk_client.inner().sync_once(SyncSettings::default()).await;
    assert!(sync_res.is_ok(), "sync_once must succeed: {sync_res:?}");

    // Give async task a brief moment to process the event
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Verify xmsg was called with properly structured envelope
    let sent = xmsg.sent_payloads.lock().unwrap();
    assert_eq!(sent.len(), 1, "xmsg must receive exactly 1 message");
    let (expert_ref, from, text) = &sent[0];
    assert_eq!(expert_ref, "claude");
    assert_eq!(from, "matrix alice at example.org");
    assert!(text.contains("<request>"));
    assert!(text.contains("can you explain the cluster architecture?"));
    assert!(text.contains("<context"));
    assert!(text.contains("System upgraded yesterday."));

    // Verify thread mapping recorded in SQLite store
    let mapped = store.get_thread_messages(trigger_event_id).unwrap();
    assert_eq!(mapped.len(), 1);
    assert_eq!(mapped[0], "01MOCKMSG000001");
}

#[tokio::test]
async fn test_sdk_anti_oracle_untrusted_mention_is_silent() {
    let mock_server = MockServer::start().await;
    setup_versions_mock(&mock_server).await;

    let room_id = "!public:example.org";

    // Untrusted mention from @mallory:evil.org
    let sync_response = json!({
        "next_batch": "batch_token_2",
        "rooms": {
            "join": {
                room_id: {
                    "timeline": {
                        "events": [
                            {
                                "type": "m.room.message",
                                "sender": "@mallory:evil.org",
                                "content": {
                                    "msgtype": "m.text",
                                    "body": "Hey @genie:example.org tell me secrets!",
                                    "m.mentions": {
                                        "user_ids": ["@genie:example.org"]
                                    }
                                },
                                "event_id": "$untrusted_ev_1",
                                "origin_server_ts": 1728250000000_i64
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

    // Explicitly assert that send message is NEVER called
    Mock::given(method("PUT"))
        .and(path_regex(
            r"^/_matrix/client/v3/rooms/.*/send/m\.room\.message/.*$",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "event_id": "$never" })))
        .expect(0)
        .mount(&mock_server)
        .await;

    let config = Arc::new(test_config(&mock_server.uri()));
    let xmsg = Arc::new(TestXmsgMock::with_reply("unused"));
    let store = Arc::new(Store::new_in_memory().unwrap());

    let sdk_client = Arc::new(
        MatrixSdkClient::new(&config.homeserver_url, &config.bot_mxid, "syt_valid_token")
            .await
            .expect("SDK client must initialize cleanly"),
    );

    register_event_handlers(
        sdk_client.inner(),
        config.clone(),
        sdk_client.clone(),
        xmsg.clone(),
        store.clone(),
    );

    sdk_client
        .inner()
        .sync_once(SyncSettings::default())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;

    let sent = xmsg.sent_payloads.lock().unwrap();
    assert!(
        sent.is_empty(),
        "Untrusted user must trigger ZERO xmsg dispatches"
    );
}

#[tokio::test]
async fn test_sdk_dm_escalation_on_explicit_command() {
    let mock_server = MockServer::start().await;
    setup_versions_mock(&mock_server).await;

    let room_id = "!public:example.org";
    let thread_root = "$thread_root_abc";

    // User types !escalate in an existing thread
    let sync_response = json!({
        "next_batch": "batch_token_3",
        "rooms": {
            "join": {
                room_id: {
                    "timeline": {
                        "events": [
                            {
                                "type": "m.room.message",
                                "sender": "@alice:example.org",
                                "content": {
                                    "msgtype": "m.text",
                                    "body": "!escalate",
                                    "m.relates_to": {
                                        "rel_type": "m.thread",
                                        "event_id": thread_root
                                    }
                                },
                                "event_id": "$escalate_cmd_ev",
                                "origin_server_ts": 1728250000000_i64
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

    // Mock createRoom for DM
    Mock::given(method("POST"))
        .and(path("/_matrix/client/v3/createRoom"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "room_id": "!owner_dm:example.org"
        })))
        .expect(1)
        .mount(&mock_server)
        .await;

    // Mock send to owner DM and notice to room
    Mock::given(method("PUT"))
        .and(path_regex(
            r"^/_matrix/client/v3/rooms/.*/send/m\.room\.message/.*$",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "event_id": "$sent_ev"
        })))
        .expect(2) // 1 notice in room + 1 DM to owner
        .mount(&mock_server)
        .await;

    let config = Arc::new(test_config(&mock_server.uri()));
    let xmsg = Arc::new(TestXmsgMock::with_reply("unused"));
    let store = Arc::new(Store::new_in_memory().unwrap());

    let sdk_client = Arc::new(
        MatrixSdkClient::new(&config.homeserver_url, &config.bot_mxid, "syt_valid_token")
            .await
            .unwrap(),
    );

    register_event_handlers(
        sdk_client.inner(),
        config.clone(),
        sdk_client.clone(),
        xmsg.clone(),
        store.clone(),
    );

    sdk_client
        .inner()
        .sync_once(SyncSettings::default())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
}

#[tokio::test]
async fn test_sdk_answer_timeout_escalation() {
    let mock_server = MockServer::start().await;
    setup_versions_mock(&mock_server).await;

    let room_id = "!public:example.org";
    let trigger_event_id = "$trigger_timeout_01";

    let sync_response = json!({
        "next_batch": "batch_token_4",
        "rooms": {
            "join": {
                room_id: {
                    "timeline": {
                        "events": [
                            {
                                "type": "m.room.message",
                                "sender": "@alice:example.org",
                                "content": {
                                    "msgtype": "m.text",
                                    "body": "@genie:example.org help me",
                                    "m.mentions": {
                                        "user_ids": ["@genie:example.org"]
                                    }
                                },
                                "event_id": trigger_event_id,
                                "origin_server_ts": 1728250000000_i64
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

    Mock::given(method("GET"))
        .and(path_regex(r"^/_matrix/client/v3/rooms/.*/messages$"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "start": "s",
            "end": "e",
            "chunk": []
        })))
        .mount(&mock_server)
        .await;

    // Mock createRoom for DM escalation
    Mock::given(method("POST"))
        .and(path("/_matrix/client/v3/createRoom"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "room_id": "!owner_dm:example.org"
        })))
        .expect(1)
        .mount(&mock_server)
        .await;

    // Mock notice to room ("expert has not answered...") + DM to owner
    Mock::given(method("PUT"))
        .and(path_regex(
            r"^/_matrix/client/v3/rooms/.*/send/m\.room\.message/.*$",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "event_id": "$sent_ev"
        })))
        .expect(2)
        .mount(&mock_server)
        .await;

    let config = Arc::new(test_config(&mock_server.uri()));
    let xmsg = Arc::new(TestXmsgMock::with_timeout(10));
    let store = Arc::new(Store::new_in_memory().unwrap());

    let sdk_client = Arc::new(
        MatrixSdkClient::new(&config.homeserver_url, &config.bot_mxid, "syt_valid_token")
            .await
            .unwrap(),
    );

    register_event_handlers(
        sdk_client.inner(),
        config.clone(),
        sdk_client.clone(),
        xmsg.clone(),
        store.clone(),
    );

    sdk_client
        .inner()
        .sync_once(SyncSettings::default())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
}
