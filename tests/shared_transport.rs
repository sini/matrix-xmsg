use matrix_xmsg::bot::delivery_cursor_session;
use matrix_xmsg::store::Store;
use matrix_xmsg::xmsg::{register_svc, ReplyOutcome, SendResponse, SvcInbox, XmsgClient};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use xmsg::client::{current_uid, ClientOptions};

fn private_dir() -> tempfile::TempDir {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    dir
}

fn options(dir: &std::path::Path) -> ClientOptions {
    ClientOptions {
        register_socket: dir.join("register.sock"),
        agent_socket: dir.join("agent.sock"),
        max_frame_bytes: 1024 * 1024,
        operation_timeout: Duration::from_secs(2),
        service: "matrix-xmsg".into(),
        expected_node: None,
        daemon_uid: current_uid(),
    }
}

async fn exchange(stream: &mut BufReader<UnixStream>, response: Value) -> Value {
    let mut line = String::new();
    stream.read_line(&mut line).await.unwrap();
    let frame = serde_json::from_str(&line).unwrap();
    stream
        .get_mut()
        .write_all(format!("{response}\n").as_bytes())
        .await
        .unwrap();
    frame
}

fn receipt_view(receipt: &SendResponse) -> Value {
    json!({"message_id": receipt.message_id, "session_id": receipt.session_id})
}

// Dropping an idempotency key, from_name, reply ID or ACK/reconnect ordering must
// change the trace recorded before the legacy transport was removed.
#[tokio::test]
async fn accepted_transport_and_cursor_trace_matches_legacy_golden() {
    let mut golden: Value =
        serde_json::from_str(include_str!("fixtures/xmsg-transport-d835c725.json")).unwrap();
    golden.as_object_mut().unwrap().remove("recording");
    let dir = private_dir();
    let register = UnixListener::bind(dir.path().join("register.sock")).unwrap();
    let agent = UnixListener::bind(dir.path().join("agent.sock")).unwrap();
    let service_task = tokio::spawn(async move {
        let mut stream = BufReader::new(register.accept().await.unwrap().0);
        let mut frames = vec![
            exchange(
                &mut stream,
                json!({
                    "status":"ok", "node":"fixture", "address":"matrix-xmsg@fixture"
                }),
            )
            .await,
        ];
        for (index, origin) in [
            json!({"node":"fixture","id":"instance"}),
            json!({"node":"fixture","service":"expert"}),
            json!({"node":"fixture","label":"display-only"}),
        ]
        .into_iter()
        .enumerate()
        {
            frames.push(
                exchange(
                    &mut stream,
                    json!({
                        "action":"deliver", "messageId":format!("m{index}"),
                        "fromName":"display · anonymous@fixture", "text":"fixture body",
                        "envelope":"fixture envelope", "origin":origin, "inReplyTo":"parent"
                    }),
                )
                .await,
            );
            frames.push(exchange(&mut stream, json!({"status":"ok"})).await);
        }
        frames
    });
    let agent_task = tokio::spawn(async move {
        let mut frames = vec![];
        for response in [
            json!({"status":"ok","message_id":"sent","address":"expert@fixture","outcome":"queued"}),
            json!({"status":"ok","message_id":"sent","address":"expert@fixture","outcome":"queued"}),
            json!({"status":"ok","reply":{"seq":1,"messageId":"sent","createdAt":1,
                "replierSessionId":"matrix-xmsg","text":"reply","pushOutcome":"queued","pushedMessageId":"reply"}}),
        ] {
            let mut frame = exchange(
                &mut BufReader::new(agent.accept().await.unwrap().0),
                response,
            )
            .await;
            assert_eq!(frame["schema_version"], 2);
            frame.as_object_mut().unwrap().remove("schema_version");
            frames.push(frame);
        }
        frames
    });
    let mut inbox = register_svc(options(dir.path())).await.unwrap();
    let client = inbox.client();
    let sent = client
        .send_message_keyed("expert@fixture", "display-not-identity", "send", "key")
        .await
        .unwrap();
    let repeated = client
        .send_message_keyed("expert@fixture", "display-not-identity", "send", "key")
        .await
        .unwrap();
    let reply = match client
        .reply_message_keyed("sent", "reply", "reply-key")
        .await
        .unwrap()
    {
        ReplyOutcome::Pushed { response, record } => {
            assert_eq!(record.push_outcome.as_deref(), Some("queued"));
            response
        }
        ReplyOutcome::Stored { .. } => panic!("fixture includes a pushed reply"),
    };
    assert_eq!(sent.outcome.as_deref(), Some("queued"));
    let store = Store::new(&dir.path().join("cursor.db")).unwrap();
    let mut deliveries = vec![];
    for index in 0..3 {
        let delivery = inbox.poll(0).await.unwrap().unwrap();
        let binding = delivery_cursor_session(&delivery, sent.session_id.as_deref());
        store
            .set_thread_cursor("thread", "event", 1, binding.as_deref())
            .unwrap();
        let stored = store.get_thread_cursor("thread").unwrap().unwrap();
        deliveries.push(json!({"delivery":delivery,"cursor_session":stored.session_id}));
        inbox.ack(&format!("m{index}")).await.unwrap();
    }
    let trace = json!({
        "registration_node":inbox.node(), "deliveries":deliveries,
        "service_frames":service_task.await.unwrap(), "agent_frames":agent_task.await.unwrap(),
        "send":receipt_view(&sent), "repeat":receipt_view(&repeated), "reply":receipt_view(&reply),
    });
    assert_eq!(trace, golden);
}

// Substituting the original request ID must never create a pushed reply identity.
#[tokio::test]
async fn stored_reply_preserves_its_record_without_inventing_a_pushed_id() {
    for pushed in [None, Some("stored-wire-id")] {
        let dir = private_dir();
        let register = UnixListener::bind(dir.path().join("register.sock")).unwrap();
        let agent = UnixListener::bind(dir.path().join("agent.sock")).unwrap();
        let service_task = tokio::spawn(async move {
            let mut stream = BufReader::new(register.accept().await.unwrap().0);
            exchange(
                &mut stream,
                json!({"status":"ok","node":"fixture","address":"matrix-xmsg@fixture"}),
            )
            .await;
            std::future::pending::<()>().await;
        });
        let agent_task = tokio::spawn(async move {
            let mut response = json!({"status":"ok","reply":{"seq":7,"messageId":"original-request","createdAt":1,
                "replierSessionId":"matrix-xmsg","text":"answer","pushOutcome":"stored"}});
            if let Some(id) = pushed {
                response["reply"]["pushedMessageId"] = id.into();
            }
            exchange(
                &mut BufReader::new(agent.accept().await.unwrap().0),
                response,
            )
            .await
        });
        let inbox = register_svc(options(dir.path())).await.unwrap();
        match inbox
            .client()
            .reply_message("original-request", "answer")
            .await
            .unwrap()
        {
            ReplyOutcome::Stored { record } => {
                assert_eq!(record.message_id, "original-request");
                assert_eq!(record.seq, 7);
                assert_eq!(record.pushed_message_id.as_deref(), pushed);
                assert_eq!(record.push_outcome.as_deref(), Some("stored"));
            }
            ReplyOutcome::Pushed { .. } => panic!("stored receipt must retain its outcome"),
        }
        assert_eq!(agent_task.await.unwrap()["message_id"], "original-request");
        service_task.abort();
        let _ = service_task.await;
    }
}

#[tokio::test]
async fn outbound_guard_send_is_not_blocked_by_the_inbox_poll() {
    use matrix_xmsg::xmsg::{
        route_guard_reply, GuardClient, GuardContent, GuardEnvelope, UnixGuardClient,
    };
    let dir = private_dir();
    let register = UnixListener::bind(dir.path().join("register.sock")).unwrap();
    let agent = UnixListener::bind(dir.path().join("agent.sock")).unwrap();
    let (poll_started, wait_for_poll) = tokio::sync::oneshot::channel();
    let (agent_sent, wait_for_agent) = tokio::sync::oneshot::channel();
    let service_task =
        tokio::spawn(async move {
            let mut stream = BufReader::new(register.accept().await.unwrap().0);
            exchange(
                &mut stream,
                json!({"status":"ok","node":"fixture","address":"matrix-xmsg@fixture"}),
            )
            .await;
            let mut line = String::new();
            stream.read_line(&mut line).await.unwrap();
            assert_eq!(
                serde_json::from_str::<Value>(&line).unwrap()["action"],
                "poll"
            );
            poll_started.send(()).unwrap();
            wait_for_agent.await.unwrap();
            stream.get_mut().write_all(format!("{}\n", json!({
            "action":"deliver","messageId":"verdict","fromName":"guard@fixture",
            "text":"{\"verdict\":\"allow\",\"reason\":\"fixture\"}","envelope":"",
            "inReplyTo":"guard-request","origin":{"service":"guard","node":"fixture"}
        })).as_bytes()).await.unwrap();
            std::future::pending::<()>().await;
        });
    let agent_task = tokio::spawn(async move {
        let frame = exchange(&mut BufReader::new(agent.accept().await.unwrap().0), json!({
            "status":"ok","message_id":"guard-request","address":"guard@fixture","outcome":"queued"
        })).await;
        assert_eq!(frame["action"], "send");
        assert_eq!(frame["to"], "guard@fixture");
        assert_eq!(frame["schema_version"], 2);
        agent_sent.send(()).unwrap();
    });
    let mut inbox = register_svc(options(dir.path())).await.unwrap();
    let client = inbox.client();
    let poll = tokio::spawn(async move {
        let delivery = inbox.poll(1).await.unwrap().unwrap();
        assert!(route_guard_reply("guard-request", &delivery, "guard@fixture").await);
        inbox
    });
    wait_for_poll.await.unwrap();
    let guard = UnixGuardClient::new(client.as_ref(), 2);
    let verdict = tokio::time::timeout(
        Duration::from_secs(3),
        guard.check_guard(
            "guard@fixture",
            &GuardEnvelope {
                source: "fixture".into(),
                history: vec![],
                content: GuardContent {
                    sender: None,
                    tier: None,
                    text: "fixture request".into(),
                },
            },
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(verdict.verdict, "allow");
    drop(poll.await.unwrap());
    agent_task.await.unwrap();
    service_task.abort();
}

#[tokio::test]
async fn invalid_origin_discards_the_registration_and_outbound_handle() {
    use matrix_xmsg::xmsg::{create_xmsg_client, ReconnectingSvcInbox};
    let dir = private_dir();
    let register = UnixListener::bind(dir.path().join("register.sock")).unwrap();
    let server = tokio::spawn(async move {
        let mut stream = BufReader::new(register.accept().await.unwrap().0);
        exchange(
            &mut stream,
            json!({"status":"ok","node":"fixture","address":"matrix-xmsg@fixture"}),
        )
        .await;
        exchange(&mut stream, json!({"action":"deliver","messageId":"bad", "text":"private fixture body", "envelope":"", "fromName":"expert"})).await;
    });
    let mut inbox = ReconnectingSvcInbox::new(options(dir.path()));
    let client = create_xmsg_client(&inbox);
    assert!(inbox.poll(0).await.unwrap().is_none());
    assert!(!inbox.is_connected());
    assert_eq!(inbox.node(), None);
    let err = client
        .send_message("expert", "display", "body")
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        matrix_xmsg::error::AppError::XmsgTransport(xmsg::client::ClientError::Disconnected)
    ));
    server.await.unwrap();
}

#[tokio::test]
async fn reconnect_rebinds_the_existing_outbound_facade() {
    use matrix_xmsg::xmsg::{create_xmsg_client, ReconnectingSvcInbox};
    let dir = private_dir();
    let register = UnixListener::bind(dir.path().join("register.sock")).unwrap();
    let agent = UnixListener::bind(dir.path().join("agent.sock")).unwrap();
    let service_task = tokio::spawn(async move {
        for node in ["first", "second"] {
            let mut stream = BufReader::new(register.accept().await.unwrap().0);
            exchange(
                &mut stream,
                json!({"status":"ok","node":node,"address":format!("matrix-xmsg@{node}")}),
            )
            .await;
            exchange(&mut stream, json!({"action":"timeout"})).await;
            if node == "second" {
                std::future::pending::<()>().await;
            }
        }
    });
    let agent_task = tokio::spawn(async move {
        exchange(&mut BufReader::new(agent.accept().await.unwrap().0), json!({
            "status":"ok","message_id":"after-reconnect","address":"expert@second","outcome":"queued"
        })).await
    });
    let mut inbox = ReconnectingSvcInbox::new(options(dir.path()));
    let client = create_xmsg_client(&inbox);
    assert!(inbox.poll(0).await.unwrap().is_none());
    assert_eq!(inbox.node().as_deref(), Some("first"));
    assert!(inbox.poll(0).await.unwrap().is_none());
    assert!(!inbox.is_connected());
    assert!(inbox.poll(0).await.unwrap().is_none());
    assert_eq!(inbox.node().as_deref(), Some("second"));
    let receipt = client
        .send_message("expert@second", "display", "after reconnect")
        .await
        .unwrap();
    assert_eq!(receipt.message_id, "after-reconnect");
    assert_eq!(agent_task.await.unwrap()["to"], "expert@second");
    service_task.abort();
    let _ = service_task.await;
}
