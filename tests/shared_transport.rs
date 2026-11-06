use matrix_xmsg::bot::delivery_cursor_session;
use matrix_xmsg::store::Store;
use matrix_xmsg::xmsg::{register_svc, ReplyOutcome, SendResponse, SvcInbox, XmsgClient};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use xmsg::client::{current_uid, ClientOptions};

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
    let dir = tempfile::tempdir().unwrap();
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
        let binding = delivery_cursor_session(&delivery, &sent);
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
    let dir = tempfile::tempdir().unwrap();
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
        exchange(
            &mut BufReader::new(agent.accept().await.unwrap().0),
            json!({
                "status":"ok","reply":{"seq":7,"messageId":"original-request","createdAt":1,
                    "replierSessionId":"matrix-xmsg","text":"answer","pushOutcome":"stored"}
            }),
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
            assert_eq!(record.pushed_message_id, None);
            assert_eq!(record.push_outcome.as_deref(), Some("stored"));
        }
        ReplyOutcome::Pushed { .. } => panic!("stored reply has no pushed identity"),
    }
    assert_eq!(agent_task.await.unwrap()["message_id"], "original-request");
    service_task.abort();
}
