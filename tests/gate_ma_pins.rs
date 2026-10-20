//! Pins for the guard path and the registration frame (MA verify v0, P1, P2, P4), each against a
//! mock `register.sock` or `agent.sock`. The pending map is process-wide, so every test here
//! keys its guard requests by a message ID no other test issues.

use matrix_xmsg::xmsg::{
    register_svc, route_guard_reply, GuardClient, GuardContent, GuardEnvelope, SvcInbox,
    UnixGuardClient,
};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::net::UnixListener;

fn envelope() -> GuardEnvelope {
    GuardEnvelope {
        source: "message".to_string(),
        history: vec![],
        content: GuardContent {
            sender: Some("@alice:example.org".to_string()),
            tier: Some("public".to_string()),
            text: "Hello".to_string(),
        },
    }
}

/// A mock socket that answers the n-th connection's first line with `answer(n)`, or holds the
/// connection open without answering when `answer` gives `None`. Returns the socket path and
/// the connection count.
fn mock_socket(
    dir: &tempfile::TempDir,
    name: &str,
    answer: fn(usize) -> Option<Value>,
) -> (PathBuf, Arc<AtomicUsize>) {
    let sock = dir.path().join(name);
    let listener = UnixListener::bind(&sock).unwrap();
    let counter = Arc::new(AtomicUsize::new(0));
    let c = counter.clone();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let n = c.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                let (read_half, mut write_half) = stream.into_split();
                let mut lines = tokio::io::BufReader::new(read_half).lines();
                if let Ok(Some(_)) = lines.next_line().await {
                    match answer(n) {
                        Some(resp) => {
                            let _ = write_half.write_all(format!("{resp}\n").as_bytes()).await;
                            let _ = lines.next_line().await;
                        }
                        None => std::future::pending::<()>().await,
                    }
                }
            });
        }
    });
    (sock, counter)
}

fn guard_ok(id: String) -> Option<Value> {
    Some(json!({ "status": "ok", "message_id": id,
                 "address": "genie-guard@sini.test", "outcome": "delivered" }))
}

// G3: `register_svc` refuses an `ok` frame that names no node; the same frame with a node is
// accepted and carries it. Mutant: default the node => RED.
#[tokio::test]
async fn register_refuses_an_ok_frame_without_node() {
    let dir = tempfile::tempdir().unwrap();
    let (no_node, _) = mock_socket(&dir, "a.sock", |_| {
        Some(json!({ "status": "ok", "address": "matrix-xmsg@sini.test" }))
    });
    let err = match register_svc(&no_node, "matrix-xmsg").await {
        Ok(_) => panic!("an ok frame without node must be refused"),
        Err(e) => e,
    };
    assert!(err.to_string().contains("names no node"), "{err}");

    let (with_node, _) = mock_socket(&dir, "b.sock", |_| {
        Some(json!({ "status": "ok", "address": "matrix-xmsg@sini.test", "node": "sini.test" }))
    });
    let inbox = register_svc(&with_node, "matrix-xmsg").await.unwrap();
    assert_eq!(inbox.node().as_deref(), Some("sini.test"));
}

// G5: with several guard checks in flight, a verdict reaches only the check whose request it
// answers, and a reply to no guard request is not taken as a verdict. Mutant: hand a verdict to
// any waiting check => RED.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_verdict_reaches_only_the_check_it_answers() {
    const N: usize = 8;
    let dir = tempfile::tempdir().unwrap();
    let (sock, counter) = mock_socket(&dir, "agent.sock", |n| guard_ok(format!("01MAPINS5{n:04}")));

    let checks: Vec<_> = (0..N)
        .map(|_| {
            let client = UnixGuardClient::new(sock.clone(), 20);
            tokio::spawn(async move { client.check_guard("genie-guard", &envelope()).await })
        })
        .collect();

    // A check connects only while it holds the map, so once all N have connected, at least
    // N - 1 entries are in, and routing waits on the last one's insert.
    while counter.load(Ordering::SeqCst) < N {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        !route_guard_reply("01MAPINS5STRAY", r#"{"verdict":"allow"}"#).await,
        "a reply to no guard request was taken as a verdict"
    );

    // Connection n is answered with ID n, but the spawn order need not be the connect order, so
    // each verdict names its own ID and each check reports which one it got.
    for n in 0..N {
        let id = format!("01MAPINS5{n:04}");
        let verdict = json!({
            "verdict": "reject",
            "class": "directs_model",
            "reason": id,
            "cleaned_text": ""
        })
        .to_string();
        while !route_guard_reply(&id, &verdict).await {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    let mut reasons = Vec::new();
    for c in checks {
        reasons.push(c.await.unwrap().expect("verdict").reason);
    }
    reasons.sort();
    let expected: Vec<String> = (0..N).map(|n| format!("01MAPINS5{n:04}")).collect();
    assert_eq!(reasons, expected);
}

// P1: a check that times out evicts its entry, so a late verdict is no guard verdict (the
// inbox loop then ignores it). Mutant: keep the entry => RED.
#[tokio::test]
async fn a_timed_out_check_evicts_its_entry() {
    let dir = tempfile::tempdir().unwrap();
    let (sock, _) = mock_socket(&dir, "agent.sock", |_| {
        guard_ok("01MAPINS1LATE".to_string())
    });
    let client = UnixGuardClient::new(sock, 1);
    let err = client
        .check_guard("genie-guard", &envelope())
        .await
        .expect_err("no verdict must time out");
    assert!(err.to_string().contains("timed out"), "{err}");
    assert!(
        !route_guard_reply("01MAPINS1LATE", r#"{"verdict":"allow"}"#).await,
        "the timed-out check's entry is still pending"
    );
}

// P4: a guard send that agent.sock never answers fails at the send bound. The map is held only
// for the send, so the bound is also what one check can hold up the inbox loop. The outer bound
// is generous because other tests in this binary share the map. Mutant: an unbounded send => RED.
#[tokio::test]
async fn a_stalled_guard_send_is_bounded() {
    let dir = tempfile::tempdir().unwrap();
    let (sock, _) = mock_socket(&dir, "agent.sock", |_| None);
    let mut client = UnixGuardClient::new(sock, 20);
    client.send_timeout = Duration::from_millis(300);
    let res = tokio::time::timeout(
        Duration::from_secs(30),
        client.check_guard("genie-guard", &envelope()),
    )
    .await
    .expect("a stalled guard send must fail within its bound, not hang");
    let err = res.expect_err("a stalled send must fail");
    assert!(err.to_string().contains("Guard send"), "{err}");
}
