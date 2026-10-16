//! MA gating oracles (xmsg addressing plan §3 MA), each against a real `xmsg serve`.
//!
//! The binary is `$XMSG_BIN`. The oracles are `#[ignore]`d, so a plain `cargo test` reports them
//! ignored rather than passed; run them with `--include-ignored`. Without `XMSG_BIN` they skip,
//! unless `MA_XMSG_REQUIRED` is set, as the `ci/` integration check sets it (with
//! `--include-ignored`): there a missing `XMSG_BIN` fails every oracle, so a run that measured
//! nothing cannot pass.
//!
//! This test process is the bot: it registers on `register.sock` as `matrix-xmsg`, so its
//! `agent.sock` sends are the service's (xmsg attributes them by pid), and Matrix is
//! `MockMatrixClient`. Every other xmsg party is a process of its own, because xmsg
//! attributes a caller by its process ancestry (A15):
//! - a service fake (`genie-guard`, a requester) is this test binary re-run as a child that
//!   registers its own name;
//! - an instance fake follows xmsg's `tests/e2e_mcp.rs`: a session file in `--sessions-dir`
//!   naming a live process and its start time, with a listener at its `messagingSocketPath`.
//!   The replying expert is a child that listens there and replies over `agent.sock`; the
//!   silent fakes of oracle 3 are a `sleep` with the listener in this process.
//!
//! The second node of oracle 2 is federated over mTLS with CA-signed certificates whose
//! single URI SAN derives each node's name, as xmsg's `tests/gate_x16_ca_peers.rs` issues them.
#![cfg(target_os = "linux")]

use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use matrix_xmsg::bot::{handle_incoming_event, handle_incoming_event_with_guard, BotOutcome};
use matrix_xmsg::config::{Admission, Config, GuardFailureMode};
use matrix_xmsg::matrix::{IncomingMatrixEvent, MatrixClient, MockMatrixClient};
use matrix_xmsg::store::Store;
use matrix_xmsg::xmsg::{
    create_xmsg_client, register_svc, ReconnectingSvcInbox, SvcInbox, UnixGuardClient,
};
use serde_json::{json, Value};
use tempfile::{tempdir, TempDir};

const REQUIRED: &str = "MA_XMSG_REQUIRED";
const CHILD_ENV: &str = "MA_CHILD";
const CHILD_TEST: &str = "ma_child_entry";
const ROOM: &str = "!support:example.org";

const ID_EXPERT: &str = "e1e1e1e1-0000-4000-8000-0000000000e1";
const ID_X: &str = "a1a1a1a1-0000-4000-8000-0000000000a1";
const ID_Y: &str = "b1b1b1b1-0000-4000-8000-0000000000b1";

/// `$XMSG_BIN`, or `None` to skip. Under the integration check a missing binary panics.
fn xmsg_bin() -> Option<PathBuf> {
    match std::env::var_os("XMSG_BIN") {
        Some(p) => Some(PathBuf::from(p)),
        None => {
            assert!(
                std::env::var_os(REQUIRED).is_none(),
                "XMSG_BIN is unset but {REQUIRED} is set: this run must exercise a real xmsg"
            );
            eprintln!("XMSG_BIN unset: skipping (set it, or run the ci/ integration check)");
            None
        }
    }
}

/// The body of every child this binary starts. In the parent under `MA_XMSG_REQUIRED`, it fails
/// unless the `#[ignore]`d oracles are included: otherwise the run would pass having run none.
#[test]
fn ma_child_entry() {
    if std::env::var_os(CHILD_ENV).is_none() && std::env::var_os(REQUIRED).is_some() {
        assert!(
            std::env::args().any(|a| a == "--include-ignored" || a == "--ignored"),
            "{REQUIRED} is set but the oracles are #[ignore]d: pass --include-ignored"
        );
    }
    child::serve_if_child();
}

fn live_starttime(pid: u32) -> String {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).expect("proc stat");
    let after = &stat[stat.rfind(')').expect("comm") + 1..];
    after
        .split_whitespace()
        .nth(19)
        .expect("starttime")
        .to_string()
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn eventually<T>(what: &str, secs: u64, mut f: impl FnMut() -> Option<T>) -> T {
    for _ in 0..secs * 20 {
        if let Some(v) = f() {
            return v;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("timed out after {secs}s waiting for {what}");
}

// -----------------------------------------------------------------------------
// A real `xmsg serve`
// -----------------------------------------------------------------------------

#[derive(Default)]
struct Spec {
    services: Vec<&'static str>,
    /// (cert PEM, key PEM, peers file JSON) for a federated node.
    fed: Option<(String, String, String)>,
    fed_listen: Option<u16>,
}

struct Node {
    tmp: TempDir,
    run: PathBuf,
    sessions: PathBuf,
    child: Child,
}

impl Node {
    fn start(bin: &Path, spec: Spec) -> Node {
        for _ in 0..5 {
            if let Some(node) = Node::try_start(bin, &spec) {
                return node;
            }
        }
        panic!("xmsg serve did not start in 5 attempts");
    }

    /// `None` when the server exits at start (a lost port race); panics if it never serves.
    fn try_start(bin: &Path, spec: &Spec) -> Option<Node> {
        let tmp = tempdir().unwrap();
        fs::set_permissions(tmp.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let run = tmp.path().join("run");
        let sessions = tmp.path().join("sessions");
        for d in [&run, &sessions] {
            fs::create_dir_all(d).unwrap();
            fs::set_permissions(d, fs::Permissions::from_mode(0o700)).unwrap();
        }
        let exe = std::env::current_exe().unwrap();
        let mut cmd = Command::new(bin);
        cmd.env("XDG_RUNTIME_DIR", &run)
            .arg("serve")
            .arg("--db-path")
            .arg(tmp.path().join("xmsg.db"))
            .arg("--sessions-dir")
            .arg(&sessions)
            .arg("--http-sock")
            .arg(run.join("http.sock"))
            .arg("--agent-sock")
            .arg(run.join("agent.sock"))
            .arg("--register-sock")
            .arg(run.join("register.sock"))
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        for name in &spec.services {
            cmd.arg("--svc-exe")
                .arg(format!("{name}={}", exe.display()));
        }
        if let Some((cert, key, peers)) = &spec.fed {
            let (c, k, p) = (
                tmp.path().join("cert.pem"),
                tmp.path().join("key.pem"),
                tmp.path().join("peers.json"),
            );
            fs::write(&c, cert).unwrap();
            fs::write(&k, key).unwrap();
            fs::write(&p, peers).unwrap();
            cmd.arg("--fed-cert")
                .arg(c)
                .arg("--fed-key")
                .arg(k)
                .arg("--peers-file")
                .arg(p);
        }
        if let Some(port) = spec.fed_listen {
            cmd.arg("--fed-listen").arg(format!("127.0.0.1:{port}"));
        }
        let mut child = cmd.spawn().expect("spawn xmsg serve");
        for _ in 0..400 {
            if let Ok(Some(_)) = child.try_wait() {
                return None;
            }
            let up = ["http.sock", "agent.sock", "register.sock"]
                .iter()
                .all(|s| run.join(s).exists())
                && spec
                    .fed_listen
                    .is_none_or(|p| std::net::TcpStream::connect(("127.0.0.1", p)).is_ok());
            if up {
                return Some(Node {
                    tmp,
                    run,
                    sessions,
                    child,
                });
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        let _ = child.kill();
        panic!("xmsg serve never served");
    }

    fn sock(&self, name: &str) -> PathBuf {
        self.run.join(name)
    }

    /// One HTTP request over `http.sock` from this process.
    fn http(&self, method: &str, path: &str, body: Option<&Value>) -> (u16, Value) {
        let mut s = UnixStream::connect(self.sock("http.sock")).expect("http.sock");
        let req = match body {
            Some(b) => {
                let b = b.to_string();
                format!(
                    "{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{b}",
                    b.len()
                )
            }
            None => {
                format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            }
        };
        s.write_all(req.as_bytes()).unwrap();
        let mut out = String::new();
        s.read_to_string(&mut out).unwrap();
        let (head, body) = out.split_once("\r\n\r\n").expect("http response");
        let status = head
            .split_whitespace()
            .nth(1)
            .and_then(|c| c.parse().ok())
            .expect("status");
        let body = if head
            .to_ascii_lowercase()
            .contains("transfer-encoding: chunked")
        {
            dechunk(body)
        } else {
            body.to_string()
        };
        (
            status,
            serde_json::from_str(&body).unwrap_or(Value::String(body)),
        )
    }

    /// The bot's config, pointed at this node's sockets.
    fn config(&self, db: &Path) -> Config {
        Config {
            homeserver_url: "https://matrix.example.org".to_string(),
            bot_mxid: "@genie:example.org".to_string(),
            access_token_file: PathBuf::from("/nonexistent/token"),
            rooms: vec![ROOM.to_string()],
            trusted_mxids: vec!["@alice:example.org".to_string()],
            owner_mxid: "@owner:example.org".to_string(),
            admission: Admission::Trusted,
            xmsg_socket: self.run.clone(),
            expert_ref: "expert".to_string(),
        expert_refs: None,
        open_thread_nodes: vec![],
            history_n: 5,
            history_byte_cap: 4096,
            rate_limit_count: 100,
            rate_limit_window_secs: 60,
            size_cap_bytes: 4096,
            answer_timeout_secs: 300,
            session_live_secs: 3600,
            resync_byte_cap: 65536,
            guard_ref: "genie-guard".to_string(),
            guard_timeout_secs: 15,
            guard_retry_budget: 3,
            screen_mentions: true,
            guard_failure_mode: GuardFailureMode::Closed,
            guard_outage_dm_window_secs: 3600,
            inbox_retry_budget: 3,
            db_path: db.to_path_buf(),
        }
    }

    /// Writes a session file presenting instance `id`, labelled `label`, for process `pid`.
    fn instance_file(&self, pid: u32, id: &str, label: &str, inbox: &Path) {
        let session = json!({
            "pid": pid,
            "sessionId": id,
            "name": label,
            "cwd": "/w",
            "status": "idle",
            "kind": "interactive",
            "startedAt": 1,
            "updatedAt": 2,
            "procStart": live_starttime(pid),
            "messagingSocketPath": inbox.display().to_string(),
        });
        fs::write(
            self.sessions.join(format!("{pid}.json")),
            session.to_string(),
        )
        .unwrap();
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn dechunk(body: &str) -> String {
    let mut out = String::new();
    let mut rest = body;
    while let Some((size, tail)) = rest.split_once("\r\n") {
        let n = usize::from_str_radix(size.trim(), 16).unwrap_or(0);
        if n == 0 {
            break;
        }
        out.push_str(&tail[..n]);
        rest = tail[n..].trim_start_matches("\r\n");
    }
    out
}

// -----------------------------------------------------------------------------
// Fakes
// -----------------------------------------------------------------------------

/// A child process of this test binary, playing a service or an instance (see `child`).
struct Fake {
    dir: TempDir,
    proc: Child,
}

impl Fake {
    fn start(spec: Value) -> Fake {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("spec.json"), spec.to_string()).unwrap();
        let proc = Command::new(std::env::current_exe().unwrap())
            .args([CHILD_TEST, "--exact", "--nocapture", "--test-threads", "1"])
            .env(CHILD_ENV, dir.path())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn child");
        Fake { dir, proc }
    }

    fn pid(&self) -> u32 {
        self.proc.id()
    }

    async fn ready(&self) {
        let ready = self.dir.path().join("ready");
        eventually("a fake to be ready", 20, || ready.exists().then_some(())).await;
    }

    /// The JSON lines the child recorded in `name`.
    fn lines(&self, name: &str) -> Vec<Value> {
        fs::read_to_string(self.dir.path().join(name))
            .unwrap_or_default()
            .lines()
            .filter_map(|l| serde_json::from_str(l).ok())
            .collect()
    }
}

impl Drop for Fake {
    fn drop(&mut self) {
        let _ = self.proc.kill();
        let _ = self.proc.wait();
    }
}

/// A live process with a listener at its messaging socket: an instance that never replies.
struct SilentInstance {
    _dir: TempDir,
    sleeper: Child,
    sock: PathBuf,
    lines: Arc<Mutex<Vec<String>>>,
}

impl SilentInstance {
    fn start() -> SilentInstance {
        let dir = tempdir().unwrap();
        let sock = dir.path().join("inbox.sock");
        let listener = tokio::net::UnixListener::bind(&sock).unwrap();
        let lines = Arc::new(Mutex::new(Vec::new()));
        let rx = lines.clone();
        tokio::spawn(async move {
            use tokio::io::AsyncReadExt;
            while let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = String::new();
                let _ = stream.read_to_string(&mut buf).await;
                if !buf.is_empty() {
                    rx.lock().unwrap().push(buf);
                }
            }
        });
        let sleeper = Command::new("sleep")
            .arg("600")
            .stdout(Stdio::null())
            .spawn()
            .expect("spawn sleep");
        SilentInstance {
            _dir: dir,
            sleeper,
            sock,
            lines,
        }
    }

    fn received(&self) -> Vec<String> {
        self.lines.lock().unwrap().clone()
    }
}

impl Drop for SilentInstance {
    fn drop(&mut self) {
        let _ = self.sleeper.kill();
        let _ = self.sleeper.wait();
    }
}

/// The bot's inbox loop, run against `node` with this config, store and Matrix mock.
struct Bot {
    config: Arc<Config>,
    store: Arc<Store>,
    matrix: Arc<MockMatrixClient>,
    shutdown: tokio::sync::broadcast::Sender<()>,
    _db: TempDir,
}

impl Bot {
    async fn start(node: &Node) -> Bot {
        let db = tempdir().unwrap();
        let config = Arc::new(node.config(&db.path().join("bot.db")));
        let store = Arc::new(Store::new(&config.db_path).unwrap());
        let matrix = Arc::new(MockMatrixClient::default());
        let (shutdown, rx) = tokio::sync::broadcast::channel(1);
        let inbox = ReconnectingSvcInbox::new(config.xmsg_register_socket(), "matrix-xmsg")
            .with_backoff(Duration::from_millis(50), Duration::from_millis(200));
        let dyn_matrix: Arc<dyn MatrixClient> = matrix.clone();
        tokio::spawn(matrix_xmsg::bot::run_inbox_loop(
            inbox,
            config.clone(),
            dyn_matrix,
            store.clone(),
            create_xmsg_client(&config),
            rx,
        ));
        let bot = Bot {
            config,
            store,
            matrix,
            shutdown,
            _db: db,
        };
        // Registered once the service is listed.
        eventually("the bot to register", 20, || {
            let (_, list) = node.http("GET", "/v1/recipients", None);
            list.to_string().contains("\"matrix-xmsg@").then_some(())
        })
        .await;
        bot
    }

    fn notices(&self) -> Vec<String> {
        self.matrix
            .sent_notices
            .lock()
            .unwrap()
            .iter()
            .map(|n| n.body.clone())
            .collect()
    }
}

impl Drop for Bot {
    fn drop(&mut self) {
        let _ = self.shutdown.send(());
    }
}

fn mention(id: &str, thread_root: Option<&str>, body: &str, ts: i64) -> IncomingMatrixEvent {
    IncomingMatrixEvent {
        room_id: ROOM.to_string(),
        event_id: id.to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        body: body.to_string(),
        formatted_body: None,
        mentions: Some(vec!["@genie:example.org".to_string()]),
        timestamp_ms: ts,
        thread_root_id: thread_root.map(String::from),
        replaces_event_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
    }
}

fn open_thread(text: &str) -> String {
    json!({ "open_thread": { "room": ROOM, "text": text } }).to_string()
}

// =============================================================================
// Oracle 1 (AM-3, D6). A verdict is not an answer: the guard service and the expert
// instance both reply into the bot's inbox. The guard's verdict is consumed by the guard
// path (the solicited line is relayed), and only the expert's reply is posted.
// Mutant: the inbox loop handles every pushed reply as an expert reply => RED.
// =============================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a real xmsg: set XMSG_BIN and pass --include-ignored"]
async fn oracle_1_a_verdict_is_not_an_answer() {
    let Some(bin) = xmsg_bin() else { return };
    let node = Node::start(
        &bin,
        Spec {
            services: vec!["matrix-xmsg", "genie-guard", "requester"],
            ..Default::default()
        },
    );
    let bot = Bot::start(&node).await;

    let verdict = json!({ "verdict": "allow", "reason": "VERDICT-REASON", "cleaned_text": "" });
    let guard = Fake::start(json!({
        "role": "service", "name": "genie-guard",
        "register_sock": node.sock("register.sock"), "agent_sock": node.sock("agent.sock"),
        "reply": verdict.to_string(),
    }));
    guard.ready().await;

    let expert_inbox = node.tmp.path().join("expert.sock");
    let expert = Fake::start(json!({
        "role": "instance", "inbox": expert_inbox, "agent_sock": node.sock("agent.sock"),
        "reply": "EXPERT-ANSWER",
    }));
    expert.ready().await;
    node.instance_file(expert.pid(), ID_EXPERT, "expert", &expert_inbox);

    // A requester opens a solicited thread; the bot answers with its root.
    let requester = Fake::start(json!({
        "role": "service", "name": "requester",
        "register_sock": node.sock("register.sock"), "agent_sock": node.sock("agent.sock"),
        "send": { "to": "matrix-xmsg", "text": open_thread("OPENING") },
    }));
    requester.ready().await;
    let root = eventually("the opening post's root", 20, || {
        requester.lines("deliveries.jsonl").iter().find_map(|d| {
            serde_json::from_str::<Value>(d["text"].as_str()?).ok()?["root"]
                .as_str()
                .map(String::from)
        })
    })
    .await;

    // A line in the solicited thread goes through the guard, then to the requester.
    let line = IncomingMatrixEvent {
        mentions: None,
        in_reply_to_event_id: Some(root.clone()),
        ..mention("$line", Some(&root), "SOLICITED-LINE", 2_000_000)
    };
    let guard_client = UnixGuardClient::new(node.sock("agent.sock"), bot.config.guard_timeout_secs);
    let xmsg = create_xmsg_client(&bot.config);
    let outcome = handle_incoming_event_with_guard(
        &line,
        &[],
        &bot.config,
        bot.matrix.as_ref(),
        xmsg.as_ref(),
        &guard_client,
        &bot.store,
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        outcome,
        BotOutcome::Forwarded,
        "the guard's verdict must reach the guard check"
    );
    eventually("the relayed line at the requester", 20, || {
        requester
            .lines("deliveries.jsonl")
            .iter()
            .any(|d| d["text"].as_str().unwrap_or("").contains("SOLICITED-LINE"))
            .then_some(())
    })
    .await;
    assert_eq!(
        guard.lines("deliveries.jsonl").len(),
        1,
        "one guard request"
    );

    // A question goes to the expert, whose answer is posted.
    let outcome = handle_incoming_event(
        &mention("$ask", None, "@genie QUESTION", 2_000_100),
        &[],
        &bot.config,
        bot.matrix.as_ref(),
        xmsg.as_ref(),
        &bot.store,
    )
    .await
    .unwrap();
    assert_eq!(outcome, BotOutcome::Forwarded);
    eventually("the expert's answer posted", 20, || {
        bot.notices()
            .iter()
            .any(|n| n.contains("EXPERT-ANSWER"))
            .then_some(())
    })
    .await;

    let notices = bot.notices();
    assert_eq!(
        notices
            .iter()
            .filter(|n| n.contains("EXPERT-ANSWER"))
            .count(),
        1,
        "{notices:?}"
    );
    assert!(
        !notices.iter().any(|n| n.contains("VERDICT-REASON")),
        "a verdict is never posted: {notices:?}"
    );
}

// =============================================================================
// Oracle 2 (A12, A8). Origin by node: `open_thread` from a local service is accepted, and
// from a second real node it is refused.
// Mutant: accept any origin => RED.
// =============================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a real xmsg: set XMSG_BIN and pass --include-ignored"]
async fn oracle_2_origin_by_node() {
    let Some(bin) = xmsg_bin() else { return };
    let ca_dir = tempdir().unwrap();
    let (ca_params, ca_key, ca_pem) = ca::generate_ca("MA test CA");
    let ca_path = ca_dir.path().join("ca.pem");
    fs::write(&ca_path, &ca_pem).unwrap();
    let uri_a = "spiffe://ma.test/host/alpha/user/bot";
    let uri_b = "spiffe://ma.test/host/beta/user/peer";
    let (cert_a, key_a) = ca::issue_cert(&ca_params, &ca_key, "bot", uri_a);
    let (cert_b, key_b) = ca::issue_cert(&ca_params, &ca_key, "peer", uri_b);
    let (port_a, port_b) = (free_port(), free_port());
    let peers = |name: &str, port: u16, uri: &str| {
        json!({ name: {
            "address": format!("127.0.0.1:{port}"), "ca": ca_path, "identities": [uri],
            "allow": ["send", "reply"],
        }})
        .to_string()
    };
    let a = Node::start(
        &bin,
        Spec {
            services: vec!["matrix-xmsg", "requester"],
            fed: Some((cert_a, key_a, peers("peer.beta", port_b, uri_b))),
            fed_listen: Some(port_a),
        },
    );
    let b = Node::start(
        &bin,
        Spec {
            fed: Some((cert_b, key_b, peers("bot.alpha", port_a, uri_a))),
            fed_listen: Some(port_b),
            ..Default::default()
        },
    );
    let bot = Bot::start(&a).await;
    // In a mesh a bare name resolves over the whole view and fails closed when a peer cannot
    // be asked (xmsg A7), so the local service addresses the bot `@node` as listed on node A.
    let (_, list) = a.http("GET", "/v1/recipients", None);
    let list = list.to_string();
    let at = list.find("\"matrix-xmsg@").expect("the bot is listed") + 1;
    let bot_addr = &list[at..at + list[at..].find('"').unwrap()];

    // Local: a service on the bot's own node.
    let requester = Fake::start(json!({
        "role": "service", "name": "requester",
        "register_sock": a.sock("register.sock"), "agent_sock": a.sock("agent.sock"),
        "send": { "to": bot_addr, "text": open_thread("FROM-LOCAL") },
    }));
    requester.ready().await;
    let reply = eventually("the local requester's answer", 20, || {
        requester
            .lines("deliveries.jsonl")
            .first()
            .and_then(|d| d["text"].as_str().map(String::from))
    })
    .await;
    assert!(
        reply.contains("\"root\""),
        "local open_thread accepted: {reply}"
    );
    assert!(
        bot.notices().iter().any(|n| n.contains("FROM-LOCAL")),
        "{:?}",
        bot.notices()
    );

    // Remote: an anonymous sender on node B (this process is no one there).
    let (status, sent) = b.http(
        "POST",
        "/v1/messages",
        Some(&json!({ "to": "matrix-xmsg@bot.alpha", "from": "ci", "text": open_thread("FROM-REMOTE") })),
    );
    assert_eq!(status, 202, "the federated send is delivered: {sent}");
    let id = sent["message_id"].as_str().expect("message_id").to_string();
    let answer = eventually("the bot's answer on node B", 30, || {
        let (_, replies) = b.http("GET", &format!("/v1/messages/{id}/replies?wait=1"), None);
        replies
            .as_array()
            .and_then(|r| r.first())
            .and_then(|r| r["text"].as_str().map(String::from))
    })
    .await;
    assert!(
        answer.contains("unauthorized origin"),
        "a remote open_thread is refused: {answer}"
    );
    assert!(
        !bot.notices().iter().any(|n| n.contains("FROM-REMOTE")),
        "nothing from another node is posted: {:?}",
        bot.notices()
    );
}

// =============================================================================
// Oracle 3 (A-13, AM-7). M16: the cursor is keyed on the ID `send` resolved. The same ID
// does not re-bootstrap; a different ID behind the same label does.
// Mutant: invert the comparison => RED.
// =============================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a real xmsg: set XMSG_BIN and pass --include-ignored"]
async fn oracle_3_m16_rebootstraps_on_a_new_resolved_id() {
    let Some(bin) = xmsg_bin() else { return };
    let node = Node::start(
        &bin,
        Spec {
            services: vec!["matrix-xmsg"],
            ..Default::default()
        },
    );
    let db = tempdir().unwrap();
    let mut config = node.config(&db.path().join("bot.db"));
    config.screen_mentions = false;
    let store = Store::new(&config.db_path).unwrap();
    let matrix = MockMatrixClient::default();
    let xmsg = create_xmsg_client(&config);
    let registration = register_svc(&config.xmsg_register_socket(), "matrix-xmsg")
        .await
        .expect("register");
    let node_name = registration.node().expect("node");
    let x = SilentInstance::start();
    node.instance_file(x.sleeper.id(), ID_X, "expert", &x.sock);

    let ask = |id: &'static str, ts: i64| mention(id, Some("$root"), "@genie Q", ts);
    for (ev, ts) in [("$q1", 1_000_000), ("$q2", 1_050_000)] {
        let out = handle_incoming_event(&ask(ev, ts), &[], &config, &matrix, xmsg.as_ref(), &store)
            .await
            .unwrap();
        assert_eq!(out, BotOutcome::Forwarded);
    }
    let got_x = eventually("two deliveries to X", 10, || {
        let r = x.received();
        (r.len() >= 2).then_some(r)
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let got_x = if x.received().len() > got_x.len() {
        x.received()
    } else {
        got_x
    };
    assert_eq!(
        got_x.len(),
        2,
        "same resolved ID: a bootstrap, then a delta, and no re-bootstrap"
    );
    assert!(!got_x.iter().any(|l| l.contains("supersedes")), "{got_x:?}");
    assert_eq!(
        store
            .get_thread_cursor("$root")
            .unwrap()
            .unwrap()
            .session_id
            .as_deref(),
        Some(format!("{ID_X}@{node_name}").as_str())
    );

    // The expert's label now names a new conversation, Y.
    fs::remove_file(node.sessions.join(format!("{}.json", x.sleeper.id()))).unwrap();
    drop(x);
    let y = SilentInstance::start();
    node.instance_file(y.sleeper.id(), ID_Y, "expert", &y.sock);

    let out = handle_incoming_event(
        &ask("$q3", 1_100_000),
        &[],
        &config,
        &matrix,
        xmsg.as_ref(),
        &store,
    )
    .await
    .unwrap();
    assert_eq!(out, BotOutcome::Forwarded);
    let got_y = eventually("two deliveries to Y", 10, || {
        let r = y.received();
        (r.len() >= 2).then_some(r)
    })
    .await;
    assert_eq!(got_y.len(), 2, "a delta, then the re-bootstrap: {got_y:?}");
    assert!(got_y[1].contains("supersedes"), "{got_y:?}");
    assert_eq!(
        store
            .get_thread_cursor("$root")
            .unwrap()
            .unwrap()
            .session_id
            .as_deref(),
        Some(format!("{ID_Y}@{node_name}").as_str())
    );
    drop(registration);
}

// =============================================================================
// Oracle 4 (M24). Real-xmsg handle proof: an instance's `reply` call over `agent.sock`
// returns `messageId` (and `pushedMessageId`), which equals the `delivery.message_id` the
// bot receives on `register.sock` and records in `bot_reply_events`. The instance then
// sends `{"redact": <returned messageId>}` to `matrix-xmsg` using only the ID its own
// `reply` call returned, and the bot redacts the posted Matrix event end to end.
// =============================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a real xmsg: set XMSG_BIN and pass --include-ignored"]
async fn oracle_4_m24_reply_then_redact_by_returned_message_id() {
    let Some(bin) = xmsg_bin() else { return };
    let node = Node::start(
        &bin,
        Spec {
            services: vec!["matrix-xmsg", "genie-guard"],
            ..Default::default()
        },
    );
    let bot = Bot::start(&node).await;

    let verdict = json!({ "verdict": "allow", "reason": "clean", "cleaned_text": "" });
    let guard = Fake::start(json!({
        "role": "service", "name": "genie-guard",
        "register_sock": node.sock("register.sock"), "agent_sock": node.sock("agent.sock"),
        "reply": verdict.to_string(),
    }));
    guard.ready().await;

    let expert_inbox = node.tmp.path().join("expert_m24.sock");
    let expert = Fake::start(json!({
        "role": "instance",
        "inbox": expert_inbox,
        "agent_sock": node.sock("agent.sock"),
        "reply": "LEAKED-SECRET-CONFIG",
        "redact_after_reply": {
            "to": "matrix-xmsg",
            "reason": "accidental config leak",
        },
    }));
    expert.ready().await;
    node.instance_file(expert.pid(), ID_EXPERT, "expert", &expert_inbox);

    let xmsg = create_xmsg_client(&bot.config);
    let outcome = handle_incoming_event(
        &mention("$ask_m24", None, "@genie show config", 3_000_000),
        &[],
        &bot.config,
        bot.matrix.as_ref(),
        xmsg.as_ref(),
        &bot.store,
    )
    .await
    .unwrap();
    assert_eq!(outcome, BotOutcome::Forwarded);

    // Wait for both the initial notice to be posted AND the subsequent redaction to land!
    let redacted = eventually(
        "the bot's post to be redacted via returned reply ID",
        20,
        || {
            let list = bot.matrix.redacted_events.lock().unwrap();
            list.iter()
                .find(|r| r.reason.as_deref() == Some("accidental config leak"))
                .cloned()
        },
    )
    .await;

    assert_eq!(redacted.room_id, ROOM);
    assert_eq!(redacted.event_id, "$mock_event_1");

    // Verify that the ID returned by `xmsg`'s `reply` call in the child process is recorded in
    // `bot.store.get_bot_reply_event(...)` and maps to `(ROOM, "$mock_event_1")`.
    let reply_frames = expert.lines("replies.jsonl");
    assert_eq!(reply_frames.len(), 1, "{reply_frames:?}");
    let returned_id = reply_frames[0]["reply"]["pushedMessageId"]
        .as_str()
        .or_else(|| reply_frames[0]["reply"]["id"].as_str())
        .or_else(|| reply_frames[0]["messageId"].as_str())
        .or_else(|| reply_frames[0]["pushedMessageId"].as_str())
        .expect("reply response must carry pushedMessageId / messageId")
        .to_string();
    let rec = bot
        .store
        .get_bot_reply_event(&returned_id)
        .unwrap()
        .expect("bot store must have recorded the exact ID returned by xmsg reply");
    assert_eq!(rec.room_id, ROOM);
    assert_eq!(rec.event_id, "$mock_event_1");
    assert!(
        rec.author_principal.starts_with(&format!("{ID_EXPERT}@")),
        "author_principal must be <instance-id>@<node>: {}",
        rec.author_principal
    );
}

// -----------------------------------------------------------------------------
// CA fixtures, after xmsg's tests/gate_x16_ca_peers.rs
// -----------------------------------------------------------------------------
mod ca {
    pub fn generate_ca(name: &str) -> (rcgen::CertificateParams, rcgen::KeyPair, String) {
        let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ED25519).unwrap();
        let mut params = rcgen::CertificateParams::default();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, name);
        params.key_usages = vec![
            rcgen::KeyUsagePurpose::KeyCertSign,
            rcgen::KeyUsagePurpose::CrlSign,
            rcgen::KeyUsagePurpose::DigitalSignature,
        ];
        let cert = params.self_signed(&key).unwrap();
        let pem = cert.pem();
        (params, key, pem)
    }

    /// A leaf certificate with one SPIFFE URI SAN: (cert PEM, key PEM).
    pub fn issue_cert(
        ca_params: &rcgen::CertificateParams,
        ca_key: &rcgen::KeyPair,
        name: &str,
        uri: &str,
    ) -> (String, String) {
        let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ED25519).unwrap();
        let mut params = rcgen::CertificateParams::default();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, name);
        params.subject_alt_names = vec![rcgen::SanType::URI(uri.to_string().try_into().unwrap())];
        params.key_usages = vec![rcgen::KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![
            rcgen::ExtendedKeyUsagePurpose::ClientAuth,
            rcgen::ExtendedKeyUsagePurpose::ServerAuth,
        ];
        let issuer = rcgen::Issuer::from_params(ca_params, ca_key);
        let cert = params.signed_by(&key, &issuer).unwrap();
        (cert.pem(), key.serialize_pem())
    }
}

// -----------------------------------------------------------------------------
// The children: a no-op unless this process was started as one (`MA_CHILD` names its
// directory, holding `spec.json`). Each writes `ready` once it can be reached.
// -----------------------------------------------------------------------------
mod child {
    use super::*;

    pub fn serve_if_child() {
        let Ok(dir) = std::env::var(CHILD_ENV) else {
            return;
        };
        let dir = PathBuf::from(dir);
        let spec: Value =
            serde_json::from_str(&fs::read_to_string(dir.join("spec.json")).unwrap()).unwrap();
        match spec["role"].as_str().unwrap() {
            "service" => service(&dir, &spec),
            "instance" => instance(&dir, &spec),
            r => panic!("unknown role {r}"),
        }
    }

    fn append(dir: &Path, name: &str, v: &Value) {
        let mut f = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join(name))
            .unwrap();
        writeln!(f, "{v}").unwrap();
    }

    /// One `agent.sock` frame and its answer.
    fn agent(sock: &str, frame: Value) -> Value {
        let mut s = UnixStream::connect(sock).unwrap();
        writeln!(s, "{frame}").unwrap();
        let mut line = String::new();
        BufReader::new(s).read_line(&mut line).unwrap();
        serde_json::from_str(&line).unwrap()
    }

    /// A registered service: optionally sends one message, then records every delivery,
    /// replies with `reply` when it is set, and acks.
    fn service(dir: &Path, spec: &Value) {
        let s = UnixStream::connect(spec["register_sock"].as_str().unwrap()).unwrap();
        let mut w = s.try_clone().unwrap();
        let mut r = BufReader::new(s);
        let mut call = |frame: Value| -> Value {
            writeln!(w, "{frame}").unwrap();
            let mut line = String::new();
            r.read_line(&mut line).unwrap();
            serde_json::from_str(&line).unwrap()
        };
        let ok = call(json!({ "harness": "svc", "name": spec["name"] }));
        assert_eq!(ok["status"], "ok", "{ok}");
        if let Some(send) = spec.get("send") {
            let resp = agent(
                spec["agent_sock"].as_str().unwrap(),
                json!({ "action": "send", "to": send["to"], "text": send["text"] }),
            );
            append(dir, "sent.jsonl", &resp);
        }
        fs::write(dir.join("ready"), "").unwrap();
        loop {
            let d = call(json!({ "action": "poll", "waitSecs": 5 }));
            if d["action"] != "deliver" {
                continue;
            }
            append(dir, "deliveries.jsonl", &d);
            if let Some(reply) = spec["reply"].as_str() {
                let resp =
                    call(json!({ "action": "reply", "messageId": d["messageId"], "text": reply }));
                append(dir, "replies.jsonl", &resp);
            }
            call(json!({ "action": "ack", "messageId": d["messageId"] }));
        }
    }

    /// An instance: listens at its messaging socket and replies to each delivery over
    /// `agent.sock`, as the instance its session file names this process as.
    fn instance(dir: &Path, spec: &Value) {
        let listener =
            std::os::unix::net::UnixListener::bind(spec["inbox"].as_str().unwrap()).unwrap();
        fs::write(dir.join("ready"), "").unwrap();
        for stream in listener.incoming() {
            let mut buf = String::new();
            stream.unwrap().read_to_string(&mut buf).unwrap();
            append(dir, "deliveries.jsonl", &json!({ "line": buf }));
            let key = "message_id=";
            let Some(at) = buf.rfind(key) else { continue };
            let id: String = buf[at + key.len()..]
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric())
                .collect();
            let resp = agent(
                spec["agent_sock"].as_str().unwrap(),
                json!({ "action": "reply", "message_id": id, "text": spec["reply"] }),
            );
            append(dir, "replies.jsonl", &resp);
            if let Some(redact_spec) = spec.get("redact_after_reply") {
                if let Some(reply_id) = resp["reply"]["pushedMessageId"]
                    .as_str()
                    .or_else(|| resp["reply"]["id"].as_str())
                    .or_else(|| resp["messageId"].as_str())
                    .or_else(|| resp["pushedMessageId"].as_str())
                {
                    std::thread::sleep(Duration::from_millis(300));
                    let redact_payload = json!({
                        "redact": reply_id,
                        "reason": redact_spec["reason"],
                    });
                    let redact_resp = agent(
                        spec["agent_sock"].as_str().unwrap(),
                        json!({
                            "action": "send",
                            "to": redact_spec["to"],
                            "text": redact_payload.to_string(),
                        }),
                    );
                    append(dir, "redacts.jsonl", &redact_resp);
                }
            }
        }
    }
}
