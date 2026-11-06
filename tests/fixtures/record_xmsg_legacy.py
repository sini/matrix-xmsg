"""Capture pinned legacy transport before migration; no Matrix server or fleet bus.

Usage: python record.py MATRIX_REPO OUTPUT_JSON
The original transport/store/error files are compiled unchanged. Config is a shim
for the two fields the transport reads; bot principal/cursor expressions are copied
verbatim. This is a transport baseline, not a full Matrix SDK integration test.
"""
import hashlib
import json
from pathlib import Path
import subprocess
import shutil
import sys
import tempfile

REVISION = "d835c7254e5197d05f9ca3a1520740867b3c35ee"
repo, output = Path(sys.argv[1]).resolve(), Path(sys.argv[2]).resolve()


def source(filename):
    return subprocess.check_output(["git", "-C", str(repo), "show", f"{REVISION}:src/{filename}"], text=True)


def function(text, name):
    start = text.index("pub fn " + name + "(")
    brace = text.index("{", start)
    depth = 1
    end = brace + 1
    while depth:
        depth += (text[end] == "{") - (text[end] == "}")
        end += 1
    return text[start:end]


files = {name: source(name) for name in ("xmsg.rs", "store.rs", "error.rs", "bot.rs")}
bot = files["bot.rs"]
start = bot.index("    let asker_origin_key = origin_author_key(delivery);")
end = bot.index("    let existing_target_ref", start)
cursor = bot[start:end]
manifest = '''[package]
name = "legacy-transport-probe"
version = "0.0.0"
edition = "2021"
[dependencies]
tokio = { version = "1.43", features = ["full"] }
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
thiserror = "2.0"
async-trait = "0.1"
tracing = "0.1"
rusqlite = { version = "=0.40.2", features = ["bundled", "fallible_uint"] }
tempfile = "3.17"
'''
main = r'''
#![allow(dead_code)]
mod xmsg;
mod store;
mod error;
mod config {
    pub struct Config { pub guard_timeout_secs: u64 }
    impl Config { pub fn xmsg_agent_socket(&self) -> std::path::PathBuf { unreachable!("config factory is not part of capture") } }
}
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener,UnixStream};
use xmsg::{SvcInbox, XmsgClient, SvcDelivery};
__PRINCIPAL_FUNCTIONS__
fn cursor_binding(delivery: &SvcDelivery, send_resp: &xmsg::SendResponse) -> Option<String> {
__CURSOR_EXPRESSION__
    asker_session_id.map(str::to_owned)
}
async fn exchange(stream: &mut BufReader<UnixStream>, response: Value) -> Value {
    let mut line = String::new(); stream.read_line(&mut line).await.unwrap();
    let frame = serde_json::from_str(&line).unwrap();
    stream.get_mut().write_all(format!("{response}\n").as_bytes()).await.unwrap();
    frame
}
#[tokio::main]
async fn main() {
    let temp = tempfile::tempdir().unwrap();
    let register_path = temp.path().join("register.sock");
    let agent_path = temp.path().join("agent.sock");
    let register = UnixListener::bind(&register_path).unwrap();
    let agent = UnixListener::bind(&agent_path).unwrap();
    let svc_task = tokio::spawn(async move {
        let mut stream = BufReader::new(register.accept().await.unwrap().0);
        let mut frames = vec![exchange(&mut stream,json!({"status":"ok","node":"fixture","address":"matrix-xmsg@fixture"})).await];
        for (index,origin) in [json!({"node":"fixture","id":"instance"}),json!({"node":"fixture","service":"expert"}),json!({"node":"fixture","label":"display-only"})].into_iter().enumerate() {
            frames.push(exchange(&mut stream,json!({"action":"deliver","messageId":format!("m{index}"),"fromName":"display · anonymous@fixture","text":"fixture body","envelope":"fixture envelope","origin":origin,"inReplyTo":"parent"})).await);
            frames.push(exchange(&mut stream,json!({"status":"ok"})).await);
        }
        frames
    });
    let agent_task = tokio::spawn(async move {
        let mut frames = vec![];
        for response in [json!({"status":"ok","message_id":"sent","address":"expert@fixture","outcome":"queued"}),json!({"status":"ok","message_id":"sent","address":"expert@fixture","outcome":"queued"}),json!({"status":"ok","reply":{"seq":1,"messageId":"sent","createdAt":1,"replierSessionId":"matrix-xmsg","text":"reply","pushOutcome":"queued","pushedMessageId":"reply"}})] {
            frames.push(exchange(&mut BufReader::new(agent.accept().await.unwrap().0),response).await);
        }
        frames
    });
    let mut inbox = xmsg::register_svc(&register_path,"matrix-xmsg").await.unwrap();
    let client = xmsg::UnixXmsgClient::new(agent_path);
    let sent = client.send_message_keyed("expert@fixture","display-not-identity","send","key").await.unwrap();
    let repeated = client.send_message_keyed("expert@fixture","display-not-identity","send","key").await.unwrap();
    let reply = client.reply_message_keyed("sent","reply","reply-key").await.unwrap();
    let db = store::Store::new(&temp.path().join("cursor.db")).unwrap();
    let mut deliveries = vec![];
    for index in 0..3 {
        let delivery = inbox.poll(0).await.unwrap().unwrap();
        let binding = cursor_binding(&delivery,&sent);
        db.set_thread_cursor("thread","event",1,binding.as_deref()).unwrap();
        let stored = db.get_thread_cursor("thread").unwrap().unwrap();
        deliveries.push(json!({"delivery":delivery,"cursor_session":stored.session_id}));
        inbox.ack(&format!("m{index}")).await.unwrap();
    }
    println!("{}",json!({"registration_node":inbox.node(),"deliveries":deliveries,"service_frames":svc_task.await.unwrap(),"agent_frames":agent_task.await.unwrap(),"send":{"message_id":sent.message_id,"session_id":sent.session_id},"repeat":{"message_id":repeated.message_id,"session_id":repeated.session_id},"reply":{"message_id":reply.message_id,"session_id":reply.session_id}}));
}
'''
main = main.replace("__PRINCIPAL_FUNCTIONS__", "\n".join(function(bot, name) for name in ("origin_session_id", "origin_author_key")))
main = main.replace("__CURSOR_EXPRESSION__", cursor)
with tempfile.TemporaryDirectory(prefix="matrix-transport-capture-") as directory:
    root = Path(directory)
    (root / "src").mkdir()
    (root / "Cargo.toml").write_text(manifest)
    for name in ("xmsg.rs", "store.rs", "error.rs"):
        (root / "src" / name).write_text(files[name])
    (root / "src/main.rs").write_text(main)
    lock = output.with_suffix(".Cargo.lock")
    if lock.exists(): shutil.copy2(lock, root / "Cargo.lock")
    argv = ["cargo", "run", "--quiet"] + (["--locked"] if lock.exists() else [])
    run = subprocess.run(argv, cwd=root, capture_output=True, text=True, timeout=180)
    if run.returncode:
        print(run.stderr[-12000:], file=sys.stderr)
        raise SystemExit(run.returncode)
    trace = json.loads(run.stdout)
    output.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(root / "Cargo.lock", lock)
    lock_hash = hashlib.sha256(lock.read_bytes()).hexdigest()
trace["recording"] = {
    "revision": REVISION,
    "capture_lock_sha256": lock_hash,
    "source_sha256": {name: hashlib.sha256(body.encode()).hexdigest() for name, body in files.items()},
    "cursor_expression_sha256": hashlib.sha256(cursor.encode()).hexdigest(),
    "scope": "unmodified legacy transport/store/error; minimal Config shim; verbatim bot principal functions and cursor selection; scripted private sockets, no Matrix SDK server",
    "comparison": "include from_name, origin, stored cursor, destinations, bodies, idempotency keys, IDs and ACK order; exclude only outgoing schema_version",
}
assert trace["deliveries"][2]["cursor_session"] == "anonymous@fixture"
assert trace["send"] == trace["repeat"]
output.parent.mkdir(parents=True, exist_ok=True)
output.write_text(json.dumps(trace, indent=2) + "\n")
print(f"recorded {len(trace['deliveries'])} deliveries from {REVISION}")
