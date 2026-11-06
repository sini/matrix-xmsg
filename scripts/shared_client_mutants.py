"""Single-site Matrix adapter mutants; build errors/timeouts never count as kills."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
CASES = [
    ("idempotency-key", "src/xmsg.rs", "self.agent()?.send(to, text, key)", "self.agent()?.send(to, text, None)", "shared_transport", "accepted_transport_and_cursor_trace_matches_legacy_golden"),
    ("display-name", "src/xmsg.rs", "from_name: delivery.from_name().into(),", "from_name: String::new(),", "shared_transport", "accepted_transport_and_cursor_trace_matches_legacy_golden"),
    ("request-id-substitution", "src/xmsg.rs", "fn from_record(record: ReplyRecord) -> Self {", "fn from_record(record: ReplyRecord) -> Self { let mut record = record; if record.pushed_message_id.is_none() { record.pushed_message_id = Some(record.message_id.clone()); }", "shared_transport", "stored_reply_preserves_its_record_without_inventing_a_pushed_id"),
    ("stored-association", "src/bot.rs", "record.pushed_message_id.filter(|id| !id.is_empty())", "None", "gate_idem_retry", "a_reply_to_a_stored_relay_routes_to_its_matrix_thread"),
    ("outcome", "src/xmsg.rs", "outcome: Some(receipt.outcome),", "outcome: None,", "gate_idem_retry", "keyed_frames_carry_the_key_and_read_pending"),
    ("reconnect-facade", "src/xmsg.rs", "*client.agent.lock().unwrap() = Some(service.agent_client());", "{ let mut slot = client.agent.lock().unwrap(); if slot.is_none() { *slot = Some(service.agent_client()); } }", "shared_transport", "reconnect_rebinds_the_existing_outbound_facade"),
    ("stored-classification", "src/xmsg.rs", 'if record.push_outcome.as_deref() != Some("stored")', "if true", "shared_transport", "stored_reply_preserves_its_record_without_inventing_a_pushed_id"),
]


def run(root, target, test):
    env = {**os.environ, "CARGO_TARGET_DIR": str(ROOT / "target")}
    profile = env.get("CARGO_PROFILE", "dev")
    subprocess.run(["cargo", "clean", "--locked", "--offline", "-p", "matrix-xmsg", "--profile", profile], cwd=root, env=env, check=True, capture_output=True, timeout=120)
    build = subprocess.run(["cargo", "test", "--locked", "--offline", "--profile", profile, "-j", "4", "--test", target, "--no-run", "--message-format=json"], cwd=root, env=env, capture_output=True, text=True, timeout=1200)
    assert build.returncode == 0, ("compilation failed; not a mutant kill", build.stdout[-10000:], build.stderr[-10000:])
    executable = []
    for line in build.stdout.splitlines():
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if item.get("reason") == "compiler-artifact" and item.get("target", {}).get("name") == target and item.get("profile", {}).get("test") and item.get("executable"):
            executable.append(item["executable"])
    assert len(executable) == 1 and Path(executable[0]).is_file(), ("missing intended executable", target)
    return subprocess.run([executable[0], test, "--exact", "--test-threads=1"], cwd=root, env=env, capture_output=True, text=True, timeout=45)


def main():
    evidence = ROOT / "target/shared-client-mutants"
    evidence.mkdir(parents=True, exist_ok=True)
    for name, filename, old, new, target, test in CASES:
        with tempfile.TemporaryDirectory(prefix="matrix-client-mutant-") as directory:
            root = Path(directory)
            for source in ("Cargo.toml", "Cargo.lock"):
                shutil.copy2(ROOT / source, root / source)
            for source in ("src", "tests", "scripts"):
                shutil.copytree(ROOT / source, root / source)
            green = run(root, target, test)
            assert green.returncode == 0 and "1 passed; 0 failed; 0 ignored" in green.stdout, (name, "invalid baseline", green.stdout, green.stderr)
            file = root / filename
            body = file.read_text()
            assert body.count(old) == 1, (name, body.count(old))
            file.write_text(body.replace(old, new, 1))
            red = run(root, target, test)
            assert red.returncode == 101 and f"test {test} ... FAILED" in red.stdout, (name, "survived or wrong red", red.stdout, red.stderr)
            (evidence / f"{name}.log").write_text(red.stdout + red.stderr)
            print(f"{name}: GREEN baseline, RED intended test", flush=True)
    print(f"{len(CASES)}/{len(CASES)} Matrix adapter mutants killed", flush=True)


if __name__ == "__main__":
    main()
