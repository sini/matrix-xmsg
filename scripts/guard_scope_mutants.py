"""Exercise producer scope source mutants; only the named assertion RED is a kill."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
CASES = [
    ("bypass-history", '        history,\n        content,', '        history: Vec::new(),\n        content,', "initial_paths_scope_and_legacy_bytes", 'l.sender == "@public:example.org"'),
    ("lose-clean-history", 'withhold_untrusted_history: false,\n                }))\n            }\n        }', 'withhold_untrusted_history: true,\n                }))\n            }\n        }', "initial_preservation_and_schema_error_modes", "GS1 clean allow history retained"),
    ("omit-scope", "config.guard_review_scope.then_some(if trigger_trusted {", "false.then_some(if trigger_trusted {", "initial_paths_scope_and_legacy_bytes", "GS1 initial scope"),
    ("ignore-enable-flag", "config.guard_review_scope.then_some(if trigger_trusted {", "true.then_some(if trigger_trusted {", "initial_paths_scope_and_legacy_bytes", "GS1 initial scope"),
    ("effective-routing-tier", "config.guard_review_scope.then_some(if trigger_trusted {", "config.guard_review_scope.then_some(if trigger_trusted || store.is_thread_promoted(thread_root).unwrap() || store.get_thread_escalation(thread_root).unwrap().is_some() {", "public_promoted_and_escalation_scope_is_all", "GS1 actual public sender scope"),
    ("effective-escalation-tier", "config.guard_review_scope.then_some(if trigger_trusted {", "config.guard_review_scope.then_some(if trigger_trusted || store.get_thread_escalation(thread_root).unwrap().is_some() {", "public_promoted_and_escalation_scope_is_all", "GS1 actual public sender scope"),
    ("trust-text", "config.guard_review_scope.then_some(if trigger_trusted {", 'config.guard_review_scope.then_some(if trigger_trusted || trigger_msg.body.contains("untrusted_lines") {', "initial_paths_scope_and_legacy_bytes", "GS1 initial scope"),
    ("omit-last-page-content", "text: last_msg.body.clone(),", "text: String::new(),", "pages_preserve_targets_and_trusted_last_across_chunks", "GS1 public page target includes last content"),
    ("rewrite-trusted-page", '(config.guard_review_scope && verdict.verdict == "rewrite")', '(false && verdict.verdict == "rewrite")', "pages_preserve_targets_and_trusted_last_across_chunks", "GS1 trusted-last byte equality"),
    ("screen-zero-target-chunks", "if non_bot.is_empty()\n            || (config.guard_review_scope", "if non_bot.is_empty()\n            || (false", "pages_preserve_targets_and_trusted_last_across_chunks", "GS1 scoped zero-target chunks bypass"),
]


def run(root, name, evidence, arm):
    profile = os.environ.get("CARGO_PROFILE", "dev")
    env = {**os.environ, "CARGO_TARGET_DIR": str(ROOT / "target" / "guard-scope-mutant-build")}
    command = ["cargo", "test", "--locked", "--offline", "--profile", profile, "-j", "4", "--test", "gate_gs1", "--no-run", "--message-format=json"]
    build = subprocess.run(command, cwd=root, env=env, capture_output=True, text=True, timeout=1200)
    (evidence / f"{name}-{arm}-build.log").write_text(build.stdout + build.stderr)
    assert build.returncode == 0, "compilation failure is not a kill"
    artifacts = [json.loads(row) for row in build.stdout.splitlines() if row.startswith("{")]
    intended = [row for row in artifacts if row.get("reason") == "compiler-artifact" and row.get("target", {}).get("name") == "gate_gs1" and row.get("executable")]
    assert len(intended) == 1 and not intended[0]["fresh"], "missing or stale intended test artifact"
    result = subprocess.run([intended[0]["executable"], name, "--exact", "--test-threads=1"], cwd=root, env=env, capture_output=True, text=True, timeout=90)
    (evidence / f"{name}-{arm}-test.log").write_text(result.stdout + result.stderr)
    return result


def main():
    nonce = uuid.uuid4().hex
    evidence = ROOT / "target" / "guard-scope-mutants" / nonce
    evidence.mkdir(parents=True)
    manifest = {"nonce": nonce, "cases": len(CASES), "model_requests": 0, "completed": False, "results": []}
    (evidence / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    started = time.monotonic()
    freshness_control = False
    with tempfile.TemporaryDirectory(prefix="matrix-guard-scope-") as directory:
        root = Path(directory)
        for filename in ("Cargo.toml", "Cargo.lock"):
            shutil.copyfile(ROOT / filename, root / filename)
        for filename in ("src", "tests"):
            shutil.copytree(ROOT / filename, root / filename, copy_function=shutil.copyfile)
        cargo = root / "Cargo.toml"
        lines, in_bin = [], False
        for row in cargo.read_text().splitlines(keepends=True):
            if row.startswith("["):
                in_bin = row.strip() == "[[bin]]"
            if not in_bin:
                lines.append(row)
                if row.strip() == "[package]":
                    lines.append("autobins = false\n")
        cargo.write_text("".join(lines))
        source = root / "src/bot.rs"
        original = source.read_text()
        test_source = root / "tests/gate_gs1.rs"
        test_original = test_source.read_text()
        for index, (label, old, new, test, assertion) in enumerate(CASES):
            assert original.count(old) == 1, (label, "source patch is not unique")
            # Fresh per-arm source makes executable freshness observable, even after restoration.
            source.write_text(original)
            test_source.write_text(test_original + f"\n// mutant baseline {nonce} {index}\n")
            green = run(root, test, evidence, f"{index}-baseline")
            assert green.returncode == 0 and "1 passed; 0 failed; 0 ignored" in green.stdout, (label, "invalid baseline")
            if not freshness_control:
                try:
                    run(root, test, evidence, "stale-control")
                except AssertionError as error:
                    assert str(error) == "missing or stale intended test artifact", error
                    freshness_control = True
                else:
                    raise AssertionError("cached executable accepted by freshness control")
            source.write_text(original.replace(old, new, 1))
            test_source.write_text(test_original + f"\n// mutant candidate {nonce} {index}\n")
            red = run(root, test, evidence, f"{index}-candidate")
            assert red.returncode == 101 and f"test {test} ... FAILED" in red.stdout and "test result: FAILED. 0 passed; 1 failed; 0 ignored" in red.stdout and assertion in red.stdout, (label, "survived or unintended RED")
            manifest["results"].append({"name": label, "test": test, "assertion": assertion, "baseline_source_sha256": hashlib.sha256(original.encode()).hexdigest(), "candidate_source_sha256": hashlib.sha256(source.read_bytes()).hexdigest(), "test_sha256": hashlib.sha256(test_source.read_bytes()).hexdigest(), "intended_red": True})
            (evidence / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
            print(f"{label}: baseline GREEN, intended assertion RED", flush=True)
    assert freshness_control and len(manifest["results"]) == len(CASES)
    manifest.update(completed=True, elapsed_seconds=time.monotonic() - started)
    (evidence / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    (evidence / "COMPLETE").write_text(f"{nonce} {len(CASES)}/{len(CASES)}\n")
    print(f"{len(CASES)}/{len(CASES)} producer scope mutants killed; artifacts {evidence}")


if __name__ == "__main__":
    main()
