"""Bind the client library, both daemon CI inputs and their immutable locks."""
import argparse
import json
from pathlib import Path
import re
import shutil
import tempfile
import tomllib


def check(root):
    pin = tomllib.loads((root / "ci/xmsg-pin.toml").read_text())
    manifest = tomllib.loads((root / "Cargo.toml").read_text())
    dependency = manifest["dependencies"]["xmsg"]
    assert dependency["git"] == pin["repository"], "client repository mismatch"
    assert dependency["rev"] == pin["revision"], "client revision mismatch"
    assert dependency["default-features"] is False and dependency["features"] == ["client"], "daemon feature leak"
    cargo = tomllib.loads((root / "Cargo.lock").read_text())
    packages = [p for p in cargo["package"] if p["name"] == "xmsg"]
    assert len(packages) == 1, "ambiguous xmsg lock"
    expected = f'git+{pin["repository"]}?rev={pin["revision"]}#{pin["revision"]}'
    assert packages[0]["source"] == expected, "Cargo.lock revision mismatch"
    flake = (root / "ci/flake.nix").read_text()
    lock = json.loads((root / "ci/flake.lock").read_text())
    for name, key in (("xmsg", "revision"), ("xmsg-legacy", "legacy_revision")):
        match = re.search(r'\b' + name + r'\s*=\s*\{\s*url\s*=\s*"github:sini/xmsg/([a-f0-9]{40})"', flake)
        assert match and match[1] == pin[key], f"{name} flake input mismatch"
        node = lock["nodes"][lock["nodes"]["root"]["inputs"][name]]
        assert node["locked"]["rev"] == pin[key], f"{name} flake lock mismatch"
        assert node["locked"]["narHash"], f"{name} missing source hash"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    check(args.root)
    print("client, daemon and legacy pins agree with their immutable locks")
    if not args.self_test:
        return
    files = ("Cargo.toml", "Cargo.lock", "ci/xmsg-pin.toml", "ci/flake.nix", "ci/flake.lock")
    revision = tomllib.loads((args.root / "ci/xmsg-pin.toml").read_text())["revision"]
    for filename in files:
        with tempfile.TemporaryDirectory(prefix="matrix-xmsg-pin-") as temporary:
            root = Path(temporary)
            (root / "ci").mkdir()
            for source in files:
                shutil.copy2(args.root / source, root / source)
            target = root / filename
            text = target.read_text()
            assert revision in text, (filename, "missing live positive control")
            target.write_text(text.replace(revision, "0" * 40, 1))
            try:
                check(root)
            except AssertionError:
                print(f"{filename}: revision mutation RED")
            else:
                raise AssertionError(f"{filename}: revision mutation survived")


if __name__ == "__main__":
    main()
