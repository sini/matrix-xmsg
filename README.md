# matrix-xmsg: Matrix Support Bot Backed by an Expert Agent Session

`matrix-xmsg` is a secure Matrix bridge connecting public Matrix support rooms to a dedicated "expert" AI agent session (such as Claude Code) running behind [xmsg](https://github.com/sini/xmsg).

---

## 1. Security Architecture & Trust Model

### 1.1 Strict Allowlist & Silence Policy
- **Public Room Posture:** Anyone may join and post in the room.
- **Mention Trigger:** The bot acts **only** on messages that `@`-mention it (via `m.mentions.user_ids`, Matrix pills, or plain text).
- **Silent Drop for Non-Trusted Users:** Mentions from non-allowlisted senders are dropped **silently**. The bot never responds or sends an error notice, preventing oracle attacks where attackers probe for valid allowlisted usernames.

### 1.2 Context Isolation & Provenance Tagging
- Messages sent to the expert include recent room history (for top-level questions) or thread history (for threaded questions) under count and byte caps.
- Each history line is explicitly tagged with the sender's trust status: `[hh:mm] <user> (trusted|public): <text>`.
- The envelope wraps context in `<context>` blocks and requests in `<request>` blocks per spec §4.4.
- Any `<request>`, `</request>`, `<context`, or `</context>` tags occurring inside user-supplied message text are automatically escaped to prevent XML sandbox breakouts.

### 1.3 Strict ASCII Sender Mapping
Matrix IDs (`@alice:example.org`) are converted to sanitized ASCII identifiers (`matrix alice at example.org`):
- Never contains `:` or `/` (conforming to xmsg HTTP sender invariants).
- Strips non-ASCII characters.
- Enforces a 64-character length cap.

---

## 2. Configuration

Configuration is provided via a TOML file passed with `--config <PATH>`:

```toml
homeserver_url = "https://matrix.example.org"
bot_mxid = "@genie:example.org"
access_token_file = "/run/agenix/matrix-token"
rooms = ["!support:example.org"]
trusted_mxids = ["@alice:example.org", "@bob:example.org"]
owner_mxid = "@owner:example.org"
xmsg_url = "http://127.0.0.1:7787"
expert_ref = "claude"
history_n = 30
history_byte_cap = 12288
rate_limit_count = 10
rate_limit_window_secs = 600
size_cap_bytes = 4096
answer_timeout_secs = 300
db_path = "matrix-xmsg.db"
```

The access token is loaded from `access_token_file` at runtime and is never logged or exposed.

---

## 3. Building & Verification

```bash
# Run unit and integration tests
cargo test --all-targets

# Run Clippy lints
cargo clippy --all-targets -- -D warnings

# Check code formatting
cargo fmt -- --check

# Build Nix package
nix build .#default --no-link

# Run Nix CI checks
nix flake check ci
```
