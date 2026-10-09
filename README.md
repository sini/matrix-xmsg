# matrix-xmsg: Matrix Support Bot Backed by an Expert Agent Session

`matrix-xmsg` is a secure Matrix bridge connecting public Matrix support rooms to a dedicated "expert" AI agent session (such as Claude Code) running behind [xmsg](https://github.com/sini/xmsg).

---

## 1. Security Architecture & Trust Model

### 1.1 Admission Policy & Silence Rule

- **Public Room Posture:** Anyone may join and post in the room.
- **Mention Trigger & Engaged Threads (M8):**
  - Top-level messages require an explicit `@`-mention (via `m.mentions.user_ids`, Matrix pills, or plain text) or in-thread control triggers (`!deeper`, `!escalate`).
  - An **engaged thread** is one whose root has a recorded asker or a bot message. Any admitted message in an engaged thread passes the mention gate without requiring a mention.
  - The envelope marks whether the trigger line addressed the bot (`addressed: true|false`). For unaddressed follow messages (`addressed: false`), a missing expert reply posts nothing (no timeout notice, no DM); addressed messages preserve existing timeout escalation notices.
- **Relay Acknowledgement & Late Answers (M9):**
  - **ACK Reaction (👀):** When an addressed message (`addressed: true`) is accepted by xmsg, the bot reacts to the event with 👀 (`m.reaction`) to signal work has begun. Unaddressed follows and dropped/refused messages receive no reaction.
  - **Late Answers & Deadline:** If the expert reply does not arrive within `answer_timeout_secs` (default 300s), an owner DM is sent, but the bot continues waiting up to `answer_deadline_secs` (default 3600s). If the reply arrives before the deadline, it is posted to the thread as normal. Past the deadline, the bot stops waiting and posts a user-facing timeout notice.
- **Admission Modes:**
  - `trusted` (default): Messages from non-allowlisted senders are dropped **silently**. The bot never responds or sends an error notice, preventing oracle attacks where attackers probe for valid allowlisted usernames.
  - `public`: Any room member's top-level question is relayed to the expert session; in engaged threads, the original thread asker and trusted senders are admitted, while bystanders are silently ignored.

### 1.2 Interaction Controls & Authorization (M5a)

- **Reactions & Commands:**
  - `✅` reaction: Accept answer.
  - `🔍` reaction: Request deeper investigation.
  - `!deeper` text fallback: Thread message requesting deeper investigation.
- **Authorization Rule:** Only the original thread asker or a trusted MXID (`trusted_mxids` or `owner_mxid`) can issue interaction controls. Reactions or `!deeper` commands from strangers are silently ignored.
- **Debouncing:** Duplicate reactions or commands from the same user on the same bot message are debounced via SQLite `(message_id, user_mxid, control)` unique constraints, emitting exactly one event to the expert.
- **Expert Event Format:** Interaction controls emit JSON payloads to the expert over xmsg: `{"thread_id": ..., "control": "accept"|"deeper", "by": ...}`.

### 1.3 Tiered Genie Rendering & Silent Decline (M5b, M9)

- **Structured JSON Replies:** When the expert responds with JSON containing `confidence`, `gaps`, and/or `tier`:
  - Renders answer body.
  - Confidence metadata line: `confidence 0.72 · gaps: ...` (or `gaps: none`).
  - `tier: "expert"` replies display `[expert review]` header and `expert review` in the metadata line.
  - Hint line: `Controls: ✅ accept · 🔍 deeper · !deeper`.
- **Silent Decline (`{"silent": true}`):** If the agent determines the message requires no response, it replies with `{"silent": true}`. The bot posts no room message, sends no owner DM, redacts its original 👀 reaction, and reacts with 🫡 ("noted"). The bot's own reactions are filtered by the self-sender guard and never parsed as controls.
- **Backward Compatibility:** Plain-text replies with no JSON structure render byte-identically to legacy outputs without confidence or control hint lines.

### 1.4 Context Isolation & Provenance Tagging (M5d)

- Messages sent to the expert include recent room history (for top-level questions) or thread history (for threaded questions) under count and byte caps.
- Header format: `[matrix] room={room_id} thread={thread_root_id} user={mapped_sender} ({trigger_tier}) thread_tier={thread_tier}`.
  - Senders in `trusted_mxids` or matching `owner_mxid` receive tier `trusted`; all others receive `public`.
  - `thread_tier`: lowest tier across all lines in the request and context history (`public < trusted`).
- The envelope wraps context in `<context>` blocks and requests in `<request>` blocks per spec §4.4.
- Each line within `<request>` and `<context>` is an authenticated JSON object:
  ```json
  {"sender": "matrix alice at example.org", "tier": "trusted", "text": "message text"}
  ```
- Any `<request>`, `</request>`, `<context`, or `</context>` tags occurring inside user-supplied message text are automatically escaped prior to JSON serialization to prevent XML sandbox breakouts.

### 1.5 Strict ASCII Sender Mapping

Matrix IDs (`@alice:example.org`) are converted to sanitized ASCII identifiers (`matrix alice at example.org`):

- Never contains `:` or `/` (conforming to xmsg HTTP sender invariants).
- Strips non-ASCII characters.
- Enforces a 64-character length cap.

---

## 2. Configuration & Direct Execution

Configuration is provided via a TOML file passed with `--config <PATH>`:

```toml
homeserver_url = "https://matrix.example.org"
bot_mxid = "@genie:example.org"
access_token_file = "/run/credentials/matrix-xmsg.service/access-token"
rooms = ["!support:example.org"]
trusted_mxids = ["@alice:example.org", "@bob:example.org"]
owner_mxid = "@owner:example.org"
admission = "trusted" # "trusted" (default) or "public"
xmsg_url = "http://127.0.0.1:7787"
expert_ref = "claude"
history_n = 30
history_byte_cap = 12288
rate_limit_count = 10
rate_limit_window_secs = 600
size_cap_bytes = 4096
answer_timeout_secs = 300
answer_deadline_secs = 3600
db_path = "/var/lib/matrix-xmsg/matrix-xmsg.db"
```

The access token is loaded from `access_token_file` at runtime and is never logged or exposed. If `access_token_file` is a relative path and `$CREDENTIALS_DIRECTORY` is set, it resolves relative to `$CREDENTIALS_DIRECTORY`.

---

## 3. NixOS Deployment (`services.matrix-xmsg`)

The flake exports a NixOS module as `nixosModules.default` under the `services.matrix-xmsg` option namespace.

### 3.1 Host Configuration Example

```nix
{ config, pkgs, inputs, ... }:
{
  imports = [
    inputs.matrix-xmsg.nixosModules.default
  ];

  services.matrix-xmsg = {
    enable = true;
    homeserverUrl = "https://matrix.json64.dev";
    botMxid = "@genie:json64.dev";
    accessTokenFile = "/run/agenix/matrix-genie-token";
    rooms = [ "!general:json64.dev" ];
    trustedMxids = [ "@owner:json64.dev" ];
    ownerMxid = "@owner:json64.dev";
    xmsgUrl = "http://127.0.0.1:7787";
    expertRef = "claude";
  };
}
```

### 3.2 Token Provisioning Steps (Host Owner)

1. **Generate Matrix Access Token:**
   Log in to the homeserver as the bot user (`@genie:json64.dev`) and obtain an access token.
2. **Provision Token Secret:**
   Store the raw access token string in an encrypted secret managed by `agenix` or `sops-nix` (e.g., `/run/agenix/matrix-genie-token`) with permissions `0400` owned by `root`.
3. **Runtime Credential Delivery:**
   The module declares `LoadCredential = [ "access-token:${cfg.accessTokenFile}" ]`.
   Systemd reads the token file at service activation and mounts it into the service's private credential directory (`$CREDENTIALS_DIRECTORY/access-token`). The secret is **never copied into the world-readable `/nix/store`**.

### 3.3 Module Options Reference

| Option                | Type          | Default                        | Description                                                                                        |
| --------------------- | ------------- | ------------------------------ | -------------------------------------------------------------------------------------------------- |
| `enable`              | `bool`        | `false`                        | Enable the matrix-xmsg daemon.                                                                     |
| `package`             | `package`     | `matrix-xmsg`                  | Package to run.                                                                                    |
| `homeserverUrl`       | `str`         | *(required)*                   | Base URL of the Matrix homeserver.                                                                 |
| `botMxid`             | `str`         | *(required)*                   | Full Matrix user ID of the bot (`@name:server`).                                                   |
| `accessTokenFile`     | `path`        | *(required)*                   | Path to file containing the Matrix access token.                                                   |
| `rooms`               | `listOf str`  | `[]`                           | Room IDs (`!id:server`) monitored by the bot. Aliases (`#alias:server`) are rejected at eval time. |
| `trustedMxids`        | `listOf str`  | `[]`                           | Allowlist of user Matrix IDs permitted to interact with the bot.                                   |
| `ownerMxid`           | `str`         | *(required)*                   | Owner Matrix ID for escalations and direct notices.                                                |
| `stateDir`            | `path`        | `"/var/lib/matrix-xmsg"`       | State directory for persistent store and database.                                                 |
| `xmsgUrl`             | `str`         | `"http://127.0.0.1:7787"`      | HTTP bridge URL to xmsg server.                                                                    |
| `xmsgSocket`          | `nullOr path` | `null`                         | Optional agent socket path (evaluated; see attestation below).                                     |
| `expertRef`           | `str`         | `"claude"`                     | Session name/ref in xmsg to route queries to.                                                      |
| `historyN`            | `uint`        | `30`                           | Number of context messages fetched from timeline.                                                  |
| `historyByteCap`      | `uint`        | `12288`                        | Maximum byte size of context history window.                                                       |
| `rateLimitCount`      | `uint`        | `10`                           | Max queries permitted per user per window.                                                         |
| `rateLimitWindowSecs` | `uint`        | `600`                          | Sliding window duration in seconds.                                                                |
| `sizeCapBytes`        | `uint`        | `4096`                         | Max body size of queries accepted.                                                                 |
| `answerTimeoutSecs`   | `uint`        | `300`                          | Timeout before escalating to owner.                                                                |
| `answerDeadlineSecs`  | `uint`        | `3600`                         | Deadline in seconds to wait for late answers before giving up.                                     |
| `dbPath`              | `path`        | `"${stateDir}/matrix-xmsg.db"` | Path to SQLite database (created mode `0600`).                                                     |
| `dynamicUser`         | `bool`        | `true`                         | Whether to allocate an ephemeral systemd DynamicUser.                                              |
| `user`                | `str`         | `"matrix-xmsg"`                | Static user when `dynamicUser = false`.                                                            |
| `group`               | `str`         | `"matrix-xmsg"`                | Static group when `dynamicUser = false`.                                                           |

### 3.4 Systemd Hardening & Lifetime Guarantees

- **Strict Isolation:** `DynamicUser = true`, `ProtectSystem = strict`, `ProtectHome = true`, `PrivateTmp = true`, `PrivateDevices = true`, `ProtectKernelTunables = true`, `ProtectControlGroups = true`, `NoNewPrivileges = true`, `RestrictNamespaces = true`, `RestrictAddressFamilies = AF_INET AF_INET6 AF_UNIX`, `UMask = 0077`.
- **State Persistence:** `StateDirectory = "matrix-xmsg"` provisions `/var/lib/matrix-xmsg` owned by the service user. The SQLite database is created mode `0600` via `Store::new`.
- **Bounded Shutdown:** `TimeoutStopSec = 60` provides generous margin above the 5-second in-flight drain and 5-second shutdown notice bounds.

### 3.5 xmsg Attestation Evaluation

`xmsg` operates two incoming interfaces:

1. **HTTP Bridge (`xmsgUrl`, default `http://127.0.0.1:7787`):**
   Used by `matrix-xmsg`. Accepts `POST /v1/sessions/{expert}/messages`. Senders are tagged as `from@host_label`. This interface requires network reachability to localhost and does not enforce process UID checks.
2. **Attested Unix Domain Socket (`agent.sock`):**
   `xmsg` creates `agent.sock` mode `0600` inside `$XDG_RUNTIME_DIR/xmsg/` (mode `0700`), owned by the desktop session UID (e.g. UID 1000). On connection, it checks `stream.peer_cred()` (`peer_uid == my_uid`), followed by an ancestor process tree walk to find a registered Claude, Antigravity, or Pi session PID.

**System Service Attestation Finding:**
A systemd system service (`matrix-xmsg.service`) running under a system user or `DynamicUser` **cannot** connect to `agent.sock` under existing `xmsg` rules due to:

- DAC permissions (`0600` / `0700` owned by desktop user UID).
- Strict peer UID check (`peer_uid != my_uid` rejection).
- PPID ancestor walk terminating at PID 1 (`systemd`), matching no known LLM harness.

**Minimal Proposed `xmsg`-side Evolution:**
To support attested daemon bridges like `matrix-xmsg` over Unix sockets in the future:

1. Provide a group-accessible socket (`mode 0660`, e.g. group `xmsg`).
2. Accept peer UIDs belonging to the trusted socket group or authorized via credential token.
3. Add a `bridge` harness identity in `resolve_caller_session` that verifies the caller executable path or systemd unit cgroup and generates an attested badge `xmsg@host · bridge:matrix-xmsg`.

---

## 4. OCI Container Image (M4)

An unprivileged, minimal OCI container image is built via `dockerTools.buildLayeredImage` and published to `ghcr.io/sini/matrix-xmsg`.

### 4.1 Security Properties

- **Non-root Execution:** Runs under UID/GID `10001:10001`.
- **No Shell:** The image contains only CA certificates (`/etc/ssl/certs/ca-bundle.crt`) and the `matrix-xmsg` static binary; no shell (`/bin/sh`) or auxiliary utilities are present in any layer.
- **Entrypoint:** Preconfigured entrypoint `matrix-xmsg` with default args `--config /etc/matrix-xmsg/config.toml` and working directory `/var/lib/matrix-xmsg`.

### 4.2 Building & Verification

```bash
# Build the OCI image archive
nix build .#image

# Run the image oracle check (asserts non-root user, bot binary entrypoint, no /bin/sh in layers)
nix build -L .#checks.x86_64-linux.image
```

---

## 5. Verification & Testing

```bash
# 1. Run Cargo tests
nix develop --command cargo test --all-targets

# 2. Check Formatting and Clippy
nix develop --command cargo fmt --check
nix develop --command cargo clippy --all-targets -- -D warnings

# 3. Run NixOS VM Integration Test (Module against stub homeserver & xmsg)
nix build -L .#checks.x86_64-linux.nixos-module

# 4. Run NixOS VM Mutant Test (Demonstrates RED on invalid token path)
nix build -L .#checks.x86_64-linux.nixos-module-mutant-wrong-token

# 5. Run OCI Image Oracle Check
nix build -L .#checks.x86_64-linux.image

# 6. Run full Nix flake checks
nix flake check
nix flake check ./ci
```
