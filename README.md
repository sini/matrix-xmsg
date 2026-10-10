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
- **Replies to Bot Messages (M14):**
  - A Matrix message replying to one of the bot's messages (`m.relates_to` carries `m.in_reply_to.event_id` naming an event previously sent by the bot) addresses the bot (`addressed: true`) identically to an explicit mention.
  - Thread fallbacks (`is_falling_back: true`) added by Matrix clients to indicate the latest thread event do NOT count as replies to the bot (preventing every subsequent thread message after a bot post from reading as addressed).
  - Top-level replies to bot messages address the bot; replies to messages from non-bot users do not address the bot.
  - Downstream admission, size cap, rate limits, claims, ACK reactions (👀), and edit handling apply identically.
- **Relay Acknowledgement & Late Answers (M9):**
  - **ACK Reaction (👀):** When an addressed message (`addressed: true`) is accepted by xmsg, the bot reacts to the event with 👀 (`m.reaction`) to signal work has begun. Unaddressed follows and dropped/refused messages receive no reaction.
  - **Late Answers & Deadline:** If the expert reply does not arrive within `answer_timeout_secs` (default 300s), an owner DM is sent, but the bot continues waiting up to `answer_deadline_secs` (default 3600s). If the reply arrives before the deadline, it is posted to the thread as normal. Past the deadline, the bot stops waiting and posts a user-facing timeout notice.
- **Admission Modes:**
  - `trusted` (default): Messages from non-allowlisted senders are dropped **silently**. The bot never responds or sends an error notice, preventing oracle attacks where attackers probe for valid allowlisted usernames.
  - `public`: Any room member's top-level question is relayed to the expert session; in engaged threads, the original thread asker and trusted senders are admitted, while bystanders are silently ignored.
- **Edit Mentions & De-duplication (M10, F3, M14):**
  - Edits (`m.replace`) are ignored by default (F3) to prevent re-triggering questions that have already been answered or relayed.
  - An edit is handled as a trigger iff:
    1. Its replacement content (`m.new_content`) mentions the bot (`is_bot_mentioned`) or turns the message into a reply to the bot,
    2. The original event did not mention or reply to the bot, and
    3. The original event ID has never been relayed (claims are keyed on the original event ID, not the edit's).
  - The replacement content is evaluated against all standard gates (room allowlist, admission where the edit sender must match the original sender, size cap, rate limit, and backlog cutoff on the edit's own timestamp).
  - Thread placement, context history, and the relay acknowledgement reaction (👀) attach to the original event ID.
  - If the original event is not in the bot's store/history, it is fetched via the Matrix client; if it cannot be read, the edit is dropped (fail closed) and logged at debug.

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
- **Thread Resync (`{"resync": true}`) (M17):** If an agent session loses epistemic context (such as following harness auto-compaction), it may re-request a thread's full history by replying with `{"resync": true}`. The bot never posts this control to the Matrix room; instead, it responds with a bounded transcript envelope containing the full thread history and room background, throttled to at most once per thread per 60 seconds.
- **Backward Compatibility:** Plain-text replies with no JSON structure render byte-identically to legacy outputs without confidence or control hint lines.

### 1.4 Context Framing: Bootstrap & Delta Contexts (M5d, M11)

- **Bootstrap vs. Delta per Root:**
  - A top-level trigger is its own root. The bot records in SQLite (`thread_cursors`) the cursor (last forwarded event ID) and timestamp of the forward per thread root.
  - A forward is a **`BOOTSTRAP`** when the root has no recorded cursor or the last forward is older than `session_live_secs` (default `3600`, 1 hour live window). Otherwise, it is a **`DELTA`**.
  - If a delta forward reaches a receiving session whose `sessionId` differs from the cursor's recorded session (e.g. after expert session restart), the bot immediately sends a bootstrap to that session with `supersedes="<delta message id>"` on its context block, and advances the cursor to the new session id once the bootstrap succeeds; rows lacking a session id re-bootstrap once. The agent should answer only the bootstrap and ignore the superseded message; the bot records both message IDs against the thread so replies to either are attributed to the thread.
  - On a failed send (`xmsg` refused), the cursor does not advance, ensuring subsequent forwards re-carry context.
- **Two Blocks for Threaded Bootstrap:**
  - When a threaded trigger is bootstrapped, it carries:
    1. A room background block of recent room messages prior to the thread root: `<context kind="channel" role="background" context="bootstrap" note="...">`.
    2. A thread block of the thread's messages so far: `<context kind="thread" role="thread" context="bootstrap" note="...">`.
  - Both blocks obey `history_byte_cap` together: oldest room lines are dropped first, and thread lines are only dropped if all room lines have been dropped and the total size still exceeds the cap.
- **Delta Context:**
  - A `DELTA` carries only the lines after the root's cursor (thread lines for threads, room lines for top-level): `<context kind="thread" role="thread" context="delta" note="...">`.
  - Deltas carry bystander lines (M8) that arrived between forwards. No room background block is emitted in deltas.
  - The cursor advances to the trigger event ID only after `xmsg.send_message` succeeds.
- **Envelope & Provenance:**
  - Header format: `[matrix] room={room_id} thread={thread_root_id} user={mapped_sender} ({trigger_tier})`.
  - Per design §5.1, thread tier folding belongs to the supervisor (C7) from the per-line `tier` tags, so `thread_tier` is not carried in the envelope header in any mode. The header carries the trigger's own user tier (`({trigger_tier})`).
  - Senders in `trusted_mxids` or matching `owner_mxid` receive tier `trusted`; all others receive `public`.
  - Each line within `<request>` and `<context>` is an authenticated JSON object:
    ```json
    {"sender": "matrix alice at example.org", "tier": "trusted", "text": "message text"}
    ```
  - XML escaping: `<request>`, `</request>`, `<context`, and `</context>` tags occurring inside user-supplied message text are escaped (`<\request`, `<\context`) prior to JSON serialization, preventing sandbox breakouts or fake block imitation.

### 1.5 Svc Inbox & Reply Contract (M13, M17)

- **The Bot as `svc:matrix-xmsg`:** On startup, the bot registers on xmsg's `register.sock` as `svc:matrix-xmsg`. Every forward is sent attested on `agent.sock` (`action: "send"`, `push_replies: true`), routing subsequent replies directly to the bot's service inbox.
- **Posting Every Reply (No Deadline):** Every reply arriving in the service inbox is posted to the Matrix thread in order. An agent may follow up by replying again; each reply is posted.
- **Silent Decline (`{"silent": true}`):** If an agent replies with `{"silent": true}`, the bot posts nothing to the room, redacts its original `👀` reaction, and reacts with `🫡`.
- **Thread Resync & Compaction Recovery (`{"resync": true}`) (M17):** An agent session recovering from auto-compaction or context loss may re-request the thread's complete transcript by replying with `{"resync": true}`. The bot treats this as a control (never posted to Matrix) and replies on xmsg with a bootstrap-formatted transcript bounded by `resync_byte_cap` (dropping room background lines before thread lines). The thread cursor advances to the newest line sent with the requesting session ID. Resync requests are rate-limited to at most one per thread per 60 seconds (throttled requests receive `{"resync": "throttled", "retry_after": <s>}`); unknown messages receive `{"resync": "unknown"}`.
- **Delivery & Ack Guarantees:** A reply is acknowledged (`ack`) on `register.sock` only after its Matrix notice post succeeds (or resync reply is dispatched). Crashes or failures cause re-delivery rather than lost replies.
- **Answer Timeout:** If no reply arrives within `answer_timeout_secs`, the bot sends an owner DM notification once. This state is tracked in the SQLite store and survives bot restarts. (`answer_deadline_secs` is retired).
- **Session Changes:** A forward's re-bootstrap envelope carries `supersedes="<delta message id>"` on its context block. The receiving agent should answer only the bootstrap and ignore the superseded message; the bot records both message IDs so a reply to either is attributed to the thread.

### 1.6 Strict ASCII Sender Mapping

Matrix IDs (`@alice:example.org`) are converted to sanitized ASCII identifiers (`matrix alice at example.org`):

- Never contains `:` or `/` (conforming to xmsg HTTP sender invariants).
- Strips non-ASCII characters.
- Enforces a 64-character length cap.

### 1.7 Solicited Threads & Guard Integration (M15)

A local session on the xmsg bus can solicit Matrix threads by asking the bot to open a thread on its behalf:

- **Opening a Thread (`open_thread`):** A message delivered to the bot's service inbox carrying `{"open_thread": {"room": "<room_id>", "text": "<text>"}}` initiates thread creation.
  - **Local Origin Enforcement:** The request must carry an xmsg origin with `kind: "local"`. Messages from any other origin (or lacking local origin) are refused with an error reply and post nothing to Matrix.
  - **Room Allowlist & Size Cap:** The requested `room` must be present in `config.rooms` and the text is capped at `size_cap_bytes`. Non-allowlisted rooms are refused with an error reply.
  - **Opening Post:** The bot posts top-level in the requested room: `On behalf of @<localpart>: <text>`, where `<localpart>` is parsed from `config.owner_mxid`.
  - **Binding & Reply:** The bot records `(root_event_id, room_id, request_message_id, opened_at)` in the SQLite `solicited_threads` table and replies to `request_message_id` with `{"root": "<root_event_id>", "permalink": "https://matrix.to/#/<room_id>/<root_event_id>"}`.
- **Bound Thread Routing & Guard Pipeline:**
  - Admitted lines occurring inside a bound thread are routed to the requesting session via `reply_message` to `request_message_id` (rather than `expert_ref`).
  - Prior to relaying, each line is sent to `config.guard_ref` (`svc:genie-guard`) with `push_replies: false`. The bot long-polls `http.sock` at `/v1/messages/{id}/replies?wait=...` bounded by `guard_timeout_secs` (default 150s) to receive the guard's reply.
  - **Guard Verdicts & Strict Parsing:**
    - `allow`: Relays the original envelope text.
    - `rewrite`: Relays `cleaned_text` with `"rewritten": true` in the context line.
    - `reject`: Relays nothing and drops the line.
    - `error / timeout / invalid`: Any error response (`{"error": ...}`), timeout, unparseable JSON, or unexpected verdict fails closed and retries up to `guard_retry_budget` (default 3). If retries are exhausted, sends a single DM to the owner with the thread permalink.
- **Session Replies:** Replies from the requesting session to a relayed line land in the bot's service inbox and are posted to Matrix inside the bound thread as a notice mentioning the asker.
- **Releasing a Thread (`!release`):** Replying `!release` to the opening post from `owner_mxid` unbinds the thread (`solicited_threads` row removed). Subsequent messages fall back to standard routing (`expert_ref`). `!release` from non-owner senders is ignored.
- **Dead Session Push Failure:** If pushing a relayed line fails because the requesting session is dead, the bot DMs the owner once with the thread permalink and continues relaying.

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
xmsg_socket = "/run/user/1000/xmsg"
expert_ref = "claude"
guard_ref = "svc:genie-guard"
guard_timeout_secs = 150
guard_retry_budget = 3
history_n = 30
history_byte_cap = 12288
rate_limit_count = 10
rate_limit_window_secs = 600
size_cap_bytes = 4096
answer_timeout_secs = 300
session_live_secs = 3600
resync_byte_cap = 65536
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
    xmsgSocket = "/run/user/1000/xmsg";
    expertRef = "claude";
    dynamicUser = false;
    user = "sini";
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

| Option                | Type                       | Default                        | Description                                                                                                                                                                                           |
| --------------------- | -------------------------- | ------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `enable`              | `bool`                     | `false`                        | Enable the matrix-xmsg daemon.                                                                                                                                                                        |
| `package`             | `package`                  | `matrix-xmsg`                  | Package to run.                                                                                                                                                                                       |
| `homeserverUrl`       | `str`                      | *(required)*                   | Base URL of the Matrix homeserver.                                                                                                                                                                    |
| `botMxid`             | `str`                      | *(required)*                   | Full Matrix user ID of the bot (`@name:server`).                                                                                                                                                      |
| `accessTokenFile`     | `path`                     | *(required)*                   | Path to file containing the Matrix access token.                                                                                                                                                      |
| `rooms`               | `listOf str`               | `[]`                           | Room IDs (`!id:server`) monitored by the bot. Aliases (`#alias:server`) are rejected at eval time.                                                                                                    |
| `trustedMxids`        | `listOf str`               | `[]`                           | Allowlist of user Matrix IDs permitted to interact with the bot.                                                                                                                                      |
| `ownerMxid`           | `str`                      | *(required)*                   | Owner Matrix ID for escalations and direct notices.                                                                                                                                                   |
| `stateDir`            | `path`                     | `"/var/lib/matrix-xmsg"`       | State directory for persistent store and database.                                                                                                                                                    |
| `xmsgSocket`          | `nullOr (either path str)` | `"/run/user/1000/xmsg"`        | Path to xmsg runtime directory containing register.sock and agent.sock.                                                                                                                               |
| `expertRef`           | `str`                      | `"claude"`                     | Session name/ref in xmsg to route queries to.                                                                                                                                                         |
| `historyN`            | `uint`                     | `30`                           | Number of context messages fetched from timeline.                                                                                                                                                     |
| `historyByteCap`      | `uint`                     | `12288`                        | Maximum byte size of context history window.                                                                                                                                                          |
| `rateLimitCount`      | `uint`                     | `10`                           | Max queries permitted per user per window.                                                                                                                                                            |
| `rateLimitWindowSecs` | `uint`                     | `600`                          | Sliding window duration in seconds.                                                                                                                                                                   |
| `sizeCapBytes`        | `uint`                     | `4096`                         | Max body size of queries accepted.                                                                                                                                                                    |
| `answerTimeoutSecs`   | `uint`                     | `300`                          | Timeout before escalating to owner via DM.                                                                                                                                                            |
| `sessionLiveSecs`     | `uint`                     | `3600`                         | Duration in seconds before an idle thread/session context is re-bootstrapped.                                                                                                                         |
| `resyncByteCap`       | `uint`                     | `65536`                        | Maximum byte size of resync transcript payload.                                                                                                                                                       |
| `dbPath`              | `path`                     | `"${stateDir}/matrix-xmsg.db"` | Path to SQLite database (created mode `0600`).                                                                                                                                                        |
| `dynamicUser`         | `bool`                     | `false`                        | Must be false when accessing user-owned xmsg sockets.                                                                                                                                                 |
| `user`                | `str`                      | `"matrix-xmsg"`                | User owning the running daemon (matches xmsg peer UID).                                                                                                                                               |
| `group`               | `nullOr str`               | `null`                         | Dedicated system group (used when dynamicUser is false). When null and user is matrix-xmsg, defaults to matrix-xmsg. For any other user, systemd uses the user's primary group unless explicitly set. |

### 3.4 Systemd Hardening & Lifetime Guarantees

- **Strict Isolation:** `ProtectSystem = strict`, `ProtectHome = "tmpfs"` with `BindPaths = [ cfg.xmsgSocket ]` (or `ProtectHome = true` when no socket is configured), `PrivateUsers = false` when accessing user-owned xmsg sockets (allowing svc attestation against `/proc/<pid>/exe`), `PrivateTmp = true`, `PrivateDevices = true`, `ProtectKernelTunables = true`, `ProtectControlGroups = true`, `NoNewPrivileges = true`, `RestrictNamespaces = true`, `RestrictAddressFamilies = AF_INET AF_INET6 AF_UNIX`, `UMask = 0077`.
- **State Persistence:** `StateDirectory = "matrix-xmsg"` provisions `/var/lib/matrix-xmsg` owned by the service user. The SQLite database is created mode `0600` via `Store::new`.
- **Bounded Shutdown:** `TimeoutStopSec = 60` provides generous margin above the 5-second in-flight drain and 5-second shutdown notice bounds.
- **Boot Synchronization & Failure Retry:** The systemd unit sets `Restart = "on-failure"` with `RestartSec = "5s"`. Because `/run/user/1000` exists only under the user's lingering user manager session, registration on `register.sock` can fail at early system boot before the user session initializes. When registration fails, the bot logs the failure and exits non-zero (`std::process::exit(1)`), causing systemd to automatically retry every 5 seconds until `register.sock` becomes available.

### 3.5 xmsg Attestation & Svc Harness

The bot runs as a non-DynamicUser service (e.g. `user = "sini"`), connecting directly to xmsg's runtime directory (`xmsgSocket`, typically `/run/user/1000/xmsg`). When `user` is set to an existing host account (anything other than the default `"matrix-xmsg"`), the module does not declare `users.users` or `users.groups`, avoiding collision with the user's existing home directory and attributes, and defaults `serviceConfig.Group` to the user's primary group unless `group` is explicitly specified.

On startup, the bot registers on `register.sock` as `svc:matrix-xmsg`. xmsg verifies the caller's peer UID and validates the executable against `--svc-exe matrix-xmsg=<path>`. Forwards are sent attested on `agent.sock` (`push_replies: true`), routing subsequent replies directly into the bot's service inbox loop.

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
