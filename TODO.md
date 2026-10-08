# TODO: matrix-xmsg Roadmap & Work in Progress

## Unit M1: Bot Core Scaffold (LANDED & CLOSED)
- [x] Matrix event payload models, context extraction, and XML escaping.
- [x] SQLite message persistence and sender mapping.
- [x] Gated against mock client in `tests/gate_m1.rs`.

## Unit M2: Real Matrix SDK Integration & Event Loop (LANDED & CLOSED)
- [x] Real `MatrixClient` backed by `matrix-sdk` client with token restoration (`MatrixSdkClient`).
- [x] Live homeserver event sync loop (`matrix_sdk::Client::sync` / event handlers).
- [x] Room and thread history fetching from homeserver timeline (`room.messages()`).
- [x] Live message and notice posting to Matrix rooms with thread relations and user mentions.
- [x] Gated against mock homeserver via `wiremock` in `tests/gate_m2.rs` (28/28 tests passing).
- [x] M2.1 repairs: backlog suppression on initial sync & restart, non-blocking spawned handlers, edit refusal, thread body assertion, context newline collapsing, and DM room reuse.
- [x] M2.2 repairs: bounded shutdown drain, direct-send DM room cache recovery, sync gating, and unicode line separator sanitization.
- [x] M2.3 repairs: server DM validation, atomic reply claim, and bounded shutdown. Verified independently and accepted at `d4870bf`.

## Unit M3: NixOS Module Packaging & VM Test (COMPLETED on branch `m3`)
- [x] NixOS module exported as `nixosModules.default` under option namespace `services.matrix-xmsg`.
- [x] Systemd system service with `DynamicUser` (or dedicated user), `StateDirectory=matrix-xmsg` (mode `0700` with mode `0600` DB), and `LoadCredential=` for token path.
- [x] Hardening: `ProtectSystem=strict`, `NoNewPrivileges=true`, `PrivateTmp=true`, `PrivateDevices=true`, `RestrictAddressFamilies`, `UMask=0077`, `TimeoutStopSec=60`.
- [x] NixOS VM test implemented as `checks.<system>.nixos-module` via `pkgs.testers.nixosTest`.
  - Boots against stub Matrix homeserver (port 8008) and stub xmsg server (port 7787).
  - Asserts service starts, loads token from credential, creates DB `0600` under stateDir, answers 1 trusted mention in-thread, and cleanly deactivates within `TimeoutStopSec`.
  - Verified green under timeout (runtime: ~10s).
- [x] Mutant 1 (`checks.<system>.nixos-module-mutant-missing`) verifying RED failure when token path is invalid (`status=243/CREDENTIALS` exit).
- [x] Mutant 2 (`checks.<system>.nixos-module-mutant-wrong-token`, Condition M3.1) verifying Bearer token authentication in mock homeserver, 401 `M_UNKNOWN_TOKEN` on unauthorized token, reply suppression, and 3s retry backoff.
- [x] xmsg attestation evaluation: analyzed `xmsg` socket permissions (`0600`/`0700`), peer UID check (`peer_cred`), and ancestor process walk. Documented findings and minimal xmsg evolution path.
- [x] Documentation updated in `README.md` (deployment guide, options table, token provisioning) and `TODO.md`.

## Next Steps: Live Host Run (Owner / Claude against matrix.json64.dev)
- [ ] Provision Matrix account `@genie:json64.dev` on `matrix.json64.dev` and extract access token.
- [ ] Provision token secret in `nix-config` via `agenix` (e.g. `/run/agenix/matrix-genie-token`).
- [ ] Import `matrix-xmsg.nixosModules.default` in host configuration and enable `services.matrix-xmsg`.
- [ ] Join `@genie:json64.dev` to target support room, verify trusted mention response and escalation to owner DM.
