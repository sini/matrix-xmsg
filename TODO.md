# TODO: matrix-xmsg Roadmap & Work in Progress

## Unit M2: Real Matrix SDK Integration & Live Daemon Loop (IN PROGRESS - Unit M2.1 Repairs)
- [x] Implement real `MatrixClient` backed by `matrix-sdk` client with token restoration (`MatrixSdkClient`).
- [x] Implement live homeserver event sync loop (`matrix_sdk::Client::sync` / event handlers).
- [x] Room and thread history fetching from homeserver timeline (`room.messages()`).
- [x] Live message and notice posting to Matrix rooms with thread relations and user mentions.
- [x] Gated against mock homeserver via `wiremock` in `tests/gate_m2.rs`.
- [x] M2.1 repairs: backlog suppression on initial sync & restart, non-blocking spawned handlers, edit refusal, thread body assertion, context newline collapsing, and DM room reuse.
- [ ] Live homeserver verification: pending verification against `matrix.json64.dev` and live bot registration by owner.


## Unit M3: Deployment & Host Integration
- Packaging under `nix-config` for dedicated `support` user.
- Systemd user services with linger enabled.
- Agenix secret management for access token file.
- Integration tests against conduit/homeserver instance.
