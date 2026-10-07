# TODO: matrix-xmsg Roadmap & Work in Progress

## Unit M2: Real Matrix SDK Integration & Live Daemon Loop (COMPLETE)
- [x] Implement real `MatrixClient` backed by `matrix-sdk` client with token restoration (`MatrixSdkClient`).
- [x] Implement live homeserver event sync loop (`matrix_sdk::Client::sync` / event handlers).
- [x] Room and thread history fetching from homeserver timeline (`room.messages()`).
- [x] Live message and notice posting to Matrix rooms with thread relations and user mentions.
- [x] Verified end-to-end via mock homeserver (`wiremock`) in `tests/gate_m2.rs` (4/4 tests passing in CI).


## Unit M3: Deployment & Host Integration
- Packaging under `nix-config` for dedicated `support` user.
- Systemd user services with linger enabled.
- Agenix secret management for access token file.
- Integration tests against conduit/homeserver instance.
