# TODO: matrix-xmsg Roadmap & Work in Progress

## Unit M2: Real Matrix SDK Integration & Live Daemon Loop
- Implement real `MatrixClient` backed by `matrix-sdk` client.
- Implement live homeserver event sync loop (`matrix_sdk::Client::sync` / event handlers).
- Room and thread history fetching from homeserver timeline.
- Live message and notice posting to Matrix rooms.
- *Dependencies:* Waits on the owner's homeserver deployment (spec: https://gist.github.com/sini/9ac23cc825d4b7445f62e2401b81ccfe) and registration of the live bot account.

## Unit M3: Deployment & Host Integration
- Packaging under `nix-config` for dedicated `support` user.
- Systemd user services with linger enabled.
- Agenix secret management for access token file.
- Integration tests against conduit/homeserver instance.
