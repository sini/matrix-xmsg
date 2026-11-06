# Legacy xmsg transport trace

`xmsg-transport-d835c725.json` was recorded from matrix-xmsg revision
`d835c7254e5197d05f9ca3a1520740867b3c35ee` before removing its transport.
It records actual registration, poll, ACK, keyed send and reply frames, delivery
fields and stored thread cursor principals. The recording metadata binds the
source files and capture lock by SHA-256. A locked second capture was identical.

Reproduce with Cargo and a C linker available:

```sh
python3 tests/fixtures/record_xmsg_legacy.py . tests/fixtures/xmsg-transport-d835c725.json
```

The recorder reads the pinned Git revision, compiles its unchanged transport,
store and error modules with a small Config shim, and uses its verbatim bot
principal functions and cursor expression. The capture uses private fake sockets,
not a Matrix server or a live xmsg node. The current parity test uses the actual
Matrix adapter and cursor policy. Only the newly required outgoing agent
`schema_version` is excluded from the legacy trace comparison.
