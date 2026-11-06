# Shared xmsg client migration

Matrix uses `xmsg::client` from revision
`0d9ee145f7875aa4b930b4efd41615bdcfd1f806`, with default features disabled.
Registration, inbound polling, expert sends and guard sends share one registered
service identity. Reconnection replaces the outbound handle held by that service.
Matrix retains its guard, sender binding, retry, thread and cursor policies.

The frozen transport trace comes from Matrix revision
`d835c7254e5197d05f9ca3a1520740867b3c35ee`. The recorder and its lockfile live in
`tests/fixtures`. The parity test compares accepted frames and cursor decisions,
including the anonymous sender display name. Agent requests additionally carry
schema version 2. Registration frames do not carry that field.

The shared client rejects oversized frames, expired operations, malformed origins,
invalid socket ownership and inconsistent daemon identities. Matrix accepts the
UID-verified handshake node without a configured node pin. It never invents an
address when the daemon omits one.

## Reply outcomes

`ReplyOutcome` distinguishes pushed replies from stored replies. Both are accepted
operations. A stored reply can already have its eventual delivery ID in
`pushed_message_id`; Matrix records its thread association under that ID, so a
later response returns to the originating Matrix thread. An accepted reply without
that ID creates no association. `ReplyRecord.message_id` identifies the original
request and is never substituted for a new reply ID. Removing that substitution
is a deliberate correction to the previous fallback behavior.

## Validation

The native suite passes 276 tests across 42 targets. Five real-daemon tests remain
ignored in the native command and must run in CI with `MA_XMSG_REQUIRED=1`.
The CI integration arms use the current shared-client daemon revision and the
previous Matrix baseline `cca3de77fbe95ea1ad90ab8ba7a83eb741fc9cb9`.
Their binaries are built from revision-named, hash-checked sources, embed their
revision, and must reject the other arm's binary. Manifest and lockfile pin checks
have independent negative controls.

`scripts/shared_client_mutants.py` exercises seven single-site source changes.
Each case requires a passing baseline and a failure in the named test after the
edit. Compilation failures and timeouts do not count as mutant kills. Mutation
results and full CI remain pending until their runs complete.
