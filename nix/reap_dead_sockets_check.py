"""Run the reaper over a dead socket, a live one, a regular file and a missing path."""

import os
import socket
import subprocess
import sys
import tempfile

reaper = sys.argv[1]
d = tempfile.mkdtemp()
dead, live, plain, missing = (os.path.join(d, n) for n in ("dead.sock", "live.sock", "plain.sock", "missing.sock"))

s = socket.socket(socket.AF_UNIX)
s.bind(dead)
s.close()  # leaves the socket file with no listener
listener = socket.socket(socket.AF_UNIX)
listener.bind(live)
listener.listen(8)
open(plain, "w").close()

subprocess.run([reaper, dead, live, plain, missing], check=True)

state = {p: os.path.exists(p) for p in (dead, live, plain, missing)}
assert state == {dead: False, live: True, plain: True, missing: False}, state
print("reap-dead-sockets: dead removed; live, regular file and missing path untouched")
