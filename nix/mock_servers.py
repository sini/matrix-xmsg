#!/usr/bin/env python3
import http.server
import itertools
import json
import os
import queue
import socketserver
import sys
import threading
import time
from urllib.parse import urlparse, parse_qs

VALID_TOKEN = "syt_valid_test_token_12345"

class MatrixHandler(http.server.BaseHTTPRequestHandler):
    sync_count = 0
    inject_line_idx = 0
    batch_seq = 3
    reply_seq = 1
    lock = threading.Lock()

    def log_message(self, format, *args):
        sys.stderr.write(f"[matrix-mock] {format % args}\n")

    def check_auth(self):
        auth = self.headers.get("Authorization", "")
        if auth != f"Bearer {VALID_TOKEN}":
            sys.stderr.write(f"[matrix-mock] rejecting unauthorized request (auth: '{auth}')\n")
            self.send_response(401)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({
                "errcode": "M_UNKNOWN_TOKEN",
                "error": "Invalid or missing access token"
            }).encode())
            return False
        return True

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query)

        # Versions endpoint is public/unauthenticated in Matrix spec
        if path == "/_matrix/client/versions":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({
                "versions": [
                    "v1.1", "v1.2", "v1.3", "v1.4", "v1.5",
                    "v1.6", "v1.7", "v1.8", "v1.9", "v1.10", "v1.11"
                ]
            }).encode())
            return

        # All other endpoints require valid Bearer token
        if not self.check_auth():
            return

        if path == "/_matrix/client/v3/account/whoami":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({
                "user_id": "@bot:test.local",
                "device_id": "MOCK_BOT_DEVICE"
            }).encode())
            return

        if "/state/m.room.encryption" in path:
            self.send_response(404)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({
                "errcode": "M_NOT_FOUND",
                "error": "Event not found."
            }).encode())
            return

        if path == "/_matrix/client/v3/sync":
            since = qs.get("since", [None])[0]
            with MatrixHandler.lock:
                count = MatrixHandler.sync_count
                MatrixHandler.sync_count += 1

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()

            if not since:
                # Initial sync: establish room membership
                resp = {
                    "next_batch": "s1",
                    "rooms": {
                        "join": {
                            "!test:test.local": {
                                "timeline": {
                                    "events": [],
                                    "prev_batch": "s0"
                                }
                            },
                            "!dm_owner:test.local": {
                                "timeline": {
                                    "events": [],
                                    "prev_batch": "s0"
                                }
                            }
                        }
                    }
                }
            elif since == "s1":
                # Second sync: deliver 1 trusted mention event
                now_ms = int(time.time() * 1000)
                resp = {
                    "next_batch": "s2",
                    "rooms": {
                        "join": {
                            "!test:test.local": {
                                "timeline": {
                                    "events": [
                                        {
                                            "type": "m.room.message",
                                            "sender": "@trusted:test.local",
                                            "content": {
                                                "msgtype": "m.text",
                                                "body": "@bot:test.local ping",
                                                "m.mentions": {
                                                    "user_ids": ["@bot:test.local"]
                                                }
                                            },
                                            "event_id": "$ev_trigger_01",
                                            "origin_server_ts": now_ms
                                        }
                                    ],
                                    "prev_batch": "s1"
                                }
                            }
                        }
                    }
                }
            else:
                injected = []
                with MatrixHandler.lock:
                    if os.path.exists("/tmp/matrix_inject_events.jsonl"):
                        with open("/tmp/matrix_inject_events.jsonl", "r", encoding="utf-8") as inf:
                            lines = [ln.strip() for ln in inf.readlines() if ln.strip()]
                        if MatrixHandler.inject_line_idx < len(lines):
                            now_base = int(time.time() * 1000)
                            for idx_offset, raw in enumerate(lines[MatrixHandler.inject_line_idx:]):
                                ev = json.loads(raw)
                                ev["origin_server_ts"] = now_base + idx_offset
                                injected.append(ev)
                            MatrixHandler.inject_line_idx = len(lines)
                            next_b = f"s{MatrixHandler.batch_seq}"
                            MatrixHandler.batch_seq += 1
                if injected:
                    resp = {
                        "next_batch": next_b,
                        "rooms": {
                            "join": {
                                "!test:test.local": {
                                    "timeline": {
                                        "events": injected,
                                        "prev_batch": since,
                                    }
                                }
                            }
                        },
                    }
                else:
                    # Subsequent syncs: block slightly for long poll, return empty
                    time.sleep(0.2)
                    resp = {
                        "next_batch": since,
                        "rooms": {}
                    }

            self.wfile.write(json.dumps(resp).encode())
            return

        if "/messages" in path:
            chunk = []
            if os.environ.get("MOCK_MATRIX_ONLY") == "1":
                chunk = [
                    {
                        "type": "m.room.message",
                        "sender": "@stranger:test.local",
                        "content": {
                            "msgtype": "m.text",
                            "body": "untrusted room context line for m38",
                        },
                        "event_id": "$ev_untrusted_hist_01",
                        "origin_server_ts": int(time.time() * 1000) - 5000,
                    }
                ]
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({
                "chunk": chunk,
                "start": "s0",
                "end": "s1"
            }).encode())
            return

        # Default fallback for other GETs (members, state, capabilities, etc.)
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b"{}")

    def do_PUT(self):
        if not self.check_auth():
            return

        parsed = urlparse(self.path)
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length) if length > 0 else b""

        if "/send/m.room.message" in parsed.path:
            try:
                body_obj = json.loads(body.decode("utf-8"))
            except Exception:
                body_obj = {}
            msg_body = body_obj.get("body", "")

            if "%21dm_owner" in parsed.path or "!dm_owner" in parsed.path:
                with MatrixHandler.lock:
                    seq = MatrixHandler.reply_seq
                    MatrixHandler.reply_seq += 1
                    with open("/tmp/matrix_dms.jsonl", "a", encoding="utf-8") as f:
                        f.write(json.dumps(body_obj, ensure_ascii=False) + "\n")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"event_id": f"$dm_ev_{seq}"}).encode())
                return

            # Cell (a) hook: hold first post of m18 reply while xmsg restarts, then fail once
            if "m18 reply across xmsg restart" in msg_body and os.path.exists("/tmp/m18_fail_first_post"):
                try:
                    os.remove("/tmp/m18_fail_first_post")
                except FileNotFoundError:
                    pass
                with open("/tmp/m18_delivery_in_flight", "w", encoding="utf-8") as f:
                    f.write("in_flight\n")
                deadline = time.monotonic() + 15.0
                while time.monotonic() < deadline and not os.path.exists("/tmp/m18_xmsg_restarted"):
                    time.sleep(0.05)
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "errcode": "M_UNKNOWN",
                    "error": "simulated transient failure during xmsg restart",
                }).encode())
                return

            # Cell (b) hook: hold slow answer for thread A until unblocked
            if "slow answer for thread A" in msg_body:
                with open("/tmp/m38_thread_a_blocked", "w", encoding="utf-8") as f:
                    f.write("blocked\n")
                deadline = time.monotonic() + 30.0
                while time.monotonic() < deadline and not os.path.exists("/tmp/m38_unblock_thread_a"):
                    time.sleep(0.05)

            with MatrixHandler.lock:
                seq = MatrixHandler.reply_seq
                MatrixHandler.reply_seq += 1
                with open("/tmp/matrix_reply_received.json", "wb") as f:
                    f.write(body)
                with open("/tmp/matrix_replies.jsonl", "a", encoding="utf-8") as f:
                    f.write(json.dumps(body_obj, ensure_ascii=False) + "\n")
                if "slow answer for thread A" in msg_body:
                    with open("/tmp/m38_thread_a_posted", "w", encoding="utf-8") as f:
                        f.write("posted\n")
                if "fast answer for thread B" in msg_body:
                    with open("/tmp/m38_thread_b_posted", "w", encoding="utf-8") as f:
                        f.write("posted\n")

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"event_id": f"$reply_ev_{seq}"}).encode())
            return

        if "/send/m.reaction" in parsed.path:
            try:
                react_obj = json.loads(body.decode("utf-8"))
            except Exception:
                react_obj = {}
            with MatrixHandler.lock:
                seq = MatrixHandler.reply_seq
                MatrixHandler.reply_seq += 1
                with open("/tmp/matrix_reactions.jsonl", "a", encoding="utf-8") as f:
                    f.write(json.dumps(react_obj, ensure_ascii=False) + "\n")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"event_id": f"$react_ev_{seq}"}).encode())
            return

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({"event_id": "$generic_ev_999"}).encode())

    def do_POST(self):
        if not self.check_auth():
            return

        parsed = urlparse(self.path)
        path = parsed.path
        length = int(self.headers.get("Content-Length", 0))
        if length > 0:
            _ = self.rfile.read(length)

        if path == "/_matrix/client/v3/keys/query":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({
                "device_keys": {},
                "master_keys": {},
                "self_signing_keys": {},
                "user_signing_keys": {},
                "failures": {}
            }).encode())
            return

        if path == "/_matrix/client/v3/keys/upload":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({
                "one_time_key_counts": {"signed_curve25519": 50}
            }).encode())
            return

        if path == "/_matrix/client/v3/keys/signatures/upload":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"failures": {}}).encode())
            return

        if "/join" in path:
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"room_id": "!test:test.local"}).encode())
            return

        if "/createRoom" in path:
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"room_id": "!dm_owner:test.local"}).encode())
            return

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b"{}")


SOCK_DIR = "/run/xmsg-mock"
NODE = "test"
send_ids = itertools.count(1)


class MockSvcInbox:
    """Shared svc inbox queue matching real xmsg at cca3de7+: deliveries remain in
    _queue until acked, in-flight messageIds are tracked per connection (XF6), and
    unacked deliveries are redelivered on reconnect."""

    def __init__(self):
        self._cond = threading.Condition()
        self._queue = []
        self.acks = []

    def put(self, frame):
        with self._cond:
            self._queue.append(frame)
            self._cond.notify_all()

    def poll(self, in_flight, timeout):
        deadline = time.monotonic() + timeout
        with self._cond:
            while True:
                for frame in self._queue:
                    msg_id = frame.get("messageId")
                    if msg_id not in in_flight:
                        in_flight.add(msg_id)
                        return frame
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                self._cond.wait(timeout=remaining)

    def ack(self, msg_id, in_flight):
        with self._cond:
            if msg_id:
                self.acks.append(msg_id)
                self._queue = [f for f in self._queue if f.get("messageId") != msg_id]
                in_flight.discard(msg_id)
            self._cond.notify_all()


pushed = MockSvcInbox()


def write_frame(wfile, frame):
    wfile.write((json.dumps(frame) + "\n").encode())
    wfile.flush()


class AgentHandler(socketserver.StreamRequestHandler):
    """agent.sock: one request per connection. A `send` is recorded and answered with a
    message ID, and the expert's reply to it is pushed to the bot's inbox (`inReplyTo`) before
    the answer is written: the reply is there before the bot's send returns, and the bot must
    still post it."""

    def handle(self):
        line = self.rfile.readline()
        if not line:
            return
        req = json.loads(line)
        sys.stderr.write(f"[xmsg-mock] agent.sock {req.get('action')}\n")
        if req.get("action") == "send":
            n = next(send_ids)
            message_id = f"01TESTMSGID{n:05d}"
            to_addr = req.get("to", "")
            if to_addr.startswith("genie-guard"):
                pushed.put({
                    "action": "deliver",
                    "messageId": f"01TESTGUARD{n:05d}",
                    "fromName": f"genie-guard@{NODE}",
                    "text": json.dumps({
                        "verdict": "allow",
                        "reason": "clean",
                        "cleaned_text": "",
                    }),
                    "envelope": "",
                    "inReplyTo": message_id,
                })
                write_frame(self.wfile, {
                    "status": "ok",
                    "message_id": message_id,
                    "address": f"genie-guard@{NODE}",
                    "outcome": "delivered",
                })
            else:
                with open("/tmp/xmsg_query_received.json", "wb") as f:
                    f.write(line)
                reply_text = os.environ.get("MOCK_EXPERT_REPLY", "pong from expert")
                pushed.put({
                    "action": "deliver",
                    "messageId": f"01TESTREPLY{n:05d}",
                    "fromName": f"expert · mock-claude-session@{NODE}",
                    "text": reply_text,
                    "envelope": "",
                    "inReplyTo": message_id,
                })
                write_frame(self.wfile, {
                    "status": "ok",
                    "message_id": message_id,
                    "address": f"mock-claude-session@{NODE}",
                    "outcome": "delivered",
                })
        else:
            write_frame(self.wfile, {"status": "ok"})


class RegisterHandler(socketserver.StreamRequestHandler):
    """register.sock: the service registration, then poll/ack on the same connection."""

    def handle(self):
        line = self.rfile.readline()
        if not line:
            return
        name = json.loads(line).get("name", "")
        sys.stderr.write(f"[xmsg-mock] register.sock registered {name}\n")
        write_frame(self.wfile, {"status": "ok", "node": NODE, "address": f"{name}@{NODE}"})
        in_flight = set()
        for line in self.rfile:
            req = json.loads(line)
            action = req.get("action")
            if action == "poll":
                frame = pushed.poll(in_flight, timeout=min(int(req.get("waitSecs", 1)), 1))
                if frame is not None:
                    sys.stderr.write(f"[xmsg-mock] deliver {frame['messageId']}\n")
                else:
                    frame = {"action": "timeout"}
                try:
                    write_frame(self.wfile, frame)
                except (BrokenPipeError, ConnectionResetError):
                    if frame.get("action") == "deliver":
                        in_flight.discard(frame.get("messageId"))
                    break
            elif action == "ack":
                pushed.ack(req.get("messageId"), in_flight)
                write_frame(self.wfile, {"status": "ok"})
            else:
                write_frame(self.wfile, {"status": "ok"})


class ThreadedUnixServer(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True


def run_unix_server(name, handler):
    sock = os.path.join(SOCK_DIR, name)
    if os.path.exists(sock):
        os.remove(sock)
    server = ThreadedUnixServer(sock, handler)
    os.chmod(sock, 0o666)
    server.serve_forever()


class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True


def run_matrix_server():
    server = ThreadedTCPServer(("127.0.0.1", 8008), MatrixHandler)
    server.serve_forever()




if __name__ == "__main__":
    if os.environ.get("MOCK_XMSG_ONLY") != "1":
        t1 = threading.Thread(target=run_matrix_server, daemon=True)
        t1.start()
    if os.environ.get("MOCK_MATRIX_ONLY") == "1":
        print("Mock Matrix server listening on 127.0.0.1:8008 (Matrix only)")
    else:
        os.makedirs(SOCK_DIR, exist_ok=True)
        os.chmod(SOCK_DIR, 0o755)
        t2 = threading.Thread(target=run_unix_server, args=("register.sock", RegisterHandler), daemon=True)
        t3 = threading.Thread(target=run_unix_server, args=("agent.sock", AgentHandler), daemon=True)
        t2.start()
        t3.start()
        print(f"Mock servers listening on {SOCK_DIR} (xmsg)")
    sys.stdout.flush()
    while True:
        time.sleep(1)
