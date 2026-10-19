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
                # Subsequent syncs: block slightly for long poll, return empty
                time.sleep(1)
                resp = {
                    "next_batch": "s3",
                    "rooms": {}
                }

            self.wfile.write(json.dumps(resp).encode())
            return

        if "/messages" in path:
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({
                "chunk": [],
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
            with open("/tmp/matrix_reply_received.json", "wb") as f:
                f.write(body)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"event_id": "$reply_ev_999"}).encode())
            return

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b"{}")

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

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b"{}")


SOCK_DIR = "/run/xmsg-mock"
NODE = "test"
pushed = queue.Queue()
send_ids = itertools.count(1)


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
        for line in self.rfile:
            req = json.loads(line)
            if req.get("action") == "poll":
                try:
                    frame = pushed.get(timeout=min(int(req.get("waitSecs", 1)), 1))
                    sys.stderr.write(f"[xmsg-mock] deliver {frame['messageId']}\n")
                except queue.Empty:
                    frame = {"action": "timeout"}
                write_frame(self.wfile, frame)
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
