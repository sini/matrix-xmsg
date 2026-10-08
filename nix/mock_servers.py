#!/usr/bin/env python3
import http.server
import json
import socketserver
import sys
import threading
import time
from urllib.parse import urlparse, parse_qs

class MatrixHandler(http.server.BaseHTTPRequestHandler):
    sync_count = 0
    lock = threading.Lock()

    def log_message(self, format, *args):
        sys.stderr.write(f"[matrix-mock] {format % args}\n")

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query)

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
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b"{}")


class XmsgHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        sys.stderr.write(f"[xmsg-mock] {format % args}\n")

    def do_POST(self):
        parsed = urlparse(self.path)
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length) if length > 0 else b""

        if "/messages" in parsed.path:
            with open("/tmp/xmsg_query_received.json", "wb") as f:
                f.write(body)
            self.send_response(202)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({
                "sessionId": "mock-claude-session",
                "fromName": "bot",
                "bytes": len(body),
                "messageId": "01TESTMSGID12345"
            }).encode())
            return

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b"{}")

    def do_GET(self):
        parsed = urlparse(self.path)
        qs = parse_qs(parsed.query)
        after = int(qs.get("after", ["0"])[0])

        if "/replies" in parsed.path:
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            if after < 1:
                resp = [{
                    "seq": 1,
                    "text": "pong from expert",
                    "body": "pong from expert"
                }]
            else:
                time.sleep(1)
                resp = []
            self.wfile.write(json.dumps(resp).encode())
            return

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b"{}")


class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True


def run_matrix_server():
    server = ThreadedTCPServer(("127.0.0.1", 8008), MatrixHandler)
    server.serve_forever()


def run_xmsg_server():
    server = ThreadedTCPServer(("127.0.0.1", 7787), XmsgHandler)
    server.serve_forever()


if __name__ == "__main__":
    t1 = threading.Thread(target=run_matrix_server, daemon=True)
    t2 = threading.Thread(target=run_xmsg_server, daemon=True)
    t1.start()
    t2.start()
    print("Mock servers listening on 127.0.0.1:8008 (Matrix) and 127.0.0.1:7787 (xmsg)")
    sys.stdout.flush()
    while True:
        time.sleep(1)
