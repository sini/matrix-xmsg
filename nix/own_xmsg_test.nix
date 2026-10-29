{
  pkgs,
  matrix-xmsg,
  xmsg,
}:

let
  system = pkgs.stdenv.hostPlatform.system;

  mockGuard = pkgs.writeScriptBin "mock-guard" ''
    #!${pkgs.python3}/bin/python3
    import json
    import os
    import socket
    import sys
    import time

    SOCK_DIR = "/run/matrix-xmsg-xmsg/xmsg"

    def recv_frame(f):
        line = f.readline()
        if not line:
            raise RuntimeError("EOF from xmsg socket")
        return json.loads(line)

    def send_frame(sock, obj):
        sock.sendall((json.dumps(obj) + "\n").encode())

    c5_held_ids = []

    while True:
        try:
            reg = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            reg.connect(f"{SOCK_DIR}/register.sock")
            reg_f = reg.makefile("r", encoding="utf-8")
            send_frame(reg, {"harness": "svc", "name": "genie-guard", "cwd": "/"})
            ok = recv_frame(reg_f)
            if ok.get("status") != "ok":
                raise RuntimeError(f"svc registration failed: {ok}")
            sys.stderr.write(f"[mock-guard] registered on xmsg: {ok}\n")
            with open("/run/matrix-xmsg-xmsg/genie-guard.ready", "w") as rf:
                rf.write("ready\n")

            while True:
                send_frame(reg, {"action": "poll", "waitSecs": 5})
                frame = recv_frame(reg_f)
                if frame.get("action") == "deliver":
                    msg_id = frame["messageId"]
                    sys.stderr.write(f"[mock-guard] received guard request {msg_id}\n")
                    with open("/tmp/own_xmsg_guard_received.json", "w") as out:
                        json.dump(frame, out)
                    if os.path.exists("/tmp/mock_guard_c5_delay"):
                        c5_held_ids.append(msg_id)
                        send_frame(reg, {"action": "ack", "messageId": msg_id})
                        recv_frame(reg_f)
                        if len(c5_held_ids) >= 16:
                            try:
                                os.remove("/tmp/mock_guard_c5_delay")
                            except FileNotFoundError:
                                pass
                            for hid in c5_held_ids:
                                agent = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                                agent.connect(f"{SOCK_DIR}/agent.sock")
                                agent_f = agent.makefile("r", encoding="utf-8")
                                send_frame(agent, {
                                    "action": "reply",
                                    "message_id": hid,
                                    "text": json.dumps({
                                        "verdict": "allow",
                                        "reason": "clean",
                                        "cleaned_text": "",
                                    }),
                                })
                                reply_resp = recv_frame(agent_f)
                                agent.close()
                                sys.stderr.write(f"[mock-guard] replied (c5 batch): {reply_resp}\n")
                            c5_held_ids.clear()
                        continue
                    if os.path.exists("/tmp/mock_guard_fail"):
                        reply_payload = {"error": "simulated guard failure"}
                    else:
                        reply_payload = {
                            "verdict": "allow",
                            "reason": "clean",
                            "cleaned_text": "",
                        }
                    agent = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    agent.connect(f"{SOCK_DIR}/agent.sock")
                    agent_f = agent.makefile("r", encoding="utf-8")
                    send_frame(agent, {
                        "action": "reply",
                        "message_id": msg_id,
                        "text": json.dumps(reply_payload),
                    })
                    reply_resp = recv_frame(agent_f)
                    agent.close()
                    sys.stderr.write(f"[mock-guard] replied: {reply_resp}\n")
                    send_frame(reg, {"action": "ack", "messageId": msg_id})
                    recv_frame(reg_f)
        except Exception as e:
            sys.stderr.write(f"[mock-guard] connection lost ({e}); reconnecting\n")
            time.sleep(0.1)
  '';

  mockExpert = pkgs.writeScriptBin "mock-expert" ''
    #!${pkgs.python3}/bin/python3
    import json
    import socket
    import sys
    import time

    SOCK_DIR = "/run/matrix-xmsg-xmsg/xmsg"

    def recv_frame(f):
        line = f.readline()
        if not line:
            raise RuntimeError("EOF from xmsg socket")
        return json.loads(line)

    def send_frame(sock, obj):
        sock.sendall((json.dumps(obj) + "\n").encode())

    def send_reply(target_msg_id, text):
        agent = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        agent.connect(f"{SOCK_DIR}/agent.sock")
        agent_f = agent.makefile("r", encoding="utf-8")
        send_frame(agent, {
            "action": "reply",
            "message_id": target_msg_id,
            "text": text,
        })
        resp = recv_frame(agent_f)
        agent.close()
        sys.stderr.write(f"[mock-expert] replied to {target_msg_id}: {resp}\n")
        return resp

    phase0_fwd_id = None
    phase0_done = False
    seen_ids = set()
    thread_a_fwd_id = None
    thread_b_fwd_id = None
    thread_ab_replied = False
    c5_fwd_ids = []
    c5_ctx_count = 0

    while True:
        try:
            reg = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            reg.connect(f"{SOCK_DIR}/register.sock")
            reg_f = reg.makefile("r", encoding="utf-8")
            send_frame(reg, {"harness": "svc", "name": "claude", "cwd": "/"})
            ok = recv_frame(reg_f)
            if ok.get("status") != "ok":
                raise RuntimeError(f"svc registration failed: {ok}")
            sys.stderr.write(f"[mock-expert] registered on xmsg: {ok}\n")
            with open("/run/matrix-xmsg-xmsg/claude.ready", "w") as rf:
                rf.write("ready\n")

            while True:
                send_frame(reg, {"action": "poll", "waitSecs": 5})
                frame = recv_frame(reg_f)
                action = frame.get("action")
                if action == "deliver":
                    msg_id = frame["messageId"]
                    text = frame.get("text", "")
                    sys.stderr.write(f"[mock-expert] received delivery {msg_id}\n")
                    send_frame(reg, {"action": "ack", "messageId": msg_id})
                    recv_frame(reg_f)
                    if msg_id in seen_ids:
                        continue
                    seen_ids.add(msg_id)

                    req_text = ""
                    if "<request" in text and "</request>" in text:
                        req_text = text.split("<request", 1)[1].split("</request>", 1)[0]

                    if "screening unavailable" in text:
                        with open("/tmp/own_xmsg_d_error_reply.json", "w") as out:
                            json.dump(frame, out)
                    elif "<context" in text and "<request" not in text:
                        if not phase0_done:
                            phase0_done = True
                            with open("/tmp/own_xmsg_context_received.json", "w") as out:
                                json.dump(frame, out)
                            send_reply(phase0_fwd_id, "pong from real xmsg expert")
                        else:
                            c5_ctx_count += 1
                            if c5_ctx_count == 17:
                                with open("/tmp/own_xmsg_c5_done.json", "w") as out:
                                    json.dump({"count": c5_ctx_count, "sample": text}, out)
                    elif "@bot:test.local m18 ping" in req_text:
                        send_reply(msg_id, "m18 reply across xmsg restart")
                    elif "@bot:test.local thread A ping" in req_text or "@bot:test.local thread B ping" in req_text:
                        if "@bot:test.local thread A ping" in req_text:
                            thread_a_fwd_id = msg_id
                        if "@bot:test.local thread B ping" in req_text:
                            thread_b_fwd_id = msg_id
                        if thread_a_fwd_id and thread_b_fwd_id and not thread_ab_replied:
                            thread_ab_replied = True
                            send_reply(thread_a_fwd_id, "slow answer for thread A")
                            send_reply(thread_b_fwd_id, "fast answer for thread B")
                    elif "@bot:test.local c5 ping " in req_text:
                        c5_fwd_ids.append(msg_id)
                        if len(c5_fwd_ids) == 17:
                            with open("/tmp/mock_guard_c5_delay", "w") as df:
                                df.write("delay\n")
                            for fid in c5_fwd_ids:
                                send_reply(fid, json.dumps({"context": {"scope": "room", "limit": 5}}))
                    elif "@bot:test.local d_ctx ping" in req_text:
                        with open("/tmp/mock_guard_fail", "w") as ff:
                            ff.write("fail\n")
                        send_reply(msg_id, json.dumps({"context": {"scope": "room", "limit": 5}}))
                    elif "@bot:test.local ping" in req_text and phase0_fwd_id is None:
                        phase0_fwd_id = msg_id
                        with open("/tmp/own_xmsg_forward_received.json", "w") as out:
                            json.dump(frame, out)
                        send_reply(phase0_fwd_id, json.dumps({"context": {"scope": "room", "limit": 5}}))
        except Exception as e:
            sys.stderr.write(f"[mock-expert] connection lost ({e}); reconnecting\n")
            time.sleep(0.1)
  '';
in
pkgs.testers.nixosTest {
  name = "matrix-xmsg-own-xmsg";

  nodes.machine =
    { ... }:
    {
      imports = [
        matrix-xmsg.nixosModules.default
      ];

      environment.etc."matrix-token" = {
        text = "syt_valid_test_token_12345\n";
        mode = "0400";
      };

      environment.etc."matrix-store-passphrase" = {
        text = "test_store_passphrase_secret\n";
        mode = "0400";
      };

      systemd.services.mock-matrix = {
        description = "Mock Matrix homeserver (Matrix only)";
        wantedBy = [ "multi-user.target" ];
        before = [ "matrix-xmsg.service" ];
        environment.MOCK_MATRIX_ONLY = "1";
        serviceConfig = {
          ExecStart = "${pkgs.python3}/bin/python3 ${./mock_servers.py}";
          Restart = "always";
        };
      };

      systemd.services.mock-guard = {
        description = "Mock genie-guard registered on matrix-xmsg's own xmsg node";
        after = [ "matrix-xmsg-xmsg.service" ];
        wants = [ "matrix-xmsg-xmsg.service" ];
        before = [ "matrix-xmsg.service" ];
        wantedBy = [ "multi-user.target" ];
        serviceConfig = {
          Type = "simple";
          User = "matrix-xmsg";
          Group = "matrix-xmsg";
          ExecStartPre = "${pkgs.bash}/bin/bash -c 'for i in $(seq 1 50); do [ -S /run/matrix-xmsg-xmsg/xmsg/register.sock ] && exit 0; sleep 0.1; done; exit 1'";
          ExecStart = "${mockGuard}/bin/mock-guard";
          ExecStartPost = "${pkgs.bash}/bin/bash -c 'for i in $(seq 1 50); do [ -f /run/matrix-xmsg-xmsg/genie-guard.ready ] && exit 0; sleep 0.1; done; exit 1'";
          Restart = "on-failure";
          RestartSec = "1s";
        };
      };

      systemd.services.mock-expert = {
        description = "Mock expert registered on matrix-xmsg's own xmsg node";
        after = [ "matrix-xmsg-xmsg.service" ];
        wants = [ "matrix-xmsg-xmsg.service" ];
        before = [ "matrix-xmsg.service" ];
        wantedBy = [ "multi-user.target" ];
        serviceConfig = {
          Type = "simple";
          User = "matrix-xmsg";
          Group = "matrix-xmsg";
          ExecStartPre = "${pkgs.bash}/bin/bash -c 'for i in $(seq 1 50); do [ -S /run/matrix-xmsg-xmsg/xmsg/register.sock ] && exit 0; sleep 0.1; done; exit 1'";
          ExecStart = "${mockExpert}/bin/mock-expert";
          ExecStartPost = "${pkgs.bash}/bin/bash -c 'for i in $(seq 1 50); do [ -f /run/matrix-xmsg-xmsg/claude.ready ] && exit 0; sleep 0.1; done; exit 1'";
          Restart = "on-failure";
          RestartSec = "1s";
        };
      };

      systemd.services.matrix-xmsg = {
        after = [
          "mock-guard.service"
          "mock-expert.service"
        ];
        wants = [
          "mock-guard.service"
          "mock-expert.service"
        ];
        environment.RUST_LOG = "info";
      };

      services.matrix-xmsg = {
        enable = true;
        package = matrix-xmsg.packages.${system}.default.overrideAttrs { doCheck = false; };
        homeserverUrl = "http://127.0.0.1:8008";
        botMxid = "@bot:test.local";
        accessTokenFile = "/etc/matrix-token";
        storePassphraseFile = "/etc/matrix-store-passphrase";
        rooms = [ "!test:test.local" ];
        trustedMxids = [ "@trusted:test.local" ];
        ownerMxid = "@owner:test.local";
        admission = "public";
        screenMentions = true;
        guardTimeoutSecs = 10;
        guardRetryBudget = 2;
        rateLimitCount = 100;
        contextRateLimitCount = 100;
        expertRef = "claude";
        xmsg = {
          enable = true;
          package = xmsg.packages.${system}.default.overrideAttrs { doCheck = false; };
          svcExe = {
            claude = pkgs.python3.interpreter;
            genie-guard = pkgs.python3.interpreter;
          };
        };
      };
    };

  testScript = ''
    start_all()

    machine.wait_for_unit("mock-matrix.service")
    machine.wait_for_open_port(8008)
    machine.wait_for_unit("matrix-xmsg-xmsg.service")
    machine.wait_for_file("/run/matrix-xmsg-xmsg/xmsg/register.sock")
    machine.wait_for_file("/run/matrix-xmsg-xmsg/xmsg/agent.sock")
    machine.wait_for_unit("mock-guard.service")
    machine.wait_for_unit("mock-expert.service")
    machine.wait_for_unit("matrix-xmsg.service")

    # 1. Verify no /run/user entries exist (no user@1000 mount)
    run_user_state = machine.succeed("test ! -d /run/user || test -z \"$(ls -A /run/user)\" && echo 'empty'").strip()
    assert run_user_state == "empty", f"Expected no /run/user mounts, got: {run_user_state}"

    # 2. Verify both matrix-xmsg and matrix-xmsg-xmsg run as system user `matrix-xmsg`
    bot_user = machine.succeed("systemctl show -p User --value matrix-xmsg.service").strip()
    bus_user = machine.succeed("systemctl show -p User --value matrix-xmsg-xmsg.service").strip()
    assert bot_user == "matrix-xmsg", f"Expected matrix-xmsg.service User=matrix-xmsg, got {bot_user}"
    assert bus_user == "matrix-xmsg", f"Expected matrix-xmsg-xmsg.service User=matrix-xmsg, got {bus_user}"

    # 3. Verify matrix-xmsg registered on its own xmsg node as matrix-xmsg@matrix-xmsg.machine
    machine.wait_until_succeeds(
        "journalctl -u matrix-xmsg.service | grep -F 'Registered on xmsg as matrix-xmsg@matrix-xmsg.machine'",
        timeout=30,
    )

    # 4. Verify full round-trip including in-flight context request screened by genie-guard:
    # Matrix -> matrix-xmsg (guard check 1) -> mock-expert -> context request -> matrix-xmsg (guard check 2 while context in-flight) -> mock-expert -> final reply -> Matrix
    machine.wait_for_file("/tmp/own_xmsg_forward_received.json", timeout=30)
    machine.wait_for_file("/tmp/own_xmsg_guard_received.json", timeout=30)
    machine.wait_for_file("/tmp/own_xmsg_context_received.json", timeout=30)
    machine.wait_for_file("/tmp/matrix_reply_received.json", timeout=30)

    ctx_content = machine.succeed("cat /tmp/own_xmsg_context_received.json")
    assert "untrusted room context line for m38" in ctx_content, f"Expected screened room context in expert reply, got: {ctx_content}"

    reply_content = machine.succeed("cat /tmp/matrix_reply_received.json")
    assert "pong from real xmsg expert" in reply_content, f"Expected expert answer in Matrix reply, got: {reply_content}"
    assert "m.relates_to" in reply_content, f"Expected threaded reply relation, got: {reply_content}"

    def inject_matrix_events(events):
        import json as pyjson
        payload = "".join(pyjson.dumps(ev) + "\n" for ev in events)
        machine.succeed(f"cat <<'EOF' >> /tmp/matrix_inject_events.jsonl\n{payload}EOF")

    def make_mention(event_id, body, sender="@trusted:test.local", ts_ms=1700000100000):
        return {
            "type": "m.room.message",
            "sender": sender,
            "content": {
                "msgtype": "m.text",
                "body": body,
                "m.mentions": {"user_ids": ["@bot:test.local"]},
            },
            "event_id": event_id,
            "origin_server_ts": ts_ms,
        }

    # -------------------------------------------------------------------------
    # 5. XM3 (a) — M18 reconnect: restart real xmsg under the running bot.
    #    The bot re-registers, and an unacked delivery from before the drop is
    #    redelivered and posted exactly once.
    # -------------------------------------------------------------------------
    bot_pid_before = machine.succeed("systemctl show -p MainPID --value matrix-xmsg.service").strip()
    xmsg_pid_before = machine.succeed("systemctl show -p MainPID --value matrix-xmsg-xmsg.service").strip()

    machine.succeed("touch /tmp/m18_fail_first_post")
    inject_matrix_events([make_mention("$ev_m18_01", "@bot:test.local m18 ping", ts_ms=1700000101000)])
    machine.wait_for_file("/tmp/m18_delivery_in_flight", timeout=25)

    # Restart the real xmsg daemon under the running bot while the delivery is in-flight and unacked
    machine.succeed("systemctl restart matrix-xmsg-xmsg.service")
    machine.wait_until_succeeds(
        "journalctl -u matrix-xmsg.service | grep -F 'xmsg registration connection lost'",
        timeout=25,
    )
    machine.succeed("touch /tmp/m18_xmsg_restarted")

    machine.wait_until_succeeds(
        f"new_pid=$(systemctl show -p MainPID --value matrix-xmsg-xmsg.service); [ -n \"$new_pid\" ] && [ \"$new_pid\" != \"0\" ] && [ \"$new_pid\" != \"{xmsg_pid_before}\" ]",
        timeout=25,
    )
    machine.wait_for_file("/run/matrix-xmsg-xmsg/xmsg/register.sock", timeout=25)
    machine.wait_for_file("/run/matrix-xmsg-xmsg/xmsg/agent.sock", timeout=25)

    bot_pid_after = machine.succeed("systemctl show -p MainPID --value matrix-xmsg.service").strip()
    assert bot_pid_after == bot_pid_before, f"Expected matrix-xmsg.service to stay running across xmsg restart ({bot_pid_before} vs {bot_pid_after})"

    # Bot must re-register at least a second time
    machine.wait_until_succeeds(
        "test $(journalctl -u matrix-xmsg.service | grep -cF 'Registered on xmsg as matrix-xmsg@matrix-xmsg.machine') -ge 2",
        timeout=25,
    )

    # The unacked delivery from before the drop must be redelivered and posted to Matrix
    machine.wait_until_succeeds(
        "grep -F 'm18 reply across xmsg restart' /tmp/matrix_replies.jsonl",
        timeout=25,
    )
    machine.succeed("sleep 1")
    m18_post_count = int(machine.succeed("grep -cF 'm18 reply across xmsg restart' /tmp/matrix_replies.jsonl").strip())
    assert m18_post_count == 1, f"Expected m18 redelivered reply to be posted exactly once, got {m18_post_count}"

    # -------------------------------------------------------------------------
    # 6. XM3 (b) — M38 oracle 4: a delivery stuck in thread A does not delay
    #    thread B against real xmsg.
    # -------------------------------------------------------------------------
    inject_matrix_events([
        make_mention("$ev_thread_a", "@bot:test.local thread A ping", ts_ms=1700000201000),
        make_mention("$ev_thread_b", "@bot:test.local thread B ping", ts_ms=1700000202000),
    ])
    machine.wait_for_file("/tmp/m38_thread_a_blocked", timeout=25)
    machine.wait_for_file("/tmp/m38_thread_b_posted", timeout=25)
    machine.succeed("test ! -f /tmp/m38_thread_a_posted")
    machine.succeed("touch /tmp/m38_unblock_thread_a")
    machine.wait_for_file("/tmp/m38_thread_a_posted", timeout=25)

    # -------------------------------------------------------------------------
    # 7. XM3 (c) — M38 oracle 5: 17 concurrent context requests (one more than
    #    the 16-permit semaphore) complete via the verdict fast path.
    # -------------------------------------------------------------------------
    c5_events = [
        make_mention(f"$ev_c5_{i:02d}", f"@bot:test.local c5 ping {i:02d}", ts_ms=1700000300000 + i * 1000)
        for i in range(1, 18)
    ]
    inject_matrix_events(c5_events)
    machine.wait_for_file("/tmp/own_xmsg_c5_done.json", timeout=30)
    c5_done = machine.succeed("cat /tmp/own_xmsg_c5_done.json")
    assert '"count": 17' in c5_done, f"Expected all 17 concurrent context requests to complete, got: {c5_done}"
    assert "untrusted room context line for m38" in c5_done, f"Expected screened room context in c5 sample, got: {c5_done}"

    # -------------------------------------------------------------------------
    # 8. XM3 (d) — M38 oracle 3: fail-closed guard budget exhaustion replies
    #    on the context request and reacts with ⚠️ on the mention.
    # -------------------------------------------------------------------------
    inject_matrix_events([
        make_mention("$ev_d_ctx", "@bot:test.local d_ctx ping", ts_ms=1700000401000),
    ])
    machine.wait_for_file("/tmp/own_xmsg_d_error_reply.json", timeout=25)
    d_err_reply = machine.succeed("cat /tmp/own_xmsg_d_error_reply.json")
    assert "screening unavailable; try again later" in d_err_reply, f"Expected fail-closed error reply on context request, got: {d_err_reply}"

    inject_matrix_events([
        make_mention("$ev_d_mention_fail", "@bot:test.local d_mention_fail ping", sender="@stranger:test.local", ts_ms=1700000402000),
    ])
    machine.wait_until_succeeds(
        "grep -F '$ev_d_mention_fail' /tmp/matrix_reactions.jsonl | grep -F '⚠️'",
        timeout=25,
    )
  '';
}
