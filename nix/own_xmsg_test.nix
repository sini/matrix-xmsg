{
  pkgs,
  matrix-xmsg,
  xmsg,
}:

let
  system = pkgs.stdenv.hostPlatform.system;

  mockExpert = pkgs.writeScriptBin "mock-expert" ''
    #!${pkgs.python3}/bin/python3
    import json
    import socket
    import sys

    SOCK_DIR = "/run/matrix-xmsg-xmsg/xmsg"

    def recv_frame(f):
        line = f.readline()
        if not line:
            raise RuntimeError("EOF from xmsg socket")
        return json.loads(line)

    def send_frame(sock, obj):
        sock.sendall((json.dumps(obj) + "\n").encode())

    # Register as service 'claude' on register.sock and keep connection open to poll
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
        send_frame(reg, {"action": "poll", "waitSecs": 30})
        frame = recv_frame(reg_f)
        action = frame.get("action")
        if action == "deliver":
            msg_id = frame["messageId"]
            sys.stderr.write(f"[mock-expert] received delivery {msg_id}\n")
            with open("/tmp/own_xmsg_forward_received.json", "w") as out:
                json.dump(frame, out)
            # Reply via agent.sock
            agent = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            agent.connect(f"{SOCK_DIR}/agent.sock")
            agent_f = agent.makefile("r", encoding="utf-8")
            send_frame(agent, {
                "action": "reply",
                "message_id": msg_id,
                "text": "pong from real xmsg expert",
            })
            reply_resp = recv_frame(agent_f)
            agent.close()
            sys.stderr.write(f"[mock-expert] replied: {reply_resp}\n")
            send_frame(reg, {"action": "ack", "messageId": msg_id})
            recv_frame(reg_f)
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
        after = [ "mock-expert.service" ];
        wants = [ "mock-expert.service" ];
        environment.RUST_LOG = "info";
      };

      services.matrix-xmsg = {
        enable = true;
        package = matrix-xmsg.packages.${system}.default.overrideAttrs { doCheck = false; };
        homeserverUrl = "http://127.0.0.1:8008";
        botMxid = "@bot:test.local";
        accessTokenFile = "/etc/matrix-token";
        rooms = [ "!test:test.local" ];
        trustedMxids = [ "@trusted:test.local" ];
        ownerMxid = "@owner:test.local";
        screenMentions = false;
        expertRef = "claude";
        xmsg = {
          enable = true;
          package = xmsg.packages.${system}.default.overrideAttrs { doCheck = false; };
          svcExe = {
            claude = pkgs.python3.interpreter;
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

    # 4. Verify full round-trip: Matrix -> matrix-xmsg -> real xmsg -> mock-expert -> real xmsg -> matrix-xmsg -> Matrix
    machine.wait_for_file("/tmp/own_xmsg_forward_received.json", timeout=30)
    machine.wait_for_file("/tmp/matrix_reply_received.json", timeout=30)

    reply_content = machine.succeed("cat /tmp/matrix_reply_received.json")
    assert "pong from real xmsg expert" in reply_content, f"Expected expert answer in Matrix reply, got: {reply_content}"
    assert "m.relates_to" in reply_content, f"Expected threaded reply relation, got: {reply_content}"
  '';
}
