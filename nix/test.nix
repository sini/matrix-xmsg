{
  pkgs,
  self,
  mode ? "green",
}:

pkgs.testers.nixosTest {
  name = "matrix-xmsg-module-${mode}";

  nodes.machine =
    { config, pkgs, ... }:
    {
      imports = [
        self.nixosModules.default
      ];

      # The mock xmsg's socket directory, present before either service starts (BindPaths).
      systemd.tmpfiles.rules = [ "d /run/xmsg-mock 0755 root root -" ];

      # Mock server service
      systemd.services.mock-servers = {
        description = "Mock Matrix homeserver and xmsg sockets";
        wantedBy = [ "multi-user.target" ];
        before = [ "matrix-xmsg.service" ];
        serviceConfig = {
          ExecStart = "${pkgs.python3}/bin/python3 ${./mock_servers.py}";
          Restart = "always";
        };
      };

      # Token file provisioned on host
      environment.etc."matrix-token" = {
        text =
          if mode == "mutant-wrong-token" then
            "syt_wrong_token_unauthorized_99999\n"
          else
            "syt_valid_test_token_12345\n";
        mode = "0400";
      };

      environment.etc."matrix-store-passphrase" = {
        text = "test_store_passphrase_secret\n";
        mode = "0400";
      };

      systemd.services.matrix-xmsg.environment.RUST_LOG = "info";

      # Enable and configure matrix-xmsg
      services.matrix-xmsg = {
        enable = true;
        package = self.packages.${pkgs.stdenv.hostPlatform.system}.default.overrideAttrs {
          doCheck = false;
        };
        homeserverUrl = "http://127.0.0.1:8008";
        botMxid = "@bot:test.local";
        accessTokenFile =
          if mode == "mutant-missing" then "/etc/nonexistent-token" else "/etc/matrix-token";
        storePassphraseFile = "/etc/matrix-store-passphrase";
        rooms = [ "!test:test.local" ];
        trustedMxids = [ "@trusted:test.local" ];
        ownerMxid = "@owner:test.local";
        xmsgSocket = "/run/xmsg-mock";
        dynamicUser = false;
        expertRef = "claude";
        stateDir = "/var/lib/matrix-xmsg";
      };
    };

  testScript =
    if mode == "mutant-missing" then
      ''
        start_all()

        # 1. Wait for mock servers to bind
        machine.wait_for_unit("mock-servers.service")
        machine.wait_for_open_port(8008)
        machine.wait_for_file("/run/xmsg-mock/agent.sock")

        # 2. Verify that matrix-xmsg fails to become active due to missing credential
        machine.wait_until_fails("systemctl is-active --quiet matrix-xmsg.service")

        # 3. Assert the exact failure is step CREDENTIALS with exit status 243. The unit's recorded exit
        #    status is the deterministic signal; the executor's "Failed at step" journal line can land after
        #    the unit is already inactive, so it is awaited rather than read once.
        machine.wait_until_succeeds(
            "test \"$(systemctl show -p ExecMainStatus --value matrix-xmsg.service)\" = 243", timeout=30
        )
        machine.wait_until_succeeds(
            "journalctl -u matrix-xmsg.service | grep -qF 'Failed at step CREDENTIALS'", timeout=30
        )
      ''
    else if mode == "mutant-wrong-token" then
      ''
        start_all()

        # 1. Wait for mock servers to bind
        machine.wait_for_unit("mock-servers.service")
        machine.wait_for_open_port(8008)
        machine.wait_for_file("/run/xmsg-mock/agent.sock")

        # 2. Wait for matrix-xmsg service to start
        machine.wait_for_unit("matrix-xmsg.service")

        # 3. Wait for three rejected sync attempts, however slowly the machine runs. Each attempt logs one
        #    `Sync error:` line (`retrying in 3 seconds`); its 401 lines also come from the room join, several per attempt.
        machine.wait_until_succeeds(
            "test \"$(journalctl -u matrix-xmsg.service | grep -cF 'Sync error:')\" -ge 3", timeout=60
        )

        # 4. Assert that no reply was ever received by the Matrix server
        res = machine.succeed("test -f /tmp/matrix_reply_received.json && echo 'present' || echo 'absent'").strip()
        assert res == "absent", "Expected no reply sent, but file was created!"

        # 5. Assert journal shows 401 / UnknownToken handling
        journal = machine.succeed("journalctl -u matrix-xmsg.service")
        assert "UnknownToken" in journal or "401" in journal, f"Expected 401/UnknownToken in journal, got: {journal}"
        assert "Invalid or missing access token" in journal, f"Expected token error in journal, got: {journal}"

        # 6. Verify the 3-second backoff from the attempts' own timestamps (no hot spinning), not from a count
        #    in a fixed wall-clock window, which depends on how fast the machine runs
        stamps = [
            float(l.split()[0])
            for l in machine.succeed("journalctl -u matrix-xmsg.service -o short-unix").splitlines()
            if "Sync error:" in l
        ]
        gaps = [b - a for a, b in zip(stamps, stamps[1:])]
        assert len(gaps) >= 2 and min(gaps) >= 2.5, f"Expected >=3s backoff between 401 attempts, got gaps {gaps}"
        assert "status_code: 401" in journal, f"Expected the 401 response in journal, got: {journal}"

        # 7. Clean shutdown within TimeoutStopSec
        machine.succeed("systemctl stop matrix-xmsg.service")
        status = machine.succeed("systemctl is-active matrix-xmsg.service || true").strip()
        assert status == "inactive", f"Expected inactive status, got {status}"
      ''
    else
      ''
        start_all()

        # 1. Wait for mock servers to bind
        machine.wait_for_unit("mock-servers.service")
        machine.wait_for_open_port(8008)
        machine.wait_for_file("/run/xmsg-mock/agent.sock")

        # 2. Wait for matrix-xmsg to start up and load token credential
        machine.wait_for_unit("matrix-xmsg.service")

        # 3. Assert SQLite store created with 0600 permissions under stateDir
        machine.wait_for_file("/var/lib/matrix-xmsg/matrix-xmsg.db")
        mode = machine.succeed("stat -c '%a' /var/lib/matrix-xmsg/matrix-xmsg.db").strip()
        assert mode == "600", f"Expected SQLite DB mode 600, got {mode}"

        # 4. Assert question was dispatched to xmsg and reply received by Matrix (proves token was used)
        machine.wait_for_file("/tmp/xmsg_query_received.json")
        machine.wait_for_file("/tmp/matrix_reply_received.json")

        reply_content = machine.succeed("cat /tmp/matrix_reply_received.json")
        assert "pong from expert" in reply_content, f"Expected expert answer in reply, got: {reply_content}"
        assert "m.relates_to" in reply_content, f"Expected threaded reply relation, got: {reply_content}"

        # 5. Assert clean shutdown within TimeoutStopSec
        machine.succeed("systemctl stop matrix-xmsg.service")
        status = machine.succeed("systemctl is-active matrix-xmsg.service || true").strip()
        assert status == "inactive", f"Expected inactive status, got {status}"
      '';
}
