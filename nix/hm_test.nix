{
  pkgs,
  matrix-xmsg,
  xmsg,
  home-manager,
  mode ? "green",
}:

pkgs.testers.nixosTest {
  name = "matrix-xmsg-hm-${mode}";

  nodes.machine =
    { lib, pkgs, ... }:
    let
      system = pkgs.stdenv.hostPlatform.system;
    in
    {
      imports = [
        home-manager.nixosModules.home-manager
      ];

      users.users.alice = {
        isNormalUser = true;
        uid = 1000;
        linger = true;
      };

      systemd.tmpfiles.rules = [
        "f /home/alice/matrix-token 0600 alice users - syt_valid_test_token_12345"
      ];

      systemd.services.mock-matrix = {
        description = "Mock Matrix homeserver (Matrix only)";
        wantedBy = [ "multi-user.target" ];
        before = [ "user@1000.service" ];
        environment.MOCK_MATRIX_ONLY = "1";
        serviceConfig = {
          ExecStart = "${pkgs.python3}/bin/python3 ${./mock_servers.py}";
          Restart = "always";
        };
      };

      home-manager.users.alice = {
        imports = [
          xmsg.homeManagerModules.xmsg
          matrix-xmsg.homeManagerModules.default
        ];

        home = {
          username = "alice";
          homeDirectory = "/home/alice";
          stateVersion = "25.11";
          enableNixpkgsReleaseCheck = false;
        };

        systemd.user.startServices = "sd-switch";

        programs.xmsg = {
          enable = true;
          package = xmsg.packages.${system}.default.overrideAttrs { doCheck = false; };
        }
        // lib.optionalAttrs (mode == "mutant-no-svcexe") {
          server.svcExe = lib.mkForce { };
        };

        services.matrix-xmsg = {
          enable = true;
          package = matrix-xmsg.packages.${system}.default.overrideAttrs { doCheck = false; };
          homeserverUrl = "http://127.0.0.1:8008";
          botMxid = "@bot:test.local";
          accessTokenFile = "/home/alice/matrix-token";
          rooms = [ "!test:test.local" ];
          trustedMxids = [ "@trusted:test.local" ];
          ownerMxid = "@owner:test.local";
          screenMentions = false;
        };
      };
    };

  testScript =
    if mode == "mutant-no-svcexe" then
      ''
        start_all()

        machine.wait_for_unit("mock-matrix.service")
        machine.wait_for_open_port(8008)
        machine.wait_for_unit("user@1000.service")
        machine.wait_for_file("/run/user/1000/xmsg/register.sock")
        machine.wait_for_file("/run/user/1000/xmsg/agent.sock")

        # Real xmsg refuses registration when svcExe.matrix-xmsg is dropped
        machine.wait_until_succeeds(
            "journalctl _SYSTEMD_USER_UNIT=xmsg.service | grep -F \"no trusted executable configured for service 'matrix-xmsg'\"",
            timeout=30,
        )
        machine.wait_until_succeeds(
            "journalctl _SYSTEMD_USER_UNIT=matrix-xmsg.service | grep -F \"Svc registration failed: bad request: no trusted executable configured for service 'matrix-xmsg'\"",
            timeout=30,
        )

        # Real xmsg also denies matrix-xmsg on agent.sock because its PID is not an active session
        machine.wait_until_succeeds(
            "journalctl _SYSTEMD_USER_UNIT=matrix-xmsg.service | grep -F 'does not descend from an active agent session'",
            timeout=30,
        )

        journal = machine.succeed("journalctl _SYSTEMD_USER_UNIT=matrix-xmsg.service")
        assert "Registered on xmsg as" not in journal, f"Expected registration to be refused, got: {journal}"
      ''
    else
      ''
        start_all()

        machine.wait_for_unit("mock-matrix.service")
        machine.wait_for_open_port(8008)
        machine.wait_for_unit("user@1000.service")
        machine.wait_for_file("/run/user/1000/xmsg/register.sock")
        machine.wait_for_file("/run/user/1000/xmsg/agent.sock")

        # SQLite database created under ~/.local/state/matrix-xmsg with 0600 permissions
        machine.wait_for_file("/home/alice/.local/state/matrix-xmsg/matrix-xmsg.db")
        db_mode = machine.succeed("stat -c '%a' /home/alice/.local/state/matrix-xmsg/matrix-xmsg.db").strip()
        assert db_mode == "600", f"Expected SQLite DB mode 600, got {db_mode}"

        # Real xmsg admits matrix-xmsg via programs.xmsg.server.svcExe.matrix-xmsg
        machine.wait_until_succeeds(
            "journalctl _SYSTEMD_USER_UNIT=matrix-xmsg.service | grep -F 'Registered on xmsg as matrix-xmsg@alice.machine'",
            timeout=30,
        )

        # Real xmsg admits matrix-xmsg's PID on agent.sock as a registered live session
        machine.wait_until_succeeds(
            "journalctl _SYSTEMD_USER_UNIT=matrix-xmsg.service | grep -F \"no live recipient matches 'claude'\"",
            timeout=30,
        )
      '';
}
