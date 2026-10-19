{
  pkgs,
  matrix-xmsg,
}:

let
  system = pkgs.stdenv.hostPlatform.system;
  mailerCerts = import "${pkgs.path}/nixos/tests/common/acme/server/snakeoil-certs.nix";
  ca_key = mailerCerts.ca.key;
  ca_pem = mailerCerts.ca.cert;

  mkBundle =
    domain:
    pkgs.runCommand "bundle-${domain}"
      {
        nativeBuildInputs = [ pkgs.minica ];
      }
      ''
        minica -ca-cert ${ca_pem} -ca-key ${ca_key} \
          -domains ${domain}
        install -Dm444 -t $out ${domain}/{key,cert}.pem
      '';

  synapseDomain = "hs1.test";
  masDomain = "mas1.test";
  matrixSecret = "test_matrix_shared_secret_0123456789abcdef";

  synapseBundle = mkBundle synapseDomain;
  masBundle = mkBundle masDomain;

  masExtraConfig = pkgs.writeText "masExtraConfig.yml" (
    builtins.toJSON {
      secrets = {
        encryption_file = "/var/lib/matrix-authentication-service/encryption";
        keys = [
          {
            kid = "rsa-4096";
            key_file = "/var/lib/matrix-authentication-service/key_rsa_4096";
          }
        ];
      };
    }
  );

  pkg = matrix-xmsg.packages.${system}.default.overrideAttrs {
    doCheck = false;
  };
in
pkgs.testers.nixosTest {
  name = "matrix-xmsg-mas-e2ee";

  nodes.machine =
    {
      config,
      pkgs,
      lib,
      ...
    }:
    {
      imports = [
        matrix-xmsg.nixosModules.default
      ];

      virtualisation.memorySize = 2048;
      virtualisation.cores = 2;

      networking.extraHosts = ''
        127.0.0.1 ${synapseDomain}
        127.0.0.1 ${masDomain}
      '';

      security.pki.certificateFiles = [ ca_pem ];

      services.postgresql = {
        enable = true;
        authentication = lib.mkOverride 10 ''
          local all all trust
          host all all 127.0.0.1/32 trust
          host all all ::1/128 trust
        '';
        initialScript = pkgs.writeText "synapse-mas-init.sql" ''
          CREATE ROLE "matrix-synapse" WITH LOGIN PASSWORD 'synapse';
          CREATE DATABASE "matrix-synapse" WITH OWNER "matrix-synapse"
            TEMPLATE template0
            LC_COLLATE = "C"
            LC_CTYPE = "C";
        '';
      };

      services.redis.servers.matrix-synapse = {
        enable = true;
        port = 6379;
      };

      services.matrix-synapse = {
        enable = true;
        extras = [ "oidc" ];
        settings = {
          server_name = synapseDomain;
          public_baseurl = "https://${synapseDomain}:8448";
          tls_certificate_path = "${synapseBundle}/cert.pem";
          tls_private_key_path = "${synapseBundle}/key.pem";
          listeners = [
            {
              port = 8448;
              bind_addresses = [ "127.0.0.1" ];
              type = "http";
              tls = true;
              x_forwarded = false;
              resources = [
                {
                  names = [ "client" ];
                  compress = true;
                }
                {
                  names = [ "federation" ];
                  compress = false;
                }
              ];
            }
          ];
          database = {
            name = "psycopg2";
            args.password = "synapse";
          };
          redis = {
            enabled = true;
            host = "127.0.0.1";
            port = 6379;
          };
          password_config.enabled = false;
          matrix_authentication_service = {
            enabled = true;
            endpoint = "https://${masDomain}:8080";
            force_http2 = true;
            secret = matrixSecret;
          };
        };
      };

      services.matrix-authentication-service = {
        enable = true;
        createDatabase = true;
        serviceDependencies = [ ];
        extraConfigFiles = [ (toString masExtraConfig) ];
        settings = {
          passwords.enabled = false;
          http = {
            public_base = "https://${masDomain}:8080/";
            tls = {
              certificate_file = "${masBundle}/cert.pem";
              key_file = "${masBundle}/key.pem";
            };
            listeners = [
              {
                name = "web";
                binds = [
                  {
                    host = "127.0.0.1";
                    port = 8080;
                  }
                ];
                resources = [
                  { name = "discovery"; }
                  { name = "human"; }
                  { name = "oauth"; }
                  { name = "compat"; }
                  { name = "graphql"; }
                  { name = "assets"; }
                ];
                tls = {
                  certificate_file = "${masBundle}/cert.pem";
                  key_file = "${masBundle}/key.pem";
                };
              }
            ];
          };
          matrix = {
            homeserver = synapseDomain;
            endpoint = "https://${synapseDomain}:8448/";
            secret_file = "/var/lib/matrix-authentication-service/matrix_secret";
          };
          database.uri = "postgresql:///matrix-authentication-service?host=/run/postgresql&user=matrix-authentication-service";
          policy.data.client_registration.allow_insecure_uris = true;
        };
      };

      systemd.services.matrix-authentication-service.preStart = ''
        echo -n '${matrixSecret}' > /var/lib/matrix-authentication-service/matrix_secret
        echo -n '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef' > /var/lib/matrix-authentication-service/encryption
        if [ ! -f /var/lib/matrix-authentication-service/key_rsa_4096 ]; then
          ${pkgs.openssl}/bin/openssl genrsa -out /var/lib/matrix-authentication-service/key_rsa_4096 2048
        fi
      '';

      systemd.tmpfiles.rules = [ "d /run/xmsg-mock 0755 root root -" ];

      systemd.services.mock-xmsg-bus = {
        description = "Mock xmsg UNIX sockets for matrix-xmsg on MAS+Synapse";
        wantedBy = [ "multi-user.target" ];
        before = [ "matrix-xmsg.service" ];
        environment = {
          MOCK_XMSG_ONLY = "1";
          MOCK_EXPERT_REPLY = "pong from real xmsg expert";
        };
        serviceConfig = {
          ExecStart = "${pkgs.python3}/bin/python3 ${./mock_servers.py}";
          Restart = "always";
        };
      };

      environment.etc."matrix-store-passphrase" = {
        text = "mas_e2ee_store_passphrase_secret\n";
        mode = "0400";
      };

      environment.etc."matrix-recovery-key" = {
        text = "mas_e2ee_recovery_passphrase_secret\n";
        mode = "0400";
      };

      services.matrix-xmsg = {
        enable = true;
        package = pkg;
        homeserverUrl = "https://${synapseDomain}:8448";
        botMxid = "@genie:${synapseDomain}";
        accessTokenFile = "/etc/matrix-token";
        storePassphraseFile = "/etc/matrix-store-passphrase";
        recoveryKeyFile = "/etc/matrix-recovery-key";
        rooms = [ "!placeholder:${synapseDomain}" ];
        trustedMxids = [ "@alice:${synapseDomain}" ];
        ownerMxid = "@alice:${synapseDomain}";
        screenMentions = false;
        xmsgSocket = "/run/xmsg-mock";
        dynamicUser = false;
        expertRef = "claude";
        stateDir = "/var/lib/matrix-xmsg";
      };

      # Do not auto-start matrix-xmsg before MAS user & compatibility token are minted in testScript
      systemd.services.matrix-xmsg.wantedBy = lib.mkForce [ ];
      systemd.services.matrix-xmsg.environment = {
        RUST_LOG = "info";
        SSL_CERT_FILE = "/etc/ssl/certs/ca-bundle.crt";
      };
      systemd.services.matrix-xmsg-e2ee-setup.environment = {
        RUST_LOG = "info";
        SSL_CERT_FILE = "/etc/ssl/certs/ca-bundle.crt";
      };

      specialisation.reset-crypto-store.configuration = {
        services.matrix-xmsg.resetCryptoStore = true;
      };

      environment.systemPackages = [
        pkgs.matrix-synapse
        pkgs.matrix-authentication-service
        pkgs.curl
        pkg
      ];
    };

  testScript = ''
    import re

    start_all()

    machine.wait_for_unit("postgresql.target")
    machine.wait_for_unit("matrix-authentication-service.service")
    machine.wait_for_unit("matrix-synapse.service")
    machine.wait_for_unit("mock-xmsg-bus.service")
    machine.wait_for_open_port(8080)
    machine.wait_for_open_port(8448)
    machine.wait_for_file("/run/xmsg-mock/agent.sock")

    machine.wait_until_succeeds("curl --fail -s https://${synapseDomain}:8448/_matrix/client/versions")
    machine.wait_until_succeeds("curl --fail -s https://${masDomain}:8080/.well-known/openid-configuration")

    mas_cmd = (
        "mas-cli manage "
        "--config /run/matrix-authentication-service/config.yaml "
        "--config ${masExtraConfig}"
    )

    # 1. Register users in MAS (--yes, no password)
    machine.succeed(f"{mas_cmd} register-user --yes genie")
    machine.succeed(f"{mas_cmd} register-user --yes alice")

    # 2. Issue compatibility tokens with explicit device IDs
    genie_out1 = machine.succeed(f"{mas_cmd} issue-compatibility-token genie MATRIX_XMSG 2>&1")
    m_genie1 = re.search(r"Compatibility token issued:\s*(\S+)", genie_out1)
    assert m_genie1 is not None, f"Failed to extract genie compatibility token 1 from: {genie_out1}"
    genie_token_1 = m_genie1.group(1)

    alice_out = machine.succeed(f"{mas_cmd} issue-compatibility-token alice ALICE_DEV 2>&1")
    m_alice = re.search(r"Compatibility token issued:\s*(\S+)", alice_out)
    assert m_alice is not None, f"Failed to extract alice compatibility token from: {alice_out}"
    alice_token = m_alice.group(1)

    # 3. Verify whoami, bootstrap Alice, create encrypted room and invite @genie:hs1.test
    setup_out = machine.succeed(
        f"SSL_CERT_FILE=/etc/ssl/certs/ca-bundle.crt "
        f"HOMESERVER_URL=https://${synapseDomain}:8448 "
        f"ALICE_MXID=@alice:${synapseDomain} "
        f"ALICE_TOKEN={alice_token} "
        f"ALICE_DEVICE=ALICE_DEV "
        f"GENIE_MXID=@genie:${synapseDomain} "
        f"GENIE_TOKEN={genie_token_1} "
        f"GENIE_DEVICE=MATRIX_XMSG "
        f"STORE_BASE_DIR=/tmp/mas-e2ee-probe "
        f"mas-e2ee-probe setup-room"
    )
    print(setup_out)
    m_room = re.search(r"ROOM_ID=(\S+)", setup_out)
    assert m_room is not None, f"Failed to extract ROOM_ID from: {setup_out}"
    room_id = m_room.group(1)

    # 4. Provision /etc/matrix-token and override each unit's config.toml room ID to the created room
    machine.succeed(f"printf '%s\\n' '{genie_token_1}' > /etc/matrix-token && chmod 0400 /etc/matrix-token")
    machine.succeed(
        f"cp $(systemctl cat matrix-xmsg.service | grep -o '/nix/store/[^ ]*-matrix-xmsg-config\\.toml' | head -n1) /etc/matrix-xmsg-live.toml && "
        f"sed -i 's|!placeholder:${synapseDomain}|{room_id}|g' /etc/matrix-xmsg-live.toml && "
        f"cp $(systemctl cat matrix-xmsg-e2ee-setup.service | grep -o '/nix/store/[^ ]*-matrix-xmsg-config\\.toml' | head -n1) /etc/matrix-xmsg-setup-live.toml && "
        f"sed -i 's|!placeholder:${synapseDomain}|{room_id}|g' /etc/matrix-xmsg-setup-live.toml && "
        f"mkdir -p /run/systemd/system/matrix-xmsg.service.d /run/systemd/system/matrix-xmsg-e2ee-setup.service.d && "
        f"printf '[Service]\\nExecStart=\\nExecStart=${pkg}/bin/matrix-xmsg --config /etc/matrix-xmsg-live.toml\\n' > /run/systemd/system/matrix-xmsg.service.d/override.conf && "
        f"printf '[Service]\\nExecStart=\\nExecStart=${pkg}/bin/matrix-xmsg --config /etc/matrix-xmsg-setup-live.toml --e2ee-setup --recovery-key-file /run/credentials/matrix-xmsg-e2ee-setup.service/recovery-key\\n' > /run/systemd/system/matrix-xmsg-e2ee-setup.service.d/override.conf && "
        f"systemctl daemon-reload"
    )

    # 5. Start matrix-xmsg.service (which triggers matrix-xmsg-e2ee-setup.service first via Wants=/Before=)
    machine.succeed("systemctl start matrix-xmsg.service")
    machine.wait_for_unit("matrix-xmsg.service")

    # 6. Verify P3 on live systemd units:
    #    - matrix-xmsg-e2ee-setup.service ran and exited 0, creating /var/lib/matrix-xmsg/matrix-sdk-crypto.sqlite3
    #    - matrix-xmsg.service LoadCredential does NOT contain recovery-key
    #    - recovery-key is not mounted anywhere under /run/credentials after oneshot exit
    setup_cond_1 = machine.succeed("systemctl show -p ConditionResult --value matrix-xmsg-e2ee-setup.service").strip()
    assert setup_cond_1 == "yes", f"Expected ConditionResult=yes on first boot, got {setup_cond_1}"
    setup_status = machine.succeed("systemctl show -p ExecMainStatus --value matrix-xmsg-e2ee-setup.service").strip()
    assert setup_status == "0", f"Expected matrix-xmsg-e2ee-setup.service exit 0, got {setup_status}"
    setup_start_ts_1 = int(machine.succeed("systemctl show -p ExecMainStartTimestampMonotonic --value matrix-xmsg-e2ee-setup.service").strip())
    assert setup_start_ts_1 > 0, f"Expected positive ExecMainStartTimestampMonotonic on first boot, got {setup_start_ts_1}"
    machine.succeed("test -s /var/lib/matrix-xmsg/matrix-sdk-crypto.sqlite3")
    bot_unit = machine.succeed("systemctl cat matrix-xmsg.service")
    assert "LoadCredential=store-passphrase:" in bot_unit, f"Expected store-passphrase in bot unit, got {bot_unit}"
    assert "recovery-key" not in bot_unit, f"P3 violation: recovery-key found in matrix-xmsg.service unit: {bot_unit}"
    machine.succeed("test -r /run/credentials/matrix-xmsg.service/store-passphrase")
    machine.succeed("test ! -e /run/credentials/matrix-xmsg.service/recovery-key")
    recovery_mounts_1 = machine.succeed("find /run/credentials -name '*recovery*' || true").strip()
    assert recovery_mounts_1 == "", f"P3 violation: recovery-key still mounted under /run/credentials after oneshot exit: {recovery_mounts_1}"

    # 7. §7.1 Roundtrip 1: Alice sends encrypted mention -> matrix-xmsg decrypts -> forwards to xmsg -> replies encrypted -> Alice decrypts
    rt1_out = machine.succeed(
        f"SSL_CERT_FILE=/etc/ssl/certs/ca-bundle.crt "
        f"HOMESERVER_URL=https://${synapseDomain}:8448 "
        f"ALICE_MXID=@alice:${synapseDomain} "
        f"ALICE_TOKEN={alice_token} "
        f"ALICE_DEVICE=ALICE_DEV "
        f"GENIE_MXID=@genie:${synapseDomain} "
        f"ROOM_ID='{room_id}' "
        f"STORE_BASE_DIR=/tmp/mas-e2ee-probe "
        f"mas-e2ee-probe verify-roundtrip-1"
    )
    print(rt1_out)
    assert "ROUNDTRIP_1_PASS" in rt1_out
    m_pre_loss = re.search(r"PRE_LOSS_EVENT_ID=(\S+)", rt1_out)
    assert m_pre_loss is not None
    pre_loss_ev_id = m_pre_loss.group(1)

    # 8. §7.1 & §7.3 + M36.1 Cell 1 Restart:
    #    systemctl restart matrix-xmsg.service skips matrix-xmsg-e2ee-setup.service (ConditionResult=no),
    #    never mounts recovery-key under /run/credentials, and matrix-xmsg.service comes up and decrypts.
    machine.succeed("systemctl restart matrix-xmsg.service")
    machine.wait_for_unit("matrix-xmsg.service")
    setup_cond_restart = machine.succeed("systemctl show -p ConditionResult --value matrix-xmsg-e2ee-setup.service").strip()
    assert setup_cond_restart == "no", f"M36.1 Cell 1 failed: expected ConditionResult=no on restart when crypto store exists, got {setup_cond_restart}"
    setup_start_ts_2 = int(machine.succeed("systemctl show -p ExecMainStartTimestampMonotonic --value matrix-xmsg-e2ee-setup.service").strip())
    assert setup_start_ts_2 == setup_start_ts_1, f"M36.1 Cell 1 failed: matrix-xmsg-e2ee-setup.service re-ran on restart ({setup_start_ts_1} -> {setup_start_ts_2})"
    recovery_mounts_restart = machine.succeed("find /run/credentials -name '*recovery*' || true").strip()
    assert recovery_mounts_restart == "", f"M36.1 Cell 1 failed: recovery-key mounted under /run/credentials on restart: {recovery_mounts_restart}"
    rt2_out = machine.succeed(
        f"SSL_CERT_FILE=/etc/ssl/certs/ca-bundle.crt "
        f"HOMESERVER_URL=https://${synapseDomain}:8448 "
        f"ALICE_MXID=@alice:${synapseDomain} "
        f"ALICE_TOKEN={alice_token} "
        f"ALICE_DEVICE=ALICE_DEV "
        f"GENIE_MXID=@genie:${synapseDomain} "
        f"ROOM_ID='{room_id}' "
        f"STORE_BASE_DIR=/tmp/mas-e2ee-probe "
        f"mas-e2ee-probe verify-restart"
    )
    print(rt2_out)
    assert "RESTART_ROUNDTRIP_PASS" in rt2_out

    # 9. Stop matrix-xmsg.service and mint new compatibility tokens for store-loss & mismatch tests
    machine.succeed("systemctl stop matrix-xmsg.service")

    genie_out2 = machine.succeed(f"{mas_cmd} issue-compatibility-token genie MATRIX_XMSG_2 2>&1")
    m_genie2 = re.search(r"Compatibility token issued:\s*(\S+)", genie_out2)
    assert m_genie2 is not None
    genie_token_2 = m_genie2.group(1)

    genie_out3 = machine.succeed(f"{mas_cmd} issue-compatibility-token genie MATRIX_XMSG_3 2>&1")
    m_genie3 = re.search(r"Compatibility token issued:\s*(\S+)", genie_out3)
    assert m_genie3 is not None
    genie_token_3 = m_genie3.group(1)

    # 10. Run store-loss (Row 1, Row 2 without flag, Row 2 with --reset-crypto-store + backup decrypt + cross-signing),
    #     --reset-crypto-store on non-empty store, store-device mismatch, missing passphrase, and pre-join placeholder checks
    cells_out = machine.succeed(
        f"SSL_CERT_FILE=/etc/ssl/certs/ca-bundle.crt "
        f"HOMESERVER_URL=https://${synapseDomain}:8448 "
        f"ALICE_MXID=@alice:${synapseDomain} "
        f"ALICE_TOKEN={alice_token} "
        f"ALICE_DEVICE=ALICE_DEV "
        f"GENIE_MXID=@genie:${synapseDomain} "
        f"GENIE_TOKEN_OLD={genie_token_1} "
        f"GENIE_TOKEN_NEW={genie_token_2} "
        f"GENIE_TOKEN_MISMATCH={genie_token_3} "
        f"STORE_PASSPHRASE=mas_e2ee_store_passphrase_secret "
        f"RECOVERY_KEY=mas_e2ee_recovery_passphrase_secret "
        f"ROOM_ID='{room_id}' "
        f"PRE_LOSS_EVENT_ID='{pre_loss_ev_id}' "
        f"STORE_BASE_DIR=/tmp/mas-e2ee-probe "
        f"mas-e2ee-probe verify-store-loss-and-prejoin"
    )
    print(cells_out)
    for marker in [
        "MISSING_PASSPHRASE_REFUSED_OK",
        "ROW1_OLD_TOKEN_REFUSED_OK",
        "ROW2_WITHOUT_FLAG_REFUSED_OK",
        "ROW2_RECOVERED_AND_BACKUP_DECRYPTED_OK",
        "NEW_DEVICE_CROSS_SIGNED_OK",
        "RESET_ON_NON_EMPTY_UNTOUCHED_OK",
        "STORE_DEVICE_MISMATCH_REFUSED_OK",
        "PREJOIN_PLACEHOLDER_OK",
        "NO_PLAINTEXT_FALLBACK_OK",
        "MAS_E2EE_ALL_CELLS_PASS",
    ]:
        assert marker in cells_out, f"Missing expected marker {marker} in cells_out: {cells_out}"

    # 11. M36.1 Cell 2: with resetCryptoStore and an empty store, matrix-xmsg-e2ee-setup.service runs and recovers
    reset_spec_unit = machine.succeed(
        "cat /run/current-system/specialisation/reset-crypto-store/etc/systemd/system/matrix-xmsg-e2ee-setup.service"
    )
    assert "--reset-crypto-store" in reset_spec_unit, f"Expected --reset-crypto-store in resetCryptoStore unit: {reset_spec_unit}"
    assert "ConditionPathExists=" not in reset_spec_unit, f"Expected no ConditionPathExists in resetCryptoStore unit: {reset_spec_unit}"

    genie_out4 = machine.succeed(f"{mas_cmd} issue-compatibility-token genie MATRIX_XMSG_4 2>&1")
    m_genie4 = re.search(r"Compatibility token issued:\s*(\S+)", genie_out4)
    assert m_genie4 is not None
    genie_token_4 = m_genie4.group(1)

    machine.succeed(
        f"printf '%s\\n' '{genie_token_4}' > /etc/matrix-token && chmod 0400 /etc/matrix-token && "
        f"rm -f /var/lib/matrix-xmsg/matrix-sdk-crypto.sqlite3* && "
        f"cp /run/current-system/specialisation/reset-crypto-store/etc/systemd/system/matrix-xmsg-e2ee-setup.service /run/systemd/system/matrix-xmsg-e2ee-setup.service && "
        f"printf '[Service]\\nExecStart=\\nExecStart=${pkg}/bin/matrix-xmsg --config /etc/matrix-xmsg-setup-live.toml --e2ee-setup --recovery-key-file /run/credentials/matrix-xmsg-e2ee-setup.service/recovery-key --reset-crypto-store\\n' > /run/systemd/system/matrix-xmsg-e2ee-setup.service.d/override.conf && "
        f"systemctl daemon-reload && "
        f"systemctl start matrix-xmsg.service"
    )
    machine.wait_for_unit("matrix-xmsg.service")
    setup_cond_reset = machine.succeed("systemctl show -p ConditionResult --value matrix-xmsg-e2ee-setup.service").strip()
    assert setup_cond_reset == "yes", f"M36.1 Cell 2 failed: expected ConditionResult=yes with resetCryptoStore and empty store, got {setup_cond_reset}"
    setup_start_ts_3 = int(machine.succeed("systemctl show -p ExecMainStartTimestampMonotonic --value matrix-xmsg-e2ee-setup.service").strip())
    assert setup_start_ts_3 > setup_start_ts_2, f"M36.1 Cell 2 failed: matrix-xmsg-e2ee-setup.service did not run ({setup_start_ts_2} -> {setup_start_ts_3})"
    setup_status_reset = machine.succeed("systemctl show -p ExecMainStatus --value matrix-xmsg-e2ee-setup.service").strip()
    assert setup_status_reset == "0", f"M36.1 Cell 2 failed: expected matrix-xmsg-e2ee-setup.service exit 0, got {setup_status_reset}"
    machine.succeed("test -s /var/lib/matrix-xmsg/matrix-sdk-crypto.sqlite3")
    recovery_mounts_reset = machine.succeed("find /run/credentials -name '*recovery*' || true").strip()
    assert recovery_mounts_reset == "", f"M36.1 Cell 2 failed: recovery-key still mounted after setup exit: {recovery_mounts_reset}"
    rt3_out = machine.succeed(
        f"SSL_CERT_FILE=/etc/ssl/certs/ca-bundle.crt "
        f"HOMESERVER_URL=https://${synapseDomain}:8448 "
        f"ALICE_MXID=@alice:${synapseDomain} "
        f"ALICE_TOKEN={alice_token} "
        f"ALICE_DEVICE=ALICE_DEV "
        f"GENIE_MXID=@genie:${synapseDomain} "
        f"ROOM_ID='{room_id}' "
        f"STORE_BASE_DIR=/tmp/mas-e2ee-probe "
        f"mas-e2ee-probe verify-restart"
    )
    print(rt3_out)
    assert "RESTART_ROUNDTRIP_PASS" in rt3_out
  '';
}
