{ lib, inputs, ... }:
let
  pkgs = inputs.nixpkgs.legacyPackages.x86_64-linux;
  matrixModule = inputs.matrix-xmsg.nixosModules.default;
  baseModule = {
    options = {
      assertions = lib.mkOption {
        type = lib.types.listOf lib.types.attrs;
        default = [ ];
      };
      users.users = lib.mkOption {
        type = lib.types.attrsOf (
          lib.types.submodule {
            options = {
              home = lib.mkOption {
                type = lib.types.str;
              };
              isNormalUser = lib.mkOption {
                type = lib.types.bool;
                default = false;
              };
              isSystemUser = lib.mkOption {
                type = lib.types.bool;
                default = false;
              };
              group = lib.mkOption {
                type = lib.types.nullOr lib.types.str;
                default = null;
              };
              description = lib.mkOption {
                type = lib.types.nullOr lib.types.str;
                default = null;
              };
            };
          }
        );
        default = { };
      };
      users.groups = lib.mkOption {
        type = lib.types.attrs;
        default = { };
      };
      systemd.services = lib.mkOption {
        type = lib.types.attrsOf (
          lib.types.submodule {
            options = {
              description = lib.mkOption {
                type = lib.types.str;
                default = "";
              };
              after = lib.mkOption {
                type = lib.types.listOf lib.types.str;
                default = [ ];
              };
              before = lib.mkOption {
                type = lib.types.listOf lib.types.str;
                default = [ ];
              };
              requires = lib.mkOption {
                type = lib.types.listOf lib.types.str;
                default = [ ];
              };
              wants = lib.mkOption {
                type = lib.types.listOf lib.types.str;
                default = [ ];
              };
              requiredBy = lib.mkOption {
                type = lib.types.listOf lib.types.str;
                default = [ ];
              };
              wantedBy = lib.mkOption {
                type = lib.types.listOf lib.types.str;
                default = [ ];
              };
              restartTriggers = lib.mkOption {
                type = lib.types.listOf lib.types.unspecified;
                default = [ ];
              };
              environment = lib.mkOption {
                type = lib.types.attrsOf lib.types.str;
                default = { };
              };
              unitConfig = lib.mkOption {
                type = lib.types.attrsOf lib.types.unspecified;
                default = { };
              };
              serviceConfig = lib.mkOption {
                type = lib.types.attrsOf lib.types.unspecified;
                default = { };
              };
            };
          }
        );
        default = { };
      };
    };
  };

  pkg = inputs.matrix-xmsg.packages.x86_64-linux.default;
  xmsgNoCheck = inputs.xmsg.packages.x86_64-linux.default.overrideAttrs { doCheck = false; };

  caCert = pkgs.writeText "test-ca.crt" ''
    -----BEGIN CERTIFICATE-----
    MIIBjjCCATWgAwIBAgIUZagzwQC8cigUO0sEt2oBGx2PpmowCgYIKoZIzj0EAwIw
    HDEaMBgGA1UEAwwReG1zZyB0ZXN0IGZpeHR1cmUwIBcNMjYxMDEzMDM1NjA3WhgP
    MjEyNjA5MTkwMzU2MDdaMBwxGjAYBgNVBAMMEXhtc2cgdGVzdCBmaXh0dXJlMFkw
    EwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEnErvvgOrXyY8zOqnYdJaOvDZZ/xEtxNJ
    cz41XO7HULs9BBLryuxIJzkbaet3ZAxpmb53LL1s6a50iFGxq49E6qNTMFEwHQYD
    VR0OBBYEFOG0Pm+jmQY5hOj8AwASuCRegHerMB8GA1UdIwQYMBaAFOG0Pm+jmQY5
    hOj8AwASuCRegHerMA8GA1UdEwEB/wQFMAMBAf8wCgYIKoZIzj0EAwIDRwAwRAIg
    N3ThKYvVXlZ198leQWRQ8jEMalKJvayuLeAz6ZVhqVMCIBX50tkVlQGhjqSVxPDW
    4eMCzYXbyaRSnuTbpJqBPIFK
    -----END CERTIFICATE-----
  '';

  cutoverPeers = {
    "sini.bitstream" = {
      address = "100.64.0.1:7788";
      ca = "${caCert}";
      identities = [ "spiffe://sini/host/bitstream/user/sini" ];
      allow = [
        "send"
        "reply"
      ];
      dial = "outbound";
    };
    "sini.cortex" = {
      address = "100.64.0.2:7788";
      ca = "${caCert}";
      identities = [ "spiffe://sini/host/cortex/user/sini" ];
      allow = [
        "send"
        "reply"
      ];
      dial = "outbound";
    };
    "genie-guard.genie" = {
      address = "genie-guard.genie.invalid:7788";
      ca = "${caCert}";
      identities = [ "spiffe://sini/ns/genie/sa/genie-guard" ];
      leaf = true;
      allow = [ "reply" ];
      targets = [ "genie-guard" ];
    };
  };

  evalModule =
    serviceCfg:
    lib.evalModules {
      specialArgs = {
        inherit pkgs;
      };
      modules = [
        baseModule
        matrixModule
        {
          services.matrix-xmsg = {
            enable = true;
            package = pkg;
            homeserverUrl = "http://localhost:8008";
            botMxid = "@bot:local";
            accessTokenFile = "/run/token";
            rooms = [ "!room:local" ];
            ownerMxid = "@owner:local";
          }
          // serviceCfg;
        }
      ];
    };

  evalConfig =
    serviceCfg:
    let
      res = evalModule serviceCfg;
    in
    map (a: a.message) (builtins.filter (a: !a.assertion) res.config.assertions);

  cutoverRes = evalModule {
    xmsg = {
      enable = true;
      package = xmsgNoCheck;
      federation = {
        enable = true;
        listenAddress = "100.64.0.1";
        port = 7791;
        cert = "/run/agenix/matrix-xmsg-fed.crt";
        key = "/run/agenix/matrix-xmsg-fed.key";
        reloadInterval = 45;
        peers = cutoverPeers;
      };
    };
  };

  outboundOnlyRes = evalModule {
    xmsg = {
      enable = true;
      package = xmsgNoCheck;
      federation = {
        enable = true;
        port = null;
        cert = "/run/agenix/matrix-xmsg-fed.crt";
        key = "/run/agenix/matrix-xmsg-fed.key";
        peers = cutoverPeers;
      };
    };
  };

  bothPinAndCaRes = evalModule {
    xmsg = {
      enable = true;
      package = xmsgNoCheck;
      federation = {
        enable = true;
        cert = "/run/agenix/matrix-xmsg-fed.crt";
        key = "/run/agenix/matrix-xmsg-fed.key";
        peers = {
          "sini.bitstream" = {
            address = "100.64.0.1:7788";
            pin = "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
            ca = "${caCert}";
            identities = [ "spiffe://sini/host/bitstream/user/sini" ];
            allow = [
              "send"
              "reply"
            ];
            dial = "outbound";
          };
        };
      };
    };
  };
in
{
  flake.tests.m7-module = {
    test-oracle3-socket-with-dynamic-user-assertion = {
      expr =
        builtins.elem
          "services.matrix-xmsg: when xmsgSocket is configured, dynamicUser must be false because the socket's UID check requires running as the socket's owner. Configure services.matrix-xmsg.user to the socket's owning user."
          (evalConfig {
            xmsgSocket = "/run/xmsg.sock";
            dynamicUser = true;
          });
      expected = true;
    };

    test-oracle3-valid-socket-passes = {
      expr = evalConfig {
        xmsgSocket = "/run/xmsg.sock";
        dynamicUser = false;
      };
      expected = [ ];
    };

    test-existing-user-no-collision-preserves-home =
      let
        res = lib.evalModules {
          specialArgs = {
            inherit pkgs;
          };
          modules = [
            baseModule
            matrixModule
            {
              users.users.alice = {
                isNormalUser = true;
                home = "/home/alice";
              };
              services.matrix-xmsg = {
                enable = true;
                package = pkg;
                homeserverUrl = "http://localhost:8008";
                botMxid = "@bot:local";
                accessTokenFile = "/run/token";
                rooms = [ "!room:local" ];
                ownerMxid = "@owner:local";
                user = "alice";
                dynamicUser = false;
                xmsgSocket = "/run/user/1000/xmsg";
              };
            }
          ];
        };
      in
      {
        expr = {
          home = res.config.users.users.alice.home;
          user = res.config.systemd.services.matrix-xmsg.serviceConfig.User;
          hasGroup = res.config.systemd.services.matrix-xmsg.serviceConfig ? Group;
        };
        expected = {
          home = "/home/alice";
          user = "alice";
          hasGroup = false;
        };
      };

    test-m28-xmsg-enable-requires-package = {
      expr =
        builtins.elem
          "services.matrix-xmsg.xmsg.enable requires services.matrix-xmsg.xmsg.package to be set."
          (evalConfig {
            xmsg.enable = true;
          });
      expected = true;
    };

    test-m28-own-xmsg-unit-and-socket-defaults =
      let
        res = evalModule {
          xmsg = {
            enable = true;
            package = xmsgNoCheck;
          };
        };
        cfg = res.config.services.matrix-xmsg;
        xmsgSvc = res.config.systemd.services.matrix-xmsg-xmsg;
      in
      {
        expr = {
          dynamicUser = cfg.dynamicUser;
          xmsgSocket = cfg.xmsgSocket;
          hasXmsgUnit = res.config.systemd.services ? matrix-xmsg-xmsg;
          xmsgUser = xmsgSvc.serviceConfig.User;
          runtimeDir = xmsgSvc.serviceConfig.RuntimeDirectory;
          runtimeDirPreserve = xmsgSvc.serviceConfig.RuntimeDirectoryPreserve;
          xdgRuntimeDir = xmsgSvc.environment.XDG_RUNTIME_DIR;
          requiredBy = xmsgSvc.requiredBy;
          wantedBy = xmsgSvc.wantedBy;
        };
        expected = {
          dynamicUser = false;
          xmsgSocket = "/run/matrix-xmsg-xmsg/xmsg";
          hasXmsgUnit = true;
          xmsgUser = "matrix-xmsg";
          runtimeDir = [
            "matrix-xmsg-xmsg"
            "matrix-xmsg-xmsg/xmsg"
          ];
          runtimeDirPreserve = "yes";
          xdgRuntimeDir = "/run/matrix-xmsg-xmsg";
          requiredBy = [ ];
          wantedBy = [
            "matrix-xmsg.service"
            "multi-user.target"
          ];
        };
      };

    test-m30-guard-ref-null-allowed-when-both-switches-off = {
      expr = evalConfig {
        xmsgSocket = "/run/xmsg.sock";
        dynamicUser = false;
        guardRef = null;
        screenMentions = false;
        screenSolicited = false;
      };
      expected = [ ];
    };

    test-m30-guard-ref-null-fails-when-screen-mentions-on = {
      expr =
        builtins.elem
          "services.matrix-xmsg.guardRef must be set when services.matrix-xmsg.screenMentions or services.matrix-xmsg.screenSolicited is enabled (globally or in roomRoutes).\n"
          (evalConfig {
            xmsgSocket = "/run/xmsg.sock";
            dynamicUser = false;
            guardRef = null;
            screenMentions = true;
            screenSolicited = false;
          });
      expected = true;
    };

    test-m30-guard-ref-null-fails-when-screen-solicited-on = {
      expr =
        builtins.elem
          "services.matrix-xmsg.guardRef must be set when services.matrix-xmsg.screenMentions or services.matrix-xmsg.screenSolicited is enabled (globally or in roomRoutes).\n"
          (evalConfig {
            xmsgSocket = "/run/xmsg.sock";
            dynamicUser = false;
            guardRef = null;
            screenMentions = false;
            screenSolicited = true;
          });
      expected = true;
    };

    test-m28-1-federation-listening-cutover-argv-and-peers =
      let
        xmsgSvc = cutoverRes.config.systemd.services.matrix-xmsg-xmsg;
        peersDrv = lib.head xmsgSvc.restartTriggers;
      in
      {
        expr = {
          assertions = map (a: a.message) (builtins.filter (a: !a.assertion) cutoverRes.config.assertions);
          execStart = xmsgSvc.serviceConfig.ExecStart;
          triggerCount = builtins.length xmsgSvc.restartTriggers;
          peers = builtins.fromJSON (builtins.unsafeDiscardStringContext peersDrv.peersJson);
        };
        expected = {
          assertions = [ ];
          execStart = "${lib.getExe xmsgNoCheck} ${
            lib.escapeShellArgs [
              "serve"
              "--svc-exe"
              "matrix-xmsg=${lib.getExe pkg}"
              "--fed-listen"
              "100.64.0.1:7791"
              "--fed-cert"
              "/run/agenix/matrix-xmsg-fed.crt"
              "--fed-key"
              "/run/agenix/matrix-xmsg-fed.key"
              "--peers-file"
              (toString peersDrv)
              "--fed-reload-interval-secs"
              "45"
            ]
          }";
          triggerCount = 1;
          peers = builtins.fromJSON (builtins.unsafeDiscardStringContext (builtins.toJSON cutoverPeers));
        };
      };

    test-m28-1-federation-outbound-only-omits-fed-listen =
      let
        xmsgSvc = outboundOnlyRes.config.systemd.services.matrix-xmsg-xmsg;
        peersDrv = lib.head xmsgSvc.restartTriggers;
      in
      {
        expr = {
          assertions = map (a: a.message) (
            builtins.filter (a: !a.assertion) outboundOnlyRes.config.assertions
          );
          execStart = xmsgSvc.serviceConfig.ExecStart;
          hasFedListen = lib.hasInfix "--fed-listen" xmsgSvc.serviceConfig.ExecStart;
        };
        expected = {
          assertions = [ ];
          execStart = "${lib.getExe xmsgNoCheck} ${
            lib.escapeShellArgs [
              "serve"
              "--svc-exe"
              "matrix-xmsg=${lib.getExe pkg}"
              "--fed-cert"
              "/run/agenix/matrix-xmsg-fed.crt"
              "--fed-key"
              "/run/agenix/matrix-xmsg-fed.key"
              "--peers-file"
              (toString peersDrv)
            ]
          }";
          hasFedListen = false;
        };
      };

    test-m28-1-federation-key-rejects-store-path = {
      expr =
        builtins.elem
          "services.matrix-xmsg.xmsg.federation.key must be a runtime secret path, never a /nix/store path."
          (evalConfig {
            xmsg = {
              enable = true;
              package = xmsgNoCheck;
              federation = {
                enable = true;
                cert = "/run/agenix/matrix-xmsg-fed.crt";
                key = "${caCert}";
              };
            };
          });
      expected = true;
    };

    test-m36-e2ee-setup-oneshot-and-no-recovery-key-on-bot-unit =
      let
        res = evalModule {
          storePassphraseFile = "/run/agenix/matrix-xmsg-store-key";
          recoveryKeyFile = "/run/agenix/matrix-xmsg-recovery-key";
          resetCryptoStore = true;
        };
        botSvc = res.config.systemd.services.matrix-xmsg;
        setupSvc = res.config.systemd.services.matrix-xmsg-e2ee-setup;
      in
      {
        expr = {
          assertions = map (a: a.message) (builtins.filter (a: !a.assertion) res.config.assertions);
          botLoadCredential = botSvc.serviceConfig.LoadCredential;
          botHasRecoveryKey = lib.any (lib.hasInfix "recovery-key") botSvc.serviceConfig.LoadCredential;
          botExecHasRecoveryKey = lib.hasInfix "recovery-key" botSvc.serviceConfig.ExecStart;
          setupType = setupSvc.serviceConfig.Type;
          setupLoadCredential = setupSvc.serviceConfig.LoadCredential;
          setupHasE2eeSetupFlag = lib.hasInfix "--e2ee-setup" setupSvc.serviceConfig.ExecStart;
          setupHasResetFlag = lib.hasInfix "--reset-crypto-store" setupSvc.serviceConfig.ExecStart;
          setupHasRecoveryKeyArg = lib.hasInfix "--recovery-key-file /run/credentials/matrix-xmsg-e2ee-setup.service/recovery-key" setupSvc.serviceConfig.ExecStart;
        };
        expected = {
          assertions = [ ];
          botLoadCredential = [
            "access-token:/run/token"
            "store-passphrase:/run/agenix/matrix-xmsg-store-key"
          ];
          botHasRecoveryKey = false;
          botExecHasRecoveryKey = false;
          setupType = "oneshot";
          setupLoadCredential = [
            "access-token:/run/token"
            "store-passphrase:/run/agenix/matrix-xmsg-store-key"
            "recovery-key:/run/agenix/matrix-xmsg-recovery-key"
          ];
          setupHasE2eeSetupFlag = true;
          setupHasResetFlag = true;
          setupHasRecoveryKeyArg = true;
        };
      };

    test-m36-reset-crypto-store-requires-recovery-key = {
      expr =
        builtins.elem
          "services.matrix-xmsg.resetCryptoStore requires services.matrix-xmsg.recoveryKeyFile to be set.\n"
          (evalConfig {
            storePassphraseFile = "/run/agenix/matrix-xmsg-store-key";
            resetCryptoStore = true;
          });
      expected = true;
    };

    test-m36-1-e2ee-setup-condition-and-wants =
      let
        defaultRes = evalModule {
          storePassphraseFile = "/run/agenix/matrix-xmsg-store-key";
          recoveryKeyFile = "/run/agenix/matrix-xmsg-recovery-key";
        };
        defaultBot = defaultRes.config.systemd.services.matrix-xmsg;
        defaultSetup = defaultRes.config.systemd.services.matrix-xmsg-e2ee-setup;

        resetRes = evalModule {
          storePassphraseFile = "/run/agenix/matrix-xmsg-store-key";
          recoveryKeyFile = "/run/agenix/matrix-xmsg-recovery-key";
          resetCryptoStore = true;
        };
        resetSetup = resetRes.config.systemd.services.matrix-xmsg-e2ee-setup;

        customDbRes = evalModule {
          storePassphraseFile = "/run/agenix/matrix-xmsg-store-key";
          recoveryKeyFile = "/run/agenix/matrix-xmsg-recovery-key";
          dbPath = "/var/lib/matrix-xmsg-custom/sub/matrix-xmsg.db";
        };
        customDbSetup = customDbRes.config.systemd.services.matrix-xmsg-e2ee-setup;
      in
      {
        expr = {
          botWants = defaultBot.wants;
          botAfterHasSetup = builtins.elem "matrix-xmsg-e2ee-setup.service" defaultBot.after;
          setupWantedBy = defaultSetup.wantedBy;
          setupRequiredBy = defaultSetup.requiredBy;
          setupRemainAfterExit = defaultSetup.serviceConfig.RemainAfterExit or false;
          defaultCondition = defaultSetup.unitConfig.ConditionPathExists or null;
          resetCondition = resetSetup.unitConfig.ConditionPathExists or null;
          customDbCondition = customDbSetup.unitConfig.ConditionPathExists or null;
        };
        expected = {
          botWants = [ "matrix-xmsg-e2ee-setup.service" ];
          botAfterHasSetup = true;
          setupWantedBy = [ "matrix-xmsg.service" ];
          setupRequiredBy = [ ];
          setupRemainAfterExit = false;
          defaultCondition = "!/var/lib/matrix-xmsg/matrix-sdk-crypto.sqlite3";
          resetCondition = null;
          customDbCondition = "!/var/lib/matrix-xmsg-custom/sub/matrix-sdk-crypto.sqlite3";
        };
      };
  };

  perSystem =
    { system, ... }:
    lib.optionalAttrs (system == "x86_64-linux") {
      checks.peers-check =
        let
          goodPeersFile = lib.head cutoverRes.config.systemd.services.matrix-xmsg-xmsg.restartTriggers;
          badPeersDrv = lib.head bothPinAndCaRes.config.systemd.services.matrix-xmsg-xmsg.restartTriggers;
          badPeersFailure = pkgs.testers.testBuildFailure badPeersDrv;
          expectedJson = pkgs.writeText "expected-peers.json" (builtins.toJSON cutoverPeers);
        in
        pkgs.runCommand "matrix-xmsg-peers-check"
          {
            nativeBuildInputs = [
              pkgs.jq
              pkgs.diffutils
            ];
          }
          ''
            # 1. Realizing goodPeersFile already ran `xmsg check-peers` at build time; verify JSON equality:
            jq -S . "${goodPeersFile}" > actual.json
            jq -S . "${expectedJson}" > expected.json
            diff -u expected.json actual.json

            # 2. Negative build oracle through the module: building badPeersDrv itself must fail via `xmsg check-peers`:
            exit_code=$(cat "${badPeersFailure}/testBuildFailure.exit")
            echo "badPeersDrv testBuildFailure.exit=$exit_code"
            cat "${badPeersFailure}/testBuildFailure.log"
            [ "$exit_code" -ne 0 ] || {
              echo "expected badPeersDrv build to fail non-zero, got $exit_code" >&2
              exit 1
            }
            grep -q "exactly one trust mechanism is allowed" "${badPeersFailure}/testBuildFailure.log" || {
              echo "expected testBuildFailure.log to contain 'exactly one trust mechanism is allowed'" >&2
              exit 1
            }
            touch "$out"
          '';
    };
}
