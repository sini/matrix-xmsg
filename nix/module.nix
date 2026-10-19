{
  self ? null,
  packages ? self.packages,
}:
{
  config,
  lib,
  pkgs,
  ...
}:

let
  cfg = config.services.matrix-xmsg;
  xmsgCfg = cfg.xmsg;
  shared = import ./options.nix;

  # Systemd LoadCredential mounts the secret at /run/credentials/matrix-xmsg.service/access-token
  configToml = shared.mkConfigToml {
    inherit lib pkgs;
  } cfg "/run/credentials/matrix-xmsg.service/access-token";

  peersFile =
    pkgs.runCommand "matrix-xmsg-xmsg-peers.json"
      {
        peersJson = builtins.toJSON xmsgCfg.federation.peers;
        passAsFile = [ "peersJson" ];
      }
      ''
        cp "$peersJsonPath" "$out"
        ${lib.getExe xmsgCfg.package} check-peers "$out"
      '';

  effectiveSvcExe = {
    matrix-xmsg = lib.getExe cfg.package;
  }
  // xmsgCfg.svcExe;

  xmsgArgs = [
    "serve"
  ]
  ++ lib.concatLists (
    lib.mapAttrsToList (name: path: [
      "--svc-exe"
      "${name}=${path}"
    ]) effectiveSvcExe
  )
  ++ lib.optionals xmsgCfg.federation.enable (
    lib.optionals (xmsgCfg.federation.port != null) [
      "--fed-listen"
      "${xmsgCfg.federation.listenAddress}:${toString xmsgCfg.federation.port}"
    ]
    ++ [
      "--fed-cert"
      (toString xmsgCfg.federation.cert)
      "--fed-key"
      (toString xmsgCfg.federation.key)
      "--peers-file"
      (toString peersFile)
    ]
    ++ lib.optionals (xmsgCfg.federation.reloadInterval != null) [
      "--fed-reload-interval-secs"
      (toString xmsgCfg.federation.reloadInterval)
    ]
  );
in
{
  options.services.matrix-xmsg =
    (shared.mkCommonOptions {
      inherit
        lib
        pkgs
        packages
        cfg
        ;
      defaultStateDir = "/var/lib/matrix-xmsg";
      defaultXmsgSocket = if cfg.xmsg.enable then "/run/matrix-xmsg-xmsg/xmsg" else null;
      defaultXmsgSocketText = lib.literalExpression ''if config.services.matrix-xmsg.xmsg.enable then "/run/matrix-xmsg-xmsg/xmsg" else null'';
      accessTokenDescription = ''
        Path to file containing the Matrix access token.
        Loaded securely at service startup via systemd LoadCredential;
        never copied into the world-readable /nix/store.
      '';
    })
    // {
      user = lib.mkOption {
        type = lib.types.str;
        default = "matrix-xmsg";
        description = "Dedicated system user (used when dynamicUser is false).";
      };

      group = lib.mkOption {
        type = lib.types.nullOr lib.types.str;
        default = null;
        description = "Dedicated system group (used when dynamicUser is false). When null and user is matrix-xmsg, defaults to matrix-xmsg. For any other user, systemd uses the user's primary group unless explicitly set.";
      };

      dynamicUser = lib.mkOption {
        type = lib.types.bool;
        default = false;
        description = "Whether to use systemd DynamicUser allocation.";
      };

      xmsg = lib.mkOption {
        default = { };
        description = "Dedicated system-user `xmsg` node (`matrix-xmsg-xmsg.service`) owned by `services.matrix-xmsg.user`.";
        type = lib.types.submodule {
          options = {
            enable = lib.mkEnableOption "dedicated xmsg node for matrix-xmsg";

            package = lib.mkOption {
              type = lib.types.nullOr lib.types.package;
              default = null;
              description = "The xmsg package to run for `matrix-xmsg-xmsg.service` (required when `services.matrix-xmsg.xmsg.enable` is true).";
            };

            svcExe = lib.mkOption {
              type = lib.types.attrsOf lib.types.str;
              default = { };
              example = {
                genie-guard = "/nix/store/.../bin/genie-guard";
              };
              description = ''
                Additional service name to executable path mappings passed as `--svc-exe <name>=<path>`
                to `matrix-xmsg-xmsg.service` (`matrix-xmsg` is registered automatically).
              '';
            };

            federation = {
              enable = lib.mkEnableOption "xmsg federation on matrix-xmsg-xmsg.service";

              listenAddress = lib.mkOption {
                type = lib.types.str;
                default = "0.0.0.0";
                description = "Address to bind for incoming federation connections when `federation.port` is set.";
              };

              port = lib.mkOption {
                type = lib.types.nullOr lib.types.port;
                default = null;
                description = "Port to bind for incoming federation connections (`--fed-listen`). When null, no federation listener is bound (outbound-only federation).";
              };

              cert = lib.mkOption {
                type = lib.types.nullOr (lib.types.either lib.types.path lib.types.str);
                default = null;
                description = "Path to the federation TLS certificate (`--fed-cert`).";
              };

              key = lib.mkOption {
                type = lib.types.nullOr (lib.types.either lib.types.path lib.types.str);
                default = null;
                description = "Runtime path to the federation TLS private key (`--fed-key`), never a `/nix/store` path.";
              };

              reloadInterval = lib.mkOption {
                type = lib.types.nullOr lib.types.ints.positive;
                default = null;
                description = "Optional federation TLS credential and CA reload interval in seconds (`--fed-reload-interval-secs`).";
              };

              peers = lib.mkOption {
                type = lib.types.attrsOf (pkgs.formats.json { }).type;
                default = { };
                description = "Freeform federation peers written to `--peers-file` and validated at build time by `xmsg check-peers`.";
              };
            };
          };
        };
      };
    };

  config = lib.mkIf cfg.enable {
    assertions = (shared.mkCommonAssertions { inherit lib cfg; }) ++ [
      {
        assertion = cfg.xmsgSocket != null -> !cfg.dynamicUser;
        message = "services.matrix-xmsg: when xmsgSocket is configured, dynamicUser must be false because the socket's UID check requires running as the socket's owner. Configure services.matrix-xmsg.user to the socket's owning user.";
      }
      {
        assertion = xmsgCfg.enable -> xmsgCfg.package != null;
        message = "services.matrix-xmsg.xmsg.enable requires services.matrix-xmsg.xmsg.package to be set.";
      }
      {
        assertion = xmsgCfg.enable -> !cfg.dynamicUser;
        message = "services.matrix-xmsg.xmsg.enable requires services.matrix-xmsg.dynamicUser = false.";
      }
      {
        assertion =
          (xmsgCfg.enable && xmsgCfg.federation.enable)
          -> (xmsgCfg.federation.cert != null && xmsgCfg.federation.key != null);
        message = "services.matrix-xmsg.xmsg.federation.enable requires both cert and key to be set.";
      }
      {
        assertion =
          (xmsgCfg.enable && xmsgCfg.federation.enable && xmsgCfg.federation.key != null)
          -> (
            !(builtins.isPath xmsgCfg.federation.key)
            && !builtins.hasContext (toString xmsgCfg.federation.key)
            && !lib.hasPrefix builtins.storeDir (toString xmsgCfg.federation.key)
          );
        message = "services.matrix-xmsg.xmsg.federation.key must be a runtime secret path, never a /nix/store path.";
      }
    ];

    users.users = lib.mkIf (!cfg.dynamicUser && cfg.user == "matrix-xmsg") {
      matrix-xmsg = {
        isSystemUser = true;
        group = if cfg.group != null then cfg.group else "matrix-xmsg";
        description = "matrix-xmsg service daemon user";
        home = cfg.stateDir;
      };
    };

    users.groups =
      lib.mkIf
        (!cfg.dynamicUser && cfg.user == "matrix-xmsg" && (cfg.group == null || cfg.group == "matrix-xmsg"))
        {
          matrix-xmsg = { };
        };

    systemd.services = {
      matrix-xmsg = {
        description = "matrix-xmsg Matrix bot backed by xmsg agent";
        after = [ "network.target" ] ++ lib.optional xmsgCfg.enable "matrix-xmsg-xmsg.service";
        wantedBy = [ "multi-user.target" ];

        serviceConfig = {
          ExecStart = "${lib.getExe cfg.package} --config ${configToml}";
          Restart = "on-failure";
          RestartSec = "5s";

          # User and Directory isolation
          DynamicUser = cfg.dynamicUser;
          User = lib.mkIf (!cfg.dynamicUser) cfg.user;
          Group = lib.mkIf (!cfg.dynamicUser && (cfg.user == "matrix-xmsg" || cfg.group != null)) (
            if cfg.group != null then cfg.group else "matrix-xmsg"
          );
          StateDirectory = "matrix-xmsg";
          RuntimeDirectory = "matrix-xmsg";
          WorkingDirectory = cfg.stateDir;

          # Secure Token Loading via Systemd Credentials
          LoadCredential = [ "access-token:${toString cfg.accessTokenFile}" ];

          # Graceful shutdown margin (drain 5s + notice 5s + margin)
          TimeoutStopSec = 60;

          # Systemd Hardening
          CapabilityBoundingSet = "";
          DevicePolicy = "closed";
          LockPersonality = true;
          MemoryDenyWriteExecute = true;
          NoNewPrivileges = true;
          PrivateDevices = true;
          PrivateTmp = true;
          # PrivateUsers must be false when xmsgSocket is configured so the xmsg daemon
          # can read /proc/<pid>/exe for svc attestation (otherwise refused with Permission denied).
          PrivateUsers = if cfg.xmsgSocket != null then false else true;
          ProtectClock = true;
          ProtectControlGroups = true;
          # ProtectHome=yes hides /run/user, preventing connection to user-owned xmsg sockets.
          # When xmsgSocket is set, ProtectHome is relaxed to tmpfs and BindPaths exposes the socket dir.
          ProtectHome = if cfg.xmsgSocket != null then "tmpfs" else true;
          ProtectHostname = true;
          ProtectKernelLogs = true;
          ProtectKernelModules = true;
          ProtectKernelTunables = true;
          ProtectProc = "invisible";
          ProtectSystem = "strict";
          RemoveIPC = true;
          RestrictAddressFamilies = [
            "AF_INET"
            "AF_INET6"
            "AF_UNIX"
          ];
          RestrictNamespaces = true;
          RestrictRealtime = true;
          RestrictSUIDSGID = true;
          SystemCallArchitectures = "native";
          SystemCallFilter = [
            "@system-service"
            "~@privileged"
          ];
          UMask = "0077";
        }
        // lib.optionalAttrs (cfg.xmsgSocket != null) {
          BindPaths = [ (toString cfg.xmsgSocket) ];
        };
      };
    }
    // lib.optionalAttrs xmsgCfg.enable {
      matrix-xmsg-xmsg = {
        description = "xmsg daemon for matrix-xmsg";
        after = [ "network.target" ];
        before = [ "matrix-xmsg.service" ];
        requiredBy = [ "matrix-xmsg.service" ];
        wantedBy = [ "multi-user.target" ];
        restartTriggers = lib.optional xmsgCfg.federation.enable peersFile;

        environment = {
          HOME = toString cfg.stateDir;
          XDG_RUNTIME_DIR = "/run/matrix-xmsg-xmsg";
          XMSG_DB_PATH = "/var/lib/matrix-xmsg-xmsg/xmsg.db";
        };

        serviceConfig = {
          Type = "simple";
          User = cfg.user;
          Group = if cfg.group != null then cfg.group else "matrix-xmsg";
          RuntimeDirectory = [
            "matrix-xmsg-xmsg"
            "matrix-xmsg-xmsg/xmsg"
          ];
          RuntimeDirectoryMode = "0700";
          StateDirectory = "matrix-xmsg-xmsg";
          StateDirectoryMode = "0700";
          ExecStart = "${lib.getExe xmsgCfg.package} ${lib.escapeShellArgs xmsgArgs}";
          Restart = "on-failure";
          RestartSec = "2s";
        };
      };
    };
  };
}
