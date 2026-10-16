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
  shared = import ./options.nix;

  # Systemd LoadCredential mounts the secret at /run/credentials/matrix-xmsg.service/access-token
  configToml = shared.mkConfigToml {
    inherit lib pkgs;
  } cfg "/run/credentials/matrix-xmsg.service/access-token";
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
      defaultXmsgSocket = "/run/user/1000/xmsg";
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
        default = true;
        description = "Whether to use systemd DynamicUser allocation.";
      };
    };

  config = lib.mkIf cfg.enable {
    assertions = (shared.mkCommonAssertions { inherit lib cfg; }) ++ [
      {
        assertion = cfg.xmsgSocket != null -> !cfg.dynamicUser;
        message = "services.matrix-xmsg: when xmsgSocket is configured, dynamicUser must be false because the socket's UID check requires running as the socket's owner. Configure services.matrix-xmsg.user to the socket's owning user.";
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

    systemd.services.matrix-xmsg = {
      description = "matrix-xmsg Matrix bot backed by xmsg agent";
      after = [ "network.target" ];
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
  };
}
