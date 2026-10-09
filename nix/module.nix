{ self }:
{
  config,
  lib,
  pkgs,
  ...
}:

let
  cfg = config.services.matrix-xmsg;

  # Format config.toml matching matrix_xmsg::config::Config
  configToml = (pkgs.formats.toml { }).generate "matrix-xmsg-config.toml" {
    homeserver_url = cfg.homeserverUrl;
    bot_mxid = cfg.botMxid;
    # Systemd LoadCredential mounts the secret at /run/credentials/matrix-xmsg.service/access-token
    access_token_file = "/run/credentials/matrix-xmsg.service/access-token";
    rooms = cfg.rooms;
    trusted_mxids = cfg.trustedMxids;
    owner_mxid = cfg.ownerMxid;
    xmsg_socket = toString cfg.xmsgSocket;
    expert_ref = cfg.expertRef;
    history_n = cfg.historyN;
    history_byte_cap = cfg.historyByteCap;
    resync_byte_cap = cfg.resyncByteCap;
    rate_limit_count = cfg.rateLimitCount;
    rate_limit_window_secs = cfg.rateLimitWindowSecs;
    size_cap_bytes = cfg.sizeCapBytes;
    answer_timeout_secs = cfg.answerTimeoutSecs;
    session_live_secs = cfg.sessionLiveSecs;
    db_path = cfg.dbPath;
  };
in
{
  options.services.matrix-xmsg = {
    enable = lib.mkEnableOption "matrix-xmsg Matrix bot backed by an expert agent session";

    package = lib.mkOption {
      type = lib.types.package;
      default = self.packages.${pkgs.system}.default;
      defaultText = lib.literalExpression "self.packages.\${pkgs.system}.default";
      description = "The matrix-xmsg package to use.";
    };

    homeserverUrl = lib.mkOption {
      type = lib.types.str;
      example = "https://matrix.org";
      description = "Base URL of the Matrix homeserver.";
    };

    botMxid = lib.mkOption {
      type = lib.types.str;
      example = "@genie:example.org";
      description = "Full Matrix user ID of the bot.";
    };

    accessTokenFile = lib.mkOption {
      type = lib.types.path;
      example = "/run/secrets/matrix-token";
      description = ''
        Path to file containing the Matrix access token.
        Loaded securely at service startup via systemd LoadCredential;
        never copied into the world-readable /nix/store.
      '';
    };

    rooms = lib.mkOption {
      type = lib.types.listOf lib.types.str;
      default = [ ];
      example = [ "!roomid:example.org" ];
      description = ''
        List of Matrix room IDs the bot is permitted to monitor.
        Must contain canonical room IDs (starting with '!'), not aliases (starting with '#').
      '';
    };

    trustedMxids = lib.mkOption {
      type = lib.types.listOf lib.types.str;
      default = [ ];
      example = [ "@admin:example.org" ];
      description = "List of Matrix user IDs trusted to interact with the bot.";
    };

    ownerMxid = lib.mkOption {
      type = lib.types.str;
      example = "@owner:example.org";
      description = "Matrix user ID of the bot owner for notifications and direct control.";
    };

    stateDir = lib.mkOption {
      type = lib.types.path;
      default = "/var/lib/matrix-xmsg";
      description = "Directory for matrix-xmsg persistent state and SQLite database.";
    };

    xmsgSocket = lib.mkOption {
      type = lib.types.either lib.types.path lib.types.str;
      default = "/run/user/1000/xmsg";
      example = "/run/user/1000/xmsg";
      description = ''
        Path to the xmsg runtime directory containing register.sock and agent.sock.
        The bot connects to register.sock as svc:matrix-xmsg and sends forwards on agent.sock.
        Because svc attests the peer UID, services.matrix-xmsg.dynamicUser must be false,
        and services.matrix-xmsg.user must be configured to the runtime directory's owning user.
        Note: The xmsg daemon instance must pass `--svc-exe matrix-xmsg=<this package's binary>`
        to authorize the service registration.
      '';
    };

    expertRef = lib.mkOption {
      type = lib.types.str;
      default = "claude";
      description = "Reference name of the expert agent session in xmsg.";
    };

    historyN = lib.mkOption {
      type = lib.types.ints.unsigned;
      default = 30;
      description = "Number of recent room messages to fetch for context framing.";
    };

    historyByteCap = lib.mkOption {
      type = lib.types.ints.unsigned;
      default = 12288;
      description = "Byte budget cap for message context history.";
    };

    resyncByteCap = lib.mkOption {
      type = lib.types.ints.unsigned;
      default = 65536;
      description = "Maximum byte budget for thread resync transcripts.";
    };

    rateLimitCount = lib.mkOption {
      type = lib.types.ints.unsigned;
      default = 10;
      description = "Maximum queries allowed per rate limit window per user.";
    };

    rateLimitWindowSecs = lib.mkOption {
      type = lib.types.ints.unsigned;
      default = 600;
      description = "Sliding rate limit window in seconds.";
    };

    sizeCapBytes = lib.mkOption {
      type = lib.types.ints.unsigned;
      default = 4096;
      description = "Maximum message size accepted for processing.";
    };

    answerTimeoutSecs = lib.mkOption {
      type = lib.types.ints.unsigned;
      default = 300;
      description = "Timeout in seconds to wait for an expert reply before escalating.";
    };

    sessionLiveSecs = lib.mkOption {
      type = lib.types.ints.unsigned;
      default = 3600;
      description = "Duration in seconds before an idle thread/session context is re-bootstrapped.";
    };

    dbPath = lib.mkOption {
      type = lib.types.path;
      default = "${cfg.stateDir}/matrix-xmsg.db";
      description = "Path to the SQLite database file.";
    };

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
    assertions = [
      {
        assertion = lib.all (r: lib.hasPrefix "!" r) cfg.rooms;
        message = ''
          services.matrix-xmsg.rooms must contain Matrix room IDs (starting with '!'), not aliases (starting with '#').
          Aliases must be resolved to room IDs before configuring matrix-xmsg.
        '';
      }
      {
        assertion = !cfg.dynamicUser;
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
        ExecStart = "${cfg.package}/bin/matrix-xmsg --config ${configToml}";
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
        PrivateUsers = true;
        ProtectClock = true;
        ProtectControlGroups = true;
        ProtectHome = true;
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
      };
    };
  };
}
