# Shared option set and config.toml renderer for `nixosModules.default` and `homeManagerModules.default`.
{
  mkCommonOptions =
    {
      lib,
      pkgs,
      packages,
      cfg,
      defaultStateDir,
      defaultStateDirText ? null,
      defaultXmsgSocket,
      defaultXmsgSocketText ? null,
      accessTokenDescription,
    }:
    {
      enable = lib.mkEnableOption "matrix-xmsg Matrix bot backed by an expert agent session";

      package = lib.mkOption {
        type = lib.types.package;
        default = packages.${pkgs.stdenv.hostPlatform.system}.default;
        defaultText = lib.literalExpression "matrix-xmsg.packages.\${system}.default";
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
        type = lib.types.addCheck lib.types.str (
          s: !builtins.hasContext s && !lib.hasPrefix builtins.storeDir s && lib.hasPrefix "/" s
        );
        example = "/run/secrets/matrix-token";
        description = accessTokenDescription;
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

      admission = lib.mkOption {
        type = lib.types.enum [
          "trusted"
          "public"
        ];
        default = "trusted";
        description = "Room admission policy (`\"trusted\"` or `\"public\"`).";
      };

      stateDir = lib.mkOption (
        {
          type = lib.types.path;
          default = defaultStateDir;
          description = "Directory for matrix-xmsg persistent state and SQLite database.";
        }
        // lib.optionalAttrs (defaultStateDirText != null) {
          defaultText = defaultStateDirText;
        }
      );

      xmsgSocket = lib.mkOption (
        {
          type = lib.types.nullOr (lib.types.either lib.types.path lib.types.str);
          default = defaultXmsgSocket;
          example = "/run/matrix-xmsg-xmsg/xmsg";
          description = ''
            Path to the xmsg runtime directory containing register.sock and agent.sock.
            The bot registers on register.sock as the service matrix-xmsg and sends forwards on agent.sock.
          '';
        }
        // lib.optionalAttrs (defaultXmsgSocketText != null) {
          defaultText = defaultXmsgSocketText;
        }
      );

      expertRef = lib.mkOption {
        type = lib.types.str;
        default = "claude";
        description = "The expert's xmsg address: a label, optionally with @<node>.";
      };

      expertRefs = lib.mkOption {
        type = lib.types.nullOr (
          lib.types.submodule {
            options = {
              public = lib.mkOption {
                type = lib.types.str;
                example = "genie-expert@genie-public.bitstream";
                description = "Expert xmsg address for public-tier forwards.";
              };
              trusted = lib.mkOption {
                type = lib.types.str;
                example = "genie-expert@genie-trusted.bitstream";
                description = "Expert xmsg address for trusted-tier forwards (including owner and !trust-promoted threads).";
              };
            };
          }
        );
        default = null;
        description = ''
          Per-tier expert xmsg addresses (`public` and `trusted`).
          When set, overrides `expertRef`: public-tier forwards address `expertRefs.public`
          and trusted-tier forwards (including owner and `!trust`-promoted threads) address `expertRefs.trusted`.
        '';
      };

      roomRoutes = lib.mkOption {
        type = lib.types.attrsOf (
          lib.types.submodule {
            options = {
              expertRef = lib.mkOption {
                type = lib.types.nullOr lib.types.str;
                default = null;
                description = "Per-room override for expertRef.";
              };
              expertRefs = lib.mkOption {
                type = lib.types.nullOr (
                  lib.types.submodule {
                    options = {
                      public = lib.mkOption {
                        type = lib.types.str;
                        example = "genie-expert@genie-public.bitstream";
                        description = "Per-room expert xmsg address for public-tier forwards.";
                      };
                      trusted = lib.mkOption {
                        type = lib.types.str;
                        example = "genie-expert@genie-trusted.bitstream";
                        description = "Per-room expert xmsg address for trusted-tier forwards.";
                      };
                    };
                  }
                );
                default = null;
                description = "Per-room override for expertRefs (`public` and `trusted`).";
              };
              screenMentions = lib.mkOption {
                type = lib.types.nullOr lib.types.bool;
                default = null;
                description = "Per-room override for screenMentions.";
              };
              screenSolicited = lib.mkOption {
                type = lib.types.nullOr lib.types.bool;
                default = null;
                description = "Per-room override for screenSolicited.";
              };
            };
          }
        );
        default = { };
        description = ''
          Per-room routing overrides keyed by Matrix room ID.
          Unset fields fall back to the global `expertRef`, `expertRefs`, `screenMentions`, and `screenSolicited`.
        '';
      };

      openThreadNodes = lib.mkOption {
        type = lib.types.listOf lib.types.str;
        default = [ ];
        example = [ "sini.bitstream" ];
        description = ''
          List of xmsg node names permitted to invoke `open_thread`.
          When empty, only senders on the bot's own xmsg node may invoke `open_thread`.
        '';
      };

      guardRef = lib.mkOption {
        type = lib.types.nullOr lib.types.str;
        default = "genie-guard";
        example = "genie-guard@sini.bitstream";
        description = ''
          The guard service's xmsg address: a bare name, `name@node`, or `null` when both
          `screenMentions` and `screenSolicited` are disabled across all rooms.
          Set `@node` when two guards are in view: a bare name then resolves `ambiguous`
          and every guard check fails.
        '';
      };

      guardTimeoutSecs = lib.mkOption {
        type = lib.types.ints.unsigned;
        default = 150;
        description = "Timeout in seconds when waiting for a guard screening verdict.";
      };

      guardRetryBudget = lib.mkOption {
        type = lib.types.ints.unsigned;
        default = 3;
        description = "Maximum attempts to screen an incoming event when guardFailureMode is \"closed\".";
      };

      screenMentions = lib.mkOption {
        type = lib.types.bool;
        default = true;
        description = ''
          Whether to screen regular mentions and thread replies through `guardRef` before
          forwarding to the expert.
        '';
      };

      screenSolicited = lib.mkOption {
        type = lib.types.bool;
        default = true;
        description = ''
          Whether to screen messages in `solicited` threads opened via `open_thread` through
          `guardRef` before relaying to the requesting session.
        '';
      };

      guardFailureMode = lib.mkOption {
        type = lib.types.enum [
          "closed"
          "open"
        ];
        default = "closed";
        description = ''
          Behavior when the guard service times out or errors:
          - `"closed"` (default): forward nothing, retry on next delivery up to `guard_retry_budget`, and DM `ownerMxid` once at budget.
          - `"open"`: forward immediately wrapped in `screened="false"` warning tags and DM `ownerMxid` at most once per `guardOutageDmWindowSecs`.
        '';
      };

      guardOutageDmWindowSecs = lib.mkOption {
        type = lib.types.ints.unsigned;
        default = 3600;
        description = ''
          Rate-limit window in seconds for owner DMs when `guardFailureMode = "open"` and the guard service is unavailable.
        '';
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

      resyncPageBytes = lib.mkOption {
        type = lib.types.ints.unsigned;
        default = 8192;
        description = "Maximum byte size per page for thread resync responses.";
      };

      contextMaxLines = lib.mkOption {
        type = lib.types.ints.unsigned;
        default = 20;
        description = "Maximum number of lines returned per room context request.";
      };

      contextMaxBytes = lib.mkOption {
        type = lib.types.ints.unsigned;
        default = 8192;
        description = "Maximum byte size per page for context request responses.";
      };

      senderWindowSecs = lib.mkOption {
        type = lib.types.ints.unsigned;
        default = 900;
        description = "Time window in seconds before a trigger within which a sender's own top-level lines are attached or queried.";
      };

      contextRateLimitCount = lib.mkOption {
        type = lib.types.ints.unsigned;
        default = 6;
        description = "Maximum context requests allowed per rate limit window per thread.";
      };

      contextRateLimitWindowSecs = lib.mkOption {
        type = lib.types.ints.unsigned;
        default = 600;
        description = "Sliding rate limit window in seconds for context requests per thread.";
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
        default = 600;
        description = "Timeout in seconds to wait for an expert reply before escalating.";
      };

      sessionLiveSecs = lib.mkOption {
        type = lib.types.ints.unsigned;
        default = 3600;
        description = "Duration in seconds before an idle thread/session context is re-bootstrapped.";
      };

      inboxRetryBudget = lib.mkOption {
        type = lib.types.ints.unsigned;
        default = 10;
        description = "Maximum attempts to process a failing inbox delivery before giving up and notifying sender and owner.";
      };

      dbPath = lib.mkOption {
        type = lib.types.path;
        default = "${cfg.stateDir}/matrix-xmsg.db";
        defaultText = lib.literalExpression "\"\${cfg.stateDir}/matrix-xmsg.db\"";
        description = "Path to the SQLite database file.";
      };
    };

  mkCommonAssertions =
    { lib, cfg }:
    [
      {
        assertion = lib.all (r: lib.hasPrefix "!" r) cfg.rooms;
        message = ''
          services.matrix-xmsg.rooms must contain Matrix room IDs (starting with '!'), not aliases (starting with '#').
          Aliases must be resolved to room IDs before configuring matrix-xmsg.
        '';
      }
      {
        assertion = lib.all (r: builtins.elem r cfg.rooms) (lib.attrNames cfg.roomRoutes);
        message = ''
          services.matrix-xmsg.roomRoutes keys must all be present in services.matrix-xmsg.rooms.
        '';
      }
      {
        assertion =
          let
            mentionsOn =
              cfg.screenMentions || lib.any (r: r.screenMentions == true) (lib.attrValues cfg.roomRoutes);
            solicitedOn =
              cfg.screenSolicited || lib.any (r: r.screenSolicited == true) (lib.attrValues cfg.roomRoutes);
          in
          !(mentionsOn || solicitedOn) || (cfg.guardRef != null && cfg.guardRef != "");
        message = ''
          services.matrix-xmsg.guardRef must be set when services.matrix-xmsg.screenMentions or services.matrix-xmsg.screenSolicited is enabled (globally or in roomRoutes).
        '';
      }
    ];

  mkConfigAttrs =
    { lib }:
    cfg: accessTokenFile:
    {
      homeserver_url = cfg.homeserverUrl;
      bot_mxid = cfg.botMxid;
      access_token_file = accessTokenFile;
      rooms = cfg.rooms;
      trusted_mxids = cfg.trustedMxids;
      owner_mxid = cfg.ownerMxid;
      admission = cfg.admission;
      expert_ref = cfg.expertRef;
      guard_timeout_secs = cfg.guardTimeoutSecs;
      guard_retry_budget = cfg.guardRetryBudget;
      screen_mentions = cfg.screenMentions;
      screen_solicited = cfg.screenSolicited;
      guard_failure_mode = cfg.guardFailureMode;
      guard_outage_dm_window_secs = cfg.guardOutageDmWindowSecs;
      history_n = cfg.historyN;
      history_byte_cap = cfg.historyByteCap;
      resync_byte_cap = cfg.resyncByteCap;
      resync_page_bytes = cfg.resyncPageBytes;
      context_max_lines = cfg.contextMaxLines;
      context_max_bytes = cfg.contextMaxBytes;
      sender_window_secs = cfg.senderWindowSecs;
      context_rate_limit_count = cfg.contextRateLimitCount;
      context_rate_limit_window_secs = cfg.contextRateLimitWindowSecs;
      rate_limit_count = cfg.rateLimitCount;
      rate_limit_window_secs = cfg.rateLimitWindowSecs;
      size_cap_bytes = cfg.sizeCapBytes;
      answer_timeout_secs = cfg.answerTimeoutSecs;
      session_live_secs = cfg.sessionLiveSecs;
      inbox_retry_budget = cfg.inboxRetryBudget;
      db_path = toString cfg.dbPath;
    }
    // lib.optionalAttrs (cfg.guardRef != null) {
      guard_ref = cfg.guardRef;
    }
    // lib.optionalAttrs (cfg.xmsgSocket != null) {
      xmsg_socket = toString cfg.xmsgSocket;
    }
    // lib.optionalAttrs (cfg.expertRefs != null) {
      expert_refs = {
        inherit (cfg.expertRefs) public trusted;
      };
    }
    // lib.optionalAttrs (cfg.roomRoutes != { }) {
      room_routes = lib.mapAttrs (
        _: route:
        lib.optionalAttrs (route.expertRef != null) {
          expert_ref = route.expertRef;
        }
        // lib.optionalAttrs (route.expertRefs != null) {
          expert_refs = {
            inherit (route.expertRefs) public trusted;
          };
        }
        // lib.optionalAttrs (route.screenMentions != null) {
          screen_mentions = route.screenMentions;
        }
        // lib.optionalAttrs (route.screenSolicited != null) {
          screen_solicited = route.screenSolicited;
        }
      ) cfg.roomRoutes;
    }
    // lib.optionalAttrs (cfg.openThreadNodes != [ ]) {
      open_thread_nodes = cfg.openThreadNodes;
    };

  mkConfigToml =
    { lib, pkgs }:
    cfg: accessTokenFile:
    let
      attrs = (import ./options.nix).mkConfigAttrs { inherit lib; } cfg accessTokenFile;
    in
    ((pkgs.formats.toml { }).generate "matrix-xmsg-config.toml" attrs).overrideAttrs (old: {
      passthru = (old.passthru or { }) // {
        inherit attrs;
      };
    });
}
