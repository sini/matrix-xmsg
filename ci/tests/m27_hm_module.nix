{ lib, inputs, ... }:
let
  inherit (inputs)
    matrix-xmsg
    xmsg
    home-manager
    nixpkgs
    ;

  shared = import ../../nix/options.nix;
  xmsgNoCheck = system: xmsg.packages.${system}.default.overrideAttrs { doCheck = false; };

  hmEval =
    system: withXmsg: extra:
    let
      pkgs = nixpkgs.legacyPackages.${system};
      hmLib = import "${home-manager}/modules/lib/stdlib-extended.nix" nixpkgs.lib;
    in
    hmLib.evalModules {
      class = "homeManager";
      specialArgs.modulesPath = "${home-manager}/modules";
      modules =
        import "${home-manager}/modules/modules.nix" {
          inherit pkgs;
          lib = hmLib;
          check = true;
        }
        ++ lib.optional withXmsg xmsg.homeManagerModules.xmsg
        ++ [
          matrix-xmsg.homeManagerModules.default
          (
            { pkgs, ... }:
            {
              home = {
                username = "alice";
                homeDirectory = if pkgs.stdenv.hostPlatform.isDarwin then "/Users/alice" else "/home/alice";
                stateVersion = "25.11";
                enableNixpkgsReleaseCheck = false;
              };
              services.matrix-xmsg = lib.mapAttrs (_: lib.mkDefault) {
                enable = true;
                homeserverUrl = "https://matrix.example.org";
                botMxid = "@genie:example.org";
                accessTokenFile = "/run/secrets/matrix-token";
                rooms = [ "!support:example.org" ];
                trustedMxids = [ "@trusted:example.org" ];
                ownerMxid = "@owner:example.org";
              };
            }
            // lib.optionalAttrs withXmsg {
              programs.xmsg = {
                enable = true;
                package = xmsgNoCheck system;
              };
            }
          )
          extra
        ];
    };

  nixosEval =
    extra:
    let
      pkgs = nixpkgs.legacyPackages.x86_64-linux;
      baseModule = {
        options = {
          assertions = lib.mkOption {
            type = lib.types.listOf lib.types.attrs;
            default = [ ];
          };
          users.users = lib.mkOption {
            type = lib.types.attrsOf lib.types.unspecified;
            default = { };
          };
          users.groups = lib.mkOption {
            type = lib.types.attrs;
            default = { };
          };
          systemd.services = lib.mkOption {
            type = lib.types.attrsOf lib.types.unspecified;
            default = { };
          };
        };
      };
    in
    lib.evalModules {
      specialArgs = { inherit pkgs; };
      modules = [
        baseModule
        matrix-xmsg.nixosModules.default
        {
          services.matrix-xmsg = lib.mapAttrs (_: lib.mkDefault) {
            enable = true;
            homeserverUrl = "https://matrix.example.org";
            botMxid = "@genie:example.org";
            accessTokenFile = "/run/secrets/matrix-token";
            rooms = [ "!support:example.org" ];
            trustedMxids = [ "@trusted:example.org" ];
            ownerMxid = "@owner:example.org";
            dynamicUser = false;
          };
        }
        {
          services.matrix-xmsg = extra;
        }
      ];
    };

  failedAssertions = cfg: map (a: a.message) (lib.filter (a: !a.assertion) cfg.assertions);

  tokenAccepted =
    token:
    let
      c =
        (hmEval "x86_64-linux" true {
          services.matrix-xmsg.accessTokenFile = lib.mkForce token;
        }).config;
    in
    (builtins.tryEval (builtins.deepSeq c.services.matrix-xmsg.accessTokenFile true)).success;

  linuxCfg = (hmEval "x86_64-linux" true { }).config;
  linuxExe = lib.getExe matrix-xmsg.packages.x86_64-linux.default;
  xmsgLinuxExe = lib.getExe (xmsgNoCheck "x86_64-linux");

  darwinCfg = (hmEval "aarch64-darwin" true { }).config;
  darwinExe = lib.getExe matrix-xmsg.packages.aarch64-darwin.default;

  sharedOverrideValues = {
    homeserverUrl = "https://matrix.custom.org";
    botMxid = "@bot:custom.org";
    accessTokenFile = "/run/secrets/custom-token";
    rooms = [
      "!r1:custom.org"
      "!r2:custom.org"
    ];
    trustedMxids = [ "@alice:custom.org" ];
    ownerMxid = "@owner:custom.org";
    admission = "public";
    stateDir = "/tmp/shared-state";
    xmsgSocket = "/tmp/shared-xmsg";
    expertRef = "expert@node";
    expertRefs = {
      public = "genie-expert@genie-public.bitstream";
      trusted = "genie-expert@genie-trusted.bitstream";
    };
    roomRoutes = {
      "!r2:custom.org" = {
        expertRef = "genie";
        screenMentions = false;
        screenSolicited = false;
      };
    };
    openThreadNodes = [ "sini.bitstream" ];
    guardRef = "guard@node";
    guardTimeoutSecs = 99;
    guardRetryBudget = 5;
    screenMentions = false;
    screenSolicited = false;
    guardFailureMode = "open";
    guardOutageDmWindowSecs = 600;
    historyN = 15;
    historyByteCap = 8192;
    resyncByteCap = 32768;
    resyncPageBytes = 4096;
    contextMaxLines = 10;
    contextMaxBytes = 4096;
    senderWindowSecs = 300;
    contextRateLimitCount = 3;
    contextRateLimitWindowSecs = 120;
    rateLimitCount = 20;
    rateLimitWindowSecs = 300;
    sizeCapBytes = 2048;
    answerTimeoutSecs = 120;
    sessionLiveSecs = 1800;
    inboxRetryBudget = 7;
    storePassphraseFile = "/run/secrets/custom-store-passphrase";
    recoveryKeyFile = "/run/secrets/custom-recovery-key";
    resetCryptoStore = true;
    undecryptableWaitSecs = 15;
    dbPath = "/tmp/shared-state/custom.db";
  };

  nixosWithOverrides = nixosEval sharedOverrideValues;
  hmWithOverrides = hmEval "x86_64-linux" true {
    services.matrix-xmsg = sharedOverrideValues;
  };
in
{
  flake.tests.m27-hm-module = {
    test-exports-aliases = {
      expr = {
        hmDefault = matrix-xmsg.homeManagerModules ? default;
        hmNamed = matrix-xmsg.homeManagerModules ? matrix-xmsg;
        nixosDefault = matrix-xmsg.nixosModules ? default;
        nixosNamed = matrix-xmsg.nixosModules ? matrix-xmsg;
        hmAnswerTimeoutSecs = linuxCfg.services.matrix-xmsg.answerTimeoutSecs;
        nixosAnswerTimeoutSecs = (nixosEval { }).config.services.matrix-xmsg.answerTimeoutSecs;
      };
      expected = {
        hmDefault = true;
        hmNamed = true;
        nixosDefault = true;
        nixosNamed = true;
        hmAnswerTimeoutSecs = 600;
        nixosAnswerTimeoutSecs = 600;
      };
    };

    # Oracle 1: One option set, two modules — zero drift between NixOS and Home Manager.
    test-zero-drift-shared-options-and-config = {
      expr = {
        hmAttrNames = builtins.attrNames hmWithOverrides.options.services.matrix-xmsg;
        expectedAttrNames = lib.subtractLists [
          "dynamicUser"
          "group"
          "user"
          "xmsg"
        ] (builtins.attrNames nixosWithOverrides.options.services.matrix-xmsg);
        nixosConfigAttrs = shared.mkConfigAttrs {
          inherit lib;
        } nixosWithOverrides.config.services.matrix-xmsg "/run/secrets/custom-token";
        hmConfigAttrs = shared.mkConfigAttrs {
          inherit lib;
        } hmWithOverrides.config.services.matrix-xmsg "/run/secrets/custom-token";
        screenMentions = hmWithOverrides.config.services.matrix-xmsg.screenMentions;
        guardFailureMode = hmWithOverrides.config.services.matrix-xmsg.guardFailureMode;
      };
      expected = {
        hmAttrNames = lib.subtractLists [
          "dynamicUser"
          "group"
          "user"
          "xmsg"
        ] (builtins.attrNames nixosWithOverrides.options.services.matrix-xmsg);
        expectedAttrNames = lib.subtractLists [
          "dynamicUser"
          "group"
          "user"
          "xmsg"
        ] (builtins.attrNames nixosWithOverrides.options.services.matrix-xmsg);
        nixosConfigAttrs = shared.mkConfigAttrs {
          inherit lib;
        } hmWithOverrides.config.services.matrix-xmsg "/run/secrets/custom-token";
        hmConfigAttrs = shared.mkConfigAttrs {
          inherit lib;
        } nixosWithOverrides.config.services.matrix-xmsg "/run/secrets/custom-token";
        screenMentions = false;
        guardFailureMode = "open";
      };
    };

    # Oracle 2a: Linux Home-Manager evaluation + svcExe wiring + socket inheritance.
    test-hm-linux-unit-and-svcexe = {
      expr = {
        hasUnit = linuxCfg.systemd.user.services ? matrix-xmsg;
        after = linuxCfg.systemd.user.services.matrix-xmsg.Unit.After;
        wantedBy = linuxCfg.systemd.user.services.matrix-xmsg.Install.WantedBy;
        restart = linuxCfg.systemd.user.services.matrix-xmsg.Service.Restart;
        triggerCount = builtins.length linuxCfg.systemd.user.services.matrix-xmsg.Unit.X-Restart-Triggers;
        firstTrigger = builtins.head linuxCfg.systemd.user.services.matrix-xmsg.Unit.X-Restart-Triggers;
        svcExe = linuxCfg.programs.xmsg.server.svcExe.matrix-xmsg;
        xmsgExec = linuxCfg.systemd.user.services.xmsg.Service.ExecStart;
        dbPath = toString linuxCfg.services.matrix-xmsg.dbPath;
        defaultSocketNull = linuxCfg.services.matrix-xmsg.xmsgSocket;
        inheritedSocket =
          (hmEval "x86_64-linux" true {
            programs.xmsg.server.httpSocket = "/custom/runtime/xmsg/http.sock";
          }).config.services.matrix-xmsg.xmsgSocket;
        warnSuggest =
          (hmEval "x86_64-linux" true {
            systemd.user.startServices = "suggest";
            programs.xmsg.server.enable = false;
          }).config.warnings;
        warnSdSwitch =
          (hmEval "x86_64-linux" true {
            systemd.user.startServices = "sd-switch";
          }).config.warnings;
        hasLaunchd = linuxCfg.launchd.agents ? matrix-xmsg;
        failed = failedAssertions linuxCfg;
      };
      expected = {
        hasUnit = true;
        after = [
          "network.target"
          "xmsg.service"
        ];
        wantedBy = [ "default.target" ];
        restart = "on-failure";
        triggerCount = 2;
        firstTrigger = "${matrix-xmsg.packages.x86_64-linux.default}";
        svcExe = linuxExe;
        xmsgExec = [
          "${xmsgLinuxExe} serve --svc-exe 'matrix-xmsg=${linuxExe}'"
        ];
        dbPath = "/home/alice/.local/state/matrix-xmsg/matrix-xmsg.db";
        defaultSocketNull = null;
        inheritedSocket = "/custom/runtime/xmsg";
        warnSuggest = [
          "services.matrix-xmsg.enable: systemd.user.startServices is not \"sd-switch\", so a home-manager switch will not restart matrix-xmsg.service."
        ];
        warnSdSwitch = [ ];
        hasLaunchd = false;
        failed = [ ];
      };
    };

    # Oracle 2b: Darwin Home-Manager evaluation + launchd agent + svcExe wiring.
    test-hm-darwin-agent-and-svcexe = {
      expr = {
        launchdEnable = darwinCfg.launchd.agents.matrix-xmsg.enable or false;
        launchdArgsPrefix = lib.take 2 (
          darwinCfg.launchd.agents.matrix-xmsg.config.ProgramArguments or [ ]
        );
        keepAliveSuccessfulExit = darwinCfg.launchd.agents.matrix-xmsg.config.KeepAlive.SuccessfulExit;
        runAtLoad = darwinCfg.launchd.agents.matrix-xmsg.config.RunAtLoad;
        hasSystemd = darwinCfg.systemd.user.services ? matrix-xmsg;
        svcExe = darwinCfg.programs.xmsg.server.svcExe.matrix-xmsg;
        dbPath = toString darwinCfg.services.matrix-xmsg.dbPath;
        failed = failedAssertions darwinCfg;
      };
      expected = {
        launchdEnable = true;
        launchdArgsPrefix = [
          darwinExe
          "--config"
        ];
        keepAliveSuccessfulExit = false;
        runAtLoad = true;
        hasSystemd = false;
        svcExe = darwinExe;
        dbPath = "/Users/alice/.local/state/matrix-xmsg/matrix-xmsg.db";
        failed = [ ];
      };
    };

    # Oracle 2c: Standalone evaluation without xmsg's Home-Manager module imported.
    test-hm-standalone-without-xmsg-module =
      let
        standalone =
          (hmEval "x86_64-linux" false {
            services.matrix-xmsg.xmsgSocket = "/run/user/1000/xmsg";
          }).config;
      in
      {
        expr = {
          after = standalone.systemd.user.services.matrix-xmsg.Unit.After;
          xmsgSocket = standalone.services.matrix-xmsg.xmsgSocket;
          failed = failedAssertions standalone;
        };
        expected = {
          after = [ "network.target" ];
          xmsgSocket = "/run/user/1000/xmsg";
          failed = [ ];
        };
      };

    # Oracle 2d: Token file path option refuses inline strings and /nix/store paths.
    test-hm-access-token-file-rejects-inline-and-store-paths = {
      expr = {
        runtimePath = tokenAccepted "/run/secrets/matrix-token";
        inlineSecret = tokenAccepted "syt_secret_token_inline";
        storePrefix = tokenAccepted "${builtins.storeDir}/00000000000000000000000000000000-matrix-token";
        storeContext = tokenAccepted "${nixpkgs.legacyPackages.x86_64-linux.writeText "tok" "secret"}";
      };
      expected = {
        runtimePath = true;
        inlineSecret = false;
        storePrefix = false;
        storeContext = false;
      };
    };
  };
}
