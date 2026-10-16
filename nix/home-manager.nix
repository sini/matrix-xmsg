{
  self ? null,
  packages ? self.packages,
}:
{
  config,
  lib,
  options,
  pkgs,
  ...
}:

let
  cfg = config.services.matrix-xmsg;
  shared = import ./options.nix;
  exe = lib.getExe cfg.package;

  xmsgEnabled =
    (options ? programs.xmsg) && config.programs.xmsg.enable && config.programs.xmsg.server.enable;

  defaultXmsgSocket =
    if xmsgEnabled && config.programs.xmsg.server.httpSocket != null then
      builtins.dirOf config.programs.xmsg.server.httpSocket
    else
      null;

  configToml = shared.mkConfigToml { inherit lib pkgs; } cfg (toString cfg.accessTokenFile);
in
{
  options.services.matrix-xmsg = shared.mkCommonOptions {
    inherit
      lib
      pkgs
      packages
      cfg
      defaultXmsgSocket
      ;
    defaultStateDir = "${config.xdg.stateHome}/matrix-xmsg";
    defaultStateDirText = lib.literalExpression "\"\${config.xdg.stateHome}/matrix-xmsg\"";
    defaultXmsgSocketText = lib.literalExpression "builtins.dirOf config.programs.xmsg.server.httpSocket when set, otherwise null (runtime default)";
    accessTokenDescription = ''
      Runtime path to file (mode 0600 or 0400) containing the Matrix access token.
      Never an inline secret or a /nix/store path.
    '';
  };

  config = lib.mkIf cfg.enable (
    lib.mkMerge [
      {
        assertions = shared.mkCommonAssertions { inherit lib cfg; };

        warnings =
          lib.optional (pkgs.stdenv.hostPlatform.isLinux && !config.systemd.user.startServices)
            "services.matrix-xmsg.enable: systemd.user.startServices is not \"sd-switch\", so a home-manager switch will not restart matrix-xmsg.service.";
      }

      (lib.optionalAttrs (options ? programs.xmsg) (
        lib.mkIf config.programs.xmsg.enable {
          programs.xmsg.server.svcExe.matrix-xmsg = exe;
        }
      ))

      (lib.mkIf pkgs.stdenv.hostPlatform.isLinux {
        systemd.user.services.matrix-xmsg = {
          Unit = {
            Description = "matrix-xmsg Matrix bot backed by xmsg agent";
            After = [ "network.target" ] ++ lib.optional xmsgEnabled "xmsg.service";
            X-Restart-Triggers = [
              "${cfg.package}"
              "${configToml}"
            ];
          };
          Service = {
            ExecStart = "${exe} --config ${configToml}";
            Restart = "on-failure";
            RestartSec = "5s";
            TimeoutStopSec = 60;
          };
          Install.WantedBy = [ "default.target" ];
        };
      })

      (lib.mkIf pkgs.stdenv.hostPlatform.isDarwin {
        launchd.agents.matrix-xmsg = {
          enable = true;
          config = {
            ProgramArguments = [
              exe
              "--config"
              "${configToml}"
            ];
            KeepAlive.SuccessfulExit = false;
            RunAtLoad = true;
          };
        };
      })
    ]
  );
}
