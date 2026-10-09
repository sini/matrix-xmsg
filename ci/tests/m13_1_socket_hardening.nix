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
              wantedBy = lib.mkOption {
                type = lib.types.listOf lib.types.str;
                default = [ ];
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

  evalServiceConfig =
    serviceCfg:
    let
      res = lib.evalModules {
        specialArgs = {
          inherit pkgs;
        };
        modules = [
          baseModule
          matrixModule
          {
            services.matrix-xmsg = {
              enable = true;
              package = inputs.matrix-xmsg.packages.x86_64-linux.default;
              homeserverUrl = "http://localhost:8008";
              botMxid = "@bot:local";
              accessTokenFile = "/run/token";
              rooms = [ "!room:local" ];
              ownerMxid = "@owner:local";
              dynamicUser = false;
            }
            // serviceCfg;
          }
        ];
      };
    in
    res.config.systemd.services.matrix-xmsg.serviceConfig;
in
{
  flake.tests.m13-1-socket-hardening = {
    test-oracle1-with-socket =
      let
        sc = evalServiceConfig {
          xmsgSocket = "/run/user/1000/xmsg";
        };
      in
      {
        expr = {
          protectHome = sc.ProtectHome;
          bindPaths = sc.BindPaths or [ ];
          privateUsers = sc.PrivateUsers;
        };
        expected = {
          protectHome = "tmpfs";
          bindPaths = [ "/run/user/1000/xmsg" ];
          privateUsers = false;
        };
      };

    test-oracle2-without-socket =
      let
        sc = evalServiceConfig {
          xmsgSocket = null;
        };
      in
      {
        expr = {
          protectHome = sc.ProtectHome;
          bindPaths = sc.BindPaths or [ ];
          privateUsers = sc.PrivateUsers;
        };
        expected = {
          protectHome = true;
          bindPaths = [ ];
          privateUsers = true;
        };
      };
  };
}
