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

  evalConfig =
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
            }
            // serviceCfg;
          }
        ];
      };
    in
    map (a: a.message) (builtins.filter (a: !a.assertion) res.config.assertions);
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
                package = inputs.matrix-xmsg.packages.x86_64-linux.default;
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
  };
}
