{ lib, inputs, ... }:
let
  matrixModule = inputs.matrix-xmsg.nixosModules.default;
  baseModule = {
    options = {
      assertions = lib.mkOption {
        type = lib.types.listOf lib.types.attrs;
        default = [ ];
      };
      users.users = lib.mkOption {
        type = lib.types.attrs;
        default = { };
      };
      users.groups = lib.mkOption {
        type = lib.types.attrs;
        default = { };
      };
      systemd.services = lib.mkOption {
        type = lib.types.attrs;
        default = { };
      };
    };
  };

  evalConfig =
    serviceCfg:
    let
      res = lib.evalModules {
        modules = [
          baseModule
          matrixModule
          {
            services.matrix-xmsg = {
              enable = true;
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
    test-oracle3-both-url-and-socket-assertion = {
      expr =
        builtins.elem
          "services.matrix-xmsg: exactly one of services.matrix-xmsg.xmsgUrl or services.matrix-xmsg.xmsgSocket must be set."
          (evalConfig {
            xmsgUrl = "http://127.0.0.1:7787";
            xmsgSocket = "/run/xmsg.sock";
            dynamicUser = false;
          });
      expected = true;
    };

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

    test-oracle3-valid-url-passes = {
      expr = evalConfig {
        xmsgUrl = "http://127.0.0.1:7787";
      };
      expected = [ ];
    };
  };
}
