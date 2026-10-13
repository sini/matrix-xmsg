{
  inputs = {
    gen-harness.url = "github:sini/gen-harness";
    # ★ THE SUBJECT IS READ BY RELATIVE PATH, NEVER AS A `path:..` INPUT. Lix refuses a relative
    # `path` node in a lock ("mutable lock"), and a Lix-written `path:..?narHash=…` lock pins a stale
    # snapshot of the tree (den-hoag-lbtnv D1). So `outputs` below applies `../flake.nix`'s own
    # `outputs` to the `nixpkgs` declared here, line for line as `../flake.nix` declares it.
    nixpkgs.url = "https://channels.nixos.org/nixos-unstable/nixexprs.tar.xz";
    # The real xmsg the integration oracles run (tests/gate_ma_integration.rs), pinned by
    # revision: xmsg main after XA4 (the four-tool surface, the register.sock `ok` frame with
    # `node`, pushed replies with `inReplyTo`). The addressing plan's re-verify step moves it.
    xmsg = {
      url = "github:sini/xmsg/948a8a6be7e5aaf73640f94eaf93ecf9427a1efa";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    inputs@{
      gen-harness,
      nixpkgs,
      xmsg,
      ...
    }:
    let
      # The published surface of THIS tree.
      matrix-xmsg = (import ../flake.nix).outputs {
        inherit (inputs) self;
        inherit nixpkgs;
      };
    in
    gen-harness.lib.mkCi {
      inputs = inputs // {
        inherit matrix-xmsg;
      };
      name = "matrix-xmsg";
      testModules = ./tests;
      extraModules = [
        # matrix-xmsg is a TOOL, not an ecosystem library: it is absent from the register roster,
        # so no capability sheet is owed.
        { gen.ci.agentsMd.sheet = "not-owed"; }
        # Nor a root library surface: there is no root default.nix.
        { gen.ci.rootSurface.entry = "not-owed"; }
        # Nor an evaluator matrix: its CI is one upstream-Nix column, not evaluators.yml.
        { gen.ci.evaluators = "not-owed"; }
        # Surface package build in CI checks (doCheck = true runs cargo test)
        {
          perSystem =
            {
              lib,
              pkgs,
              system,
              ...
            }:
            {
              checks = {
                package = matrix-xmsg.packages.${system}.default;
              }
              # The integration oracles against the real `xmsg serve`. They are `#[ignore]`d, so
              # a plain `cargo test` reports them ignored; this check runs them with
              # `--include-ignored`, and MA_XMSG_REQUIRED makes a missing XMSG_BIN fail every
              # oracle rather than skip it. xmsg's own suite is not this check's subject, so its
              # package is built without it. Linux only: the oracle file is
              # `#![cfg(target_os = "linux")]`, so elsewhere it would collect 0 tests and pass.
              // lib.optionalAttrs pkgs.stdenv.hostPlatform.isLinux {
                integration = matrix-xmsg.packages.${system}.default.overrideAttrs {
                  pname = "matrix-xmsg-integration";
                  XMSG_BIN = "${xmsg.packages.${system}.default.overrideAttrs { doCheck = false; }}/bin/xmsg";
                  MA_XMSG_REQUIRED = "1";
                  cargoTestFlags = [
                    "--test"
                    "gate_ma_integration"
                  ];
                  checkFlags = [ "--include-ignored" ];
                };
              };
            };
        }
      ];
    };
}
