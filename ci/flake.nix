{
  inputs = {
    gen-harness.url = "github:sini/gen-harness";
    # ★ THE SUBJECT IS READ BY RELATIVE PATH, NEVER AS A `path:..` INPUT. Lix refuses a relative
    # `path` node in a lock ("mutable lock"), and a Lix-written `path:..?narHash=…` lock pins a stale
    # snapshot of the tree (den-hoag-lbtnv D1). So `outputs` below applies `../flake.nix`'s own
    # `outputs` to the `nixpkgs` declared here, line for line as `../flake.nix` declares it.
    nixpkgs.url = "https://channels.nixos.org/nixos-unstable/nixexprs.tar.xz";
    crane.url = "github:ipetkov/crane";
    # The real xmsg the integration oracles run (tests/gate_ma_integration.rs), pinned by
    # revision: xmsg main after its atomic idempotency (the key reserved and the ID minted
    # before delivery, `reply` taking `idempotency_key`), which every forward send now carries.
    # The addressing plan's re-verify step moves it.
    xmsg = {
      url = "github:sini/xmsg/a6f7adf31516e4ebc56fb409cd302a54c18647b6";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.crane.follows = "crane";
    };
    # The flake modules are evaluated against home-manager at nix-config's pin (its
    # `home-manager-unstable` node), matching xmsg/ci/flake.nix.
    home-manager = {
      url = "github:nix-community/home-manager/fae6e9e42c3b762ab47635cddcfaf6f52374a61b";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    inputs@{
      gen-harness,
      nixpkgs,
      crane,
      xmsg,
      home-manager,
      ...
    }:
    let
      # The published surface of THIS tree.
      matrix-xmsg = (import ../flake.nix).outputs {
        inherit (inputs) self;
        inherit nixpkgs crane;
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
        # Surface root flake checks and package build in CI checks (doCheck = true runs cargo test)
        {
          perSystem =
            {
              lib,
              pkgs,
              system,
              ...
            }:
            {
              checks =
                (matrix-xmsg.checks.${system} or { })
                // {
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
                    doNotPostBuildInstallCargoBinaries = true;
                    passthru = {
                      xmsgNoCheck = xmsg.packages.${system}.default.overrideAttrs { doCheck = false; };
                      pkgNoCheck = matrix-xmsg.packages.${system}.default.overrideAttrs { doCheck = false; };
                    };
                    buildPhase = ''
                      runHook preBuild
                      runHook postBuild
                    '';
                    checkPhase = ''
                      runHook preCheck
                      cargoWithProfile test --locked --test gate_ma_integration -- --include-ignored
                      runHook postCheck
                    '';
                    installPhase = ''
                      runHook preInstall
                      mkdir -p $out
                      runHook postInstall
                    '';
                  };
                  hm-user-service = import ../nix/hm_test.nix {
                    inherit
                      pkgs
                      matrix-xmsg
                      xmsg
                      home-manager
                      ;
                  };
                  hm-user-service-mutant-no-svcexe = import ../nix/hm_test.nix {
                    inherit
                      pkgs
                      matrix-xmsg
                      xmsg
                      home-manager
                      ;
                    mode = "mutant-no-svcexe";
                  };
                  nixos-own-xmsg = import ../nix/own_xmsg_test.nix {
                    inherit
                      pkgs
                      matrix-xmsg
                      xmsg
                      ;
                  };
                };
            };
        }
      ];
    };
}
