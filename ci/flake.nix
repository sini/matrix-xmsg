{
  inputs = {
    gen-harness.url = "github:sini/gen-harness";
    # ★ THE SUBJECT IS READ BY RELATIVE PATH, NEVER AS A `path:..` INPUT. Lix refuses a relative
    # `path` node in a lock ("mutable lock"), and a Lix-written `path:..?narHash=…` lock pins a stale
    # snapshot of the tree (den-hoag-lbtnv D1). So `outputs` below applies `../flake.nix`'s own
    # `outputs` to the `nixpkgs` declared here, line for line as `../flake.nix` declares it.
    nixpkgs.url = "https://channels.nixos.org/nixos-unstable/nixexprs.tar.xz";
    crane.url = "github:ipetkov/crane";
    # The library and current daemon share one immutable revision. Keep the previous
    # daemon as an explicit compatibility arm, not as a second client implementation.
    xmsg = {
      url = "github:sini/xmsg/2d76fd63f4ebc63946a9303c7019a50d169f7ed3";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.crane.follows = "crane";
    };
    xmsg-legacy = {
      url = "github:sini/xmsg/cca3de77fbe95ea1ad90ab8ba7a83eb741fc9cb9";
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
      xmsg-legacy,
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
            let
              package = matrix-xmsg.packages.${system}.default;
              inputLock = builtins.fromJSON (builtins.readFile ./flake.lock);
              sourceFor =
                inputName:
                let
                  node = inputLock.nodes.${inputLock.nodes.root.inputs.${inputName}};
                in
                import (xmsg.outPath + "/ci/xmsg-source.nix") {
                  inherit pkgs;
                  pin = {
                    repository = "https://github.com/sini/xmsg";
                    revision = node.locked.rev;
                    source_hash = node.locked.narHash;
                  };
                };
              binaryFor =
                inputName: upstream:
                upstream.packages.${system}.default.overrideAttrs (old: {
                  doCheck = false;
                  src = sourceFor inputName;
                  postPatch = (old.postPatch or "") + ''
                    substituteInPlace src/main.rs --replace-fail \
                      '    version,' '    version = "0.1.0+${upstream.rev}",'
                  '';
                });
              integrationFor =
                name: inputName: upstream:
                package.overrideAttrs {
                  pname = name;
                  XMSG_BIN = "${binaryFor inputName upstream}/bin/xmsg";
                  MA_XMSG_REVISION = upstream.rev;
                  MA_XMSG_REQUIRED = "1";
                  doNotPostBuildInstallCargoBinaries = true;
                  passthru = {
                    xmsgNoCheck = binaryFor inputName upstream;
                    pkgNoCheck = package.overrideAttrs { doCheck = false; };
                  };
                  buildPhase = "runHook preBuild; runHook postBuild";
                  checkPhase = ''
                    runHook preCheck
                    assertIntegrationRun() {
                      if ! test "$(grep -Ec '^test result: ok\. [1-9][0-9]* passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;' "$1")" -eq 2; then
                        echo "integration targets must both run with no skipped cases" >&2
                        return 1
                      fi
                      for oracle in \
                        oracle_1_a_verdict_is_not_an_answer \
                        oracle_2_origin_by_node \
                        oracle_3_m16_rebootstraps_on_a_new_resolved_id \
                        oracle_4_m24_reply_then_redact_by_returned_message_id \
                        oracle_f_real_xmsg_context_reply_and_session_binding; do
                        if ! test "$(grep -Fc "xmsg-compat-complete:$oracle" "$1")" -eq 1; then
                          echo "missing or repeated integration completion: $oracle" >&2
                          return 1
                        fi
                      done
                    }
                    if ! cargoWithProfile test --locked --test gate_ma_integration --test gate_m33 -- --include-ignored --nocapture > integration.log 2>&1; then
                      cat integration.log
                      exit 1
                    fi
                    cat integration.log
                    assertIntegrationRun integration.log
                    set +e
                    XMSG_BIN="${
                      if inputName == "xmsg" then binaryFor "xmsg-legacy" xmsg-legacy else binaryFor "xmsg" xmsg
                    }/bin/xmsg" \
                      cargoWithProfile test --locked --test gate_ma_integration pinned_binary_revision_is_verified -- --exact > wrong-revision.log 2>&1
                    wrong_rc=$?
                    set -e
                    test "$wrong_rc" -eq 101
                    grep -F 'test pinned_binary_revision_is_verified ... FAILED' wrong-revision.log
                    cp tests/gate_ma_integration.rs gate_ma_integration.original
                    substituteInPlace tests/gate_ma_integration.rs --replace-fail \
                      'async fn oracle_1_a_verdict_is_not_an_answer() {' \
                      'async fn oracle_1_a_verdict_is_not_an_answer() { return;'
                    cargo clean --locked --offline -p matrix-xmsg --profile "$CARGO_PROFILE"
                    if ! cargoWithProfile test --locked --test gate_ma_integration --test gate_m33 -- --include-ignored --nocapture > skipped.log 2>&1; then
                      cat skipped.log
                      echo "a compilation/test failure is not a killed completion mutant" >&2
                      exit 1
                    fi
                    cat skipped.log
                    if assertIntegrationRun skipped.log > skipped-guard.log 2>&1; then
                      echo "early-return completion mutant survived" >&2
                      exit 1
                    fi
                    cat skipped-guard.log
                    grep -F 'missing or repeated integration completion: oracle_1_a_verdict_is_not_an_answer' skipped-guard.log
                    cp gate_ma_integration.original tests/gate_ma_integration.rs
                    echo "single-site test early return makes the completion guard RED"
                    runHook postCheck
                  '';
                  installPhase = ''runHook preInstall; mkdir -p "$out"; runHook postInstall'';
                };
            in
            {
              checks =
                (matrix-xmsg.checks.${system} or { })
                // {
                  package = matrix-xmsg.packages.${system}.default;
                  xmsg-pins = pkgs.runCommand "matrix-xmsg-pins" { nativeBuildInputs = [ pkgs.python3 ]; } ''
                    mkdir ci
                    cp ${../Cargo.toml} Cargo.toml
                    cp ${../Cargo.lock} Cargo.lock
                    cp ${./flake.nix} ci/flake.nix
                    cp ${./flake.lock} ci/flake.lock
                    cp ${./xmsg-pin.toml} ci/xmsg-pin.toml
                    python3 ${../scripts/check_xmsg_pin.py} --root . --self-test
                    touch "$out"
                  '';
                }
                # The integration oracles against the real `xmsg serve`. They are `#[ignore]`d, so
                # a plain `cargo test` reports them ignored; this check runs them with
                # `--include-ignored`, and MA_XMSG_REQUIRED makes a missing XMSG_BIN fail every
                # oracle rather than skip it. xmsg's own suite is not this check's subject, so its
                # package is built without it. Linux only: the oracle file is
                # `#![cfg(target_os = "linux")]`, so elsewhere it would collect 0 tests and pass.
                // lib.optionalAttrs pkgs.stdenv.hostPlatform.isLinux {
                  integration = integrationFor "matrix-xmsg-integration" "xmsg" xmsg;
                  integration-legacy = integrationFor "matrix-xmsg-integration-legacy" "xmsg-legacy" xmsg-legacy;
                  shared-client = package.overrideAttrs {
                    pname = "matrix-xmsg-shared-client";
                    nativeBuildInputs = (package.nativeBuildInputs or [ ]) ++ [ pkgs.python3 ];
                    doNotPostBuildInstallCargoBinaries = true;
                    buildPhase = "runHook preBuild; runHook postBuild";
                    checkPhase = ''
                      runHook preCheck
                      cargoWithProfile test --locked --test shared_transport --test gate_idem_retry -- --test-threads=1
                      python3 scripts/shared_client_mutants.py
                      python3 scripts/guard_scope_mutants.py
                      runHook postCheck
                    '';
                    installPhase = ''
                      runHook preInstall
                      mkdir -p "$out"
                      cp -r target/guard-scope-mutants "$out/"
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
