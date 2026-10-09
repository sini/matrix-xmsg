{
  description = "matrix-xmsg: Matrix support bot backed by an expert agent session";

  inputs.nixpkgs.url = "https://channels.nixos.org/nixos-unstable/nixexprs.tar.xz";

  outputs =
    { self, nixpkgs }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "aarch64-darwin"
      ];
      forAllSystems = f: nixpkgs.lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});
    in
    {
      packages = forAllSystems (pkgs: {
        default = pkgs.rustPlatform.buildRustPackage {
          pname = "matrix-xmsg";
          version = "0.1.0";
          src = nixpkgs.lib.cleanSourceWith {
            src = ./.;
            filter =
              path: type:
              let
                base = baseNameOf path;
              in
              !(
                base == "nix"
                || base == "docs"
                || base == "ci"
                || base == ".github"
                || base == "README.md"
                || base == "TODO.md"
                || base == "LICENSE"
                || nixpkgs.lib.hasSuffix ".nix" base
              );
          };
          cargoLock.lockFile = ./Cargo.lock;
          doCheck = true;
          nativeCheckInputs = [ pkgs.cacert ];
          SSL_CERT_FILE = "${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt";
          meta = {
            description = "Matrix support bot backed by an expert agent session";
            mainProgram = "matrix-xmsg";
            license = nixpkgs.lib.licenses.mit;
          };
        };

        image = pkgs.dockerTools.buildLayeredImage {
          name = "ghcr.io/sini/matrix-xmsg";
          tag = "latest";
          contents = [
            pkgs.cacert
            self.packages.${pkgs.system}.default
          ];
          extraCommands = ''
            mkdir -p -m 1777 tmp
            mkdir -p -m 0755 var/lib/matrix-xmsg
            mkdir -p etc/matrix-xmsg
          '';
          config = {
            User = "10001:10001";
            Entrypoint = [ "${self.packages.${pkgs.system}.default}/bin/matrix-xmsg" ];
            Cmd = [
              "--config"
              "/etc/matrix-xmsg/config.toml"
            ];
            WorkingDir = "/var/lib/matrix-xmsg";
            Env = [
              "SSL_CERT_FILE=${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt"
            ];
          };
        };
      });

      nixosModules.default = import ./nix/module.nix { inherit self; };

      checks = forAllSystems (
        pkgs:
        nixpkgs.lib.optionalAttrs pkgs.stdenv.hostPlatform.isLinux {
          nixos-module = import ./nix/test.nix { inherit pkgs self; };
          nixos-module-mutant-missing = import ./nix/test.nix {
            inherit pkgs self;
            mode = "mutant-missing";
          };
          nixos-module-mutant-wrong-token = import ./nix/test.nix {
            inherit pkgs self;
            mode = "mutant-wrong-token";
          };
          image =
            pkgs.runCommand "check-image-oracle"
              {
                nativeBuildInputs = [ pkgs.python3 ];
              }
              ''
                python3 ${./nix/check_image.py} ${self.packages.${pkgs.system}.image}
                touch $out
              '';
        }
      );

      devShells = forAllSystems (pkgs: {
        default = pkgs.mkShell {
          packages = [
            pkgs.cargo
            pkgs.rustc
            pkgs.clippy
            pkgs.rustfmt
            pkgs.rust-analyzer
          ];
        };
      });
    };
}
