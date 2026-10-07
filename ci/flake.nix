{
  inputs = {
    gen-harness.url = "github:sini/gen-harness";
    nixpkgs.url = "https://channels.nixos.org/nixos-unstable/nixexprs.tar.xz";
    matrix-xmsg.url = "path:..";
  };

  outputs =
    inputs@{
      gen-harness,
      matrix-xmsg,
      ...
    }:
    gen-harness.lib.mkCi {
      inherit inputs;
      name = "matrix-xmsg";
      testModules = ./tests;
      extraModules = [
        # matrix-xmsg is a TOOL, not an ecosystem library: it is absent from the register roster,
        # so no capability sheet is owed.
        { gen.ci.agentsMd.sheet = "not-owed"; }
        # Nor a root library surface: there is no root default.nix.
        { gen.ci.rootSurface.entry = "not-owed"; }
        # Surface package build in CI checks (doCheck = true runs cargo test)
        {
          perSystem =
            { pkgs, system, ... }:
            {
              checks.package = matrix-xmsg.packages.${system}.default;
            };
        }
      ];
    };
}
