{ lib, inputs, ... }:
{
  flake.tests.basic = {
    test-meta-description = {
      expr = inputs.matrix-xmsg.packages.x86_64-linux.default.meta.description;
      expected = "Matrix support bot backed by an expert agent session";
    };
  };
}
