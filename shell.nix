{
  pkgs ? import ./nixpkgs.nix { },
}:

let
  snouty = import ./default.nix { inherit pkgs; };
in
pkgs.mkShell {
  # Take openssl, sqlite, and pkg-config from the package itself, so the shell
  # cannot drift from what package.nix needs to compile.
  inputsFrom = [ snouty ];

  # Only what the package does not already bring: the toolchain, and the tools
  # a developer runs by hand.
  packages = with pkgs; [
    rustToolchain
    cargo-nextest
    npins
    nixfmt
    # snouty drives Docker Compose v2 via the `docker-compose` binary.
    docker-compose
  ];

  env = {
    # Match package.nix: link against the openssl above, not a vendored copy.
    OPENSSL_NO_VENDOR = true;
  };
}
