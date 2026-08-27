{
  pkgs ? import ./nixpkgs.nix { },
}:

let
  snouty = import ./default.nix { inherit pkgs; };
in
pkgs.mkShell {
  # Take openssl, sqlite, and pkg-config from the package itself, so the shell
  # cannot drift from the libraries package.nix builds against.
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

  # `inputsFrom` carries packages, not environment. mkShell folds only
  # buildInputs, nativeBuildInputs, their propagated variants, and shellHook out
  # of it, so everything package.nix sets in `env` must be repeated here. Left
  # out, cargo links its own vendored copy of a library that the shell already
  # provides: rusqlite has the `bundled` feature, so it compiles the sqlite C
  # sources rather than the sqlite above.
  env = {
    LIBSQLITE3_SYS_USE_PKG_CONFIG = true;
    OPENSSL_NO_VENDOR = true;
  };
}
