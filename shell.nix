let
  pins = import ./npins;
  pkgs = import pins.nixpkgs { overlays = [ (import pins.rust-overlay) ]; };
in
pkgs.mkShell {
  packages = with pkgs; [
    (rust-bin.fromRustupToolchainFile ./rust-toolchain.toml)
    cargo-nextest
    npins
    nixfmt
    openssl
    pkg-config
    # snouty drives Docker Compose v2 via the `docker-compose` binary.
    docker-compose
  ];

  env.OPENSSL_NO_VENDOR = true;
}
