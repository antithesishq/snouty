let
  npins = import ./npins;
  pkgs = import npins.nixpkgs {
    overlays = [
      (import npins.rust-overlay)
    ];
  };
  rust = pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
in

pkgs.callPackage ./package.nix {
  rustPlatform = pkgs.makeRustPlatform {
    cargo = rust;
    rustc = rust;
  };
}
