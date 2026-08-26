{
  pkgs ? import ./nixpkgs.nix { },
}:

pkgs.callPackage ./package.nix {
  rustPlatform = pkgs.rustToolchainPlatform;
}
