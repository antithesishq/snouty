# The one place nixpkgs is instantiated. default.nix and shell.nix both go
# through here, so the build and the dev shell can never drift apart on the
# nixpkgs revision or on the Rust toolchain.
#
# Pass `pkgs` explicitly to either of those files to build against a different
# nixpkgs; this is only the default.
{
  system ? builtins.currentSystem,
}:

let
  sources = import ./npins;
in
import sources.nixpkgs {
  inherit system;

  overlays = [
    (import sources.rust-overlay)

    (final: _prev: {
      # The exact toolchain rust-toolchain.toml names, so `nix-build`,
      # `nix-shell`, and a plain `cargo` outside nix all use one compiler.
      rustToolchain = final.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;

      # Deliberately a new attribute rather than an override of the top-level
      # `rustPlatform`. Overriding that one would rebuild every Rust package in
      # nixpkgs against this toolchain — cargo-nextest included — instead of
      # substituting them from the binary cache.
      rustToolchainPlatform = final.makeRustPlatform {
        cargo = final.rustToolchain;
        rustc = final.rustToolchain;
      };
    })
  ];
}
