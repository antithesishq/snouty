{
  lib,
  rustPlatform,
  pkg-config,
  openssl,
  sqlite,
}:

rustPlatform.buildRustPackage {
  pname = "snouty";
  version = "${(lib.importTOML ./Cargo.toml).package.version}-unstable";
  __structuredAttrs = true;

  src = lib.fileset.toSource {
    root = ./.;
    fileset = lib.fileset.unions [
      ./Cargo.lock
      ./Cargo.toml
      ./build.rs
      ./examples
      ./src
    ];
  };

  cargoLock = {
    lockFile = ./Cargo.lock;
    outputHashes = {
      "testscript-rs-0.2.10" = "sha256-I+RmIZVS8pJF1yVyiNp0r0ZCiQ8maHC9o2zRNlXq7HA=";
    };
  };

  nativeBuildInputs = [
    pkg-config
  ];

  buildInputs = [
    openssl
    sqlite
  ];

  # Some tests require a container runtime, unavailable in the Nix sandbox
  doCheck = false;

  env = {
    LIBSQLITE3_SYS_USE_PKG_CONFIG = true;
    OPENSSL_NO_VENDOR = true;
  };

  meta = {
    description = "A CLI for the Antithesis platform";
    homepage = "https://github.com/antithesishq/snouty";
    license = lib.licenses.asl20;
    mainProgram = "snouty";
  };
}
