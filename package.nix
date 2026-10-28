{
  lib,
  rustPlatform,
  pkg-config,
  sqlite,
}:

rustPlatform.buildRustPackage (finalAttrs: {
  pname = "snouty";
  version = "${(lib.importTOML ./Cargo.toml).package.version}-unstable";
  __structuredAttrs = true;

  # Every path that Cargo.toml declares must appear here: workspace members,
  # path dependencies, and [[example]] paths. The askama derive reads
  # ./askama.toml, ./templates and ./help at compile time, so they must appear
  # too.
  src = lib.fileset.toSource {
    root = ./.;
    fileset = lib.fileset.unions [
      ./Cargo.lock
      ./Cargo.toml
      ./askama.toml
      ./build.rs
      ./examples
      ./help
      ./snouty-macros
      ./src
      ./templates
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
    sqlite
  ];

  # Some tests require a container runtime, unavailable in the Nix sandbox
  doCheck = false;

  # Docker and Podman run the credential helper by this name, and snouty runs
  # the helper when it starts by this name.
  postInstall = ''
    ln -s "$out/bin/snouty" "$out/bin/docker-credential-snouty"
  '';

  env = {
    LIBSQLITE3_SYS_USE_PKG_CONFIG = true;
  };

  meta = {
    description = "A CLI for the Antithesis platform";
    homepage = "https://github.com/antithesishq/snouty";
    license = lib.licenses.asl20;
    mainProgram = "snouty";
  };
})
