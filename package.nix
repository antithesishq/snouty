{
  lib,
  rustPlatform,
  pkg-config,
  sqlite,
  cacert,
  cargo-nextest,
  git,
  jq,
  openssh,
  python3,
}:

rustPlatform.buildRustPackage (finalAttrs: {
  pname = "snouty";
  version = "${(lib.importTOML ./Cargo.toml).package.version}-unstable";
  __structuredAttrs = true;

  # Every path that Cargo.toml declares must appear here: workspace members,
  # path dependencies, and [[example]] paths. The askama derive reads
  # ./askama.toml, ./templates and ./help at compile time, so they must appear
  # too. The tests also read ./.config/nextest.toml, ./dist-workspace.toml,
  # ./scripts/no-engine-shims, ./server.json, ./specs and ./tests.
  src = lib.fileset.toSource {
    root = ./.;
    fileset = lib.fileset.unions [
      ./Cargo.lock
      ./Cargo.toml
      ./.config/nextest.toml
      ./askama.toml
      ./build.rs
      ./dist-workspace.toml
      ./examples
      ./help
      ./scripts/no-engine-shims
      ./server.json
      ./snouty-macros
      ./specs
      ./src
      ./templates
      ./tests
      ./vendor
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

  # The sandbox has no container engine, so the tests that need one do not run.
  # The shims answer the engine probes of `snouty doctor`, which specs/doctor.txt
  # needs.
  useNextest = true;
  cargoTestFlags = [
    "--filterset"
    "not group(container)"
  ];
  preCheck = ''
    export PATH="$PWD/scripts/no-engine-shims:$PATH"
  '';
  # The sandbox has CA certificates only through SSL_CERT_FILE, which cacert
  # sets. Some specs run git, and the simulate tests run Python, jq and ssh.
  nativeCheckInputs = [
    cacert
    cargo-nextest
    git
    jq
    openssh
    python3
  ];

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
