use std::fs;
use std::path::Path;
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=RUSTC");
    println!("cargo:rerun-if-changed=src/openapi.json");
    println!(
        "cargo:rustc-env=SNOUTY_RUSTC_VERSION={}",
        rustc_version().unwrap()
    );
    emit_version();

    let out_dir = std::env::var_os("OUT_DIR").unwrap();
    fs::create_dir_all(&out_dir).unwrap();
    generate_api_client(Path::new(&out_dir));
}

fn generate_api_client(out_dir: &Path) {
    let file = std::fs::File::open("src/openapi.json").unwrap();
    let mut spec_value: serde_json::Value = serde_json::from_reader(file).unwrap();

    // A schema's `additionalProperties: false` makes progenitor/typify emit
    // `#[serde(deny_unknown_fields)]`, which turns a forwards-compatible server
    // change (a new field added to a response) into a hard deserialization
    // error — e.g. `snouty doctor` would report a healthy API as "unreachable"
    // the day `/api/version` grows a field. typify has no setting to disable
    // this (the choice is hardwired from the schema value), so strip the
    // constraint from the spec itself before generating, rather than patching
    // the generated text afterwards. Removing the key is equivalent to the
    // permissive default: no `deny_unknown_fields` is emitted, and no flattened
    // `extra` map is added, so struct shapes are unchanged. Operating on the
    // structured spec (not the formatted output) also handles the attribute
    // wherever it would appear — including combined with other serde options on
    // one line (`#[serde(rename = "…", deny_unknown_fields)]`) and on enums —
    // which a line-text patch could silently miss.
    let stripped = strip_additional_properties_false(&mut spec_value);
    assert!(
        stripped > 0,
        "expected the openapi spec to mark some schema `\"additionalProperties\": false`; \
         none found — the lenient-client transform is now a no-op and can be removed"
    );
    let spec: openapiv3::OpenAPI = serde_json::from_value(spec_value).unwrap();

    let mut settings = progenitor::GenerationSettings::default();
    settings.with_interface(progenitor::InterfaceStyle::Builder);
    settings.with_inner_type(quote::quote!(crate::api::ClientState));
    let mut generator = progenitor::Generator::new(&settings);
    let tokens = generator.generate_tokens(&spec).unwrap();
    let ast = syn::parse2(tokens).unwrap();
    let content = prettyplease::unparse(&ast);
    let content = patch_lenient_booleans(content);

    fs::write(out_dir.join("antithesis_api.rs"), content).unwrap();
}

/// Recursively remove every `"additionalProperties": false` from the spec so
/// the generated client is lenient about unknown response fields (see the call
/// site for why). Returns the number of occurrences removed.
fn strip_additional_properties_false(value: &mut serde_json::Value) -> usize {
    let mut count = 0;
    match value {
        serde_json::Value::Object(map) => {
            if map.get("additionalProperties") == Some(&serde_json::Value::Bool(false)) {
                map.remove("additionalProperties");
                count += 1;
            }
            for v in map.values_mut() {
                count += strip_additional_properties_false(v);
            }
        }
        serde_json::Value::Array(items) => {
            for v in items.iter_mut() {
                count += strip_additional_properties_false(v);
            }
        }
        _ => {}
    }
    count
}

// The API represents booleans as the strings "true"/"false", but some
// historical run data stored "on"/"off" instead. Accept those as aliases when
// deserializing API responses so commands like `snouty runs list` don't hard
// error on old runs (#122). Panics if the expected generated code is missing,
// so a progenitor upgrade that changes the output shape fails the build
// instead of silently dropping the aliases.
fn patch_lenient_booleans(content: String) -> String {
    let replacements = [
        (
            r##"#[serde(rename = "true")]"##,
            r##"#[serde(rename = "true", alias = "on")]"##,
        ),
        (
            r##"#[serde(rename = "false")]"##,
            r##"#[serde(rename = "false", alias = "off")]"##,
        ),
    ];

    let mut content = content;
    for (from, to) in replacements {
        assert_eq!(
            content.matches(from).count(),
            1,
            "expected generated API client to contain `{from}` exactly once; \
             progenitor output may have changed"
        );
        content = content.replace(from, to);
    }
    content
}

// Compose the display version string as `SNOUTY_VERSION`, used by both `snouty
// version` and clap's `--version`. It is the crate version, plus the short git
// commit hash the build came from when available — with a `-dirty` suffix when
// tracked files differ from HEAD (the standard `git describe --dirty`
// convention) — e.g. `0.6.0 (a1b2c3d)` or `0.6.0 (a1b2c3d-dirty)`. When git or
// the repository is unavailable (e.g. building from a published source
// tarball), it falls back to the bare crate version, `0.6.0`.
fn emit_version() {
    // Rebuild when the checked-out commit or staged state changes, so the stamp
    // stays current. (Purely unstaged edits don't retrigger on their own; the
    // next rebuild for any reason picks them up — the same caveat vergen and
    // similar build-stamp tools carry.)
    for path in [".git/HEAD", ".git/index"] {
        if Path::new(path).exists() {
            println!("cargo:rerun-if-changed={path}");
        }
    }
    // CARGO_PKG_VERSION is provided to build scripts by cargo.
    let pkg = std::env::var("CARGO_PKG_VERSION").unwrap();
    let version = match git_sha() {
        Some(sha) => format!("{pkg} ({sha})"),
        None => pkg,
    };
    println!("cargo:rustc-env=SNOUTY_VERSION={version}");
}

fn git_sha() -> Option<String> {
    let output = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let sha = String::from_utf8(output.stdout).ok()?.trim().to_owned();
    if sha.is_empty() {
        return None;
    }

    // `git status --porcelain` refreshes the index as a side effect (avoiding
    // stat-only false positives) and, with untracked files excluded, reports
    // only tracked modifications — matching `git describe --dirty` semantics.
    let dirty = Command::new("git")
        .args(["status", "--porcelain", "--untracked-files=no"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| !o.stdout.is_empty())
        .unwrap_or(false);

    Some(if dirty { format!("{sha}-dirty") } else { sha })
}

fn rustc_version() -> Result<String, Box<dyn std::error::Error>> {
    let rustc = std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into());
    let output = Command::new(rustc).arg("-V").output()?;
    let stdout = String::from_utf8(output.stdout)?;

    stdout
        .split_whitespace()
        .nth(1)
        .map(ToOwned::to_owned)
        .ok_or_else(|| "rustc -V did not return a parseable version".into())
}
