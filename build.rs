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

/// How many `"additionalProperties": false` occurrences the vendored spec
/// carries (tenant release 60.0: `Search_Request`, `Search_Count_Response`).
const EXPECTED_ADDITIONAL_PROPERTIES_FALSE: usize = 2;

fn generate_api_client(out_dir: &Path) {
    let file = std::fs::File::open("src/openapi.json").unwrap();
    let mut spec_value: serde_json::Value = serde_json::from_reader(file).unwrap();

    // A schema's `additionalProperties: false` makes progenitor/typify emit
    // `#[serde(deny_unknown_fields)]`, which turns a forwards-compatible server
    // change (a new field added to a response) into a hard deserialization
    // error — e.g. `snouty doctor` would report a healthy API as "unreachable"
    // the day `/api/version` grows a field. typify has no setting to disable
    // this (the choice is hardwired from the schema value), so strip the
    // constraint from the spec itself before generating. Removing the key is
    // equivalent to the permissive default: no `deny_unknown_fields` is
    // emitted, and no flattened `extra` map is added, so struct shapes are
    // unchanged. The recursive strip catches the attribute wherever it
    // appears, including on nested schemas and enums, which a line-text patch
    // could miss. The occurrence count is pinned exactly: every occurrence is
    // a spec defect the API team has to hear about, so a spec refresh that
    // moves the count in either direction fails the build until they have
    // been reminded and the pin updated.
    let stripped = strip_additional_properties_false(&mut spec_value);
    assert_eq!(
        stripped, EXPECTED_ADDITIONAL_PROPERTIES_FALSE,
        "openapi spec marks {stripped} schema(s) `\"additionalProperties\": false`, but build.rs \
         pins {EXPECTED_ADDITIONAL_PROPERTIES_FALSE}. The constraint makes generated clients \
         reject unknown response fields, turning additive server changes into breaking ones; \
         snouty strips it before generating. ACTION: remind the API team to publish schemas \
         without `additionalProperties: false`, then update \
         EXPECTED_ADDITIONAL_PROPERTIES_FALSE in build.rs to {stripped}."
    );
    untype_error_responses(&mut spec_value);
    drop_use_otis(&mut spec_value);
    mark_vtime_schema(&mut spec_value);
    mark_run_status_schema(&mut spec_value);
    let spec: openapiv3::OpenAPI = serde_json::from_value(spec_value).unwrap();

    let mut settings = progenitor::GenerationSettings::default();
    settings.with_interface(progenitor::InterfaceStyle::Builder);
    settings.with_inner_type(quote::quote!(crate::api::ClientState));
    // Map the marked vtime schema onto the handwritten VTime type, which
    // enforces the exact string<->f64 conversion a vtime needs (the
    // conversion lookup ignores schema metadata such as description/example,
    // so `type` + `format` is the whole match key).
    let vtime_schema: schemars::schema::SchemaObject =
        serde_json::from_value(serde_json::json!({"type": "string", "format": "vtime"})).unwrap();
    settings.with_conversion(
        vtime_schema,
        "crate::vtime::VTime",
        std::iter::empty::<progenitor::TypeImpl>(),
    );
    // Map the marked run-status schema onto the handwritten RunStatus, whose
    // `Other` variant preserves a status value the enum doesn't name instead
    // of failing the whole response parse.
    let run_status_schema: schemars::schema::SchemaObject =
        serde_json::from_value(serde_json::json!({"type": "string", "format": "run_status"}))
            .unwrap();
    settings.with_conversion(
        run_status_schema,
        "crate::api::RunStatus",
        std::iter::empty::<progenitor::TypeImpl>(),
    );
    let mut generator = progenitor::Generator::new(&settings);
    let tokens = generator.generate_tokens(&spec).unwrap();
    let ast = syn::parse2(tokens).unwrap();
    let content = prettyplease::unparse(&ast);
    let content = patch_lenient_booleans(content);
    assert_no_typed_error_responses(&content);

    // The conversion fails open: an unmatched schema silently falls back to a
    // plain String field. Assert it took, so a progenitor/typify change that
    // breaks the match fails the build instead of quietly shipping a client
    // without the vtime precision guarantees.
    assert!(
        content.contains("pub vtime: crate::vtime::VTime"),
        "generated client does not use crate::vtime::VTime for Moment.vtime; \
         the with_conversion schema match no longer applies"
    );
    assert!(
        content.contains("pub status: crate::api::RunStatus"),
        "generated client does not use crate::api::RunStatus for run statuses; \
         the with_conversion schema match no longer applies"
    );

    fs::write(out_dir.join("antithesis_api.rs"), content).unwrap();
}

/// Hold `untype_error_responses` to its promise: with no error response
/// documented anywhere, progenitor emits no `Error::ErrorResponse` arm, so that
/// variant is unreachable and `classify_client_error` treats it as such. A spec
/// refresh that slipped one back in — under a status shape the transform misses
/// — would otherwise only surface as a status-less failure against a live
/// tenant.
fn assert_no_typed_error_responses(content: &str) {
    assert!(
        !content.contains("Error::ErrorResponse"),
        "generated client has an `Error::ErrorResponse` arm; every error response is supposed \
         to be undocumented so failures keep their HTTP status (see `untype_error_responses`)"
    );
}

/// Stop the generated client from typing error response bodies, so an HTTP
/// failure always reaches snouty with its status attached.
///
/// progenitor decodes every *documented* response into a generated type. When a
/// documented status arrives with a body that doesn't match its schema, the
/// generated client returns `Error::InvalidResponsePayload(Bytes,
/// serde_json::Error)` — a variant with nowhere to put the HTTP status, for
/// which `Error::status()` is `None`. An *undocumented* status takes
/// `Error::UnexpectedResponse(reqwest::Response)` instead, which keeps the
/// status *and* the raw body.
///
/// Error bodies are exactly the ones that don't honour the schema: the API
/// gateway rejects a bad token with an empty `text/plain` body, an intermediary
/// answers with an HTML page. Typed, any of those masked the status — an
/// empty-bodied 401 was reaching `snouty doctor` as "API unreachable" (#180).
/// Untyped, every failure arrives as status + raw body and one status-first
/// formatter renders all of them.
///
/// Success responses are left alone: the spec describes them accurately, and
/// the typed bodies are what the rest of the client is built on.
fn untype_error_responses(spec: &mut serde_json::Value) {
    let paths = spec
        .get_mut("paths")
        .and_then(serde_json::Value::as_object_mut)
        .expect("openapi spec has a `paths` object");
    for path_item in paths.values_mut() {
        let Some(path_item) = path_item.as_object_mut() else {
            continue;
        };
        // A path item holds non-operation keys too (`parameters`, `summary`);
        // an operation is anything that documents responses.
        for operation in path_item.values_mut() {
            let Some(responses) = operation
                .get_mut("responses")
                .and_then(serde_json::Value::as_object_mut)
            else {
                continue;
            };
            // Retaining only 2xx also drops any `default` response, which
            // progenitor would otherwise turn into a typed catch-all covering
            // every error status — reintroducing the problem by another door.
            responses.retain(|status, _| status.starts_with('2'));
        }
    }
}

/// Tag `Moment.vtime` with a private `format: vtime` marker for the
/// `with_conversion` mapping registered above. The marker is injected here
/// rather than edited into `src/openapi.json`, because that file is a
/// vendored upstream artifact — the next spec refresh would silently drop the
/// edit.
/// Remove `use_otis` from the execute-command request schema, so the generated
/// request type has no such field and snouty never sends one.
///
/// The field is a server-side testing knob that routes output through the
/// tenant coordinator; snouty always wants the live session's output, so it
/// would only ever send `false`. Sending `false` is not the same as saying
/// nothing: the API team expects to retire the field, and a client that keeps
/// naming it makes that harder. Leaving it out defers to whatever the server
/// defaults to, and the eventual removal costs us nothing.
///
/// The pointer is asserted, so a spec refresh that drops the field fails the
/// build. ACTION when that happens: delete this transform and its call.
fn drop_use_otis(spec: &mut serde_json::Value) {
    let properties = spec
        .pointer_mut("/components/schemas/Execute_Command_Request/properties")
        .and_then(serde_json::Value::as_object_mut)
        .expect("openapi spec has no Execute_Command_Request.properties");
    assert!(
        properties.remove("use_otis").is_some(),
        "Execute_Command_Request no longer has `use_otis`; delete `drop_use_otis` in build.rs"
    );
}

/// Replace every `Run_Status` reference with an inline string schema tagged
/// for the `run_status` conversion mapping (see the registration above), and
/// drop the named component. Generated as an enum, an undocumented status
/// value fails the whole response parse; the handwritten
/// `crate::api::RunStatus` preserves it instead. Inlining matters: typify
/// wraps a *named* schema that hits a conversion in a needless newtype, while
/// an inline schema maps to the conversion type directly (like `Moment.vtime`
/// and `VTime`). The assertions pin the spec shape, so a refresh that changes
/// it fails the build.
fn mark_run_status_schema(spec: &mut serde_json::Value) {
    let status = spec
        .pointer("/components/schemas/Run_Status")
        .expect("openapi spec has no Run_Status schema; update the RunStatus wiring in build.rs");
    let known = serde_json::json!([
        "starting",
        "in_progress",
        "completed",
        "cancelled",
        "incomplete",
        "unknown"
    ]);
    assert_eq!(
        status["enum"], known,
        "Run_Status gained or lost values; give crate::api::RunStatus a matching variant, then \
         update this pin"
    );

    let marker = serde_json::json!({"type": "string", "format": "run_status"});
    let replaced = replace_refs(spec, "#/components/schemas/Run_Status", &marker);
    assert!(
        replaced > 0,
        "nothing references Run_Status anymore; delete mark_run_status_schema in build.rs"
    );
    spec.pointer_mut("/components/schemas")
        .and_then(serde_json::Value::as_object_mut)
        .unwrap()
        .remove("Run_Status");
}

/// Recursively replace every `{"$ref": target}` object with `replacement`,
/// returning how many were replaced.
fn replace_refs(
    value: &mut serde_json::Value,
    target: &str,
    replacement: &serde_json::Value,
) -> usize {
    match value {
        serde_json::Value::Object(map) => {
            if map.get("$ref").and_then(serde_json::Value::as_str) == Some(target) {
                *value = replacement.clone();
                return 1;
            }
            map.values_mut()
                .map(|v| replace_refs(v, target, replacement))
                .sum()
        }
        serde_json::Value::Array(items) => items
            .iter_mut()
            .map(|v| replace_refs(v, target, replacement))
            .sum(),
        _ => 0,
    }
}

fn mark_vtime_schema(spec: &mut serde_json::Value) {
    let vtime = spec
        .pointer_mut("/components/schemas/Moment/properties/vtime")
        .expect("openapi spec has no Moment.properties.vtime; update the VTime wiring in build.rs");
    assert_eq!(
        vtime["type"],
        serde_json::json!("string"),
        "Moment.vtime is no longer a string in the openapi spec; revisit the VTime wiring in build.rs"
    );
    vtime["format"] = serde_json::json!("vtime");
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
