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
/// carries (tenant release 64.0: none).
const EXPECTED_ADDITIONAL_PROPERTIES_FALSE: usize = 0;

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
    unrequire_include_system_logs_default(&mut spec_value);
    unrequire_exec_timeout_default(&mut spec_value);
    drop_property_description(&mut spec_value);
    drop_launch_status_code(&mut spec_value);
    mark_vtime_schema(&mut spec_value);
    open_performance_tier(&mut spec_value);
    unrequire_search_limit_default(&mut spec_value);
    add_mvd_test_name(&mut spec_value);
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
    settings.with_patch(
        PERFORMANCE_TIER_SCHEMA,
        progenitor::TypePatch::default().with_derive("clap::ValueEnum"),
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

    // The API cache buries this hash in every cache key: the generated file
    // covers the spec, the progenitor version, and every build.rs transform,
    // so entries written by one generated client never serve another. The
    // value is baked into the binary, so DefaultHasher only has to be
    // deterministic within one build.
    let client_hash = {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        content.hash(&mut hasher);
        hasher.finish()
    };
    println!("cargo:rustc-env=SNOUTY_GENERATED_API_HASH={client_hash:016x}");

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

/// Strip the `default: 50` from `Search_Request.limit`, so the generated
/// field is an `Option` that is omitted from the request body when unset.
///
/// An omitted limit is meaningful to the server: a non-streaming request
/// falls to the server-side default, and a streaming request stays unbounded.
/// With the default in the schema, progenitor bakes 50 into the generated
/// type and serializes it on every request — which would cut an unbounded
/// `--follow` off at 50 events once the server honors `limit` together with
/// `is_streaming`.
///
/// The pointer is asserted, so a spec refresh that drops the default (the
/// upstream fix) fails the build. ACTION when that happens: delete this
/// transform and its call.
fn unrequire_search_limit_default(spec: &mut serde_json::Value) {
    remove_schema_key(
        spec,
        "/components/schemas/Search_Request/properties/limit",
        "default",
        "unrequire_search_limit_default",
    );
}

/// Strip the schema default from `Execute_Command_Request.include_system_logs`,
/// so the request omits the field unless `runs exec --events` sets it.
fn unrequire_include_system_logs_default(spec: &mut serde_json::Value) {
    remove_schema_key(
        spec,
        "/components/schemas/Execute_Command_Request/properties/include_system_logs",
        "default",
        "unrequire_include_system_logs_default",
    );
}

/// Strip the schema default from `Execute_Command_Request.timeout_seconds`,
/// so the request omits the field unless `runs exec --timeout` sets it, and
/// the server's own default applies.
fn unrequire_exec_timeout_default(spec: &mut serde_json::Value) {
    remove_schema_key(
        spec,
        "/components/schemas/Execute_Command_Request/properties/timeout_seconds",
        "default",
        "unrequire_exec_timeout_default",
    );
}

/// snouty does not show a property's description, in its human output or in
/// `--json`, so drop the field from the generated property types.
fn drop_property_description(spec: &mut serde_json::Value) {
    remove_schema_key(
        spec,
        "/components/schemas/Property_Base/properties",
        "description",
        "drop_property_description",
    );
}

/// Drop `statusCode` from the launch success responses. The HTTP status is the
/// success signal (#180). Tenant release 63.3 does not send the field (#336).
fn drop_launch_status_code(spec: &mut serde_json::Value) {
    for name in ["Launch_Response", "Launch_MVD_Response"] {
        let schema = spec
            .pointer_mut(&format!("/components/schemas/{name}"))
            .and_then(serde_json::Value::as_object_mut)
            .unwrap_or_else(|| panic!("openapi spec has no {name}"));
        let required = schema
            .get_mut("required")
            .and_then(serde_json::Value::as_array_mut)
            .filter(|required| required.iter().any(|field| field == "statusCode"))
            .unwrap_or_else(|| {
                panic!(
                    "{name} no longer requires `statusCode`; \
                     delete `drop_launch_status_code` in build.rs"
                )
            });
        required.retain(|field| field != "statusCode");
        // OpenAPI 3.0 requires a non-empty `required` array.
        if required.is_empty() {
            schema.remove("required");
        }
        schema
            .get_mut("properties")
            .and_then(serde_json::Value::as_object_mut)
            .and_then(|properties| properties.remove("statusCode"));
    }
}

/// The spec omits `antithesis.test_name` from `MVD_Params`, but
/// `/api/v1/launch/debugging` accepts it: on orbitinghail, release 64.0, the
/// new session lists it in its `parameters`.
fn add_mvd_test_name(spec: &mut serde_json::Value) {
    let variants = spec
        .pointer_mut("/components/schemas/MVD_Params/oneOf")
        .and_then(serde_json::Value::as_array_mut)
        .expect("openapi spec has no MVD_Params.oneOf; update `add_mvd_test_name` in build.rs");
    for variant in variants {
        let properties = variant
            .get_mut("properties")
            .and_then(serde_json::Value::as_object_mut)
            .expect("an MVD_Params variant has no properties; update `add_mvd_test_name`");
        assert!(
            properties
                .insert(
                    "antithesis.test_name".to_owned(),
                    serde_json::json!({
                        "type": "string",
                        "description": "Title for the debugging session"
                    }),
                )
                .is_none(),
            "MVD_Params now documents antithesis.test_name; delete `add_mvd_test_name` in build.rs"
        );
    }
}

/// Decode `Params.antithesis.performance_tier` as a plain string. `runs list`
/// and `runs show` decode every run's parameters through `Params`, so a closed
/// enum would fail a whole listing on one tier this build does not know. The
/// enum moves to a `PerformanceTier` schema, which `--performance-tier` parses
/// into.
fn open_performance_tier(spec: &mut serde_json::Value) {
    let tiers = remove_schema_key(
        spec,
        "/components/schemas/Params/properties/antithesis.performance_tier",
        "enum",
        "open_performance_tier",
    );
    assert!(
        tiers
            .as_array()
            .is_some_and(|tiers| !tiers.is_empty() && tiers.iter().all(|t| t.is_string())),
        "Params.antithesis.performance_tier's enum is not a list of strings; update \
         `open_performance_tier` in build.rs"
    );
    let description = spec
        .pointer("/components/schemas/Params/properties/antithesis.performance_tier/description")
        .cloned()
        .unwrap_or_default();
    let schemas = spec
        .pointer_mut("/components/schemas")
        .and_then(serde_json::Value::as_object_mut)
        .expect("openapi spec has /components/schemas");
    assert!(
        schemas
            .insert(
                PERFORMANCE_TIER_SCHEMA.to_owned(),
                serde_json::json!({"type": "string", "enum": tiers, "description": description}),
            )
            .is_none(),
        "openapi spec now defines {PERFORMANCE_TIER_SCHEMA}; delete `open_performance_tier`'s \
         copy in build.rs"
    );
}

/// The schema `open_performance_tier` adds.
const PERFORMANCE_TIER_SCHEMA: &str = "PerformanceTier";

/// Remove `key` from the object at `pointer`, and return its value. Both must
/// exist, so a spec refresh that makes `transform` a no-op fails the build.
fn remove_schema_key(
    spec: &mut serde_json::Value,
    pointer: &str,
    key: &str,
    transform: &str,
) -> serde_json::Value {
    let object = spec
        .pointer_mut(pointer)
        .and_then(serde_json::Value::as_object_mut)
        .unwrap_or_else(|| {
            panic!("openapi spec has no object at {pointer}; update `{transform}` in build.rs")
        });
    object.remove(key).unwrap_or_else(|| {
        panic!("openapi spec has no `{key}` at {pointer}; delete `{transform}` in build.rs")
    })
}

/// Tag `Moment.vtime` with a private `format: vtime` marker for the
/// `with_conversion` mapping registered above. The marker is injected here
/// rather than edited into `src/openapi.json`, because that file is a
/// vendored upstream artifact — the next spec refresh would silently drop the
/// edit.
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
