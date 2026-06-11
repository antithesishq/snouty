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

    let out_dir = std::env::var_os("OUT_DIR").unwrap();
    fs::create_dir_all(&out_dir).unwrap();
    generate_api_client(Path::new(&out_dir));
}

fn generate_api_client(out_dir: &Path) {
    let file = std::fs::File::open("src/openapi.json").unwrap();
    let spec = serde_json::from_reader(file).unwrap();

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
