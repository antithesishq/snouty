//! The filters of `runs coverage`: globs over a module's name, and over a
//! location's file, class and function.
//!
//! Each flag is repeatable. A location passes a flag when it matches any of
//! its globs, and passes the filter when it passes every flag: the values of
//! one flag are alternatives, because a location has one file, one class and
//! one function, and different flags narrow different fields. A value with a
//! leading `!` excludes what it matches instead, and `\!` starts a value
//! with a literal `!`.

use color_eyre::eyre::Result;
use globset::{GlobBuilder, GlobMatcher};

use crate::error::user_error;

/// What a glob's `*` matches.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// A path: `*` and `?` stay within one `/`-separated segment, and `**`
    /// crosses segments. A glob without a `/` matches the file's base name.
    Path,
    /// A name: `*` matches any text.
    Name,
}

struct Glob {
    matcher: GlobMatcher,
    /// A path glob without a `/`, matched against the base name.
    base_name: bool,
}

impl Glob {
    fn new(glob: &str, kind: Kind) -> Result<Self> {
        let matcher = GlobBuilder::new(glob)
            .literal_separator(kind == Kind::Path)
            .backslash_escape(true)
            .build()
            .map_err(|err| user_error(format!("invalid glob {glob:?}: {err}")))?
            .compile_matcher();
        Ok(Self {
            matcher,
            base_name: kind == Kind::Path && !glob.contains('/'),
        })
    }

    fn is_match(&self, value: &str) -> bool {
        let value = if self.base_name {
            value.rsplit('/').next().unwrap_or(value)
        } else {
            value
        };
        self.matcher.is_match(value)
    }
}

/// The globs of one flag.
#[derive(Default)]
pub(super) struct Field {
    include: Vec<Glob>,
    exclude: Vec<Glob>,
}

impl Field {
    fn new(values: &[String], kind: Kind) -> Result<Self> {
        let mut field = Self::default();
        for value in values {
            if let Some(glob) = value.strip_prefix('!') {
                field.exclude.push(Glob::new(glob, kind)?);
            } else {
                let glob = value.strip_prefix('\\').filter(|v| v.starts_with('!'));
                field.include.push(Glob::new(glob.unwrap_or(value), kind)?);
            }
        }
        Ok(field)
    }

    pub(super) fn is_empty(&self) -> bool {
        self.include.is_empty() && self.exclude.is_empty()
    }

    /// Whether a value passes: it matches an include glob, or there is none,
    /// and it matches no exclude glob. A field has several names (a class's
    /// full and short names), and passes when any name does. A missing value
    /// matches no glob.
    pub(super) fn passes(&self, names: &[&str]) -> bool {
        let any = |globs: &[Glob]| names.iter().any(|n| globs.iter().any(|g| g.is_match(n)));
        (self.include.is_empty() || any(&self.include)) && !any(&self.exclude)
    }
}

/// The filters of a `runs coverage` call, other than `--module`, which the
/// server applies.
pub(super) struct Filter {
    pub(super) module_name: Field,
    pub(super) file: Field,
    pub(super) class: Field,
    pub(super) function: Field,
}

impl Filter {
    pub(super) fn new(
        module_names: &[String],
        files: &[String],
        classes: &[String],
        functions: &[String],
    ) -> Result<Self> {
        Ok(Self {
            module_name: Field::new(module_names, Kind::Name)?,
            file: Field::new(files, Kind::Path)?,
            class: Field::new(classes, Kind::Name)?,
            function: Field::new(functions, Kind::Name)?,
        })
    }
}

/// `--module` values: the module IDs to request, and the IDs to skip. An ID
/// is exact, not a glob.
pub(super) struct ModuleIds {
    pub(super) include: Vec<String>,
    pub(super) exclude: Vec<String>,
}

impl ModuleIds {
    pub(super) fn new(values: &[String]) -> Self {
        let mut ids = Self {
            include: Vec::new(),
            exclude: Vec::new(),
        };
        for value in values {
            if let Some(id) = value.strip_prefix('!') {
                ids.exclude.push(id.to_string());
            } else {
                let id = value.strip_prefix('\\').filter(|v| v.starts_with('!'));
                ids.include.push(id.unwrap_or(value).to_string());
            }
        }
        ids.include.dedup();
        ids
    }

    pub(super) fn passes(&self, id: &str) -> bool {
        (self.include.is_empty() || self.include.iter().any(|i| i == id))
            && !self.exclude.iter().any(|e| e == id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn field(values: &[&str], kind: Kind) -> Field {
        let values: Vec<String> = values.iter().map(ToString::to_string).collect();
        Field::new(&values, kind).unwrap()
    }

    #[test]
    fn a_path_glob_stays_in_its_segment_and_double_star_crosses() {
        let f = field(&["src/*.rs"], Kind::Path);
        assert!(f.passes(&["src/main.rs"]));
        assert!(!f.passes(&["src/raft/log.rs"]));
        let f = field(&["src/**/*.rs"], Kind::Path);
        assert!(f.passes(&["src/main.rs"]));
        assert!(f.passes(&["src/raft/log.rs"]));
    }

    #[test]
    fn a_path_glob_without_a_slash_matches_the_base_name() {
        let f = field(&["api.go"], Kind::Path);
        assert!(f.passes(&["/src/api.go"]));
        assert!(!f.passes(&["/src/api.go.orig"]));
    }

    #[test]
    fn a_name_glob_star_matches_any_text() {
        let f = field(&["*Disk*"], Kind::Name);
        assert!(f.passes(&["krill_torture::disk::Disk"]));
    }

    #[test]
    fn repeated_values_are_alternatives_and_a_bang_excludes() {
        let f = field(&["*.go", "*.rs", "!*_test.go"], Kind::Path);
        assert!(f.passes(&["a.go"]));
        assert!(f.passes(&["b.rs"]));
        assert!(!f.passes(&["a_test.go"]));
        assert!(!f.passes(&["c.py"]));
        // Only excludes: everything else passes.
        let f = field(&["!**/vendor/**"], Kind::Path);
        assert!(f.passes(&["src/a.go"]));
        assert!(!f.passes(&["src/vendor/x/a.go"]));
    }

    #[test]
    fn an_escaped_bang_is_literal() {
        let f = field(&["\\!*"], Kind::Name);
        assert!(f.passes(&["!weird"]));
        assert!(!f.passes(&["weird"]));
    }

    #[test]
    fn a_missing_value_fails_an_include_and_passes_an_exclude() {
        assert!(!field(&["*"], Kind::Name).passes(&[]));
        assert!(field(&["!*"], Kind::Name).passes(&[]));
    }

    #[test]
    fn module_ids_are_exact() {
        let ids = ModuleIds::new(&["a".into(), "!b".into()]);
        assert!(ids.passes("a"));
        assert!(!ids.passes("b"));
        assert!(!ids.passes("c"));
        let ids = ModuleIds::new(&["!b".into()]);
        assert!(ids.passes("c"));
        assert!(!ids.passes("b"));
    }
}
