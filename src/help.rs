//! Long help text. build.rs renders each `help/*.txt` template, this module
//! embeds the renders, and [`HelpPage::cli`] wraps a page for `--help`.

use std::fmt;

use crate::render::{CodeSpans, wrap_text};

/// The widest a help page wraps on a terminal. Prose reads best well under
/// this width, so a wide terminal does not stretch the paragraphs. It is also
/// clap's own cap, so clap does not wrap the text again.
const MAX_WIDTH: usize = 100;

/// The width off a terminal. Pipes, files and tests get one fixed layout.
const PIPE_WIDTH: usize = 80;

/// Declares [`HelpPage`] with one row per page: the variant and the name of
/// its template, `help/$name.txt`.
macro_rules! help_pages {
    ($($variant:ident => $name:literal,)*) => {
        /// One page for each `help/*.txt` template.
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub enum HelpPage {
            $($variant,)*
        }

        impl HelpPage {
            pub const ALL: [HelpPage; [$($name),*].len()] = [$(HelpPage::$variant,)*];

            /// The page as build.rs rendered it, with each paragraph on one
            /// line.
            pub fn text(self) -> &'static str {
                match self {
                    $(HelpPage::$variant => include_str!(concat!(
                        env!("OUT_DIR"), "/help/", $name, ".txt"
                    )),)*
                }
            }
        }

        impl fmt::Display for HelpPage {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(match self {
                    $(HelpPage::$variant => $name,)*
                })
            }
        }
    };
}

help_pages! {
    Launch => "launch",
    Runs => "runs",
    Debug => "debug",
    Completions => "completions",
    Validate => "validate",
    Doctor => "doctor",
    Update => "update",
    Docs => "docs",
    Login => "login",
    DocsSearch => "docs_search",
    DocsTree => "docs_tree",
    DocsShow => "docs_show",
    DocsSqlite => "docs_sqlite",
    RunsList => "runs_list",
    RunsShow => "runs_show",
    RunsWait => "runs_wait",
    RunsCancel => "runs_cancel",
    RunsProperties => "runs_properties",
    RunsProperty => "runs_property",
    RunsStream => "runs_stream",
    RunsBuildLogs => "runs_build_logs",
    RunsLogs => "runs_logs",
    RunsExec => "runs_exec",
    RunsEvents => "runs_events",
    RunsSearch => "runs_search",
    RunsUsage => "runs_usage",
}

impl HelpPage {
    /// The page for `--help`, wrapped to the width of the terminal on stdout,
    /// up to [`MAX_WIDTH`]. Off a terminal, the page wraps to [`PIPE_WIDTH`].
    pub fn cli(self) -> String {
        let term = console::Term::stdout();
        let width = if term.is_term() {
            MAX_WIDTH.min(usize::from(term.size().1))
        } else {
            PIPE_WIDTH
        };
        self.wrapped(width)
    }

    /// The page with each paragraph wrapped to `width`.
    pub fn wrapped(self, width: usize) -> String {
        wrap_help(self.text(), width)
    }
}

/// Wraps each line of `text` that is a paragraph. An indented line is
/// preformatted, such as an example command or a table row, and is kept as
/// it is.
fn wrap_help(text: &str, width: usize) -> String {
    let mut lines = Vec::new();
    for line in text.split('\n') {
        if line.is_empty() || line.starts_with(char::is_whitespace) {
            lines.push(line.to_owned());
        } else {
            lines.extend(wrap_text(line, width, CodeSpans::Keep));
        }
    }
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use hegel::generators;

    /// A help text of paragraphs and indented lines.
    fn help_text(tc: &hegel::TestCase) -> String {
        let lines = tc.draw(generators::vecs(generators::text().alphabet("ab `-.:")));
        let indent = tc.draw(generators::vecs(generators::booleans()).min_size(lines.len()));
        lines
            .iter()
            .zip(indent)
            .map(|(line, indented)| {
                let line = line.trim();
                if indented && !line.is_empty() {
                    format!("  {line}")
                } else {
                    line.to_owned()
                }
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[hegel::test]
    fn wrapping_keeps_the_words_and_the_indented_lines(tc: hegel::TestCase) {
        let text = help_text(&tc);
        let width = tc.draw(generators::integers::<usize>().min_value(1).max_value(40));
        let wrapped = wrap_help(&text, width);
        let words = |s: &str| s.split_whitespace().map(str::to_owned).collect::<Vec<_>>();
        assert_eq!(words(&text), words(&wrapped));
        let indented = |s: &str| {
            s.split('\n')
                .filter(|l| l.starts_with(' '))
                .map(str::to_owned)
                .collect::<Vec<_>>()
        };
        assert_eq!(indented(&text), indented(&wrapped));
    }

    /// Every page wraps to the width, apart from indented lines and words
    /// that are longer than the width.
    #[test]
    fn pages_fit_the_width() {
        for page in HelpPage::ALL {
            for line in page.wrapped(PIPE_WIDTH).split('\n') {
                let one_word = !line.trim().contains(' ');
                assert!(
                    line.starts_with(' ') || one_word || line.chars().count() <= PIPE_WIDTH,
                    "{page}: {line}"
                );
            }
        }
    }
}
