//! Long help text. Each `help/*.txt` file is an askama template, which the
//! build compiles. A page renders at run time, for the command line or for
//! the `tool_help` MCP tool. [`HelpPage::cli`] wraps a page for `--help`.

mod render;

use std::fmt;

pub use render::Reader;

use crate::render::{CodeSpans, wrap_text};

/// The widest a help page wraps on a terminal. Prose reads best well under
/// this width, so a wide terminal does not stretch the paragraphs. It is also
/// clap's own cap, so clap does not wrap the text again.
const MAX_WIDTH: usize = 100;

/// The width off a terminal. Pipes, files and tests get one fixed layout.
const PIPE_WIDTH: usize = 80;

/// Declares [`HelpPage`] with one row per page: the variant, its template
/// file in `help/`, and the path of its command below `snouty`.
macro_rules! help_pages {
    ($($variant:ident => $file:literal, [$($part:literal),*];)*) => {
        /// One page for each `help/*.txt` template.
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub enum HelpPage {
            $($variant,)*
        }

        impl HelpPage {
            pub const ALL: [HelpPage; [$($file),*].len()] = [$(HelpPage::$variant,)*];

            /// Sets the page as its command's long help. clap runs this when it
            /// builds the command, which it does only for the commands on the
            /// path that runs, so a run renders at most those pages.
            pub fn set_long_help(self) -> fn(clap::Command) -> clap::Command {
                match self {
                    $(HelpPage::$variant => |command| command.long_about(HelpPage::$variant.cli()),)*
                }
            }

            /// The path below `snouty` of the command whose long help this is.
            pub fn path(self) -> &'static [&'static str] {
                match self {
                    $(HelpPage::$variant => &[$($part),*],)*
                }
            }

            fn render_template(self, context: render::Context) -> askama::Result<String> {
                use askama::Template;
                let values: [(&str, &dyn std::any::Any); 1] = [("context", &context)];
                match self {
                    $(HelpPage::$variant => pages::$variant { cli: context.is_cli(), context }
                        .render_with_values(&values),)*
                }
            }
        }

        impl fmt::Display for HelpPage {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let file = match self {
                    $(HelpPage::$variant => $file,)*
                };
                f.write_str(file.strip_suffix(".txt").unwrap_or(file))
            }
        }

        mod pages {
            use super::render::{self, Context};

            $(
                #[derive(askama::Template)]
                #[template(path = $file, escape = "none")]
                #[allow(dead_code, reason = "a page uses only what its template calls")]
                pub(super) struct $variant {
                    pub(super) cli: bool,
                    pub(super) context: Context,
                }

                #[allow(dead_code, reason = "a page uses only what its template calls")]
                impl $variant {
                    fn t(&self, text: &str) -> askama::Result<String> {
                        self.context.t(text)
                    }

                    fn tool_table(&self) -> String {
                        render::tool_table()
                    }
                }
            )*

            mod filters {
                /// `{% filter cmd %}…{% endfilter %}` is `t()` of the block's
                /// text, for an example that spans lines. The tags can stand on
                /// lines of their own: the whitespace around the text drops out.
                #[askama::filter_fn]
                pub fn cmd(
                    text: impl std::fmt::Display,
                    values: &dyn askama::Values,
                ) -> askama::Result<String> {
                    let context: &super::Context = askama::get_value(values, "context")?;
                    context.t(text.to_string().trim())
                }
            }
        }
    };
}

help_pages! {
    Launch => "launch.txt", ["launch"];
    Runs => "runs.txt", ["runs"];
    Debug => "debug.txt", ["debug"];
    Completions => "completions.txt", ["completions"];
    Validate => "validate.txt", ["validate"];
    Doctor => "doctor.txt", ["doctor"];
    Update => "update.txt", ["update"];
    Docs => "docs.txt", ["docs"];
    Login => "login.txt", ["login"];
    Mcp => "mcp.txt", ["mcp"];
    DocsSearch => "docs_search.txt", ["docs", "search"];
    DocsTree => "docs_tree.txt", ["docs", "tree"];
    DocsShow => "docs_show.txt", ["docs", "show"];
    DocsSqlite => "docs_sqlite.txt", ["docs", "sqlite"];
    RunsList => "runs_list.txt", ["runs", "list"];
    RunsShow => "runs_show.txt", ["runs", "show"];
    RunsWait => "runs_wait.txt", ["runs", "wait"];
    RunsCancel => "runs_cancel.txt", ["runs", "cancel"];
    RunsProperties => "runs_properties.txt", ["runs", "properties"];
    RunsProperty => "runs_property.txt", ["runs", "property"];
    RunsStream => "runs_stream.txt", ["runs", "stream"];
    RunsBuildLogs => "runs_build_logs.txt", ["runs", "build-logs"];
    RunsLogs => "runs_logs.txt", ["runs", "logs"];
    RunsExec => "runs_exec.txt", ["runs", "exec"];
    RunsEvents => "runs_events.txt", ["runs", "events"];
    RunsSearch => "runs_search.txt", ["runs", "search"];
    RunsUsage => "runs_usage.txt", ["runs", "usage"];
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
        wrap_help(&self.text(), width)
    }

    /// The page for the command line, with each paragraph on one line.
    pub fn text(self) -> String {
        self.render(Reader::Cli)
    }

    /// The page for the `tool_help` MCP tool, which names each arg by its
    /// param and each command by its tool.
    pub fn mcp(self) -> String {
        self.render(Reader::Mcp)
    }

    /// A template error is a bug in the template, which the tests render.
    fn render(self, reader: Reader) -> String {
        let context = render::Context { page: self, reader };
        let text = self
            .render_template(context)
            .unwrap_or_else(|err| panic!("help/{self}.txt for {reader:?}: {err}"));
        // A file ends in a newline; the page text does not.
        match text.strip_suffix('\n') {
            Some(text) => text.to_owned(),
            None => text,
        }
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

    /// Every template file in `help/` is a page. A page that no row names
    /// would never render.
    #[test]
    fn every_template_is_a_page() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("help");
        let mut files: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "txt"))
            .map(|path| path.file_stem().unwrap().to_string_lossy().into_owned())
            .collect();
        files.sort();
        let mut pages: Vec<String> = HelpPage::ALL.iter().map(ToString::to_string).collect();
        pages.sort();
        assert_eq!(files, pages);
    }
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
