//! Long help text. Each `help/*.txt` file is an askama template, which the
//! build compiles. A page renders at run time, for the command line or for
//! the `tool_help` MCP tool. [`HelpPage::cli`] wraps a page for `--help`.

mod example;
mod names;
mod pages;
mod template;

pub use pages::HelpPage;
pub use template::Reader;

use crate::cli::tree;
use crate::render::{CodeSpans, stdout_prose_width, wrap_text};

/// The width off a terminal. Pipes, files and tests get one fixed layout.
const PIPE_WIDTH: usize = 80;

/// When a command gets its long help.
#[derive(Clone, Copy, Debug)]
pub enum Attach {
    /// When clap builds the command, which it does only for the commands on
    /// the path that runs, wrapped for the terminal.
    Deferred,
    /// Now, wrapped to the width.
    Eager(usize),
}

/// Sets each page as the long help of its command below `command`.
pub fn attach(command: clap::Command, attach: Attach) -> clap::Command {
    HelpPage::ALL.into_iter().fold(command, |command, page| {
        tree::mut_at(command, page.path(), |command| match attach {
            Attach::Deferred => command.defer(page.set_long_help()),
            Attach::Eager(width) => command.long_about(page.wrapped(width)),
        })
    })
}

impl HelpPage {
    /// The page for `--help`, wrapped to the width of the terminal on stdout,
    /// up to the prose width. Off a terminal, the page wraps to
    /// [`PIPE_WIDTH`].
    pub fn cli(self) -> String {
        self.wrapped(stdout_prose_width().unwrap_or(PIPE_WIDTH))
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
        let text = template::render(self, reader)
            .unwrap_or_else(|err| panic!("help/{self}.txt for {reader:?}: {err}"));
        collapse_blank_lines(&text)
    }
}

/// `text` with no blank line at either end, and no two blank lines in a
/// row. A tag on a line of its own leaves a blank line, so a template can
/// put blank lines around a block whose text renders for one reader only.
fn collapse_blank_lines(text: &str) -> String {
    let mut lines: Vec<&str> = Vec::new();
    for line in text.trim_matches('\n').split('\n') {
        let line = if line.trim().is_empty() { "" } else { line };
        if !(line.is_empty() && lines.last().is_some_and(|last| last.is_empty())) {
            lines.push(line);
        }
    }
    lines.join("\n")
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

fn error(message: impl std::fmt::Display) -> askama::Error {
    askama::Error::custom(message.to_string())
}

#[cfg(test)]
mod tests {
    use hegel::generators;

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

    /// Each page is the long help of its own command, and no command has a
    /// long help that is not a page.
    #[test]
    fn every_page_is_one_commands_long_help() {
        fn walk(command: &clap::Command) {
            assert!(command.get_long_about().is_none(), "{}", command.get_name());
            command.get_subcommands().for_each(walk);
        }
        for page in HelpPage::ALL {
            assert!(tree::find(page.path()).is_some(), "{page}: no command");
            let others = HelpPage::ALL
                .into_iter()
                .filter(|other| other.path() == page.path());
            assert_eq!(others.count(), 1, "{page}: another page has its command");
        }
        walk(tree::root());
    }

    #[test]
    fn events_search_help_names_min_release() {
        let (major, minor) = crate::api::MIN_SEARCH_RELEASE;
        let sentence = format!("tenant release {major}.{minor} or newer");
        for page in [HelpPage::RunsSearch, HelpPage::RunsEvents] {
            assert!(page.text().contains(&sentence), "{page}");
        }
    }

    /// The verb list in the search help names every verb in
    /// [`crate::event_set_dsl::VERBS`].
    #[test]
    fn search_help_names_every_verb() {
        let text = HelpPage::RunsSearch.text();
        for verb in crate::event_set_dsl::VERBS {
            assert!(text.contains(verb), "missing verb {verb}");
        }
    }

    /// `--json` is a global flag, so every page says what its command does
    /// with it: prints JSON, or ignores the flag. The commands that warn
    /// "--json has no effect" at run time are the exception: their help
    /// never raises the subject.
    #[test]
    fn every_page_says_what_json_does() {
        const NO_JSON: [&str; 7] = [
            "validate",
            "completions",
            "update",
            "login",
            "mcp",
            "config trust",
            "config deny",
        ];
        for page in HelpPage::ALL {
            if !NO_JSON.contains(&page.path().join(" ").as_str()) {
                assert!(
                    page.text().contains("--json"),
                    "{page} says nothing about --json"
                );
            }
        }
    }

    #[test]
    fn blank_lines_collapse() {
        assert_eq!(
            collapse_blank_lines("\na\n\n \n\nb\n  c\n\n"),
            "a\n\nb\n  c"
        );
    }

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
