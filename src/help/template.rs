//! What a template sees: its reader, and the helpers that it calls.

use super::HelpPage;
use super::names::Name;
use crate::mcp;

/// Who reads a render of a help page.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reader {
    /// `--help` on the command line.
    Cli,
    /// The `tool_help` MCP tool, whose reader passes params by name.
    Mcp,
}

pub(super) fn render(page: HelpPage, reader: Reader) -> askama::Result<String> {
    page.render_template(Context { page, reader })
}

/// The page that a template renders, and its reader.
#[derive(Clone, Copy, Debug)]
pub(super) struct Context {
    page: HelpPage,
    reader: Reader,
}

impl Context {
    pub(super) fn is_cli(self) -> bool {
        self.reader == Reader::Cli
    }

    /// `t("--events")` is a flag of the page's command, `t("input_hash")` a
    /// positional arg of it, and anything else a command or an example. Each
    /// is written for the reader.
    pub(super) fn t(self, text: &str) -> askama::Result<String> {
        Name::resolve(self.page, text)?.write(self.page, self.reader, text)
    }

    /// `t()` of an example that spans lines. Over MCP, its call puts one
    /// param on each line, below a first line indented by `indent`.
    pub(super) fn t_multiline(self, text: &str, indent: &str) -> askama::Result<String> {
        match (Name::resolve(self.page, text)?, self.reader) {
            (Name::Example(example), Reader::Mcp) => {
                Ok(example.to_call(text)?.to_multiline(indent))
            }
            (name, reader) => name.write(self.page, reader, text),
        }
    }

    /// One line for each MCP tool: its name and what it does.
    #[allow(clippy::unused_self, reason = "a template calls it as a method")]
    pub(super) fn tool_table(self) -> String {
        let rows = mcp::summaries();
        let width = rows.iter().map(|(name, _)| name.len()).max().unwrap_or(0);
        rows.iter()
            .map(|(name, about)| format!("  {name:width$}  {about}"))
            .collect::<Vec<_>>()
            .join("\n")
    }
}

#[allow(
    clippy::inline_always,
    clippy::unused_self,
    reason = "askama::filter_fn generates the filter's code"
)]
pub(super) mod filters {
    /// `{% filter cmd %}…{% endfilter %}` is `t_multiline()` of the block's
    /// text, for an example that spans lines. The tags can stand on lines of their
    /// own: the whitespace around the text drops out.
    #[askama::filter_fn]
    pub fn cmd(
        text: impl std::fmt::Display,
        values: &dyn askama::Values,
    ) -> askama::Result<String> {
        let context: &super::Context = askama::get_value(values, "context")?;
        let text = text.to_string();
        let first_line = text.lines().find(|line| !line.trim().is_empty());
        let indent = first_line.map_or("", |line| &line[..line.len() - line.trim_start().len()]);
        context.t_multiline(text.trim(), indent)
    }
}
