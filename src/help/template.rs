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
        Name::resolve(self.page, text)?.write(self.page, self.reader, text, None)
    }

    /// `t()` of a filter block's text, without the whitespace around it.
    /// Over MCP, the call of an example that spans lines puts one param on
    /// each line, under the indent of the example's first line.
    pub(super) fn t_block(self, text: &str) -> askama::Result<String> {
        let body = text.trim();
        let indent = body.contains('\n').then(|| first_line_indent(text));
        Name::resolve(self.page, body)?.write(self.page, self.reader, body, indent)
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
    /// `{% filter cmd %}…{% endfilter %}` is `t_block()` of the block's
    /// text, for an example that spans lines or holds quotes. The tags can
    /// stand on lines of their own: the whitespace around the text drops out.
    #[askama::filter_fn]
    pub fn cmd(
        text: impl std::fmt::Display,
        values: &dyn askama::Values,
    ) -> askama::Result<String> {
        let context: &super::Context = askama::get_value(values, "context")?;
        context.t_block(&text.to_string())
    }
}

/// The indent of the first line of `text` that holds more than whitespace.
fn first_line_indent(text: &str) -> &str {
    let lead = &text[..text.len() - text.trim_start().len()];
    lead.rsplit('\n').next().unwrap_or(lead)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_line_indent_skips_blank_lines() {
        for (text, indent) in [
            ("snouty", ""),
            ("\n  snouty \\\n    --x\n  ", "  "),
            ("  \n\t\n    snouty", "    "),
            ("\r\n  snouty", "  "),
            ("", ""),
        ] {
            assert_eq!(first_line_indent(text), indent, "{text:?}");
        }
    }

    #[test]
    fn a_block_spans_lines_over_mcp_only_when_its_text_does() {
        let context = Context {
            page: HelpPage::RunsWait,
            reader: Reader::Mcp,
        };
        let one_line = context.t_block("\n  runs wait <run_id> --until ready\n  ");
        assert_eq!(
            one_line.unwrap(),
            r#"runs_wait {"run_id": "<run_id>", "until": "ready"}"#
        );
        let two_lines = context.t_block("\n  runs wait <run_id> \\\n    --until ready\n  ");
        assert_eq!(
            two_lines.unwrap(),
            "runs_wait {\n    \"run_id\": \"<run_id>\",\n    \"until\": \"ready\"\n  }"
        );
    }
}
