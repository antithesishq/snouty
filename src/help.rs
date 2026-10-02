//! Long help text. build.rs renders each `help/*.txt` template once per
//! [`Target`], and this module embeds the rendered pages.

/// Who reads a rendered help page.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Target {
    /// The command's `--help`.
    Cli,
    /// The full help of an MCP tool, which `tool_help` returns.
    Mcp,
}

/// One page for each `help/*.txt` template.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HelpPage {
    Launch,
    Runs,
    Debug,
    Completions,
    Validate,
    Doctor,
    Update,
    Docs,
    Login,
    Mcp,
    DocsSearch,
    DocsTree,
    DocsShow,
    DocsSqlite,
    RunsList,
    RunsShow,
    RunsWait,
    RunsProperties,
    RunsBuildLogs,
    RunsLogs,
    RunsExec,
    RunsEvents,
    RunsSearch,
}

/// The page `help/$name.txt`, as build.rs rendered it for `$target`.
macro_rules! page {
    ($target:expr, $name:literal) => {
        match $target {
            Target::Cli => include_str!(concat!(env!("OUT_DIR"), "/help/cli/", $name, ".txt")),
            Target::Mcp => include_str!(concat!(env!("OUT_DIR"), "/help/mcp/", $name, ".txt")),
        }
    };
}

impl HelpPage {
    /// A `const fn`, so that a clap `long_about` attribute can take the text.
    pub const fn text(self, target: Target) -> &'static str {
        match self {
            HelpPage::Launch => page!(target, "launch"),
            HelpPage::Runs => page!(target, "runs"),
            HelpPage::Debug => page!(target, "debug"),
            HelpPage::Completions => page!(target, "completions"),
            HelpPage::Validate => page!(target, "validate"),
            HelpPage::Doctor => page!(target, "doctor"),
            HelpPage::Update => page!(target, "update"),
            HelpPage::Docs => page!(target, "docs"),
            HelpPage::Login => page!(target, "login"),
            HelpPage::Mcp => page!(target, "mcp"),
            HelpPage::DocsSearch => page!(target, "docs_search"),
            HelpPage::DocsTree => page!(target, "docs_tree"),
            HelpPage::DocsShow => page!(target, "docs_show"),
            HelpPage::DocsSqlite => page!(target, "docs_sqlite"),
            HelpPage::RunsList => page!(target, "runs_list"),
            HelpPage::RunsShow => page!(target, "runs_show"),
            HelpPage::RunsWait => page!(target, "runs_wait"),
            HelpPage::RunsProperties => page!(target, "runs_properties"),
            HelpPage::RunsBuildLogs => page!(target, "runs_build_logs"),
            HelpPage::RunsLogs => page!(target, "runs_logs"),
            HelpPage::RunsExec => page!(target, "runs_exec"),
            HelpPage::RunsEvents => page!(target, "runs_events"),
            HelpPage::RunsSearch => page!(target, "runs_search"),
        }
    }
}
