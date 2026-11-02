//! The table of help pages: each page's template and command.

use std::fmt;

use super::template::Context;

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

            /// The path below `snouty` of the command whose long help this is.
            pub fn path(self) -> &'static [&'static str] {
                match self {
                    $(HelpPage::$variant => &[$($part),*],)*
                }
            }

            /// Sets the page as its command's long help, for
            /// [`clap::Command::defer`], which takes a function.
            pub(super) fn set_long_help(self) -> fn(clap::Command) -> clap::Command {
                match self {
                    $(HelpPage::$variant => |command| command.long_about(HelpPage::$variant.cli()),)*
                }
            }

            pub(super) fn render_template(self, context: Context) -> askama::Result<String> {
                use askama::Template;
                let values: [(&str, &dyn std::any::Any); 1] = [("context", &context)];
                match self {
                    $(HelpPage::$variant => templates::$variant { cli: context.is_cli(), context }
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

        /// A template calls the methods of [`Context`], such as `t()`.
        mod templates {
            use super::super::template::{Context, filters};

            $(
                #[derive(askama::Template)]
                #[template(path = $file, escape = "none")]
                #[allow(dead_code, reason = "a page uses only what its template calls")]
                pub(super) struct $variant {
                    pub(super) cli: bool,
                    pub(super) context: Context,
                }

                impl std::ops::Deref for $variant {
                    type Target = Context;

                    fn deref(&self) -> &Context {
                        &self.context
                    }
                }
            )*
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
    Version => "version.txt", ["version"];
    Update => "update.txt", ["update"];
    Docs => "docs.txt", ["docs"];
    Login => "login.txt", ["login"];
    Settings => "settings.txt", ["settings"];
    SettingsShow => "settings_show.txt", ["settings", "show"];
    SettingsTrust => "settings_trust.txt", ["settings", "trust"];
    SettingsDeny => "settings_deny.txt", ["settings", "deny"];
    Mcp => "mcp.txt", ["mcp"];
    DockerCredential => "docker_credential.txt", ["docker-credential"];
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
