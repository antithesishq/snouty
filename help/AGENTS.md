# Help templates

These rules cover the pages in `help/`, `src/help/`, and the help of an MCP
tool. The root `AGENTS.md` section "User-facing text" says what to write.
This file says how the templates work.

Each command's long help (`--help`) is a template in `help/`, one file per
command, named for its path below `snouty` with `_` between the parts:
`help/runs_build_logs.txt`. Do not write a long help as a string in Rust code.

- Write each paragraph on one line. `HelpPage::cli` wraps the paragraphs when
  the help prints: to the terminal width, up to 100 columns, and to 80
  columns off a terminal. Indent a line that must keep its layout, such as an
  example command or a table row; an indented line never wraps.
- Each template is an [askama](https://askama.rs) template, which the build
  compiles: a syntax error or a missing include fails `cargo build`. A page
  renders at run time, for two readers: `--help`, and the `tool_help` MCP
  tool. A `HelpPage` row in `src/help/pages.rs` names each page and its
  command.
  `cli::command()` sets each page as its command's long help; parse with
  `cli::parse()`, not with `Cli::parse`. clap renders a page only when it
  builds that command, so a run renders only the pages on its command path.
- Name each flag, positional arg, command and example with `t()`, never as
  literal text. `t()` looks the name up in the clap definition of the page's
  command, and writes it for the reader:

  | template | `--help` | `tool_help` |
  |---|---|---|
  | `{{ t("--begin-vtime")? }}` | `--begin-vtime` | `` `begin_vtime` `` |
  | `{{ t("input_hash")? }}` | `INPUT_HASH` | `` `input_hash` `` |
  | `` `{{ t("runs search")? }}` `` | `` `runs search` `` | `` `runs_search` `` |
  | `{{ t("snouty runs wait <run_id> --until ready")? }}` | the command as written | `runs_wait {"run_id": "<run_id>", "until": "ready"}` |

  Write an example that spans lines, or that holds quotes or backslashes, in
  a filter block, which takes its text as written. The whitespace around
  the text drops out, so the tags stand on lines of their own:

  ```
    {% filter cmd %}
    snouty debug \
      --run-id <run_id> \
      --title "stalled leader election"
    {% endfilter %}
  ```

  Over MCP, the call of a filter block that spans lines puts one param on
  each line, and a placeholder for a number param prints unquoted, as in
  `"vtime": <vtime>`.

  A name that does not exist, an example that clap refuses, or a literal
  value that its parser refuses is an error. A `<placeholder>` can stand for
  any value. A command with flags but not its required args, such as
  `runs properties --detail`, names the command, and over MCP its flags drop
  out. Over MCP, `t()` refuses a command with no tool, an arg that the tool
  leaves out, and a shell pipeline or redirect.
- A command has two output modes: text output, the default, and JSON, with
  `--json`. Do not call the default "human output".
- An MCP tool call returns the command's text output, with `--detail` when
  the command has that flag, and its `json` param runs it with `--json`
  instead. So the MCP reader needs the layout of the detailed output, and
  the JSON shape as what `json` returns: `With {{ t("--json")? }}, each run
  prints as one JSON object`.
- Put text that only one reader needs, such as the CLI's `--json` paragraph,
  a pipeline example, or a layout that only the default (not `--detail`)
  output has, in `{% if cli %}…{% else %}…{% endif %}`. The other
  reader's branch is not rendered, so `t()` does not check it for that
  reader. `{{ tool_table() }}` is the list of MCP tools.
- Describe what a command reports in words that suit both readers. Put a
  layout that both readers see, such as the columns of a command with no
  `--detail`, outside a block. Say that a command succeeds or fails; name exit
  codes, stdout and stderr only in a CLI block.
- A line that holds only a block tag (`if`, `else`, `endif`) prints as a
  blank line, and the render collapses each run of blank lines into one. So
  put a block that holds whole paragraphs on lines of its own, with blank
  lines around it as you like. In a run of lines that must stay together,
  such as a list of examples, write `{%- if cli %}`, `{%- else %}` and
  `{%- endif %}`: the mark drops the tag's line. A tag inside a line of text
  takes no mark.
- A `t()` error is a test failure: `tests/help_snapshots.rs` renders every
  page for both readers, and the error names the page.
- Write each page's whole text in its own file. When two pages share a
  paragraph, copy it into both: a reviewer reads a page as one file.
- `tests/help_snapshots.rs` keeps an [insta](https://insta.rs) snapshot of
  every command's `--help` as it prints off a terminal, in
  `tests/snapshots/help/`, and of each MCP tool's `tool_help`, in
  `tests/snapshots/mcp_help/`. A test fails when an MCP render names a flag
  outside an example. After you change a template, a flag or a
  subcommand, review the snapshot diff with `cargo insta review`, or accept
  every change and read the diff in git:

  ```
  INSTA_UPDATE=always cargo nextest run --test help_snapshots
  ```
