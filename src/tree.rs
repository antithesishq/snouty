//! A tree printed one node to a line, behind a gutter column:
//!
//! ```text
//! 60.0%  3/5  module
//! 66.7%  2/3  ┣━━ src/main.rs
//!             ┃   a line of the body of src/main.rs
//! 50.0%  1/2  ┗━━ src/work.rs
//! ```
//!
//! [`render`] knows nothing of what the nodes hold: every [`Node`] prints.

use std::fmt::Write as _;

use console::{Style, measure_text_width, truncate_str};
use ptree::print_config::UTF_CHARS_BOLD;

use crate::table::truncate_start;

/// The fewest columns that a label gets, however deep its node is.
const MIN_LABEL_WIDTH: usize = 8;

/// Where a [`Span`] is cut when its label does not fit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Cut {
    Never,
    Start,
    End,
}

/// A part of a label, in one style.
pub(crate) struct Span {
    text: String,
    style: Style,
    cut: Cut,
}

impl Span {
    /// Plain text, cut at its end.
    pub(crate) fn new(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            style: Style::new(),
            cut: Cut::End,
        }
    }

    pub(crate) fn style(self, style: Style) -> Self {
        Self { style, ..self }
    }

    pub(crate) fn cut(self, cut: Cut) -> Self {
        Self { cut, ..self }
    }
}

/// One line of the tree, then the lines of its body, then its children.
#[derive(Default)]
pub(crate) struct Node {
    /// The text in front of the branches, left-aligned in a column as wide
    /// as the widest gutter of the tree. It can hold styles.
    pub gutter: String,
    /// Cut to fit the width, span by span from the left.
    pub label: Vec<Span>,
    /// Lines under the label, at the indent of the node's children. They
    /// can hold styles, and are never cut.
    pub body: Vec<String>,
    pub children: Vec<Node>,
}

/// The spans of `label`, each cut to the room that the spans before it
/// leave.
fn fit(label: &[Span], mut room: usize) -> String {
    let mut out = String::new();
    for span in label {
        let text = match span.cut {
            Cut::Never => span.text.clone(),
            Cut::Start => truncate_start(&span.text, room),
            Cut::End => truncate_str(&span.text, room, "…").into_owned(),
        };
        room = room.saturating_sub(measure_text_width(&text));
        let _ = write!(out, "{}", span.style.apply_to(text));
    }
    out
}

fn gutter_width(node: &Node) -> usize {
    node.children
        .iter()
        .map(gutter_width)
        .fold(measure_text_width(&node.gutter), usize::max)
}

struct Printer {
    gutter_width: usize,
    width: usize,
}

impl Printer {
    /// `branch` leads the node's own line, and `stem` the lines under it.
    fn node(&self, node: &Node, branch: &str, stem: &str, lines: &mut Vec<String>) {
        let gutter_width = self.gutter_width;
        let pad = gutter_width - measure_text_width(&node.gutter);
        let room = self
            .width
            .saturating_sub(gutter_width + 2 + measure_text_width(branch))
            .max(MIN_LABEL_WIDTH);
        let dim = Style::new().dim();
        lines.push(format!(
            "{}{:pad$}  {}{}",
            node.gutter,
            "",
            dim.apply_to(branch),
            fit(&node.label, room)
        ));
        for line in &node.body {
            lines.push(format!(
                "{:gutter_width$}  {}{line}",
                "",
                dim.apply_to(stem)
            ));
        }
        let chars = UTF_CHARS_BOLD;
        let right = chars.right.repeat(2);
        for (index, child) in node.children.iter().enumerate() {
            let (turn, down) = if index + 1 == node.children.len() {
                (chars.turn_right, chars.empty)
            } else {
                (chars.down_and_right, chars.down)
            };
            self.node(
                child,
                &format!("{stem}{turn}{right} "),
                &format!("{stem}{down}   "),
                lines,
            );
        }
    }
}

/// The tree under `root`, one line per node and body line, in `width`
/// columns. The root has no branch.
pub(crate) fn render(root: &Node, width: usize) -> String {
    let printer = Printer {
        gutter_width: gutter_width(root),
        width,
    };
    let mut lines = Vec::new();
    printer.node(root, "", "", &mut lines);
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn leaf(gutter: &str, label: &str) -> Node {
        Node {
            gutter: gutter.to_string(),
            label: vec![Span::new(label)],
            ..Node::default()
        }
    }

    fn parent(gutter: &str, label: &str, children: Vec<Node>) -> Node {
        Node {
            children,
            ..leaf(gutter, label)
        }
    }

    #[test]
    fn branches_follow_the_depth_and_the_last_child() {
        let root = parent(
            "1",
            "root",
            vec![
                parent("22", "a", vec![leaf("3", "a1"), leaf("3", "a2")]),
                parent("4", "b", vec![leaf("5", "b1")]),
            ],
        );
        assert_eq!(
            render(&root, usize::MAX),
            "1   root\n\
             22  ┣━━ a\n\
             3   ┃   ┣━━ a1\n\
             3   ┃   ┗━━ a2\n\
             4   ┗━━ b\n\
             5       ┗━━ b1"
        );
    }

    #[test]
    fn a_body_hangs_at_the_indent_of_the_children() {
        let root = parent(
            "1",
            "root",
            vec![
                Node {
                    body: vec!["x".to_string(), "y".to_string()],
                    ..leaf("2", "a")
                },
                Node {
                    body: vec!["z".to_string()],
                    ..leaf("3", "b")
                },
            ],
        );
        assert_eq!(
            render(&root, usize::MAX),
            "1  root\n\
             2  ┣━━ a\n   \
             ┃   x\n   \
             ┃   y\n\
             3  ┗━━ b\n       \
             z"
        );
    }

    #[test]
    fn labels_are_cut_where_their_spans_say() {
        let root = Node {
            label: vec![Span::new("id").cut(Cut::Never), Span::new(" abcdefghij")],
            children: vec![
                Node {
                    label: vec![Span::new("a/b/c/d/e/f.rs").cut(Cut::Start)],
                    ..Node::default()
                },
                Node {
                    label: vec![Span::new("abcdefghijklmn")],
                    ..Node::default()
                },
            ],
            ..Node::default()
        };
        // 2 columns of gutter, 4 of branch: a child label has 14 - 6 = 8.
        assert_eq!(
            render(&root, 14),
            "  id abcdefgh…\n  \
             ┣━━ …/e/f.rs\n  \
             ┗━━ abcdefg…"
        );
    }

    #[test]
    fn a_label_never_gets_fewer_than_the_minimum() {
        let root = parent("", "root", vec![leaf("", "abcdefghijklmn")]);
        assert_eq!(render(&root, 0), "  root\n  ┗━━ abcdefg…");
    }

    #[test]
    fn a_span_that_never_cuts_overflows() {
        let root = Node {
            label: vec![Span::new("a-long-identifier").cut(Cut::Never)],
            ..Node::default()
        };
        assert_eq!(render(&root, 10), "  a-long-identifier");
    }
}
