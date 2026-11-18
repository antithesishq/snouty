//! A tree printed one node to a line, behind an optional gutter column:
//!
//! ```text
//! 2 files  src
//! 1 file   ┣━━ main.rs
//!          ┃   a line of the body of main.rs
//! 1 file   ┗━━ lib
//!              ┗━━ util.rs
//! ```
//!
//! [`render`] knows nothing of what the nodes hold: every [`Node`] prints.

use std::borrow::Cow;
use std::fmt::Write as _;

use console::{Style, measure_text_width};

use crate::render::Truncate;

/// The branch in front of a child that has a sibling after it, and the stem
/// under it.
const BRANCH: &str = "┣━━ ";
const STEM: &str = "┃   ";
/// The branch in front of the last child, and the stem under it.
const LAST_BRANCH: &str = "┗━━ ";
const LAST_STEM: &str = "    ";

/// A part of a label, in one style.
#[must_use]
pub struct Span {
    text: String,
    style: Style,
    truncate: Option<Truncate>,
}

impl Span {
    /// Plain text that is never cut.
    pub fn new(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            style: Style::new(),
            truncate: None,
        }
    }

    pub fn style(self, style: Style) -> Self {
        Self { style, ..self }
    }

    /// Cut the span to the room that the spans before it leave.
    pub fn truncate(self, truncate: Truncate) -> Self {
        Self {
            truncate: Some(truncate),
            ..self
        }
    }
}

/// One line of the tree, then the lines of its body, then its children.
#[derive(Default)]
pub struct Node {
    /// The text in front of the branches, left-aligned in a column as wide
    /// as the widest gutter of the tree. It can hold styles. When no node
    /// of the tree has a gutter, the tree has no gutter column.
    pub gutter: String,
    /// The spans of the line, from the left.
    pub label: Vec<Span>,
    /// Lines under the label, at the indent of the node's children. They
    /// can hold styles, and are never cut.
    pub body: Vec<String>,
    pub children: Vec<Node>,
}

/// The spans of `label` in `room` columns, each cut to the room that the
/// spans before it leave.
fn fit_label(label: &[Span], mut room: usize) -> String {
    let mut out = String::new();
    for span in label {
        let text = span
            .truncate
            .map_or(Cow::Borrowed(span.text.as_str()), |truncate| {
                truncate.apply(room, &span.text)
            });
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
    /// The widest gutter of the tree, 0 when the tree has no gutter column.
    gutter_width: usize,
    width: usize,
}

impl Printer {
    /// `gutter` padded to the gutter column, then two spaces.
    fn gutter(&self, gutter: &str) -> String {
        if self.gutter_width == 0 {
            return String::new();
        }
        let pad = self.gutter_width - measure_text_width(gutter);
        format!("{gutter}{:pad$}  ", "")
    }

    /// `branch` leads the node's own line, and `stem` the lines under it.
    fn node(&self, node: &Node, branch: &str, stem: &str, lines: &mut Vec<String>) {
        let gutter = self.gutter(&node.gutter);
        let room = self
            .width
            .saturating_sub(measure_text_width(&gutter) + measure_text_width(branch));
        let dim = Style::new().dim();
        lines.push(format!(
            "{gutter}{}{}",
            dim.apply_to(branch),
            fit_label(&node.label, room)
        ));
        let empty = self.gutter("");
        for line in &node.body {
            lines.push(format!("{empty}{}{line}", dim.apply_to(stem)));
        }
        for (index, child) in node.children.iter().enumerate() {
            let (turn, down) = if index + 1 == node.children.len() {
                (LAST_BRANCH, LAST_STEM)
            } else {
                (BRANCH, STEM)
            };
            self.node(
                child,
                &format!("{stem}{turn}"),
                &format!("{stem}{down}"),
                lines,
            );
        }
    }
}

/// The tree under `root`, one line per node and body line, in `width`
/// columns. The root has no branch.
pub fn render(root: &Node, width: usize) -> String {
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
        insta::assert_snapshot!(render(&root, usize::MAX), @r"
            1   root
            22  ┣━━ a
            3   ┃   ┣━━ a1
            3   ┃   ┗━━ a2
            4   ┗━━ b
            5       ┗━━ b1
        ");
    }

    #[test]
    fn a_tree_without_gutters_has_no_gutter_column() {
        let root = parent(
            "",
            "root",
            vec![parent("", "a", vec![leaf("", "a1")]), leaf("", "b")],
        );
        insta::assert_snapshot!(render(&root, usize::MAX), @r"
            root
            ┣━━ a
            ┃   ┗━━ a1
            ┗━━ b
        ");
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
        insta::assert_snapshot!(render(&root, usize::MAX), @r"
            1  root
            2  ┣━━ a
               ┃   x
               ┃   y
            3  ┗━━ b
                   z
        ");
    }

    #[test]
    fn spans_are_cut_where_they_say() {
        let root = Node {
            label: vec![
                Span::new("id"),
                Span::new(" abcdefghij").truncate(Truncate::end(0)),
            ],
            children: vec![
                Node {
                    label: vec![Span::new("a/b/c/d/e/f.rs").truncate(Truncate::start(0))],
                    ..Node::default()
                },
                Node {
                    label: vec![Span::new("abcdefghijklmn").truncate(Truncate::end(0))],
                    ..Node::default()
                },
            ],
            ..Node::default()
        };
        // 4 columns of branch: a child label has 12 - 4 = 8.
        insta::assert_snapshot!(render(&root, 12), @r"
            id abcdefgh…
            ┣━━ …/e/f.rs
            ┗━━ abcdefg…
        ");
    }

    #[test]
    fn a_span_never_gets_fewer_columns_than_its_floor() {
        let root = Node {
            label: vec![Span::new("abcdefghijklmn").truncate(Truncate::end(8))],
            ..Node::default()
        };
        insta::assert_snapshot!(render(&root, 0), @"abcdefg…");
    }

    #[test]
    fn a_span_without_truncation_overflows() {
        let root = Node {
            label: vec![Span::new("a-long-identifier")],
            ..Node::default()
        };
        insta::assert_snapshot!(render(&root, 10), @"a-long-identifier");
    }
}
