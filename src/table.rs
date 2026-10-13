//! Borderless tables for human-facing output, laid out by `tabled`.

use tabled::builder::Builder;
use tabled::settings::formatting::TrimStrategy;
use tabled::settings::object::{Columns, Segment};
use tabled::settings::peaker::Peaker;
use tabled::settings::{Alignment, Padding, Style, Width};

/// One step of a table's shrink order: shrink `column` until its content is
/// `floor` columns wide. A column never shrinks below its header.
pub(crate) struct Shrink {
    pub(crate) column: usize,
    pub(crate) floor: usize,
}

/// How a table fits into a terminal width. A table that is narrower than
/// `width` does not change, so `usize::MAX` keeps every cell whole.
pub(crate) enum Fit<'a> {
    /// Each column is as wide as its widest cell.
    Unbounded,
    /// Cut cells with an ellipsis, one column at a time in the order of
    /// `order`, until the table is at most `width` columns wide. When every
    /// step is at its floor, the table stays wider than `width`.
    Truncate { width: usize, order: &'a [Shrink] },
    /// Like [`Fit::Truncate`], but wrap the cells at spaces instead of cutting
    /// them. Continuation lines start under the column. `tabled` wraps, not
    /// `render::wrap_text`, because the shrink order sets each column width.
    Wrap { width: usize, order: &'a [Shrink] },
}

/// Tells `tabled` which column to make one column narrower. `tabled` calls
/// `peak` until the table fits, and it calls it again if the column it gets
/// cannot shrink, so `peak` returns `None` when no step can shrink.
struct ShrinkOrder {
    /// Each floor is raised to the header width.
    steps: Vec<Shrink>,
}

impl Peaker for ShrinkOrder {
    /// `mins` holds the padding of each column, and `values` the width of
    /// each column with its padding.
    fn peak(&mut self, mins: &[usize], values: &[usize]) -> Option<usize> {
        self.steps
            .iter()
            .find(|step| values[step.column] > mins[step.column] + step.floor)
            .map(|step| step.column)
    }
}

/// Lays out `headers` above `rows`, with two spaces between columns and no
/// trailing whitespace. The columns in `right_aligned` align right; the other
/// columns align left. The caller sanitizes each cell.
pub(crate) fn render_table(
    headers: &[&str],
    rows: Vec<Vec<String>>,
    right_aligned: &[usize],
    fit: Fit,
) -> String {
    let shrink_order = |order: &[Shrink]| ShrinkOrder {
        steps: order
            .iter()
            .map(|step| Shrink {
                column: step.column,
                floor: step
                    .floor
                    .max(console::measure_text_width(headers[step.column])),
            })
            .collect(),
    };

    let mut builder = Builder::with_capacity(rows.len() + 1, headers.len());
    builder.push_record(headers.iter().copied());
    for row in rows {
        builder.push_record(row);
    }
    let mut table = builder.build();
    table
        .with(Style::empty())
        .with(Padding::new(0, 2, 0, 0))
        .modify(Columns::last(), Padding::zero());
    for &column in right_aligned {
        table.modify(Columns::one(column), Alignment::right());
    }
    match fit {
        Fit::Unbounded => {}
        Fit::Truncate { width, order } => {
            table.with(
                Width::truncate(width)
                    .suffix("…")
                    .priority(shrink_order(order)),
            );
        }
        Fit::Wrap { width, order } => {
            // A line that wraps at a space would otherwise start with it.
            table.modify(Segment::all(), TrimStrategy::Horizontal);
            table.with(
                Width::wrap(width)
                    .keep_words(true)
                    .priority(shrink_order(order)),
            );
        }
    }

    table
        .to_string()
        .lines()
        .map(str::trim_end)
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use hegel::Generator;
    use hegel::generators;

    fn row(cells: &[&str]) -> Vec<String> {
        cells.iter().map(|cell| cell.to_string()).collect()
    }

    #[test]
    fn unbounded_pads_columns_to_their_widest_cell() {
        let out = render_table(
            &["A", "NUM", "LAST"],
            vec![row(&["long cell", "7", "x"]), row(&["b", "1234", "yy"])],
            &[1],
            Fit::Unbounded,
        );
        assert_eq!(
            out,
            "A           NUM  LAST\n\
             long cell     7  x\n\
             b          1234  yy"
        );
    }

    #[test]
    fn colored_cells_measure_by_visible_width() {
        let red = console::style("red").red().force_styling(true).to_string();
        let out = render_table(
            &["A", "B"],
            vec![vec![red.clone(), "x".to_string()]],
            &[],
            Fit::Unbounded,
        );
        assert_eq!(out, format!("A    B\n{red}  x"));
    }

    #[test]
    fn truncate_shrinks_columns_in_order_and_keeps_the_rest_whole() {
        let order = [
            Shrink {
                column: 2,
                floor: 6,
            },
            Shrink {
                column: 1,
                floor: 0,
            },
            Shrink {
                column: 2,
                floor: 0,
            },
        ];
        let rows = || vec![row(&["id-1", "launcher-name", "a-long-run-name"])];
        let fit = |width| Fit::Truncate {
            width,
            order: &order,
        };

        // Column 2 shrinks first, down to its floor.
        let out = render_table(&["ID", "L", "RUN"], rows(), &[], fit(27));
        assert_eq!(out.lines().nth(1), Some("id-1  launcher-name  a-lon…"));
        // Then column 1 shrinks; column 0 has no step, so it stays whole.
        let out = render_table(&["ID", "L", "RUN"], rows(), &[], fit(22));
        assert_eq!(out.lines().nth(1), Some("id-1  launche…  a-lon…"));
        // Column 1 is at its header width, so column 2 shrinks again, but not
        // below its header.
        let out = render_table(&["ID", "L", "RUN"], rows(), &[], fit(1));
        assert_eq!(out, "ID    L  RUN\nid-1  …  a-…");
    }

    #[test]
    fn wrap_starts_continuation_lines_under_the_column() {
        let name = "can`t lose a write, even when the leader won`t answer in time";
        let out = render_table(
            &["STATUS", "N", "NAME"],
            vec![row(&["failing", "12", name])],
            &[1],
            Fit::Wrap {
                width: 34,
                order: &[Shrink {
                    column: 2,
                    floor: 20,
                }],
            },
        );
        assert_eq!(
            out,
            "STATUS    N  NAME\n\
             failing  12  can`t lose a write,\n\
             \x20            even when the leader\n\
             \x20            won`t answer in time"
        );
    }

    #[test]
    fn wrap_stops_at_the_floor() {
        let out = render_table(
            &["STATUS", "NAME"],
            vec![row(&["failing", "a property name that does not fit"])],
            &[],
            Fit::Wrap {
                width: 10,
                order: &[Shrink {
                    column: 1,
                    floor: 20,
                }],
            },
        );
        assert_eq!(
            out,
            "STATUS   NAME\n\
             failing  a property name that\n\
             \x20        does not fit"
        );
    }

    /// With an unlimited width, a fitted table is the same as an unbounded
    /// one: piped output keeps every cell whole.
    #[hegel::test]
    fn unlimited_width_changes_nothing(tc: hegel::TestCase) {
        // Printable ASCII: space through `~`.
        let cells = generators::text()
            .min_size(1)
            .min_codepoint(0x20)
            .max_codepoint(0x7e);
        let rows = tc.draw(generators::vecs(
            generators::vecs(cells).min_size(3).max_size(3),
        ));
        let order = [
            Shrink {
                column: 2,
                floor: 0,
            },
            Shrink {
                column: 0,
                floor: 0,
            },
        ];
        let headers = ["A", "B", "C"];
        let unbounded = render_table(&headers, rows.clone(), &[1], Fit::Unbounded);
        let truncated = render_table(
            &headers,
            rows.clone(),
            &[1],
            Fit::Truncate {
                width: usize::MAX,
                order: &order,
            },
        );
        assert_eq!(truncated, unbounded);
        // Wrap trims each cell, so compare tables whose cells have no
        // leading or trailing spaces.
        if rows.iter().flatten().all(|cell| cell.trim() == cell) {
            let wrapped = render_table(
                &headers,
                rows,
                &[1],
                Fit::Wrap {
                    width: usize::MAX,
                    order: &order,
                },
            );
            assert_eq!(wrapped, unbounded);
        }
    }

    /// A table that can shrink enough fits in the width, and no line ends in
    /// whitespace.
    #[hegel::test]
    fn truncated_lines_fit_the_width(tc: hegel::TestCase) {
        let cells = generators::text().filter(|s: &String| s.chars().all(|c| !c.is_control()));
        let rows = tc.draw(generators::vecs(
            generators::vecs(cells).min_size(2).max_size(2),
        ));
        let first = rows
            .iter()
            .map(|row| console::measure_text_width(&row[0]))
            .chain([1])
            .max()
            .unwrap();
        // The narrowest the table can be: column 0 whole, then column 1 at
        // its header width.
        let width = tc.draw(
            generators::integers::<usize>()
                .min_value(first + 3)
                .max_value(first + 80),
        );
        let out = render_table(
            &["A", "B"],
            rows,
            &[],
            Fit::Truncate {
                width,
                order: &[Shrink {
                    column: 1,
                    floor: 0,
                }],
            },
        );
        for line in out.lines() {
            assert!(
                console::measure_text_width(line) <= width,
                "{line:?} > {width}"
            );
            assert_eq!(line.trim_end(), line);
        }
    }
}
