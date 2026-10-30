//! Borderless tables for text output.

use console::{Alignment, measure_text_width, pad_str, truncate_str};

use crate::render::{CodeSpans, wrap_text};

/// The spaces between two columns.
const GAP: &str = "  ";

#[derive(Clone, Copy)]
pub(crate) enum Align {
    Left,
    Right,
}

/// How a column gives up width when the table is wider than the terminal.
/// No column shrinks below its header.
#[derive(Clone, Copy)]
pub(crate) enum Fit {
    /// The column is always as wide as its widest cell.
    Fixed,
    /// Cut cells with an ellipsis, down to `floor` columns.
    Truncate { floor: usize },
    /// Wrap cells at spaces, down to `floor` columns. Continuation lines start
    /// under the column. A word that is longer than the column stays whole,
    /// and widens the column.
    Wrap { floor: usize },
}

pub(crate) struct Column {
    header: &'static str,
    align: Align,
    fit: Fit,
    priority: usize,
}

impl Column {
    /// A left-aligned [`Fit::Fixed`] column with priority 0.
    pub(crate) fn new(header: &'static str) -> Self {
        Self {
            header,
            align: Align::Left,
            fit: Fit::Fixed,
            priority: 0,
        }
    }

    pub(crate) fn align(self, align: Align) -> Self {
        Self { align, ..self }
    }

    pub(crate) fn fit(self, fit: Fit) -> Self {
        Self { fit, ..self }
    }

    /// Columns shrink in ascending priority: a column shrinks to its floor
    /// before a column with a higher priority starts to shrink. Columns with
    /// equal priority shrink left to right.
    pub(crate) fn priority(self, priority: usize) -> Self {
        Self { priority, ..self }
    }
}

pub(crate) struct Table {
    columns: Vec<Column>,
}

impl Table {
    pub(crate) fn builder() -> Self {
        Self {
            columns: Vec::new(),
        }
    }

    pub(crate) fn column(mut self, column: Column) -> Self {
        self.columns.push(column);
        self
    }

    /// Lays out the headers above `rows`, with two spaces between columns and
    /// no trailing whitespace. Columns shrink until the table is at most
    /// `width` columns wide, or every column is at its floor. A table that is
    /// narrower than `width` does not change, so `usize::MAX` keeps every cell
    /// whole. Each row has one cell per column, and the caller sanitizes each
    /// cell.
    pub(crate) fn render(&self, rows: Vec<Vec<String>>, width: usize) -> String {
        let mut widths: Vec<usize> = self
            .columns
            .iter()
            .enumerate()
            .map(|(index, column)| {
                rows.iter()
                    .map(|row| measure_text_width(&row[index]))
                    .fold(measure_text_width(column.header), usize::max)
            })
            .collect();

        let total = widths.iter().sum::<usize>() + GAP.len() * (widths.len() - 1);
        let mut excess = total.saturating_sub(width);
        let mut order: Vec<usize> = (0..self.columns.len()).collect();
        order.sort_by_key(|&index| self.columns[index].priority);
        for index in order {
            let column = &self.columns[index];
            let floor = match column.fit {
                Fit::Fixed => continue,
                Fit::Truncate { floor } | Fit::Wrap { floor } => floor,
            };
            let floor = floor.max(measure_text_width(column.header));
            let shrink = excess.min(widths[index].saturating_sub(floor));
            widths[index] -= shrink;
            excess -= shrink;
        }

        // Each row as a list of cells, and each cell as its lines.
        let header = self
            .columns
            .iter()
            .map(|column| vec![column.header.to_string()]);
        let mut lines: Vec<Vec<Vec<String>>> = vec![header.collect()];
        for row in rows {
            let cells = row.into_iter().zip(&self.columns).zip(&widths);
            lines.push(
                cells
                    .map(|((cell, column), &width)| match column.fit {
                        Fit::Fixed => vec![cell],
                        Fit::Truncate { .. } => vec![truncate_str(&cell, width, "…").into_owned()],
                        Fit::Wrap { .. } => {
                            let wrapped = wrap_text(&cell, width, CodeSpans::Ignore);
                            if wrapped.is_empty() {
                                vec![cell]
                            } else {
                                wrapped
                            }
                        }
                    })
                    .collect(),
            );
        }
        // A wrapped word that is longer than its column widens the column.
        for row in &lines {
            for (width, cell) in widths.iter_mut().zip(row) {
                for line in cell {
                    *width = (*width).max(measure_text_width(line));
                }
            }
        }

        let mut out = Vec::new();
        for row in &lines {
            let height = row.iter().map(Vec::len).max().unwrap_or(1);
            for line_index in 0..height {
                let line = row
                    .iter()
                    .zip(&self.columns)
                    .zip(&widths)
                    .map(|((cell, column), &width)| {
                        let text = cell.get(line_index).map_or("", String::as_str);
                        let align = match column.align {
                            Align::Left => Alignment::Left,
                            Align::Right => Alignment::Right,
                        };
                        pad_str(text, width, align, None)
                    })
                    .collect::<Vec<_>>()
                    .join(GAP);
                out.push(line.trim_end().to_string());
            }
        }
        out.join("\n")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hegel::Generator;
    use hegel::generators;

    fn row(cells: &[&str]) -> Vec<String> {
        cells.iter().map(ToString::to_string).collect()
    }

    #[test]
    fn fixed_columns_take_their_widest_cell() {
        let out = Table::builder()
            .column(Column::new("A"))
            .column(Column::new("NUM").align(Align::Right))
            .column(Column::new("LAST"))
            .render(
                vec![row(&["long cell", "7", "x"]), row(&["b", "1234", "yy"])],
                10,
            );
        assert_eq!(
            out,
            "A           NUM  LAST\n\
             long cell     7  x\n\
             b          1234  yy"
        );
    }

    #[test]
    fn right_aligned_cells_line_up_under_a_multibyte_header() {
        let out = Table::builder()
            .column(Column::new("N (✓/✗)").align(Align::Right))
            .column(Column::new("B"))
            .render(vec![row(&["1/2", "x"])], usize::MAX);
        assert_eq!(out, "N (✓/✗)  B\n    1/2  x");
    }

    #[test]
    fn colored_cells_measure_by_visible_width() {
        let red = console::style("red").red().force_styling(true).to_string();
        let out = Table::builder()
            .column(Column::new("A"))
            .column(Column::new("B"))
            .render(vec![vec![red.clone(), "x".to_string()]], usize::MAX);
        assert_eq!(out, format!("A    B\n{red}  x"));
    }

    #[test]
    fn columns_shrink_in_priority_order_down_to_their_floor() {
        let table = Table::builder()
            .column(Column::new("ID"))
            .column(Column::new("L").fit(Fit::Truncate { floor: 8 }).priority(0))
            .column(
                Column::new("RUN")
                    .fit(Fit::Truncate { floor: 0 })
                    .priority(1),
            );
        let rows = || vec![row(&["id-1", "launcher-name", "a-long-run-name"])];

        // L shrinks first, down to its floor; ID never shrinks.
        let out = table.render(rows(), 31);
        assert_eq!(out.lines().nth(1), Some("id-1  launche…  a-long-run-name"));
        // Then RUN shrinks, but not below its header.
        let out = table.render(rows(), 22);
        assert_eq!(out.lines().nth(1), Some("id-1  launche…  a-lon…"));
        let out = table.render(rows(), 1);
        assert_eq!(out, "ID    L         RUN\nid-1  launche…  a-…");
    }

    #[test]
    fn equal_priorities_shrink_left_to_right() {
        let column = |header| Column::new(header).fit(Fit::Truncate { floor: 0 });
        let out = Table::builder()
            .column(column("A"))
            .column(column("B"))
            .render(vec![row(&["aaaaaa", "bbbbbb"])], 12);
        assert_eq!(out.lines().nth(1), Some("aaa…  bbbbbb"));
    }

    #[test]
    fn priority_beats_column_order() {
        let out = Table::builder()
            .column(Column::new("A").fit(Fit::Truncate { floor: 0 }).priority(1))
            .column(Column::new("B").fit(Fit::Truncate { floor: 0 }))
            .render(vec![row(&["aaaaaa", "bbbbbb"])], 12);
        assert_eq!(out.lines().nth(1), Some("aaaaaa  bbb…"));
    }

    fn wrap_table(floor: usize) -> Table {
        Table::builder()
            .column(Column::new("STATUS"))
            .column(Column::new("N").align(Align::Right))
            .column(Column::new("NAME").fit(Fit::Wrap { floor }))
    }

    #[test]
    fn wrap_starts_continuation_lines_under_the_column() {
        let name = "can`t lose a write, even when the leader won`t answer in time";
        let out = wrap_table(20).render(vec![row(&["failing", "12", name])], 34);
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
        let name = "a property name that does not fit";
        let out = wrap_table(20).render(vec![row(&["failing", "1", name])], 10);
        assert_eq!(
            out,
            "STATUS   N  NAME\n\
             failing  1  a property name that\n\
             \x20           does not fit"
        );
    }

    #[test]
    fn wrap_keeps_a_long_word_whole() {
        let name = "Safety ▸ my_service::partition_tolerance_holds";
        let out = wrap_table(8).render(vec![row(&["failing", "1", name])], 30);
        assert_eq!(
            out,
            "STATUS   N  NAME\n\
             failing  1  Safety ▸\n\
             \x20           my_service::partition_tolerance_holds"
        );
    }

    #[test]
    fn a_long_word_widens_its_column_for_every_row() {
        let out = Table::builder()
            .column(Column::new("NAME").fit(Fit::Wrap { floor: 0 }))
            .column(Column::new("N"))
            .render(vec![row(&["a very_long_word", "1"]), row(&["b", "2"])], 8);
        assert_eq!(
            out,
            [
                "NAME            N",
                "a               1",
                "very_long_word",
                "b               2",
            ]
            .join("\n")
        );
    }

    /// With an unlimited width, a table that can shrink is the same as one
    /// that cannot: piped output keeps every cell whole.
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
        let table = |fit| {
            Table::builder()
                .column(Column::new("A").fit(fit))
                .column(Column::new("B").align(Align::Right))
                .column(Column::new("C").fit(fit))
        };
        let fixed = table(Fit::Fixed).render(rows.clone(), usize::MAX);
        let truncated = table(Fit::Truncate { floor: 0 }).render(rows.clone(), usize::MAX);
        let wrapped = table(Fit::Wrap { floor: 0 }).render(rows, usize::MAX);
        assert_eq!(truncated, fixed);
        assert_eq!(wrapped, fixed);
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
            .map(|row| measure_text_width(&row[0]))
            .chain([1])
            .max()
            .unwrap();
        // `first + 3` is the narrowest table: column 0 whole, a gap, and
        // column 1 at its header width.
        let width = tc.draw(
            generators::integers::<usize>()
                .min_value(first + 3)
                .max_value(first + 80),
        );
        let out = Table::builder()
            .column(Column::new("A"))
            .column(Column::new("B").fit(Fit::Truncate { floor: 0 }))
            .render(rows, width);
        for line in out.lines() {
            assert!(measure_text_width(line) <= width, "{line:?} > {width}");
            assert_eq!(line.trim_end(), line);
        }
    }
}
