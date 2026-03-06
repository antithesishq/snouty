use pulldown_cmark::{Event, Options, Parser, Tag, TagEnd, TextMergeStream};
use regex::RegexBuilder;

pub const MATCH_START: &str = "«‹";
pub const MATCH_END: &str = "›»";

pub fn strip_markers(s: &str) -> String {
    s.replace(MATCH_START, "").replace(MATCH_END, "")
}

// Treat inline markdown as part of the surrounding text so snippet extraction
// doesn't insert artificial spacing around emphasis, links, or code spans.
fn is_inline(tag: &Tag) -> bool {
    matches!(
        tag,
        Tag::Emphasis
            | Tag::Strong
            | Tag::Strikethrough
            | Tag::Superscript
            | Tag::Subscript
            | Tag::Link { .. }
            | Tag::Image { .. }
    )
}

// Mirror `is_inline` for end tags so block-level transitions can still add
// separator spaces when we flatten markdown into plain text.
fn is_inline_end(tag: &TagEnd) -> bool {
    matches!(
        tag,
        TagEnd::Emphasis
            | TagEnd::Strong
            | TagEnd::Strikethrough
            | TagEnd::Superscript
            | TagEnd::Subscript
            | TagEnd::Link
            | TagEnd::Image
    )
}

// --- Step 1: Parse query ---

fn parse_query_terms(query: &str) -> Vec<String> {
    let mut terms = Vec::new();
    let mut skip_next = false;

    // Snippets only need highlightable terms, not full FTS semantics, so this
    // strips the small amount of query syntax that can appear in user input.
    let normalized = query.replace("NEAR(", " ").replace([')', '"'], " ");

    for token in normalized.split_whitespace() {
        if skip_next {
            skip_next = false;
            continue;
        }

        // Skip NOT-prefixed terms
        if token == "NOT" {
            skip_next = true;
            continue;
        }
        if token.starts_with('-') {
            continue;
        }

        // Skip operators
        if matches!(token, "AND" | "OR") {
            continue;
        }

        // Strip column prefixes like "title:"
        let token = if let Some((_prefix, rest)) = token.split_once(':') {
            rest
        } else {
            token
        };

        // Strip trailing * (prefix operator)
        let token = token.trim_end_matches('*');

        if token.is_empty() {
            continue;
        }

        // Keep only tokens that are alphanumeric/_/-
        if token
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
        {
            terms.push(token.to_lowercase());
        }
    }

    terms
}

// --- Step 2: Flatten markdown ---

fn flatten_markdown(content: &str) -> String {
    let parser = TextMergeStream::new(Parser::new_ext(content, Options::all()));

    let mut depth: usize = 0;
    let mut result = String::new();

    for event in parser {
        match event {
            // Block boundaries get a separating space so adjacent paragraphs,
            // headings, and list items don't run together in the snippet text.
            Event::Start(ref tag) if !is_inline(tag) => {
                depth += 1;
                if depth == 1 && !result.is_empty() && !result.ends_with(' ') {
                    result.push(' ');
                }
            }
            Event::End(ref tag) if !is_inline_end(tag) => {
                depth = depth.saturating_sub(1);
                if !result.is_empty() && !result.ends_with(' ') {
                    result.push(' ');
                }
            }
            Event::Text(ref text) | Event::Code(ref text) if depth > 0 => {
                result.push_str(text);
            }
            Event::SoftBreak | Event::HardBreak if depth > 0 => {
                if !result.ends_with(' ') {
                    result.push(' ');
                }
            }
            _ => {}
        }
    }

    result
}

// --- Step 3: Find all hits ---

struct Hit {
    start: usize,
    end: usize,
    term_idx: usize,
}

fn is_word_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_' || c == '-'
}

fn build_term_regex(term: &str) -> regex::Regex {
    // Regex handles Unicode-aware case-insensitive matching for us and returns
    // offsets in the original string, which keeps hit extraction simple.
    RegexBuilder::new(&regex::escape(term))
        .case_insensitive(true)
        .unicode(true)
        .build()
        .expect("query term regex should always compile")
}

fn find_hits(text: &str, terms: &[String]) -> Vec<Hit> {
    let mut hits = Vec::new();

    for (term_idx, term) in terms.iter().enumerate() {
        let re = build_term_regex(term);
        for matched in re.find_iter(text) {
            let start = matched.start();
            let end = matched.end();

            // Regex finds the term anywhere; we still enforce word-ish
            // boundaries so `rust` does not match inside `rusty`.
            let start_ok = text[..start]
                .chars()
                .next_back()
                .is_none_or(|c| !is_word_char(c));
            let end_ok = text[end..].chars().next().is_none_or(|c| !is_word_char(c));

            if start_ok && end_ok {
                hits.push(Hit {
                    start,
                    end,
                    term_idx,
                });
            }
        }
    }

    hits.sort_by_key(|h| h.start);
    hits
}

// --- Step 4: Pick best hit ---

const PROXIMITY_WINDOW: usize = 60;

fn proximity_score(hit: &Hit, hits: &[Hit]) -> usize {
    // Prefer clusters of distinct query terms so multi-word searches surface a
    // meaningful passage instead of the first isolated match.
    let mut seen = std::collections::HashSet::new();
    for other in hits {
        if other.term_idx == hit.term_idx {
            continue;
        }
        let distance = if other.start >= hit.end {
            other.start - hit.end
        } else {
            hit.start.saturating_sub(other.end)
        };
        if distance <= PROXIMITY_WINDOW {
            seen.insert(other.term_idx);
        }
    }
    seen.len()
}

fn pick_best_hit(hits: &[Hit], multi_term: bool) -> Option<usize> {
    if hits.is_empty() {
        return None;
    }

    if !multi_term {
        return Some(0); // single-term snippets can just center on the first hit
    }

    let mut best_idx = 0;
    let mut best_score = 0;

    for (i, hit) in hits.iter().enumerate() {
        let score = proximity_score(hit, hits);
        if score > best_score {
            best_score = score;
            best_idx = i;
        }
    }

    Some(best_idx)
}

// --- Step 5-6: Extract window ---

/// Snap a byte position forward to a char boundary.
fn snap_forward(text: &str, pos: usize) -> usize {
    let mut p = pos;
    while p < text.len() && !text.is_char_boundary(p) {
        p += 1;
    }
    p
}

/// Snap a byte position backward to a char boundary.
fn snap_backward(text: &str, pos: usize) -> usize {
    let mut p = pos;
    while p > 0 && !text.is_char_boundary(p) {
        p -= 1;
    }
    p
}

fn adjust_start_to_word_boundary(text: &str, pos: usize) -> usize {
    let pos = snap_forward(text, pos);
    if pos == 0 || pos >= text.len() || text.as_bytes()[pos] == b' ' {
        return pos;
    }
    // Avoid opening a snippet in the middle of a word.
    if let Some(next_space) = text[pos..].find(' ') {
        pos + next_space + 1
    } else {
        pos
    }
}

fn adjust_end_to_word_boundary(text: &str, pos: usize) -> usize {
    let pos = snap_backward(text, pos);
    if pos >= text.len() {
        return text.len();
    }
    if pos == 0 || text.as_bytes()[pos] == b' ' {
        return pos;
    }
    // Avoid ending a snippet in the middle of a word.
    if let Some(prev_space) = text[..pos].rfind(' ') {
        prev_space
    } else {
        pos
    }
}

fn extract_window(text: &str, center: usize, max_visible: usize) -> (usize, usize) {
    let half = max_visible / 2;
    let raw_start = center.saturating_sub(half);
    let raw_end = (raw_start + max_visible).min(text.len());

    // Start from a rough centered window, then nudge both sides to readable
    // boundaries so the snippet looks intentional instead of mechanically cut.
    let start = adjust_start_to_word_boundary(text, raw_start);
    let end = adjust_end_to_word_boundary(text, raw_end);

    (start, end)
}

// --- Step 7: Highlight ---

fn highlight_hits(text: &str, hits: &[Hit], multi_term: bool) -> String {
    // For multi-term searches, only highlight hits that participate in a local
    // cluster; isolated terms add noise to the excerpt.
    let kept: Vec<&Hit> = if multi_term {
        hits.iter()
            .filter(|h| proximity_score(h, hits) > 0)
            .collect()
    } else {
        hits.iter().collect()
    };

    // Insert lightweight markers first; styling happens later in `docs.rs`.
    let mut result = String::new();
    let mut pos = 0;

    for hit in &kept {
        if hit.start < pos {
            continue; // skip overlapping hits
        }
        result.push_str(&text[pos..hit.start]);
        result.push_str(MATCH_START);
        result.push_str(&text[hit.start..hit.end]);
        result.push_str(MATCH_END);
        pos = hit.end;
    }

    result.push_str(&text[pos..]);
    result
}

// --- Step 8: Entry points ---

pub fn extract_snippet(
    content: &str,
    query: &str,
    max_visible: usize,
    title_boosted: bool,
) -> String {
    // The snippet pipeline is:
    // 1. flatten markdown to readable plain text
    // 2. find term hits
    // 3. choose the best excerpt window
    // 4. mark highlighted ranges for later styling/output
    let terms = parse_query_terms(query);
    if terms.is_empty() {
        return String::new();
    }

    let plain = flatten_markdown(content);
    if plain.is_empty() {
        return String::new();
    }

    let hits = find_hits(&plain, &terms);
    let multi_term = terms.len() > 1;

    let best = pick_best_hit(&hits, multi_term);

    // Title boosts can rank a page highly even when the body lacks a strong
    // local cluster, so in that case we prefer the opening text over a weak hit.
    let use_opening = match best {
        None => true,
        Some(best_idx) => {
            if multi_term {
                let score = proximity_score(&hits[best_idx], &hits);
                if score == 0 && title_boosted {
                    true
                } else {
                    score == 0 // no proximity hits at all → opening
                }
            } else {
                false // single-term with a hit → center on it
            }
        }
    };

    let (win_start, win_end) = if use_opening {
        let end = adjust_end_to_word_boundary(&plain, max_visible.min(plain.len()));
        (0, end)
    } else {
        let best_idx = best.unwrap();
        extract_window(&plain, hits[best_idx].start, max_visible)
    };

    let window_text = &plain[win_start..win_end];

    // Recompute hits inside the final window so highlight offsets line up with
    // the text we actually emit.
    let window_hits = find_hits(window_text, &terms);

    let highlighted = highlight_hits(window_text, &window_hits, multi_term);

    // Add ellipsis
    let mut result = String::new();
    if win_start > 0 {
        result.push_str("...");
    }
    result.push_str(&highlighted);
    if win_end < plain.len() {
        result.push_str("...");
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_query_simple() {
        assert_eq!(
            parse_query_terms("rust instrumentation"),
            vec!["rust", "instrumentation"]
        );
    }

    #[test]
    fn test_parse_query_not() {
        assert_eq!(
            parse_query_terms("NOT java instrumentation"),
            vec!["instrumentation"]
        );
    }

    #[test]
    fn test_parse_query_dash_prefix() {
        assert_eq!(
            parse_query_terms("-java instrumentation"),
            vec!["instrumentation"]
        );
    }

    #[test]
    fn test_parse_query_operators() {
        assert_eq!(
            parse_query_terms("rust AND instrumentation"),
            vec!["rust", "instrumentation"]
        );
        assert_eq!(parse_query_terms("rust OR go"), vec!["rust", "go"]);
    }

    #[test]
    fn test_parse_query_prefix_star() {
        assert_eq!(parse_query_terms("setup*"), vec!["setup"]);
    }

    #[test]
    fn test_parse_query_column_prefix() {
        assert_eq!(
            parse_query_terms("title:rust content:test"),
            vec!["rust", "test"]
        );
    }

    #[test]
    fn test_parse_query_near() {
        let terms = parse_query_terms("NEAR(docker compose)");
        assert_eq!(terms, vec!["docker", "compose"]);
    }

    #[test]
    fn test_parse_query_quoted() {
        let terms = parse_query_terms("\"docker compose\"");
        assert_eq!(terms, vec!["docker", "compose"]);
    }

    #[test]
    fn test_flatten_simple_markdown() {
        let md = "# Hello\n\nSome **bold** text here.\n\n- item one\n- item two";
        let flat = flatten_markdown(md);
        assert!(flat.contains("Hello"));
        assert!(flat.contains("Some bold text here."));
        assert!(flat.contains("item one"));
    }

    #[test]
    fn test_find_hits_word_boundary() {
        let text = "the rust language is rusty";
        let hits = find_hits(text, &["rust".to_string()]);
        assert_eq!(hits.len(), 1);
        assert_eq!(&text[hits[0].start..hits[0].end], "rust");
    }

    #[test]
    fn test_find_hits_unicode_word_boundary() {
        let text = "cafeine is not a cafe and café is coffee";
        let hits = find_hits(text, &["café".to_string(), "cafe".to_string()]);
        let matches: Vec<&str> = hits.iter().map(|hit| &text[hit.start..hit.end]).collect();
        assert!(matches.contains(&"café"));
        assert_eq!(
            matches.iter().filter(|matched| **matched == "cafe").count(),
            1
        );
    }

    #[test]
    fn test_single_term_highlights_all() {
        let content = "# Setup\n\nFirst setup step. Then another setup.";
        let result = extract_snippet(content, "setup", 300, false);
        // Single term should highlight all occurrences
        assert!(result.contains(&format!("{MATCH_START}Setup{MATCH_END}")));
        assert!(result.contains(&format!("{MATCH_START}setup{MATCH_END}")));
    }

    #[test]
    fn test_multi_term_proximity_filtering() {
        // "docker" and "compose" near each other should be highlighted
        let content = "# Docker\n\nUse docker compose to run services. Some other long text about unrelated things that goes on for a while to create distance. Docker is great.";
        let result = extract_snippet(content, "docker compose", 300, false);
        // Should highlight where they co-occur
        assert!(result.contains(MATCH_START));
    }

    #[test]
    fn test_not_term_excluded() {
        let content = "# Java\n\nJava instrumentation is different from Rust instrumentation.";
        let result = extract_snippet(content, "NOT java instrumentation", 300, false);
        // "java" should NOT be highlighted
        assert!(!result.contains(&format!("{MATCH_START}Java{MATCH_END}")));
        assert!(!result.contains(&format!("{MATCH_START}java{MATCH_END}")));
        // "instrumentation" should be highlighted
        assert!(result.contains(&format!("{MATCH_START}instrumentation{MATCH_END}")));
    }

    #[test]
    fn test_title_boosted_no_content_match_uses_opening() {
        let content = "# Getting Started\n\nThis is the introduction to the documentation. It covers many topics.";
        // query terms not in content, but title_boosted
        let result = extract_snippet(content, "nonexistent", 300, true);
        // Should return opening text (no highlights but text present)
        assert!(result.contains("Getting Started"));
    }

    #[test]
    fn test_ellipsis_added() {
        let content = "# Start\n\nSome prefix text. The rust language is great. Some suffix text that goes on for a long time to exceed the window size and trigger ellipsis behavior at the end of the snippet.";
        let result = extract_snippet(content, "rust", 50, false);
        assert!(result.contains("..."));
    }

    #[test]
    fn test_strip_markers() {
        let s = format!("hello {MATCH_START}world{MATCH_END} foo");
        assert_eq!(strip_markers(&s), "hello world foo");
    }

    #[test]
    fn test_empty_content() {
        let result = extract_snippet("", "test", 300, false);
        assert!(result.is_empty());
    }

    #[test]
    fn test_no_matches_no_title_boost() {
        let content = "# Hello\n\nSome text about nothing relevant.";
        let result = extract_snippet(content, "nonexistent", 300, false);
        // No hits and no title boost → opening text, no highlights
        assert!(result.contains("Hello"));
        assert!(!result.contains(MATCH_START));
    }
}
