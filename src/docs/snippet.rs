use pulldown_cmark::{Event, Options, Parser, Tag, TagEnd, TextMergeStream};

pub const MATCH_START: &str = "«‹";
pub const MATCH_END: &str = "›»";

pub fn strip_markers(s: &str) -> String {
    s.replace(MATCH_START, "").replace(MATCH_END, "")
}

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

    // Handle NEAR(...) and quoted phrases by extracting inner tokens
    let normalized = query
        .replace("NEAR(", " ")
        .replace(')', " ")
        .replace('"', " ");

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
            Event::Start(ref tag) if !is_inline(tag) => {
                depth += 1;
                if depth == 1 && !result.is_empty() && !result.ends_with(' ') {
                    result.push(' ');
                }
            }
            Event::End(ref tag) if !is_inline_end(tag) => {
                depth = depth.saturating_sub(1);
                if depth == 0 && !result.is_empty() && !result.ends_with(' ') {
                    result.push(' ');
                } else if depth > 0 && !result.is_empty() && !result.ends_with(' ') {
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

fn find_hits(text: &str, terms: &[String]) -> Vec<Hit> {
    let lower = text.to_lowercase();
    let mut hits = Vec::new();

    for (term_idx, term) in terms.iter().enumerate() {
        let mut search_from = 0;
        while let Some(pos) = lower[search_from..].find(term.as_str()) {
            let start = search_from + pos;
            let end = start + term.len();

            // Word boundary: byte before start (if any) and byte at end (if any)
            // must not be alphanumeric
            let start_ok = start == 0 || !text.as_bytes()[start - 1].is_ascii_alphanumeric();
            let end_ok = end >= text.len() || !text.as_bytes()[end].is_ascii_alphanumeric();

            if start_ok && end_ok {
                hits.push(Hit {
                    start,
                    end,
                    term_idx,
                });
            }

            search_from = start + 1;
        }
    }

    hits.sort_by_key(|h| h.start);
    hits
}

// --- Step 4: Pick best hit ---

const PROXIMITY_WINDOW: usize = 60;

fn proximity_score(hit: &Hit, hits: &[Hit]) -> usize {
    let mut seen = std::collections::HashSet::new();
    for other in hits {
        if other.term_idx == hit.term_idx {
            continue;
        }
        let distance = if other.start >= hit.end {
            other.start - hit.end
        } else if hit.start >= other.end {
            hit.start - other.end
        } else {
            0 // overlapping
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
        return Some(0); // first hit for single-term
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

fn adjust_start_to_word_boundary(text: &str, pos: usize) -> usize {
    if pos == 0 || text.as_bytes().get(pos).map_or(true, |b| *b == b' ') {
        return pos;
    }
    // Find next space forward from pos
    if let Some(next_space) = text[pos..].find(' ') {
        pos + next_space + 1
    } else {
        pos
    }
}

fn adjust_end_to_word_boundary(text: &str, pos: usize) -> usize {
    if pos >= text.len() {
        return text.len();
    }
    if text.as_bytes().get(pos).map_or(true, |b| *b == b' ') {
        return pos;
    }
    // Find previous space backward from pos
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

    let start = adjust_start_to_word_boundary(text, raw_start);
    let end = adjust_end_to_word_boundary(text, raw_end);

    (start, end)
}

// --- Step 7: Highlight ---

fn highlight_hits(text: &str, hits: &[Hit], multi_term: bool) -> String {
    // Filter hits based on proximity for multi-term queries
    let kept: Vec<&Hit> = if multi_term {
        hits.iter()
            .filter(|h| proximity_score(h, hits) > 0)
            .collect()
    } else {
        hits.iter().collect()
    };

    // Build result string with markers inserted
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

    // Decide fragment strategy
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

    // Find hits within the window
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
