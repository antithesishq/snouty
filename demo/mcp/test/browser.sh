#!/usr/bin/env bash
# Loads the demo UI in a headless browser, checks that it renders the tools
# and the suggestions without page errors, and saves screenshots at
# projector sizes in both themes. Needs agent-browser
# (npm install -g agent-browser && agent-browser install).
#
#   test/browser.sh [url]        # default http://127.0.0.1:8017/

set -euo pipefail

url=${1:-http://127.0.0.1:8017/}
out=$(dirname "$0")/screenshots
mkdir -p "$out"
export AGENT_BROWSER_SESSION=snouty-mcp-demo-check

ab() { agent-browser "$@"; }
fail() {
  echo "FAIL: $*"
  exit 1
}

ab set viewport 1920 1080 >/dev/null
ab open "$url" >/dev/null
ab wait '[data-testid=tool-runs_list]' >/dev/null

tools=$(ab eval 'document.querySelectorAll("[data-testid^=tool-]").length')
[ "$tools" -ge 17 ] || fail "expected at least 17 tools, found $tools"
suggestions=$(ab eval 'document.querySelectorAll("[data-testid=suggestion]").length')
[ "$suggestions" -eq 4 ] || fail "expected 4 suggestions, found $suggestions"
status=$(ab eval 'document.querySelector("[data-testid=header-status]").textContent')
[[ $status == *connected* ]] || fail "MCP is not connected: $status"
echo "ok    UI renders $tools tools and $suggestions suggestions; $status"

for theme in light dark; do
  ab eval "localStorage.setItem('theme', '$theme')" >/dev/null
  ab reload >/dev/null
  ab wait "[data-testid=tool-runs_list]" >/dev/null
  for size in "1920 1080" "1440 900" "1280 720"; do
    read -r w h <<<"$size"
    ab set viewport "$w" "$h" >/dev/null
    ab wait 300 >/dev/null
    overflow=$(ab eval 'document.documentElement.scrollWidth > document.documentElement.clientWidth')
    [ "$overflow" = "false" ] || fail "horizontal overflow at ${w}x${h} ($theme)"
    ab screenshot "$out/$theme-${w}x$h.png" >/dev/null
  done
done
echo "ok    no horizontal overflow; screenshots in $out"

errors=$(ab errors 2>&1 || true)
if [ -n "$errors" ]; then
  fail "page errors: $errors"
fi
echo "ok    no page errors"
ab close >/dev/null
