#!/usr/bin/env bash
# Poll one or more pull requests and print one line per new event.
# Events: issue comments, review comments, reviews, completed CI checks,
# and close/merge. The script exits when every watched PR is closed.
#
# Usage: watch-prs.sh [-i seconds] [-R owner/repo] [-u ignore-login] PR [PR ...]

set -u

interval=45
repo=""
ignore=""
while getopts "i:R:u:" opt; do
  case $opt in
    i) interval=$OPTARG ;;
    R) repo=$OPTARG ;;
    u) ignore=$OPTARG ;;
    *) exit 2 ;;
  esac
done
shift $((OPTIND - 1))
if [ $# -lt 1 ]; then
  echo "usage: watch-prs.sh [-i seconds] [-R owner/repo] [-u ignore-login] PR [PR ...]" >&2
  exit 2
fi
for pr in "$@"; do
  case $pr in
    *[!0-9]*) echo "not a PR number: $pr" >&2; exit 2 ;;
  esac
done

if [ -z "$repo" ]; then
  repo=$(gh repo view --json nameWithOwner -q .nameWithOwner) || exit 1
fi

declare -A open checks
for pr in "$@"; do
  open[$pr]=1
  checks[$pr]=""
done

# One line per event; truncate bodies so a long comment stays one line.
fmt_body='(.body // "") | gsub("\r?\n"; " ") | .[0:300]'

since=$(date -u +%Y-%m-%dT%H:%M:%SZ)

while :; do
  now=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  remaining=0
  for pr in "$@"; do
    [ "${open[$pr]}" = 1 ] || continue

    state=$(gh api "repos/$repo/pulls/$pr" \
      --jq '"\(.state) \(.merged)"' 2>/dev/null) || state=""
    case $state in
      "closed true")  echo "PR #$pr merged"; open[$pr]=0; continue ;;
      "closed false") echo "PR #$pr closed without merge"; open[$pr]=0; continue ;;
    esac
    remaining=1

    gh api "repos/$repo/issues/$pr/comments?since=$since" --jq \
      ".[] | select(.user.login != \"$ignore\")
           | \"PR #$pr comment by \(.user.login): \($fmt_body)\"" 2>/dev/null

    gh api "repos/$repo/pulls/$pr/comments?since=$since" --jq \
      ".[] | select(.user.login != \"$ignore\")
           | \"PR #$pr review comment by \(.user.login) on \(.path): \($fmt_body)\"" 2>/dev/null

    # The reviews endpoint has no ?since filter; compare timestamps instead.
    gh api "repos/$repo/pulls/$pr/reviews" --jq \
      ".[] | select((.submitted_at // \"\") > \"$since\")
           | select(.user.login != \"$ignore\")
           | \"PR #$pr review by \(.user.login): \(.state) \($fmt_body)\"" 2>/dev/null

    # gh pr checks exits non-zero on failed or pending checks; keep the output.
    out=$(gh pr checks "$pr" -R "$repo" --json name,bucket 2>/dev/null) || true
    if [ -n "$out" ]; then
      cur=$(jq -r '.[] | select(.bucket != "pending") | "\(.name): \(.bucket)"' \
        <<<"$out" | sort)
      if [ "$cur" != "${checks[$pr]}" ]; then
        comm -13 <(printf '%s\n' "${checks[$pr]}") <(printf '%s\n' "$cur") \
          | sed -e '/^$/d' -e "s/^/PR #$pr check /"
        checks[$pr]=$cur
      fi
    fi
  done
  [ $remaining = 1 ] || exit 0
  since=$now
  sleep "$interval"
done
