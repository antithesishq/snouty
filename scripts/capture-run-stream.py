#!/usr/bin/env -S uv run
"""Capture the raw run status stream of one run, with timing, for replay.

usage: uv run scripts/capture-run-stream.py RUN_ID OUT.jsonl

Credentials come from $ANTITHESIS_BASE_URL and $ANTITHESIS_API_KEY. Start the
capture before the run starts to record the whole stream, the 404s before it
opens included. tests/fixtures/run_stream/ holds one capture.

Each output line is one JSON record; `t` is seconds since the capture began:
  {"t": 0.0,  "kind": "connect", "attempt": 1}
  {"t": 0.12, "kind": "status", "status": 404, "body": "..."}   (non-200)
  {"t": 0.13, "kind": "status", "status": 200}
  {"t": 5.31, "kind": "message", "line": "<the raw NDJSON line, unparsed>"}
  {"t": 902.1, "kind": "eof"}
The request names every notification type, as `snouty runs stream` does. A
connection that fails or ends before RUN_FINALIZED is retried after 15 s.
"""

import json
import os
import sys
import time
import urllib.error
import urllib.request

run_id, out_path = sys.argv[1], sys.argv[2]
url = f"{os.environ['ANTITHESIS_BASE_URL'].rstrip('/')}/api/v0/runs/{run_id}/stream"
body = json.dumps(
    {
        "notification_type_filter": [
            "NEW_PROPERTIES_ADDED",
            "PROPERTIES_PASSED",
            "PROPERTIES_FAILED",
            "NEW_EXAMPLES_FOUND",
        ]
    }
).encode()
start = time.monotonic()


def record(out, kind, **fields):
    out.write(
        json.dumps({"t": round(time.monotonic() - start, 3), "kind": kind, **fields})
        + "\n"
    )


with open(out_path, "a", buffering=1) as out:
    finalized = False
    attempt = 0
    while not finalized:
        attempt += 1
        record(out, "connect", attempt=attempt)
        request = urllib.request.Request(
            url,
            data=body,
            method="POST",
            headers={
                "Authorization": f"Bearer {os.environ['ANTITHESIS_API_KEY']}",
                "Content-Type": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=3600) as response:
                record(out, "status", status=response.status)
                for raw in response:
                    line = raw.decode("utf-8", "replace").rstrip("\n")
                    if not line:
                        continue
                    record(out, "message", line=line)
                    if '"RUN_FINALIZED"' in line:
                        finalized = True
            record(out, "eof")
        except urllib.error.HTTPError as err:
            record(
                out,
                "status",
                status=err.code,
                body=err.read().decode("utf-8", "replace")[:500],
            )
        except (OSError, ValueError) as err:  # a dropped connection, a timeout
            record(out, "error", error=repr(err)[:500])
        if not finalized:
            time.sleep(15)
print(f"captured {out_path}", file=sys.stderr)
