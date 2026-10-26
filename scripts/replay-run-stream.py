#!/usr/bin/env -S uv run
"""Replay a capture from capture-run-stream.py to snouty, with its timing.

usage: uv run scripts/replay-run-stream.py CAPTURE.jsonl RUN_ID [--port 8931] [--speed 10]
       [--startup] [--from SECONDS]

GET /api/v0/runs/<id> answers that the run is ready. POST
/api/v0/runs/<id>/stream replays the messages of the last 200 connection in
the capture, sleeping between them for the captured gap divided by --speed
(0 sends everything at once). Point snouty at it:

  ANTITHESIS_BASE_URL=http://127.0.0.1:8931 snouty runs stream RUN_ID
"""

import argparse
import json
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

parser = argparse.ArgumentParser()
parser.add_argument("capture")
parser.add_argument("run_id")
parser.add_argument("--port", type=int, default=8931)
parser.add_argument("--speed", type=float, default=10.0)
parser.add_argument(
    "--from",
    dest="start",
    type=float,
    default=0.0,
    help="join the capture at this second: send only the messages after it",
)
parser.add_argument(
    "--startup",
    action="store_true",
    help="answer the stream with 404 until the captured time of the first 200, scaled by --speed",
)
args = parser.parse_args()

with open(args.capture) as capture:
    records = [json.loads(line) for line in capture]
# The messages after the last 200 status: the connection that ran to the end.
last_ok = max(
    i for i, r in enumerate(records) if r["kind"] == "status" and r["status"] == 200
)
messages = [
    r for r in records[last_ok:] if r["kind"] == "message" and r["t"] >= args.start
]
opens_at = (
    records[last_ok]["t"] / args.speed if args.startup and args.speed > 0 else 0.0
)
started = time.monotonic()


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps(
            {
                "run_id": args.run_id,
                "status": "in_progress",
                "sub_status": "Ready",
                "created_at": "2026-10-25T00:00:00Z",
                "launcher": "replay",
            }
        ).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        request = json.loads(
            self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}"
        )
        ids = request.get("property_filter")
        if time.monotonic() - started < opens_at:
            body = b'{"message":"Resource not found"}'
            self.send_response(404)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/x-ndjson")
        self.end_headers()
        previous = max(records[last_ok]["t"], args.start)
        for message in messages:
            if args.speed > 0:
                time.sleep(max(0.0, message["t"] - previous) / args.speed)
            previous = message["t"]
            line = message["line"]
            if ids is not None:
                notification = json.loads(line)
                if (
                    "data" in notification
                    and notification.get("type") != "RUN_FINALIZED"
                ):
                    notification["data"] = [
                        p for p in notification["data"] if p.get("id") in ids
                    ]
                    if not notification["data"]:
                        continue
                line = json.dumps(notification)
            self.wfile.write((line + "\n").encode())
            self.wfile.flush()

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        pass


ThreadingHTTPServer(("127.0.0.1", args.port), Handler).serve_forever()
