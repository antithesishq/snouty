"""A tiny bank: three accounts and an HTTP API to move money between them.

GET  /health     200 once the bank is serving
GET  /accounts   {"accounts": {name: balance}, "total": sum of balances}
POST /transfer   {"from": name, "to": name, "amount": int}
                 200 applied, 400 invalid, 409 insufficient funds

The balances live in memory and in a JSON file, which is rewritten
atomically after every transfer so that a killed and restarted container
comes back with the same balances.

The bank has one deliberate bug, in `transfer`: see the comment there.
"""

import json
import os
import random
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from antithesis.assertions import always, reachable

STATE_FILE = "/var/lib/bank/state.json"
OPENING_BALANCES = {"alice": 100, "bob": 100, "carol": 100}

lock = threading.Lock()
accounts: dict[str, int] = {}


def load() -> None:
    try:
        with open(STATE_FILE) as f:
            accounts.update(json.load(f))
    except FileNotFoundError:
        accounts.update(OPENING_BALANCES)
        save()


def save() -> None:
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(accounts, f)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, STATE_FILE)


def fraud_check() -> None:
    """Stands in for a slow call to another service."""
    time.sleep(random.uniform(0.005, 0.05))


def transfer(src: str, dst: str, amount: int) -> int:
    """Move `amount` from `src` to `dst` and return the HTTP status."""
    if src not in accounts or dst not in accounts or src == dst or amount <= 0:
        return 400
    with lock:
        balance = accounts[src]
    if balance < amount:
        return 409
    # DELIBERATE BUG: the funds check above and the debit below run in two
    # separate critical sections, with a slow fraud check between them. Two
    # concurrent transfers out of one account can both pass the check before
    # either debits, and together overdraw the account. The fix is to hold
    # the lock from the check through the debit.
    fraud_check()
    with lock:
        accounts[src] -= amount
        accounts[dst] += amount
        save()
        always(
            accounts[src] >= 0,
            "Account balances never go negative",
            {"account": src, "balance": accounts[src], "amount": amount},
        )
    reachable("The bank applies a transfer", {"from": src, "to": dst, "amount": amount})
    return 200


class Handler(BaseHTTPRequestHandler):
    def reply(self, status: int, body: object) -> None:
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        if self.path == "/health":
            self.reply(200, {"ok": True})
        elif self.path == "/accounts":
            with lock:
                snapshot = dict(accounts)
            self.reply(200, {"accounts": snapshot, "total": sum(snapshot.values())})
        else:
            self.reply(404, {"error": "not found"})

    def do_POST(self) -> None:
        if self.path != "/transfer":
            self.reply(404, {"error": "not found"})
            return
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length))
            src, dst, amount = str(body["from"]), str(body["to"]), int(body["amount"])
        except (ValueError, KeyError, TypeError):
            self.reply(400, {"error": "bad request"})
            return
        status = transfer(src, dst, amount)
        self.reply(status, {"status": status})

    def log_message(self, format: str, *args: object) -> None:
        pass  # one line per request would drown the run's logs


if __name__ == "__main__":
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    load()
    print(f"bank: serving on :8080 with balances {accounts}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
