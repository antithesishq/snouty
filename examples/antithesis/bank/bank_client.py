"""Shared helpers for the workload: talk to the bank over HTTP."""

import http.client
import json
import urllib.error
import urllib.request

BANK_URL = "http://bank:8080"
# Sorted: eventually_bank_recovers.py compares it with the sorted names that
# the bank answers with.
ACCOUNTS = ["alice", "bob", "carol"]
TOTAL_MONEY = 300  # the opening balances: 100 in each account


class BankUnavailable(Exception):
    """The request never got an HTTP answer: the bank is down or cut off."""


def request(
    method: str, path: str, body: object = None, timeout: float = 5
) -> tuple[int, dict]:
    """Send one request. Return the status and the decoded JSON body. Raise
    BankUnavailable when no HTTP answer arrives."""
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(BANK_URL + path, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    try:
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.status, json.load(resp)
        except urllib.error.HTTPError as e:
            return e.code, json.load(e)
    # URLError, timeouts, and resets are OSErrors; a truncated body raises
    # IncompleteRead, an HTTPException; a torn JSON body raises ValueError.
    except (OSError, http.client.HTTPException, ValueError) as e:
        raise BankUnavailable(str(e)) from e


def balances() -> dict:
    """GET /accounts: {"accounts": {...}, "total": int}."""
    status, body = request("GET", "/accounts")
    if status != 200:
        raise BankUnavailable(f"GET /accounts returned {status}")
    return body


def transfer(src: str, dst: str, amount: int) -> int:
    """POST /transfer and return the HTTP status."""
    status, _ = request("POST", "/transfer", {"from": src, "to": dst, "amount": amount})
    return status
