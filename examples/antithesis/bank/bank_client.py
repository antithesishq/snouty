"""Shared helpers for the workload: talk to the bank over HTTP."""

import json
import urllib.error
import urllib.request

BANK_URL = "http://bank:8080"
ACCOUNTS = ["alice", "bob", "carol"]
TOTAL_MONEY = 300  # the opening balances: 100 in each account


class BankUnavailable(Exception):
    """The request never got an HTTP answer: the bank is down or cut off."""


def request(
    method: str, path: str, body: object = None, timeout: float = 5
) -> tuple[int, dict]:
    """Send one request and return (status, decoded JSON body)."""
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(BANK_URL + path, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.load(resp)
    except urllib.error.HTTPError as e:
        return e.code, json.load(e)
    except (OSError, ValueError) as e:  # URLError, timeouts, resets, torn bodies
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
