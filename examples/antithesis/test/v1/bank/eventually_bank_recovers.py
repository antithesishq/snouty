#!/usr/bin/env -S python3 -m antithesis
"""Check that the bank comes back once faults stop.

Antithesis stops injecting faults before it runs an `eventually_` command,
so a bank that was partitioned, paused, or restarted should answer again
within a short grace period, with every account still present.
"""

import time

import bank_client
from antithesis.assertions import always_or_unreachable

GRACE_SECONDS = 120

deadline = time.monotonic() + GRACE_SECONDS
body = None
while body is None and time.monotonic() < deadline:
    try:
        body = bank_client.balances()
    except bank_client.BankUnavailable as e:
        print(f"recovery: bank unavailable: {e}")
        time.sleep(2)

print(f"recovery: {body}")
always_or_unreachable(
    body is not None and sorted(body["accounts"]) == bank_client.ACCOUNTS,
    "The bank answers with every account once faults stop",
    {"answered": body is not None, "body": body},
)
