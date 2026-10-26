#!/usr/bin/env -S python3 -m antithesis
"""Audit the bank at any moment, while transfers and faults are in flight.

A transfer moves money; it never creates or destroys it. Whatever else goes
wrong, the balances must add up to the money the bank opened with.
"""

import bank_client
from antithesis.assertions import always

try:
    body = bank_client.balances()
except bank_client.BankUnavailable as e:
    print(f"audit: bank unavailable: {e}")
    raise SystemExit(0) from None

print(f"audit: {body}")
always(
    body["total"] == bank_client.TOTAL_MONEY,
    "Total money in the bank is conserved",
    {"accounts": body["accounts"], "total": body["total"]},
)
