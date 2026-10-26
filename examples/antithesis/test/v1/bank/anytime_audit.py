#!/usr/bin/env -S python3 -m antithesis
"""Audit the bank at any moment, while transfers and faults are in flight.

A transfer moves money. It does not create or destroy money. The balances
must always add up to the opening total.
"""

import bank_client
from antithesis.assertions import always

try:
    body = bank_client.balances()
except bank_client.BankUnavailable as e:
    print(f"audit: bank unavailable: {e}")
    # Faults make the bank unreachable at times, which is no failure: this
    # audit checks only the balances that the bank answers with.
    raise SystemExit(0) from None

print(f"audit: {body}")
always(
    body["total"] == bank_client.TOTAL_MONEY,
    "Total money in the bank is conserved",
    {"accounts": body["accounts"], "total": body["total"]},
)
