#!/usr/bin/env -S python3 -m antithesis
"""Send a burst of concurrent transfers between random accounts.

Antithesis runs several copies of this driver at once, and each copy sends
its transfers from a few threads, so transfers out of one account overlap.
About one transfer in ten is invalid on purpose, to check that the bank
turns it away.
"""

import threading

import bank_client
from antithesis.assertions import sometimes, unreachable
from antithesis.random import get_random, random_choice

INVALID_TRANSFERS = [
    ("alice", "alice", 10),  # to the same account
    ("bob", "carol", 0),  # nothing to move
    ("carol", "alice", -5),  # a negative amount would pull money backwards
    ("mallory", "bob", 10),  # no such account
]


def one_transfer() -> None:
    if get_random() % 10 == 0:
        src, dst, amount = random_choice(INVALID_TRANSFERS)
    else:
        src = random_choice(bank_client.ACCOUNTS)
        dst = random_choice([a for a in bank_client.ACCOUNTS if a != src])
        amount = 1 + get_random() % 100
    try:
        status = bank_client.transfer(src, dst, amount)
    except bank_client.BankUnavailable as e:
        print(f"transfer {src}->{dst} {amount}: bank unavailable: {e}")
        return
    print(f"transfer {src}->{dst} {amount}: {status}")
    valid = src in bank_client.ACCOUNTS and dst in bank_client.ACCOUNTS
    valid = valid and src != dst and amount > 0
    if not valid and status == 200:
        unreachable(
            "The bank never accepts an invalid transfer",
            {"from": src, "to": dst, "amount": amount},
        )
    if valid:
        sometimes(
            status == 409,
            "A transfer is rejected for insufficient funds",
            {"from": src, "amount": amount},
        )


for _ in range(5):
    threads = [
        threading.Thread(target=one_transfer) for _ in range(1 + get_random() % 4)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
