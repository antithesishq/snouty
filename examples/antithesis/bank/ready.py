"""Workload container entrypoint: wait for the bank, signal setup complete, idle.

Antithesis starts injecting faults and running the test commands in
/opt/antithesis/test/v1/bank only after `setup_complete`.
"""

import os
import time

import bank_client
from antithesis.lifecycle import setup_complete

while True:
    try:
        body = bank_client.balances()
        break
    except bank_client.BankUnavailable as e:
        print(f"workload: waiting for the bank: {e}", flush=True)
        time.sleep(1)

print(f"workload: the bank is up with {body}", flush=True)
setup_complete({"accounts": body["accounts"]})
os.execvp("sleep", ["sleep", "infinity"])
