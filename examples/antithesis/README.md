# Example Antithesis project: a tiny bank

A small Docker Compose project that snouty can validate and launch on
Antithesis. It shows a simple workload with properties of every kind, one of
which fails because of a deliberate bug. snouty's own end-to-end runs use it.

## The system

`bank/server.py` is an HTTP service that holds three accounts (alice, bob,
carol), each opened with 100. It serves:

- `GET /accounts`: the balances and their total.
- `POST /transfer` with `{"from", "to", "amount"}`: moves money and answers
  200 when applied, 400 when invalid, and 409 when the source account lacks
  the funds.

It saves the balances to a file after every transfer, so a container that
Antithesis kills and restarts comes back with the same balances.

## The workload

The `workload` container waits for the bank, calls `setup_complete`, and
then idles while Antithesis Test Composer runs the commands in
`test/v1/bank/`:

| Command | What it does |
| --- | --- |
| `parallel_driver_transfer.py` | Sends bursts of concurrent transfers between random accounts, about one in ten of them invalid on purpose. Antithesis runs several copies at once. |
| `anytime_audit.py` | Reads the balances at any moment, also during faults, and checks the total. |
| `eventually_bank_recovers.py` | Runs after Antithesis stops the faults, and checks that the bank answers again within two minutes. |

## Properties

| Property | Kind | Where | Expected |
| --- | --- | --- | --- |
| Account balances never go negative | `always` | server, after each debit | **fails**: the deliberate bug |
| Total money in the bank is conserved | `always` | `anytime_audit.py` | passes |
| A transfer is rejected for insufficient funds | `sometimes` | `parallel_driver_transfer.py` | passes |
| The bank applies a transfer | `reachable` | server | passes |
| The bank never accepts an invalid transfer | `unreachable` | `parallel_driver_transfer.py` | passes |
| The bank answers with every account once faults stop | `always_or_unreachable` | `eventually_bank_recovers.py` | passes |

Each image links its own Python sources into `/opt/antithesis/catalog`, so
Antithesis catalogs every assertion, and the report lists a property even
when its assertion never runs.

## The deliberate bug

`transfer` in `bank/server.py` checks the funds and debits the account in two
separate critical sections, with a slow "fraud check" between them. Two
concurrent transfers out of one account can both pass the check before either
debits, and together overdraw it. Concurrent drivers trigger it within
minutes, and "Account balances never go negative" fails with a counterexample.
Money is still conserved, because each debit and its credit happen together,
so the audit property keeps passing. To fix the bug, hold the lock from the
check through the debit.

## Validate and launch

Build the images, then validate the setup locally. Validation starts the
containers and waits for `setup_complete`:

```sh
docker compose -f examples/antithesis/docker-compose.yaml build
snouty validate examples/antithesis
```

Launch a 15-minute run. snouty pushes the config image and the two images
that the compose file names:

```sh
snouty launch --launcher basic_test --config examples/antithesis --duration 15m \
  --test-name snouty-example \
  --description "snouty example project: a tiny bank with one deliberate bug"
```

Then follow the run and read its properties:

```sh
snouty runs wait <run_id>
snouty runs properties <run_id>
```
