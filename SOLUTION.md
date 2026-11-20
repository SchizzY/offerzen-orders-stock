# Design and trade-offs

The assignment asks for an orders flow that updates stock, survives duplicate submissions and a short outage, and gives other teams a way to learn about accepted orders. This document explains how the system does that, why it is built this way, and what it does not do. [README.md](README.md) covers how to run it.

## Decisions at a glance

| Problem | Decision |
| --- | --- |
| The same `order_ref` is submitted more than once | A unique constraint on `order_ref`, `INSERT ... ON CONFLICT DO NOTHING`, and a request fingerprint that tells a replay (`200`) from a conflict (`409`) |
| Orders must be accepted while stock is unreachable | Order intake writes only to Postgres. Stock is applied later by a separate worker process. |
| The order and its event must not drift apart | A transactional outbox. The order, its lines and its event commit in one transaction. |
| Events must reach the worker in a stable order, with no gaps | A relay assigns a gapless `seq` to committed events and publishes each to a Valkey stream with the entry id `<seq>-0` |
| Stock must be applied exactly once per order | All lines reserved or none, guarded by the order's `stock_status`, with the stream entry acknowledged only after commit |
| Other teams need accepted orders (Task 2, Option A) | A pull feed, `GET /events?after=<seq>`, served from Postgres, with at-least-once delivery |
| Prices change over time | Integer cents everywhere, and each order line stores the unit price it was charged |

## Architecture

```text
  web, mobile, scripts                         feed consumer (make consumer)
          │ POST /orders                            │ GET /events?after=<seq>
          ▼                                         ▼
 ┌──────────────────────────── API process (make run) ───────────────────────────┐
 │  routers: /products  /orders  /stock  /events  /health                        │
 │  relay: a sequence loop numbers committed rows, a publish loop XADDs them     │
 └───────┬───────────────────────────────────────────────────────────┬───────────┘
         │ order + lines + outbox row in one transaction             │ XADD id "<seq>-0"
         ▼                                                           ▼
 ┌───────────────────────┐                             ┌────────────────────────────┐
 │ Postgres 17           │                             │ Valkey 8                   │
 │ source of truth       │                             │ stream "orders.accepted"   │
 │ products, orders,     │                             │ consumer group "stock"     │
 │ order_items,          │                             └──────────────┬─────────────┘
 │ outbox_events         │                                            │ XREADGROUP
 └───────────▲───────────┘                                            ▼
             │   one transaction per entry: lock the      ┌────────────────────────────┐
             └── order and its products, then reserve ────┤ stock worker (make worker) │
                 or mark insufficient                     │ XACK after commit          │
                                                          └────────────────────────────┘
```

| Process | Command | Reads | Writes |
| --- | --- | --- | --- |
| API | `make run` | all tables | `products`, `orders`, `order_items`, `outbox_events` |
| Relay, inside the API process | started by the API lifespan | `outbox_events`, the stream's last id | `outbox_events.seq`, `outbox_events.published_at`, the stream |
| Stock worker | `make worker` | the stream, `orders`, `order_items`, `products` | `products.stock`, `orders.stock_status`, stream acknowledgements |
| Feed consumer | `make consumer` | `GET /events` over HTTP | its own cursor file |

Postgres is the source of truth for everything. Valkey carries events between processes, and the system can rebuild the stream from Postgres at any time.

## Components and boundaries

The code is split into four capability packages: `catalog`, `orders`, `stock` and `events`. Each package uses these layers where it needs them:

- `router.py` handles HTTP only: paths, status codes and response models.
- `controller.py` holds the use case and owns its transaction.
- `models.py` holds the SQLAlchemy tables, and `schemas.py` holds the pydantic request and response models.

Logic with no I/O lives in plain functions. `app/orders/domain.py` computes the request fingerprint and the order total without importing FastAPI or SQLAlchemy.

The processes talk to each other through two data contracts:

- **The stream entry.** Each entry in `orders.accepted` has the id `<seq>-0` and the fields `seq`, `event_type`, `order_id`, `order_ref` and `payload`. The payload is the accepted order as JSON.
- **The feed.** `GET /events` returns events with `seq`, `type`, `order_id`, `occurred_at` and `data`, plus `next_after`. External teams depend on this HTTP contract and nothing else.

### Where the boundaries are not clean yet

All capabilities share one database and one codebase. Orders reads product prices from the `products` table through its own query. The stock worker imports the orders and catalog models, reads order lines from `order_items`, and writes `orders.stock_status`. That coupling is acceptable for one deployable. It is the first thing to change if stock becomes its own service.

### How the parts can run and evolve on their own

- **Scale the API.** Run more API processes, or `uvicorn --workers N`. Each process runs its own relay. An advisory lock lets only one relay number events at a time, and an atomic append script keeps the stream free of holes, so extra relays are safe. The relay could also move to its own process with a small entry point, because it needs only Postgres and Valkey.
- **Scale the worker.** Run more stock workers. The consumer group gives each entry to one worker. Row locks keep two workers from applying one order twice. A worker that dies leaves its unacknowledged entries behind, and the next live worker claims them after 30 seconds.
- **Add consumers.** Another internal service can read the stream with its own consumer group, without any change to orders. An external team uses the feed.
- **Split stock out.** The worker would read the order lines from the event payload instead of `order_items`. It would keep its own table of applied orders keyed by `order_id` in place of `orders.stock_status`, and its own stock table. Orders would learn the outcome from a `stock.reserved` or `stock.insufficient` event. The stream contract stays the same.

## Data model

Two Alembic migrations create the schema. `0001` creates the tables. `0002` adds `stock_status`, the outbox `seq`, and two partial indexes.

| Table | Key columns | Constraints and indexes |
| --- | --- | --- |
| `products` | `sku` (primary key), `name`, `price_cents`, `stock` | `price_cents >= 0`, `stock >= 0` |
| `orders` | `id` (uuid primary key), `order_ref`, `customer_id`, `total_cents` (bigint), `status`, `stock_status`, `request_hash`, `created_at` | `order_ref` unique. `status` in (`accepted`). `stock_status` in (`pending`, `reserved`, `insufficient`). Partial index on `id` where `stock_status = 'pending'`. |
| `order_items` | `order_id`, `sku`, `qty`, `unit_price_cents` | Primary key (`order_id`, `sku`). Foreign keys to `orders` and `products`. `qty > 0`. |
| `outbox_events` | `id` (bigserial primary key), `event_type`, `aggregate_id`, `payload` (jsonb), `created_at`, `seq`, `published_at` | `seq` unique and nullable. Partial index on `id` where `seq IS NULL`. |

- The orders table has its own uuid key. `order_ref` is a client value with a unique constraint, and the events carry both.
- `unit_price_cents` is a copy of the price at the time of the order.
- `stock >= 0` is a backstop. The worker checks stock before it decrements, so the constraint should never fire.
- The partial index on unsequenced outbox rows keeps the relay's scan small. The partial index on pending orders serves `pending_qty` in `GET /stock/{sku}`.

## Idempotent order intake

`POST /orders` runs in one transaction:

1. Pydantic validates the body. Shape errors return `422`.
2. The controller computes a fingerprint, the SHA-256 of canonical JSON that holds `customer_id` and the items sorted by SKU.
3. It reads the prices of the requested SKUs. Unknown SKUs return `422`, and nothing is written.
4. It runs `INSERT INTO orders ... ON CONFLICT (order_ref) DO NOTHING RETURNING *`.
5. If a row comes back, the order is new. The controller inserts the lines with their unit prices and one outbox row, and commits. The response is `201` with `Idempotent-Replay: false`.
6. If no row comes back, the `order_ref` exists. The controller loads the stored order. If its fingerprint matches, the response is `200` with `Idempotent-Replay: true` and the stored order. If the fingerprint differs, the response is `409`.

### Why this holds under concurrent duplicates

The unique constraint on `order_ref` is the only judge, and there is no check-then-insert window. Under Postgres's default `READ COMMITTED` isolation, a second insert with the same `order_ref` waits for the first transaction to finish. If the first commits, the second insert does nothing, and the following `SELECT` sees the committed row because each statement takes a new snapshot. If the first rolls back, the second insert succeeds.

Three things show it works:

- `test_concurrent_duplicates_count_once` sends 10 identical orders at once. It expects one `201`, nine `200`, one order row, two line rows and one outbox row.
- `make demo-duplicates` does the same over real HTTP, with a conflicting body in the race.
- Without `ON CONFLICT DO NOTHING`, six order tests fail. See [Mutation checks](#mutation-checks).

### Replay semantics

- The fingerprint covers `customer_id` and the items, not prices. A replay after a price change returns the original order with its original prices.
- Item order does not matter, because the items are sorted before hashing.
- A replay returns the order's current `stock_status`, which may have moved from `pending` to `reserved`. Everything else is the stored order.
- The client's `order_ref` is the idempotency key. A separate `Idempotency-Key` header would be a second key for the same thing.

## Prices and money

- All money is integer cents. There are no floats and no rounding.
- `price_cents` is a 32-bit integer from 0 to 2,147,483,647. An order has at most 100 lines of at most 10,000 units each, so the largest possible total is about 2.1 × 10^15. A 32-bit column would overflow, so `total_cents` is a bigint.
- Each line stores `unit_price_cents`, read in the same transaction that inserts the order. `total_cents` is the sum of `qty × unit_price_cents` over the lines. A later price change does not touch existing orders.

## Outbox, relay and the gapless seq

The order, its lines and its outbox row commit together or not at all. The request never writes to Valkey, so there is no dual write to get wrong, and `POST /orders` works while Valkey or the worker is down.

The relay is a background task in the API process. It runs two loops side by side in one `asyncio.TaskGroup`:

1. **Sequence.** Under a Postgres advisory lock, number the committed rows that have no `seq` yet. Numbering continues from the highest existing `seq`, in `id` order, up to 100 rows per round. A round that numbers rows wakes the publish loop.
2. **Publish.** Read the stream's last generated id. Select the events with a higher `seq`. Append them to the stream with the ids `<seq>-0`, then set `published_at` on them.

In each loop, a round that filled a batch starts the next round at once. Otherwise the loop waits 0.5 seconds. A failed round logs `relay sequence failed` or `relay publish failed` with the exception class, and only that loop backs off, doubling the wait up to 5 seconds. The loops are separate so that a Valkey outage never holds up sequencing, which is what `GET /events` serves. On shutdown, the API stops the relay, waits up to 1 second, then cancels it.

### Why not use `outbox_events.id` as the cursor

A bigserial id is taken at insert time, before the transaction commits. Transaction A takes id 10. Transaction B takes id 11 and commits first. A reader that sees 11 moves its cursor past 10, and when A commits, the reader never sees event 10. Measured against this schema, a reader paging by id missed 2 to 3 events out of 3,000 with 4 concurrent API workers.

`seq` is assigned only to rows that are already committed, by one sequencer at a time. When a reader sees `seq` N, every lower `seq` is already visible, and no later commit can take a lower number. `test_uncommitted_event_waits_and_takes_the_next_seq_once_committed` holds an insert open while another order commits. The open event has the lower `id`, but it gets `seq` 3 after the other order's `seq` 2.

### The advisory lock

`pg_advisory_xact_lock` makes sequencers run one at a time, across processes. It is scoped to the transaction, so Postgres releases it on commit, on rollback, or when a crashed relay's connection drops. The numbering statement runs after the lock is taken. Its snapshot therefore includes the previous sequencer's commit, and it continues from the right number.

### Explicit stream ids and the atomic append

The entry id is `<seq>-0`. Valkey rejects an `XADD` whose id is not above the stream's last id, so no `seq` can enter the stream twice, and stream order is `seq` order. The relay decides what to publish from the stream's last id, not from `published_at`. `published_at` is for people reading the table.

The batch goes in through one Lua script, `_APPEND_AT_TOP`, which the relay loads with `register_script`. Valkey runs a script atomically. The script compares the stream's last generated id with the top that the relay read before its Postgres query. If they match, it adds the whole batch with `XADD`. If they differ, it writes nothing, and the relay starts the round again at once from the stream's real end. Without the check, a stream deleted in the middle of a round could receive `seq` 11 onward with 1 to 10 missing, and the worker would never see orders 1 to 10. A second relay that publishes first also fails the check.

`WATCH` on the stream key with `MULTI` and `EXEC` looks like the obvious guard, but consumer group commands such as `XREADGROUP`, `XACK` and `XAUTOCLAIM` also count as changes to the key. With a worker running, most rounds would abort; measured, publishing ran 20 to 30 times slower. `test_relay_publishes_as_fast_while_a_consumer_group_reads_and_acks` publishes 5,000 events while another connection reads and acknowledges them. It fails if that takes more than three times as long as publishing alone, plus 1 second.

### Republish after the stream is lost

If the stream key is gone, its last id counts as 0 and the relay republishes every event from `seq` 1. The worker recreates its consumer group from the start and reads every entry again. The `stock_status` guard turns each one into a duplicate. I checked the republish path by hand on a live stack. I deleted a stream that held 1,416 events while a worker ran. The relay republished all 1,416 within 0.5 seconds. The worker logged that the group was missing, recreated it, and logged 1,416 `result=duplicate` lines within 10 seconds, and stock did not change.

## Stock worker

`python -m app.stock.worker` starts the worker. `app/stock/worker.py` is a thin entry point. Before its slow imports, it installs `SIGINT` and `SIGTERM` handlers that exit 0. It then runs the consumer in `app/stock/consumer.py`. Once the interpreter has started running the module, a stop signal exits 0, including during the slow imports and interpreter shutdown. A signal in the first few milliseconds, before Python executes any code, still ends the process by the signal; nothing is in flight then.

The consumer reads the stream through the `stock` consumer group, 50 entries at a time. Before its first read, it creates the group if the group is missing, reading from the start of the stream, and logs `stock group created` at `INFO`. Each round of its loop does three things:

1. Claim entries that another consumer left unacknowledged for 30 seconds or more (`XAUTOCLAIM`).
2. Read this worker's own unacknowledged entries again.
3. Read new entries, and wait up to 1 second if there are none.

The worker applies each entry in its own transaction, then acknowledges it with `XACK`. `apply_order_accepted` does the work:

1. Lock the order row. If the order does not exist, the result is `unknown`. If its `stock_status` is not `pending`, the result is `duplicate`.
2. Lock the order's products, sorted by SKU.
3. If every line has `stock >= qty`, decrement every line and set `reserved`. Otherwise decrement nothing and set `insufficient`.
4. Commit.

### All lines or none

An order is either filled as a whole or not at all. Partial reservations would need compensation and a backorder policy, which this exercise does not have. An order with too little stock is still accepted, because intake does not wait for stock. Its `stock_status` tells downstream systems what happened, and the worker acknowledges the entry so the queue keeps moving.

### Why FOR NO KEY UPDATE and not FOR UPDATE

Both locks use `FOR NO KEY UPDATE`. When an order is placed, each `order_items` insert runs a foreign key check that holds a `FOR KEY SHARE` lock on its product until the placement commits. `FOR UPDATE` conflicts with that lock, and `FOR NO KEY UPDATE` does not. With `FOR UPDATE`, this sequence deadlocks:

1. A placement inserts its `BAN-1` line and holds a key-share lock on `BAN-1`.
2. The worker locks `APL-1`, then waits for `BAN-1`.
3. The placement inserts its `APL-1` line and waits for the worker.

`FOR NO KEY UPDATE` is also the lock that the stock `UPDATE` takes anyway, because `stock` is not a key column. It still makes two workers wait for each other on the same product. `test_reserving_does_not_deadlock_with_an_order_being_placed_on_the_same_skus` replays that exact sequence, and it fails if the lock goes back to `FOR UPDATE`.

Locking products in SKU order means two workers always take locks in the same order, so they cannot deadlock each other.

### Deduplication by stock_status

The order row lock is where duplicates meet. A redelivered entry, a claimed entry or a republished stream can hand the same order to two workers. They wait for each other on the order row, and the second one sees a status other than `pending` and returns `duplicate`. `test_two_workers_racing_over_shared_skus_apply_every_order_once` publishes 100 orders twice, the second time in reverse order, and drains them with two workers at once. Every order is applied once, and the final stock matches the reserved orders exactly.

### Acknowledge after commit

- If the worker crashes before commit, the transaction rolls back. The entry stays unacknowledged, comes back, and is applied once.
- If the worker crashes after commit but before `XACK`, the entry comes back and the guard returns `duplicate`.
- An acknowledgement before commit could lose a decrement in a crash, so the worker never does that.

Tests cover both crash points.

### Claiming a dead worker's entries

Consumer names are `<hostname>-<pid>`, so a restarted worker has a new name. Without a claim step, the old name's unacknowledged entries would never be read again. Each round claims entries that have been idle for 30 seconds or more.

On a clean stop, the worker finishes the entries it holds and removes its own consumer name from the group if no entries are pending on it. Cleanly stopped workers therefore leave no names behind in `XINFO CONSUMERS`. A worker killed with `SIGKILL` leaves its name; its entries are claimed after 30 seconds, and the idle name is harmless. A name that still has pending entries stays, so the next worker can claim them.

### Errors

- A malformed entry is logged with its error class and acknowledged.
- An entry for an order that does not exist returns `unknown` and is acknowledged.
- A database error leaves the entry unacknowledged. The worker backs off from 0.5 up to 5 seconds and tries the same entry again.
- A consumer group lost mid-run (`NOGROUP`) logs a warning and is created again from the start of the stream, and the worker carries on.

### Reading stock

`GET /stock/{sku}` returns `on_hand`, the product's current stock, and `pending_qty`, the quantity of that SKU in orders that are still `pending`. During an outage, `pending_qty` shows the backlog that the worker will apply.

## Outage and catch-up

Postgres holds every accepted order and its event, so any process can stop and pick up where it left off.

| What is down | `POST /orders` | `GET /events` | Stock | Recovery |
| --- | --- | --- | --- | --- |
| Stock worker | `201`, `pending` | keeps serving | Entries wait in the consumer group | The restarted worker drains the backlog |
| Valkey | `201`, `pending` | keeps serving, because the sequence loop runs apart from the publish loop | Waits | The publish loop backs off up to 5 seconds and publishes when Valkey returns |
| Postgres | `500` | `500` | The worker retries | Nothing is accepted without the source of truth. `/health` returns `503`. |

`make demo-outage` is the required demonstration of the first row. It stops the worker process, places five orders that all return `201`, holds for 3 seconds with `on_hand` unchanged, then restarts the worker. In my run, stock caught up 0.6 seconds after the restart, from 94 to 79, with all eight orders `reserved`. `test_relay_keeps_sequencing_new_orders_while_valkey_is_down` and `test_relay_survives_valkey_outage_and_publishes_once_it_returns` cover the Valkey row.

The demo stops a real process rather than pausing a loop inside one. The assignment allows either. A real stop exercises the real recovery path: a new consumer name, the group's saved position, and unacknowledged entries.

## Option A: the order-accepted feed

`GET /events?after=<seq>&limit=<n>` returns the events with `seq` above `after`, in `seq` order, and `next_after`. A consumer starts at 0, handles a page, stores `next_after`, and asks again. An empty page keeps the cursor where it is.

- **Delivery is at least once.** `scripts/feed_consumer.py` prints a page, then saves the cursor with a write to a temporary file, `fsync` and a rename. A crash between print and save prints the page again. A real consumer ignores a `seq` it has already handled. A consumer that needs exactly-once processing can store the cursor in the same transaction as its own side effects.
- **The feed reads Postgres, not the stream.** Postgres is durable and complete, so a consumer can replay from `seq` 0 at any time. The stream is internal transport and can be lost, rebuilt or replaced. HTTP and JSON work from any language.
- **Pull, not webhook.** The service keeps no list of subscribers, no retry queue and no delivery state per subscriber. A slow or offline consumer costs the service nothing, and it can replay from any point. The cost is polling latency, 1 second by default in the example consumer, and a cheap indexed query per poll. A webhook sender could be built later as one more consumer of this feed.
- **Why Option A over Option B.** The context asks for clear interfaces that other teams can build on. A feed serves any number of consumers, and the outbox already existed. A daily report answers one fixed question, and a consumer of the feed could build it.
- **The feed reports acceptance, not stock.** An order that turns out `insufficient` still appears. A consumer that needs the stock outcome reads `GET /orders/{order_ref}`.

## Logging and data hygiene

- Every log line goes through loguru to one stderr sink, as text or as JSON with `LOG_JSON=true`. The stdlib loggers of uvicorn, SQLAlchemy and asyncpg are routed into loguru, so each record is printed once.
- Logs never contain `customer_id` or request bodies. Relay and worker lines carry `order_ref` and `seq`. Tests place orders with a recognizable customer id and assert that it never reaches the relay, worker or error logs.
- A Postgres error carries row values in its `DETAIL` line and its SQL text. SQLAlchemy runs with `hide_parameters=True`. A loguru patcher also reduces any record that holds a database error to its first message line, the error class and the SQLSTATE, and drops the traceback. Loguru's `diagnose` and `backtrace` are off, so tracebacks never print variable values.
- An unhandled error in a request returns `{"detail": "internal server error"}` with status `500` and logs one line.
- Validation errors return `type`, `loc` and `msg` only. They do not echo the rejected input, which can be bytes, `NaN`, a lone surrogate or deeply nested JSON that does not encode safely.
- Postgres and Valkey listen on `127.0.0.1` only.
- The feed's `data` includes `customer_id`, because a consumer of an order needs to know whose order it is. The feed has no authentication. Known limitations lists that gap.

## Testing strategy

- **Real services.** The suite runs against the Compose Postgres 17 and Valkey 8. There is no SQLite and no mock of the database or the stream, because the behaviour under test is in Postgres and Valkey: unique constraints, row locks, advisory locks, consumer groups and Lua scripts.
- **Isolation.** Each run uses its own database (`TEST_DB`) and Valkey database number (`TEST_VALKEY_DB`). Every test starts with empty tables and an empty Valkey database. The suite refuses to run against a database name that does not end in `_test`, or against Valkey database 0.
- **One step at a time.** Tests turn the background relay off and call `sequence_pending`, `publish_pending` and `consume_once` directly. That makes each interleaving reproducible. Other tests run the relay through the app's lifespan, run the worker as a real process and signal it while it starts, runs and stops, and run `scripts/outage_demo.py` end to end.
- **Concurrency.** Ten concurrent duplicate orders. Two workers racing over 100 orders on shared SKUs. A reservation during an order placement on the same SKUs. Two reservations whose query plans lock the SKUs in different orders. Two sequencers at once. Two publishers at once. A consumer group that reads and acknowledges while the relay publishes. An event that commits late.
- **Failure.** Valkey down during a relay round, and Valkey down while new orders still reach the feed. The stream deleted during a read, or between the relay's read of its top and the append. The group destroyed during a read. A worker crash before commit, and after commit but before `XACK`. A dead worker's entries claimed by its replacement. A database error during apply. A relay stuck on a hung Valkey or a held Postgres lock at shutdown.
- **Migrations.** Phase 1 rows survive the 0002 upgrade, a downgrade and a second upgrade, and `alembic check` finds no drift between the models and the schema.
- **Test-first fixes.** Each bug found in review got a failing test first, then the fix.

### Mutation checks

A test suite proves little if it still passes when the code is wrong. I broke six key mechanisms on purpose and ran the full suite against each. Each edit was made in a fresh copy of the source, so the checkout stayed clean.

| Mutation | Tests that failed |
| --- | --- |
| Lock with `FOR UPDATE` instead of `FOR NO KEY UPDATE` | `test_reserving_does_not_deadlock_with_an_order_being_placed_on_the_same_skus` |
| Remove the `stock_status` guard in the worker | Seven tests in `tests/test_stock.py`, including `test_redelivering_the_whole_stream_changes_nothing`, `test_two_workers_racing_over_shared_skus_apply_every_order_once`, and the after-commit-before-ack case of `test_an_unacked_entry_is_applied_once_when_its_worker_resumes` |
| Remove the SKU order from the product lock query | `test_reservations_whose_plans_read_skus_in_different_orders_do_not_deadlock`, in 12 of 20 runs. The race it sets up does not deadlock every time, so one run can miss this mutation. |
| Remove the advisory lock from the sequencer | `test_concurrent_sequencers_number_each_event_exactly_once`, `test_relay_publishes_what_it_sequences_while_the_publisher_waits_to_poll` and `test_shutdown_cancels_a_relay_stuck_on_a_postgres_lock` |
| Put back the original `WATCH` guard, which ended the round on a mismatch, in place of the append script | `test_relay_publishes_as_fast_while_a_consumer_group_reads_and_acks` and `test_stream_lost_between_reading_its_top_and_writing_is_republished_whole` |
| Remove `ON CONFLICT DO NOTHING` from order intake | Six tests in `tests/test_orders_api.py`, including `test_concurrent_duplicates_count_once` |

### Review loop

I built the system with AI coding agents, in small units with a written design for each phase. The assignment allows such tools. A fresh reviewer with no context from the build then reviewed each unit cold. Every finding was checked against a real run or the code before it was fixed. Findings that did not justify a change were declined, and the reason was recorded. The reviews caught real defects, for example:

- Concurrent load exhausted Postgres connections, so the connection pool is now bounded and configurable.
- Postgres error details put customer ids into logs. The fix is the logging patcher.
- 422 bodies echoed hostile input that could not encode as JSON, so they stopped echoing input.
- Values above the 32-bit range caused `500`, so input types now have explicit bounds.
- `POST /products` overwrote stock on every seed run, so stock is now set only on first insert.
- A reader keyed on `outbox_events.id` skipped late commits. The fix is the gapless `seq`.
- Consumer group reads and acknowledgements aborted most `WATCH` rounds, so the relay now appends through an atomic script.

## Trade-offs and alternatives considered

- **Idempotency lock in Valkey (`SET NX`).** Rejected. It would create a second owner of the truth. A lock that expires, or a Valkey restart, would let a duplicate through. The unique constraint lives in the same transaction as the insert and cannot disagree with it.
- **Check stock synchronously in `POST /orders`.** Rejected. The assignment says orders must be accepted while stock is unreachable. A synchronous check would also tie intake latency to stock.
- **Stock worker polls the Postgres outbox directly** (`FOR UPDATE SKIP LOCKED`, no Valkey). Polling is simpler, with one store and no relay, and a `FOR UPDATE SKIP LOCKED` poller over this schema lost nothing in testing. I chose the stream instead. Consumer groups give delivery tracking, pending lists and claiming without new tables. Another service can add its own consumer group without touching Postgres. The stream is also where stock would split off into its own database. The cost is a second store to run, the relay, and one new failure mode, described under known limitations. Postgres still holds everything needed to fall back to polling.
- **Webhook for Option A.** Not chosen for the first version. See the feed section.
- **Option B, the daily report.** Not chosen. See the feed section.
- **One process.** The assignment allows a single process with a background worker. I split the stock worker into its own process, so the outage demo stops a real process and the stock capability can be deployed and scaled on its own. The relay stays in the API process, because it belongs to the orders capability.
- **Partial reservations and backorders.** Out of scope. All lines or none keeps the state machine to three values.
- **Throughput.** The worker applies each entry in its own transaction and acknowledges it before the next one. On my laptop, one stock worker applied 80 to 110 events per second, measured on queued backlogs of 400 and 1,000 orders. More workers in the group should raise that, because the group gives each entry to one worker. After the stream is lost, the relay republishes from `seq` 1 and the worker reads every entry again as a duplicate, so that recovery takes longer as the history grows. I kept one transaction per entry, because it makes the crash cases simple.
- **`published_at` can stay empty.** If the reply from the append script is lost, or shutdown cancels the relay between the append and the update, the entries are in the stream but `published_at` stays `NULL`. The next round starts from the stream's top, so it never stamps them. Nothing reads `published_at`, so I left it as information only.
- **Migration 0002 needs a stop.** It renames `outbox_events.processed_at` to `published_at`, so code from before and after 0002 cannot share one schema. A zero-downtime path would add the new column, write both, and drop the old one later. With no deployed version to protect, the README tells you to stop the API and the worker, then migrate.

## Known limitations

- **The stream can be ahead of Postgres after a database restore.** The relay publishes only events with a `seq` above the stream's last id. A restored database numbers new events from its own, lower highest `seq`, so the relay never publishes them, and it logs nothing about it. The fix today is to delete the stream key, after which the relay republishes and the worker deduplicates. The README lists the command.
- **No stream trimming.** The stream grows by one small entry per order and is never trimmed. Trimming up to the consumer group's acknowledged position would bound it. Trimming does not change the stream's last id, so the relay would keep working.
- **No restock API.** Stock is set when a product is first created and only goes down after that. An `insufficient` order stays `insufficient` and is never checked again.
- **The 0002 downgrade discards `stock_status`.** It also drops `seq`. After an upgrade again, every order is `pending` and every event is numbered again, so stock and the stream no longer agree with the orders. The downgrade is fit for a development reset only.
- **No authentication.** The feed exposes `customer_id` to anyone who can reach the API.
- **No dead-letter handling.** A database error that repeats on every attempt keeps the worker retrying that entry with backoff. Insufficient stock and malformed entries are not in this class, because they are acknowledged.
- **Polling latency.** The relay's sequence loop polls every 0.5 seconds and the example consumer every second. `LISTEN/NOTIFY` could wake the relay instead.

## What I would do next

1. Add a restock endpoint that changes stock by a delta, and decide whether `insufficient` orders get another try.
2. Publish `stock.reserved` and `stock.insufficient` events, so consumers learn the stock outcome from the feed.
3. Make the relay detect a stream that is ahead of Postgres at startup and log an error, or put a generation number in the stream name.
4. Trim the stream up to the consumer group's acknowledged position.
5. Count deliveries per entry with `XPENDING` and move entries that keep failing to a dead-letter stream.
6. Add metrics: relay lag (highest `seq` minus the stream's last id), consumer group lag, and the number of pending orders.
7. Add authentication to the API and the feed.
8. Move stock into its own schema, with order lines read from the event payload.

## Suggested video walkthrough

A 9 to 10 minute outline. Before you record, run `make up` and `make migrate`, and open three terminals plus an editor.

| Time | Section | On screen |
| --- | --- | --- |
| 0:00 to 0:45 | The problem and the shape | The decisions table and the architecture diagram in this file |
| 0:45 to 1:30 | Start the system | `make run`, `make worker` and `make consumer` in three terminals, then `curl localhost:8000/health` |
| 1:30 to 3:00 | Demo 1: duplicates | `make demo-duplicates`. Point at the one `201` per order ref, the `200` replays and the `409`. Show the worker's `result=reserved` lines and the consumer's `seq=` lines. Run `curl` on `GET /orders/{order_ref}` to show `stock_status`. |
| 3:00 to 4:30 | Demo 2: outage and catch-up | Stop `make worker`. Run `make demo-outage` and walk through the healthy, outage and recovery rows. |
| 4:30 to 5:45 | Idempotent intake | `create_order` in `app/orders/controller.py`, `request_fingerprint` in `app/orders/domain.py`, and `test_concurrent_duplicates_count_once` |
| 5:45 to 7:15 | Outbox, relay and seq | `_ASSIGN_SEQ` and the advisory lock in `app/orders/relay.py`. Explain the late-commit gap with `test_uncommitted_event_waits_and_takes_the_next_seq_once_committed`. Then `publish_pending`, the `_APPEND_AT_TOP` script, and the two loops in `run_relay`. |
| 7:15 to 8:15 | Stock worker | `apply_order_accepted` in `app/stock/controller.py`: the `FOR NO KEY UPDATE` comment, all lines or none, and the `stock_status` guard. Then `consume_once` and `_read` in `app/stock/consumer.py`: acknowledge after commit, and `XAUTOCLAIM`. |
| 8:15 to 9:00 | The feed | `curl 'localhost:8000/events?after=0&limit=2'`. Stop the consumer, start it again, and show that it continues from its cursor. |
| 9:00 to 10:00 | Trade-offs and limitations | The trade-offs and known limitations sections of this file, then what comes next |
