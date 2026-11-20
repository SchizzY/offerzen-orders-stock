# Orders and stock

A small orders and stock system in Python and PostgreSQL. A FastAPI service accepts orders, a separate stock worker applies them to stock through a Valkey stream, and other systems follow accepted orders through a pull feed.

This file tells you how to run it. [SOLUTION.md](SOLUTION.md) explains the design and the trade-offs.

## Prerequisites

- Python 3.12
- Poetry 2.2 or later
- Docker with Compose v2 (`docker compose`)
- `curl`, for the API examples

The Compose project is named `mrd-orders`. It publishes Postgres 17 on `127.0.0.1:55432` and Valkey 8 on `127.0.0.1:56379`, so it does not clash with a local Postgres on 5432 or a local Redis or Valkey on 6379.

## Quick start

Run every command from the repository root. You need three terminals.

1. Install the dependencies into `.venv`:

	```sh
	make install
	```

	If Poetry picks another Python version, run `poetry env use python3.12` first.

2. Start Postgres and Valkey. The command returns when both are healthy.

	```sh
	make up
	```

3. Create the schema:

	```sh
	make migrate
	```

4. Start the API on http://localhost:8000. The same process runs the outbox relay.

	```sh
	make run
	```

5. In a second terminal, start the stock worker:

	```sh
	make worker
	```

	On its first start the worker also logs `stock group created`.

6. In a third terminal, start the feed consumer:

	```sh
	make consumer
	```

7. Check that the API can reach both stores:

	```sh
	curl localhost:8000/health
	```

	You should see `{"postgres":"ok","valkey":"ok"}`.

You do not need a `.env` file. The defaults match the Compose ports. To change a setting, copy `.env.example` to `.env` and edit it, or export the variable. See [Configuration](#configuration).

The interactive API docs are at http://localhost:8000/docs.

Stop each process with Ctrl+C. The worker and the consumer also exit 0 on `SIGTERM`; the API shuts down cleanly on `SIGTERM` and then exits 143, as uvicorn re-raises the signal. `make down` stops the containers and keeps the data. `make reset` also deletes the volumes, and with them every database in that Postgres.

### Make targets

| Target | What it runs |
| --- | --- |
| `make install` | `poetry install` |
| `make up` | Start Postgres and Valkey and wait until healthy |
| `make down` | Stop the containers, keep the data |
| `make reset` | Stop the containers and delete their volumes |
| `make migrate` | `alembic upgrade head` |
| `make run` | API and relay with `uvicorn --reload` on port 8000 |
| `make worker` | Stock worker, `python -m app.stock.worker` |
| `make consumer` | Feed consumer, `scripts/feed_consumer.py` |
| `make demo-duplicates` | Demo 1, `scripts/seed_and_burst.py` (`make seed` is the same script) |
| `make demo-outage` | Demo 2, `scripts/outage_demo.py` |
| `make test` | `scripts/test.sh` |
| `make lint` | `ruff check` and `ruff format --check` |
| `make fmt` | `ruff format` and `ruff check --fix` |

## Demo 1: duplicate submissions

The API must be running. Start the stock worker too if you want the orders to move from `pending` to `reserved`.

```sh
make demo-duplicates
```

The script seeds four products (`BAN-001`, `MLK-002`, `APL-003`, `BRD-004`) through `POST /products`. It then sends 12 order submissions at the same time. They cover six order refs:

- `ORD-001` is sent four times with the same body and once with a different body.
- `ORD-002` is sent three times with the same body.
- `ORD-003` to `ORD-006` are sent once each.

Each run prefixes the refs with a timestamp and a random suffix, so every run creates new orders. The first table lists every response. The second table checks each order ref:

```text
order_ref                          sent 201 200 409 total_cents  verdict
RUN20261120-022236-31e0d1-ORD-001     5   1   3   1         747  ok
RUN20261120-022236-31e0d1-ORD-002     3   1   2   0         837  ok
RUN20261120-022236-31e0d1-ORD-003     1   1   0   0         388  ok
RUN20261120-022236-31e0d1-ORD-004     1   1   0   0         698  ok
RUN20261120-022236-31e0d1-ORD-005     1   1   0   0         657  ok
RUN20261120-022236-31e0d1-ORD-006     1   1   0   0         995  ok

PASS
```

What to look for:

- Each order ref has exactly one `201`. Every identical resubmission gets `200` with `Idempotent-Replay: true`.
- The submission with a different body gets `409`. The submissions race, so sometimes the different body is stored first. Then it gets the `201` and the four identical copies get `409`. The verdict accepts both outcomes.
- `total_cents` comes from `GET /orders/{order_ref}` after the burst, so it shows what was stored.

The script exits 0 on `PASS`, 1 if any order ref was created twice or any response broke the contract, and 2 if a request fails, for example because the API is not running.

To show replays across runs, use fixed refs. The first run creates the orders. Every later run gets only `200` and `409`.

```sh
poetry run python scripts/seed_and_burst.py --fixed-refs
```

Each run draws stock down. Seeding again does not restock, because `POST /products` keeps the stock of an existing SKU. After several runs, orders start to come back `insufficient`.

## Demo 2: outage and catch-up

The API must be running. Stop `make worker` first. The script starts and stops its own stock worker, and another live worker would hide the outage.

```sh
make demo-outage
```

The script creates a new SKU with stock 100 and runs four phases. Each row shows the time since start and what `GET /stock/{sku}` returned:

```text
worker log: /var/folders/5t/84h9v5k91mj5bck13y34h6hr0000gn/T/outage-demo-OUT-5A32AB-au0wmrkg.log

phase        t+s on_hand pending  note
seed         0.0     100       0  seeded OUT-5A32AB
healthy      0.7     100       0  worker started
healthy      0.8     100       6  placed 3 x qty 2
healthy      1.0      94       0  drained
healthy      1.0      94       0  3 orders reserved
outage       2.1      94       0  worker stopped (SIGTERM, exit 0)
outage       2.2      94      15  placed 5 x qty 3, all 201
outage       2.2      94      15  5 orders pending
outage       5.2      94      15  after a 3s hold
recovery     5.8      79       0  worker restarted
recovery     5.8      79       0  caught up 0.6s after restart
recovery     5.8      79       0  8 orders reserved

PASS
```

What to look for:

- **Healthy.** Three orders of 2 units each take stock from 100 to 94.
- **Outage.** The worker process is stopped. Five more orders still return `201`. `on_hand` stays at 94 and `pending_qty` shows the 15 units that wait in the stream.
- **Recovery.** The restarted worker drains the backlog. `on_hand` reaches 79 and all eight orders are `reserved`. The eight orders took 21 units in total.

The script exits 0 on `PASS`, 1 if an expectation failed, 2 if it cannot run, and 128 + N if signal N stopped it. It stops its worker on every exit, including Ctrl+C, `SIGTERM` and `SIGHUP`. It cannot run when the API or Valkey is unreachable, or when another stock worker is active. A worker stopped a few seconds ago still looks active, so the script prints `waiting for recently active consumers to go idle` and waits up to about 11 seconds. If a worker is still active then, it exits with `a stock worker was active in the last 10 s (<name>); stop it and re-run, or pass --force`. `--force` skips that check.

`--timeout` (default 30 seconds, a positive, finite number) limits how long the worker may take to register. It also limits how long catch-up may go without progress, not the total wait. Progress means a change in the SKU's `pending_qty`, or in the stock group's last delivered id or pending count, so a worker that is still working through a large backlog does not fail. On a stall, the failure names the backlog in the group and suggests a larger `--timeout`. The worker's own log goes to a new file for each run, `outage-demo-<sku>-<random>.log` in the system temp directory, and the script prints its path before the table.

## API reference

The base URL is `http://localhost:8000`. Requests and responses are JSON. There is no authentication.

### Input rules

| Field | Rule |
| --- | --- |
| `sku`, `order_ref`, `customer_id` | 1 to 64 characters. Starts with a letter or digit, then letters, digits, `.`, `_`, `:` or `-`. Surrounding whitespace is stripped. |
| `name` | 1 to 200 characters, no NUL character |
| `price_cents`, `stock` | JSON integer from 0 to 2147483647 |
| `qty` | JSON integer from 1 to 10000 |
| `items` | 1 to 100 lines, each SKU at most once |

Integer fields reject strings, decimals such as `1.0`, and booleans. A path parameter that breaks these rules returns `422`.

The examples below use the products that `make demo-duplicates` seeds. Values such as `order_id`, `created_at` and stock levels will differ on your machine.

### Error responses

| Status | Body |
| --- | --- |
| `404` | `{"detail": "product 'NOPE' not found"}` |
| `409` | `{"detail": {"message": "...", "order_ref": "..."}}` |
| `422` | `{"detail": [{"type": "...", "loc": [...], "msg": "..."}]}`. Validation errors do not echo the rejected input, except an unknown or duplicate SKU, which is named. |
| `500` | `{"detail": "internal server error"}` |

### GET /health

Checks Postgres and Valkey, each with a 2 second timeout.

```sh
curl localhost:8000/health
```

Returns `200 {"postgres":"ok","valkey":"ok"}`. If either check fails it returns `503` and names the failed store, for example `{"postgres":"ok","valkey":"unavailable"}`.

### POST /products

Creates a product, or updates the name and price of an existing SKU. Stock is set only when the SKU is first created. A later `POST` for the same SKU keeps the current stock, so seeding again never erases what the worker reserved. There is no endpoint that changes stock.

```sh
curl -X POST localhost:8000/products \
	-H 'content-type: application/json' \
	-d '{"sku":"BAN-001","name":"Bananas 1kg","price_cents":199,"stock":50}'
```

Returns `200` with the stored product:

```json
{"sku": "BAN-001", "name": "Bananas 1kg", "price_cents": 199, "stock": 50}
```

### GET /products

Lists every product, ordered by SKU.

```sh
curl localhost:8000/products
```

Returns `200` with a list of products in the shape above.

### GET /products/{sku}

```sh
curl localhost:8000/products/BAN-001
```

Returns `200` with one product, or `404`.

### POST /orders

Accepts an order. The response does not wait for stock. A new order has `stock_status` `pending` until the stock worker applies it.

```sh
curl -i -X POST localhost:8000/orders \
	-H 'content-type: application/json' \
	-d '{"order_ref":"web-100045","customer_id":"cust-42","items":[{"sku":"BAN-001","qty":2},{"sku":"APL-003","qty":1}]}'
```

The checks run in this order:

1. The body is validated. Errors return `422`.
2. Every SKU must exist. Unknown SKUs return `422` with one entry per unknown SKU, and nothing is stored.
3. The `order_ref` decides the rest:

| Case | Status | `Idempotent-Replay` | Body |
| --- | --- | --- | --- |
| New `order_ref` | `201` | `false` | The new order |
| Known `order_ref`, same `customer_id` and items | `200` | `true` | The stored order, unchanged except for its current `stock_status` |
| Known `order_ref`, different `customer_id` or items | `409` | none | `{"detail": {"message": "order_ref was already used for a different order", "order_ref": "web-100045"}}` |

Item order does not matter when two submissions are compared. A replay returns the prices stored with the original order, even if a product price changed since.

The order body:

```json
{
	"order_ref": "web-100045",
	"customer_id": "cust-42",
	"status": "accepted",
	"stock_status": "pending",
	"total_cents": 677,
	"items": [
		{"sku": "APL-003", "qty": 1, "unit_price_cents": 279, "line_total_cents": 279},
		{"sku": "BAN-001", "qty": 2, "unit_price_cents": 199, "line_total_cents": 398}
	],
	"created_at": "2026-11-20T00:22:56.136339Z"
}
```

- `unit_price_cents` is the product price when the order was accepted.
- `total_cents` is the sum of `line_total_cents`.
- Items are sorted by SKU.
- `status` is always `accepted`.

An unknown SKU returns:

```json
{"detail": [{"type": "unknown_sku", "loc": ["body", "items", 1, "sku"], "msg": "Unknown sku", "input": "ZZZ-999"}]}
```

### GET /orders/{order_ref}

```sh
curl localhost:8000/orders/web-100045
```

Returns `200` with the order body shown above, or `404`. `stock_status` is one of these values:

| `stock_status` | Meaning |
| --- | --- |
| `pending` | Accepted. The stock worker has not applied it yet. |
| `reserved` | Every line had enough stock. Stock was reduced for every line. |
| `insufficient` | At least one line did not have enough stock. Stock was not reduced for any line. |

### GET /stock/{sku}

```sh
curl localhost:8000/stock/BAN-001
```

Returns `200`, or `404` for an unknown SKU:

```json
{"sku": "BAN-001", "on_hand": 40, "pending_qty": 0}
```

- `on_hand` is the current stock.
- `pending_qty` is the quantity of this SKU in orders that are still `pending`. During a worker outage it shows the backlog. It also counts orders that will turn out `insufficient`.

### GET /events

The order-accepted feed for other systems. It returns accepted orders in `seq` order.

| Parameter | Default | Range | Meaning |
| --- | --- | --- | --- |
| `after` | `0` | 0 to 2^63 - 1 | Return events with `seq` greater than this |
| `limit` | `100` | 1 to 500 | Maximum events in one page |

```sh
curl 'localhost:8000/events?after=0&limit=2'
```

Returns `200`:

```json
{
	"events": [
		{
			"seq": 1,
			"type": "order.accepted",
			"order_id": "ce4b9c16-78ce-44ca-bd7a-1b6c2fcee6c9",
			"occurred_at": "2026-11-20T00:22:56.136339Z",
			"data": {
				"items": [
					{"qty": 1, "sku": "APL-003", "line_total_cents": 279, "unit_price_cents": 279},
					{"qty": 2, "sku": "BAN-001", "line_total_cents": 398, "unit_price_cents": 199}
				],
				"status": "accepted",
				"order_ref": "web-100045",
				"created_at": "2026-11-20T00:22:56.136339Z",
				"customer_id": "cust-42",
				"total_cents": 677
			}
		}
	],
	"next_after": 1
}
```

- `data` is the order as it was accepted. It has no `stock_status`. An order that later turns out `insufficient` is still in the feed.
- `next_after` is the last `seq` in the page. If the page is empty it equals `after`.
- To follow the feed, start with `after=0`, handle the page, store `next_after`, and ask again with `after=<next_after>`.
- Delivery is at least once. A consumer that crashes before it stores its cursor sees the page again, so it should ignore a `seq` it has already handled.

## Feed consumer

`scripts/feed_consumer.py` is a small example consumer. It prints one line per event and saves the cursor to a file after each page:

```text
seq=1 order=web-100045 total_cents=677 items=APL-003x1,BAN-001x2
```

Stop it and start it again, and it continues after the saved cursor.

| Option | Default | Meaning |
| --- | --- | --- |
| `--base-url` | `$BASE_URL` or `http://localhost:8000` | API to poll |
| `--cursor-file` | `.feed_cursor` | Where the last handled `seq` is saved |
| `--from-start` | off | Ignore the saved cursor and start at `seq` 0 |
| `--once` | off | Read every page after the cursor, then exit |
| `--interval` | `1.0` | Seconds to wait between polls when there are no new events |

Pass options through Poetry, for example `poetry run python scripts/feed_consumer.py --from-start --once`.

- Before the first poll, the consumer checks that it can write the cursor file. If it cannot, or the file does not hold a number, it exits 2.
- A network error, a `5xx`, a `429` or a `200` that is not the feed is retried. The consumer prints the reason on stderr, for example `feed unavailable: HTTP 503; retrying in 2s`, and backs off up to 30 seconds. With `--once` it exits 1 instead.
- Any other `4xx` exits 1, because a retry cannot fix it. A `422` means the feed rejected the saved cursor.
- If stdout closes, for example in `| head`, the consumer exits 141 without an error message.
- Give each consumer its own `--cursor-file`. Two consumers that share one file overwrite each other's cursor.
- After `make reset` or a switch to a new database, the saved cursor can be ahead of the new feed. The consumer then skips every event up to the old cursor. Delete `.feed_cursor` or pass `--from-start`.

## Configuration

Settings come from environment variables and from a `.env` file in the current directory. An environment variable wins over `.env`.

| Variable | Default | Meaning |
| --- | --- | --- |
| `DATABASE_URL` | `postgresql+asyncpg://orders:orders@localhost:55432/orders` | Postgres connection |
| `VALKEY_URL` | `redis://localhost:56379/0` | Valkey connection. The path is the database number. |
| `DB_POOL_SIZE` | `5` | Connections kept open per process, at least 1 |
| `DB_MAX_OVERFLOW` | `0` | Extra connections per process above the pool size |
| `DB_CONNECT_TIMEOUT_SECONDS` | `5` | Postgres connect timeout |
| `DB_COMMAND_TIMEOUT_SECONDS` | `10` | Postgres statement timeout on the client side |
| `ORDERS_STREAM` | `orders.accepted` | Valkey stream name |
| `STOCK_GROUP` | `stock` | Consumer group of the stock worker |
| `RELAY_ENABLED` | `true` | Run the relay inside the API process |
| `RELAY_BATCH_SIZE` | `100` | Events per relay step, 1 to 1000 |
| `POLL_INTERVAL_SECONDS` | `0.5` | Relay wait after a round that did not fill a batch |
| `LOG_LEVEL` | `INFO` | `TRACE`, `DEBUG`, `INFO`, `SUCCESS`, `WARNING`, `ERROR` or `CRITICAL`, any case. `WARN` and `FATAL` also work. |
| `LOG_JSON` | `false` | Write one JSON object per log line |

The scripts and tools read a few more variables:

| Variable | Used by | Meaning |
| --- | --- | --- |
| `BASE_URL` | the three scripts | API base URL, default `http://localhost:8000` |
| `UVICORN_PORT` | `make run` | API port, default 8000 |
| `TEST_DB` | `make test` | Test database, default `orders_test` |
| `TEST_VALKEY_DB` | `make test` | Valkey database number for tests, 1 to 15, default 15 |

**Pool sizing.** Every API process and every stock worker has its own pool of up to `DB_POOL_SIZE + DB_MAX_OVERFLOW` connections. The relay shares the API's pool, and the outage demo starts one extra worker. Keep the total over all processes below Postgres `max_connections`, which is 100 in the Compose image. Zero would mean an unbounded pool, so `DB_POOL_SIZE` must be at least 1.

### Run a second copy beside the default one

Point every process at its own database and Valkey database number. This example uses a database named `scratch`, Valkey database 9, and port 8290:

```sh
docker compose exec -T postgres createdb -U orders scratch
export DATABASE_URL=postgresql+asyncpg://orders:orders@localhost:55432/scratch
export VALKEY_URL=redis://localhost:56379/9
export UVICORN_PORT=8290 BASE_URL=http://localhost:8290
make migrate
make run
```

Export the same variables in the terminals for `make worker`, `make consumer` and the demos.

## Tests

```sh
make test
```

`scripts/test.sh` starts the containers, creates the test database if it is missing, migrates it, and runs pytest. The tests use the real Postgres and Valkey from Compose. Nothing is mocked. The script sets `RELAY_ENABLED=false`, so the tests drive the relay one step at a time.

Pass pytest arguments to the script directly:

```sh
./scripts/test.sh -k relay
```

- Before each test, the suite empties the tables and flushes its Valkey database. It refuses to run against a database name that does not end in `_test`, or against Valkey database 0.
- `TEST_DB` must be lowercase letters, digits or `_`, and end in `_test`. The migration test also creates and drops a database named after it, for example `orders_migrations_test`.
- Two test runs at the same time need a different `TEST_DB` and a different `TEST_VALKEY_DB` each. Otherwise one run deletes the other's data:

	```sh
	TEST_DB=mine_test TEST_VALKEY_DB=9 make test
	```

## Lint

```sh
make lint
```

`make lint` runs `ruff check` and `ruff format --check`. `make fmt` rewrites the files.

## Project layout

Each capability is a package. `router.py` handles HTTP only. `controller.py` holds the use case and its transaction. `models.py` holds the SQLAlchemy tables and `schemas.py` the pydantic request and response models.

```text
app/
  main.py            app factory, lifespan that starts the relay, error handling
  config.py          settings
  db.py              async engine and session
  valkey.py          Valkey client
  logging.py         loguru setup and scrubbing of database errors
  health.py          GET /health
  types.py           shared input types and bounds
  catalog/           products: router, controller, models, schemas
  orders/            order intake: router, controller, models, schemas
    domain.py        request fingerprint and totals, no framework imports
    relay.py         outbox sequencer and stream publisher
  stock/             stock: router, controller, schemas
    consumer.py      stream consumer loop of the stock worker
    worker.py        stock worker entry point, exits 0 on a stop signal
  events/            order-accepted feed: router, controller, schemas
migrations/versions/ 0001 catalog and orders, 0002 stock status and outbox seq
scripts/
  seed_and_burst.py  demo 1
  outage_demo.py     demo 2
  feed_consumer.py   example feed consumer
  test.sh            test runner
tests/               pytest suite
```

## Operational notes

- **Stop the API and the worker before you migrate an existing database.** Migration 0002 renames `outbox_events.processed_at` to `published_at` and adds `orders.stock_status`. Code from before 0002 fails against the new schema, and the current code fails against the old one. Stop both processes, run `make migrate`, then start them again. Orders that existed before 0002 start as `pending`, and the relay and worker process them once.
- **Do not use the 0002 downgrade as a rollback.** It drops `stock_status` and `seq`. After an upgrade again, every order is `pending` and every event gets a new `seq`, so stock and the stream no longer agree with the orders. Use it only on a development database you will reset.
- **`LOG_JSON=true` with `make run`.** `make run` uses `--reload`. The reloader's parent process prints a few plain-text lines before and after the server process, for example `Uvicorn running on ...` and `Stopping reloader process`. Every line from the server process is JSON. Run `poetry run uvicorn app.main:app` without `--reload` if you need JSON only.
- **After you restore Postgres from a backup, delete the stream.** The relay publishes only events with a `seq` above the stream's last entry. A restored database numbers its new events from its own highest `seq`, which is below the stream's last entry, so the relay would never publish them. Run `docker compose exec valkey valkey-cli DEL orders.accepted`. The relay then republishes every event, and the worker skips every order that the restored database already shows as `reserved` or `insufficient`. If you use a non-default `VALKEY_URL`, add `-n <database number>` after `valkey-cli`.
- **Concurrent test runs** need a distinct `TEST_DB` and `TEST_VALKEY_DB`. See [Tests](#tests).
