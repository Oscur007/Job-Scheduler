# Job Scheduler

A distributed job queue written in Go. Producers submit jobs over HTTP, any number of worker processes pick them up, and a web dashboard shows what is happening. Redis handles queueing and scheduling, Postgres keeps the job history.

![Dashboard](docs/dashboard.png)

## Features

- **Priorities and delayed jobs.** Jobs run in order of scheduled time, with priority breaking ties. A job can be scheduled for the future.
- **Retries with exponential backoff.** A failed job is retried after 2s, 4s, 8s and so on, up to a configurable limit.
- **Dead-letter queue.** Jobs that use up all their retries are marked `failed` and moved to a DLQ.
- **Crash recovery.** If a worker dies mid-job, the job is detected and requeued automatically.
- **Horizontal scaling.** Run as many workers as you like. A job is never handed to two workers at once.
- **REST API and dashboard.** Submit jobs, watch their status, paginate through history.

## Architecture

```mermaid
flowchart LR
    C[Dashboard / API clients] -->|HTTP| S[Server]
    S -->|insert job| PG[(Postgres)]
    S -->|enqueue| R[(Redis)]
    W1[Worker 1] -->|dequeue| R
    W2[Worker 2] -->|dequeue| R
    W1 -->|update status| PG
    W2 -->|update status| PG
```

**Job lifecycle**

```
pending -> running -> done
              |
              +--> pending (retry with backoff) --> ...
              |
              +--> failed (retries exhausted, moved to DLQ)
```

**Redis data structures**

| Key | Type | Purpose |
|---|---|---|
| `jobs:queue` | Sorted set | Job IDs, scored by scheduled time and priority |
| `jobs:data` | Hash | Full job JSON, keyed by job ID |
| `jobs:processing` | Sorted set | Jobs currently being worked on, scored by deadline |
| `jobs:dlq` | List | IDs of jobs that failed permanently |

Postgres has a single `jobs` table with the status, retry count and timestamps for every job. It is the source of truth for history and for the dashboard.

## Quick start (Docker)

Requires Docker with Compose.

```bash
git clone https://github.com/Oscur007/Job-Scheduler.git
cd Job-Scheduler
docker compose up --build
```

Open <http://localhost:8080>. This starts Redis, Postgres, the server and two workers. The `jobs` table is created automatically the first time Postgres starts.

```bash
docker compose up -d --scale worker=4   # change the number of workers
docker compose logs -f worker           # follow worker logs
docker compose down -v                  # stop and delete all data
```

## Running locally (without Docker)

Requires Go, Redis and Postgres.

1. Create the table:
   ```bash
   psql -U jobuser -d jobscheduler -f internal/store/migrations.sql
   ```
2. Optionally create a `.env` file in the project root (see [Configuration](#configuration)). The defaults match a local Redis and Postgres.
3. Start the server and a worker in two terminals, from the project root:
   ```bash
   go run ./cmd/server
   go run ./cmd/worker
   ```

## Configuration

Settings are read from environment variables, or from a `.env` file in the working directory. Real environment variables take precedence.

| Variable | Default | Description |
|---|---|---|
| `REDIS_ADDR` | `localhost:6379` | Redis address |
| `DATABASE_URL` | `postgres://jobuser:jobpass@localhost:5432/jobscheduler?sslmode=disable` | Postgres connection string |
| `HTTP_ADDR` | `:8080` | Address the server listens on |
| `UI_DIR` | `./ui` | Folder the dashboard is served from |
| `PROCESSING_TIMEOUT` | `60s` | How long a worker may hold a job before it is treated as crashed |

The defaults are for local development only. Change the credentials for any real deployment.

## API

| Method | Path | Description |
|---|---|---|
| `POST` | `/jobs` | Submit a job |
| `GET` | `/jobs?page=1&page_size=10` | List jobs, newest first. `page_size` is 10, 25, 50 or 100 |
| `GET` | `/jobs/{id}` | Get one job |
| `GET` | `/stats` | Job counts per status |

Submit a job:

```bash
curl -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{"type":"send_email","payload":"{\"to\":\"a@b.com\"}","priority":5,"max_retries":3,"delay_seconds":0}'
```

| Field | Description |
|---|---|
| `type` | Required. The kind of job |
| `payload` | A string, usually JSON, passed to the worker |
| `priority` | 0 to 100. Higher runs first among jobs due at the same time |
| `max_retries` | Defaults to 3 |
| `delay_seconds` | Wait this long before the job becomes eligible to run |

`GET /jobs` returns `{ "jobs": [...], "total": 57, "page": 1, "page_size": 10 }`.

## How it works

**Ordering.** Each job's score in the `jobs:queue` sorted set is `scheduled_unix_seconds * 1000 + (100 - priority)`. Time dominates, so a delayed job is never picked early, and priority only breaks ties between jobs due in the same second. A worker asks for the lowest-scored job whose time has arrived.

**No double processing.** A worker claims a job by removing it from the sorted set. Redis executes that removal atomically and reports whether it removed anything, so when several workers race for the same job exactly one wins and the others move on. This is covered by a concurrency test.

**Crash recovery.** When a worker claims a job it records a deadline in `jobs:processing`. It clears the entry when the job finishes, fails or is retried. Every worker runs a reaper that checks every 5 seconds for expired deadlines and puts those jobs back on the queue.

**Delivery guarantee.** This gives at-least-once delivery. If a worker is only slow rather than dead, its job can be requeued and run twice, so handlers should be idempotent. Keep `PROCESSING_TIMEOUT` comfortably longer than your slowest job.

## Testing

```bash
go test ./...            # all tests
go test ./... -short     # unit tests only, no Redis needed
```

Integration tests run against a real Redis and use database 15, so they never touch your data. They are skipped automatically if Redis is not reachable. They cover ordering and priorities, delayed jobs, concurrent consumers never sharing a job, the dead-letter queue, and the full crash-recovery flow.

## Project structure

```
cmd/
  server/          HTTP API and dashboard
  worker/          Worker process (processing, retries, reaper)
internal/
  api/             HTTP handlers
  config/          Environment configuration
  job/             Job model
  queue/           Redis queue (enqueue, dequeue, DLQ, processing set)
  store/           Postgres persistence and migrations.sql
ui/                Dashboard (plain HTML, CSS and JS)
```

## Limitations and next steps

- The worker simulates work (a short sleep and a random 50% failure). Real job handlers are the main thing to plug in.
- The dead-letter queue is not shown in the dashboard yet, and failed jobs cannot be re-queued from the UI.
- Writing to Postgres and Redis is not transactional. If the server crashes between the two writes, a job can exist in one but not the other.
- No authentication on the API.
- No metrics endpoint yet. Prometheus metrics would be a natural next addition.
