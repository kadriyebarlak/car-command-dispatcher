# Connected-Car Command Dispatch

A backend service in Go that reliably delivers remote commands (e.g. `START_CLIMATE`,
`START_CHARGING`) to connected vehicles over Kafka.

The hard part it solves: cars are physical devices that may be offline, slow, or
unreachable — so a command must **never be lost** and **never be executed twice**, even
across crashes and retries. This project is a hands-on exploration of the distributed-systems
patterns that make that guarantee possible, built as a learning project with production-grade
practices.

> **The design reasoning behind every part of this system is documented in
> [`docs/connected-car-concepts.md`](docs/connected-car-concepts.md)** — the concepts, the
> trade-offs, and the deliberate simplifications. That document is the heart of this project.

---

## Architecture

```
HTTP API  →  Service  →  Kafka  →  Consumer  →  Car
POST         store +      car-      idempotent    (simulated)
/commands    publish      commands  processing
                            ↑            |
                            └── retry with backoff (poller)
```

A command flows through a status lifecycle:

```
PENDING → PUBLISHED → SENT → ACKNOWLEDGED   (success)
                       ↓
                     FAILED → (retry) → ACKNOWLEDGED   (recovered)
                       ↓
                     DEAD    (gave up after max retries)
```

---

## What it demonstrates

- **Guaranteed delivery** — at-least-once processing via Kafka (`FetchMessage` +
  `CommitMessages`), so a crash never loses a command.
- **Idempotency** — a state-bearing `processed_commands` record (`TryClaim` → `PROCESSING`
  / `DONE`) so a redelivered or retried command is never executed twice.
- **Per-car ordering** — commands are keyed by `CarID`, so all commands for one car stay
  ordered within a Kafka partition, while different cars are processed in parallel.
- **Retry with backoff and jitter** — a separate poller re-drives `FAILED` commands with
  exponential backoff + full jitter, and marks them `DEAD` after a max number of attempts.
- **Timeouts** — each car call is bounded by a context timeout; a hanging car cannot stall
  the pipeline.
- **Observability** — structured logging (`slog`) with a command-ID correlation ID,
  Prometheus metrics (`/metrics`), and distributed tracing (OpenTelemetry → Jaeger) that
  follows a single command across the producer and consumer, through Kafka.

Each of these is explained — with the failure cases that motivate it and the trade-offs it
carries — in the [concepts document](docs/connected-car-concepts.md).

---

## Tech stack

| Area | Technology |
|---|---|
| Language | Go |
| Messaging | Apache Kafka (`segmentio/kafka-go`, KRaft mode) |
| Storage | PostgreSQL (`pgx`, `goose` migrations) |
| HTTP | `chi` router |
| Metrics | Prometheus (`client_golang`) |
| Tracing | OpenTelemetry + Jaeger |
| Runtime | Docker / docker-compose |

---

## Getting started

**Prerequisites:** Docker, Docker Compose, and Go installed.

```bash
make docker-up      # start postgres + kafka (and jaeger, for tracing)
make migrate-up     # run database migrations
make kafka-topic    # create the car-commands topic
make run            # start the service on :8080
```

Submit a command:

```bash
curl -X POST http://localhost:8080/commands \
  -H "Content-Type: application/json" \
  -d '{"car_id":"car-001","type":"START_CLIMATE","payload":"22C"}'
```

Then explore:

- **Traces** — open Jaeger at http://localhost:16686, pick `car-command-dispatcher`, and
  view a single command's journey across the service and consumer.
- **Metrics** — http://localhost:8080/metrics (command outcomes, car-send latency,
  retry backlog).
- **Logs** — structured JSON on stdout; every line for one command shares its `command_id`.

### Watch the retry mechanism

The car is simulated with a configurable offline rate. To see retries and the `DEAD` path,
raise the offline rate in `main.go` (`car.NewCarSimulator(0.7)`), then submit a command and
watch the logs and the `car_commands_total` metric climb through `failed` → `dead`, or
recover to `acknowledged` when a retry catches the car online.

---

## Documentation

- **[Concepts: distributed-systems design & trade-offs](docs/connected-car-concepts.md)** —
  the main document. Acknowledgement patterns, Kafka delivery guarantees, partition ordering,
  idempotency, the dual-write / outbox problem, retry/backoff/jitter, and `SELECT FOR UPDATE
  SKIP LOCKED`.
- **[Observability lab](docs/observability-lab.md)** — a hands-on log of turning the project
  into an observability laboratory (metrics, distributed tracing), with real findings such as
  a hidden 1-second latency stall traced to Kafka producer batching.

---

## Scope and honesty

This is a **learning project**, and it is deliberately honest about what is and isn't built:

- The **car is a simulator** (synchronous responses). Real telematics would acknowledge
  asynchronously or via state reconciliation — both are discussed in the concepts doc.
- The topic runs a **single partition** for simplicity; the code is keyed by `CarID` so it is
  ready for multiple partitions without changes.
- The **outbox pattern**, **stuck-command sweeper**, and **`SELECT FOR UPDATE SKIP LOCKED`**
  scaling are documented as the production next steps rather than implemented.

These are conscious scoping choices, each with its production upgrade path written down —
because knowing *what you simplified and why* is part of the engineering.