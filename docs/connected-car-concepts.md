# Connected-Car Command Dispatch — New Concepts

A running notes file for the genuinely new concepts in this project.
Patterns reused from the notification dispatcher (layered structure, retry, status
tracking, worker pool, graceful shutdown, table-driven tests) are not repeated here —
see that project's `docs/learning/` for those.

This file covers only what is new: asynchronous acknowledgement, Kafka, partition
ordering, and idempotency.

---

## Concept 1 — Asynchronous acknowledgement

### The difference from notifications

In the notification dispatcher, `Send` was **synchronous**:

```go
err := notifier.Send(ctx, event)
// the result is known immediately, in the return value
```

One call, one answer. The terminal status `DELIVERED` was set the moment `Send` returned.

In connected-car commands, the acknowledgement is **asynchronous**:

```
1. Send command to car   → status SENT
2. ... time passes ...
3. Car confirms execution → status ACKNOWLEDGED  (arrives on a separate path)
```

The acknowledgement does NOT come back as the return value of the send call.
It arrives later, through a separate channel — another Kafka topic or an HTTP callback.

### The design consequence

`SENT → ACKNOWLEDGED` cannot happen inside the function that sent the command.
There is no return value to wait for. The system needs a **second, independent path**
that receives acknowledgements and updates the status of a command sent earlier.

- **Request/response** (notifications) — send and get the answer in one call
- **Asynchronous messaging** (connected car) — send now, receive the answer separately later

### Interview phrasing

> "Remote car commands are asynchronous — you send a command but the acknowledgement
> comes back on a separate path. So the system has to correlate the response to the
> original command and handle the case where the acknowledgement never arrives."

### The status lifecycle

```
PENDING      — received, not yet published to Kafka
PUBLISHED    — placed on the Kafka topic
SENT         — consumer picked it up and sent to the car
ACKNOWLEDGED — the car confirmed execution  (asynchronous, terminal success)
FAILED       — a send attempt failed, will retry
DEAD         — max retries reached, given up
```

---

## Concept 2 — Kafka

### What it is

A real distributed message log that lives outside the process, persists messages to
disk, and lets multiple consumers read independently. Unlike a Go channel (in-memory,
lost on crash), Kafka persists messages and survives restarts.

### Three core terms

- **Topic** — a named log of messages. This project uses `car-commands`.
- **Producer** — writes messages to a topic.
- **Consumer** — reads messages. Consumers in the same **consumer group** share the work;
  each message goes to only one consumer in the group. This is how you scale horizontally.

### Why Kafka over a Go channel

| | Go channel | Kafka |
|---|---|---|
| Lives | In process memory | External, on disk |
| Survives restart | No — all jobs lost | Yes — messages persist |
| Multiple readers | No | Yes, independent consumer groups |
| Replay | No | Yes, re-read from any offset |

### Offsets and delivery guarantees

Kafka tracks a **committed offset** per consumer group — a bookmark saying
"this group has processed up to here." The critical question is *when* the offset commits.

- **`ReadMessage`** (segmentio/kafka-go) auto-commits the offset *after reading* but
  *before* your code processes it → **at-most-once** → a crash mid-processing loses the message.
- **`FetchMessage` + `CommitMessages`** lets you commit *after* processing →
  **at-least-once** → a crash before commit means the message is redelivered, not lost.

At-least-once is the right choice for commands (do not lose a command), but it creates
duplicates → which is why idempotency (Concept 4) is required.

### The chain that drives this whole project

```
Kafka gives at-least-once delivery
   → at-least-once causes duplicate messages
      → duplicates would execute a command twice (e.g. start climate twice)
         → idempotency makes duplicate processing safe
```

### Local setup notes

- Runs in Docker via KRaft mode (no Zookeeper needed in modern Kafka).
- Topics do not auto-create by default — create explicitly with a `make` target using
  `--if-not-exists` so it is safe to run every startup.
- Add a volume so the topic and data survive `docker-compose down`.

---

## Concept 3 — Partition keys and per-car ordering

### The problem

Kafka only guarantees message order **within a single partition** — not across partitions.
If commands for one car were spread across partitions, they could be consumed out of order.

### Concrete failure example

```
User sends: START_CLIMATE (car-001, 22C), then STOP_CLIMATE (car-001)
If these land on different partitions and are consumed out of order:
  STOP_CLIMATE processed first, START_CLIMATE second
  → climate ends up RUNNING, even though the user's last command was STOP
```

### The fix — key by CarID

Using `CarID` as the Kafka message key means all commands for one car hash to the
**same partition**, so they stay ordered. Different cars may be on different partitions,
processed in parallel.

### The elegant property

```
Key by CarID  →  per-car ordering (correctness)
              →  cross-car parallelism (scale)
```

You get both at once, just from choosing the key well.

### Interview phrasing

> "I keyed by car ID so commands for a single car stay ordered within one partition,
> while different cars are processed in parallel across partitions. Kafka only guarantees
> ordering within a partition, so the key choice is what gives you per-entity ordering."

---

## Concept 4 — Idempotency / exactly-once (Day 5)

_to be added — read the AWS "making retries safe with idempotent APIs" article before this day_

---

## Concept 5 — Retry, timeouts, backoff with jitter (Day 6)

_to be added — read the AWS "timeouts, retries and backoff with jitter" article before this day_

docker-compose exec postgres psql -U notify -d car_commands -c "\dt"