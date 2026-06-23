# Connected-Car Command Dispatch — New Concepts

A running notes file for the genuinely new concepts in this project.
Patterns reused from the notification dispatcher (layered structure, retry, status
tracking, worker pool, graceful shutdown, table-driven tests) are not repeated here —
see that project's `docs/learning/` for those.

This file covers only what is new: asynchronous acknowledgement, Kafka, partition
ordering, idempotency, and retry/backoff.

---

## Useful commands (so I do not lose them)

```bash
# connect to postgres and list tables
docker-compose exec postgres psql -U notify -d car_commands -c "\dt"

# create the kafka topic (safe to run every startup)
make kafka-topic

# inspect a topic's messages from the console
docker-compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic car-commands --from-beginning
```

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

### How to scale without breaking ordering

You do NOT scale by adding goroutines to one partition — that destroys ordering.
You scale by adding **partitions and consumers in a group**:

```
1 topic, 6 partitions, 6 consumers in a group
→ each partition is processed by exactly one consumer, in order
→ different cars (different partitions) run in parallel
→ same car (same partition) stays ordered
```

This is why the consumer runs a single goroutine per partition — concurrent goroutines
on one partition would reorder a single car's commands.

### Interview phrasing

> "I keyed by car ID so commands for a single car stay ordered within one partition,
> while different cars are processed in parallel across partitions. Kafka only guarantees
> ordering within a partition, so the key choice is what gives you per-entity ordering.
> To scale, you add partitions and consumers — one consumer per partition — not goroutines."

---

## Concept 4 — Idempotency

### The problem

The chain from Concept 2: at-least-once delivery means the same command can be delivered
twice. Without protection, the car executes it twice. A duplicate START_CHARGING is
harmless, but a duplicate "unlock doors" or a billing event is a real bug.

Idempotency: doing an operation multiple times has the same effect as doing it once.

### The mechanism — idempotency key + database unique constraint

Every command has a unique `ID` — that is the idempotency key.
A `processed_commands` table records which IDs have been handled, with the ID as
primary key. Before processing, try to insert the ID:

- Insert succeeds → first time → proceed
- Insert fails with unique-violation → already processed → skip

The database's unique constraint does the concurrency work. It is race-free: even if two
consumers try at the same instant, only one insert of a given primary key can succeed.

### Detecting the duplicate-key error in pgx

```go
func (r *PostgresCommandRepository) MarkProcessed(ctx context.Context, commandID string) (bool, error) {
    _, err := r.pool.Exec(ctx,
        "INSERT INTO processed_commands (command_id) VALUES ($1)", commandID)
    if err == nil {
        return true, nil // first time
    }

    var pgErr *pgconn.PgError
    if errors.As(err, &pgErr) && pgErr.Code == "23505" { // unique_violation
        return false, nil // already processed
    }
    return false, err // a real error
}
```

`errors.As` extracts the typed `*pgconn.PgError`; SQL state `23505` is unique violation.
Same `errors.As` technique used in the notification project for typed error extraction.

### The check goes BEFORE the side effect

`MarkProcessed` is called before `Car.Send`, so a duplicate never reaches the car.

### The limitation — claim, not proof (the dual-write problem)

The `processed_commands` row is an idempotency **claim**, not proof of execution.

```
1. MarkProcessed (insert token)
2. CRASH before Car.Send
3. Redelivery → MarkProcessed returns false → skipped
4. The command was claimed but NEVER executed → lost
```

The token insert and the car call cannot be atomic — they live in different systems
(the database and the car over the network). This is the **dual-write problem**: you
cannot atomically write to two different systems at once. ACID transactions only cover
operations inside one database.

### Record-first vs send-first — which failure is worse

| Order | Failure on crash | Worse when... |
|---|---|---|
| Record then send | command lost | never — but bad for must-run commands |
| Send then record | command executed twice | charging/billing/unlock — double effect |

Neither order eliminates both failures. The choice depends on the command:
double-charging is unacceptable; double-starting climate is harmless.

### The real solution

Make the operation **idempotent on the receiving side**. If the car treats
"start climate" as "ensure climate is on" rather than "toggle," executing twice is
harmless — and at-least-once delivery becomes safe regardless of order. A production
system also adds a sweeper to find commands stuck in an intermediate state and re-drive them.

### Interview phrasing

> "Recording before sending risks losing the command on a crash; sending before recording
> risks executing twice. Which is worse depends on the command. The robust solution is to
> make the operation idempotent on the receiving side, so duplicates are safe regardless of
> delivery order. My processed_commands table is a best-effort claim — it cannot be fully
> atomic with the external car call, which is the dual-write problem."

---

## Concept 5 — At-least-once delivery, and how it fights idempotency

### The switch: ReadMessage → FetchMessage + CommitMessages

`ReadMessage` auto-commits the offset the instant it returns a message — before any
processing. Crash mid-process → Kafka thinks it was handled → never redelivered →
**at-most-once** → commands can vanish.

`FetchMessage` does not commit. You process first, then call `CommitMessages` only
after the outcome is durably recorded → **at-least-once** → crash before commit means
Kafka redelivers → no lost command.

```
at-least-once delivery  →  no lost commands (Kafka redelivers on crash)
idempotency check       →  redelivery does not execute twice
together                →  effectively exactly-once
```

### The key rule — what "commit" means

> Committing the offset means "I durably recorded what happened to this message" —
> NOT "the command succeeded."

| Outcome | Recorded? | Commit? |
|---|---|---|
| Car acknowledged → ACKNOWLEDGED | yes | commit |
| Car offline → FAILED | yes (a retry layer re-drives it later) | commit |
| DB write itself failed (could not record anything) | no | do NOT commit → redeliver |
| Malformed message (will never parse) | n/a | commit (see poison messages) |

So `process` returns an error only when an *infrastructure* operation stopped it from
recording an outcome. A car being offline is an outcome, not an infrastructure failure.

### Poison messages

A message that can never be processed (e.g. malformed JSON that fails `Unmarshal` every
time). If you skip the commit on it, Kafka redelivers it forever → infinite loop → the
consumer is stuck and no other messages flow. This is a **poison message** (poison pill).

- Quick fix (this project): `return nil` on unmarshal failure so the offset commits and
  the consumer moves on.
- Production fix: publish the bad message to a **dead-letter queue** (`car-commands-dlq`)
  *then* commit — preserves it for inspection instead of silently dropping it.

> Interview phrasing: "I commit past poison messages to avoid an infinite redelivery loop,
> and in production I'd route them to a dead-letter queue rather than dropping them."

### The bug: idempotency claim fights at-least-once redelivery

A real flaw in the simple design, found by tracing the failure path:

```
1. MarkProcessed inserts command_id    → claimed
2. UpdateStatus(SENT) fails             → process returns error
3. offset NOT committed                 → Kafka redelivers
4. redelivery: MarkProcessed → false    → "duplicate, skip"
5. command claimed but never sent       → LOST
```

The redelivery that should retry is blocked by your own idempotency claim. The two
mechanisms fight each other. Root cause: `MarkProcessed` is a **claim**, not **proof** —
the command is marked "seen" before the work is confirmed done.

### Two fixes

**Fix 1 — transaction around the DB writes.**
Put `MarkProcessed` and `UpdateStatus(SENT)` in one transaction: either both commit or
both roll back. A failure rolls back the claim, so redelivery genuinely retries.
Limitation: a transaction can only cover *database* writes. It cannot include `Car.Send`
(external system, over the network), so the claim-vs-actually-sent gap remains — this is
the dual-write problem again. A transaction shrinks the window; it does not close it.

**Fix 2 — state-bearing idempotency record (the stronger pattern).**
Give `processed_commands` a state instead of a binary seen/not-seen:

```
PROCESSING  — claimed, work not yet confirmed done
DONE        — work confirmed complete
```

On redelivery, a row in `PROCESSING` means "claimed but maybe unfinished → retry",
NOT "skip". Only `DONE` means skip.

```
First delivery:           insert PROCESSING → do work → update DONE
Crash after claim:        row stuck in PROCESSING
Redelivery:               sees PROCESSING (not DONE) → retries instead of skipping
```

Production systems combine: state-bearing record + transaction around DB writes +
operation idempotent on the receiving side (the car ignores a true duplicate). Together
these get genuinely close to exactly-once.

### Scope note for this project

The simple binary claim is kept here, with the limitation documented. Finding and
explaining the flaw matters more than a perfect implementation.

> Interview phrasing: "My first idempotency design had a gap — a failure between claiming
> and doing the work would block redelivery from retrying. The fix is a state-bearing
> idempotency record plus a transaction around the database writes, though the external
> car call still can't be made fully atomic — that's the dual-write problem."

---

## Concept 6 — Retry, timeouts, backoff with jitter (Day 7)

_to be added — read the AWS "timeouts, retries and backoff with jitter" article before this day_