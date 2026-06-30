# Connected-Car Command Dispatch — New Concepts

A running notes file for the genuinely new concepts in this project.
Patterns reused from the notification dispatcher (layered structure, retry, status
tracking, worker pool, graceful shutdown, table-driven tests) are not repeated here —
see that project's `docs/learning/` for those.

This file covers only what is new: asynchronous acknowledgement, Kafka, partition
ordering, idempotency, at-least-once delivery, retry/backoff, the dual-write/outbox
problem, and SELECT FOR UPDATE SKIP LOCKED.

---

## Useful commands (so I do not lose them)

### Run the project

```bash
make docker-up      # start postgres + kafka
make migrate-up     # run goose migrations
make run            # start the service
```

### Inspect the database

```bash
# list tables
docker-compose exec postgres psql -U notify -d car_commands -c "\dt"

# describe a table's columns (useful for checking column types, e.g. TIMESTAMPTZ)
docker-compose exec postgres psql -U notify -d car_commands -c "\d commands"
docker-compose exec postgres psql -U notify -d car_commands -c "\d processed_commands"

# see command states and retry counts
docker-compose exec postgres psql -U notify -d car_commands \
  -c "SELECT id, status, retry_count, last_attempt_at FROM commands;"

# clear test data and start fresh
docker-compose exec postgres psql -U notify -d car_commands \
  -c "DELETE FROM commands; DELETE FROM processed_commands;"
```

### Kafka

```bash
# create the topic (safe to run every startup)
make kafka-topic

# inspect messages on the topic from the console
docker-compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic car-commands --from-beginning
```

### End-to-end retry test

```bash
# 1. set a high offline rate in main.go so commands fail: car.NewCarSimulator(0.7)
# 2. run, then submit a command:
curl -X POST http://localhost:8080/commands \
  -H "Content-Type: application/json" \
  -d '{"car_id":"car-001","type":"START_CLIMATE","payload":"22C"}'

# watch the logs for the journey:
#   car offline                 -> FAILED
#   ~seconds later (backoff)     -> "retry poller: republished command ... retry_count=1"
#   then ACKNOWLEDGED (a retry caught the car online) or DEAD (after maxRetries attempts)

# confirm the final state in the DB:
docker-compose exec postgres psql -U notify -d car_commands \
  -c "SELECT id, status, retry_count FROM commands;"

# 3. set the offline rate back to 0 when done.
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

## Concept 6 — Timeouts, retry, and backoff with jitter

### Three separate concerns, often confused

| Concept | Question it answers |
|---|---|
| **Timeout** | how long to wait for ONE attempt before calling it failed |
| **Backoff** | how long to wait BETWEEN attempts |
| **Retry / max** | how many attempts before giving up (→ DEAD) |

These are independent. You need all three.

### Timeout — bounding a single car call

A real car might hang and never respond. Without a deadline, that one call blocks the
consumer forever — and since one partition is processed one message at a time, the whole
pipeline for that car stalls.

```go
sendCtx, cancel := context.WithTimeout(ctx, c.sendTimeout)
err = c.car.Send(sendCtx, command)
cancel()  // called immediately after the call, not deferred
```

- The timeout wraps ONLY the car call, not the surrounding DB writes.
- `cancel()` is called right after the call returns, to release resources promptly.
- A timeout is treated as a failure → status FAILED → the retry layer re-drives it.

### Testing a timeout — a fake that respects the context

The real `CarSimulator` returns instantly, so it never triggers the timeout.
To test the timeout path, use a separate test fake that is deliberately slow and races
its own delay against the context deadline:

```go
func (s *slowCar) Send(ctx context.Context, command domain.RemoteCommand) error {
    select {
    case <-time.After(s.delay):
        return nil          // car responded before the deadline
    case <-ctx.Done():
        return ctx.Err()    // the timeout fired first
    }
}
```

Set `delay` longer than `sendTimeout` → the context wins → command marked FAILED.
This is also how a real car client should behave: respect the caller's deadline.

### The deepest insight — a timeout is ambiguous

When a car call times out, the system CANNOT tell which happened:
- the command never reached the car, OR
- the command reached the car, executed, and only the acknowledgement was lost on the way back

From the caller's side these are indistinguishable — you only know you got no response,
not that the work did not happen.

**Consequence:** since you must retry on timeout, and the original attempt may have
actually succeeded, the operation MUST be idempotent on the receiving side (the car).
Idempotency is an end-to-end property, not a single checkpoint in your service.

> Interview phrasing: "When a remote command times out, I can't tell whether it failed or
> whether it succeeded and the acknowledgement was lost. The two are indistinguishable, so
> safe retry requires idempotency on the car's side too — idempotency is an end-to-end
> property, not just a check in my service."

### Backoff — spreading retries over time

Naive retry is dangerous. If a downstream system (the car fleet, a backend) is struggling
and every client retries immediately on the same schedule, the retries pile up and hit the
recovering system all at once — a **retry storm** that makes the outage worse.

**Exponential backoff** — wait longer after each failure: 1s, 2s, 4s, ... capped at a max.

**Jitter** — add randomness to the delay. Without jitter, all clients that failed at the
same instant retry at the same intervals — synchronized waves. Jitter spreads them out.

### The full-jitter formula

```
base = 1s, cap = 30s
exponential = min(cap, base * 2^attempt)
delay       = random(0, exponential)   ← full jitter
```

```go
func Backoff(attempt int, base, cap time.Duration) time.Duration {
    if attempt < 0 { attempt = 0 }

    // compute the ceiling in float to avoid int64 overflow on large attempts,
    // and cap BEFORE converting to Duration so the conversion can never overflow
    exponentialFloat := float64(base) * math.Pow(2, float64(attempt))
    if exponentialFloat > float64(cap) {
        exponentialFloat = float64(cap)
    }
    exponential := time.Duration(exponentialFloat)
    if exponential <= 0 {
        exponential = cap
    }

    return time.Duration(rand.Int64N(int64(exponential) + 1)) // [0, exponential]
}
```

Key detail: cap in float space, *before* the `time.Duration` conversion. Converting a
huge float to int64 first can overflow into garbage; capping first makes it safe.

### Why full jitter beats a guaranteed minimum wait

Full jitter returns `random(0, exponential)` — a retry can fire almost immediately even
on a high attempt. That feels wasteful, but the goal of backoff is not "each client waits
politely" — it is "the herd never moves together."

With a guaranteed minimum wait, 1000 clients that failed together are all forced into the
*same narrow band* of time → a synchronized burst. Full jitter spreads them across the
*entire* window → lower peak load at any instant. A few early retries are far less harmful
than a synchronized wave.

> Interview phrasing: "Full jitter allows near-zero delays, which seems wasteful, but it
> spreads retries across the whole window so the peak load is lower. The point of backoff
> is to desynchronize the herd, not to make each client wait a fixed amount."

### How retry fits the architecture (Part 2 — the poller)

The consumer records the outcome of ONE delivery. A separate **retry poller** owns
"try again later" — like the notification dispatcher's polling loop:

```
Every interval:
  find commands where status = FAILED
    and retry_count < max
    and last_attempt_at + backoff(retry_count) < now
  for each:
    increment retry_count, re-publish to Kafka
  commands at retry_count >= max → DEAD
```

The `last_attempt_at + backoff(retry_count) < now` clause implements backoff WITHOUT
sleeping in code — you store when the last attempt happened and only retry once enough
time has elapsed. Database-driven backoff is more robust than sleeping in a goroutine.

---

## Concept 7 — The retry poller, and the dual-write / outbox problem

### What the poller does

The consumer records the outcome of ONE delivery. A separate **retry poller** owns
"try this again later." It is a background ticker loop (same shape as the notification
dispatcher) that runs every interval:

```
1. FindRetryable: SELECT commands WHERE status = FAILED AND retry_count < max
2. for each command:
     due = last_attempt_at + Backoff(retry_count)
     if now < due            → skip (not yet time)
     else if retry_count+1 >= max → mark DEAD (give up)
     else                    → increment retry_count, re-publish to Kafka
```

The re-published command flows back through the consumer. `TryClaim` sees its state is
`PROCESSING` (not `DONE`), so it reprocesses instead of skipping — this is exactly why
the state-bearing record from Concept 5 was needed.

### Database-driven backoff (no sleeping)

The poller does NOT sleep for the backoff duration. Each command stores `last_attempt_at`.
The poller compares `last_attempt_at + Backoff(retry_count)` against `now` and skips
commands not yet due. Backoff is computed in Go (so the jitter from Concept 6 applies on
every decision), not baked into the SQL.

### Per-item errors use continue, not return

One command failing to publish or update must not abandon the rest of the batch:

```go
if err := p.repository.MarkForRetry(...); err != nil {
    log.Printf(...); continue   // skip this one, keep processing the others
}
```

Only `FindRetryable` failing aborts the whole cycle — there is nothing to loop over.
(Same lesson as the notification dispatcher's dispatch loop.)

---

### THE DUAL-WRITE PROBLEM (general, interview-ready)

This is the single most important distributed-systems idea in the project, and a common
interview question. Understand it independently of this project.

**The problem, stated generally:**

> A service often needs to do TWO things that must happen together:
>   1. change its own database (e.g. mark a row, save state)
>   2. tell the outside world (publish a message / call another service)
>
> There is no way to make a database write and a message-broker publish ATOMIC.
> They are two different systems. A crash or error between them leaves the two
> out of sync. This is the **dual-write problem**.

**The two failure shapes (whichever order you pick):**

```
Order A — write DB first, then publish:
  DB committed → CRASH → publish never happens
  → the rest of the world never hears about a change that IS in your DB
  → "lost message"

Order B — publish first, then write DB:
  published → CRASH → DB write never happens
  → the world acted on something your DB has no record of
  → "phantom / duplicate"
```

You cannot escape this by reordering. Both orders fail; they just fail differently.

**Where it shows up in this project (three times — same root cause):**

| Location | The two writes that can't be atomic |
|---|---|
| Idempotency claim (Concept 4) | `processed_commands` insert + the external `Car.Send` |
| Submit (Day 3) | insert command in DB + publish to Kafka |
| Retry poller | `MarkForRetry` in DB + re-publish to Kafka |

---

### THE OUTBOX PATTERN (the standard solution)

The outbox pattern makes the dual-write into a SINGLE atomic database write, then
publishes separately and reliably.

**The core idea:**

> Instead of "write my data AND publish a message" (two systems, not atomic),
> do "write my data AND write the message into an outbox table — in ONE transaction"
> (one system, fully atomic). A separate process reads the outbox and publishes.

**Step by step:**

```
1. In ONE database transaction:
     - make the business change (e.g. insert/update the command)
     - INSERT a row into an `outbox` table describing the message to send
   Both commit together or neither does. Atomic — same database, one transaction.

2. A separate "relay" / publisher process:
     - reads unpublished rows from the outbox
     - publishes each to Kafka
     - marks the outbox row as published

3. If the relay crashes after publishing but before marking published:
     - on restart it re-publishes that row → a DUPLICATE, not a loss
     - duplicates are safe because consumers are idempotent (Concept 4)
```

**Why this works — it converts the unsolvable into the solvable:**

```
Dual-write (unsolvable):  DB write + broker publish  → cannot be atomic
Outbox (solvable):        DB write + outbox-row write → ONE transaction, atomic
                          then: relay publishes at-least-once → duplicates → idempotency handles them
```

The outbox does not magically make two systems atomic. It moves the message into the
database so the "must happen together" part is a single-database transaction. The
publish becomes a separate, retryable step that is allowed to produce duplicates,
because the downstream is idempotent.

**The guarantee it gives:**

> Every committed business change WILL eventually be published (at-least-once),
> and never a phantom publish for a change that did not commit.
> Combined with idempotent consumers → effectively exactly-once end to end.

**Trade-offs / cost:**

- Extra table and a relay process to run and monitor.
- Messages are published slightly later (the relay polls or tails the table).
- Still at-least-once → consumers MUST be idempotent (which is why outbox and
  idempotency are always discussed together).

**Related real-world mechanisms (good to name in an interview):**

- **Transactional outbox + CDC**: instead of the relay polling the outbox table, a
  Change-Data-Capture tool (e.g. Debezium) tails the database write-ahead log and
  publishes outbox rows to Kafka. Lower latency, no polling.
- This is the standard answer to "how do you reliably publish an event when you also
  update your database?" — the outbox pattern.

### Interview phrasing (memorize the shape, not the words)

> "You can't atomically write to your database and publish to a broker — they're two
> systems, so a crash between them desyncs you. That's the dual-write problem. The
> standard fix is the transactional outbox: in one database transaction you write your
> business change AND insert the message into an outbox table, so that part is atomic.
> A separate relay reads the outbox and publishes to Kafka, marking rows as sent. If it
> crashes mid-way it re-publishes — a duplicate, not a loss — which is fine because
> consumers are idempotent. So outbox gives at-least-once delivery of every committed
> change, and with idempotent consumers you get effectively exactly-once. In production
> you often replace the polling relay with CDC, like Debezium tailing the WAL."

### How this project relates to the outbox

This project does the simple store-then-publish in Submit and in the retry poller,
WITHOUT an outbox — so it has the dual-write gap documented above (a crash between the
DB write and the Kafka publish can strand a command). That is an acceptable, documented
limitation for a learning project. The outbox pattern is the production upgrade that
closes it.

---

## Concept 8 — Scaling the poller safely: SELECT FOR UPDATE SKIP LOCKED

### The problem

The poller does: `FindRetryable` (read FAILED rows) → loop → `MarkForRetry` + publish.
There is a window between reading a row and changing its status. In that window the SAME
command can be picked up again, causing a double re-publish, if either:

- a poll cycle takes longer than the interval and overlaps the next cycle, or
- more than one poller instance runs (horizontal scaling).

```
Poller A: SELECT FAILED → gets command X
Poller B: SELECT FAILED → also gets command X (A hasn't updated it yet)
both re-publish X → double retry, double retry_count increment
```

### The fix — SELECT ... FOR UPDATE SKIP LOCKED

Lock the rows you select, and let other readers skip locked rows instead of waiting:

```sql
SELECT id, car_id, type, payload, status, retry_count, last_attempt_at
FROM commands
WHERE status = 'FAILED' AND retry_count < $1
ORDER BY updated_at ASC
LIMIT $2
FOR UPDATE SKIP LOCKED;
```

- `FOR UPDATE` — locks each selected row for the duration of the transaction; no other
  transaction can select-for-update or modify it until this one commits.
- `SKIP LOCKED` — instead of BLOCKING on a row another worker already locked, just skip
  it and move to the next available row.

Result: two pollers running at once each grab a DISJOINT set of rows. No command is
processed by two workers in the same moment. This is the standard pattern for a
database-backed work queue.

### How it fits with the rest

Defense in depth:
- `SELECT FOR UPDATE SKIP LOCKED` PREVENTS double-pickup at the source.
- Even if a duplicate somehow slipped through, the idempotency layer (`TryClaim` →
  PROCESSING/DONE) CATCHES it downstream.

### Scope for this project

This project runs a single poller instance and does NOT use `FOR UPDATE SKIP LOCKED`,
which is fine for one instance. It is documented as the path to safe horizontal scaling.

### Interview phrasing

> "To run multiple poller instances safely, I'd change the fetch query to
> SELECT ... FOR UPDATE SKIP LOCKED. FOR UPDATE locks the rows I'm working on, and
> SKIP LOCKED lets other workers skip them rather than block — so each instance gets a
> disjoint batch. It's the standard pattern for a database-backed work queue, and it
> pairs with idempotency as defense in depth."


---

## References

Articles read while building this project, both from the Amazon Builders' Library:

- **Timeouts, retries, and backoff with jitter** — the source for Concept 6 (retry storms,
  exponential backoff, why full jitter spreads load better than a guaranteed minimum wait).
  https://aws.amazon.com/builders-library/timeouts-retries-and-backoff-with-jitter/

- **Making retries safe with idempotent APIs** — the source for Concepts 4 and 5 (idempotency
  keys, why retries require idempotency, the claim-vs-proof distinction and the dual-write problem).
  https://aws.amazon.com/builders-library/making-retries-safe-with-idempotent-APIs/