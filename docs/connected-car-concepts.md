# Connected-Car Command Dispatch — Concepts

A running notes file for the distributed-systems concepts applied in this project. Each concept is written with the reasoning and the trade-offs, not just the code.

Concepts covered so far: asynchronous acknowledgement, Kafka, partition ordering,
idempotency, at-least-once delivery, timeouts and retry with backoff/jitter, the
dual-write/outbox problem, and SELECT FOR UPDATE SKIP LOCKED. More are added as the
project grows.

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

## Concept 1 — Acknowledgement: how you learn a command worked

### The idea

A remote command to a car is not like a normal function call. When you send a command,
you cannot know immediately whether the car executed it — the car is a physical device
that may be offline, in a tunnel, or slow to respond. So the real question is: **how does
the service learn whether the command actually took effect?**

There are three patterns, from simplest to messiest in real-world systems:

1. **Synchronous response** — the send call returns the result immediately.
   Simple, but only works when the device answers right away.
2. **Asynchronous acknowledgement** — the device sends a confirmation back *later*, on a
   separate channel (a different topic, an HTTP callback), correlated to the command by ID.
3. **State reconciliation** — there is *no* acknowledgement at all. You fire the command,
   then separately watch the device's own reported state (telemetry) to *infer* whether it
   worked. Common in real telematics, because a dedicated ack channel often does not exist.

### Why the lifecycle has separate SENT and ACKNOWLEDGED states

Because confirmation does not arrive with the send call, the model needs a distinct state
for "sent to the car but not yet confirmed" versus "confirmed done":

```
PENDING      — received, not yet published to Kafka
PUBLISHED    — placed on the Kafka topic
SENT         — consumer picked it up and sent to the car/device
ACKNOWLEDGED — the car's execution was confirmed (terminal success)
FAILED       — a send attempt failed or was not confirmed, will retry
DEAD         — max retries reached, given up
```

### A real production example — state reconciliation (pattern 3)

On a car-sharing platform I worked on, door open/close commands followed a fire-and-forget
plus telemetry/state reconciliation model, which is a good example of why acknowledgement
is hard.

- The send was **fire-and-forget**. The service handed the command to the telematics layer
  ("send when connected") and returned. The call was `void` — "success" only meant the send
  did not throw, not that the door actually opened or closed.
- There was **no direct ACK tied to the door command** in the car-sharing service. Door
  state was learned from a separate telemetry/event pipeline.
- **Door open** was checked later using vehicle state such as ignition, physical door status,
  central lock status, and related timestamps.
- **Door close** was checked later using `doorLockStatus`, which represents whether the vehicle was locked or unlocked.

So there was no clean "the car said yes." There was: send the command, store timestamps,
watch the vehicle's telemetry and events, and infer the result from later state. This is
mostly pattern 3 — the service relied on telemetry/state/event reconciliation because there
was no reliable per-command ACK for the door command.

### What this project actually does — a deliberate simplification

In this project the car is a **simulator** (`CarSimulator`) whose `Send` returns
synchronously — success or failure comes straight back as the return value (pattern 1). So
here `SENT → ACKNOWLEDGED` happens inside the same `process` call, right after `car.Send`
returns. There is no separate ack path and no state reconciliation.

This is a conscious scoping choice. The status model still keeps `SENT` and `ACKNOWLEDGED`
as distinct states so the concept stays visible, but the implementation collapses them into
one synchronous step. The real-world versions — an async ack channel (pattern 2), or the
telemetry-reconciliation approach from the car-sharing platform above (pattern 3) — are the
natural extensions if the project grew toward production behaviour.

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
If commands for one car were spread across multiple partitions, they could be consumed out
of order.

### Concrete failure example

```
User sends: START_CLIMATE (car-001, 22C), then STOP_CLIMATE (car-001)
If these land on different partitions and are consumed out of order:
  STOP_CLIMATE processed first, START_CLIMATE second
  → climate ends up RUNNING, even though the user's last command was STOP
```

### The fix — key by CarID (and how the hashing works)

You do not compute a partition yourself. You set a **key** on each message, and Kafka's
partitioner hashes that key to pick a partition:

```
partition = hash(key) % numberOfPartitions
```

By using `CarID` as the key (`Key: []byte(command.CarID)` in segmentio/kafka-go), every
command for one car hashes to the **same partition**, so that car's commands stay ordered.
Different cars hash to different partitions and are processed in parallel. The default hash
is murmur2 (same as the Java client, so keys map consistently across clients).

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

The consumer runs a single goroutine per partition — concurrent goroutines on one
partition would reorder a single car's commands.

The partition count is the **ceiling on parallelism** within a group: you can have at most
as many actively-working consumers as there are partitions. Extra consumers sit idle.
Fewer consumers than partitions is fine — each consumer just owns more than one partition
and reads them one at a time (no partition is ever ignored).

### Partition mechanics worth remembering

**Where the partition is chosen: the producer.** The producer decides which partition
each message goes to, before it ever reaches the broker. Two cases:

- **Key set** (this project, key = CarID): `partition = hash(key) % partitionCount`.
  Same key → same partition → ordered. This is keyed/hash partitioning.
- **No key**: the producer spreads messages across partitions roughly evenly
  (round-robin / sticky batching). No ordering guarantee — fine when order does not matter.

So "how do messages split across 3 partitions?" → keyed messages by `hash(key) % 3`,
keyless messages round-robin. Either way it is the **producer's** decision, not the broker's.

**Choosing the strategy in code (segmentio/kafka-go).** The producer's key and balancer
decide this:

```go
// Keyed → hash-by-key → same key to same partition → ORDERED (this project)
kafka.Message{Key: []byte(command.CarID), Value: value}

// No key → round-robin across partitions → NOT ordered, maximum spread
kafka.Message{Value: value}                 // drop the Key
// optionally make it explicit on the Writer:
//   Balancer: &kafka.RoundRobin{}          // cycle partitions
//   Balancer: &kafka.LeastBytes{}          // send to the least-loaded partition
//   Balancer: &kafka.Hash{}                // hash the key (what a keyed writer does)
```

When to use which: **keyed/hash** when per-entity order matters (car commands — START then
STOP for one car must stay ordered). **Round-robin/keyless** for independent messages where
order is irrelevant and even spread is the goal — metrics, logs, click/telemetry events.
This project keeps the key = CarID because command ordering per car is a real requirement;
switching to round-robin here would reintroduce the out-of-order bug from the failure
example above.

**Consumer-count scenarios (group of consumers, N partitions):**

```
3 partitions, 3 consumers → 1 partition each, full parallelism
3 partitions, 2 consumers → one consumer gets 2 partitions, the other 1 (no idle, less parallel)
3 partitions, 4 consumers → 3 work, 1 sits IDLE (a partition maps to only one consumer)
3 partitions, 1 consumer  → that consumer reads all 3, one message at a time (correct, not parallel)
```

**One consumer dies → rebalance.** If a consumer in the group crashes, Kafka detects it
(missed heartbeats) and **rebalances**: its partitions are reassigned to the surviving
consumers automatically. With 3 partitions / 3 consumers, losing one leaves 2 consumers
splitting 3 partitions (2 + 1). No messages are lost — the new owner resumes from the last
committed offset. When the consumer comes back, another rebalance redistributes evenly again.

**The ceiling:** parallelism within a group is capped at the partition count. To go faster,
add partitions first, then consumers — never more consumers than partitions.

### Caveat — changing partition count re-maps keys

Because the partition is `hash(key) % numberOfPartitions`, changing the partition count
changes where existing keys land: `hash(k) % 6` and `hash(k) % 8` can differ for the same
key. Adding partitions can therefore break ordering for in-flight commands, which is why
partition count is a decision you try to fix up front rather than change casually.

### How to actually scale this (mechanics)

- **Partition count lives on the topic**, not in the Go code. To change it you alter the
  topic (`kafka-topics --alter --partitions N`) or delete and recreate it. The Kafka
  writer, the consumer code, and docker-compose do **not** change — keying by `CarID`
  already makes the code partition-count-agnostic.
- **To run a second consumer instance**, start the program again with the **same `GroupID`**
  (on a different HTTP port locally). Kafka's consumer group automatically assigns partitions
  to the instances — no assignment code. Stop one and Kafka **rebalances** the partition to a
  survivor automatically.

### What this project actually does

This project creates the topic with **a single partition** (`--partitions 1`). With one
partition, every message is on the same partition, so ordering is trivially guaranteed and
the CarID key is not yet doing real work — it is set deliberately so that the moment the
topic is scaled to multiple partitions, per-car ordering holds automatically without any
code change. The keying is correct and future-proof; the multi-partition behaviour above is
the design it enables, not something the single-partition setup exercises today.

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

### Key point

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

> Key point: "I commit past poison messages to avoid an infinite redelivery loop,
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

This project implements **Fix 2** — a state-bearing `processed_commands` record with a
`TryClaim` method (claims/reprocesses when `PROCESSING`, skips only when `DONE`), plus a
transaction around the acknowledge-and-done writes. The claim-vs-actually-sent gap against
the external car call is documented as the remaining dual-write limitation.

> Key point: "My first idempotency design had a gap — a failure between claiming
> and doing the work would block redelivery from retrying. The fix is a state-bearing
> idempotency record plus a transaction around the database writes, though the external
> car call still can't be made fully atomic — that's the dual-write problem."

### Why a stuck SENT command is rare — and where a sweeper still helps

It is worth being precise about when a command can strand in `SENT`. `SENT` is written
*before* the Kafka offset is committed (the commit only happens after `process` returns
success). So under a normal crash — power loss, OOM kill, a DB error from
`MarkAcknowledgedAndDone` — the offset was never committed, Kafka **redelivers**, `TryClaim`
sees `PROCESSING`, and the command is reprocessed. It self-heals. The one caveat is that the
car may then be sent the command twice, which is why the car should be idempotent on its own
side.

A command only truly strands in `SENT` if the Kafka offset advances past it *without* the
command ever reaching `DONE` — which needs an abnormal event, not a routine crash: a manual
offset reset, a consumer-group reset that skips the message, or a retention edge case. So a
**stuck-command sweeper** (find commands sitting in `SENT` past a staleness threshold and
reset them to `FAILED` so retry re-drives them) is defense-in-depth for those rare cases, not
something the normal happy/crash paths require. This project does not implement it; it is a
documented safety net.

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

> Key point: "When a remote command times out, I can't tell whether it failed or
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

> Key point: "Full jitter allows near-zero delays, which seems wasteful, but it
> spreads retries across the whole window so the peak load is lower. The point of backoff
> is to desynchronize the herd, not to make each client wait a fixed amount."

### How retry fits the architecture (Part 2 — the poller)

The consumer records the outcome of ONE delivery. A separate **retry poller** owns
"try again later" — a background polling loop:

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
"try this again later." It is a background ticker loop that runs every interval:

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

---

### THE DUAL-WRITE PROBLEM (general, interview-ready)

This is the single most important distributed-systems idea in the project.

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

**Related real-world mechanisms (good to name):**

- **Transactional outbox + CDC**: instead of the relay polling the outbox table, a
  Change-Data-Capture tool (e.g. Debezium) tails the database write-ahead log and
  publishes outbox rows to Kafka. Lower latency, no polling.
- This is the standard answer to "how do you reliably publish an event when you also
  update your database?" — the outbox pattern.

### Key point

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
DB write and the Kafka publish can strand a command). The outbox pattern is the production upgrade that closes it.

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

### Key point

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


## Concept 9 — Atomic Writes, PID Files, and Write-Ahead Logging (WAL)

### The one root problem behind all of this

A crash or power loss can happen at ANY moment — even in the middle of a write.
Every idea below exists to answer: **how do we keep data correct even if that happens?**

### Atomic file operations

Writing directly to a file can leave it half-written if a crash happens mid-write.
UNIX gives us some system calls that are atomic at the OS level — the operation either
fully happens or does not happen at all, never a half-done state (see the list of atomic
UNIX operations: https://rcrowley.org/2010/01/06/things-unix-can-do-atomically.html).
This is the building block everything else relies on.

### PID files

A PID file stores a running process's ID, used to answer "is this already running?" —
so a crash-restart doesn't accidentally start a duplicate process that fights the
original over the same DB/files. A small, simple example of "safely record state on disk."

### Why databases care about crashes

Databases must guarantee **durability**: once something is "committed," it must survive
a crash. Writing directly to the real data files (tables, B-tree pages, indexes) is risky
for this — those writes are large and scattered, and a half-applied change can corrupt
a page that's hard to repair. This is the problem WAL solves.

### WAL (Write-Ahead Log) — one word: **recovery**

Before changing the real data, first write down *what you're about to do*, in a simple
append-only log. Think of it as a diary of intentions written before the action.

1. append to log: "about to change X from A to B"
2. fsync the log            ← must happen before step 3
3. now change the real data


Append-only writes are simple and safe — much safer than in-place edits to complex
data structures.

### Why WAL is written BEFORE the real change

If the crash happens between step 1 and step 3, the log still has proof of what was
supposed to happen. On restart, that unfinished instruction can be redone (**redo**).
If the real data were changed first and the crash hit there, you'd have a broken,
unexplained state with no record of what was intended.

### Recovery after a crash
on restart:

read the WAL from the last checkpoint

for each entry:

not yet applied → redo (apply it)

should not have been kept → undo

Recovery is only possible because the log survived the crash and memory didn't.

### fsync — the detail that makes it real

`Write()` often only lands in an OS buffer, not physical disk. `fsync()` forces a real
flush to disk. Without it, a "durable" write can still vanish on power loss. This is a
speed vs. durability trade-off — batching writes and fsyncing once per batch is a common
optimization.

### Terms — one line each

- **Atomic file op** → all-or-nothing write, no half-written file
- **PID file** → detect "already running," avoid duplicate processes
- **WAL** → recovery — write intent first, so a crash never loses what should happen
- **fsync** → force OS buffer to real disk, so "written" really means durable
- **mmap** → treat a file like an in-memory array, fast reads/writes without manual I/O
- **B-tree** → keep large on-disk data sorted and fast to search (used for indexes)
- **Page** → fixed-size chunk (e.g. 4KB) databases read/write at once
- **Checkpoint** → periodically save current state, so replay doesn't start from zero
- **Idempotency** → doing something twice = same as doing it once (needed because WAL/Kafka can replay)

### How this connects to my project — Outbox Pattern

My dual-write gap (DB write + Kafka publish are not atomic) is the same category of
problem as WAL solves. The outbox pattern is WAL's idea applied here:
outbox row = the "log entry" (intent written first, same transaction as the business write)

publish to Kafka = the "real data change" (the actual outside-world effect)

relay/poller replaying unpublished rows = "crash recovery / redo"

Insert the business row + an outbox row in ONE transaction → a separate relay reads
unpublished outbox rows and publishes them → if the relay crashes mid-publish, it
re-publishes on restart (a duplicate, not a loss) → safe because consumers are
idempotent (Concept 4). This is the natural next addition on top of what I already have.

## Concept 10 — CDC Disaster Recovery Across Two Data Centers

### The scenario

A CDC pipeline normally runs in one data center: Debezium reads changes from PostgreSQL,
publishes them to Kafka, and a sink connector writes them into another database. This
works fine day-to-day — but what happens if that entire data center goes down? A DR
(Disaster Recovery) design lets a second data center take over without losing data or
creating duplicates.

Call the two data centers **DC-A** (primary) and **DC-B** (standby).

### The key insight — "staying in sync" is actually three separate layers

It's tempting to think "just replicate the database" is enough. It is not. Three
independent layers all need to stay in sync, or failover breaks:

1. **Database layer** — the actual data
2. **Kafka layer** — the messages and how far each consumer has read
3. **CDC (Debezium) layer** — which position in the database log it has already read

Missing any one of these causes either **duplicate data** or **lost data** after failover.

### Layer 1 — Database replication slots

Tools like Patroni already handle PostgreSQL replication and automatic failover — that
part is familiar. The subtle problem is that **Debezium doesn't read the database
directly** — it reads through a **logical replication slot**, which tracks its exact
read position (an LSN — Log Sequence Number, PostgreSQL's version of a WAL position).

If DC-A dies and DC-B becomes primary, but the replication slot doesn't exist in DC-B at
the same position, Debezium has no choice but to start over with a fresh snapshot →
duplicate events, or worse, missed changes.

**Fix**: PostgreSQL 17+ supports **failover slots** — replication slots are
automatically kept in sync on the standby server, so DC-B has the same slot at the same
LSN as DC-A. After failover, Debezium can resume from exactly where it left off.

### Layer 2 — Kafka topics AND consumer offsets

Just copying Kafka topics to DC-B is not enough. The sink connector also tracks **which
offset it has already processed** (its consumer group offset). If that offset is wrong
after failover:

- offset too old → already-processed messages get reprocessed → duplicates
- offset too new → unprocessed messages get skipped → data loss

**Fix**: replicate topics *and* consumer group offsets together. **MirrorMaker 2 (MM2)**,
Kafka's official cross-cluster replication tool, does both — it has separate connectors
for replicating topic data, syncing consumer offsets, and monitoring replication lag.

**The non-obvious placement rule**: MM2 should run **next to the target cluster, not the
source** — "remote consume, local produce." For DC-A → DC-B replication, MM2 runs in
DC-B, pulling remotely from DC-A. Reasoning: the producer side of a replication link is
the fragile side. If MM2 lived in DC-A and DC-A died completely, replication dies with
it. Running MM2 in DC-B means it can keep pulling as long as DC-A is reachable at all,
independent of what else fails in DC-A.

### Layer 3 — Debezium: active-passive, not active-active

Only **one** Debezium connector should be active at a time. Running the same connector
in both data centers simultaneously means each one manages its own replication slot and
offset independently, reading the same source and producing duplicate events into Kafka.
Coordinating two active readers safely is a lot of extra complexity for little benefit.

So: DC-A's connector runs normally. DC-B's connector is fully configured but stays off,
ready to be switched on.

### Failover sequence (DC-A dies → DC-B takes over)

```
1. Database failover promotes DC-B to primary
2. Failover slots mean the replication slot in DC-B is already at the same LSN as DC-A
3. Kafka topics + consumer offsets are already synced via MM2
4. MM2 is stopped
5. DC-B's Debezium connector is switched on, resumes from the same LSN → no re-snapshot, no duplicates, no data loss
```

### Failback (DC-A comes back)

You don't just switch back immediately — DC-A has fallen behind and must catch up first:

```
1. DC-A rejoins as a standby, catches up on missing WAL via normal replication
2. A temporary MM2 is set up in DC-A (this time syncing DC-B → DC-A)
3. DC-B's connectors are stopped
4. DC-A's temporary MM2 is stopped
5. DC-A's connectors are switched back on
6. DC-B's original MM2 (DC-A → DC-B direction) is reactivated
```

### How this connects to what I already know

This is the same WAL / offset / idempotency thinking from my project, just at a bigger
scale — two data centers instead of one process:

- A **replication slot position (LSN)** is the same idea as a **Kafka consumer offset**
  in my retry poller — both are "how far have I read/processed," just at different
  layers (Postgres WAL vs. Kafka log).
- **Active-passive Debezium** is the same reasoning as why my consumer processes one
  partition with a single goroutine — one active reader avoids duplicate/out-of-order
  processing; coordinating multiple concurrent readers safely is much harder.
- The overall goal — resume exactly where you left off after a crash/failover, without
  losing or duplicating anything — is the same durability goal WAL and the outbox
  pattern solve, just applied across two entire data centers instead of one database.


## Concept 11 — Observability: metrics, logs, and traces

Source: Amazon Builders' Library — "Instrumenting distributed systems for operational visibility"
https://aws.amazon.com/builders-library/instrumenting-distributed-systems-for-operational-visibility/

### Why it matters

A service that works is not the same as a service you can *operate*. When something goes
wrong at 3am — commands piling up in FAILED, the poller falling behind, the car endpoint
slow — you need to answer "what is happening and why" without guessing. Observability is
the instrumentation that lets you measure how the system behaves from the outside.

### The three pillars — what each answers

- **Metrics** — *what* is happening, in aggregate: request rate, latency, error rate,
  queue depth. Cheap to store, good for dashboards and alarms. Answers "is something wrong?"
- **Logs** — *why*, in detail, for one event: a structured line per command with its ID,
  status, and any error. Answers "what happened to this specific command?"
- **Traces** — *where*, across steps: follow one unit of work through the whole pipeline
  using a shared ID, so you can line up what happened where.

Rough rule: metrics find the problem, traces locate it, logs explain it.

### Correlation ID vs trace ID — the difference

These get used loosely, but they are not the same thing:

- **Correlation ID** — the general idea: any ID stamped on related log lines so you can
  group them by filtering on that one field. Does not require multiple services.
- **Trace ID** — a *specific kind* of correlation ID, used to follow one request as it
  crosses **service/process boundaries** in a distributed system. It is propagated over
  the network (an HTTP header like `traceparent`, or a message header) so a *different*
  process continues the same trace. Full distributed tracing tools (Jaeger, OpenTelemetry)
  add **span IDs** on top — one span per segment of the journey — to show the call tree and
  timing.

So: every trace ID is a correlation ID; not every correlation ID is a trace ID.

### What this project uses — the command ID as a correlation ID

In this project the command `ID` is used as the correlation ID. It is created at the
"front door" (the service, when the command is first built) and threaded into every log
line in the service, the consumer, and the retry poller. Filtering logs by one `command_id`
reconstructs that command's whole journey — PENDING → PUBLISHED → SENT → retries →
ACKNOWLEDGED/DEAD — across all three components. It costs almost nothing (one `slog.With`
per command) and gives most of the value of tracing.

**Why the command ID is a natural correlation ID:** it already uniquely identifies the unit
of work, it already exists everywhere the command goes, and it is already stored — so no new
identifier had to be invented.

### Why it is trace-like across the retry poller (not just in-process)

The command ID does more than group logs inside one process — it **travels inside the Kafka
message** from the producer to the consumer, and the retry poller re-publishes the same
command (same ID) back onto the topic. So the ID crosses a message boundary between stages
that could run as **separate processes** (the consumer can be scaled to multiple instances —
see Concept 3). Because the same ID propagates across that boundary and lets a different
process continue following the same command, it functions as a lightweight **trace ID**, not
merely an in-process correlation ID.

What this project does *not* have is a full tracing backend: there are no span IDs, no
per-segment timing spans, and no tool like Jaeger to visualize the call tree. So the honest
description is: the command ID is a correlation ID that also propagates through Kafka, giving
**trace-like end-to-end visibility** with nothing but structured logging.

### Metrics worth emitting here (the plan)

Not one lumped "errors" counter — break metrics out by dimension so the most common
problems surface first:

- commands by outcome: submitted, published, sent, acknowledged, failed, dead
- retry poller: cycles run, commands re-published, commands marked DEAD
- car send: latency (a histogram/timer), timeout count, offline count
- consumer: messages processed, processing latency, consumer lag

Break dependency metrics out per call and per status so "the car endpoint is slow" or
"DB writes are failing" is visible directly, not inferred.

### Key point

> "I use the command ID as a correlation ID, stamped at the front door and carried through
> every log line in the service, consumer, and poller — so I can trace one command's whole
> journey by filtering on that one field. It even propagates through the Kafka message to the
> consumer, which may be a separate process, so it's trace-like, not just in-process. It's not
> full distributed tracing — no span IDs or a tracing backend — but it gives the same
> follow-one-request-end-to-end capability with just structured logging. A correlation ID is
> the general idea; a trace ID is the version that crosses service boundaries."

### What this project does now — structured logging

Implemented: **structured logging** with `slog`, with the command ID (plus car ID and command
type) attached as fields on every line via a per-command child logger. All three components —
service, consumer, retry poller — log through it, so one command is traceable end to end.

### Next step — metrics

Not yet implemented: a **`/metrics`** endpoint with Prometheus counters for commands by
outcome (published, acknowledged, failed, dead) and a histogram for car-send latency (the
`send_duration_ms` already measured in the consumer's logs becomes the histogram).