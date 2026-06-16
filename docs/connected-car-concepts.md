# Connected-Car Command Dispatch — New Concepts

A running notes file for the genuinely new concepts in this project.
Patterns reused from the notification dispatcher (layered structure, retry, status
tracking, worker pool, graceful shutdown, table-driven tests) are not repeated here —
see that project's `docs/learning/` for those.

This file covers only what is new: asynchronous acknowledgement, Kafka, and idempotency.

---

## Concept 1 — Asynchronous acknowledgement (Day 1)

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

This is the difference between:
- **Request/response** (notifications) — send and get the answer in one call
- **Asynchronous messaging** (connected car) — send now, receive the answer separately later

### Questions this raises (handled later in the project)

| Question | Where it is solved |
|---|---|
| What if the acknowledgement never arrives? | retry + timeout — car stayed offline |
| What if the acknowledgement arrives twice? | idempotency |
| How to match an acknowledgement to its command? | correlate by command ID |

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

## Concept 2 — Kafka (Day 2)

_to be added_

---

## Concept 3 — Idempotency / exactly-once (Day 5)

_to be added_