
# Observability Lab — Hands-On Log

This section is the practical, experiment-driven log: each phase turns this project a little more into an observability laboratory, with real findings recorded as they happen.

---

## Phase 0 — First metric, first investigation (HTTP request rate + latency)

**Goal:** the smallest useful observability loop — instrument one thing, collect it, query
it, see a result. Answer the most basic question: "how many requests are arriving, and how
long do they take?"

### What was added

- An HTTP request **counter** — total requests received.
- An HTTP request-duration **histogram** — how long each request took.
- Prometheus running locally (one container), scraping `/metrics`.

### Queries run (PromQL)

```promql
http_requests_total                       # total requests received so far
rate(http_requests_total[1m])             # avg requests per second (last 1m)
rate(http_requests_total[1m]) * 60        # avg requests per minute
increase(http_requests_total[1m])         # approx how many arrived in the last minute

# average request duration = total time / number of requests
http_request_duration_seconds_sum
  / http_request_duration_seconds_count
```

The `sum / count` pattern is the standard way to get an average from a Prometheus histogram:
the histogram tracks both the running total of observed durations and the count of
observations, so dividing them gives the mean.

### The finding — a hidden 1-second stall (Kafka producer batching)

This is the point of the whole exercise. The latency query returned **~1 second per request**.
Nothing was erroring, commands were being published, the system "worked" — but 1 second to
submit a command is absurdly long.

**Investigation:** added a log line around the Kafka writer's publish call. The log confirmed
~1000 ms was spent *inside the publish*. The cause: `segmentio/kafka-go`'s writer **batches
messages** by default and waits up to `BatchTimeout` (default **1 second**) to fill a batch
before sending. At low traffic (one curl at a time) the batch never fills, so the writer waits
the entire timeout every single time. The publish call was blocking on that wait.

**Fix / experiment:** set `BatchSize: 1` on the writer. Re-ran. Latency dropped from ~1000 ms
to **~7 ms**, and `rate(...)` reflected the change.

### The lesson — this was a tradeoff, not just a "bug"

Batching exists for **throughput**: shipping many messages per network round-trip is efficient
at high volume. Setting `BatchSize: 1` optimizes for **latency at low traffic** but gives up
that throughput efficiency at high traffic. Neither is universally "correct" — they are
different operating points.

- The production-honest fix is usually **not** `BatchSize: 1`, but lowering `BatchTimeout`
  (e.g. 10 ms) — keeping most of the batching benefit while removing the full 1-second stall
  when traffic is thin.
- And a real question: was 1 second even a *problem*? For a fire-and-forget async command that
  goes through Kafka + retries anyway, extra submit latency may not matter. If a user is waiting
  on the HTTP response, it does. **The metric reveals the behavior; requirements decide whether
  it's a problem.** Observability shows what is happening; the engineer decides if it matters.

### Why this is the whole thesis in miniature

The system was silently lying that it was fine — no errors, everything "worked" — while hiding
a 1-second stall. The latency histogram made the invisible behavior visible, which turned into
a question ("why 1s?"), which turned into an investigation, a root cause, a fix, and a confirmed
change in the metric. That loop — instrument → observe → notice → drill down → find cause → fix
→ confirm — is the core of observability engineering, and it happened on the first metric added.

### War-story summary

> "I added latency instrumentation to my Go service and immediately found a ~1-second stall on
> command submit. I traced it to the Kafka producer batching, waiting on a `BatchTimeout` that
> never filled at low traffic. I recognized it as a latency-vs-throughput tradeoff rather than a
> plain bug, and tuned the batch timeout instead of blindly disabling batching."