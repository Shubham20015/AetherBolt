# Failure modes

| Failure | Detection | Impact | Recovery / prevention |
|---|---|---|---|
| PostgreSQL unavailable/exhausted | readiness, pool metrics, query timeouts | metadata and durable state unavailable | fail requests with `503`; bounded pools/timeouts; retries only for safe operations |
| Kafka unavailable | publish error in the republisher, consumer lag, broker health | async **processing** is delayed, not lost | acceptance is durable via the outbox, so the invocation stays `QUEUED` and the republisher retries every 2s; consumers are supervised and reconnect with backoff |
| Worker/container crash | Docker exit/error and request logs | invocation fails | classify function versus Docker startup/runtime failure; `--rm` plus cancellation cleanup prevents orphans; retry until attempts are exhausted |
| Function timeout | context deadline and container stop result | no result; resources may linger | force-remove named container, classify `TIMED_OUT`, cap timeout, and test deadline cleanup |
| Duplicate message | conditional claim transition returns `ErrNotClaimable` | none (intended) | at-least-once contract; the consumer acknowledges duplicates without re-running them |
| Idempotency-key replay | unique index on `(function, version, key)` | none (intended) when payload matches | identical payload returns the original invocation (`duplicate:true`); a different payload is a hard `409 idempotency_conflict` |
| Poison message/function bug | repeated terminal function errors | wasted capacity/backlog | bounded retries (default 3) with exponential backoff and full jitter, then `DEAD_LETTERED` and a durable `DLQ` outbox row |
| Capacity exhaustion | bounded-admission rejection | request rejected before container creation | bounded queue, `429`, later per-function fairness and autoscaling |
| Deployment partially fails | version state and artifact validation | ambiguous version availability | immutable versions, atomic active-version switch under a row lock, retain prior version for rollback |
| Shutdown/restart | drain timeout and startup reconciliation | in-flight work interrupted | `BeginDrain` makes new work `503`, then HTTP shutdown and worker context cancellation drain admitted work within the deadline; the terminal write uses a detached, bounded context so in-flight invocations are recorded (and rescheduled) rather than stranded; the republisher reconciles unpublished outbox rows and stranded leases on restart |
| Consumer crash mid-execution | a `RUNNING` row whose `lease_expires_at` elapsed | that invocation stalls until the lease expires | the reconciler (same loop as the republisher) returns expired `RUNNING` rows to `RETRY_SCHEDULED` (or `DEAD_LETTERED` when the budget is spent) and queues their outbox rows |
| Consumer returns an error / hot loop | `Consume` returns an error or the broker connection drops | async processing stops | `Consume` retries the record in place with capped backoff and never commits past a failed offset; a supervisor restarts the reader with backoff so processing recovers automatically |
| Sensitive logs/payloads | review/scanning/audit | confidentiality loss | structured allowlisted fields, redaction, access controls, retention |

## Delivery guarantees

Kafka delivery and the outbox provide **at-least-once** processing, not exactly-once execution. A crash between running a function and committing the Kafka offset can run the function again. Idempotency keys deduplicate *acceptance*; function business logic must still make its own side effects idempotent.

`DEAD_LETTERED` means the failure is durably recorded and its `DLQ` outbox row is durable. The Kafka DLQ publication happens asynchronously afterwards and is reconciled after a broker outage; it is not a synchronous guarantee that the message is already on the DLQ topic.

## Known gaps

- **Retry rows accumulate.** Each retry writes another `INVOCATION` outbox row, so a heavily retried invocation leaves several historical rows. They are all marked published and are harmless, but they are not compacted.
- **Scheduler admission is global per plane.** `429` reflects total admission capacity within the synchronous or asynchronous plane, not per-function fairness.
- **Async parallelism is bounded by partitions.** Each async consumer is a member of one group, so effective concurrency is `min(AETHERBOLT_ASYNC_CONSUMERS, topic partitions)`. Scale the topic partitions alongside the consumer count.

The platform prefers an explicit unavailable/error response over silently dropping work or reporting success before durable acceptance. Where a guarantee is not yet implemented, it is listed above rather than implied.
