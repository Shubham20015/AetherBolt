# Failure modes

| Failure | Detection | Impact | Recovery / prevention |
|---|---|---|---|
| PostgreSQL unavailable/exhausted | readiness, pool metrics, query timeouts | metadata and durable state unavailable | fail requests with `503`; bounded pools/timeouts; retries only for safe operations |
| Kafka unavailable | publish error in the republisher, consumer lag, broker health | async **processing** is delayed, not lost | acceptance is durable via the outbox, so the invocation stays `QUEUED` and the republisher retries every 2s; alert and resume consumers |
| Worker/container crash | Docker exit/error and request logs | invocation fails | classify function versus Docker startup/runtime failure; `--rm` plus cancellation cleanup prevents orphans; retry until attempts are exhausted |
| Function timeout | context deadline and container stop result | no result; resources may linger | force-remove named container, classify `TIMED_OUT`, cap timeout, and test deadline cleanup |
| Duplicate message | conditional claim transition returns `ErrNotClaimable` | none (intended) | at-least-once contract; the consumer acknowledges duplicates without re-running them |
| Idempotency-key replay | unique index on `(function, version, key)` | none (intended) when payload matches | identical payload returns the original invocation (`duplicate:true`); a different payload is a hard `409 idempotency_conflict` |
| Poison message/function bug | repeated terminal function errors | wasted capacity/backlog | bounded retries (default 3) with exponential backoff and full jitter, then `DEAD_LETTERED` and a durable `DLQ` outbox row |
| Capacity exhaustion | bounded-admission rejection | request rejected before container creation | bounded queue, `429`, later per-function fairness and autoscaling |
| Deployment partially fails | version state and artifact validation | ambiguous version availability | immutable versions, atomic active-version switch under a row lock, retain prior version for rollback |
| Shutdown/restart | drain timeout and startup reconciliation | in-flight work interrupted | `BeginDrain` makes new work `503`, then HTTP shutdown and worker context cancellation drain admitted work within the deadline; the republisher reconciles unpublished outbox rows on restart |
| Consumer crash mid-execution | a row left in `RUNNING` | that invocation stalls | **not yet handled automatically**: there is no lease/visibility timeout, so an operator (or a later reconciliation phase) must reset the row. See "Known gaps". |
| Sensitive logs/payloads | review/scanning/audit | confidentiality loss | structured allowlisted fields, redaction, access controls, retention |

## Delivery guarantees

Kafka delivery and the outbox provide **at-least-once** processing, not exactly-once execution. A crash between running a function and committing the Kafka offset can run the function again. Idempotency keys deduplicate *acceptance*; function business logic must still make its own side effects idempotent.

`DEAD_LETTERED` means the failure is durably recorded and its `DLQ` outbox row is durable. The Kafka DLQ publication happens asynchronously afterwards and is reconciled after a broker outage; it is not a synchronous guarantee that the message is already on the DLQ topic.

## Known gaps

- **No execution lease.** A consumer that dies after claiming a row leaves it `RUNNING` forever. A visibility-timeout / lease-reclaim mechanism is future work.
- **Retry rows accumulate.** Each retry writes another `INVOCATION` outbox row, so a heavily retried invocation leaves several historical rows. They are all marked published and are harmless, but they are not compacted.
- **Scheduler admission is global.** `429` reflects total admission capacity, not per-function fairness.

The platform prefers an explicit unavailable/error response over silently dropping work or reporting success before durable acceptance. Where a guarantee is not yet implemented, it is listed above rather than implied.
