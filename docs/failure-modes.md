# Failure modes

| Failure | Detection | Impact | Recovery / prevention |
|---|---|---|---|
| PostgreSQL unavailable/exhausted | readiness, pool metrics, query timeouts | metadata and durable state unavailable | fail requests with `503`; bounded pools/timeouts; retries only for safe operations |
| Kafka unavailable | publish/consumer lag and health | async acceptance/processing unavailable | do not falsely accept; outbox/reconciliation; alert and resume consumers |
| Worker/container crash | Docker exit/error and request logs | invocation fails | classify function versus Docker startup/runtime failure; `--rm` plus cancellation cleanup prevents orphans |
| Function timeout | context deadline and container stop result | no result; resources may linger | force-remove named container, return `504`, cap timeout, and test deadline cleanup |
| Duplicate message | idempotency key/invocation record | repeated business side effect | at-least-once contract; handler idempotency guidance; conditional claim/attempt records |
| Poison message/function bug | repeated terminal function errors | wasted capacity/backlog | bounded exponential backoff with jitter, then DLQ and diagnostics |
| Capacity exhaustion | bounded-admission rejection | request rejected before container creation | bounded queue, `429`, later per-function fairness and autoscaling |
| Deployment partially fails | deployment state and artifact validation | ambiguous version availability | immutable versions, atomic active-version switch, retain prior version |
| Shutdown/restart | drain timeout and startup reconciliation | in-flight work interrupted | stop intake, drain bounded time, requeue/reconcile nonterminal records |
| Sensitive logs/payloads | review/scanning/audit | confidentiality loss | structured allowlisted fields, redaction, access controls, retention |

Every row gains a reproducible failure-injection test and operations procedure in Phases 4–8. The platform must prefer an explicit unavailable/error response over silently dropping work or reporting success before durable acceptance.
