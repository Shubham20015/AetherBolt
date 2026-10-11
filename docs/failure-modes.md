# Failure modes

| Failure | Detection | Impact | Recovery / prevention |
|---|---|---|---|
| PostgreSQL unavailable/exhausted | readiness, pool metrics, query timeouts | metadata and durable state unavailable | fail requests with `503`; bounded pools/timeouts; retries only for safe operations |
| Kafka unavailable | publish/consumer lag and health | async acceptance/processing unavailable | do not falsely accept; outbox/reconciliation; alert and resume consumers |
| Worker/container crash | exit event, deadline, missing heartbeat | invocation lost or incomplete | classify infrastructure failure, persist attempt, retry async; clean orphan containers |
| Function timeout | context deadline and container stop result | no result; resources may linger | kill/cleanup, terminal `TIMED_OUT`; cap timeout and test cleanup |
| Duplicate message | idempotency key/invocation record | repeated business side effect | at-least-once contract; handler idempotency guidance; conditional claim/attempt records |
| Poison message/function bug | repeated terminal function errors | wasted capacity/backlog | bounded exponential backoff with jitter, then DLQ and diagnostics |
| Capacity exhaustion | semaphore/queue metrics | latency or rejected work | bounded queue, `429/503`, per-function fairness, autoscaling with guardrails |
| Deployment partially fails | deployment state and artifact validation | ambiguous version availability | immutable versions, atomic active-version switch, retain prior version |
| Shutdown/restart | drain timeout and startup reconciliation | in-flight work interrupted | stop intake, drain bounded time, requeue/reconcile nonterminal records |
| Sensitive logs/payloads | review/scanning/audit | confidentiality loss | structured allowlisted fields, redaction, access controls, retention |

Every row gains a reproducible failure-injection test and operations procedure in Phases 4–8. The platform must prefer an explicit unavailable/error response over silently dropping work or reporting success before durable acceptance.
