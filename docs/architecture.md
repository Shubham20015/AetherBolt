# Architecture

## Objective and requirements

### Functional requirements

1. Register functions, deploy immutable versions, select/roll back an active version, and disable a function.
2. Invoke an active or explicitly selected version synchronously with a JSON event and JSON response.
3. Accept asynchronous invocations, retry transient failures, and send exhausted messages to a dead-letter stream.
4. Execute untrusted function artifacts outside the control-plane process with configured timeout and resource limits.
5. Persist function/deployment metadata and invocation history; expose logs and operational metrics.
6. Bound per-function and global concurrency, support warm runtime reuse where safe, and shut down gracefully.

### Non-functional requirements

- Correctness: explicit state transitions, idempotent write APIs, durable invocation records, and no silent version mutation.
- Reliability: bounded queues/retries/resources, restart recovery, useful failure classification, and graceful dependency degradation.
- Security: authenticated control plane, authorization, input-size limits, secret redaction, least-privilege containers, and artifact provenance.
- Operability: JSON structured logs, correlation IDs, Prometheus metrics, health/readiness checks, dashboards, and runbooks.
- Performance: measure actual p50/p95/p99, cold-start versus handler time, queue wait, worker utilization, and resource use.

## Boundaries: control plane and data plane

The **control plane** owns metadata: functions, versions, policies, deployment state, and authorization. The **data plane** accepts and schedules invocation work, records outcomes, and eventually asks an isolated runtime to execute. Keeping execution out of the API/control-plane process limits blast radius and permits independent scaling.

```mermaid
flowchart LR
  Dev[Developer / client] --> API[HTTP API]
  API --> Auth[Authentication & authorization]
  Auth --> CP[Control-plane service]
  CP --> PG[(PostgreSQL)]
  API --> Sync[Sync invocation coordinator]
  API --> Async[Async producer]
  Async --> Kafka[(Kafka)]
  Kafka --> Consumer[Invocation consumer group]
  Sync --> Scheduler[Bounded scheduler]
  Consumer --> Scheduler
  Scheduler --> Workers[Runtime / container workers]
  Workers --> Logs[Structured logs]
  Workers --> PG
  API --> Metrics[Prometheus metrics]
  Workers --> Metrics
  Metrics --> Prom[Prometheus]
  Prom --> Grafana[Grafana]
```

In Phase 1, `Runtime / container workers` is temporarily represented by trusted built-in Go handlers behind the same runtime interface. PostgreSQL is introduced for the registry, but no user artifact executes yet.

## Invocation lifecycle

Synchronous request flow:

```mermaid
sequenceDiagram
  participant C as Client
  participant A as API
  participant R as Registry
  participant S as Scheduler
  participant X as Runtime
  participant D as Database
  C->>A: POST /v1/functions/{name}:invoke (JSON)
  A->>A: limit body, validate, assign request ID
  A->>R: resolve ACTIVE function version
  R-->>A: immutable version configuration
  A->>D: create RUNNING invocation record
  A->>S: acquire bounded capacity with deadline
  S->>X: invoke(event, context)
  X-->>S: result or classified error
  S->>D: persist terminal outcome and timings
  A-->>C: JSON result or structured error
```

Async flow differs after validation: the API writes an `invocations` row in `QUEUED` and an `invocation_outbox` row in one transaction, then returns `202` without touching the broker. A republisher publishes `{"invocation_id":...}` and marks the outbox row published; a durable consumer claims the record, runs it, stores the terminal outcome, and commits its Kafka offset only after that durable transition. Publication, execution, and offset commit are separate, so delivery and processing are at-least-once.

## State machines

Function lifecycle (implemented states): `ACTIVE`, `DISABLED`, `FAILED`. A function is `ACTIVE` on registration; `DISABLED` and `FAILED` exist in the schema and reject new invocations. Multi-step deploy states such as `CREATED`/`DEPLOYING` are not modelled; a deployment moves the active version atomically instead.

Version lifecycle: each version is `ACTIVE`, `RETIRED`, or `FAILED`. A deployment retires the current `ACTIVE` row and inserts a new `ACTIVE` row in one transaction; a rollback re-activates a retained version. Retired versions stay immutable and addressable.

Invocation lifecycle: `QUEUED -> RUNNING -> SUCCEEDED | FUNCTION_ERROR | TIMED_OUT | INFRASTRUCTURE_ERROR`, with `RETRY_SCHEDULED` (returning to a due time) for non-terminal failures and `DEAD_LETTERED` once attempts are exhausted. A distinct `ACCEPTED` state and `CANCELLED` are not currently produced.

## Execution and worker lifecycle

An execution environment is a process/container initialized with a function artifact and runtime adapter. Initialization includes image/container start and runtime handshake; handler execution begins only after initialization. A worker moves `PROVISIONING -> IDLE -> BUSY -> IDLE`, then `DRAINING -> TERMINATED`; unrecoverable startup or process failure yields `FAILED`. The scheduler owns these transitions, applies global/per-function semaphores, and never creates unbounded goroutines or containers.

Docker provides process/resource isolation, but it is weaker than a purpose-built sandbox. Containers run non-root, without privileged mode or Docker socket access, with restricted capabilities, read-only root filesystem where practical, memory/CPU/pid limits, and limited networking. Stronger isolation (microVMs/gVisor) is deferred and documented as an option, not claimed.

## Package boundaries (target structure)

```
cmd/aetherbolt/       composition root; config and HTTP/worker lifecycle
internal/httpapi/     routing, decoding, response/error mapping, request IDs
internal/function/    function/version domain types and service
internal/async/       invocation lifecycle, outbox, retry/DLQ, claiming
internal/runtime/     Runtime interface and constrained Docker implementation
internal/worker/      bounded admission, concurrency, graceful drain
internal/store/       function/version PostgreSQL repository
internal/queue/       publisher/consumer interfaces and Kafka adapter
migrations/           ordered SQL migrations
```

The HTTP layer must depend on application services, not SQL or Docker directly. Runtime and queue adapters implement small domain-owned interfaces (`function.Executor`, `async.Store`, `async.Publisher`), so built-in handlers can be replaced without changing endpoint semantics. Later phases add `internal/observability/` and `internal/auth/` boundaries.

## Scaling and known limits

Initial scaling is single-host and deliberately bounded by configured global/per-function concurrency and queue capacity. Signals for later autoscaling are queue depth, active workers, latency, and idle time; hysteresis/cooldowns prevent oscillation. PostgreSQL invocation history will become a write bottleneck at high rates, and a single Kafka/Docker host is not highly available. Those constraints are acceptable for a local portfolio platform and must be visible in operations documentation.
