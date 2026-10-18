# Prioritized backlog

## Phase 0 — complete pending approval

- [x] Document requirements, boundaries, lifecycle states, architecture, API, data model, security, failures, and decisions.
- [x] Review and approve Phase 0 acceptance criteria before implementation.

## Phase 1 — registry and trusted HTTP invocation

- [x] Initialize Go module and minimal package layout.
- [x] Add PostgreSQL local setup and baseline migrations.
- [x] Implement bounded HTTP API, request IDs, structured logging, built-in runtime interface/handlers.
- [x] Add unit/HTTP-integration tests, sample requests, and setup instructions.
- [x] Format, compile, and run tests; run PostgreSQL-backed integration coverage.

## Phase 2–3 — isolated execution and bounded concurrency

- [x] Define a stdin/stdout container execution contract and pre-built local image policy.
- [x] Add constrained Docker execution, deadline cleanup, output limits, and runtime/function failure classification.
- [x] Add CPU, memory, process, user, filesystem, capability, and network restrictions.
- [x] Add bounded admission, concurrency limits, client cancellation, overload response, and graceful draining.
- [x] Add runtime, scheduler, race, PostgreSQL, and live Docker verification.

## Phase 4 — asynchronous delivery

- [x] Durable outbox-driven publishing; the request path never publishes directly.
- [x] Kafka publisher/consumer adapter, invocation and DLQ topics, Compose topic bootstrap.
- [x] Idempotency keys, including a dedicated `409 idempotency_conflict` for a mismatched payload.
- [x] Bounded retries with exponential backoff and full jitter; a durable DLQ outbox row on exhaustion.
- [x] Duplicate deliveries acknowledged without re-running (conditional claim transition).
- [x] Consumer and republisher cancellation during shutdown.
- [x] Focused async unit tests and opt-in PostgreSQL/Kafka integration tests.
- [ ] Per-invocation execution lease and reclaim of stale `RUNNING` rows.
- [ ] Outbox compaction / retention for historical retry rows.

## Phase 5 — immutable versions and deployment

- [x] `function_versions` table; initial version 1 created in the same transaction as the function.
- [x] Deploy inserts an immutable version and atomically moves the active pointer under a row lock.
- [x] Rollback selects a retained version.
- [x] Container images resolved to a `sha256:` content ID before activation.
- [x] Concurrent-deploy and invocation-version-pinning integration tests.
- [ ] API authentication and role-based authorization; audit events.

## Remaining phases

1. Phase 6: warm reuse, idle eviction, bounded autoscaling.
2. Phase 7: Prometheus/Grafana, runbooks, operational checks.
3. Phase 8: failure injection, reconciliation, distributed-systems hardening.
4. Phase 9: reproducible benchmarks, profiles, measured report.
5. Phase 10: Compose, CI, deployment guide, portfolio demo.

## Phase 0 acceptance criteria

- [x] Functional/non-functional requirements and scope are explicit.
- [x] Control/data-plane components, request flows, states, API, schema, risks, security, and broker trade-off are documented.
- [x] A prioritized plan maps phases to deliverables.
- [x] No application implementation or later-phase source files were created.
