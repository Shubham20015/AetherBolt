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

## Subsequent priority order

1. Phase 4: Kafka async delivery, retry, DLQ, idempotency/outbox.
2. Phase 5: artifacts, immutable deployment versions, rollback, access control.
3. Phase 6: warm reuse, idle eviction, bounded autoscaling.
4. Phase 7: Prometheus/Grafana, runbooks, operational checks.
5. Phase 8: failure injection, reconciliation, distributed-systems hardening.
6. Phase 9: reproducible benchmarks, profiles, measured report.
7. Phase 10: Compose, CI, deployment guide, portfolio demo.

## Phase 0 acceptance criteria

- [x] Functional/non-functional requirements and scope are explicit.
- [x] Control/data-plane components, request flows, states, API, schema, risks, security, and broker trade-off are documented.
- [x] A prioritized plan maps phases to deliverables.
- [x] No application implementation or later-phase source files were created.
