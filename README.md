# AetherBolt

AetherBolt is a learning-focused, self-hosted serverless platform inspired by AWS Lambda. It exposes a control plane for function metadata and deployments, and a data plane that accepts invocations and dispatches them to isolated runtimes. It is deliberately **not** an AWS-compatible implementation.

## Current status

Phases 1–5 implement a Go HTTP service, PostgreSQL function registry and immutable versions, trusted built-in handlers, constrained Docker execution, bounded synchronous scheduling, and Kafka-backed asynchronous delivery. Warm container reuse and autoscaling remain deferred.

## Scope

In scope over the project:

- Function registration, immutable versions, synchronous and asynchronous invocation.
- Docker-based execution with bounded concurrency, timeouts, observability, and local Docker Compose infrastructure.
- PostgreSQL metadata, Kafka delivery, Prometheus metrics, Grafana dashboards, tests, and a local-first deployment story.

Out of scope:

- AWS API compatibility, multi-region replication, billing, public multi-tenant hosting, arbitrary language support at launch, or a claim of hardened sandbox security.

## Design documents

- [Architecture](docs/architecture.md)
- [API contract](docs/api.md)
- [Data model](docs/data-model.md)
- [Security model](docs/security.md)
- [Failure modes](docs/failure-modes.md)
- [Decisions](docs/decisions/)
- [Prioritized backlog](TODO.md)

## Planned local prerequisites

Go, Docker/Compose, PostgreSQL, Kafka, Prometheus, and Grafana. They will be introduced only in the phase where they are required.

## Phase 1 quick start

Requirements: Go 1.26+ and Docker Compose. Kafka is not needed until Phase 4.

```sh
docker compose up -d postgres kafka kafka-init
psql "postgres://localuser:localpassword@localhost:5432/aetherbolt?sslmode=disable" -f migrations/000001_create_functions.up.sql
psql "postgres://localuser:localpassword@localhost:5432/aetherbolt?sslmode=disable" -f migrations/000002_add_execution_config.up.sql
psql "postgres://localuser:localpassword@localhost:5432/aetherbolt?sslmode=disable" -f migrations/000003_versions_and_async.up.sql
go mod tidy
AETHERBOLT_DATABASE_URL='postgres://localuser:localpassword@localhost:5432/aetherbolt?sslmode=disable' go run ./cmd/aetherbolt
```

In another terminal:

```sh
# synchronous
curl -sS -X POST http://localhost:8080/v1/functions -H 'Content-Type: application/json' -d '{"name":"upper","handler":"uppercase"}'
curl -sS -X POST http://localhost:8080/v1/functions/upper:invoke -H 'Content-Type: application/json' -d '{"event":{"text":"hello"}}'

# asynchronous (durable accept, then poll)
curl -sS -X POST http://localhost:8080/v1/functions/upper:invoke-async -H 'Content-Type: application/json' -d '{"event":{"text":"hello"},"idempotency_key":"demo-1"}'
curl -sS http://localhost:8080/v1/invocations/<invocation_id>

# deploy an immutable version, then roll back
curl -sS -X POST http://localhost:8080/v1/functions/upper/versions -H 'Content-Type: application/json' -d '{"handler":"uppercase"}'
curl -sS -X POST http://localhost:8080/v1/functions/upper/rollback -H 'Content-Type: application/json' -d '{"version":1}'
```

## Tests

```sh
go test ./...
go test -race ./...
go vet ./...

# PostgreSQL integration tests (schema must be applied)
AETHERBOLT_TEST_DATABASE_URL='postgres://localuser:localpassword@localhost:5432/aetherbolt?sslmode=disable' go test ./internal/store ./internal/async

# Kafka end-to-end test (needs a running broker)
AETHERBOLT_TEST_KAFKA_BROKER=localhost:9092 go test ./internal/queue
```

Without the environment variables, the integration tests skip and only the pure unit tests run.

## Container function and bounded scheduling

Build the sample function image before registering a container function:

```sh
docker build -t aetherbolt-function-echo:dev examples/functions/echo
curl -sS -X POST http://localhost:8080/v1/functions \
  -H 'Content-Type: application/json' \
  -d '{"name":"container-echo","handler":"stdin-json","runtime":"container","image":"aetherbolt-function-echo:dev","timeout_ms":3000,"memory_mb":128,"cpu_millis":250}'
curl -sS -X POST http://localhost:8080/v1/functions/container-echo:invoke \
  -H 'Content-Type: application/json' \
  -d '{"event":{"message":"hello from a container"}}'
```

The platform only accepts locally built `aetherbolt-function-*` image references and uses `docker run --pull never`. Registering or deploying a container function resolves the image to its `sha256:` content ID (`docker image inspect`) and stores that digest as the immutable artifact reference, so a later re-tag or rebuild cannot silently change what a deployed version runs. Remote registries, signatures, and image upload remain future work. See [the execution contract and limits](docs/execution.md).

The scheduler is configured by `AETHERBOLT_MAX_CONCURRENT` (default `4`) and `AETHERBOLT_QUEUE_CAPACITY` (default `16`). Once `active + waiting` reaches their sum, synchronous invocation returns `429`; during graceful shutdown, new work returns `503` while admitted work drains.

The async data plane is configured by `AETHERBOLT_KAFKA_BROKER` (default `localhost:9092`) and `AETHERBOLT_KAFKA_TOPIC` (default `aetherbolt.invocations`); the dead-letter topic is derived as `<topic>.dlq`. An accepted async invocation is written to an outbox in the same transaction as its row, so acceptance succeeds even while the broker is down; a republisher reconciles unpublished records (including DLQ records) after the broker returns.

### Test intent

- `internal/function`: validates handler allowlisting, registration defaults, duplicate names, and invocation result propagation. These prevent invalid registry state and accidental execution of an unapproved handler.
- `internal/httpapi`: exercises the API end-to-end via `httptest`, including request ID propagation, registration, invocation, and structured `404` errors. This prevents route/serialization and public error-contract regressions.
- `internal/runtime` verifies the constrained Docker command, execution-envelope validation, timeout cleanup, and failure classification without requiring Docker in every unit-test run. `internal/worker` verifies bounded admission and graceful drain behavior.
- `internal/async` has unit tests (duplicate acknowledgement, retry delay bounds/jitter, outbox reconciliation after a broker outage, deadline classification) and opt-in PostgreSQL integration tests (claim/complete lifecycle, idempotency dedupe and conflict, retry-then-dead-letter with DLQ outbox row, claim guards, single-winner concurrent claim, and version pinning after a deploy).
- `internal/store` has an opt-in PostgreSQL integration test for create/read/duplicate-name, plus versioning tests for deploy, rollback, and concurrent deployment (unique version allocation with exactly one active version).
- `internal/queue` has an opt-in Kafka end-to-end test covering publish/consume round-trip, DLQ topic routing, and clean consumer shutdown on cancellation.

Run the unit suites normally; run the integration suites with the environment variables shown under [Tests](#tests). Race-check the scheduler, async, and store suites because they exercise concurrent claim/admission paths.

## Project principles

- Explicit states and bounded resources over implicit/unbounded behavior.
- `context.Context` carries cancellation and deadlines through every request path.
- Metadata lives in PostgreSQL; execution is never performed in the API process once arbitrary user code is introduced.
- At-least-once asynchronous delivery is acknowledged and made safe through idempotency guidance; exactly-once business effects are not promised.
- Tests and measured results are reported only when actually run.
