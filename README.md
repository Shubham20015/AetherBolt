# AetherBolt

AetherBolt is a learning-focused, self-hosted serverless platform inspired by AWS Lambda. It exposes a control plane for function metadata and deployments, and a data plane that accepts invocations and dispatches them to isolated runtimes. It is deliberately **not** an AWS-compatible implementation.

## Current status

Phases 1–3 implement a Go HTTP service, PostgreSQL function registry, trusted built-in handlers, constrained Docker execution of pre-built local images, and bounded synchronous scheduling. Asynchronous delivery, deployment/version APIs, warm container reuse, and autoscaling remain deferred.

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
docker compose up -d postgres
psql "postgres://localuser:localpassword@localhost:5432/aetherbolt?sslmode=disable" -f migrations/000001_create_functions.up.sql
psql "postgres://localuser:localpassword@localhost:5432/aetherbolt?sslmode=disable" -f migrations/000002_add_execution_config.up.sql
go mod tidy
AETHERBOLT_DATABASE_URL='postgres://localuser:localpassword@localhost:5432/aetherbolt?sslmode=disable' go run ./cmd/aetherbolt
```

In another terminal:

```sh
curl -sS -X POST http://localhost:8080/v1/functions -H 'Content-Type: application/json' -d '{"name":"upper","handler":"uppercase"}'
curl -sS -X POST http://localhost:8080/v1/functions/upper:invoke -H 'Content-Type: application/json' -d '{"event":{"text":"hello"}}'
go test ./...
go test -race ./...
go vet ./...
AETHERBOLT_TEST_DATABASE_URL='postgres://localuser:localpassword@localhost:5432/aetherbolt?sslmode=disable' go test ./internal/store -run TestPostgresFunctionStore
```

The tests exercise the service and HTTP API with an in-memory repository; they do not prove PostgreSQL wiring. Add the database integration test in Phase 1 only when a local Docker daemon and Go toolchain are available.

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

The platform only accepts locally built `aetherbolt-function-*` image references in this milestone and uses `docker run --pull never`; this is intentional until Phase 5 supplies image provenance and deployment workflows. See [the execution contract and limits](docs/execution.md).

The scheduler is configured by `AETHERBOLT_MAX_CONCURRENT` (default `4`) and `AETHERBOLT_QUEUE_CAPACITY` (default `16`). Once `active + waiting` reaches their sum, synchronous invocation returns `429`; during graceful shutdown, new work returns `503` while admitted work drains.

### Phase 1 test intent

- `internal/function`: validates handler allowlisting, registration defaults, duplicate names, and invocation result propagation. These prevent invalid registry state and accidental execution of an unapproved handler.
- `internal/httpapi`: exercises the API end-to-end via `httptest`, including request ID propagation, registration, invocation, and structured `404` errors. This prevents route/serialization and public error-contract regressions.
- `internal/store` contains an opt-in PostgreSQL integration test. It verifies that the migration-backed repository creates, reads, and rejects duplicate function names, then deletes only its own test row. Run it with `AETHERBOLT_TEST_DATABASE_URL` as shown above.
- `internal/runtime` verifies the constrained Docker command, execution-envelope validation, timeout cleanup, and failure classification without requiring Docker in every unit-test run. `internal/worker` verifies bounded admission and graceful drain behavior; run both with the race detector as part of the commands above.

## Project principles

- Explicit states and bounded resources over implicit/unbounded behavior.
- `context.Context` carries cancellation and deadlines through every request path.
- Metadata lives in PostgreSQL; execution is never performed in the API process once arbitrary user code is introduced.
- At-least-once asynchronous delivery is acknowledged and made safe through idempotency guidance; exactly-once business effects are not promised.
- Tests and measured results are reported only when actually run.
