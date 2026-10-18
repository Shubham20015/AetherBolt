# Data model

PostgreSQL is the source of truth for metadata and durable invocation state. Kafka carries delivery work; it is not the authoritative execution history.

## Implemented tables

| Table | Key fields | Purpose / indexes |
|---|---|---|
| `functions` | `name` (PK), `state`, `active_version`, `handler`, `runtime`, `image`, `timeout_ms`, `memory_mb`, `cpu_millis`, `created_at` | Current pointer and configuration for a function; index on `state`. `active_version` always references a row in `function_versions`. |
| `function_versions` | `(function_name, version_number)` (PK), `handler`, `runtime`, `image`, `artifact_ref`, `timeout_ms`, `memory_mb`, `cpu_millis`, `status` (`ACTIVE`/`RETIRED`/`FAILED`), `created_at` | Immutable, addressable version history. A deployment inserts a row and atomically moves `functions.active_version`; the previous active row becomes `RETIRED`. Container images are stored resolved to their `sha256:` content ID in `artifact_ref`. |
| `invocations` | `id` (PK), `function_name`, `version_number`, `event` (JSONB), `payload_hash`, `idempotency_key`, `mode`, `status`, `attempt_count`, `max_attempts`, `next_attempt_at`, `lease_expires_at`, `result` (JSONB), `error_code`, timestamps | Durable asynchronous invocation state. Partial unique index on `(function_name, version_number, idempotency_key) WHERE idempotency_key IS NOT NULL` deduplicates acceptance; index on `(status, next_attempt_at)` drives the claim scan; partial index on `(lease_expires_at) WHERE status='RUNNING'` drives crash recovery. `(function_name, version_number)` references `function_versions`, so an accepted invocation is pinned to the version it was accepted against. |
| `invocation_outbox` | `id` (BIGSERIAL PK), `invocation_id`, `topic` (`INVOCATION`/`DLQ`), `published_at`, `created_at` | Transactional outbox. The `INVOCATION` row is written in the same transaction that accepts the invocation; retry rows are added while `RETRY_SCHEDULED`; a `DLQ` row is added when the record is dead-lettered. Partial index on `created_at WHERE published_at IS NULL` finds unpublished work. |

Only asynchronous invocations are persisted. Synchronous invocations execute inline and return their result without writing an `invocations` row; the schema's `mode` column and `SYNC` value are reserved for a later durable sync-history phase.

`function_versions.artifact_ref` is what execution should reference. For built-in functions it is empty; for container functions it holds the resolved content ID, so a moved or re-tagged image cannot silently change what a deployed version runs.

## Invocation lifecycle

An async request resolves the active version once, then writes the `invocations` row and its `INVOCATION` outbox row in one transaction. Status transitions:

```
QUEUED ──claim──▶ RUNNING ──success──▶ SUCCEEDED
                     │
                     ├──failure, attempts remain──▶ RETRY_SCHEDULED ──(backoff)──▶ QUEUED
                     └──failure, attempts exhausted──▶ DEAD_LETTERED
```

Terminal outcomes record the failure classification in `error_code`: `FUNCTION_ERROR`, `TIMED_OUT`, or `INFRASTRUCTURE_ERROR`. `attempt_count` increments on each claim; `next_attempt_at` gates when a retry becomes claimable. Scheduler failures (`ErrCapacity`, `ErrDraining`) and shutdown cancellation classify as `INFRASTRUCTURE_ERROR` and do **not** consume the retry budget, so local pressure never dead-letters otherwise-valid work.

A `RUNNING` row holds a lease (`lease_expires_at`, set at claim time). If a worker dies before writing a terminal state, the reconciler claims any `RUNNING` row whose lease elapsed and atomically returns it to `RETRY_SCHEDULED` — or to `DEAD_LETTERED` when the retry budget is already spent — together with the matching outbox row.

## Transactions and consistency

Transactions cover metadata writes and status transitions. The version pointer (`functions.active_version`) and the immutable `function_versions` insert commit together, so a concurrent deployment and invocation can never observe a half-applied version. The claim transition is a single conditional `UPDATE ... WHERE status IN ('QUEUED','RETRY_SCHEDULED') AND next_attempt_at <= now()` guarded by a row lock, so two consumers cannot run the same record and the database clock alone decides retry due times.

Kafka publication cannot be committed atomically with PostgreSQL, so it is deferred to the outbox: the republisher publishes an outbox row, then marks it `published_at`. A crash between publish and mark causes a duplicate publication, which is safe because consumers are idempotent. This deliberately yields at-least-once publication, not exactly-once execution.

## DLQ durability contract

An invocation is marked `DEAD_LETTERED` once its `DLQ` outbox row is durable. Kafka publication of that row happens asynchronously afterwards; if the broker is unavailable, the unpublished outbox row survives and reconciliation publishes it after the outage. `DEAD_LETTERED` therefore means "durably recorded and queued for dead-letter publication", not "already delivered to the DLQ topic".

## Deferred structures

The following remain design-only and are not created by the current migrations: a separate `deployments` audit table, an `invocation_attempts` forensics table, an `audit_events` table, and object storage for oversized payloads or logs. Current migrations stop at `000004_invocation_lease`.
