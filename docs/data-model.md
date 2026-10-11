# Data model

PostgreSQL is the source of truth for metadata and durable invocation state. Kafka carries delivery work; it is not the authoritative execution history.

| Table | Key fields | Purpose / indexes |
|---|---|---|
| `functions` | `id`, unique `name`, `state`, `active_version_id`, timestamps | Lookup by name; state checks |
| `function_versions` | `id`, `function_id`, unique `(function_id, version_number)`, `artifact_ref`, `runtime`, `config`, `state`, checksum | Resolve active/explicit version; immutable artifact/config after activation |
| `deployments` | `id`, `function_version_id`, `status`, error, timestamps | Deployment audit and failure explanation |
| `invocations` | `id`, `function_version_id`, `mode`, `status`, attempts, deadlines, timings, result/error references | Lookup by ID; `(status, created_at)` queue/reconciliation scans; `(function_version_id, created_at)` history |
| `idempotency_keys` | function/version scope, key, payload hash, `invocation_id`, expiry | unique scope/key; prevents duplicate acceptance |
| `invocation_attempts` | `id`, `invocation_id`, attempt number, worker ID, outcome, timings | unique `(invocation_id, attempt_number)`; forensic history |
| `audit_events` | actor, action, target, request ID, timestamp | security/audit investigations |

Phase 1 creates only `functions` and its state index. Its `active_version` is fixed at `1` because Phase 1 has trusted built-in handlers rather than deployable artifacts. `function_versions`, invocation, and deployment structures arrive with their owning phase, keeping the schema and service behavior synchronized.

Important constraints: names are normalized and unique; exactly one active version is enforced transactionally; version artifact references/checksums cannot change after activation; legal lifecycle transitions are checked by the service and guarded by conditional SQL updates. Store only bounded result/error metadata in the main row; logs and oversized payloads eventually use object storage or a separate retention strategy.

Transactions cover metadata writes and status transitions. Publishing an async message cannot be atomically committed with PostgreSQL without additional machinery; use an outbox table/publisher in Phase 4, then reconcile unpublished outbox records after crashes. This deliberately yields at-least-once publication, not exactly-once execution.
