# Async delivery and immutable versions (Phases 4–5)

An async request resolves the currently active version once, writes an `invocations` row and an `invocation_outbox` row in one PostgreSQL transaction, then returns `202`. Kafka receives only `{"invocation_id":"…"}`. If Kafka is temporarily unavailable, the accepted outbox row remains available for a reconciler to publish later.

The consumer atomically claims only `QUEUED` or due `RETRY_SCHEDULED` work. It persists a terminal result before committing its Kafka offset. A crash between execution and offset commit can therefore execute work again: delivery is **at least once**. Idempotency keys deduplicate acceptance, while function business logic must still make its side effects idempotent.

Each deployment inserts an immutable `function_versions` row and atomically moves `functions.active_version`. Invocation records reference that version, so a later deployment or rollback cannot change an accepted invocation. New container deployments must resolve the local image to its content ID before activation; tags are input conveniences, not the execution artifact identity.

Retry is bounded to three attempts by default (`max_attempts`). Retries use exponential delay plus full jitter and are gated by `next_attempt_at`; the database clock, not the application clock, decides when a retry is due. Exhausted records become `DEAD_LETTERED` and gain a durable `DLQ` outbox row; Kafka publication of that row is reconciled asynchronously, so `DEAD_LETTERED` means "durably recorded and queued for dead-letter publication", not "already on the DLQ topic". Replaying an idempotency key with a different payload is rejected with `409 idempotency_conflict`; replaying it with an identical payload returns the original invocation marked `duplicate:true`. Kafka consumer groups enable horizontal consumers, while PostgreSQL's conditional claim transition prevents two consumers from running the same record at once.

A consumer that dies while a record is `RUNNING` leaves no lease, so the row is not reclaimed automatically; that is documented as a known gap in [failure-modes.md](failure-modes.md).
