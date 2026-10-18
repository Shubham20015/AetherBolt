CREATE TABLE function_versions (
    function_name TEXT NOT NULL REFERENCES functions(name),
    version_number INTEGER NOT NULL CHECK (version_number > 0),
    handler TEXT NOT NULL,
    runtime TEXT NOT NULL CHECK (runtime IN ('builtin', 'container')),
    image TEXT NOT NULL DEFAULT '',
    artifact_ref TEXT NOT NULL DEFAULT '',
    timeout_ms INTEGER NOT NULL CHECK (timeout_ms BETWEEN 1 AND 30000),
    memory_mb INTEGER NOT NULL CHECK (memory_mb BETWEEN 64 AND 1024),
    cpu_millis INTEGER NOT NULL CHECK (cpu_millis BETWEEN 50 AND 1000),
    status TEXT NOT NULL CHECK (status IN ('ACTIVE', 'RETIRED', 'FAILED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (function_name, version_number)
);

INSERT INTO function_versions (function_name, version_number, handler, runtime, image, timeout_ms, memory_mb, cpu_millis, status)
SELECT name, active_version, handler, runtime, image, timeout_ms, memory_mb, cpu_millis, 'ACTIVE'
FROM functions;

CREATE TABLE invocations (
    id TEXT PRIMARY KEY,
    function_name TEXT NOT NULL,
    version_number INTEGER NOT NULL,
    event JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    idempotency_key TEXT,
    mode TEXT NOT NULL CHECK (mode IN ('SYNC', 'ASYNC')),
    status TEXT NOT NULL CHECK (status IN ('QUEUED', 'RUNNING', 'SUCCEEDED', 'FUNCTION_ERROR', 'TIMED_OUT', 'INFRASTRUCTURE_ERROR', 'RETRY_SCHEDULED', 'DEAD_LETTERED')),
    attempt_count INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3 CHECK (max_attempts BETWEEN 1 AND 10),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    result JSONB,
    error_code TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (function_name, version_number) REFERENCES function_versions(function_name, version_number)
);
CREATE UNIQUE INDEX invocations_idempotency_idx ON invocations (function_name, version_number, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX invocations_claim_idx ON invocations (status, next_attempt_at);

CREATE TABLE invocation_outbox (
    id BIGSERIAL PRIMARY KEY,
    invocation_id TEXT NOT NULL REFERENCES invocations(id),
    topic TEXT NOT NULL DEFAULT 'INVOCATION' CHECK (topic IN ('INVOCATION', 'DLQ')),
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX invocation_outbox_unpublished_idx ON invocation_outbox (created_at) WHERE published_at IS NULL;
