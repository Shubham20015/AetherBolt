CREATE TABLE functions (
    name TEXT PRIMARY KEY,
    handler TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    state TEXT NOT NULL CHECK (state IN ('ACTIVE', 'DISABLED', 'FAILED')),
    active_version INTEGER NOT NULL CHECK (active_version > 0),
    timeout_ms INTEGER NOT NULL CHECK (timeout_ms BETWEEN 1 AND 30000),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX functions_state_idx ON functions (state);
