ALTER TABLE invocations ADD COLUMN lease_expires_at TIMESTAMPTZ;

-- Reclaim scan finds crashed workers: RUNNING rows whose lease has elapsed.
CREATE INDEX invocations_lease_expires_at_idx ON invocations (lease_expires_at) WHERE status = 'RUNNING';
