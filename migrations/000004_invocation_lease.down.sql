DROP INDEX invocations_lease_expires_at_idx;
ALTER TABLE invocations DROP COLUMN lease_expires_at;
