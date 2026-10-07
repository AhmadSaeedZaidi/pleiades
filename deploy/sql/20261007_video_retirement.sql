-- Apply explicitly before deploying the Janitor retirement/claim changes.
-- Nullable additions preserve status, artifacts, phases and all existing rows.
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '10s';
ALTER TABLE videos ADD COLUMN IF NOT EXISTS retired_at TIMESTAMPTZ;
ALTER TABLE videos ADD COLUMN IF NOT EXISTS retirement_reason TEXT;
COMMENT ON COLUMN videos.retired_at IS
'Reversible source retirement owned by Janitor; Tracker keeps monthly rechecks. Never proves deletion.';
COMMIT;
