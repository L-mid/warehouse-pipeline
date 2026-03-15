-- Adds extraction-window tracking to run_ledger.
-- These columns are NULL for snapshot/live (full-pull) modes,
-- and populated for incremental runs.

ALTER TABLE run_ledger
    ADD COLUMN IF NOT EXISTS watermark_column   text,
    ADD COLUMN IF NOT EXISTS watermark_low      timestamptz,
    ADD COLUMN IF NOT EXISTS watermark_high     timestamptz;

COMMENT ON COLUMN run_ledger.watermark_column IS
    'The source column used as the incremental cursor (e.g. order_ts, updated_at).';
COMMENT ON COLUMN run_ledger.watermark_low IS
    'Inclusive lower bound of the extraction window for this run.';
COMMENT ON COLUMN run_ledger.watermark_high IS
    'Exclusive upper bound of the extraction window for this run.';


ALTER TABLE run_ledger
    DROP CONSTRAINT IF EXISTS run_ledger_mode_check;

ALTER TABLE run_ledger
    ADD CONSTRAINT run_ledger_mode_check
    CHECK (mode IN ('snapshot', 'live', 'incremental'));
