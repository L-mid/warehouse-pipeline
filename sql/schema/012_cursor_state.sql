ALTER TABLE run_ledger
    ADD COLUMN IF NOT EXISTS cursor_state_json jsonb NOT NULL DEFAULT '{}'::jsonb;

COMMENT ON COLUMN run_ledger.cursor_state_json IS
    'Source-specific incremental cursor metadata such as boundary semantics, strategy name, native vs emulated incremental, and future composite cursor state.';
