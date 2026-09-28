-- #401: Track whether a contract's spec section was fully decoded.
-- When false, interface diffs are suppressed to prevent false breaking-change
-- alerts caused by partial parses truncating entries mid-section.
ALTER TABLE contract_specs
    ADD COLUMN IF NOT EXISTS spec_complete BOOLEAN NOT NULL DEFAULT true;

ALTER TABLE contract_spec_versions
    ADD COLUMN IF NOT EXISTS spec_complete BOOLEAN NOT NULL DEFAULT true;
