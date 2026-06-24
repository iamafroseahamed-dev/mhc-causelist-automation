-- Issue 3: today_matched_listings upsert fails with "Column 'organization_id' missing".
-- Add organization_id so every matched listing can be scoped to its organization
-- and remain visible only to that organization.

ALTER TABLE today_matched_listings
    ADD COLUMN IF NOT EXISTS organization_id uuid
    REFERENCES organizations(id);

-- Helps per-organization filtering / RLS lookups on matched listings.
CREATE INDEX IF NOT EXISTS idx_today_matched_listings_organization_id
    ON today_matched_listings (organization_id);
