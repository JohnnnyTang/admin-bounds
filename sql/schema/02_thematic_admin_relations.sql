-- Thematic admin relations: annotation results linking arbitrary vector features
-- to Chinese administrative boundaries.
--
-- Design: scalar fields extracted as native types (B-tree indexable);
-- array fields kept as JSONB with GIN indexes.
-- feature_uuid references the uuid column added to each source table on upload.

CREATE SCHEMA IF NOT EXISTS adminbounds;

CREATE TABLE IF NOT EXISTS adminbounds.thematic_admin_relations (
    id                BIGSERIAL PRIMARY KEY,
    source_table      TEXT NOT NULL,
    feature_uuid      UUID NOT NULL,              -- references uuid column in source table
    admin_level_match INTEGER,                     -- scalar: B-tree indexed
    confidence        FLOAT8,                      -- scalar: B-tree indexed
    coincides_with    JSONB,                       -- array: GIN indexed
    intersects_with   JSONB,                       -- array: GIN indexed
    covers_children   JSONB,                       -- array: GIN indexed
    contained_by      JSONB,                       -- array: GIN indexed
    computed_at       TIMESTAMPTZ DEFAULT now(),
    UNIQUE (source_table, feature_uuid)
);

-- B-tree indexes on scalar columns for fast filtering
CREATE INDEX IF NOT EXISTS idx_tar_source     ON adminbounds.thematic_admin_relations (source_table);
CREATE INDEX IF NOT EXISTS idx_tar_uuid       ON adminbounds.thematic_admin_relations (feature_uuid);
CREATE INDEX IF NOT EXISTS idx_tar_level      ON adminbounds.thematic_admin_relations (admin_level_match);
CREATE INDEX IF NOT EXISTS idx_tar_confidence ON adminbounds.thematic_admin_relations (confidence);

-- GIN indexes on JSONB arrays for membership queries
CREATE INDEX IF NOT EXISTS idx_tar_coincides  ON adminbounds.thematic_admin_relations USING GIN (coincides_with);
CREATE INDEX IF NOT EXISTS idx_tar_intersects ON adminbounds.thematic_admin_relations USING GIN (intersects_with);
CREATE INDEX IF NOT EXISTS idx_tar_covers     ON adminbounds.thematic_admin_relations USING GIN (covers_children);
CREATE INDEX IF NOT EXISTS idx_tar_contained  ON adminbounds.thematic_admin_relations USING GIN (contained_by);
