-- Enable PostGIS if not already active
CREATE EXTENSION IF NOT EXISTS postgis;

-- Create schema
CREATE SCHEMA IF NOT EXISTS adminbounds;

-- Admin units table: stores administrative boundaries at 4 levels
-- Level encoding: 1=country, 2=province, 3=city, 4=district (GADM levels 0–3 shifted by 1)

CREATE TABLE IF NOT EXISTS adminbounds.admin_units (
    id           SERIAL PRIMARY KEY,
    adcode       TEXT UNIQUE NOT NULL,
    name         TEXT,
    level        INTEGER NOT NULL CHECK (level BETWEEN 1 AND 4),
    parent_code  TEXT,                 -- NULL for level=1 (country)
    geom         GEOMETRY(GEOMETRY, 4326) NOT NULL,
    geom_bbox    GEOMETRY(POLYGON, 4326),
    geom_hull    GEOMETRY(POLYGON, 4326),
    geom_simple  GEOMETRY(GEOMETRY, 4326),
    centroid     GEOMETRY(POINT, 4326),
    area_m2      FLOAT8,
    vertex_count INTEGER
);

-- GIST indexes for spatial queries on all geometry columns
CREATE INDEX IF NOT EXISTS idx_admin_units_geom        ON adminbounds.admin_units USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_admin_units_geom_bbox   ON adminbounds.admin_units USING GIST (geom_bbox);
CREATE INDEX IF NOT EXISTS idx_admin_units_geom_hull   ON adminbounds.admin_units USING GIST (geom_hull);
CREATE INDEX IF NOT EXISTS idx_admin_units_geom_simple ON adminbounds.admin_units USING GIST (geom_simple);
CREATE INDEX IF NOT EXISTS idx_admin_units_centroid    ON adminbounds.admin_units USING GIST (centroid);

-- B-tree indexes for hierarchical queries
CREATE INDEX IF NOT EXISTS idx_admin_units_level       ON adminbounds.admin_units (level);
CREATE INDEX IF NOT EXISTS idx_admin_units_parent_code ON adminbounds.admin_units (parent_code);
