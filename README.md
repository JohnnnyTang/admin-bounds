# adminbounds

Geospatial admin-unit semantic relation inference system for worldwide administrative boundaries.

Given any vector geometry, the system infers **how it relates to an administrative hierarchy** — whether it coincides with a known boundary, intersects multiple units, contains child units, or sits inside a parent region. Results are stored as structured JSONB annotations and are queryable at scale via PostGIS.

Bundled data covers China's four-level hierarchy. Additional countries can be downloaded on demand via GADM 4.1.

---

## What It Does

The core is a PostGIS function `infer_admin_semantic_relation(geom)` that classifies a geometry into four relationship types:

| Relationship | Meaning | Example |
|---|---|---|
| `coincides_with` | Substantially overlaps a known boundary (IoU ≥ 0.85) | A polygon matching Beijing municipality exactly |
| `intersects_with` | Partially overlaps units at the dominant level | A corridor crossing Nanjing and Suzhou |
| `covers_children` | The geometry contains child-level units | A province polygon covering its cities |
| `contained_by` | The ancestor chain above the matched unit | A city → its province → country |

The function returns a single JSONB blob with all four arrays plus a scalar `admin_level_match` and `confidence` score. A Python batch script stores results in a `thematic_admin_relations` table, linking any source feature table to its administrative context.

---

## Project Structure

```
adminbounds/
├── src/adminbounds/
│   ├── _import.py              # DDL deploy + bundled boundary import pipeline
│   ├── _gadm.py                # GADM 4.1 worldwide download + import
│   ├── _annotate.py            # Batch annotation logic
│   ├── _upload.py              # GeoJSON → PostGIS upload helper
│   ├── _diagnose.py            # Annotation diagnostic checks
│   ├── client.py               # AdminBoundsClient high-level Python API
│   ├── config.py               # Pydantic settings (ADMINBOUNDS_DB_* env vars)
│   ├── db.py                   # SQLAlchemy engine + raw psycopg2 connection
│   ├── cli/__init__.py         # CLI entry point (adminbounds command)
│   └── sql/
│       ├── schema/
│       │   ├── 01_admin_units.sql
│       │   └── 02_thematic_admin_relations.sql
│       └── functions/
│           └── infer_admin_semantic_relation.sql
├── sql/                        # Source copies of the SQL files (mirrors src/adminbounds/sql/)
├── validation/
│   └── sample_queries.sql      # Post-import validation and smoke tests
├── .env.example
└── pyproject.toml
```

---

## Prerequisites

- Python 3.12+
- [uv](https://github.com/astral-sh/uv) package manager
- PostgreSQL 14+ with PostGIS 3.x extension enabled on the target database

---

## Setup

**1. Install dependencies**

```bash
uv sync
```

**2. Configure environment**

```bash
cp .env.example .env
```

Edit `.env` with your database credentials:

```dotenv
ADMINBOUNDS_DB_HOST=localhost
ADMINBOUNDS_DB_PORT=5432
ADMINBOUNDS_DB_NAME=your_database
ADMINBOUNDS_DB_USER=your_username
ADMINBOUNDS_DB_PASSWORD=your_password_here
ADMINBOUNDS_DB_SCHEMA=adminbounds
```

**3. Ensure PostGIS is enabled**

```sql
CREATE EXTENSION IF NOT EXISTS postgis;
```

---

## Usage

### Initialize the database

Creates the `adminbounds` schema, tables, and deploys the inference function. Safe to re-run — also applies any pending schema migrations (e.g. widening `adcode` from `VARCHAR(6)` to `TEXT` for GADM compatibility).

```bash
adminbounds init-db
```

### Import bundled Chinese boundaries

```bash
adminbounds import-boundaries
```

Loads four GeoJSON files into `admin_units`, computes derived geometry columns (bbox, convex hull, simplified geometry, centroid, area), and deploys the inference function. Idempotent — re-running updates existing rows.

### Download GADM worldwide boundaries

```bash
adminbounds download-gadm Germany
adminbounds download-gadm DEU                    # same — ISO3 code accepted
adminbounds download-gadm USA --levels 0,1       # country + state only (level 2+ is large)
adminbounds download-gadm France --force         # re-download even if cached
adminbounds download-gadm Japan --cache-dir /tmp/gadm
```

Downloads GADM 4.1 GeoJSON zips from the UC Davis CDN, extracts, maps to the `admin_units` schema, and upserts. Files are cached in `~/.adminbounds/gadm_cache/` by default.

**GADM level → DB level mapping:**

| GADM level | Meaning | DB `level` value |
|---|---|---|
| 0 | Country | 1 |
| 1 | State / Province | 2 |
| 2 | County / City | 3 |
| 3 | Municipality / District | 4 |

**GADM field → `admin_units` column mapping:**

| `admin_units` column | GADM level 0 | GADM level 1 | GADM level 2 | GADM level 3 |
|---|---|---|---|---|
| `adcode` | `GID_0` | `GID_1` | `GID_2` | `GID_3` |
| `name` | `NAME_0` | `NAME_1` | `NAME_2` | `NAME_3` |
| `level` | `1` | `2` | `3` | `4` |
| `parent_code` | `NULL` | `GID_0` | `GID_1` | `GID_2` |
| `geom` | geometry | geometry | geometry | geometry |

GADM GIDs look like `DEU`, `DEU.1_1`, `DEU.1.2_1` — the `adcode` column is `TEXT` (not `VARCHAR`) to accommodate these.

### Upload a GeoJSON file

```bash
adminbounds upload path/to/file.geojson my_table
adminbounds upload path/to/file.geojson my_table --if-exists append
```

### Annotate a thematic table

```bash
adminbounds annotate --source-table sample_pois --geom-col geom
adminbounds annotate --source-table myschema.sample_pois --geom-col geom   # schema-qualified
adminbounds annotate --source-table sample_pois --geom-col geom --batch-size 200
```

`--source-table` accepts a plain table name (uses `public` schema by default) or a schema-qualified name (`myschema.mytable`). When schema-qualified, `--schema` is ignored. Resume-safe: only processes rows not yet present in `thematic_admin_relations`.

### Diagnose annotation issues

```bash
adminbounds diagnose --source-table sample_pois --geom-col geom
adminbounds diagnose --source-table myschema.sample_pois --geom-col geom
```

### Python API

```python
from adminbounds import AdminBoundsClient

c = AdminBoundsClient(dbname="geo_prism", user="postgres", password="...")

# Setup
c.init_db()
c.import_boundaries()            # bundled China data

# GADM worldwide
c.download_gadm("Germany")       # all 4 levels
c.download_gadm("DEU")           # same via ISO3 code
c.download_gadm("USA", levels=[0, 1])   # country + state only

# Inference
from shapely.geometry import box
result = c.infer(box(116.3, 39.8, 116.5, 40.0))
print(result["coincides_with"])

# Batch annotation
c.annotate("sample_pois", geom_col="geom")
```

All CLI connection flags (`--host`, `--port`, `--dbname`, `--user`, `--password`) fall back to `GEO_ADMIN_DB_*` environment variables.

---

## Database Schema

### `admin_units`

Stores administrative boundaries at four levels (1=country, 2=province/state, 3=city/county, 4=district/municipality). Supports both Chinese numeric adcodes (`100000`) and GADM GIDs (`DEU.1_1`).

| Column | Type | Description |
|---|---|---|
| `adcode` | TEXT | Unique admin code — 6-digit numeric for China, GADM GID for other countries |
| `name` | TEXT | Place name |
| `level` | INTEGER | 1=country, 2=province, 3=city, 4=district |
| `parent_code` | TEXT | Parent `adcode` (NULL for level=1) |
| `geom` | GEOMETRY | Full boundary polygon |
| `geom_bbox` | GEOMETRY | Bounding box (fast coarse filter) |
| `geom_hull` | GEOMETRY | Convex hull (medium filter) |
| `geom_simple` | GEOMETRY | Simplified geometry for complex polygons |
| `centroid` | GEOMETRY | Centroid point |
| `area_m2` | FLOAT8 | Area in square metres |
| `vertex_count` | INTEGER | Vertex count (drives simplification choice) |

### `thematic_admin_relations`

Stores per-feature annotation results linking any source table to its administrative context.

| Column | Type | Description |
|---|---|---|
| `source_table` | TEXT | Name of the annotated table |
| `geom_hash` | TEXT | MD5 of `ST_AsEWKB(geom)` — deduplication key |
| `admin_level_match` | INTEGER | Dominant admin level of the match |
| `confidence` | FLOAT8 | 0–1 score |
| `coincides_with` | JSONB | Array of coinciding units |
| `intersects_with` | JSONB | Array of intersecting units |
| `covers_children` | JSONB | Array of child units covered |
| `contained_by` | JSONB | Ancestor chain |

---

## Inference Function

```sql
SELECT adminbounds.infer_admin_semantic_relation(ST_GeomFromText('POLYGON(...)', 4326));
```

**Example output (Chinese boundary):**

```json
{
  "coincides_with":    [{"code": "110000", "name": "北京市", "level": 2, "similarity": 0.9731}],
  "intersects_with":   [],
  "covers_children":   [{"code": "110101", "name": "东城区", "level": 4}],
  "contained_by":      [{"code": "100000", "name": "中国", "level": 1}],
  "admin_level_match": 2,
  "confidence":        0.9866
}
```

**Example output (German boundary after `download-gadm Germany`):**

```json
{
  "coincides_with":    [{"code": "DEU.1_1", "name": "Baden-Württemberg", "level": 2, "similarity": 0.9812}],
  "intersects_with":   [],
  "covers_children":   [{"code": "DEU.1.1_1", "name": "Freiburg im Breisgau", "level": 3}],
  "contained_by":      [{"code": "DEU", "name": "Germany", "level": 1}],
  "admin_level_match": 2,
  "confidence":        0.9906
}
```

**Three-layer spatial filter** (performance):
1. Bounding box overlap — GIST index scan
2. Convex hull intersection — narrows candidates
3. Actual geometry intersection — precise check (uses simplified geometry for polygons with >500 vertices)

**Similarity metric** (for `coincides_with`, threshold IoU ≥ 0.85):

```
similarity = 0.5 × IoU + 0.3 × area_ratio + 0.2 × (1 − normalised_centroid_offset)
```

> **Note:** The `contained_by` fallback in the PL/pgSQL function uses substring-based ancestor lookup tuned for 6-digit Chinese codes. For GADM GIDs the primary parent-chain walkup (via `parent_code`) is used instead and works correctly. The substring fallback is only triggered when no parent-chain match is found, so GADM data is fully functional.

---

## Querying Results

**Verify imported GADM data:**

```sql
SELECT level, COUNT(*) FROM adminbounds.admin_units GROUP BY level ORDER BY level;
SELECT adcode, name, level FROM adminbounds.admin_units WHERE adcode LIKE 'DEU%' LIMIT 10;
```

**Find all features that coincide with a specific province:**

```sql
SELECT source_table, geom_hash
FROM thematic_admin_relations
WHERE coincides_with @> '[{"code": "320000"}]';
```

**Find features at city level with high confidence:**

```sql
SELECT *
FROM thematic_admin_relations
WHERE admin_level_match = 3
  AND confidence > 0.8;
```

**Join back to source table:**

```sql
SELECT src.*, tar.coincides_with, tar.contained_by
FROM sample_pois_pg_test src
JOIN thematic_admin_relations tar
    ON tar.source_table = 'sample_pois_pg_test'
   AND tar.geom_hash = md5(ST_AsEWKB(src.geom));
```
