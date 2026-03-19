# adminbounds

**Administrative boundary semantic relation inference for geospatial datasets.**

Given any vector geometry, `adminbounds` answers: *where does this geometry sit in the administrative hierarchy?* It infers whether the geometry coincides with a known boundary, intersects multiple units, contains child units, or is contained by ancestor units — and stores the results as structured JSONB for downstream querying.

Bundled data covers China's four-level hierarchy (country → province → city → district). Any other country can be added on demand via GADM 4.1.

---

## How It Works

The pipeline has three stages:

```
1. Boundary data          2. Inference              3. Annotation results
─────────────────         ──────────────────        ─────────────────────
admin_units table    →    infer_admin_semantic   →  thematic_admin_relations
(PostGIS polygons)        _relation(geom)            (JSONB per feature)
```

**Stage 1 — Load boundary data** into `adminbounds.admin_units`. Either use the bundled China data or download any country from GADM 4.1.

**Stage 2 — Inference** is a single PL/pgSQL function `adminbounds.infer_admin_semantic_relation(geom)` that classifies a geometry into four relationship types:

| Relationship | Meaning | Example |
|---|---|---|
| `coincides_with` | Substantially overlaps a known boundary (IoU ≥ 0.85) | A polygon matching Beijing municipality exactly |
| `intersects_with` | Partially overlaps units at the dominant level | A corridor crossing Nanjing and Suzhou |
| `covers_children` | The geometry contains child-level units | A province polygon covering its cities |
| `contained_by` | The ancestor chain above the matched unit | A city → its province → country |

**Stage 3 — Batch annotation** runs the inference function over every row of any PostGIS table and writes the results into `adminbounds.thematic_admin_relations`, keyed by `(source_table, feature_uuid)`.

---

## Prerequisites

- **PostgreSQL 14+** with the **PostGIS 3.x** extension enabled
- **Python 3.12+**

Enable PostGIS on your target database if not already done:

```sql
CREATE EXTENSION IF NOT EXISTS postgis;
```

---

## Installation

**From PyPI** (recommended for users):

```bash
pip install adminbounds
```

**From source** (for development):

```bash
git clone https://github.com/JohnnnyTang/admin-bounds.git
cd admin-bounds
uv sync          # or: pip install -e .
```

---

## Configuration

The package reads database credentials from environment variables or a `.env` file in the working directory.

```bash
cp .env.example .env
```

Edit `.env`:

```dotenv
ADMINBOUNDS_DB_HOST=localhost
ADMINBOUNDS_DB_PORT=5432
ADMINBOUNDS_DB_NAME=your_database
ADMINBOUNDS_DB_USER=your_username
ADMINBOUNDS_DB_PASSWORD=your_password
```

All CLI commands and the Python client fall back to these variables if no explicit connection arguments are given. You can also pass credentials directly:

```bash
adminbounds --host localhost --dbname mydb --user postgres --password secret init-db
```

```python
from adminbounds import AdminBoundsClient
c = AdminBoundsClient(host="localhost", dbname="mydb", user="postgres", password="secret")
```

---

## Quick Start

A complete walk-through from zero to annotated results:

```bash
# 1. Create the adminbounds schema and tables in your database
adminbounds init-db

# 2. Load boundary data (choose one or both)
adminbounds import-boundaries          # bundled China data
adminbounds download-gadm Germany      # or any country via GADM

# 3. Upload your dataset (adds a uuid primary key automatically)
adminbounds upload my_data.geojson my_table

# 4. Annotate — infers admin relations for every row
adminbounds annotate --source-table my_table --geom-col geom

# 5. Query results in PostgreSQL
psql mydb -c "
SELECT src.name, tar.admin_level_match, tar.confidence, tar.coincides_with
FROM public.my_table src
JOIN adminbounds.thematic_admin_relations tar
  ON tar.source_table = 'public.my_table'
 AND tar.feature_uuid = src.uuid;
"
```

---

## CLI Reference

### `init-db`

Creates the `adminbounds` schema, `admin_units` table, `thematic_admin_relations` table, and deploys the PL/pgSQL inference function. Safe to re-run — idempotent DDL and applies any pending migrations.

```bash
adminbounds init-db
```

Run this once before anything else. Must be re-run after upgrading the package to pick up schema changes.

---

### `import-boundaries`

Loads bundled Chinese administrative boundaries into `adminbounds.admin_units` at four levels:

| Level | Coverage |
|---|---|
| 1 | Country (China) |
| 2 | Provinces (34) |
| 3 | Cities (~300) |
| 4 | Districts (~3000) |

```bash
adminbounds import-boundaries
```

Idempotent — re-running updates existing rows and skips already-computed derived fields. Run after `init-db`.

---

### `download-gadm`

Downloads GADM 4.1 administrative boundaries for any country and imports them into `adminbounds.admin_units`. Accepts either an ISO3 code or a common English country name.

```bash
adminbounds download-gadm Germany
adminbounds download-gadm DEU                      # ISO3 code — same result
adminbounds download-gadm "United States"
adminbounds download-gadm USA --levels 0,1         # country + state only (level 2+ can be very large)
adminbounds download-gadm France --force           # re-download even if already cached
adminbounds download-gadm Japan --cache-dir /data/gadm_cache
```

Downloaded zip files are cached in `~/.adminbounds/gadm_cache/` by default so repeated calls are fast. HTTP 404 for a level (not all countries have all 4 levels) is silently skipped.

**GADM level → DB level mapping:**

| GADM level | Meaning | DB `level` |
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

GADM GIDs look like `DEU`, `DEU.1_1`, `DEU.1.2_1`. The `adcode` column is `TEXT` to accommodate these (unlike the 6-digit numeric Chinese codes).

---

### `upload`

Uploads a local GeoJSON file into a PostGIS table under the `public` schema. Automatically reprojects to EPSG:4326 if needed, and **adds a `uuid` primary key column** — which is required for the `annotate` command.

```bash
adminbounds upload path/to/data.geojson my_table
adminbounds upload path/to/data.geojson my_table --if-exists append   # append to existing table
adminbounds upload path/to/data.geojson my_table --if-exists fail     # error if table exists
```

The default `--if-exists replace` drops and recreates the table. Use `append` to add more features to an existing table without losing previous rows.

> **Tip:** If you already have data in PostgreSQL and want to use `annotate`, your table needs a `uuid` column of type `UUID` with a primary key. You can add one with:
> ```sql
> ALTER TABLE myschema.my_table ADD COLUMN uuid UUID DEFAULT gen_random_uuid() PRIMARY KEY;
> ```

---

### `annotate`

Runs `infer_admin_semantic_relation` on every geometry in a source table and writes the results into `adminbounds.thematic_admin_relations`. The source table must have a `uuid` column (added automatically by `upload`).

```bash
adminbounds annotate --source-table my_table --geom-col geom
adminbounds annotate --source-table myschema.my_table --geom-col geom   # schema-qualified
adminbounds annotate --source-table my_table --geom-col geom --batch-size 50
```

Before processing, a pre-flight report is always printed:

```
Source table: public.my_table
  Total rows:        1234
  Already annotated: 0
  Unannotated:       1234
  Mode: skip → will annotate 1234 new row(s)
```

**Re-annotation modes** (`--mode`):

| Mode | Behavior | When to use |
|---|---|---|
| `skip` | **(default)** Only annotate rows not yet in `thematic_admin_relations`. Second run does nothing if fully annotated. | Normal incremental runs; adding new rows to the source table |
| `update` | Re-infer **all** rows, overwriting existing results. Rows that fail inference keep their old result. | After refreshing boundary data (e.g. re-running `download-gadm`) |
| `replace` | Delete all existing annotations for this table first, then annotate everything from scratch. Clean slate. | After major data changes; guaranteed fresh results |

```bash
# Only annotate new rows added since last run
adminbounds annotate --source-table my_table --geom-col geom

# Re-infer everything after importing new boundary data
adminbounds annotate --source-table my_table --geom-col geom --mode update

# Full reset and re-annotate
adminbounds annotate --source-table my_table --geom-col geom --mode replace
```

The `--schema` flag sets the default schema when `--source-table` is not schema-qualified (default: `public`). If the table name already contains a dot (e.g. `myschema.my_table`), `--schema` is ignored.

Annotation is **resume-safe** in `skip` mode — you can interrupt and restart without reprocessing completed rows.

---

### `diagnose`

Runs a series of diagnostic checks when annotation returns empty or unexpected results. Useful for debugging geometry CRS mismatches, missing derived fields, or spatial overlap issues.

```bash
adminbounds diagnose --source-table my_table --geom-col geom
adminbounds diagnose --source-table myschema.my_table --geom-col geom
```

Checks performed:

1. `admin_units` row count and whether derived fields (`geom_bbox` etc.) have been computed
2. Level distribution of loaded boundaries
3. Source table geometry count and SRID
4. Source extent bounding box and whether it falls within loaded boundary extents
5. Three-layer spatial filter pass-through counts on the first geometry (bbox → hull → full geom)
6. Full function call result on the first geometry

---

## Python API

All CLI operations are available as methods on `AdminBoundsClient`.

### Connecting

```python
from adminbounds import AdminBoundsClient

# Credentials from keyword arguments
c = AdminBoundsClient(
    host="localhost",
    port=5432,
    dbname="mydb",
    user="postgres",
    password="secret",
)

# Or rely entirely on ADMINBOUNDS_DB_* environment variables / .env file
c = AdminBoundsClient()
```

### Database setup

```python
# Create schema + tables + deploy inference function
c.init_db()

# Load bundled Chinese boundaries
c.import_boundaries()

# Download GADM boundaries for any country
c.download_gadm("Germany")                    # all 4 levels
c.download_gadm("DEU")                        # ISO3 code, same result
c.download_gadm("USA", levels=[0, 1])         # country + state only
c.download_gadm("France", force=True)         # re-download even if cached
c.download_gadm("Japan", cache_dir="/tmp/g")  # custom cache directory
```

### Single-geometry inference

```python
from shapely.geometry import box, shape
import json

# Infer for a single Shapely geometry — returns a dict
result = c.infer(box(116.3, 39.8, 116.5, 40.0))

print(result["admin_level_match"])   # 2  (province level)
print(result["confidence"])          # 0.94
print(result["coincides_with"])      # [{"code": "110000", "name": "北京市", ...}]
print(result["contained_by"])        # [{"code": "100000", "name": "中国", ...}]
print(result["covers_children"])     # [{"code": "110101", ...}, ...]
```

### Uploading data

```python
# Upload a GeoJSON file → public.my_table, adds uuid primary key
count = c.upload("path/to/data.geojson", "my_table")
print(f"Uploaded {count} features")

# Append to existing table
c.upload("more_data.geojson", "my_table", if_exists="append")
```

### Batch annotation

```python
# Annotate all rows — only new rows on subsequent calls (skip mode)
count = c.annotate("my_table", geom_col="geom")
count = c.annotate("myschema.my_table", geom_col="geom")   # schema-qualified

# Re-infer all rows after refreshing boundary data
count = c.annotate("my_table", mode="update")

# Clean slate
count = c.annotate("my_table", mode="replace")

# Progress callback
def on_progress(processed, _):
    print(f"\r{processed} rows done", end="")

count = c.annotate("my_table", batch_size=50, on_progress=on_progress)
```

### Diagnostics

```python
results = c.diagnose("my_table", geom_col="geom")
# Prints a structured diagnostic report and returns a dict of check results
```

---

## Database Schema

### `adminbounds.admin_units`

Stores administrative boundaries at four levels. Supports both Chinese numeric adcodes (`100000`) and GADM GIDs (`DEU.1_1`).

| Column | Type | Description |
|---|---|---|
| `id` | SERIAL | Auto-incrementing primary key |
| `adcode` | TEXT | Unique admin code — 6-digit numeric (China) or GADM GID |
| `name` | TEXT | Place name |
| `level` | INTEGER | 1=country, 2=province/state, 3=city/county, 4=district |
| `parent_code` | TEXT | `adcode` of the parent unit (NULL for level 1) |
| `geom` | GEOMETRY | Full boundary polygon (EPSG:4326) |
| `geom_bbox` | GEOMETRY | Bounding box — used for fast coarse spatial filter |
| `geom_hull` | GEOMETRY | Convex hull — used for medium spatial filter |
| `geom_simple` | GEOMETRY | Simplified geometry for polygons with >500 vertices |
| `centroid` | GEOMETRY | Centroid point |
| `area_m2` | FLOAT8 | Area in square metres |
| `vertex_count` | INTEGER | Vertex count of the original geometry |

### `adminbounds.thematic_admin_relations`

Stores per-feature annotation results. One row per `(source_table, feature_uuid)` pair.

| Column | Type | Description |
|---|---|---|
| `id` | BIGSERIAL | Auto-incrementing primary key |
| `source_table` | TEXT | Fully qualified source table name (e.g. `public.my_table`) |
| `feature_uuid` | UUID | UUID of the feature from the source table's `uuid` column |
| `admin_level_match` | INTEGER | Dominant admin level of the best match |
| `confidence` | FLOAT8 | Confidence score 0–1 |
| `coincides_with` | JSONB | Array of units that substantially overlap the geometry |
| `intersects_with` | JSONB | Array of units that partially overlap at the dominant level |
| `covers_children` | JSONB | Array of child units contained within the geometry |
| `contained_by` | JSONB | Ancestor chain of the best-matched unit |
| `computed_at` | TIMESTAMPTZ | Timestamp of when this annotation was computed |

---

## Inference Function

Can be called directly in SQL for ad-hoc queries:

```sql
SELECT adminbounds.infer_admin_semantic_relation(
    ST_GeomFromText('POLYGON((116.3 39.8, 116.5 39.8, 116.5 40.0, 116.3 40.0, 116.3 39.8))', 4326)
);
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

**Three-layer spatial filter** (performance — avoids full-table geometry intersection):
1. **Bounding box overlap** — GIST index scan, eliminates most candidates immediately
2. **Convex hull intersection** — narrows the remaining candidates
3. **Full geometry intersection** — precise check; uses simplified geometry for polygons with >500 vertices

**Similarity metric** (for `coincides_with`, threshold IoU ≥ 0.85):

```
similarity = 0.5 × IoU + 0.3 × area_ratio + 0.2 × (1 − normalised_centroid_offset)
```

> **Note on GADM and `contained_by`:** The `contained_by` fallback in the PL/pgSQL function uses substring-based ancestor lookup tuned for 6-digit Chinese codes. For GADM GIDs, the primary parent-chain walkup (via the `parent_code` column) is used instead. Since `parent_code` is correctly populated for all GADM data, this works correctly for all countries.

---

## Querying Results

**Check what's been loaded:**

```sql
-- Boundary data by level
SELECT level, COUNT(*) FROM adminbounds.admin_units GROUP BY level ORDER BY level;

-- GADM data for a specific country
SELECT adcode, name, level FROM adminbounds.admin_units WHERE adcode LIKE 'DEU%' LIMIT 10;
```

**Check annotation coverage:**

```sql
SELECT source_table, COUNT(*) AS annotated_rows
FROM adminbounds.thematic_admin_relations
GROUP BY source_table;
```

**Join annotation results back to the source table:**

```sql
SELECT
    src.*,
    tar.admin_level_match,
    tar.confidence,
    tar.coincides_with,
    tar.contained_by
FROM public.my_table src
JOIN adminbounds.thematic_admin_relations tar
    ON tar.source_table = 'public.my_table'
   AND tar.feature_uuid = src.uuid;
```

**Find features that coincide with a specific admin unit:**

```sql
-- Features coinciding with Jiangsu province (adcode 320000)
SELECT source_table, feature_uuid
FROM adminbounds.thematic_admin_relations
WHERE coincides_with @> '[{"code": "320000"}]';

-- Features coinciding with Germany
SELECT source_table, feature_uuid
FROM adminbounds.thematic_admin_relations
WHERE coincides_with @> '[{"code": "DEU"}]';
```

**Find features at city level with high confidence:**

```sql
SELECT tar.feature_uuid, tar.confidence, tar.coincides_with
FROM adminbounds.thematic_admin_relations tar
WHERE admin_level_match = 3
  AND confidence > 0.85
ORDER BY confidence DESC;
```

**Extract the first coinciding unit name as a plain text column:**

```sql
SELECT
    feature_uuid,
    coincides_with -> 0 ->> 'name'  AS matched_unit,
    confidence
FROM adminbounds.thematic_admin_relations
WHERE coincides_with IS NOT NULL
  AND jsonb_array_length(coincides_with) > 0;
```

---

## Project Structure

```
admin-bounds/
├── src/adminbounds/
│   ├── __init__.py             # Package entry point, exports AdminBoundsClient
│   ├── client.py               # AdminBoundsClient — high-level Python API
│   ├── config.py               # Pydantic settings (ADMINBOUNDS_DB_* env vars)
│   ├── db.py                   # SQLAlchemy engine + raw psycopg2 connection
│   ├── cli/__init__.py         # CLI entry point (adminbounds command)
│   ├── _import.py              # DDL deploy + bundled boundary import pipeline
│   ├── _gadm.py                # GADM 4.1 worldwide download + import
│   ├── _annotate.py            # Batch annotation logic with mode support
│   ├── _upload.py              # GeoJSON → PostGIS upload helper
│   ├── _diagnose.py            # Annotation diagnostic checks
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
