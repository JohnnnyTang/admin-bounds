"""
Boundary import logic: DDL + parse GeoJSON from package data + upsert + derived fields + deploy function.
"""

import json
from importlib.resources import files

import geopandas as gpd
from shapely.geometry import shape
from sqlalchemy import text
from tqdm import tqdm

_data = files("adminbounds").joinpath("data")
_sql = files("adminbounds").joinpath("sql")

FILES = [
    ("china.geojson",          1),
    ("china_state.geojson",    2),
    ("china_city.geojson",     3),
    ("china_district.geojson", 4),
]


def _read_sql(relative: str) -> str:
    return _sql.joinpath(relative).read_text(encoding="utf-8")


def infer_parent_code(adcode: str, level: int, json_parent_adcode) -> str | None:
    if level == 1:
        return None
    if json_parent_adcode is not None:
        try:
            p = str(int(float(json_parent_adcode))).zfill(6)
            if p.isdigit():
                return p
        except (ValueError, TypeError):
            pass
    if level == 2:
        return "100000"
    if level == 3:
        return adcode[:2] + "0000"
    if level == 4:
        return adcode[:4] + "00"
    return None


def _parse_geojson(filename: str, fallback_level: int) -> list[dict]:
    with _data.joinpath(filename).open(encoding="utf-8") as f:
        data = json.load(f)

    rows = []
    for feature in tqdm(data["features"], desc=f"Parsing {filename}", leave=False):
        props = feature.get("properties", {})

        adcode = props.get("adcode") or props.get("adCode") or props.get("code")
        if adcode is None:
            continue
        adcode = str(adcode).split(".")[0]
        if not adcode.isdigit():
            continue
        adcode = adcode.zfill(6)

        level_str = props.get("level", "")
        level_map = {"country": 1, "province": 2, "city": 3, "district": 4}
        level = level_map.get(str(level_str).lower(), fallback_level)

        name = props.get("name") or props.get("Name") or adcode

        parent_obj = props.get("parent") or {}
        json_parent_adcode = None
        if isinstance(parent_obj, dict):
            json_parent_adcode = parent_obj.get("adcode") or parent_obj.get("adCode")
        elif parent_obj is not None:
            json_parent_adcode = parent_obj

        parent_code = infer_parent_code(adcode, level, json_parent_adcode)
        geom = shape(feature["geometry"])

        rows.append({
            "adcode":      adcode,
            "name":        name,
            "level":       level,
            "parent_code": parent_code,
            "geometry":    geom,
        })
    return rows


def deploy_schema(engine) -> None:
    """Apply DDL for admin_units and thematic_admin_relations tables."""
    for sql_path in ("schema/01_admin_units.sql", "schema/02_thematic_admin_relations.sql"):
        sql = _read_sql(sql_path)
        with engine.begin() as conn:
            conn.execute(text(sql))
        print(f"  DDL applied: {sql_path}")

    # Idempotent migration: widen adcode/parent_code from VARCHAR(6) to TEXT
    # (no-op if columns are already TEXT; needed for GADM GIDs like "DEU.1_1")
    with engine.begin() as conn:
        conn.execute(text("""
            ALTER TABLE IF EXISTS adminbounds.admin_units
                ALTER COLUMN adcode TYPE TEXT,
                ALTER COLUMN parent_code TYPE TEXT
        """))


def deploy_function(engine) -> None:
    """Deploy the infer_admin_semantic_relation PL/pgSQL function."""
    sql = _read_sql("functions/infer_admin_semantic_relation.sql")
    with engine.begin() as conn:
        conn.execute(text(sql))
    print("  Function infer_admin_semantic_relation deployed.")


def _upsert_staging(engine, gdf: gpd.GeoDataFrame) -> None:
    staging_schema = "adminbounds"
    staging_table  = "admin_units_staging"

    gdf[["adcode", "name", "level", "parent_code", "geom"]].to_postgis(
        staging_table,
        engine,
        schema=staging_schema,
        if_exists="replace",
        index=False,
    )

    upsert_sql = f"""
        INSERT INTO adminbounds.admin_units (adcode, name, level, parent_code, geom)
        SELECT adcode, name, level, parent_code, geom
        FROM {staging_schema}.{staging_table}
        ON CONFLICT (adcode) DO UPDATE SET
            name        = EXCLUDED.name,
            level       = EXCLUDED.level,
            parent_code = EXCLUDED.parent_code,
            geom        = EXCLUDED.geom,
            geom_bbox   = NULL
    """
    with engine.begin() as conn:
        conn.execute(text(upsert_sql))
        conn.execute(text(f"DROP TABLE IF EXISTS {staging_schema}.{staging_table}"))


def _compute_derived_fields(engine) -> int:
    print("  Computing derived geometry fields (may take a minute)...")
    sql = """
        UPDATE adminbounds.admin_units SET
            geom_bbox    = ST_Envelope(geom),
            geom_hull    = ST_ConvexHull(geom),
            geom_simple  = COALESCE(ST_SimplifyPreserveTopology(geom, 0.001), geom),
            centroid     = ST_Centroid(geom),
            area_m2      = ST_Area(geom::GEOGRAPHY),
            vertex_count = ST_NPoints(geom)
        WHERE geom_bbox IS NULL
    """
    with engine.begin() as conn:
        result = conn.execute(text(sql))
        count = result.rowcount
        print(f"  Updated {count} rows with derived fields.")
    return count


def import_boundaries(engine) -> int:
    """Full pipeline: DDL + parse bundled GeoJSON + upsert + derived fields + deploy function.

    Returns the number of unique adcodes loaded.
    """
    deploy_schema(engine)

    all_rows: list[dict] = []
    for filename, fallback_level in FILES:
        print(f"Loading {filename}...")
        rows = _parse_geojson(filename, fallback_level)
        print(f"  → {len(rows)} features parsed")
        all_rows.extend(rows)

    if not all_rows:
        print("No data to load.")
        return 0

    seen: dict[str, dict] = {}
    for row in all_rows:
        seen[row["adcode"]] = row
    deduped = list(seen.values())
    print(f"\nTotal unique adcodes: {len(deduped)}")

    print("Upserting into admin_units...")
    gdf = gpd.GeoDataFrame(deduped, crs="EPSG:4326")
    gdf = gdf.rename_geometry("geom")
    _upsert_staging(engine, gdf)
    print("  Upsert complete.")

    _compute_derived_fields(engine)

    print("Deploying SQL function...")
    deploy_function(engine)

    print("\nImport complete.")
    return len(deduped)
