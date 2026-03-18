"""
Diagnostic checks for infer_admin_semantic_relation returning empty results.
"""

import json

PASS = "  [OK]"
FAIL = "  [FAIL]"
WARN = "  [WARN]"


def _resolve_table(source_table: str, schema: str) -> str:
    """Return a fully qualified table reference, respecting an embedded schema prefix."""
    if "." in source_table:
        return source_table
    return f"{qualified}"


def diagnose(conn, source_table: str, geom_col: str, schema: str) -> dict:
    """Run diagnostic checks. Returns structured result dict.

    source_table may be schema-qualified (e.g. "myschema.mytable"), in which
    case the schema parameter is ignored.
    """
    qualified = _resolve_table(source_table, schema)
    results = {}
    cur = conn.cursor()

    # 1. admin_units row count
    cur.execute("SELECT COUNT(*) FROM adminbounds.admin_units")
    total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM adminbounds.admin_units WHERE geom_bbox IS NULL")
    null_bbox = cur.fetchone()[0]
    results["admin_units_total"] = total
    results["admin_units_null_bbox"] = null_bbox

    print("\n=== 1. admin_units row count ===")
    print(f"{PASS if total > 0 else FAIL} Total rows: {total}")
    print(f"{PASS if null_bbox == 0 else FAIL} Rows with NULL geom_bbox (derived fields missing): {null_bbox}")
    if null_bbox > 0:
        print("      → Run import-boundaries again; compute_derived_fields() did not complete.")

    print("\n=== 2. admin_units level distribution ===")
    cur.execute("SELECT level, COUNT(*) FROM adminbounds.admin_units GROUP BY level ORDER BY level")
    level_dist = {}
    for row in cur.fetchall():
        level_dist[row[0]] = row[1]
        print(f"  Level {row[0]}: {row[1]} rows")
    results["level_distribution"] = level_dist

    print(f"\n=== 3. Source table: {qualified} ===")
    cur.execute(f"SELECT COUNT(*) FROM {qualified} WHERE {geom_col} IS NOT NULL")
    src_count = cur.fetchone()[0]
    results["source_non_null_geoms"] = src_count
    print(f"{PASS if src_count > 0 else FAIL} Non-null geometries: {src_count}")

    cur.execute(f"SELECT DISTINCT ST_SRID({geom_col}) FROM {qualified} WHERE {geom_col} IS NOT NULL LIMIT 5")
    srids = [r[0] for r in cur.fetchall()]
    results["source_srids"] = srids
    print(f"{PASS if srids == [4326] else FAIL} Geometry SRIDs in source table: {srids}")
    if srids and srids != [4326]:
        print("      → Geometries are NOT in EPSG:4326. The function expects 4326.")

    cur.execute(f"""
        SELECT
            ST_XMin(ST_Extent({geom_col})),
            ST_YMin(ST_Extent({geom_col})),
            ST_XMax(ST_Extent({geom_col})),
            ST_YMax(ST_Extent({geom_col}))
        FROM {qualified}
        WHERE {geom_col} IS NOT NULL
    """)
    row = cur.fetchone()
    if row and row[0] is not None:
        xmin, ymin, xmax, ymax = row
        results["bbox"] = {"xmin": xmin, "ymin": ymin, "xmax": xmax, "ymax": ymax}
        print(f"  Bounding box: ({xmin:.4f}, {ymin:.4f}) → ({xmax:.4f}, {ymax:.4f})")
        in_china = (70 <= xmin <= 140) and (15 <= ymin <= 55)
        results["in_china_range"] = in_china
        print(f"{PASS if in_china else FAIL} Coordinates look like China (lon 70–140, lat 15–55): {in_china}")

    print("\n=== 4. Spatial overlap: source bbox vs admin_units bbox ===")
    cur.execute(f"""
        SELECT COUNT(*)
        FROM adminbounds.admin_units au
        WHERE au.geom_bbox && (
            SELECT ST_Extent({geom_col}) FROM {qualified} WHERE {geom_col} IS NOT NULL
        )
    """)
    overlap_count = cur.fetchone()[0]
    results["spatial_overlap_count"] = overlap_count
    print(f"{PASS if overlap_count > 0 else FAIL} admin_units whose bbox overlaps source extent: {overlap_count}")
    if overlap_count == 0:
        print("      → No spatial overlap at all. Likely a CRS or coordinate system mismatch.")

    print("\n=== 5. Manual function call on first source geometry ===")
    cur.execute(f"""
        SELECT
            ST_AsText({geom_col})  AS wkt,
            ST_SRID({geom_col})    AS srid,
            ST_IsValid({geom_col}) AS is_valid
        FROM {qualified}
        WHERE {geom_col} IS NOT NULL
        LIMIT 1
    """)
    row = cur.fetchone()
    if row:
        wkt, srid, is_valid = row
        results["sample_srid"] = srid
        results["sample_is_valid"] = is_valid
        print(f"  SRID: {srid}, IsValid: {is_valid}")
        print(f"  WKT (first 120 chars): {wkt[:120]}...")

        cur.execute(
            "SELECT adminbounds.infer_admin_semantic_relation(ST_GeomFromText(%s, 4326))",
            (wkt,),
        )
        func_result = cur.fetchone()[0]
        results["function_result"] = func_result
        print(f"\n  Function result:\n  {json.dumps(func_result, ensure_ascii=False, indent=2)}")

        cur.execute(f"""
            WITH input AS (
                SELECT ST_GeomFromText(%s, 4326) AS g
            ),
            layer1 AS (
                SELECT adcode FROM adminbounds.admin_units, input
                WHERE geom_bbox && ST_Envelope(input.g)
            ),
            layer2 AS (
                SELECT au.adcode FROM adminbounds.admin_units au, input
                WHERE au.geom_bbox && ST_Envelope(input.g)
                  AND ST_Intersects(au.geom_hull, input.g)
            ),
            layer3 AS (
                SELECT au.adcode FROM adminbounds.admin_units au, input
                WHERE au.geom_bbox && ST_Envelope(input.g)
                  AND ST_Intersects(au.geom_hull, input.g)
                  AND ST_Intersects(
                        CASE WHEN au.vertex_count > 500 THEN au.geom_simple ELSE au.geom END,
                        input.g
                      )
            )
            SELECT
                (SELECT COUNT(*) FROM layer1) AS after_layer1_bbox,
                (SELECT COUNT(*) FROM layer2) AS after_layer2_hull,
                (SELECT COUNT(*) FROM layer3) AS after_layer3_geom
        """, (wkt,))
        row = cur.fetchone()
        results["filter_layers"] = {
            "after_bbox": row[0],
            "after_hull": row[1],
            "after_geom": row[2],
        }
        print(f"\n  Three-layer filter candidates (first geometry):")
        print(f"    After layer 1 (bbox):  {row[0]}")
        print(f"    After layer 2 (hull):  {row[1]}")
        print(f"    After layer 3 (geom):  {row[2]}")
        if row[0] == 0:
            print(f"    {FAIL} Nothing passes bbox filter → geom_bbox NULL or CRS mismatch")
        elif row[2] == 0:
            print(f"    {WARN} Passes bbox/hull but not fine geometry → simplification or topology issue")

    cur.close()
    return results
