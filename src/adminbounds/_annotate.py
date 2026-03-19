"""
Batch-annotation of geometries in a source table with admin-unit semantic relations.
"""

import json
import logging

from tqdm import tqdm

log = logging.getLogger(__name__)

# Used by skip mode: ignore rows already annotated
FETCH_SQL = """
SELECT
    src.uuid                    AS feature_uuid,
    ST_AsText(src.{geom_col})   AS geom_wkt
FROM {qualified} src
LEFT JOIN adminbounds.thematic_admin_relations tar
    ON tar.source_table = %(source_table)s
   AND tar.feature_uuid = src.uuid
WHERE tar.id IS NULL
  AND src.{geom_col} IS NOT NULL
LIMIT %(batch_size)s
"""

# Used by update mode (phase 2): fetch geometries for a specific batch of UUIDs
UPDATE_FETCH_SQL = """
SELECT
    uuid                        AS feature_uuid,
    ST_AsText({geom_col})       AS geom_wkt
FROM {qualified}
WHERE uuid = ANY(%(uuids)s)
  AND {geom_col} IS NOT NULL
"""

INFER_SQL = """
SELECT adminbounds.infer_admin_semantic_relation(ST_GeomFromText(%(wkt)s, 4326)) AS result
"""

# Used by skip/replace modes: silently ignore already-existing rows
INSERT_SQL = """
INSERT INTO adminbounds.thematic_admin_relations
    (source_table, feature_uuid, admin_level_match, confidence,
     coincides_with, intersects_with, covers_children, contained_by)
SELECT
    %(source_table)s,
    %(feature_uuid)s::UUID,
    (r->>'admin_level_match')::INTEGER,
    (r->>'confidence')::FLOAT8,
    r->'coincides_with',
    r->'intersects_with',
    r->'covers_children',
    r->'contained_by'
FROM (SELECT %(relations)s::jsonb AS r) sub
ON CONFLICT (source_table, feature_uuid) DO NOTHING
"""

# Used by update mode: overwrite existing rows and refresh computed_at
UPDATE_INSERT_SQL = """
INSERT INTO adminbounds.thematic_admin_relations
    (source_table, feature_uuid, admin_level_match, confidence,
     coincides_with, intersects_with, covers_children, contained_by)
SELECT
    %(source_table)s,
    %(feature_uuid)s::UUID,
    (r->>'admin_level_match')::INTEGER,
    (r->>'confidence')::FLOAT8,
    r->'coincides_with',
    r->'intersects_with',
    r->'covers_children',
    r->'contained_by'
FROM (SELECT %(relations)s::jsonb AS r) sub
ON CONFLICT (source_table, feature_uuid) DO UPDATE SET
    admin_level_match = EXCLUDED.admin_level_match,
    confidence        = EXCLUDED.confidence,
    coincides_with    = EXCLUDED.coincides_with,
    intersects_with   = EXCLUDED.intersects_with,
    covers_children   = EXCLUDED.covers_children,
    contained_by      = EXCLUDED.contained_by,
    computed_at       = now()
"""


def _resolve_table(source_table: str, schema: str) -> tuple[str, str]:
    """Return (qualified_ref, table_key) from a potentially schema-qualified table name.

    If source_table already contains a schema (e.g. "myschema.mytable"), that
    schema takes precedence over the schema parameter. The returned table_key is
    the fully qualified name used as the identifier in thematic_admin_relations.
    """
    if "." in source_table:
        qualified = source_table
    else:
        qualified = f"{schema}.{source_table}"
    return qualified, qualified


def _preflight(conn, qualified: str, table_key: str, geom_col: str, mode: str) -> tuple[int, int]:
    """Query and print annotation status. Returns (total, annotated)."""
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                COUNT(*)        AS total,
                COUNT(tar.id)   AS annotated
            FROM {qualified} src
            LEFT JOIN adminbounds.thematic_admin_relations tar
                ON tar.source_table = %s
               AND tar.feature_uuid = src.uuid
            WHERE src.{geom_col} IS NOT NULL
            """,
            (table_key,),
        )
        total, annotated = cur.fetchone()

    unannotated = total - annotated
    print(f"\nSource table: {table_key}")
    print(f"  Total rows:        {total}")
    print(f"  Already annotated: {annotated}")
    print(f"  Unannotated:       {unannotated}")

    if mode == "skip":
        print(f"  Mode: skip → will annotate {unannotated} new row(s)")
    elif mode == "update":
        print(f"  Mode: update → will re-infer all {total} row(s), overwriting existing results")
    elif mode == "replace":
        print(f"  Mode: replace → will delete {annotated} existing result(s) and re-annotate all {total} row(s)")

    return total, annotated


def _run_infer_and_insert(conn, qualified, geom_col, table_key, rows, insert_sql, pbar, on_progress, total_processed):
    """Infer and insert/update for a list of (feature_uuid, geom_wkt) rows."""
    for feature_uuid, geom_wkt in rows:
        try:
            with conn.cursor() as cur:
                cur.execute(INFER_SQL, {"wkt": geom_wkt})
                result = cur.fetchone()[0]
                if result is None:
                    pbar.update(1)
                    continue

                relations_str = json.dumps(result) if isinstance(result, dict) else result
                cur.execute(
                    insert_sql,
                    {
                        "source_table": table_key,
                        "feature_uuid": str(feature_uuid),
                        "relations":    relations_str,
                    },
                )
            conn.commit()
            total_processed += 1
            pbar.update(1)
            if on_progress:
                on_progress(total_processed, None)

        except Exception as exc:
            conn.rollback()
            log.warning("Row %s failed: %s", feature_uuid, exc)
            pbar.update(1)

    return total_processed


def annotate_batch(
    conn,
    source_table: str,
    geom_col: str,
    schema: str,
    batch_size: int,
    on_progress=None,
    mode: str = "skip",
) -> int:
    """Batch-annotate source table. Returns count of rows processed.

    Args:
        source_table: Table name, optionally schema-qualified (e.g. "myschema.mytable").
                      When schema-qualified, the schema parameter is ignored.
        geom_col:     Geometry column name.
        schema:       Default schema if source_table is not schema-qualified.
        batch_size:   Rows to process per batch.
        on_progress:  Optional callback(processed, total).
        mode:         Re-annotation strategy:
                      - "skip"    (default) Only annotate rows with no existing entry.
                      - "update"  Re-infer all rows, overwriting existing results.
                      - "replace" Delete all existing results first, then annotate all.
    """
    if mode not in ("skip", "update", "replace"):
        raise ValueError(f"Invalid mode {mode!r}. Choose 'skip', 'update', or 'replace'.")

    qualified, table_key = _resolve_table(source_table, schema)
    conn.autocommit = False
    total_processed = 0

    try:
        total, annotated = _preflight(conn, qualified, table_key, geom_col, mode)

        # replace: delete existing annotations, then fall through to skip logic
        if mode == "replace" and annotated > 0:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM adminbounds.thematic_admin_relations WHERE source_table = %s",
                    (table_key,),
                )
            conn.commit()
            print(f"  Deleted {annotated} existing annotation(s).")

        # skip / replace: paginate via LEFT JOIN (unannotated rows only)
        if mode in ("skip", "replace"):
            remaining = total if mode == "replace" else (total - annotated)
            if remaining == 0:
                print("  Nothing to annotate.")
                return 0

            pbar = tqdm(total=remaining, unit="row")
            fetch_sql = FETCH_SQL.format(geom_col=geom_col, qualified=qualified)

            while True:
                with conn.cursor() as cur:
                    cur.execute(fetch_sql, {"source_table": table_key, "batch_size": batch_size})
                    rows = cur.fetchall()
                if not rows:
                    break
                total_processed = _run_infer_and_insert(
                    conn, qualified, geom_col, table_key,
                    rows, INSERT_SQL, pbar, on_progress, total_processed,
                )
            pbar.close()

        # update: collect all UUIDs upfront, then batch-fetch by UUID list
        else:
            if total == 0:
                print("  Nothing to annotate.")
                return 0

            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT uuid FROM {qualified} WHERE {geom_col} IS NOT NULL ORDER BY uuid"
                )
                all_uuids = [row[0] for row in cur.fetchall()]

            pbar = tqdm(total=len(all_uuids), unit="row")
            fetch_sql = UPDATE_FETCH_SQL.format(geom_col=geom_col, qualified=qualified)

            for i in range(0, len(all_uuids), batch_size):
                batch_uuids = all_uuids[i : i + batch_size]
                with conn.cursor() as cur:
                    cur.execute(fetch_sql, {"uuids": batch_uuids})
                    rows = cur.fetchall()
                total_processed = _run_infer_and_insert(
                    conn, qualified, geom_col, table_key,
                    rows, UPDATE_INSERT_SQL, pbar, on_progress, total_processed,
                )
            pbar.close()

    finally:
        conn.close()

    log.info("Done. Processed %d rows (mode=%s).", total_processed, mode)
    return total_processed
