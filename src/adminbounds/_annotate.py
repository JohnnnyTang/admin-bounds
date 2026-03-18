"""
Batch-annotation of geometries in a source table with admin-unit semantic relations.
"""

import json
import logging

from tqdm import tqdm

log = logging.getLogger(__name__)

FETCH_SQL = """
SELECT
    src.uuid                    AS feature_uuid,
    ST_AsText(src.{geom_col})   AS geom_wkt
FROM {schema}.{source_table} src
LEFT JOIN adminbounds.thematic_admin_relations tar
    ON tar.source_table = %(source_table)s
   AND tar.feature_uuid = src.uuid
WHERE tar.id IS NULL
  AND src.{geom_col} IS NOT NULL
LIMIT %(batch_size)s
"""

INFER_SQL = """
SELECT adminbounds.infer_admin_semantic_relation(ST_GeomFromText(%(wkt)s, 4326)) AS result
"""

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


def annotate_batch(
    conn,
    source_table: str,
    geom_col: str,
    schema: str,
    batch_size: int,
    on_progress=None,
) -> int:
    """Batch-annotate source table. Returns count of newly annotated rows."""
    conn.autocommit = False
    total_processed = 0

    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT COUNT(*)
                FROM {schema}.{source_table} src
                LEFT JOIN adminbounds.thematic_admin_relations tar
                    ON tar.source_table = %s
                   AND tar.feature_uuid = src.uuid
                WHERE tar.id IS NULL AND src.{geom_col} IS NOT NULL
                """,
                (source_table,),
            )
            remaining = cur.fetchone()[0]
            log.info("Rows to annotate: %d", remaining)

        pbar = tqdm(total=remaining, unit="row")

        while True:
            fetch_sql = FETCH_SQL.format(
                geom_col=geom_col,
                schema=schema,
                source_table=source_table,
            )
            with conn.cursor() as cur:
                cur.execute(fetch_sql, {"source_table": source_table, "batch_size": batch_size})
                rows = cur.fetchall()

            if not rows:
                break

            for feature_uuid, geom_wkt in rows:
                try:
                    with conn.cursor() as cur:
                        cur.execute(INFER_SQL, {"wkt": geom_wkt})
                        result = cur.fetchone()[0]
                        if result is None:
                            continue

                        relations_str = json.dumps(result) if isinstance(result, dict) else result

                        cur.execute(
                            INSERT_SQL,
                            {
                                "source_table": source_table,
                                "feature_uuid": str(feature_uuid),
                                "relations":    relations_str,
                            },
                        )
                    conn.commit()
                    total_processed += 1
                    pbar.update(1)
                    if on_progress:
                        on_progress(total_processed, remaining)

                except Exception as exc:
                    conn.rollback()
                    log.warning("Row %s failed: %s", feature_uuid, exc)
                    pbar.update(1)

        pbar.close()

    finally:
        conn.close()

    log.info("Done. Annotated %d rows.", total_processed)
    return total_processed
