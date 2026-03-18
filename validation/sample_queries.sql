-- sample_queries.sql
-- Validation queries to run after import_admin_units.py

-- 1. Level distribution (expect rows for levels 1-4)
SELECT level, COUNT(*) AS cnt
FROM adminbounds.admin_units
GROUP BY level
ORDER BY level;

-- 2. Dangling parent check (should return 0 rows)
SELECT child.adcode, child.name, child.parent_code
FROM adminbounds.admin_units child
LEFT JOIN adminbounds.admin_units parent ON parent.adcode = child.parent_code
WHERE child.parent_code IS NOT NULL
  AND parent.adcode IS NULL;

-- 3. Null derived fields (should return 0)
SELECT COUNT(*) AS null_derived_count
FROM adminbounds.admin_units
WHERE geom_bbox IS NULL;

-- 4. Function smoke test: Beijing province → expect coincides_with Beijing
SELECT adminbounds.infer_admin_semantic_relation(
    (SELECT geom FROM adminbounds.admin_units WHERE adcode = '110000')
) AS beijing_result;

-- 5. Cross-city test: polygon spanning Nanjing (320100) and Suzhou (320500)
-- Expect intersects_with both; contained_by Jiangsu (320000)
WITH test_poly AS (
    SELECT ST_Buffer(
        ST_MakeLine(
            (SELECT centroid FROM adminbounds.admin_units WHERE adcode = '320100'),
            (SELECT centroid FROM adminbounds.admin_units WHERE adcode = '320500')
        )::GEOGRAPHY,
        30000  -- 30 km buffer
    )::GEOMETRY AS geom
)
SELECT adminbounds.infer_admin_semantic_relation(geom) AS cross_city_result
FROM test_poly;

-- 6. District coverage test: Jiangsu province → expect covers_children to include cities
SELECT adminbounds.infer_admin_semantic_relation(
    (SELECT geom FROM adminbounds.admin_units WHERE adcode = '320000')
) AS jiangsu_coverage;

-- 7. Quick thematic_admin_relations check (if batch annotation has been run)
SELECT source_table, COUNT(*) AS annotated_rows
FROM adminbounds.thematic_admin_relations
GROUP BY source_table
ORDER BY source_table;

-- 8. Join annotation results back to source table
-- Replace 'yrd_pop_city' with your actual table name
SELECT src.*, tar.admin_level_match, tar.confidence, tar.coincides_with, tar.contained_by
FROM public.yrd_pop_city src
JOIN adminbounds.thematic_admin_relations tar
    ON tar.source_table = 'yrd_pop_city'
   AND tar.feature_uuid = src.uuid;
