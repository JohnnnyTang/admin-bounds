-- infer_admin_semantic_relation(input_geom GEOMETRY) RETURNS JSONB
--
-- Infers spatial-semantic relationships between an arbitrary geometry and
-- Chinese administrative boundaries stored in admin_units.
--
-- Returns JSONB with four relationship arrays plus scalar metadata:
--   coincides_with   – units that substantially overlap the input (IoU ≥ 0.85)
--   intersects_with  – units at the dominant level that partially overlap
--   covers_children  – child-level units contained within the dominant unit
--   contained_by     – ancestor chain of the dominant match
--   admin_level_match – integer level of the primary match
--   confidence        – float 0–1

CREATE OR REPLACE FUNCTION adminbounds.infer_admin_semantic_relation(input_geom GEOMETRY)
RETURNS JSONB
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    -- Working geometry (validated)
    v_input          GEOMETRY;

    -- Candidate rows from three-layer filter
    v_rec            RECORD;

    -- Coincides tracking
    v_coincides      JSONB := '[]'::JSONB;
    v_coincide_codes TEXT[] := '{}';
    v_best_sim       FLOAT8 := 0.0;
    v_best_level     INTEGER := NULL;

    -- Intersects tracking
    v_intersects     JSONB := '[]'::JSONB;
    v_max_overlap    FLOAT8 := 0.0;
    v_intersect_level INTEGER := NULL;

    -- Per-candidate metrics
    v_iou               FLOAT8;
    v_area_ratio        FLOAT8;
    v_centroid_dist     FLOAT8;
    v_similarity        FLOAT8;
    v_overlap_ratio     FLOAT8;   -- intersection / input_area
    v_admin_coverage    FLOAT8;   -- intersection / unit_area
    v_intersection_area FLOAT8;
    v_input_area        FLOAT8;
    v_unit_area         FLOAT8;
    v_intersection      GEOMETRY;

    -- covers_children
    v_covers         JSONB := '[]'::JSONB;
    v_match_level    INTEGER;

    -- contained_by
    v_contained      JSONB := '[]'::JSONB;
    v_cur_code       TEXT;
    v_parent_code    TEXT;
    v_parent_rec     RECORD;
    v_loop_guard     INTEGER;

    -- LCA for intersects-only path
    v_lca_code       TEXT;
    v_lca_prefix     TEXT;
    v_intersect_codes TEXT[];

    -- Output
    v_confidence     FLOAT8 := 0.0;
    v_level_match    INTEGER;
BEGIN
    -- -------------------------------------------------------------------------
    -- Guard: NULL or empty input
    -- -------------------------------------------------------------------------
    IF input_geom IS NULL OR ST_IsEmpty(input_geom) THEN
        RETURN jsonb_build_object(
            'coincides_with',    '[]'::JSONB,
            'intersects_with',   '[]'::JSONB,
            'covers_children',   '[]'::JSONB,
            'contained_by',      '[]'::JSONB,
            'admin_level_match', NULL,
            'confidence',        0.0
        );
    END IF;

    -- Always apply ST_MakeValid: ST_IsValid can return true for geometries that
    -- still cause GEOS TopologyException during ST_Intersection / ST_Union.
    v_input := ST_MakeValid(input_geom);

    -- Pre-compute input area (geography, m²)
    v_input_area := ST_Area(v_input::GEOGRAPHY);

    -- -------------------------------------------------------------------------
    -- Three-layer filtering + similarity metrics
    -- -------------------------------------------------------------------------
    FOR v_rec IN
        WITH candidates AS (
            SELECT
                adcode,
                name,
                level,
                parent_code,
                area_m2,
                centroid,
                -- Fine-pass geometry: use simplified for complex polygons
                CASE WHEN vertex_count > 500 THEN geom_simple ELSE geom END AS work_geom
            FROM adminbounds.admin_units
            WHERE
                -- Layer 1 (coarse): bbox overlap – uses GIST index
                geom_bbox && ST_Envelope(v_input)
                -- Layer 2 (medium): convex hull intersection
                AND ST_Intersects(geom_hull, v_input)
        )
        SELECT
            adcode, name, level, parent_code, area_m2, centroid, work_geom
        FROM candidates
        -- Layer 3 (fine): actual geometry intersection
        WHERE ST_Intersects(work_geom, v_input)
    LOOP
        -- Skip degenerate units
        IF v_rec.area_m2 IS NULL OR v_rec.area_m2 = 0 THEN
            CONTINUE;
        END IF;

        v_unit_area   := v_rec.area_m2;
        -- ST_MakeValid on work_geom guards against TopologyException on near-valid geometries
        v_intersection := ST_Intersection(ST_MakeValid(v_rec.work_geom), v_input);

        IF ST_IsEmpty(v_intersection) THEN
            CONTINUE;
        END IF;

        -- Compute intersection area once; reuse for IoU, overlap_ratio, admin_coverage
        v_intersection_area := ST_Area(v_intersection::GEOGRAPHY);

        -- IoU: algebraic form avoids ST_Union TopologyException on near-valid geometries
        v_iou := v_intersection_area
                 / NULLIF(v_unit_area + v_input_area - v_intersection_area, 0);
        v_iou := COALESCE(v_iou, 0.0);

        -- Area ratio
        v_area_ratio := LEAST(v_unit_area, v_input_area)
                        / NULLIF(GREATEST(v_unit_area, v_input_area), 0);
        v_area_ratio := COALESCE(v_area_ratio, 0.0);

        -- Normalised centroid distance
        v_centroid_dist := ST_Distance(
                               v_rec.centroid::GEOGRAPHY,
                               ST_Centroid(v_input)::GEOGRAPHY
                           ) / NULLIF(SQRT(v_unit_area / pi()), 0);
        v_centroid_dist := COALESCE(LEAST(v_centroid_dist, 1.0), 1.0);

        -- Composite similarity (IoU-based)
        v_similarity := 0.5 * v_iou
                      + 0.3 * v_area_ratio
                      + 0.2 * (1.0 - v_centroid_dist);

        -- Coverage ratios (used for secondary coincide check and intersects_with)
        v_overlap_ratio  := v_intersection_area / NULLIF(v_input_area, 0);
        v_overlap_ratio  := COALESCE(v_overlap_ratio, 0.0);
        v_admin_coverage := v_intersection_area / NULLIF(v_unit_area, 0);
        v_admin_coverage := COALESCE(v_admin_coverage, 0.0);

        -- -----------------------------------------------------------------
        -- Classify: coincides_with
        --   Primary path:   IoU-based similarity ≥ 0.85
        --   Secondary path: mutual coverage check for island-rich regions where
        --                   a dataset polygon may legitimately omit remote islands
        --                   (≥90% of input inside admin AND ≥75% of admin inside input)
        -- -----------------------------------------------------------------
        IF v_similarity >= 0.85
           OR (v_overlap_ratio >= 0.90 AND v_admin_coverage >= 0.75)
        THEN
            -- If secondary path triggered, use coverage-based similarity score
            IF v_similarity < 0.85 THEN
                v_similarity := (v_overlap_ratio + v_admin_coverage) / 2.0;
            END IF;
            v_coincides := v_coincides || jsonb_build_object(
                'code',       v_rec.adcode,
                'name',       v_rec.name,
                'level',      v_rec.level,
                'similarity', ROUND(v_similarity::NUMERIC, 4)
            );
            v_coincide_codes := v_coincide_codes || v_rec.adcode;

            IF v_similarity > v_best_sim THEN
                v_best_sim   := v_similarity;
                v_best_level := v_rec.level;
            END IF;

        -- -----------------------------------------------------------------
        -- Candidate for intersects_with (overlap_ratio > 0.05)
        -- -----------------------------------------------------------------
        ELSIF v_overlap_ratio > 0.05 THEN
            -- Store temporarily; will filter by dominant level later.
            -- We tag each entry with level so we can filter after the loop.
            v_intersects := v_intersects || jsonb_build_object(
                'code',          v_rec.adcode,
                'name',          v_rec.name,
                'level',         v_rec.level,
                'overlap_ratio', ROUND(v_overlap_ratio::NUMERIC, 4)
            );

            IF v_overlap_ratio > v_max_overlap THEN
                v_max_overlap    := v_overlap_ratio;
                v_intersect_level := v_rec.level;
            END IF;
        END IF;

    END LOOP;

    -- -------------------------------------------------------------------------
    -- Determine dominant level and filter intersects_with
    -- -------------------------------------------------------------------------
    IF v_best_level IS NOT NULL THEN
        -- coincides found: intersects uses same level, excludes already-coinciding
        v_intersect_level := v_best_level;
        v_intersects := (
            SELECT jsonb_agg(elem)
            FROM jsonb_array_elements(v_intersects) AS elem
            WHERE (elem->>'level')::INTEGER = v_intersect_level
              AND NOT (v_coincide_codes @> ARRAY[elem->>'code'])
        );
        v_level_match := v_best_level;
        v_confidence  := 0.5 + 0.5 * v_best_sim;
    ELSIF jsonb_array_length(v_intersects) > 0 THEN
        -- intersects only: keep dominant level entries
        v_intersects := (
            SELECT jsonb_agg(elem)
            FROM jsonb_array_elements(v_intersects) AS elem
            WHERE (elem->>'level')::INTEGER = v_intersect_level
        );
        v_level_match := v_intersect_level;
        v_confidence  := 0.3 + 0.4 * v_max_overlap;
    ELSE
        v_level_match := NULL;
        v_confidence  := 0.0;
    END IF;

    -- Null-safe empty arrays
    v_intersects := COALESCE(v_intersects, '[]'::JSONB);

    -- -------------------------------------------------------------------------
    -- covers_children: child-level units that intersect input
    -- -------------------------------------------------------------------------
    v_match_level := v_level_match;
    IF v_match_level IS NOT NULL AND v_match_level < 4 THEN
        SELECT jsonb_agg(jsonb_build_object('code', adcode, 'name', name, 'level', level))
        INTO v_covers
        FROM adminbounds.admin_units
        WHERE level = v_match_level + 1
          AND ST_Intersects(geom, v_input);
        v_covers := COALESCE(v_covers, '[]'::JSONB);
    END IF;

    -- -------------------------------------------------------------------------
    -- contained_by: ancestor chain
    -- -------------------------------------------------------------------------
    IF jsonb_array_length(v_coincides) > 0 THEN
        -- Walk parent_code chain from the best-match unit
        SELECT parent_code INTO v_cur_code
        FROM adminbounds.admin_units
        WHERE adcode = v_coincide_codes[1];

        v_loop_guard := 0;
        WHILE v_cur_code IS NOT NULL AND v_loop_guard < 10 LOOP
            SELECT adcode, name, level, parent_code
            INTO v_parent_rec
            FROM adminbounds.admin_units
            WHERE adcode = v_cur_code;

            EXIT WHEN NOT FOUND;

            v_contained := v_contained || jsonb_build_object(
                'code',  v_parent_rec.adcode,
                'name',  v_parent_rec.name,
                'level', v_parent_rec.level
            );
            v_cur_code   := v_parent_rec.parent_code;
            v_loop_guard := v_loop_guard + 1;
        END LOOP;

    ELSIF jsonb_array_length(v_intersects) > 0 THEN
        -- LCA: longest common adcode prefix among all intersecting units
        SELECT array_agg(elem->>'code')
        INTO v_intersect_codes
        FROM jsonb_array_elements(v_intersects) AS elem;

        -- Find longest common prefix of 2-, 4-, or 6-char adcode stems
        -- Try 4-char prefix first (city level), then 2-char (province)
        IF array_length(v_intersect_codes, 1) = 1 THEN
            v_lca_prefix := v_intersect_codes[1];
        ELSE
            -- Check if all share first 4 chars
            SELECT CASE
                WHEN count(DISTINCT LEFT(c, 4)) = 1 THEN LEFT(v_intersect_codes[1], 4)
                WHEN count(DISTINCT LEFT(c, 2)) = 1 THEN LEFT(v_intersect_codes[1], 2)
                ELSE NULL
            END INTO v_lca_prefix
            FROM unnest(v_intersect_codes) AS c;
        END IF;

        -- Round to valid adcode boundary
        IF v_lca_prefix IS NOT NULL THEN
            IF LENGTH(v_lca_prefix) >= 4 THEN
                v_lca_code := LEFT(v_lca_prefix, 4) || '00';
            ELSIF LENGTH(v_lca_prefix) >= 2 THEN
                v_lca_code := LEFT(v_lca_prefix, 2) || '0000';
            ELSE
                v_lca_code := '100000';
            END IF;

            -- Walk up from LCA
            v_cur_code   := v_lca_code;
            v_loop_guard := 0;
            WHILE v_cur_code IS NOT NULL AND v_loop_guard < 10 LOOP
                SELECT adcode, name, level, parent_code
                INTO v_parent_rec
                FROM adminbounds.admin_units
                WHERE adcode = v_cur_code;

                EXIT WHEN NOT FOUND;

                v_contained := v_contained || jsonb_build_object(
                    'code',  v_parent_rec.adcode,
                    'name',  v_parent_rec.name,
                    'level', v_parent_rec.level
                );
                v_cur_code   := v_parent_rec.parent_code;
                v_loop_guard := v_loop_guard + 1;
            END LOOP;
        END IF;
    END IF;

    -- -------------------------------------------------------------------------
    -- Return result
    -- -------------------------------------------------------------------------
    RETURN jsonb_build_object(
        'coincides_with',    v_coincides,
        'intersects_with',   v_intersects,
        'covers_children',   v_covers,
        'contained_by',      v_contained,
        'admin_level_match', v_level_match,
        'confidence',        ROUND(v_confidence::NUMERIC, 4)
    );

EXCEPTION WHEN OTHERS THEN
    RETURN jsonb_build_object(
        'coincides_with',    '[]'::JSONB,
        'intersects_with',   '[]'::JSONB,
        'covers_children',   '[]'::JSONB,
        'contained_by',      '[]'::JSONB,
        'admin_level_match', NULL,
        'confidence',        0.0,
        'error',             SQLERRM
    );
END;
$$;
