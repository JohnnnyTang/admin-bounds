"""
AdminBoundsClient — high-level Python API for the adminbounds package.
"""

import json
from pathlib import Path

from .config import make_settings
from .db import get_engine, get_raw_connection


class AdminBoundsClient:
    """High-level client for administrative boundary inference."""

    def __init__(
        self,
        host: str = None,
        port: int = None,
        dbname: str = None,
        user: str = None,
        password: str = None,
        admin_schema: str = "adminbounds",
        db_url: str = None,
    ):
        # db_url is not handled by Settings directly; build settings from kwargs
        self._settings = make_settings(
            host=host,
            port=port,
            name=dbname,
            user=user,
            password=password,
        )
        self._admin_schema = admin_schema
        self._engine = get_engine(self._settings)

    # ------------------------------------------------------------------
    # Schema / data setup
    # ------------------------------------------------------------------

    def init_db(self) -> None:
        """Create adminbounds schema, tables, and deploy the PL/pgSQL function."""
        from ._import import deploy_schema, deploy_function
        deploy_schema(self._engine)
        deploy_function(self._engine)

    def import_boundaries(self) -> int:
        """Load bundled Chinese admin boundary GeoJSON into adminbounds.admin_units.

        Returns the number of unique adcodes loaded.
        """
        from ._import import import_boundaries
        return import_boundaries(self._engine)

    # ------------------------------------------------------------------
    # Data operations
    # ------------------------------------------------------------------

    def upload(self, path: str | Path, table_name: str, if_exists: str = "replace") -> int:
        """Upload a GeoJSON file to public.<table_name> with uuid primary key.

        Returns the number of features uploaded.
        """
        from ._upload import upload_geojson
        return upload_geojson(self._engine, path, table_name, if_exists)

    def annotate(
        self,
        source_table: str,
        geom_col: str = "geom",
        schema: str = "public",
        batch_size: int = 100,
        on_progress=None,
        mode: str = "skip",
    ) -> int:
        """Batch-annotate source table. Returns count of rows processed.

        Args:
            source_table: Table name, optionally schema-qualified (e.g. "myschema.mytable").
            geom_col:     Geometry column name (default: "geom").
            schema:       Schema to use if source_table is not schema-qualified (default: "public").
            batch_size:   Rows to process per batch (default: 100).
            on_progress:  Optional callback(processed, total).
            mode:         Re-annotation strategy (default: "skip"):
                          - "skip"    Only annotate rows with no existing entry.
                          - "update"  Re-infer all rows, overwriting existing results.
                          - "replace" Delete all existing results first, then annotate all.
        """
        from ._annotate import annotate_batch
        conn = get_raw_connection(self._settings)
        return annotate_batch(conn, source_table, geom_col, schema, batch_size, on_progress, mode)

    def infer(self, geometry) -> dict:
        """Call infer_admin_semantic_relation on a single Shapely geometry.

        Returns the JSONB result dict.
        """
        from sqlalchemy import text
        wkt = geometry.wkt
        with self._engine.connect() as conn:
            result = conn.execute(
                text("SELECT adminbounds.infer_admin_semantic_relation(ST_GeomFromText(:wkt, 4326))"),
                {"wkt": wkt},
            ).scalar()
        if isinstance(result, str):
            return json.loads(result)
        return result or {}

    def download_gadm(
        self,
        country: str,
        levels: list[int] = None,
        cache_dir: "str | Path" = None,
        force: bool = False,
    ) -> int:
        """Download and import GADM 4.1 boundaries for a country.

        Args:
            country: ISO3 code (e.g. "DEU") or English country name (e.g. "Germany").
            levels: GADM levels to import (0=country, 1=province, 2=city, 3=district).
                    Default: all available levels [0, 1, 2, 3].
            cache_dir: Directory to cache downloaded zip files.
                       Default: ~/.adminbounds/gadm_cache/
            force: Re-download even if already cached.

        Returns:
            Total number of rows upserted into adminbounds.admin_units.
        """
        from pathlib import Path as _Path
        from ._gadm import download_gadm
        return download_gadm(
            country, self._engine,
            levels=levels,
            cache_dir=_Path(cache_dir) if cache_dir else None,
            force=force,
        )

    def diagnose(
        self,
        source_table: str,
        geom_col: str = "geom",
        schema: str = "public",
    ) -> dict:
        """Run diagnostic checks. Returns structured result dict."""
        from ._diagnose import diagnose
        conn = get_raw_connection(self._settings)
        try:
            return diagnose(conn, source_table, geom_col, schema)
        finally:
            conn.close()
