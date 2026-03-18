"""
GeoJSON upload to PostgreSQL public schema with uuid primary key.
"""

import uuid
from pathlib import Path

import geopandas as gpd
from sqlalchemy import text


def upload_geojson(engine, path: str | Path, table_name: str, if_exists: str = "replace") -> int:
    """Upload a GeoJSON file to public.<table_name> with uuid primary key.

    Returns the number of features uploaded.
    """
    path = Path(path)
    print(f"Reading {path.name}...")
    gdf = gpd.read_file(path)
    print(f"  {len(gdf)} features, CRS: {gdf.crs}")

    if gdf.crs is None:
        print("  Warning: no CRS detected, assuming EPSG:4326")
        gdf = gdf.set_crs("EPSG:4326")
    elif gdf.crs.to_epsg() != 4326:
        print(f"  Reprojecting from {gdf.crs} to EPSG:4326...")
        gdf = gdf.to_crs("EPSG:4326")

    with engine.begin() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))

    gdf = gdf.rename_geometry("geom")
    gdf.insert(0, "uuid", [str(uuid.uuid4()) for _ in range(len(gdf))])

    print(f"Uploading to public.{table_name} (if_exists='{if_exists}')...")
    gdf.to_postgis(
        table_name,
        engine,
        schema="public",
        if_exists=if_exists,
        index=False,
    )

    with engine.begin() as conn:
        conn.execute(text(f'ALTER TABLE public."{table_name}" ALTER COLUMN uuid TYPE UUID USING uuid::UUID'))
        conn.execute(text(f'ALTER TABLE public."{table_name}" ADD PRIMARY KEY (uuid)'))

    print(f"Done. Table public.{table_name} ready (uuid primary key added).")
    return len(gdf)
