"""
adminbounds CLI entry point.

Usage:
    adminbounds [--host H] [--port P] [--dbname DB] [--user U] [--password PW]
                [--db-url URL] [--admin-schema SCHEMA]
                <command> [options]

Commands:
    init-db             Create schema + tables + deploy SQL function
    import-boundaries   Load bundled Chinese admin boundaries into DB
    upload FILE TABLE   Upload GeoJSON file → public.<TABLE> with uuid PK
    annotate            Batch-annotate a source table
    diagnose            Diagnose annotation issues
    download-gadm       Download GADM 4.1 worldwide admin boundaries
"""

import argparse
import sys
from pathlib import Path


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="adminbounds",
        description="Administrative boundary semantic relation inference for geospatial datasets",
    )

    # Shared connection options
    conn = parser.add_argument_group("connection options (fall back to ADMINBOUNDS_DB_* env vars)")
    conn.add_argument("--host",         default=None, help="PostgreSQL host")
    conn.add_argument("--port",         type=int, default=None, help="PostgreSQL port")
    conn.add_argument("--dbname",       default=None, help="Database name")
    conn.add_argument("--user",         default=None, help="Database user")
    conn.add_argument("--password",     default=None, help="Database password")
    conn.add_argument("--db-url",       default=None, help="Full SQLAlchemy DB URL (overrides individual params)")
    conn.add_argument("--admin-schema", default="adminbounds", help="Admin schema name (default: adminbounds)")

    sub = parser.add_subparsers(dest="command", metavar="<command>")
    sub.required = True

    # init-db
    sub.add_parser("init-db", help="Create schema + tables + deploy SQL function")

    # import-boundaries
    sub.add_parser("import-boundaries", help="Load bundled Chinese admin boundaries into DB")

    # upload
    p_upload = sub.add_parser("upload", help="Upload GeoJSON file to public.<TABLE>")
    p_upload.add_argument("file",  type=Path, help="Path to GeoJSON file")
    p_upload.add_argument("table", help="Target table name")
    p_upload.add_argument(
        "--if-exists",
        choices=["replace", "append", "fail"],
        default="replace",
        help="What to do if table already exists (default: replace)",
    )

    # annotate
    p_annotate = sub.add_parser("annotate", help="Batch-annotate a source table")
    p_annotate.add_argument("--source-table", required=True, help="Source table name (schema-qualified accepted, e.g. myschema.mytable)")
    p_annotate.add_argument("--geom-col",     default="geom", help="Geometry column (default: geom)")
    p_annotate.add_argument("--schema",        default="public", help="Source table schema (default: public; ignored if --source-table is schema-qualified)")
    p_annotate.add_argument("--batch-size",   type=int, default=100, help="Rows per batch (default: 100)")
    p_annotate.add_argument(
        "--mode",
        choices=["skip", "update", "replace"],
        default="skip",
        help="Re-annotation strategy: skip=new rows only (default), update=overwrite existing, replace=delete+rerun",
    )

    # diagnose
    p_diagnose = sub.add_parser("diagnose", help="Diagnose annotation issues")
    p_diagnose.add_argument("--source-table", required=True, help="Source table name")
    p_diagnose.add_argument("--geom-col",     default="geom", help="Geometry column (default: geom)")
    p_diagnose.add_argument("--schema",        default="public", help="Source table schema (default: public)")

    # download-gadm
    p_gadm = sub.add_parser(
        "download-gadm",
        help="Download and import GADM 4.1 worldwide admin boundaries",
    )
    p_gadm.add_argument(
        "country",
        help="ISO3 code (e.g. DEU) or English country name (e.g. Germany)",
    )
    p_gadm.add_argument(
        "--levels",
        default=None,
        help="Comma-separated GADM levels to download (default: 0,1,2,3)",
    )
    p_gadm.add_argument(
        "--cache-dir",
        default=None,
        help="Directory to cache downloads (default: ~/.adminbounds/gadm_cache/)",
    )
    p_gadm.add_argument(
        "--force",
        action="store_true",
        help="Re-download even if already cached",
    )

    return parser


def main():
    from dotenv import load_dotenv
    load_dotenv()

    parser = _build_parser()
    args = parser.parse_args()

    from adminbounds import AdminBoundsClient

    client = AdminBoundsClient(
        host=args.host,
        port=args.port,
        dbname=args.dbname,
        user=args.user,
        password=args.password,
        admin_schema=args.admin_schema,
        db_url=args.db_url,
    )

    if args.command == "init-db":
        client.init_db()
        print("Database initialized.")

    elif args.command == "import-boundaries":
        count = client.import_boundaries()
        print(f"Loaded {count} unique admin boundaries.")

    elif args.command == "upload":
        if not args.file.exists():
            print(f"Error: file not found: {args.file}", file=sys.stderr)
            sys.exit(1)
        count = client.upload(args.file, args.table, args.if_exists)
        print(f"Uploaded {count} features to public.{args.table}.")

    elif args.command == "annotate":
        count = client.annotate(
            source_table=args.source_table,
            geom_col=args.geom_col,
            schema=args.schema,
            batch_size=args.batch_size,
            mode=args.mode,
        )
        print(f"Annotated {count} rows.")

    elif args.command == "diagnose":
        client.diagnose(
            source_table=args.source_table,
            geom_col=args.geom_col,
            schema=args.schema,
        )

    elif args.command == "download-gadm":
        levels = [int(x) for x in args.levels.split(",")] if args.levels else None
        count = client.download_gadm(
            args.country,
            levels=levels,
            cache_dir=args.cache_dir,
            force=args.force,
        )
        print(f"Imported {count} admin units for '{args.country}'.")
