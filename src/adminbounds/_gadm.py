"""
GADM 4.1 worldwide admin boundary downloader and importer.

Downloads GeoJSON zips from the GADM CDN, maps fields to the admin_units schema,
and upserts via the existing staging pipeline.
"""

from __future__ import annotations

import io
import zipfile
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import HTTPError

import geopandas as gpd
from shapely.geometry import shape
from tqdm import tqdm

from ._import import _upsert_staging, _compute_derived_fields

# ---------------------------------------------------------------------------
# Country name → ISO3 lookup (lowercase keys)
# ---------------------------------------------------------------------------
_COUNTRY_ISO3: dict[str, str] = {
    "afghanistan": "AFG", "albania": "ALB", "algeria": "DZA", "andorra": "AND",
    "angola": "AGO", "argentina": "ARG", "armenia": "ARM", "australia": "AUS",
    "austria": "AUT", "azerbaijan": "AZE", "bahrain": "BHR", "bangladesh": "BGD",
    "belarus": "BLR", "belgium": "BEL", "belize": "BLZ", "benin": "BEN",
    "bhutan": "BTN", "bolivia": "BOL", "bosnia and herzegovina": "BIH",
    "botswana": "BWA", "brazil": "BRA", "brunei": "BRN", "bulgaria": "BGR",
    "burkina faso": "BFA", "burundi": "BDI", "cambodia": "KHM", "cameroon": "CMR",
    "canada": "CAN", "central african republic": "CAF", "chad": "TCD",
    "chile": "CHL", "china": "CHN", "colombia": "COL", "congo": "COG",
    "democratic republic of the congo": "COD", "dr congo": "COD",
    "costa rica": "CRI", "croatia": "HRV", "cuba": "CUB", "cyprus": "CYP",
    "czech republic": "CZE", "czechia": "CZE", "denmark": "DNK", "djibouti": "DJI",
    "dominican republic": "DOM", "ecuador": "ECU", "egypt": "EGY",
    "el salvador": "SLV", "eritrea": "ERI", "estonia": "EST", "eswatini": "SWZ",
    "ethiopia": "ETH", "finland": "FIN", "france": "FRA", "gabon": "GAB",
    "gambia": "GMB", "georgia": "GEO", "germany": "DEU", "ghana": "GHA",
    "greece": "GRC", "guatemala": "GTM", "guinea": "GIN",
    "guinea-bissau": "GNB", "guyana": "GUY", "haiti": "HTI", "honduras": "HND",
    "hungary": "HUN", "iceland": "ISL", "india": "IND", "indonesia": "IDN",
    "iran": "IRN", "iraq": "IRQ", "ireland": "IRL", "israel": "ISR",
    "italy": "ITA", "jamaica": "JAM", "japan": "JPN", "jordan": "JOR",
    "kazakhstan": "KAZ", "kenya": "KEN", "kuwait": "KWT", "kyrgyzstan": "KGZ",
    "laos": "LAO", "latvia": "LVA", "lebanon": "LBN", "lesotho": "LSO",
    "liberia": "LBR", "libya": "LBY", "liechtenstein": "LIE", "lithuania": "LTU",
    "luxembourg": "LUX", "madagascar": "MDG", "malawi": "MWI", "malaysia": "MYS",
    "mali": "MLI", "malta": "MLT", "mauritania": "MRT", "mexico": "MEX",
    "moldova": "MDA", "mongolia": "MNG", "montenegro": "MNE", "morocco": "MAR",
    "mozambique": "MOZ", "myanmar": "MMR", "namibia": "NAM", "nepal": "NPL",
    "netherlands": "NLD", "new zealand": "NZL", "nicaragua": "NIC",
    "niger": "NER", "nigeria": "NGA", "north korea": "PRK",
    "north macedonia": "MKD", "norway": "NOR", "oman": "OMN", "pakistan": "PAK",
    "panama": "PAN", "papua new guinea": "PNG", "paraguay": "PRY", "peru": "PER",
    "philippines": "PHL", "poland": "POL", "portugal": "PRT", "qatar": "QAT",
    "romania": "ROU", "russia": "RUS", "rwanda": "RWA", "saudi arabia": "SAU",
    "senegal": "SEN", "serbia": "SRB", "sierra leone": "SLE", "singapore": "SGP",
    "slovakia": "SVK", "slovenia": "SVN", "somalia": "SOM", "south africa": "ZAF",
    "south korea": "KOR", "south sudan": "SSD", "spain": "ESP", "sri lanka": "LKA",
    "sudan": "SDN", "sweden": "SWE", "switzerland": "CHE", "syria": "SYR",
    "taiwan": "TWN", "tajikistan": "TJK", "tanzania": "TZA", "thailand": "THA",
    "timor-leste": "TLS", "east timor": "TLS", "togo": "TGO", "tunisia": "TUN",
    "turkey": "TUR", "turkiye": "TUR", "turkmenistan": "TKM", "uganda": "UGA",
    "ukraine": "UKR", "united arab emirates": "ARE", "uae": "ARE",
    "united kingdom": "GBR", "uk": "GBR", "great britain": "GBR",
    "united states": "USA", "united states of america": "USA", "usa": "USA",
    "us": "USA", "uruguay": "URY", "uzbekistan": "UZB", "venezuela": "VEN",
    "vietnam": "VNM", "viet nam": "VNM", "yemen": "YEM", "zambia": "ZMB",
    "zimbabwe": "ZWE",
}


def _resolve_iso3(country: str) -> str:
    """Resolve a country name or ISO3 code to uppercase ISO3."""
    upper = country.strip().upper()
    # Direct ISO3 match (3-letter alpha)
    if len(upper) == 3 and upper.isalpha():
        return upper
    # Name lookup
    lower = country.strip().lower()
    if lower in _COUNTRY_ISO3:
        return _COUNTRY_ISO3[lower]
    # Partial suggestions
    suggestions = [k for k in _COUNTRY_ISO3 if lower in k or k in lower]
    msg = f"Country not recognised: {country!r}."
    if suggestions:
        msg += f" Did you mean: {', '.join(suggestions[:5])}?"
    raise ValueError(msg)


def _gadm_url(iso3: str, level: int) -> str:
    return f"https://geodata.ucdavis.edu/gadm/gadm4.1/json/gadm41_{iso3}_{level}.json.zip"


def _download_file(url: str, dest: Path, force: bool) -> Path | None:
    """Download *url* to *dest*; return None on HTTP 404."""
    if dest.exists() and not force:
        return dest

    print(f"  Downloading {url} ...")
    try:
        req = Request(url, headers={"User-Agent": "adminbounds/1.0"})
        with urlopen(req) as response:
            total = int(response.headers.get("Content-Length", 0))
            dest.parent.mkdir(parents=True, exist_ok=True)
            buf = io.BytesIO()
            with tqdm(
                total=total or None,
                unit="B",
                unit_scale=True,
                desc=dest.name,
                leave=False,
            ) as pbar:
                while True:
                    chunk = response.read(65536)
                    if not chunk:
                        break
                    buf.write(chunk)
                    pbar.update(len(chunk))
            dest.write_bytes(buf.getvalue())
    except HTTPError as exc:
        if exc.code == 404:
            print(f"    Level not available (HTTP 404): {url}")
            return None
        raise
    return dest


def _extract_json(zip_path: Path) -> dict:
    """Extract and parse the first .json file from a zip archive."""
    import json
    with zipfile.ZipFile(zip_path) as zf:
        json_names = [n for n in zf.namelist() if n.endswith(".json")]
        if not json_names:
            raise ValueError(f"No .json file found in {zip_path}")
        with zf.open(json_names[0]) as f:
            return json.load(f)


def _parse_gadm_features(data: dict, gadm_level: int) -> list[dict]:
    """Map GeoJSON features from a GADM file to admin_units row dicts."""
    gid_key    = f"GID_{gadm_level}"
    name_key   = f"NAME_{gadm_level}"
    parent_key = f"GID_{gadm_level - 1}" if gadm_level > 0 else None
    db_level   = gadm_level + 1  # GADM 0→level 1, GADM 1→level 2, …

    rows = []
    for feature in data.get("features", []):
        props = feature.get("properties", {}) or {}
        geom_data = feature.get("geometry")

        adcode = props.get(gid_key)
        if not adcode or not geom_data:
            continue

        name        = props.get(name_key) or adcode
        parent_code = props.get(parent_key) if parent_key else None
        geometry    = shape(geom_data)

        rows.append({
            "adcode":      adcode,
            "name":        name,
            "level":       db_level,
            "parent_code": parent_code,
            "geometry":    geometry,
        })
    return rows


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def download_gadm(
    country: str,
    engine,
    levels: list[int] | None = None,
    cache_dir: Path | None = None,
    force: bool = False,
) -> int:
    """Download and import GADM 4.1 boundaries for a country.

    Args:
        country:   ISO3 code (e.g. "DEU") or English name (e.g. "Germany").
        engine:    SQLAlchemy engine connected to the adminbounds DB.
        levels:    GADM levels to import (0=country … 3=district).
                   Default: all available [0, 1, 2, 3].
        cache_dir: Directory for cached zip files.
                   Default: ~/.adminbounds/gadm_cache/
        force:     Re-download even if already cached.

    Returns:
        Total rows upserted into adminbounds.admin_units.
    """
    if levels is None:
        levels = [0, 1, 2, 3]

    iso3 = _resolve_iso3(country)
    print(f"Resolved '{country}' → ISO3={iso3}")

    if cache_dir is None:
        cache_dir = Path.home() / ".adminbounds" / "gadm_cache"
    cache_dir = Path(cache_dir)

    all_rows: list[dict] = []

    for lvl in levels:
        url      = _gadm_url(iso3, lvl)
        zip_path = cache_dir / f"gadm41_{iso3}_{lvl}.json.zip"

        zip_file = _download_file(url, zip_path, force)
        if zip_file is None:
            continue  # level not available

        print(f"  Parsing level {lvl} ...")
        data = _extract_json(zip_file)
        rows = _parse_gadm_features(data, lvl)
        print(f"    → {len(rows)} features")
        all_rows.extend(rows)

    if not all_rows:
        print("No data downloaded.")
        return 0

    # Deduplicate by adcode
    seen: dict[str, dict] = {}
    for row in all_rows:
        seen[row["adcode"]] = row
    deduped = list(seen.values())
    print(f"Total unique units: {len(deduped)}")

    print("Upserting into adminbounds.admin_units ...")
    gdf = gpd.GeoDataFrame(deduped, crs="EPSG:4326")
    gdf = gdf.rename_geometry("geom")
    _upsert_staging(engine, gdf)
    print("  Upsert complete.")

    _compute_derived_fields(engine)

    return len(deduped)
