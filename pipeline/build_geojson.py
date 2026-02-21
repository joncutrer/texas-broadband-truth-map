"""
build_geojson.py — Spatial join of FCC + Ookla data → county-level GeoJSON.

Pipeline:
  1. Load fcc_tx_fwa.parquet        (from fetch_fcc.py)
  2. Load ookla_tx_{y}_q{q}.parquet (from fetch_ookla.py)
  3. Load Texas county boundaries   (from TIGER/Line shapefiles or USCB API)
  4. Spatially join FCC points → counties  (point-in-polygon)
  5. Convert Ookla quadkey tiles → polygons, spatially join → counties
  6. Aggregate per county:
       - Top fixed wireless providers (by # of locations served)
       - Max FCC claimed download/upload speed (per county)
       - Average Ookla actual download/upload speed (weighted by test count)
       - Overstatement ratio = fcc_max_dl / ookla_avg_dl
  7. Merge aggregates onto county GeoJSON features
  8. Write:
       data/processed/counties.geojson
       data/processed/providers.json

Usage:
    python pipeline/build_geojson.py --year 2023 --quarter 4

Dependencies:
    pip install geopandas shapely pyarrow pandas tqdm

County boundaries source (TIGER/Line):
    https://www2.census.gov/geo/tiger/TIGER2023/COUNTY/
    → tl_2023_us_county.zip  (filter to STATE_FP == '48' for Texas)
"""

import argparse
import json
import math
import sys
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import Polygon
from tqdm import tqdm

RAW_DIR       = Path(__file__).parent.parent / "data" / "raw"
PROCESSED_DIR = Path(__file__).parent.parent / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

TIGER_COUNTY_URL = (
    "https://www2.census.gov/geo/tiger/TIGER2023/COUNTY/tl_2023_us_county.zip"
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def quadkey_to_bbox(quadkey: str):
    """Return (min_lon, min_lat, max_lon, max_lat) for a quadkey tile."""
    x = y = 0
    zoom = len(quadkey)
    for i in range(zoom):
        mask = 1 << (zoom - 1 - i)
        digit = int(quadkey[i])
        if digit & 1:
            x |= mask
        if digit & 2:
            y |= mask

    n = 2 ** zoom
    lon_min = (x / n) * 360.0 - 180.0
    lon_max = ((x + 1) / n) * 360.0 - 180.0

    def merc_to_lat(my):
        return math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * my / n))))

    lat_max = merc_to_lat(y)
    lat_min = merc_to_lat(y + 1)
    return lon_min, lat_min, lon_max, lat_max


def quadkey_to_polygon(quadkey: str) -> Polygon:
    lon_min, lat_min, lon_max, lat_max = quadkey_to_bbox(quadkey)
    return Polygon([
        (lon_min, lat_min), (lon_max, lat_min),
        (lon_max, lat_max), (lon_min, lat_max),
        (lon_min, lat_min),
    ])


def load_county_boundaries() -> gpd.GeoDataFrame:
    """Load Texas county boundaries from TIGER/Line or a cached shapefile."""
    cached = RAW_DIR / "tl_2023_us_county" / "tl_2023_us_county.shp"
    if not cached.exists():
        zip_path = RAW_DIR / "tl_2023_us_county.zip"
        if not zip_path.exists():
            print(f"[build] Downloading TIGER/Line county boundaries from {TIGER_COUNTY_URL} …")
            import urllib.request
            urllib.request.urlretrieve(TIGER_COUNTY_URL, zip_path)
        import zipfile
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(RAW_DIR / "tl_2023_us_county")

    gdf = gpd.read_file(cached)
    gdf = gdf[gdf["STATEFP"] == "48"].copy()
    gdf = gdf.rename(columns={"GEOID": "GEOID", "NAME": "NAME"})
    return gdf.to_crs("EPSG:4326")


# ── Main ──────────────────────────────────────────────────────────────────────

def build_geojson(year: int, quarter: int) -> None:
    # 1. Load FCC data
    fcc_path = RAW_DIR / "fcc_tx_fwa.parquet"
    if not fcc_path.exists():
        print(f"[build] Missing {fcc_path}. Run fetch_fcc.py first.")
        sys.exit(1)
    print("[build] Loading FCC data …")
    fcc = pd.read_parquet(fcc_path)
    fcc_gdf = gpd.GeoDataFrame(
        fcc,
        geometry=gpd.points_from_xy(fcc["longitude"], fcc["latitude"]),
        crs="EPSG:4326",
    )

    # 2. Load Ookla data
    ookla_path = RAW_DIR / f"ookla_tx_{year}_q{quarter}.parquet"
    if not ookla_path.exists():
        print(f"[build] Missing {ookla_path}. Run fetch_ookla.py first.")
        sys.exit(1)
    print("[build] Loading Ookla data …")
    ookla = pd.read_parquet(ookla_path)
    print(f"  {len(ookla):,} Ookla tiles")

    # 3. County boundaries
    print("[build] Loading county boundaries …")
    counties = load_county_boundaries()
    print(f"  {len(counties)} Texas counties")

    # 4. FCC → county join
    print("[build] Joining FCC points → counties …")
    fcc_joined = gpd.sjoin(fcc_gdf, counties[["GEOID", "NAME", "geometry"]],
                           how="left", predicate="within")
    fcc_joined = fcc_joined.dropna(subset=["GEOID"])

    # Aggregate FCC: max claimed speed per county, top providers
    fcc_county = (
        fcc_joined.groupby("GEOID")
        .agg(
            fcc_claimed_down_mbps=("max_advertised_download_speed", "max"),
            fcc_claimed_up_mbps=("max_advertised_upload_speed", "max"),
        )
        .reset_index()
    )

    provider_counts = (
        fcc_joined.groupby(["GEOID", "brand_name"])
        .size()
        .reset_index(name="loc_count")
        .sort_values(["GEOID", "loc_count"], ascending=[True, False])
    )
    top_providers = (
        provider_counts.groupby("GEOID")
        .apply(lambda g: g.head(3)["brand_name"].tolist())
        .reset_index(name="top_providers")
    )

    # 5. Ookla → county join (tile polygons)
    print("[build] Converting Ookla quadkeys → polygons (this may take a while) …")
    ookla_geoms = [quadkey_to_polygon(qk) for qk in tqdm(ookla["quadkey"])]
    ookla_gdf = gpd.GeoDataFrame(ookla, geometry=ookla_geoms, crs="EPSG:4326")

    print("[build] Joining Ookla tiles → counties …")
    ookla_joined = gpd.sjoin(
        ookla_gdf[["quadkey", "avg_d_mbps", "avg_u_mbps", "tests", "geometry"]],
        counties[["GEOID", "geometry"]],
        how="left",
        predicate="intersects",
    ).dropna(subset=["GEOID"])

    ookla_county = (
        ookla_joined.groupby("GEOID")
        .apply(lambda g: pd.Series({
            "ookla_actual_down_mbps": (g["avg_d_mbps"] * g["tests"]).sum() / g["tests"].sum(),
            "ookla_actual_up_mbps":   (g["avg_u_mbps"] * g["tests"]).sum() / g["tests"].sum(),
        }))
        .reset_index()
    )

    # 6. Merge onto county GeoDataFrame
    print("[build] Merging aggregates …")
    result = counties.merge(fcc_county,   on="GEOID", how="left")
    result = result.merge(ookla_county,   on="GEOID", how="left")
    result = result.merge(top_providers,  on="GEOID", how="left")

    result["fcc_claimed_down_mbps"].fillna(25.0, inplace=True)
    result["fcc_claimed_up_mbps"].fillna(3.0,   inplace=True)
    result["ookla_actual_down_mbps"].fillna(10.0, inplace=True)
    result["ookla_actual_up_mbps"].fillna(2.0,   inplace=True)
    result["top_providers"].fillna("", inplace=True)
    result["top_providers"] = result["top_providers"].apply(
        lambda x: x if isinstance(x, list) else []
    )

    result["overstatement_ratio"] = (
        result["fcc_claimed_down_mbps"] / result["ookla_actual_down_mbps"]
    ).round(2)

    # 7. Write counties.geojson
    out_geojson = PROCESSED_DIR / "counties.geojson"
    print(f"[build] Writing {out_geojson} …")
    result[["GEOID", "NAME", "geometry",
            "fcc_claimed_down_mbps", "fcc_claimed_up_mbps",
            "ookla_actual_down_mbps", "ookla_actual_up_mbps",
            "overstatement_ratio", "top_providers"]].to_file(
        out_geojson, driver="GeoJSON"
    )

    # 8. Write providers.json
    out_providers = PROCESSED_DIR / "providers.json"
    print(f"[build] Writing {out_providers} …")
    providers_dict = {}
    for _, row in fcc_joined.iterrows():
        geoid = row.get("GEOID")
        if not geoid or pd.isna(geoid):
            continue
        entry = {
            "name":           row.get("brand_name", "Unknown"),
            "fcc_claimed_down": float(row.get("max_advertised_download_speed", 0) or 0),
            "fcc_claimed_up":   float(row.get("max_advertised_upload_speed", 0) or 0),
            "tech_code": 70,
        }
        providers_dict.setdefault(str(geoid), []).append(entry)
    with open(out_providers, "w") as f:
        json.dump(providers_dict, f)

    print(f"[build] Done. {len(result)} counties written.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build county GeoJSON from FCC + Ookla data.")
    parser.add_argument("--year",    type=int, default=2023)
    parser.add_argument("--quarter", type=int, default=4, choices=[1, 2, 3, 4])
    args = parser.parse_args()
    build_geojson(args.year, args.quarter)
