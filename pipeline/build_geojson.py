"""
build_geojson.py — Build county GeoJSON from FCC BDC data.

Pipeline:
  1. Load fcc_tx_fwa.parquet        (from fetch_fcc.py)
  2. Load Texas county boundaries   (from local zip in data/raw/)
  3. Aggregate FCC data by county_geoid (no spatial join needed)
  4. Optionally load Ookla data     (if parquet exists in data/raw/)
  5. Merge aggregates onto county GeoDataFrame
  6. Write:
       data/processed/counties.geojson
       data/processed/providers.json

Usage:
    uv run python pipeline/build_geojson.py [--year 2023] [--quarter 4]
"""

import argparse
import io
import json
import math
import sys
import zipfile
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import Polygon

RAW_DIR       = Path(__file__).parent.parent / "data" / "raw"
PROCESSED_DIR = Path(__file__).parent.parent / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


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
    """Load Texas county boundaries from the local zip in data/raw/."""
    matches = list(RAW_DIR.glob("Texas_County_Boundaries_*.zip"))
    if not matches:
        print(
            "[build] ERROR: No Texas_County_Boundaries_*.zip found in data/raw/.\n"
            "  Expected: Texas_County_Boundaries_*.zip"
        )
        sys.exit(1)

    zip_path = matches[0]
    print(f"[build] Loading county boundaries from {zip_path.name} …")

    with zipfile.ZipFile(zip_path) as zf:
        geojson_files = [n for n in zf.namelist() if n.endswith(".geojson")]
        if not geojson_files:
            print(f"[build] ERROR: No .geojson file found inside {zip_path.name}")
            sys.exit(1)
        geojson_name = geojson_files[0]
        geojson_bytes = zf.read(geojson_name)

    gdf = gpd.read_file(io.BytesIO(geojson_bytes))

    # Rename local-zip fields to match the rest of the pipeline
    # Local zip: FIPS_ST_CNTY_CD (county GEOID), CNTY_NM (county name)
    # Pipeline:  GEOID, NAME
    gdf = gdf.rename(columns={
        "FIPS_ST_CNTY_CD": "GEOID",
        "CNTY_NM":         "NAME",
    })
    gdf = gdf[["GEOID", "NAME", "geometry"]].copy()

    # Ensure CRS is set (GeoJSON may not include explicit CRS member)
    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")
    else:
        gdf = gdf.to_crs("EPSG:4326")

    print(f"[build]   {len(gdf)} Texas counties loaded")
    return gdf


# ── Main ──────────────────────────────────────────────────────────────────────

def build_geojson(year: int, quarter: int) -> None:
    # ── 1. Load FCC data ──────────────────────────────────────────────────────
    fcc_path = RAW_DIR / "fcc_tx_fwa.parquet"
    if not fcc_path.exists():
        print(f"[build] Missing {fcc_path}. Run fetch_fcc.py first.")
        sys.exit(1)
    print("[build] Loading FCC data …")
    fcc = pd.read_parquet(fcc_path)
    print(f"[build]   {len(fcc):,} FCC rows, {fcc['county_geoid'].nunique()} unique counties")

    # ── 2. Load county boundaries ─────────────────────────────────────────────
    counties = load_county_boundaries()

    # ── 3. Aggregate FCC by county — no spatial join needed ──────────────────
    # county_geoid was derived from block_geoid[:5] in fetch_fcc.py
    print("[build] Aggregating FCC data by county …")
    fcc_county = (
        fcc.groupby("county_geoid")
        .agg(
            fcc_claimed_down_mbps=("max_advertised_download_speed", "max"),
            fcc_claimed_up_mbps=("max_advertised_upload_speed", "max"),
        )
        .reset_index()
        .rename(columns={"county_geoid": "GEOID"})
    )

    # Top 3 providers per county by unique location count
    provider_counts = (
        fcc.groupby(["county_geoid", "brand_name"])["location_id"]
        .nunique()
        .reset_index(name="loc_count")
        .sort_values(["county_geoid", "loc_count"], ascending=[True, False])
    )
    top_providers = (
        provider_counts.groupby("county_geoid")
        .apply(lambda g: g.head(3)["brand_name"].tolist(), include_groups=False)
        .reset_index(name="top_providers")
        .rename(columns={"county_geoid": "GEOID"})
    )

    # ── 4. Ookla data (optional) ──────────────────────────────────────────────
    ookla_path = RAW_DIR / f"ookla_tx_{year}_q{quarter}.parquet"
    has_ookla = ookla_path.exists()

    if has_ookla:
        print(f"[build] Loading Ookla data from {ookla_path.name} …")
        ookla = pd.read_parquet(ookla_path)
        print(f"[build]   {len(ookla):,} Ookla tiles")

        print("[build] Converting Ookla quadkeys → polygons …")
        ookla_geoms = [quadkey_to_polygon(qk) for qk in ookla["quadkey"]]
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
            .apply(
                lambda g: pd.Series({
                    "ookla_actual_down_mbps": (
                        (g["avg_d_mbps"] * g["tests"]).sum() / g["tests"].sum()
                    ),
                    "ookla_actual_up_mbps": (
                        (g["avg_u_mbps"] * g["tests"]).sum() / g["tests"].sum()
                    ),
                }),
                include_groups=False,
            )
            .reset_index()
        )
    else:
        print(
            f"[build] Ookla data not found at {ookla_path.name}. "
            "Skipping — using defaults (10/2 Mbps). "
            "Run fetch_ookla.py to enable real measurements."
        )
        ookla_county = None

    # ── 5. Merge aggregates onto county GeoDataFrame ──────────────────────────
    print("[build] Merging aggregates …")
    result = counties.merge(fcc_county,   on="GEOID", how="left")
    result = result.merge(top_providers,  on="GEOID", how="left")

    if ookla_county is not None:
        result = result.merge(ookla_county, on="GEOID", how="left")
    else:
        result["ookla_actual_down_mbps"] = 10.0
        result["ookla_actual_up_mbps"]   = 2.0

    # Fill defaults for counties with no FCC data
    result["fcc_claimed_down_mbps"]  = result["fcc_claimed_down_mbps"].fillna(25.0)
    result["fcc_claimed_up_mbps"]    = result["fcc_claimed_up_mbps"].fillna(3.0)
    result["ookla_actual_down_mbps"] = result["ookla_actual_down_mbps"].fillna(10.0)
    result["ookla_actual_up_mbps"]   = result["ookla_actual_up_mbps"].fillna(2.0)
    result["top_providers"] = result["top_providers"].apply(
        lambda x: x if isinstance(x, list) else []
    )

    result["overstatement_ratio"] = (
        result["fcc_claimed_down_mbps"] / result["ookla_actual_down_mbps"]
    ).round(2)

    # ── 6. Write counties.geojson ─────────────────────────────────────────────
    out_geojson = PROCESSED_DIR / "counties.geojson"
    print(f"[build] Writing {out_geojson} …")
    result[[
        "GEOID", "NAME", "geometry",
        "fcc_claimed_down_mbps", "fcc_claimed_up_mbps",
        "ookla_actual_down_mbps", "ookla_actual_up_mbps",
        "overstatement_ratio", "top_providers",
    ]].to_file(out_geojson, driver="GeoJSON")

    # ── 7. Write providers.json ───────────────────────────────────────────────
    out_providers = PROCESSED_DIR / "providers.json"
    print(f"[build] Writing {out_providers} …")

    # Aggregate by (county_geoid, brand_name) — one entry per provider per county
    provider_agg = (
        fcc.groupby(["county_geoid", "brand_name"])
        .agg(
            fcc_claimed_down=("max_advertised_download_speed", "max"),
            fcc_claimed_up=("max_advertised_upload_speed", "max"),
            tech_code=("technology", "first"),
        )
        .reset_index()
    )

    providers_dict = {}
    for _, row in provider_agg.iterrows():
        geoid = str(row["county_geoid"])
        entry = {
            "name":             row["brand_name"],
            "fcc_claimed_down": float(row["fcc_claimed_down"] or 0),
            "fcc_claimed_up":   float(row["fcc_claimed_up"] or 0),
            "tech_code":        int(row["tech_code"]),
        }
        providers_dict.setdefault(geoid, []).append(entry)

    with open(out_providers, "w") as f:
        json.dump(providers_dict, f)

    print(f"[build] Done. {len(result)} counties written.")
    if not has_ookla:
        print(
            "[build] NOTE: Ookla data unavailable — overstatement ratios use "
            "default 10 Mbps actual-speed baseline."
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Build county GeoJSON from FCC BDC + optional Ookla data."
    )
    parser.add_argument("--year",    type=int, default=2023)
    parser.add_argument("--quarter", type=int, default=4, choices=[1, 2, 3, 4])
    args = parser.parse_args()
    build_geojson(args.year, args.quarter)
