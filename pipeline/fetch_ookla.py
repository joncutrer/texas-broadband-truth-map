"""
fetch_ookla.py — Download Ookla Open Data speed test tiles for Texas.

Ookla publishes quarterly Parquet files of aggregated speed-test results
at the quadkey tile level (zoom 16). Each row covers roughly 600 m × 600 m.

Source: https://github.com/teamookla/ookla-open-data
Data released under the Ookla Open Data License.

This script:
  1. Downloads the specified quarter's "fixed" (non-mobile) tile Parquet from
     the Ookla Open Data S3 bucket.
  2. Clips to Texas bbox (lon -106.7 → -93.5, lat 25.8 → 36.5).
  3. Writes data/raw/ookla_tx_{year}_{quarter}.parquet.

Usage:
    python pipeline/fetch_ookla.py --year 2023 --quarter 4

Column reference (from Ookla schema):
    quadkey         str     Bing Maps quadkey (zoom 16)
    avg_d_mbps      float   Average download speed (Mbps)
    avg_u_mbps      float   Average upload speed (Mbps)
    avg_lat_ms      float   Average latency (ms)
    tests           int     Number of tests in tile
    devices         int     Number of unique devices
"""

import argparse
import sys
from pathlib import Path

import pandas as pd
import requests
from tqdm import tqdm

RAW_DIR = Path(__file__).parent.parent / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

# Ookla open data on S3 — path pattern from the GitHub repo README.
OOKLA_S3_URL_TEMPLATE = (
    "https://ookla-open-data.s3.amazonaws.com/parquet/performance/"
    "type=fixed/year={year}/quarter={quarter}/"
    "2023-10-01_performance_fixed_tiles.parquet"
    # NOTE: actual filename varies by quarter — check the GitHub repo for the
    # exact filename, e.g.:
    # https://github.com/teamookla/ookla-open-data/tree/master/tutorials
)

# Texas bounding box
TX_BBOX = {"lon_min": -106.7, "lon_max": -93.5, "lat_min": 25.8, "lat_max": 36.5}


def quadkey_to_lon_lat(quadkey: str):
    """Convert a zoom-16 quadkey to the centroid (lon, lat) of that tile."""
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
    lon = (x / n) * 360.0 - 180.0
    import math
    lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * y / n)))
    lat = math.degrees(lat_rad)
    return lon, lat


def fetch_ookla_data(year: int, quarter: int) -> None:
    out_file = RAW_DIR / f"ookla_tx_{year}_q{quarter}.parquet"
    if out_file.exists():
        print(f"[fetch_ookla] Output already exists: {out_file}. Delete to re-download.")
        return

    # Build the URL — adjust filename to match the actual Ookla release.
    # The true URL for each quarter can be found at:
    # https://github.com/teamookla/ookla-open-data#available-data
    url = (
        f"https://ookla-open-data.s3.amazonaws.com/parquet/performance/"
        f"type=fixed/year={year}/quarter={quarter}/"
        f"{year}-{quarter:02d}-01_performance_fixed_tiles.parquet"
    )
    print(f"[fetch_ookla] Downloading: {url}")

    resp = requests.get(url, stream=True, timeout=120)
    if resp.status_code == 404:
        print(
            f"[fetch_ookla] 404: File not found at {url}\n"
            "  Check https://github.com/teamookla/ookla-open-data for correct URLs."
        )
        sys.exit(1)
    resp.raise_for_status()

    tmp_file = RAW_DIR / f"_ookla_world_{year}_q{quarter}.parquet"
    total = int(resp.headers.get("content-length", 0))
    with open(tmp_file, "wb") as f, tqdm(total=total, unit="B", unit_scale=True) as bar:
        for chunk in resp.iter_content(chunk_size=65536):
            f.write(chunk)
            bar.update(len(chunk))

    print("[fetch_ookla] Clipping to Texas bbox …")
    df = pd.read_parquet(tmp_file)

    # Derive lon/lat from quadkey for spatial filtering
    coords = df["quadkey"].apply(quadkey_to_lon_lat)
    df["_lon"] = coords.str[0]
    df["_lat"] = coords.str[1]

    mask = (
        (df["_lon"] >= TX_BBOX["lon_min"]) & (df["_lon"] <= TX_BBOX["lon_max"]) &
        (df["_lat"] >= TX_BBOX["lat_min"]) & (df["_lat"] <= TX_BBOX["lat_max"])
    )
    df_tx = df[mask].drop(columns=["_lon", "_lat"])

    df_tx.to_parquet(out_file, index=False)
    tmp_file.unlink()
    print(f"[fetch_ookla] Saved {len(df_tx):,} Texas tiles → {out_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download Ookla Open Data for Texas.")
    parser.add_argument("--year",    type=int, default=2023)
    parser.add_argument("--quarter", type=int, default=4, choices=[1, 2, 3, 4])
    args = parser.parse_args()
    fetch_ookla_data(args.year, args.quarter)
