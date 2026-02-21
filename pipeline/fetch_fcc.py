"""
fetch_fcc.py — Download and filter FCC Broadband Data Collection (BDC) data.

The FCC BDC is a location-level dataset published as large CSV/Parquet files.
This script:
  1. Downloads the availability data for the most recent filing period.
  2. Filters to Texas (state_abbr == 'TX') and technology code 70 (fixed wireless).
  3. Selects relevant columns and writes data/raw/fcc_tx_fwa.parquet.

Usage:
    python pipeline/fetch_fcc.py [--period 2023-06]

Data source:
    https://broadbandmap.fcc.gov/data-download
    Availability Data > Fixed Broadband > By State > Texas

Notes:
  - Files can be 1–5 GB. Run once and cache in data/raw/ (gitignored).
  - The BDC data requires a free account / API token from the FCC.
  - See: https://broadbandmap.fcc.gov/home  (click "Download Data")
"""

import argparse
import sys
from pathlib import Path

import pandas as pd
import requests
from tqdm import tqdm

RAW_DIR = Path(__file__).parent.parent / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

# FCC BDC Fabric availability download endpoint (example URL shape).
# Replace with the actual URL from the FCC data-download page for your period.
FCC_DOWNLOAD_URL_TEMPLATE = (
    "https://broadbandmap.fcc.gov/api/public/map/downloads/listAvailabilityData"
    "?category=State&state_code=TX&filing_type=availability&period_date={period}"
)

COLUMNS_TO_KEEP = [
    "frn",                     # FCC Registration Number (provider ID)
    "provider_id",
    "brand_name",
    "location_id",
    "technology",              # 70 = fixed wireless
    "max_advertised_download_speed",
    "max_advertised_upload_speed",
    "latitude",
    "longitude",
    "state_abbr",
    "county_geoid",
]


def fetch_fcc_data(period: str) -> None:
    out_file = RAW_DIR / "fcc_tx_fwa.parquet"
    if out_file.exists():
        print(f"[fetch_fcc] Output already exists: {out_file}. Delete to re-download.")
        return

    # In practice: manually download the state CSV from broadbandmap.fcc.gov
    # and place it at data/raw/fcc_tx_availability.csv, then run this script.
    source_csv = RAW_DIR / "fcc_tx_availability.csv"
    if not source_csv.exists():
        print(
            "[fetch_fcc] ERROR: data/raw/fcc_tx_availability.csv not found.\n"
            "  1. Visit https://broadbandmap.fcc.gov/data-download\n"
            "  2. Download 'Availability Data > Fixed Broadband > Texas'\n"
            "  3. Place the CSV at data/raw/fcc_tx_availability.csv\n"
            "  4. Re-run this script."
        )
        sys.exit(1)

    print(f"[fetch_fcc] Reading {source_csv} …")
    chunks = []
    for chunk in pd.read_csv(
        source_csv,
        chunksize=500_000,
        dtype=str,
        usecols=lambda c: c in COLUMNS_TO_KEEP,
    ):
        # Filter: Texas fixed wireless (tech code 70)
        chunk = chunk[
            (chunk["state_abbr"] == "TX") &
            (chunk["technology"] == "70")
        ].copy()
        if not chunk.empty:
            chunks.append(chunk)

    if not chunks:
        print("[fetch_fcc] No matching rows found. Check column names / filters.")
        sys.exit(1)

    df = pd.concat(chunks, ignore_index=True)

    # Coerce numeric columns
    for col in ["max_advertised_download_speed", "max_advertised_upload_speed",
                "latitude", "longitude"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df.to_parquet(out_file, index=False)
    print(f"[fetch_fcc] Saved {len(df):,} rows → {out_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Filter FCC BDC data for TX fixed wireless.")
    parser.add_argument("--period", default="2023-06", help="Filing period (YYYY-MM)")
    args = parser.parse_args()
    fetch_fcc_data(args.period)
