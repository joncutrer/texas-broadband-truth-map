"""
fetch_fcc.py — Download and filter FCC Broadband Data Collection (BDC) data.

The FCC BDC is a location-level dataset published as large CSV/Parquet files.
This script:
  1. Reads one or more manually-downloaded FCC BDC availability CSVs.
  2. Filters to Texas (state_abbr == 'TX') and fixed wireless technology codes
     70 (Licensed Fixed Wireless), 71 (Unlicensed Fixed Wireless), and
     72 (LBR Fixed Wireless).
  3. Selects relevant columns and writes data/raw/fcc_tx_fwa.parquet.

Usage:
    python pipeline/fetch_fcc.py [--period 2023-06]

Input files (place any/all in data/raw/ before running):
    fcc_tx_availability.csv         — legacy single-file download
    fcc_tx_licensed_fwa.csv         — tech 70 (Licensed Fixed Wireless)
    fcc_tx_unlicensed_fwa.csv       — tech 71 (Unlicensed Fixed Wireless)
    fcc_tx_lbr_fwa.csv              — tech 72 (LBR Fixed Wireless)

Data source:
    https://broadbandmap.fcc.gov/data-download
    Availability Data > Fixed Broadband > By State > Texas
    Download separately for: Licensed Fixed Wireless, Unlicensed Fixed Wireless,
    and LBR Fixed Wireless.

Notes:
  - Files can be 1–5 GB each. Run once and cache in data/raw/ (gitignored).
  - The BDC data requires a free account / API token from the FCC.
  - See: https://broadbandmap.fcc.gov/home  (click "Download Data")
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

RAW_DIR = Path(__file__).parent.parent / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

# All three fixed wireless technology codes
FWA_TECH_CODES = {"70", "71", "72"}

# Named input files to look for (all are optional; at least one must exist)
CANDIDATE_CSVS = [
    "fcc_tx_availability.csv",       # legacy single-file download
    "fcc_tx_licensed_fwa.csv",       # tech 70
    "fcc_tx_unlicensed_fwa.csv",     # tech 71
    "fcc_tx_lbr_fwa.csv",            # tech 72
]

COLUMNS_TO_KEEP = [
    "frn",                     # FCC Registration Number (provider ID)
    "provider_id",
    "brand_name",
    "location_id",
    "technology",              # 70/71/72 = fixed wireless variants
    "max_advertised_download_speed",
    "max_advertised_upload_speed",
    "latitude",
    "longitude",
    "state_abbr",
    "county_geoid",
]


def _read_csv_filtered(path: Path) -> pd.DataFrame:
    """Read a single FCC BDC CSV, keeping only TX fixed wireless rows."""
    print(f"[fetch_fcc] Reading {path.name} …")
    chunks = []
    for chunk in pd.read_csv(
        path,
        chunksize=500_000,
        dtype=str,
        usecols=lambda c: c in COLUMNS_TO_KEEP,
    ):
        # Filter: Texas + fixed wireless tech codes 70, 71, 72.
        # state_abbr may be absent if the file is already TX-only.
        if "state_abbr" in chunk.columns:
            chunk = chunk[chunk["state_abbr"] == "TX"]
        if "technology" in chunk.columns:
            chunk = chunk[chunk["technology"].isin(FWA_TECH_CODES)]
        if not chunk.empty:
            chunks.append(chunk.copy())

    if not chunks:
        return pd.DataFrame()
    return pd.concat(chunks, ignore_index=True)


def fetch_fcc_data(period: str) -> None:
    out_file = RAW_DIR / "fcc_tx_fwa.parquet"
    if out_file.exists():
        print(f"[fetch_fcc] Output already exists: {out_file}. Delete to re-run.")
        return

    source_files = [RAW_DIR / name for name in CANDIDATE_CSVS if (RAW_DIR / name).exists()]

    if not source_files:
        print(
            "[fetch_fcc] ERROR: No FCC BDC CSV files found in data/raw/.\n"
            "  1. Visit https://broadbandmap.fcc.gov/data-download\n"
            "  2. Download Availability Data for Texas:\n"
            "       - Licensed Fixed Wireless  → data/raw/fcc_tx_licensed_fwa.csv\n"
            "       - Unlicensed Fixed Wireless → data/raw/fcc_tx_unlicensed_fwa.csv\n"
            "       - LBR Fixed Wireless        → data/raw/fcc_tx_lbr_fwa.csv\n"
            "  3. Re-run this script."
        )
        sys.exit(1)

    frames = []
    for path in source_files:
        df = _read_csv_filtered(path)
        if not df.empty:
            frames.append(df)
            print(f"[fetch_fcc]   {len(df):,} rows from {path.name}")

    if not frames:
        print("[fetch_fcc] No matching rows found. Check column names / filters.")
        sys.exit(1)

    df = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["location_id", "technology"])

    # Coerce numeric columns
    for col in ["max_advertised_download_speed", "max_advertised_upload_speed",
                "latitude", "longitude"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    tech_counts = df["technology"].value_counts().to_dict() if "technology" in df.columns else {}
    print(f"[fetch_fcc] Tech breakdown: {tech_counts}")
    df.to_parquet(out_file, index=False)
    print(f"[fetch_fcc] Saved {len(df):,} rows → {out_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Filter FCC BDC data for TX fixed wireless.")
    parser.add_argument("--period", default="2023-06", help="Filing period (YYYY-MM)")
    args = parser.parse_args()
    fetch_fcc_data(args.period)
