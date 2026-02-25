"""
fetch_fcc.py — Process FCC Broadband Data Collection (BDC) zip files.

Reads three local BDC zip files from data/raw/, filters for fixed wireless
technology codes (70, 71, 72), derives county_geoid from block_geoid, and
writes data/raw/fcc_tx_fwa.parquet.

Expected input files in data/raw/:
  bdc_48_UnlicensedFixedWireless_fixed_broadband_J25_17feb2026.zip
  bdc_48_LBRFixedWireless_fixed_broadband_J25_17feb2026.zip
  bdc_48_LicensedFixedWireless_fixed_broadband_J25_17feb2026.zip

Usage:
    uv run python pipeline/fetch_fcc.py
"""

import sys
import zipfile
from pathlib import Path

import pandas as pd

RAW_DIR = Path(__file__).parent.parent / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

# All three BDC zip files to process (fixed wireless variants)
BDC_ZIPS = [
    "bdc_48_UnlicensedFixedWireless_fixed_broadband_J25_17feb2026.zip",
    "bdc_48_LBRFixedWireless_fixed_broadband_J25_17feb2026.zip",
    "bdc_48_LicensedFixedWireless_fixed_broadband_J25_17feb2026.zip",
]

# Technology codes to keep: 70=Fixed Wireless, 71=Licensed FW, 72=Unlicensed FW
FWA_TECH_CODES = {"70", "71", "72"}

# Columns to keep from each CSV
COLUMNS_TO_KEEP = [
    "frn",
    "provider_id",
    "brand_name",
    "location_id",
    "technology",
    "max_advertised_download_speed",
    "max_advertised_upload_speed",
    "state_usps",
    "block_geoid",
]


def process_bdc_zip(zip_path: Path) -> pd.DataFrame:
    """Read one BDC zip, filter for FWA tech codes, return a DataFrame."""
    print(f"[fetch_fcc] Reading {zip_path.name} …")
    with zipfile.ZipFile(zip_path) as zf:
        csv_name = zf.namelist()[0]
        with zf.open(csv_name) as raw_bytes:
            chunks = []
            for chunk in pd.read_csv(
                raw_bytes,
                chunksize=500_000,
                dtype=str,
                usecols=lambda c: c in COLUMNS_TO_KEEP,
            ):
                chunk = chunk[chunk["technology"].isin(FWA_TECH_CODES)].copy()
                if not chunk.empty:
                    chunks.append(chunk)

    if not chunks:
        print(f"[fetch_fcc]   WARNING: no matching rows in {zip_path.name}")
        return pd.DataFrame(columns=COLUMNS_TO_KEEP)

    df = pd.concat(chunks, ignore_index=True)
    print(f"[fetch_fcc]   {len(df):,} rows (tech {sorted(df['technology'].unique())})")
    return df


def fetch_fcc_data() -> None:
    out_file = RAW_DIR / "fcc_tx_fwa.parquet"
    if out_file.exists():
        print(f"[fetch_fcc] Output already exists: {out_file}. Delete to re-run.")
        return

    # Verify all input zips are present before starting
    missing = [z for z in BDC_ZIPS if not (RAW_DIR / z).exists()]
    if missing:
        print("[fetch_fcc] ERROR: Missing BDC zip files in data/raw/:")
        for m in missing:
            print(f"  {m}")
        sys.exit(1)

    all_frames = []
    for zip_name in BDC_ZIPS:
        df = process_bdc_zip(RAW_DIR / zip_name)
        if not df.empty:
            all_frames.append(df)

    if not all_frames:
        print("[fetch_fcc] ERROR: No data found in any BDC zip file.")
        sys.exit(1)

    combined = pd.concat(all_frames, ignore_index=True)

    # Derive county_geoid from the first 5 chars of block_geoid
    # block_geoid is a 15-digit census block FIPS: SS+CCC+TTTTTT+BBB
    # where SS=state(48), CCC=county, TTTTTT=tract, BBB=block
    combined["county_geoid"] = combined["block_geoid"].str[:5]

    # Drop rows with malformed block_geoid
    valid_mask = combined["county_geoid"].str.len() == 5
    n_dropped = (~valid_mask).sum()
    if n_dropped:
        print(f"[fetch_fcc] Dropping {n_dropped:,} rows with malformed block_geoid")
    combined = combined[valid_mask].copy()

    # Coerce numeric speed columns
    for col in ["max_advertised_download_speed", "max_advertised_upload_speed"]:
        combined[col] = pd.to_numeric(combined[col], errors="coerce")

    combined.to_parquet(out_file, index=False)
    print(f"[fetch_fcc] Saved {len(combined):,} rows -> {out_file}")
    print(f"[fetch_fcc] Unique counties: {combined['county_geoid'].nunique()}")


if __name__ == "__main__":
    fetch_fcc_data()
