# CLAUDE.md — Texas Broadband Truth Map

## Project overview

A static map that visualizes the gap between FCC-reported broadband speeds and
real-world Ookla speed-test measurements for every Texas county, with a focus on
fixed wireless providers that overstate their capabilities.

```
overstatement_ratio = FCC max advertised download ÷ Ookla avg actual download
```

A ratio of 1.0 means the provider delivers what it claims; 3.0 means users get
roughly a third of the advertised speed.

---

## Repository layout

```
texas-broadband-truth-map/
├── frontend/                    # Static Leaflet site (no build step)
│   ├── index.html
│   ├── style.css
│   └── map.js
├── pipeline/                    # Python data-processing scripts
│   ├── requirements.txt
│   ├── fetch_fcc.py             # Filter FCC BDC CSVs → fcc_tx_fwa.parquet
│   ├── fetch_ookla.py           # Download/clip Ookla open data → parquet
│   ├── build_geojson.py         # Spatial join → counties.geojson + providers.json
│   └── generate_mock_data.py    # Realistic mock data for dev/staging
├── data/
│   ├── raw/                     # Source data — gitignored, large files
│   └── processed/
│       ├── counties.geojson     # County choropleth consumed by frontend
│       └── providers.json       # Per-county provider detail
└── .github/workflows/
    ├── deploy.yml               # Deploys frontend to GitHub Pages / VPS
    └── pipeline.yml             # Quarterly automated data refresh
```

---

## Development environment

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r pipeline/requirements.txt
```

Python 3.11+ is required. Key dependencies: `pandas`, `geopandas`, `shapely`,
`pyarrow`, `requests`, `tqdm`.

---

## Running the frontend locally

```bash
python3 -m http.server 8080
# Open http://localhost:8080/frontend/
```

No build step. The frontend reads `data/processed/counties.geojson` and
`data/processed/providers.json` via relative fetch calls.

To generate realistic mock data without running the full pipeline:

```bash
python pipeline/generate_mock_data.py
# Writes mock data for all 254 Texas counties to data/processed/
```

---

## Data pipeline

### FCC data

FCC BDC CSVs must be downloaded manually from
<https://broadbandmap.fcc.gov/data-download>.

Download **Availability Data > Fixed Broadband > Texas** for each of these
technology types and place the files in `data/raw/`:

| File name                        | Technology code | Description               |
|----------------------------------|-----------------|---------------------------|
| `fcc_tx_licensed_fwa.csv`        | 70              | Licensed Fixed Wireless   |
| `fcc_tx_unlicensed_fwa.csv`      | 71              | Unlicensed Fixed Wireless |
| `fcc_tx_lbr_fwa.csv`             | 72              | LBR Fixed Wireless        |

A legacy single-file download (`fcc_tx_availability.csv`) is also accepted.
At least one file must be present. Then run:

```bash
python pipeline/fetch_fcc.py --period 2023-06
# Output: data/raw/fcc_tx_fwa.parquet
```

The script deduplicates on `(location_id, technology)` and reports a tech-code
breakdown at the end.

### Ookla data

```bash
python pipeline/fetch_ookla.py --year 2023 --quarter 4
# Output: data/raw/ookla_tx_2023_q4.parquet  (~500 MB download, clipped to TX)
```

### Build GeoJSON

```bash
python pipeline/build_geojson.py --year 2023 --quarter 4
# Output: data/processed/counties.geojson
#         data/processed/providers.json
```

County boundaries (TIGER/Line 2023) are auto-downloaded on first run and cached
at `data/raw/tl_2023_us_county/`.

---

## Key data fields

### `counties.geojson` properties

| Field                    | Description                                            |
|--------------------------|--------------------------------------------------------|
| `GEOID`                  | 5-digit FIPS county code                               |
| `NAME`                   | County name                                            |
| `fcc_claimed_down_mbps`  | Max FCC advertised download speed in county (Mbps)     |
| `fcc_claimed_up_mbps`    | Max FCC advertised upload speed in county (Mbps)       |
| `ookla_actual_down_mbps` | Weighted-average Ookla actual download speed (Mbps)    |
| `ookla_actual_up_mbps`   | Weighted-average Ookla actual upload speed (Mbps)      |
| `overstatement_ratio`    | `fcc_claimed_down_mbps / ookla_actual_down_mbps`       |
| `top_providers`          | List of up to 3 top FWA providers by location count    |

### FCC technology codes tracked

| Code | Type                      |
|------|---------------------------|
| 70   | Licensed Fixed Wireless   |
| 71   | Unlicensed Fixed Wireless |
| 72   | LBR Fixed Wireless        |

---

## Frontend layer modes

| Button        | Choropleth field          |
|---------------|---------------------------|
| Overstatement | `overstatement_ratio`     |
| FCC Claimed   | `fcc_claimed_down_mbps`   |
| Ookla Actual  | `ookla_actual_down_mbps`  |

---

## Automated pipeline (GitHub Actions)

`pipeline.yml` runs on a quarterly schedule (`cron: "0 4 2 1,4,7,10 *"`).

- If `FCC_API_KEY` secret is set → runs the full pipeline scripts.
- Otherwise → runs `generate_mock_data.py` (safe for CI/staging).
- Commits any changes to `data/processed/` with `[skip ci]`.

---

## Gitignore notes

`data/raw/` is gitignored (large source files). `data/processed/` is committed
(small output files consumed by the frontend). The `venv/` directory is also
gitignored.
