# Texas Broadband Truth Map

Visualizes the disparity between FCC-reported broadband speeds and real-world
Ookla speed-test measurements for Texas, county by county, with a focus on
fixed wireless providers (technology code 70) that overstate their capabilities.

```
Overstatement ratio = FCC max advertised download ÷ Ookla avg actual download
```

A ratio of **1.0** means the provider delivers what it claims. A ratio of **3.0**
means users get roughly a third of the advertised speed.

---

## Project Structure

```
tx-broadband-truth/
├── frontend/                 # Static Leaflet site
│   ├── index.html
│   ├── style.css
│   └── map.js
├── pipeline/                 # Python data-processing scripts
│   ├── requirements.txt
│   ├── generate_mock_data.py # Generates realistic mock data for dev/staging
│   ├── fetch_fcc.py          # Download & filter FCC BDC data
│   ├── fetch_ookla.py        # Download & clip Ookla open data
│   └── build_geojson.py      # Spatial join → counties.geojson + providers.json
└── data/
    ├── raw/                  # Source data (gitignored — large files)
    └── processed/
        ├── counties.geojson  # County choropleth data consumed by the frontend
        └── providers.json    # Per-county provider detail
```

---

## Data Sources

### FCC Broadband Data Collection (BDC)

- **URL:** <https://broadbandmap.fcc.gov/data-download>
- **What it contains:** Location-level availability filings from ISPs. Each row
  represents a location where a provider claims to offer service at a given speed.
- **Filters applied:**
  - State: Texas (`state_abbr == 'TX'`)
  - Technology: Fixed Wireless Access (`technology == 70`)
- **Key fields used:**
  - `brand_name` — provider name
  - `max_advertised_download_speed` — claimed download speed (Mbps)
  - `max_advertised_upload_speed` — claimed upload speed (Mbps)
  - `latitude`, `longitude` — service location coordinates
  - `county_geoid` — FIPS county code

### Ookla Open Data

- **URL:** <https://github.com/teamookla/ookla-open-data>
- **License:** Ookla Open Data License
- **What it contains:** Quarterly aggregated speed-test results at zoom-16
  quadkey tiles (~600 m × 600 m cells).
- **Key fields used:**
  - `quadkey` — Bing Maps tile identifier
  - `avg_d_mbps` — average download speed across all tests in tile
  - `avg_u_mbps` — average upload speed
  - `tests` — test count (used as weight in county-level aggregation)

---

## Pipeline Steps

### 0. Install dependencies

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r pipeline/requirements.txt
```

### 1. Fetch FCC data

```bash
# Download Texas FCC BDC availability CSV from broadbandmap.fcc.gov manually
# and place it at:  data/raw/fcc_tx_availability.csv
# Then run:
python pipeline/fetch_fcc.py --period 2023-06
# Output: data/raw/fcc_tx_fwa.parquet
```

### 2. Fetch Ookla data

```bash
python pipeline/fetch_ookla.py --year 2023 --quarter 4
# Output: data/raw/ookla_tx_2023_q4.parquet
# (~500 MB download from S3; clipped to Texas bbox automatically)
```

### 3. Build GeoJSON

```bash
python pipeline/build_geojson.py --year 2023 --quarter 4
# Output:
#   data/processed/counties.geojson
#   data/processed/providers.json
```

Internally this script:
1. Loads FCC point locations → spatial join to Texas county polygons
   (TIGER/Line boundaries, auto-downloaded on first run)
2. Converts Ookla quadkey tiles to polygons → spatial join to counties
3. Aggregates per county: max FCC claimed speed, weighted-average Ookla speed
4. Computes overstatement ratio
5. Writes GeoJSON and provider index

### 4. (Dev) Generate mock data

To develop or demo the frontend without running the full pipeline:

```bash
python pipeline/generate_mock_data.py
# Writes realistic mock data for all 254 Texas counties to data/processed/
```

---

## Running Locally

The frontend is plain HTML — no build step needed.

```bash
# From the repo root:
python3 -m http.server 8080
# Then open http://localhost:8080/frontend/
```

Or with any static file server (caddy, nginx, live-server, etc.).

---

## Deployment (Nginx on VPS)

### One-time server setup

```nginx
# /etc/nginx/sites-available/broadband-truth
server {
    listen 80;
    server_name broadband-truth.example.com;

    root /var/www/broadband-truth;
    index index.html;

    location / {
        try_files $uri $uri/ =404;
    }

    # GeoJSON files — allow cross-origin for local dev
    location /data/ {
        add_header Access-Control-Allow-Origin "*";
    }

    gzip on;
    gzip_types text/plain application/json application/geo+json;
}
```

```bash
sudo ln -s /etc/nginx/sites-available/broadband-truth \
           /etc/nginx/sites-enabled/
sudo nginx -t && sudo systemctl reload nginx
```

### Deploy frontend

```bash
rsync -avz --delete frontend/ user@your-vps:/var/www/broadband-truth/
```

### Deploy processed data

```bash
rsync -avz --delete data/processed/ user@your-vps:/var/www/broadband-truth/data/processed/
```

### Full deploy script

```bash
#!/usr/bin/env bash
set -euo pipefail

REMOTE="user@your-vps"
DEST="/var/www/broadband-truth"

rsync -avz --delete frontend/         "$REMOTE:$DEST/"
rsync -avz --delete data/processed/   "$REMOTE:$DEST/data/processed/"
echo "Deployed."
```

---

## Frontend Architecture

| File        | Role |
|-------------|------|
| `index.html` | Shell: header, layer-toggle buttons, map container, stats bar |
| `style.css`  | Dark-theme styles; Leaflet popup overrides; mobile breakpoints |
| `map.js`     | Leaflet init, GeoJSON fetch, color scales, popup builder, legend |

**Layer modes:**

| Button | Data shown |
|--------|-----------|
| Overstatement | `overstatement_ratio` choropleth — the primary view |
| FCC Claimed | `fcc_claimed_down_mbps` choropleth |
| Ookla Actual | `ookla_actual_down_mbps` choropleth |

---

## License

Source code: MIT.
FCC BDC data: public domain (US government work).
Ookla data: [Ookla Open Data License](https://github.com/teamookla/ookla-open-data#license).
