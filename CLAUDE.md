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

---

## Workflow orchestration

### 1. Plan first
- Enter plan mode for any non-trivial task (3+ steps or architectural decisions)
- If something goes sideways, stop and re-plan immediately — don't keep pushing
- Use plan mode for verification steps, not just building
- Write detailed specs upfront to reduce ambiguity

### 2. Subagent strategy
- Use subagents liberally to keep the main context window clean
- Offload research, exploration, and parallel analysis to subagents
- For complex problems, throw more compute at it via subagents
- One tack per subagent for focused execution

### 3. Self-improvement loop
- After any correction from the user: update `tasks/lessons.md` with the pattern
- Write rules that prevent the same mistake from recurring
- Ruthlessly iterate on these lessons until mistake rate drops
- Review `tasks/lessons.md` at session start for relevant context

### 4. Verification before done
- Never mark a task complete without proving it works
- Diff behavior between main and your changes when relevant
- Ask: "Would a staff engineer approve this?"
- Run tests, check logs, demonstrate correctness

### 5. Demand elegance (balanced)
- For non-trivial changes: pause and ask "is there a more elegant way?"
- If a fix feels hacky: "Knowing everything I know now, implement the elegant solution"
- Skip this for simple, obvious fixes — don't over-engineer
- Challenge your own work before presenting it

### 6. Autonomous bug fixing
- When given a bug report: just fix it — don't ask for hand-holding
- Point at logs, errors, failing tests — then resolve them
- Zero context switching required from the user
- Go fix failing CI tests without being told how

---

## Task management

1. **Plan first**: Write plan to `tasks/todo.md` with checkable items
2. **Verify plan**: Check in before starting implementation
3. **Track progress**: Mark items complete as you go
4. **Explain changes**: High-level summary at each step
5. **Document results**: Add a review section to `tasks/todo.md`
6. **Capture lessons**: Update `tasks/lessons.md` after any correction

---

## Core principles

- **Simplicity first**: Make every change as simple as possible. Impact minimal code.
- **No laziness**: Find root causes. No temporary fixes. Senior developer standards.
- **Minimal impact**: Changes should only touch what is necessary. Avoid introducing bugs.
