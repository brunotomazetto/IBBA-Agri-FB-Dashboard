# U.S. Beef Packer Margin Tracker — Setup Guide

## File structure in the GitHub repo

```
/ (repo root)
├── index.html                        ← existing portal shell (1 line to change)
└── U.S. Beef Tracker/
    ├── beef_margin_dashboard.html    ← new dashboard (module 5)
    ├── schema.sql                    ← run once in Supabase SQL editor
    ├── load_history.py               ← run once to upload V4 data
    └── update_weekly.py              ← run weekly (or via GitHub Actions)
```

---

## Step 1 — Supabase schema (run once)

1. Open your Supabase project → **SQL Editor**
2. Paste the contents of `schema.sql` and click **Run**
3. This creates `beef_weekly`, `beef_quarterly`, sets RLS policies,
   and inserts the module-5 row into your `dashboards` table

---

## Step 2 — index.html patch (1 line)

Find this block in `index.html` (~line 514):

```js
const DASH_MODULES = {
  1: 'Agri Monitor/conab/producao_dashboard.html',
  2: 'Agri Monitor/imea/imea_margin_dashboard.html',
  3: 'Agri Monitor/secex/dashboard_secex.html',
};
```

Add module 5:

```js
const DASH_MODULES = {
  1: 'Agri Monitor/conab/producao_dashboard.html',
  2: 'Agri Monitor/imea/imea_margin_dashboard.html',
  3: 'Agri Monitor/secex/dashboard_secex.html',
  5: 'U.S. Beef Tracker/beef_margin_dashboard.html',   // ← add this line
};
```

---

## Step 3 — Historical load (run once)

```bash
pip install supabase openpyxl
```

Edit `load_history.py`:
- Set `SUPABASE_SERVICE` to your **service_role** key
  (Supabase → Settings → API → service_role — keep this secret!)
- Set `TRACKER_PATH` to the path of `US_Beef_Packer_Margin_Tracker_v4.xlsx`

Then run:
```bash
python load_history.py
```

---

## Step 4 — Weekly updater

### Dependencies

```bash
pip install supabase requests pandas pdfplumber
```

### Configuration

Edit `update_weekly.py` (or pass as env vars — see GitHub Actions below):

| Variable | Where to get it |
|---|---|
| `SUPABASE_SERVICE` | Supabase → Settings → API → **service_role** key |
| `EIA_API_KEY` | Free at https://www.eia.gov/opendata/register/ |

### Data sources (all automated, no manual steps)

| # | Data | Source |
|---|---|---|
| ① | CT150 (5-area) | USDA AMS Datamart JSON — report 2461 |
| ② | Choice / Select cutout | USDA AMS Datamart JSON — report 2527 |
| ③ | Weekly cattle slaughter | USDA NASS Quick Stats API |
| ④ | **KS prices** | USDA AMS PDF — report 2484 (`mymarketnews.ams.usda.gov/viewReport/2484`) |
|   | **NE prices** | USDA AMS PDF — report 2667 (`mymarketnews.ams.usda.gov/viewReport/2667`) |
|   | | Parses "WEEKLY ACCUMULATED" → Live Steer / Heifer Avg Price |
| ⑤ | **Drop Credit** | USDA AMS PDF — report 2829 (`ams.usda.gov/mnreports/ams_2829.pdf`) |
|   | | Parses "Dressed Equivalent Basis" from By-Product Values table ($/cwt dressed) |
| ⑥ | Henry Hub (nat. gas) | EIA Open Data API — series NG.RNGWHHD.D |

### Manual run

```bash
python update_weekly.py           # updates last 6 weeks
python update_weekly.py --full    # rebuilds ALL quarterly rows from beef_weekly
```

### Automate with GitHub Actions

Create `.github/workflows/weekly_update.yml`:

```yaml
name: Weekly Beef Data Update
on:
  schedule:
    - cron: '0 12 * * 1'    # Every Monday at 12:00 UTC
  workflow_dispatch:          # Allow manual trigger from GitHub UI

jobs:
  update:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v4
        with: { python-version: '3.11' }
      - run: pip install supabase requests pandas pdfplumber
      - run: python "U.S. Beef Tracker/update_weekly.py"
        env:
          SUPABASE_SERVICE: ${{ secrets.SUPABASE_SERVICE }}
          EIA_API_KEY:      ${{ secrets.EIA_API_KEY }}
```

Add secrets: GitHub repo → **Settings → Secrets and variables → Actions**.

Then use env vars in `update_weekly.py` instead of hard-coded values:

```python
import os
SUPABASE_SERVICE = os.environ.get('SUPABASE_SERVICE', 'YOUR_SERVICE_ROLE_KEY')
EIA_API_KEY      = os.environ.get('EIA_API_KEY',      'YOUR_EIA_API_KEY')
```

---

## Dashboard features

The `beef_margin_dashboard.html` shows:
- **KPI cards** — latest Choice cutout, CT150, KS avg, spread proxy %, reported margin % for all 3 companies
- **Spread % trend** — quarterly line chart (MBRF, JBS, Tyson)
- **Reported margin trend** — GM% / Op% historical series
- **ΔSpread vs ΔMargin scatter** — the correlation chart per company with Pearson r
- **Quarterly data table** — most recent 16 quarters, all columns

The spread formula is computed client-side in JavaScript using the V4 optimal weights. No server-side computation needed.
