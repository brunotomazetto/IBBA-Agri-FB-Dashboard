#!/usr/bin/env python3
"""
extractor_chicken.py — U.S. Chicken Industry Spread Tracker
============================================================
Builds / refreshes  chicken.db  with quarterly data for the dashboard.

DATA SOURCES
  • Broiler composite wholesale price  → USDA ERS API  (monthly)
  • Chicken parts weekly prices         → USDA AMS API  (weekly, AMS-3646)
  • SBM + Corn prices                   → USDA AMS API  (weekly cost inputs)
  • PPC U.S. Gross Margin               → baked-in from current Excel history;
                                          update manually in the HARD_PPC dict
                                          after each quarterly earnings release.

USAGE
  pip install requests openpyxl
  python extractor_chicken.py

OUTPUT
  chicken.db  (SQLite, ~50-100 KB)

SCHEMA  — table: quarterly
  quarter    TEXT PRIMARY KEY   e.g. "1Q17"
  year_q     INTEGER            sortable: 20171, 20172 …
  bw         REAL               Broiler wholesale cts/lb   (quarterly avg)
  breast     REAL               Breast B/S cts/lb          (quarterly avg)
  leg_qtrs   REAL               Leg Quarters cts/lb        (quarterly avg)
  wings      REAL               Wings cts/lb               (quarterly avg)
  tenders    REAL               Tenderloins cts/lb         (quarterly avg)
  sbm        REAL               SBM Illinois FOB $/ton     (quarterly avg)
  corn       REAL               Corn Central IL $/bu       (quarterly avg)
  fc_spot    REAL               2.9802*corn + 0.03851*sbm  (current quarter)
  fc_0q5     REAL               0.5*fc_spot + 0.5*fc_prior (0.5Q lag)
  fc_1q5     REAL               0.5*fc_prior + 0.5*fc_2prior (1.5Q lag)
  ppc_us_gm  REAL               PPC U.S. Gross Margin %    (decimal, NULL if N/A)
  ppc_cnl_gm REAL               PPC Consolidated GM %      (decimal, NULL if N/A)
  updated_at TEXT               ISO timestamp of last update
"""

import sqlite3, calendar, math, os, sys, json, time
from datetime import datetime, date
from typing import Optional

try:
    import requests
except ImportError:
    sys.exit("Missing dependency: pip install requests")

# ─── Configuration ─────────────────────────────────────────────────────────────
DB_PATH = os.path.join(os.path.dirname(__file__), "chicken.db")
TIMEOUT  = 30   # HTTP timeout seconds
RETRY    = 3    # API retry attempts

# Quarter range to maintain in DB
FIRST_QUARTER = ("1Q17", 2017, 1)
# Last expected quarter:  build up through current + 1
_now = datetime.now()
LAST_YEAR = _now.year
LAST_Q    = (_now.month - 1) // 3 + 1   # current calendar quarter

# ─── Hard-coded PPC data ────────────────────────────────────────────────────────
# Update this dict after each quarterly earnings release.
# Keys: "1Q17" … format.  Values: (us_gm, cnl_gm) as decimals (e.g. 0.1085)
# us_gm = None means use cnl_gm as fallback.
HARD_PPC = {
    # ── 2017 ──
    "1Q17": (0.10845,  0.10845),
    "2Q17": (0.17793,  0.17793),
    "3Q17": (0.19458,  0.19458),
    "4Q17": (0.10315,  0.10315),
    # ── 2018 ──
    "1Q18": (0.09682,  None),
    "2Q18": (0.08104,  None),
    "3Q18": (0.07047,  None),
    "4Q18": (0.02648,  None),
    # ── 2019 ──
    "1Q19": (0.09032,  None),
    "2Q19": (0.12864,  None),
    "3Q19": (0.09950,  None),
    "4Q19": (0.06542,  None),
    # ── 2020 ──
    "1Q20": (0.07172,  None),
    "2Q20": (0.04893,  None),
    "3Q20": (0.09672,  None),
    "4Q20": (0.04856,  None),
    # ── 2021 ──
    "1Q21": (0.06640,  None),
    "2Q21": (0.10690,  None),
    "3Q21": (0.11270,  None),
    "4Q21": (0.11450,  None),
    # ── 2022 ──
    "1Q22": (0.16350,  None),
    "2Q22": (0.18780,  None),
    "3Q22": (0.15700,  None),
    "4Q22": (0.00990,  None),
    # ── 2023 ──
    "1Q23": (0.01580,  None),
    "2Q23": (0.04660,  None),
    "3Q23": (0.06860,  None),
    "4Q23": (0.07490,  None),
    # ── 2024 ──
    "1Q24": (0.09200,  None),
    "2Q24": (0.16980,  None),
    "3Q24": (0.17770,  None),
    "4Q24": (0.14600,  None),
    # ── 2025 ──
    "1Q25": (0.14130,  None),
    "2Q25": (0.17350,  None),
    "3Q25": (0.17040,  None),
    "4Q25": (0.10540,  None),
    # ── 2026 (update as released) ──
    # "1Q26": (None, None),
}

# ─── Quarter helpers ─────────────────────────────────────────────────────────
def quarter_label(yr: int, q: int) -> str:
    return f"{q}Q{str(yr)[2:]}"

def qstart(yr: int, q: int) -> datetime:
    return datetime(yr, (q-1)*3+1, 1)

def qend(yr: int, q: int) -> datetime:
    m = q*3
    return datetime(yr, m, calendar.monthrange(yr, m)[1])

def all_quarters():
    """Yield (year, q, label) from FIRST_QUARTER through current quarter."""
    fy, fq = FIRST_QUARTER[1], FIRST_QUARTER[2]
    for yr in range(fy, LAST_YEAR+1):
        for q in range(1, 5):
            if (yr == fy and q < fq): continue
            if (yr == LAST_YEAR and q > LAST_Q): continue
            yield yr, q, quarter_label(yr, q)

# ─── HTTP helper ─────────────────────────────────────────────────────────────
def get_json(url: str, params: dict = None) -> dict:
    for attempt in range(RETRY):
        try:
            r = requests.get(url, params=params, timeout=TIMEOUT)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt == RETRY - 1:
                print(f"  ✗ HTTP error ({url[:60]}…): {e}")
                return {}
            time.sleep(2 ** attempt)
    return {}

# ─── USDA AMS weekly commodity fetcher ────────────────────────────────────────
# AMS MARS API: https://marsapi.ams.usda.gov/services/v1.2/reports/
# We use the "Livestock, Poultry, and Grain" report endpoints.
#
# AMS-3646  = National Chicken Parts   (weekly)
# AMS-3192  = Central Illinois Corn    (weekly)
# AMS-3511  = Soybean Meal             (weekly)
# USDA ERS  = Broiler Composite        (monthly, CSV download)

AMS_BASE = "https://marsapi.ams.usda.gov/services/v1.2/reports"

def fetch_ams_weekly(report_id: str, slug_filter: str, value_col: str,
                     date_from: str = "2016-01-01") -> list[dict]:
    """
    Fetch weekly AMS report data.
    Returns list of {'date': datetime, 'value': float}.
    """
    url = f"{AMS_BASE}/{report_id}"
    params = {
        "q":         slug_filter,
        "startDate": date_from,
        "endDate":   datetime.now().strftime("%m/%d/%Y"),
        "allSections": "true",
        "allCommodities": "true",
    }
    data = get_json(url, params)
    rows = []
    for item in data.get("results", []):
        try:
            dt = datetime.strptime(item.get("report_date",""), "%m/%d/%Y")
            v  = float(item.get(value_col, "") or "nan")
            if not math.isnan(v):
                rows.append({"date": dt, "value": v})
        except (ValueError, TypeError):
            continue
    return rows

def quarterly_avg(rows: list[dict], yr: int, q: int) -> Optional[float]:
    s, e = qstart(yr, q), qend(yr, q)
    vals = [r["value"] for r in rows if s <= r["date"] <= e]
    return sum(vals)/len(vals) if vals else None

# ─── Broiler Composite from USDA ERS ─────────────────────────────────────────
ERS_BW_URL = ("https://apps.fas.usda.gov/psdonline/circulars/livestock.pdf")
# Fallback: use AMS-2920 (Broiler Composite) or the ERS download
# The ERS table is not easily API-accessible; we use the USDA AMS
# Broiler report AMS-2020 (National Composite).

AMS_BROILER_ID = "2020"   # AMS Weekly National Composite Broiler report

def fetch_bw_wholesale() -> list[dict]:
    """
    Fetch Broiler Composite Wholesale price from USDA AMS report 2020.
    Falls back to USDA ERS monthly data if AMS is unavailable.
    """
    url = f"{AMS_BASE}/{AMS_BROILER_ID}"
    params = {
        "q": "composite,wholesale",
        "startDate": "01/01/2016",
        "endDate": datetime.now().strftime("%m/%d/%Y"),
        "allSections": "true",
    }
    data = get_json(url, params)
    rows = []
    for item in data.get("results", []):
        try:
            dt  = datetime.strptime(item.get("report_date",""), "%m/%d/%Y")
            # Try several field names the API uses
            for field in ("weighted_avg", "price", "avg_price", "wtd_avg"):
                v = item.get(field)
                if v is not None:
                    rows.append({"date": dt, "value": float(v)})
                    break
        except (ValueError, TypeError):
            continue
    return rows

# ─── Chicken Parts from USDA AMS 3646 ────────────────────────────────────────
# AMS NW_LS644 = National Chicken Parts (weekly, cts/lb)
# Column mapping (may vary by API version):
#   Breast B/S   Leg Quarters   Wings   Tenderloins

def fetch_parts() -> dict[str, list[dict]]:
    """
    Returns dict with keys: 'breast', 'leg_qtrs', 'wings', 'tenders'
    Each: list of {'date': datetime, 'value': float}.
    """
    # Try USDA AMS MARS for the NW_LS644 report
    url = f"{AMS_BASE}/3646"
    params = {
        "startDate": "09/01/2016",
        "endDate":   datetime.now().strftime("%m/%d/%Y"),
        "allSections": "true",
    }
    data = get_json(url, params)
    results = {k: [] for k in ("breast", "leg_qtrs", "wings", "tenders")}
    for item in data.get("results", []):
        try:
            dt = datetime.strptime(item.get("report_date",""), "%m/%d/%Y")
            # Map AMS fields → our keys  (field names vary; try several)
            field_map = {
                "breast":    ["breast_boneless_skinless","b_s_breast","breast","brest"],
                "leg_qtrs":  ["leg_quarters_bulk","leg_quarters","leg_qtrs","legquarters"],
                "wings":     ["wings_whole","wings","wing"],
                "tenders":   ["tenderloins","tenders","tenderloin"],
            }
            for key, field_candidates in field_map.items():
                for fc in field_candidates:
                    v = item.get(fc)
                    if v is not None:
                        try:
                            results[key].append({"date": dt, "value": float(v)})
                        except ValueError:
                            pass
                        break
        except (ValueError, TypeError):
            continue
    return results

# ─── Feed costs from USDA AMS ────────────────────────────────────────────────
def fetch_sbm() -> list[dict]:
    """SBM Illinois FOB Truck $/ton from AMS 3511."""
    url = f"{AMS_BASE}/3511"
    params = {
        "startDate": "01/01/2017",
        "endDate":   datetime.now().strftime("%m/%d/%Y"),
        "allSections": "true",
    }
    data = get_json(url, params)
    rows = []
    for item in data.get("results", []):
        try:
            dt = datetime.strptime(item.get("report_date",""), "%m/%d/%Y")
            for field in ("illinois_fob_truck","il_fob_truck","price","avg_price"):
                v = item.get(field)
                if v is not None:
                    rows.append({"date": dt, "value": float(v)}); break
        except (ValueError, TypeError):
            continue
    return rows

def fetch_corn() -> list[dict]:
    """Corn Central Illinois $/bu from AMS 3192."""
    url = f"{AMS_BASE}/3192"
    params = {
        "startDate": "01/01/2017",
        "endDate":   datetime.now().strftime("%m/%d/%Y"),
        "allSections": "true",
    }
    data = get_json(url, params)
    rows = []
    for item in data.get("results", []):
        try:
            dt = datetime.strptime(item.get("report_date",""), "%m/%d/%Y")
            for field in ("central_illinois","central_il","price","avg_price"):
                v = item.get(field)
                if v is not None:
                    rows.append({"date": dt, "value": float(v)}); break
        except (ValueError, TypeError):
            continue
    return rows

# ─── Fallback: read from existing Excel files ─────────────────────────────────
def load_from_excel(base_dir: str) -> dict:
    """
    Fallback loader: reads from the IBBA Excel files if they exist.
    Returns dict of quarterly data keyed by label.
    """
    try:
        from openpyxl import load_workbook
    except ImportError:
        return {}

    data = {}  # keyed by (yr, q)

    # ─ Broiler Composite ─
    bw_path = os.path.join(base_dir, "Broiler Composite Price.xlsx")
    if os.path.exists(bw_path):
        wb = load_workbook(bw_path, data_only=True)
        ws = wb["broiler"]
        bw_rows = []
        for r in ws.iter_rows(min_row=5, values_only=True):
            dt = r[0]
            if not dt or not isinstance(dt, datetime): continue
            v = r[1] if isinstance(r[1], (int, float)) else None
            if v: bw_rows.append({"date": dt, "value": v})
        print(f"  Excel BW: {len(bw_rows)} monthly rows")
        data["bw_rows"] = bw_rows

    # ─ Parts + Costs ─
    parts_path = os.path.join(base_dir, "US_Chicken_Weekly_Prices_IBBA.xlsx")
    if os.path.exists(parts_path):
        wb2 = load_workbook(parts_path, data_only=True)
        # Parts Weekly
        ws_p = wb2["Weekly Prices"]
        breast_rows, leg_rows, wings_rows, tenders_rows = [], [], [], []
        for r in ws_p.iter_rows(min_row=5, values_only=True):
            dt = r[0]
            if not dt or not isinstance(dt, datetime): continue
            if isinstance(r[1], (int,float)): breast_rows.append({"date":dt,"value":r[1]})
            if isinstance(r[2], (int,float)): leg_rows.append({"date":dt,"value":r[2]})
            if isinstance(r[3], (int,float)): wings_rows.append({"date":dt,"value":r[3]})
            if isinstance(r[4], (int,float)): tenders_rows.append({"date":dt,"value":r[4]})
        print(f"  Excel parts: breast={len(breast_rows)}, leg={len(leg_rows)}, "
              f"wings={len(wings_rows)}, tenders={len(tenders_rows)}")
        data["breast_rows"]  = breast_rows
        data["leg_rows"]     = leg_rows
        data["wings_rows"]   = wings_rows
        data["tenders_rows"] = tenders_rows
        # Cost Inputs
        ws_c = wb2["Cost Inputs"]
        sbm_rows, corn_rows = [], []
        for r in ws_c.iter_rows(min_row=3, values_only=True):
            dt = r[0]
            if not dt or not isinstance(dt, datetime): continue
            def sf(v):
                if v is None or v == '': return None
                try: return float(str(v).replace(';','.').replace(',',''))
                except: return None
            sbm_v  = sf(r[1])
            corn_v = sf(r[2])
            if sbm_v:  sbm_rows.append({"date":dt,"value":sbm_v})
            if corn_v: corn_rows.append({"date":dt,"value":corn_v})
        print(f"  Excel costs: sbm={len(sbm_rows)}, corn={len(corn_rows)}")
        data["sbm_rows"]  = sbm_rows
        data["corn_rows"] = corn_rows

    return data

# ─── Build database ───────────────────────────────────────────────────────────
def build_db(data: dict):
    """Populate chicken.db from the data dictionary."""
    print(f"\nWriting to {DB_PATH} …")
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS quarterly (
            quarter    TEXT PRIMARY KEY,
            year_q     INTEGER,
            bw         REAL,
            breast     REAL,
            leg_qtrs   REAL,
            wings      REAL,
            tenders    REAL,
            sbm        REAL,
            corn       REAL,
            fc_spot    REAL,
            fc_0q5     REAL,
            fc_1q5     REAL,
            ppc_us_gm  REAL,
            ppc_cnl_gm REAL,
            updated_at TEXT
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_year_q ON quarterly(year_q)")

    rows_data = {}   # label → dict of raw values (pre-lag)
    quarters_list = list(all_quarters())

    for yr, q, label in quarters_list:
        bw_avg     = quarterly_avg(data.get("bw_rows", []),      yr, q)
        breast_avg = quarterly_avg(data.get("breast_rows", []),  yr, q)
        leg_avg    = quarterly_avg(data.get("leg_rows", []),      yr, q)
        wings_avg  = quarterly_avg(data.get("wings_rows", []),   yr, q)
        tenders_avg= quarterly_avg(data.get("tenders_rows", []), yr, q)
        sbm_avg    = quarterly_avg(data.get("sbm_rows", []),     yr, q)
        corn_avg   = quarterly_avg(data.get("corn_rows", []),    yr, q)

        fc = (2.9802*corn_avg + 0.03851*sbm_avg) if (corn_avg and sbm_avg) else None
        ppc_us, ppc_cnl = HARD_PPC.get(label, (None, None))

        rows_data[label] = {
            "yr": yr, "q": q,
            "bw": bw_avg, "breast": breast_avg, "leg_qtrs": leg_avg,
            "wings": wings_avg, "tenders": tenders_avg,
            "sbm": sbm_avg, "corn": corn_avg,
            "fc_spot": fc,
            "ppc_us_gm": ppc_us, "ppc_cnl_gm": ppc_cnl,
        }

    # Compute lag columns
    labels_ordered = [ql for _, _, ql in quarters_list]
    for i, label in enumerate(labels_ordered):
        rd = rows_data[label]
        fc_cur  = rd["fc_spot"]
        fc_prev = rows_data[labels_ordered[i-1]]["fc_spot"] if i >= 1 else None
        fc_p2   = rows_data[labels_ordered[i-2]]["fc_spot"] if i >= 2 else None

        fc_0q5 = 0.5*fc_cur + 0.5*fc_prev if (fc_cur and fc_prev) else None
        fc_1q5 = 0.5*fc_prev + 0.5*fc_p2  if (fc_prev and fc_p2)  else None

        yr, q = rd["yr"], rd["q"]
        year_q = yr*10 + q
        ts = datetime.now().isoformat(timespec="seconds")

        cur.execute("""
            INSERT OR REPLACE INTO quarterly
            (quarter, year_q, bw, breast, leg_qtrs, wings, tenders,
             sbm, corn, fc_spot, fc_0q5, fc_1q5,
             ppc_us_gm, ppc_cnl_gm, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            label, year_q,
            rd["bw"], rd["breast"], rd["leg_qtrs"], rd["wings"], rd["tenders"],
            rd["sbm"], rd["corn"], rd["fc_spot"], fc_0q5, fc_1q5,
            rd["ppc_us_gm"], rd["ppc_cnl_gm"], ts
        ))

    con.commit()
    n = cur.execute("SELECT COUNT(*) FROM quarterly").fetchone()[0]
    # Print summary
    sample = cur.execute("""
        SELECT quarter, bw, breast, leg_qtrs, sbm, corn, fc_spot, ppc_us_gm
        FROM quarterly
        ORDER BY year_q DESC LIMIT 6
    """).fetchall()
    print(f"  {n} rows written. Latest 6:")
    print(f"  {'Q':<7} {'BW':>7} {'Breast':>7} {'Leg':>7} {'SBM':>8} {'Corn':>7} {'FC':>7} {'PPC_GM':>9}")
    for row in sample:
        def f(v): return f"{v:7.2f}" if v is not None else "   N/A "
        def fp(v): return f"{v*100:8.2f}%" if v is not None else "     N/A"
        print(f"  {row[0]:<7} {f(row[1])} {f(row[2])} {f(row[3])} {f(row[4])} {f(row[5])} {f(row[6])} {fp(row[7])}")
    con.close()
    print(f"\n✓ chicken.db ready  ({os.path.getsize(DB_PATH)//1024} KB)")

# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    print("="*60)
    print("U.S. Chicken Spread Tracker — Database Extractor")
    print("="*60)

    # Try Excel files first (preferred for historical depth)
    script_dir   = os.path.dirname(os.path.abspath(__file__))
    excel_base   = os.path.join(script_dir, "..", "..", "U.S. Chicken")  # relative to repo structure
    if not os.path.isdir(excel_base):
        excel_base = os.path.join(os.path.expanduser("~"), "OneDrive",
                                  "Documentos", "Claude", "U.S. Chicken")  # Windows OneDrive fallback

    data = {}
    if os.path.isdir(excel_base):
        print(f"\n[1/2] Loading from Excel files at {excel_base} …")
        data = load_from_excel(excel_base)
    else:
        print(f"\n[1/2] Excel source not found — fetching from USDA APIs …")
        print("  Fetching broiler wholesale …")
        bw_rows = fetch_bw_wholesale()
        print(f"  → {len(bw_rows)} rows")
        print("  Fetching chicken parts (AMS-3646) …")
        parts = fetch_parts()
        for k,v in parts.items():
            print(f"  → {k}: {len(v)} rows")
        print("  Fetching SBM (AMS-3511) …")
        sbm_rows = fetch_sbm()
        print(f"  → {len(sbm_rows)} rows")
        print("  Fetching Corn (AMS-3192) …")
        corn_rows = fetch_corn()
        print(f"  → {len(corn_rows)} rows")
        data = {
            "bw_rows":      bw_rows,
            "breast_rows":  parts.get("breast",  []),
            "leg_rows":     parts.get("leg_qtrs",[]),
            "wings_rows":   parts.get("wings",   []),
            "tenders_rows": parts.get("tenders", []),
            "sbm_rows":     sbm_rows,
            "corn_rows":    corn_rows,
        }

    print(f"\n[2/2] Building database …")
    build_db(data)

if __name__ == "__main__":
    main()
