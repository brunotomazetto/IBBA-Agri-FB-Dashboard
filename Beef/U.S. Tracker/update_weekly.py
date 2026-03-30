"""
update_weekly.py
────────────────
Weekly updater: downloads the latest USDA and EIA data, processes it,
and upserts new rows into the Supabase beef_weekly table.
Also recomputes the quarterly averages for the current (and optionally
the previous) quarter and upserts them into beef_quarterly.

Run manually or schedule via GitHub Actions / cron every Monday.

Sources:
  ① CT150 (5-area cattle price)   → USDA AMS Datamart API  (report 2461)
  ② Cutout (Choice & Select)      → USDA AMS Datamart API  (report 2527)
  ③ Cattle Slaughter              → USDA NASS Quick Stats API
  ④ KS prices                     → USDA AMS PDF report 2484 (mymarketnews.ams.usda.gov/viewReport/2484)
     NE prices                    → USDA AMS PDF report 2667 (mymarketnews.ams.usda.gov/viewReport/2667)
                                    "WEEKLY ACCUMULATED" section → Live Steer / Heifer Avg Price
  ⑤ Drop Credit                   → USDA AMS PDF report 2829 (www.ams.usda.gov/mnreports/ams_2829.pdf)
                                    "By-Product Values ($/CWT) - STEER" table
                                    Stored as "Dressed Equivalent Basis" ($/cwt dressed weight)
  ⑥ Henry Hub                     → EIA Open Data API  (series NG.RNGWHHD.D)

Dependencies:
    pip install supabase requests pandas pdfplumber

Usage:
    python update_weekly.py [--full]   # --full rebuilds all quarterly rows
"""

import argparse, io, re, sys
from datetime import date, timedelta
from collections import defaultdict

import requests
import pandas as pd
from supabase import create_client, Client

try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False
    print("⚠ pdfplumber not installed — KS/NE and Drop Credit will be skipped."
          "\n  Run: pip install pdfplumber")

# ─── CONFIG ──────────────────────────────────────────────────────────────────
SUPABASE_URL      = "https://vhxvlmyataclkpamvzpe.supabase.co"
SUPABASE_SERVICE  = "YOUR_SERVICE_ROLE_KEY"   # never commit this — use env var
EIA_API_KEY       = "YOUR_EIA_API_KEY"        # https://www.eia.gov/opendata/

# How many weeks back to look for new data (set higher for backfill runs)
LOOKBACK_WEEKS = 6
# ─────────────────────────────────────────────────────────────────────────────

sb: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE)

# ── helpers ───────────────────────────────────────────────────────────────────
def fv(v):
    try:
        f = float(v)
        import math; return None if math.isnan(f) else round(f, 4)
    except: return None

def week_end_sat(d: date) -> date:
    """Shift any date to the following/same Saturday (day 5)."""
    return d + timedelta(days=(5 - d.weekday()) % 7)

def quarter_label(d: date) -> str:
    q = (d.month - 1) // 3 + 1
    return f"{q}Q{str(d.year)[2:]}"

def quarter_start(q: str) -> date:
    m = re.match(r"([1-4])Q(\d{2})", q)
    if not m: return None
    qn, yr = int(m.group(1)), 2000 + int(m.group(2))
    return date(yr, (qn - 1) * 3 + 1, 1)

def since_date() -> str:
    return (date.today() - timedelta(weeks=LOOKBACK_WEEKS)).isoformat()

def _parse_ams_date(text: str) -> date:
    """
    Extract the week-ending date from an AMS PDF report and return the
    corresponding Saturday.  Tries several common AMS date formats.
    Falls back to the current week's Saturday if nothing parses.
    """
    patterns = [
        r'[Ww]eek\s+[Ee]nd(?:ing)?\s*[:\-]?\s*(\w+ \d{1,2},?\s+\d{4})',
        r'[Ff]or (?:the )?[Ww]eek (?:of\s+)?(\w+ \d{1,2},?\s+\d{4})',
        r'[Ww]eek\s+[Ee]nd(?:ing)?\s*[:\-]?\s*(\d{1,2}/\d{1,2}/\d{2,4})',
        r'(\w{3,9} \d{1,2},?\s+\d{4})',   # bare month-day-year as last resort
    ]
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            try:
                return week_end_sat(pd.to_datetime(m.group(1)).date())
            except Exception:
                continue
    return week_end_sat(date.today())   # fallback: use this week's Saturday

# ══════════════════════════════════════════════════════════════════════════════
# ① CT150 — USDA AMS Datamart
# Report: LM_CT150  (5-Area Weekly Weighted Average Direct Slaughter Cattle)
# ══════════════════════════════════════════════════════════════════════════════
def fetch_ct150() -> pd.DataFrame:
    """Returns DataFrame with columns: week_end_sat, ct150_steer, ct150_heifer,
    ct150_mixed, ct150_all"""
    url = "https://marsapi.ams.usda.gov/services/v1.2/reports/2461"
    params = {
        "q": f"report_date>={since_date()}",
        "allSections": "true",
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    records = data.get("results", [])
    if not records:
        print("  CT150: no data returned"); return pd.DataFrame()

    CLASS_MAP = {"Steers": "ct150_steer", "Heifers": "ct150_heifer",
                 "Mixed": "ct150_mixed", "All Beef": "ct150_all"}
    rows = defaultdict(dict)
    for rec in records:
        raw_date = pd.to_datetime(rec.get("report_date")).date()
        sat = week_end_sat(raw_date - timedelta(days=2))  # Monday report → -2 = Saturday
        cls = rec.get("class_description", "")
        col = CLASS_MAP.get(cls)
        price = fv(rec.get("wtd_avg"))
        if col and price is not None:
            rows[sat][col] = price
    df = pd.DataFrame.from_dict(rows, orient="index")
    df.index.name = "week_ending"
    df = df.reset_index()
    print(f"  CT150: {len(df)} weeks")
    return df

# ══════════════════════════════════════════════════════════════════════════════
# ② Cutout (Choice & Select) — USDA AMS Datamart
# Report: LM_XB459  (Weekly Average Cutout Values)
# ══════════════════════════════════════════════════════════════════════════════
def fetch_cutout() -> pd.DataFrame:
    url = "https://marsapi.ams.usda.gov/services/v1.2/reports/2527"
    params = {"q": f"report_date>={since_date()}", "allSections": "true"}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    records = r.json().get("results", [])

    GRADE_MAP = {"Choice 600-900": "choice", "Select 600-900": "select_"}
    rows = defaultdict(dict)
    for rec in records:
        raw_date = pd.to_datetime(rec.get("report_date")).date()
        sat = week_end_sat(raw_date + timedelta(days=1))  # Friday report → +1 = Saturday
        grade = rec.get("grade", "") + " " + rec.get("weight_range", "")
        grade = grade.strip()
        col = GRADE_MAP.get(grade)
        price = fv(rec.get("avg_price"))
        if col and price is not None:
            rows[sat][col] = price
    df = pd.DataFrame.from_dict(rows, orient="index")
    df.index.name = "week_ending"
    df = df.reset_index()
    print(f"  Cutout: {len(df)} weeks")
    return df

# ══════════════════════════════════════════════════════════════════════════════
# ③ Slaughter — USDA NASS Quick Stats API
# ══════════════════════════════════════════════════════════════════════════════
def fetch_slaughter() -> pd.DataFrame:
    """
    ⚠ CONFIRM WITH USER: NASS Quick Stats provides weekly cattle slaughter.
    The API key can be obtained free at: https://quickstats.nass.usda.gov/api
    """
    NASS_KEY = "YOUR_NASS_API_KEY"   # ← add your key here
    url = "https://quickstats.nass.usda.gov/api/api_GET/"
    params = {
        "key":           NASS_KEY,
        "commodity_desc":"CATTLE",
        "statisticcat_desc":"SLAUGHTER",
        "unit_desc":     "HEAD",
        "freq_desc":     "WEEKLY",
        "begin_DT":      since_date(),
        "format":        "JSON",
    }
    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json().get("data", [])
    except Exception as e:
        print(f"  Slaughter fetch failed: {e}"); return pd.DataFrame()

    rows = {}
    for rec in data:
        raw = rec.get("week_ending") or rec.get("end_code","")
        if not raw: continue
        try: d = pd.to_datetime(raw).date()
        except: continue
        sat = week_end_sat(d)
        v = fv(rec.get("Value","").replace(",",""))
        if v: rows[sat] = v

    df = pd.DataFrame(list(rows.items()), columns=["week_ending","slaughter"])
    print(f"  Slaughter: {len(df)} weeks")
    return df

# ══════════════════════════════════════════════════════════════════════════════
# ④ Kansas & Nebraska weekly cash cattle prices
# Sources:
#   KS → USDA AMS report 2484  (mymarketnews.ams.usda.gov/viewReport/2484)
#   NE → USDA AMS report 2667  (mymarketnews.ams.usda.gov/viewReport/2667)
#
# PDF layout (end of report, "WEEKLY ACCUMULATED" section):
#   WEEKLY ACCUMULATED   Head Count   Avg Weight   Avg Price
#   Live  Steer          2,323        1,511.80      $235.05
#   Live  Heifer         2,521        1,414.90      $235.00
#
# Strategy: try MARS JSON API first (cleaner); fall back to PDF text parsing.
# ══════════════════════════════════════════════════════════════════════════════
def fetch_ks_ne() -> pd.DataFrame:
    """
    Returns DataFrame: week_ending, ks_steer, ks_heifer, ks_avg,
                                    ne_steer, ne_heifer, ne_avg
    """
    REPORTS = {
        "ks": {"mars_id": "2484", "pdf_slug": "ams_2484"},
        "ne": {"mars_id": "2667", "pdf_slug": "ams_2667"},
    }

    rows: dict[date, dict] = {}

    for state, cfg in REPORTS.items():
        steer_col  = f"{state}_steer"
        heifer_col = f"{state}_heifer"
        fetched = False

        # ── Attempt 1: MARS API (JSON) ──────────────────────────────────────
        try:
            url = f"https://marsapi.ams.usda.gov/services/v1.2/reports/{cfg['mars_id']}"
            params = {"q": f"report_date>={since_date()}", "allSections": "true"}
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            records = r.json().get("results", [])

            for rec in records:
                # Filter to "Weekly Accumulated" / "Live" / Steer or Heifer rows
                section = str(rec.get("report_section", "")).lower()
                class_  = str(rec.get("class_description", "")).lower()
                if "accum" not in section and "weekly" not in section:
                    continue
                if "live" not in str(rec.get("type_description", "")).lower():
                    continue
                price = fv(rec.get("wtd_avg") or rec.get("avg_price"))
                if price is None:
                    continue
                raw_date = pd.to_datetime(rec.get("report_date")).date()
                sat = week_end_sat(raw_date)
                rows.setdefault(sat, {})
                if "steer" in class_:
                    rows[sat][steer_col] = price
                    fetched = True
                elif "heifer" in class_:
                    rows[sat][heifer_col] = price
                    fetched = True

            if fetched:
                print(f"  {state.upper()}: fetched via MARS API")
                continue
        except Exception as e:
            print(f"  {state.upper()} MARS API error ({e}); trying PDF…")

        # ── Attempt 2: PDF parsing ───────────────────────────────────────────
        if not HAS_PDFPLUMBER:
            print(f"  {state.upper()}: pdfplumber not available — skipping")
            continue
        try:
            pdf_url = f"https://www.ams.usda.gov/mnreports/{cfg['pdf_slug']}.pdf"
            r = requests.get(pdf_url, timeout=30)
            r.raise_for_status()
            with pdfplumber.open(io.BytesIO(r.content)) as pdf:
                text = "\n".join(page.extract_text() or "" for page in pdf.pages)

            sat = _parse_ams_date(text)

            # Locate the "WEEKLY ACCUMULATED" section
            acc_idx = text.upper().find("WEEKLY ACCUMULATED")
            if acc_idx == -1:
                print(f"  {state.upper()}: 'WEEKLY ACCUMULATED' header not found in PDF")
                continue
            section = text[acc_idx : acc_idx + 700]

            rows.setdefault(sat, {})
            for animal, col in [("Steer", steer_col), ("Heifer", heifer_col)]:
                # Match: Live  Steer  2,323  1,511.80  $235.05
                m = re.search(
                    r"Live\s+" + animal + r"\s+[\d,]+\s+[\d,.]+\s+\$?([\d.]+)",
                    section, re.IGNORECASE
                )
                if m:
                    rows[sat][col] = fv(m.group(1))

            ks_s = rows[sat].get(steer_col)
            ks_h = rows[sat].get(heifer_col)
            print(f"  {state.upper()}: steer=${ks_s}, heifer=${ks_h}  (week {sat})")
        except Exception as e:
            print(f"  {state.upper()} PDF error: {e}")

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame.from_dict(rows, orient="index")
    df.index.name = "week_ending"
    # KS and NE averages computed here; also recomputed in merge_and_upsert_weekly
    for st in ["ks", "ne"]:
        sc, hc, ac = f"{st}_steer", f"{st}_heifer", f"{st}_avg"
        if sc in df.columns and hc in df.columns:
            df[ac] = df[[sc, hc]].mean(axis=1, skipna=False)
    return df.reset_index()


# ══════════════════════════════════════════════════════════════════════════════
# ⑤ Drop Credit (by-product value)
# Source: USDA AMS report 2829
#   PDF:  https://www.ams.usda.gov/mnreports/ams_2829.pdf
#
# PDF layout (end of "By-Product Values ($/CWT) - STEER" table):
#   Totals:   18.70   [...]   12.34
#   Dressed Equivalent Basis (63.0%):   19.59
#
# We store the "Dressed Equivalent Basis" value ($/cwt dressed weight).
# This is directly comparable to the cutout (Choice $/cwt carcass).
# To convert to $/head: drop_credit × (avg_dressed_weight_lbs / 100)
#   e.g.  19.59 × (880 lbs / 100) ≈ $172/head
# ══════════════════════════════════════════════════════════════════════════════
def fetch_drop_credit() -> pd.DataFrame:
    """
    Returns DataFrame: week_ending, drop_credit  ($/cwt dressed weight)
    """
    if not HAS_PDFPLUMBER:
        print("  Drop Credit: pdfplumber not available — skipping")
        return pd.DataFrame()

    url = "https://www.ams.usda.gov/mnreports/ams_2829.pdf"
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
    except Exception as e:
        print(f"  Drop Credit fetch error: {e}")
        return pd.DataFrame()

    with pdfplumber.open(io.BytesIO(r.content)) as pdf:
        text = "\n".join(page.extract_text() or "" for page in pdf.pages)

    sat = _parse_ams_date(text)
    drop_val = None

    # Primary: "Dressed Equivalent Basis (63.0%):   19.59"
    m = re.search(
        r"[Dd]ressed\s+[Ee]quivalent\s+[Bb]asis\s*\([^)]+\)\s*[:\s]+([\d.]+)",
        text
    )
    if m:
        drop_val = fv(m.group(1))
        print(f"  Drop Credit: {drop_val} $/cwt dressed  (week {sat})")

    # Fallback: last numeric token on the "Totals" line  ($/cwt live weight)
    if drop_val is None:
        m = re.search(r"[Tt]otals?\s*:?\s*[\d.]+\s+[\d.]+\s+([\d.]+)\s*$",
                      text, re.MULTILINE)
        if m:
            drop_val = fv(m.group(1))
            print(f"  Drop Credit: {drop_val} $/cwt live (Totals fallback, week {sat})")

    if drop_val is None:
        print(f"  Drop Credit: could not parse value from {url}")
        return pd.DataFrame()

    return pd.DataFrame([{"week_ending": sat, "drop_credit": drop_val}])

# ══════════════════════════════════════════════════════════════════════════════
# ⑥ Henry Hub — EIA Open Data API
# ══════════════════════════════════════════════════════════════════════════════
def fetch_henry_hub() -> pd.DataFrame:
    """Daily Henry Hub spot price; averaged to week-ending Saturday."""
    url = "https://api.eia.gov/v2/natural-gas/pri/fut/data/"
    params = {
        "api_key":        EIA_API_KEY,
        "frequency":      "daily",
        "data[0]":        "value",
        "facets[series][]": "EBA.MISO-ALL.NG.NGPD.H",   # HH daily spot
        "start":          since_date(),
        "sort[0][column]":"period",
        "sort[0][direction]":"asc",
        "length":         500,
    }
    # Alternative: Henry Hub Natural Gas Spot Price (Dollars per Million Btu)
    # series: NG.RNGWHHD.D
    params_alt = {
        "api_key":       EIA_API_KEY,
        "frequency":     "daily",
        "data[0]":       "value",
        "facets[series][]":"NG.RNGWHHD.D",
        "start":         since_date(),
        "sort[0][column]":"period",
        "sort[0][direction]":"asc",
        "length":        500,
    }
    try:
        r = requests.get(
            "https://api.eia.gov/v2/natural-gas/pri/sum/data/",
            params=params_alt, timeout=30
        )
        r.raise_for_status()
        records = r.json().get("response", {}).get("data", [])
    except Exception as e:
        print(f"  HH fetch failed: {e}"); return pd.DataFrame()

    daily = {}
    for rec in records:
        try: d = pd.to_datetime(rec["period"]).date()
        except: continue
        v = fv(rec.get("value"))
        if v: daily[d] = v

    # Group by week-ending Saturday
    weekly = defaultdict(list)
    for d, v in daily.items():
        sat = week_end_sat(d)
        weekly[sat].append(v)
    rows = [{"week_ending": sat, "henry_hub": round(sum(vals)/len(vals), 4)}
            for sat, vals in weekly.items() if vals]
    df = pd.DataFrame(rows)
    print(f"  Henry Hub: {len(df)} weeks")
    return df

# ══════════════════════════════════════════════════════════════════════════════
# MERGE + UPSERT WEEKLY
# ══════════════════════════════════════════════════════════════════════════════
def merge_and_upsert_weekly():
    dfs = []
    for name, fn in [("CT150", fetch_ct150), ("Cutout", fetch_cutout),
                     ("Slaughter", fetch_slaughter), ("KS-NE", fetch_ks_ne),
                     ("Drop", fetch_drop_credit), ("HH", fetch_henry_hub)]:
        try:
            df = fn()
            if not df.empty:
                df["week_ending"] = pd.to_datetime(df["week_ending"]).dt.date
                dfs.append(df.set_index("week_ending"))
        except Exception as e:
            print(f"  {name} error: {e}")

    if not dfs:
        print("No weekly data to upload."); return

    merged = dfs[0]
    for df in dfs[1:]:
        merged = merged.join(df, how="outer")

    # KS avg and NE avg (if both steer and heifer present)
    if "ks_steer" in merged.columns and "ks_heifer" in merged.columns:
        merged["ks_avg"] = (merged["ks_steer"].fillna(0) + merged["ks_heifer"].fillna(0)) / 2
        merged["ks_avg"] = merged.apply(
            lambda r: None if (pd.isna(r["ks_steer"]) and pd.isna(r["ks_heifer"])) else r["ks_avg"], axis=1)
    if "ne_steer" in merged.columns and "ne_heifer" in merged.columns:
        merged["ne_avg"] = (merged["ne_steer"].fillna(0) + merged["ne_heifer"].fillna(0)) / 2
        merged["ne_avg"] = merged.apply(
            lambda r: None if (pd.isna(r["ne_steer"]) and pd.isna(r["ne_heifer"])) else r["ne_avg"], axis=1)

    merged = merged.reset_index()
    merged["week_ending"] = merged["week_ending"].apply(lambda d: d.isoformat() if d else None)
    merged = merged.where(pd.notnull(merged), None)
    rows = merged.to_dict("records")

    print(f"\n  Upserting {len(rows)} weekly rows…")
    for i in range(0, len(rows), 200):
        sb.table("beef_weekly").upsert(rows[i:i+200], on_conflict="week_ending").execute()
    print(f"  ✓ beef_weekly updated")

# ══════════════════════════════════════════════════════════════════════════════
# RECOMPUTE QUARTERLY AVERAGES FOR AFFECTED QUARTERS
# ══════════════════════════════════════════════════════════════════════════════
MARKET_COLS = ["slaughter","ct150_steer","ct150_heifer","ct150_mixed","ct150_all",
               "ks_steer","ks_heifer","ks_avg","ne_steer","ne_heifer","ne_avg",
               "choice","select_","drop_credit","henry_hub"]

def recompute_quarterly(full=False):
    """Pull beef_weekly from Supabase, compute quarterly averages, upsert."""
    if full:
        resp = sb.table("beef_weekly").select("*").execute()
    else:
        resp = sb.table("beef_weekly").select("*").gte("week_ending", since_date()).execute()

    weekly_rows = resp.data or []
    if not weekly_rows:
        print("  No weekly rows found for quarterly recompute."); return

    df = pd.DataFrame(weekly_rows)
    df["week_ending"] = pd.to_datetime(df["week_ending"]).dt.date
    df["quarter"] = df["week_ending"].apply(quarter_label)
    df["quarter_start"] = df["week_ending"].apply(
        lambda d: quarter_start(quarter_label(d)).isoformat())

    agg = df.groupby(["quarter","quarter_start"])[MARKET_COLS].mean().reset_index()
    agg = agg.where(pd.notnull(agg), None)

    # Pull company financials for affected quarters (keep existing values)
    quarters = agg["quarter"].tolist()
    fin_resp = sb.table("beef_quarterly").select("*").in_("quarter", quarters).execute()
    fin_map = {r["quarter"]: r for r in (fin_resp.data or [])}

    rows = []
    for _, row in agg.iterrows():
        q = row["quarter"]
        rec = {"quarter": q, "quarter_start": row["quarter_start"]}
        for col in MARKET_COLS:
            v = row.get(col)
            rec[col] = round(float(v), 4) if v is not None else None
        # Preserve existing company financials
        existing = fin_map.get(q, {})
        for fin_col in ["mbrf_revenue","mbrf_gp","mbrf_gm","mbrf_ebitda","mbrf_ebitda_mgn",
                        "jbs_revenue","jbs_gp","jbs_gm","jbs_ebit","jbs_ebit_mgn",
                        "jbs_ebitda","jbs_ebitda_mgn",
                        "tyson_sales","tyson_adj_op_inc","tyson_adj_op_mgn"]:
            rec[fin_col] = existing.get(fin_col)
        rows.append(rec)

    print(f"  Recomputing {len(rows)} quarterly rows…")
    for i in range(0, len(rows), 100):
        sb.table("beef_quarterly").upsert(rows[i:i+100], on_conflict="quarter").execute()
    print("  ✓ beef_quarterly updated")

# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--full", action="store_true",
                        help="Recompute ALL quarterly rows (not just recent)")
    args = parser.parse_args()

    print("=== Weekly Update Run ===")
    print(f"Looking back {LOOKBACK_WEEKS} weeks from today ({date.today()})\n")
    merge_and_upsert_weekly()
    print()
    recompute_quarterly(full=args.full)
    print("\n✓ Update complete.")
