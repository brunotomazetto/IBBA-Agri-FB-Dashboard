#!/usr/bin/env python3
"""
extractor_beef.py — U.S. Beef Packer Margin Tracker
All data fetched from USDA AMS PDFs — no API keys required.

PDF sources (always current week):
  CT150  (5-Area live steer/heifer): https://www.ams.usda.gov/mnreports/lm_ct150.pdf
  Cutout (Choice / Select boxed beef): https://www.ams.usda.gov/mnreports/ams_2461.pdf
  Kansas weekly price:                 https://www.ams.usda.gov/mnreports/ams_2484.pdf
  Nebraska weekly price:               https://www.ams.usda.gov/mnreports/ams_2667.pdf

Usage:
  python extractor_beef.py                  # default: update latest week from PDFs
  python extractor_beef.py --history FILE   # one-time load from V4 Excel
  python extractor_beef.py --full           # force full quarterly recompute
"""

import os, re, io, sqlite3, argparse, datetime, logging
import requests
import pdfplumber
import pandas as pd

logging.basicConfig(level=logging.INFO, format="  %(message)s")
log = logging.getLogger(__name__)

# ── Paths & URLs ──────────────────────────────────────────────────────────────
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "beef.db")

PDF_URLS = {
    "ct150":    "https://www.ams.usda.gov/mnreports/lm_ct150.pdf",
    "cutout":   "https://www.ams.usda.gov/mnreports/ams_2461.pdf",
    "kansas":   "https://www.ams.usda.gov/mnreports/ams_2484.pdf",
    "nebraska": "https://www.ams.usda.gov/mnreports/ams_2667.pdf",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/pdf,*/*",
}


# ══ DATABASE ══════════════════════════════════════════════════════════════════

def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS beef_weekly (
        week_ending   TEXT PRIMARY KEY,
        choice        REAL,
        select_       REAL,
        ct150_steer   REAL,
        ct150_heifer  REAL,
        ks_avg        REAL,
        ne_avg        REAL
    );
    CREATE TABLE IF NOT EXISTS beef_quarterly (
        quarter       TEXT PRIMARY KEY,
        quarter_start TEXT,
        choice        REAL,
        select_       REAL,
        ct150_steer   REAL,
        ct150_heifer  REAL,
        ct150_all     REAL,
        ks_avg        REAL,
        ne_avg        REAL,
        mbrf_gm       REAL,
        jbs_gm        REAL
    );
    """)
    conn.commit()


def upsert_weekly(conn: sqlite3.Connection, rows: list[dict]) -> int:
    sql = """
    INSERT INTO beef_weekly (week_ending, choice, select_, ct150_steer, ct150_heifer, ks_avg, ne_avg)
    VALUES (:week_ending,:choice,:select_,:ct150_steer,:ct150_heifer,:ks_avg,:ne_avg)
    ON CONFLICT(week_ending) DO UPDATE SET
        choice       = COALESCE(excluded.choice,       choice),
        select_      = COALESCE(excluded.select_,      select_),
        ct150_steer  = COALESCE(excluded.ct150_steer,  ct150_steer),
        ct150_heifer = COALESCE(excluded.ct150_heifer, ct150_heifer),
        ks_avg       = COALESCE(excluded.ks_avg,       ks_avg),
        ne_avg       = COALESCE(excluded.ne_avg,       ne_avg)
    """
    conn.executemany(sql, rows)
    conn.commit()
    return len(rows)


# ══ PDF HELPERS ═══════════════════════════════════════════════════════════════

def fetch_pdf_text(key: str) -> str:
    """Download a PDF and return all pages concatenated as plain text."""
    url = PDF_URLS[key]
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    text_pages = []
    with pdfplumber.open(io.BytesIO(r.content)) as pdf:
        for page in pdf.pages:
            t = page.extract_text()
            if t:
                text_pages.append(t)
    return "\n".join(text_pages)


def _parse_date(text: str) -> str:
    """
    Try to extract the 'week ending' date from a USDA PDF header.
    Returns ISO date string 'YYYY-MM-DD' or Saturday of current week as fallback.
    """
    patterns = [
        # "Week Ending Sunday, 4/5/2026"
        r"[Ww]eek\s+[Ee]nding\s+\w+,?\s+(\d{1,2}/\d{1,2}/\d{4})",
        # "For Week Ending April 4, 2026"
        r"[Ww]eek\s+[Ee]nding\s+(\w+ \d{1,2},? \d{4})",
        # "4/5/2026"  or  "04/05/2026"
        r"\b(\d{1,2}/\d{1,2}/\d{4})\b",
        # "April 4, 2026" or "April 04, 2026"
        r"\b([A-Za-z]+ \d{1,2},?\s+\d{4})\b",
    ]
    for pat in patterns:
        m = re.search(pat, text[:800])
        if m:
            raw = m.group(1).strip().rstrip(",")
            for fmt in ("%m/%d/%Y", "%B %d %Y", "%B %d, %Y", "%b %d %Y", "%b %d, %Y"):
                try:
                    return datetime.datetime.strptime(raw, fmt).date().isoformat()
                except ValueError:
                    continue
    # fallback: last Saturday
    today = datetime.date.today()
    sat = today - datetime.timedelta(days=(today.weekday() + 2) % 7)
    log.warning("Could not parse date from PDF header; using %s", sat)
    return sat.isoformat()


def _num(s: str) -> float | None:
    """Parse a number string like '$245.82' or '22,550' → float."""
    try:
        return float(re.sub(r"[$,]", "", s.strip()))
    except (ValueError, AttributeError):
        return None


# ══ CT150 — lm_ct150.pdf ══════════════════════════════════════════════════════
# Layout (WEEKLY ACCUMULATED section near end of report):
#
#   WEEKLY ACCUMULATED    Head Count    Avg Weight    Avg Price
#   Live    Steer          2,877         1,501.60      $245.82
#   Live    Heifer         2,743         1,365.30      $245.83
#
# Also has LIVE FOB "Total all grades" rows per section:
#   Total all grades    22,550  1,275•1,750  238.00•246.50  1,571  244.96
#                                                                   ^^^^^^ Wtd Avg Price

def fetch_ct150() -> dict:
    """
    Returns dict with keys: week_ending, ct150_steer, ct150_heifer
    Parses WEEKLY ACCUMULATED section first; falls back to LIVE FOB totals.
    """
    text = fetch_pdf_text("ct150")
    week = _parse_date(text)
    result = {"week_ending": week, "ct150_steer": None, "ct150_heifer": None}

    # Primary: WEEKLY ACCUMULATED block
    # Pattern: Live  Steer  <count>  <avg_weight>  $<avg_price>
    m_steer = re.search(
        r"WEEKLY\s+ACCUMULATED.*?Live\s+Steer\s+[\d,]+\s+[\d,.]+\s+\$?([\d.]+)",
        text, re.IGNORECASE | re.DOTALL
    )
    m_heifer = re.search(
        r"WEEKLY\s+ACCUMULATED.*?Live\s+Heifer\s+[\d,]+\s+[\d,.]+\s+\$?([\d.]+)",
        text, re.IGNORECASE | re.DOTALL
    )
    if m_steer:
        result["ct150_steer"] = _num(m_steer.group(1))
    if m_heifer:
        result["ct150_heifer"] = _num(m_heifer.group(1))

    # Fallback: "Total all grades" row under STEERS: LIVE FOB
    # The Wtd Avg Price is the last number on that line
    if result["ct150_steer"] is None:
        m = re.search(
            r"STEERS:?\s+LIVE\s+FOB.*?Total\s+all\s+grades\s+[\d,]+\s+[\d•,. -]+\s+([\d.]+)",
            text, re.IGNORECASE | re.DOTALL
        )
        if m:
            result["ct150_steer"] = _num(m.group(1))

    if result["ct150_steer"] is not None:
        log.info("CT150: steer=$%.2f, heifer=%s  (week %s)",
                 result["ct150_steer"],
                 f"${result['ct150_heifer']:.2f}" if result["ct150_heifer"] else "n/a",
                 week)
    else:
        log.warning("CT150: could not parse price from PDF")

    return result


# ══ CUTOUT — ams_2461.pdf (report LM_XB459) ══════════════════════════════════
# "National Weekly Boxed Beef Cutout And Boxed Beef Cuts - Negotiated Sales"
# Values in US dollars per 100 lbs ($/cwt).
#
# Target: "Weekly Cutout Value Summary" → "Weekly Average" row
#
#   Weekly Cutout Value Summary
#   Date   Choice  Select  Trim  Grinds  Total   Choice    Select
#                                               600-900   600-900
#   04/03    73       4     12     13     102    387.78    386.19
#   ...
#   Weekly Average                              392.28    390.09   ← extract these
#   Change From Prior Week                       -2.44     -2.99
#   Choice/Select Spread:  2.19
#
# The "Weekly Average" line has the two cutout values as the last two numbers.

def fetch_cutout() -> dict:
    """
    Returns dict with keys: week_ending, choice, select_
    Parses the 'Weekly Average' row from LM_XB459 (ams_2461.pdf).
    """
    text = fetch_pdf_text("cutout")
    week = _parse_date(text)
    result = {"week_ending": week, "choice": None, "select_": None}

    # Primary: "Weekly Average  ...  392.28  390.09"
    # The line ends with two $/cwt values (Choice 600-900, Select 600-900)
    m = re.search(
        r"Weekly\s+Average\s+[\d.\s-]*([\d]{2,3}\.\d{2})\s+([\d]{2,3}\.\d{2})",
        text, re.IGNORECASE
    )
    if m:
        result["choice"]  = _num(m.group(1))
        result["select_"] = _num(m.group(2))

    # Fallback A: look for the two numbers right after "Weekly Average"
    if result["choice"] is None:
        m = re.search(
            r"Weekly\s+Average[^\n]*([\d]{2,3}\.\d{2})[^\n]*([\d]{2,3}\.\d{2})",
            text, re.IGNORECASE
        )
        if m:
            result["choice"]  = _num(m.group(1))
            result["select_"] = _num(m.group(2))

    # Fallback B: scan for "Choice/Select Spread" and work backwards
    if result["choice"] is None:
        # Find all $/cwt-range numbers (250-500 range) near "Weekly Average"
        block = re.search(r"Weekly\s+Average(.{0,200})", text, re.IGNORECASE | re.DOTALL)
        if block:
            nums = re.findall(r"\b(\d{3}\.\d{2})\b", block.group(1))
            if len(nums) >= 2:
                result["choice"]  = float(nums[0])
                result["select_"] = float(nums[1])

    if result["choice"] is not None:
        log.info("Cutout: Choice=%.2f, Select=%s  (week %s)",
                 result["choice"],
                 f"{result['select_']:.2f}" if result["select_"] else "n/a",
                 week)
    else:
        log.warning("Cutout: could not parse Choice/Select from PDF (LM_XB459)")

    return result


# ══ KANSAS — ams_2484.pdf ═════════════════════════════════════════════════════
# WEEKLY ACCUMULATED section (same layout as CT150):
#
#   WEEKLY ACCUMULATED    Head Count    Avg Weight    Avg Price
#   Live    Steer          X,XXX         X,XXX.XX      $XXX.XX
#   Live    Heifer         X,XXX         X,XXX.XX      $XXX.XX

def _parse_weekly_accumulated(text: str, label: str) -> tuple[float | None, str]:
    """
    Generic parser for USDA reports that have a WEEKLY ACCUMULATED section.
    Returns (avg_price_steer, week_ending_iso).
    Falls back to heifer price if steer is missing.
    """
    week = _parse_date(text)

    # Try steer first
    m = re.search(
        r"WEEKLY\s+ACCUMULATED.*?Live\s+Steer\s+[\d,]+\s+[\d,.]+\s+\$?([\d.]+)",
        text, re.IGNORECASE | re.DOTALL
    )
    if m:
        return _num(m.group(1)), week

    # Try heifer
    m = re.search(
        r"WEEKLY\s+ACCUMULATED.*?Live\s+Heifer\s+[\d,]+\s+[\d,.]+\s+\$?([\d.]+)",
        text, re.IGNORECASE | re.DOTALL
    )
    if m:
        log.info("%s: steer not found, using heifer price", label)
        return _num(m.group(1)), week

    # Fallback: any dollar price after "WEEKLY ACCUMULATED"
    m = re.search(
        r"WEEKLY\s+ACCUMULATED.*?\$\s*([\d.]+)",
        text, re.IGNORECASE | re.DOTALL
    )
    if m:
        log.info("%s: using first price after WEEKLY ACCUMULATED as fallback", label)
        return _num(m.group(1)), week

    log.warning("%s: 'WEEKLY ACCUMULATED' block not parsed", label)
    return None, week


def fetch_kansas() -> dict:
    text = fetch_pdf_text("kansas")
    price, week = _parse_weekly_accumulated(text, "KS")
    if price:
        log.info("KS: avg=$%.2f  (week %s)", price, week)
    return {"week_ending": week, "ks_avg": price}


def fetch_nebraska() -> dict:
    # NOTE: Nebraska report is AMS 2667 (ams_2667.pdf), same layout as Kansas.
    text = fetch_pdf_text("nebraska")
    price, week = _parse_weekly_accumulated(text, "NE")
    if price:
        log.info("NE: avg=$%.2f  (week %s)", price, week)
    return {"week_ending": week, "ne_avg": price}


# ══ MERGE WEEKLY DATA ═════════════════════════════════════════════════════════

def build_weekly_rows(ct150: dict, cutout: dict, ks: dict, ne: dict) -> list[dict]:
    """
    Merge data from the four PDFs into a list of weekly rows keyed by week_ending.
    Each PDF may have a slightly different week_ending date (e.g. Friday vs Saturday).
    We normalise to the latest date seen across all four sources.
    """
    dates = [d for d in [
        ct150.get("week_ending"), cutout.get("week_ending"),
        ks.get("week_ending"),    ne.get("week_ending")
    ] if d]
    # Use the single most common date, or max date if all differ
    from collections import Counter
    week = Counter(dates).most_common(1)[0][0] if dates else datetime.date.today().isoformat()

    row = {
        "week_ending":   week,
        "choice":        cutout.get("choice"),
        "select_":       cutout.get("select_"),
        "ct150_steer":   ct150.get("ct150_steer"),
        "ct150_heifer":  ct150.get("ct150_heifer"),
        "ks_avg":        ks.get("ks_avg"),
        "ne_avg":        ne.get("ne_avg"),
    }
    return [row]


# ══ QUARTERLY RECOMPUTE ═══════════════════════════════════════════════════════

def quarter_label(d: datetime.date) -> str:
    return f"{((d.month - 1) // 3) + 1}Q{str(d.year)[-2:]}"

def quarter_start(d: datetime.date) -> datetime.date:
    m = ((d.month - 1) // 3) * 3 + 1
    return datetime.date(d.year, m, 1)


def recompute_quarterly(conn: sqlite3.Connection, full: bool = False) -> None:
    df = pd.read_sql("SELECT * FROM beef_weekly ORDER BY week_ending", conn)
    if df.empty:
        log.warning("beef_weekly is empty; skipping quarterly recompute")
        return

    # Fix: use format='mixed' to handle both 'YYYY-MM-DD' and 'YYYY-MM-DDTHH:MM:SS'
    df["week_ending"] = pd.to_datetime(df["week_ending"], format="mixed").dt.date
    df["quarter"]       = df["week_ending"].apply(quarter_label)
    df["quarter_start"] = df["week_ending"].apply(lambda d: quarter_start(d).isoformat())

    agg = df.groupby(["quarter", "quarter_start"]).agg(
        choice       =("choice",       "mean"),
        select_      =("select_",      "mean"),
        ct150_steer  =("ct150_steer",  "mean"),
        ct150_heifer =("ct150_heifer", "mean"),
        ks_avg       =("ks_avg",       "mean"),
        ne_avg       =("ne_avg",       "mean"),
    ).reset_index()

    # ct150_all = average of steer and heifer (if both present)
    agg["ct150_all"] = agg[["ct150_steer", "ct150_heifer"]].mean(axis=1, skipna=True)

    # Preserve existing mbrf_gm / jbs_gm (manually entered)
    existing = pd.read_sql("SELECT quarter, mbrf_gm, jbs_gm FROM beef_quarterly", conn)
    agg = agg.merge(existing, on="quarter", how="left")

    sql = """
    INSERT INTO beef_quarterly
        (quarter, quarter_start, choice, select_, ct150_steer, ct150_heifer, ct150_all, ks_avg, ne_avg, mbrf_gm, jbs_gm)
    VALUES
        (:quarter,:quarter_start,:choice,:select_,:ct150_steer,:ct150_heifer,:ct150_all,:ks_avg,:ne_avg,:mbrf_gm,:jbs_gm)
    ON CONFLICT(quarter) DO UPDATE SET
        quarter_start = excluded.quarter_start,
        choice        = COALESCE(excluded.choice,        choice),
        select_       = COALESCE(excluded.select_,       select_),
        ct150_steer   = COALESCE(excluded.ct150_steer,   ct150_steer),
        ct150_heifer  = COALESCE(excluded.ct150_heifer,  ct150_heifer),
        ct150_all     = COALESCE(excluded.ct150_all,     ct150_all),
        ks_avg        = COALESCE(excluded.ks_avg,        ks_avg),
        ne_avg        = COALESCE(excluded.ne_avg,        ne_avg),
        mbrf_gm       = COALESCE(mbrf_gm, excluded.mbrf_gm),
        jbs_gm        = COALESCE(jbs_gm,  excluded.jbs_gm)
    """
    rows = agg.where(pd.notnull(agg), None).to_dict("records")
    conn.executemany(sql, rows)
    conn.commit()
    log.info("✓ beef_quarterly: %d quarters recomputed", len(rows))


# ══ HISTORICAL LOAD ═══════════════════════════════════════════════════════════

def load_history(conn: sqlite3.Connection, xlsx_path: str) -> None:
    """
    One-time load from the V4 Excel workbook.
    Expects a sheet with at least: week_ending, choice, select_, ct150_steer (or ct150),
    ks_avg (or ks), ne_avg (or ne).
    """
    log.info("Loading history from %s …", xlsx_path)
    xl = pd.ExcelFile(xlsx_path)

    # Try to find the weekly data sheet
    weekly_sheet = None
    for name in xl.sheet_names:
        if "week" in name.lower() or "raw" in name.lower() or "data" in name.lower():
            weekly_sheet = name
            break
    if weekly_sheet is None:
        weekly_sheet = xl.sheet_names[0]
        log.warning("No obvious weekly sheet found; using '%s'", weekly_sheet)

    df = xl.parse(weekly_sheet)
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]

    # Normalise column names
    rename = {
        "week_end": "week_ending", "week": "week_ending", "date": "week_ending",
        "choice_cutout": "choice",  "choice_$/cwt": "choice",
        "select_cutout": "select_", "select_$/cwt": "select_",
        "ct150": "ct150_steer",    "ct150_steer_avg": "ct150_steer",
        "ks":    "ks_avg",         "ks_price": "ks_avg",
        "ne":    "ne_avg",         "ne_price": "ne_avg",
    }
    df.rename(columns={k: v for k, v in rename.items() if k in df.columns}, inplace=True)

    required = ["week_ending"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Required columns not found in Excel: {missing}. Columns available: {list(df.columns)}")

    df["week_ending"] = pd.to_datetime(df["week_ending"], format="mixed").dt.date.astype(str)

    for col in ["choice", "select_", "ct150_steer", "ct150_heifer", "ks_avg", "ne_avg"]:
        if col not in df.columns:
            df[col] = None

    rows = df[["week_ending","choice","select_","ct150_steer","ct150_heifer","ks_avg","ne_avg"]]\
             .where(pd.notnull(df), None).to_dict("records")
    n = upsert_weekly(conn, rows)
    log.info("✓ beef_weekly: %d rows loaded from Excel", n)


# ══ WEEKLY UPDATE ═════════════════════════════════════════════════════════════

def update_weekly(conn: sqlite3.Connection) -> None:
    log.info("=== Fetching weekly data from USDA PDFs ===")

    ct150_data  = {"week_ending": None, "ct150_steer": None, "ct150_heifer": None}
    cutout_data = {"week_ending": None, "choice": None, "select_": None}
    ks_data     = {"week_ending": None, "ks_avg": None}
    ne_data     = {"week_ending": None, "ne_avg": None}

    try:
        ct150_data = fetch_ct150()
    except Exception as e:
        log.error("CT150 fetch failed: %s", e)

    try:
        cutout_data = fetch_cutout()
    except Exception as e:
        log.error("Cutout fetch failed: %s", e)

    try:
        ks_data = fetch_kansas()
    except Exception as e:
        log.error("Kansas fetch failed: %s", e)

    try:
        ne_data = fetch_nebraska()
    except Exception as e:
        log.error("Nebraska fetch failed: %s", e)

    rows = build_weekly_rows(ct150_data, cutout_data, ks_data, ne_data)
    n = upsert_weekly(conn, rows)
    log.info("✓ beef_weekly: %d row(s) upserted  (week_ending=%s)", n, rows[0]["week_ending"])


# ══ MAIN ══════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="U.S. Beef Packer Margin Extractor")
    parser.add_argument("--history", metavar="XLSX", help="One-time load from V4 Excel workbook")
    parser.add_argument("--full", action="store_true", help="Force full quarterly recompute")
    parser.add_argument("--db", default=DB_PATH, help="Path to beef.db (default: same folder as script)")
    args = parser.parse_args()

    if args.db != DB_PATH:
        DB_PATH = args.db

    log.info("Database: %s", DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    if args.history:
        load_history(conn, args.history)
    else:
        update_weekly(conn)

    log.info("=== Recomputing quarterly averages ===")
    recompute_quarterly(conn, full=args.full)

    conn.close()
    log.info("Done.")
