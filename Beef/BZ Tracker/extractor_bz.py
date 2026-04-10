#!/usr/bin/env python3
"""
extractor_bz.py — Brazil Beef Export Spread Tracker
=====================================================
Builds / refreshes  beef_bz.db  with monthly and weekly spread data.

DATA SOURCES
  • SECEX/MDIC monthly exports  → balanca.economia.gov.br annual CSVs
                                   NCM 0201 (fresh) + 0202 (frozen)
  • BCB PTAX BRL/USD daily FX   → BCB OLINDA API (primary) / BCB SGS (fallback)
  • CEPEA Boi Gordo R$/arroba   → local XLS file (seeding only)
                                   stored in DB, not re-fetched on weekly runs

USAGE
  pip install requests openpyxl
  python extractor_bz.py                          # incremental (SECEX + FX only)
  python extractor_bz.py --init                   # full seed (needs CEPEA XLS)
  python extractor_bz.py --init --cepea PATH.xls  # explicit CEPEA file path

OUTPUT
  beef_bz.db  (SQLite)

SCHEMA — table: monthly
  period       TEXT PRIMARY KEY   e.g. "2006-01"
  year         INTEGER
  month        INTEGER
  secex_usd_kg REAL               FOB USD / kg net weight
  fx           REAL               avg BCB PTAX BRL/USD for the month
  secex_brl_kg REAL               secex_usd_kg × fx
  cepea_r_kg   REAL               avg CEPEA R$/arroba ÷ 15 for the month
  spread       REAL               secex_brl_kg / cepea_r_kg
  updated_at   TEXT               ISO timestamp

SCHEMA — table: weekly
  start_date   TEXT PRIMARY KEY   ISO date "YYYY-MM-DD"
  end_date     TEXT               ISO date "YYYY-MM-DD"
  secex_usd_kg REAL               from SECEX weekly cumulative data
  fx           REAL               avg BCB PTAX BRL/USD for the period
  secex_brl_kg REAL               secex_usd_kg × fx
  cepea_r_kg   REAL               avg CEPEA R$/arroba ÷ 15 for the period
  spread       REAL               secex_brl_kg / cepea_r_kg
  updated_at   TEXT               ISO timestamp
"""

import sqlite3, os, sys, time, warnings
from datetime import datetime, date
from calendar import monthrange
from pathlib import Path

try:
    import requests
    # Suppress InsecureRequestWarning for Brazilian government domains
    # (balanca.economia.gov.br uses ICP-Brasil cert not trusted by default on Linux)
    from urllib3.exceptions import InsecureRequestWarning
    warnings.filterwarnings("ignore", category=InsecureRequestWarning)
except ImportError:
    sys.exit("Missing: pip install requests")

# Domains that require SSL verification disabled (ICP-Brasil / SERPRO chain)
_NO_VERIFY_HOSTS = ("balanca.economia.gov.br", "olinda.bcb.gov.br", "api.bcb.gov.br")

# ── Paths ──────────────────────────────────────────────────────────────────────
DB_PATH    = Path(__file__).parent / "beef_bz.db"
TIMEOUT    = 30
RETRY      = 3
NCM_CODES  = {"0201", "0202"}
ANO_INI    = 2006   # earliest SECEX year to seed

# ── Weekly historical data (Aug 2022 – Mar 2026, 176 weeks) ───────────────────
# Format: (start_date, end_date, price_usd_kg)
# price_usd_kg = Revenue_000USD * 1000 / Volume_tons / 1000 = Revenue / Volume_kg
WEEKLY_SEED = [
    # ── 2022 ──────────────────────────────────────────────────────────────────
    ("2022-08-01","2022-08-05",6.25), ("2022-08-08","2022-08-12",6.31),
    ("2022-08-15","2022-08-19",6.19), ("2022-08-22","2022-08-31",5.92),
    ("2022-09-01","2022-09-09",6.06), ("2022-09-12","2022-09-16",5.90),
    ("2022-09-19","2022-09-23",5.96), ("2022-09-26","2022-09-30",6.07),
    ("2022-10-03","2022-10-07",5.96), ("2022-10-10","2022-10-15",5.88),
    ("2022-10-17","2022-10-21",5.84), ("2022-10-24","2022-10-31",5.71),
    ("2022-11-01","2022-11-14",5.35), ("2022-11-14","2022-11-18",5.16),
    ("2022-11-21","2022-11-30",5.10),
    ("2022-12-01","2022-12-09",5.03), ("2022-12-12","2022-12-16",4.92),
    ("2022-12-19","2022-12-23",4.89), ("2022-12-26","2022-12-30",4.93),
    # ── 2023 ──────────────────────────────────────────────────────────────────
    ("2023-01-02","2023-01-06",4.88), ("2023-01-09","2023-01-13",4.85),
    ("2023-01-16","2023-01-20",4.83), ("2023-01-23","2023-01-27",4.81),
    ("2023-02-01","2023-02-10",4.82), ("2023-02-13","2023-02-17",4.89),
    ("2023-02-20","2023-02-28",4.85),
    ("2023-03-01","2023-03-10",4.87), ("2023-03-13","2023-03-17",4.85),
    ("2023-03-20","2023-03-31",4.63),
    ("2023-04-03","2023-04-07",4.55), ("2023-04-10","2023-04-14",4.72),
    ("2023-04-17","2023-04-21",4.83), ("2023-04-24","2023-04-28",4.94),
    ("2023-05-01","2023-05-05",5.06), ("2023-05-08","2023-05-12",5.07),
    ("2023-05-15","2023-05-19",5.12), ("2023-05-22","2023-05-31",5.13),
    ("2023-06-01","2023-06-09",5.14), ("2023-06-12","2023-06-16",5.06),
    ("2023-06-19","2023-06-23",4.99), ("2023-06-26","2023-06-30",4.95),
    ("2023-07-03","2023-07-07",4.86), ("2023-07-10","2023-07-14",4.80),
    ("2023-07-17","2023-07-21",4.61), ("2023-07-24","2023-07-31",4.70),
    ("2023-08-01","2023-08-04",4.53), ("2023-08-07","2023-08-11",4.48),
    ("2023-08-14","2023-08-18",4.53), ("2023-08-21","2023-08-31",4.51),
    ("2023-09-01","2023-09-08",4.49), ("2023-09-11","2023-09-15",4.53),
    ("2023-09-18","2023-09-22",4.60), ("2023-09-25","2023-09-29",4.58),
    ("2023-10-02","2023-10-06",4.59), ("2023-10-09","2023-10-13",4.62),
    ("2023-10-16","2023-10-20",4.55), ("2023-10-23","2023-10-31",4.61),
    ("2023-11-01","2023-11-10",4.60), ("2023-11-13","2023-11-17",4.56),
    ("2023-11-20","2023-11-30",4.61),
    ("2023-12-01","2023-12-08",4.59), ("2023-12-11","2023-12-15",4.54),
    ("2023-12-18","2023-12-22",4.53), ("2023-12-25","2023-12-29",4.52),
    # ── 2024 ──────────────────────────────────────────────────────────────────
    ("2024-01-01","2024-01-05",4.52), ("2024-01-08","2024-01-12",4.54),
    ("2024-01-15","2024-01-19",4.47), ("2024-01-22","2024-01-26",4.51),
    ("2024-01-29","2024-01-31",4.70),
    ("2024-02-01","2024-02-09",4.58), ("2024-02-12","2024-02-23",4.52),
    ("2024-02-26","2024-02-29",4.47),
    ("2024-03-01","2024-03-08",4.50), ("2024-03-11","2024-03-15",4.51),
    ("2024-03-18","2024-03-22",4.56), ("2024-03-25","2024-03-29",4.54),
    ("2024-04-01","2024-04-05",4.48), ("2024-04-08","2024-04-12",4.55),
    ("2024-04-15","2024-04-19",4.56), ("2024-04-22","2024-04-26",4.53),
    ("2024-04-29","2024-04-30",4.62),
    ("2024-05-01","2024-05-10",4.49), ("2024-05-13","2024-05-31",4.51),
    ("2024-06-03","2024-06-07",4.46), ("2024-06-10","2024-06-14",4.45),
    ("2024-06-17","2024-06-21",4.50), ("2024-06-24","2024-06-28",4.46),
    ("2024-07-01","2024-07-05",4.44), ("2024-07-08","2024-07-12",4.39),
    ("2024-07-15","2024-07-19",4.41), ("2024-07-22","2024-07-26",4.42),
    ("2024-07-29","2024-07-31",4.33),
    ("2024-08-01","2024-08-09",4.42), ("2024-08-12","2024-08-16",4.43),
    ("2024-08-19","2024-08-23",4.51), ("2024-08-26","2024-08-30",4.38),
    ("2024-09-02","2024-09-06",4.41), ("2024-09-09","2024-09-13",4.49),
    ("2024-09-16","2024-09-20",4.57), ("2024-09-23","2024-09-27",4.61),
    ("2024-10-01","2024-10-04",4.60), ("2024-10-07","2024-10-11",4.60),
    ("2024-10-14","2024-10-18",4.61), ("2024-10-21","2024-10-25",4.74),
    ("2024-10-28","2024-10-31",4.81),
    ("2024-11-04","2024-11-08",4.82), ("2024-11-11","2024-11-15",4.84),
    ("2024-11-18","2024-11-22",4.98), ("2024-11-25","2024-11-29",4.87),
    ("2024-12-02","2024-12-06",4.94), ("2024-12-09","2024-12-13",4.88),
    ("2024-12-16","2024-12-20",5.06), ("2024-12-23","2024-12-31",4.95),
    # ── 2025 ──────────────────────────────────────────────────────────────────
    ("2025-01-02","2025-01-10",5.06), ("2025-01-13","2025-01-17",5.01),
    ("2025-01-20","2025-01-24",5.03), ("2025-01-27","2025-01-31",4.99),
    ("2025-02-03","2025-02-07",4.96), ("2025-02-10","2025-02-14",4.94),
    ("2025-02-17","2025-02-21",4.90), ("2025-02-24","2025-02-28",4.90),
    ("2025-03-03","2025-03-07",4.89), ("2025-03-10","2025-03-14",4.86),
    ("2025-03-17","2025-03-21",4.91), ("2025-03-24","2025-03-31",4.95),
    ("2025-04-01","2025-04-04",4.95), ("2025-04-07","2025-04-11",4.97),
    ("2025-04-14","2025-04-17",5.04), ("2025-04-21","2025-04-25",5.10),
    ("2025-04-28","2025-04-30",5.09),
    ("2025-05-02","2025-05-09",5.10), ("2025-05-12","2025-05-16",5.13),
    ("2025-05-19","2025-05-23",5.33), ("2025-05-26","2025-05-30",5.29),
    ("2025-06-02","2025-06-06",5.37), ("2025-06-09","2025-06-13",5.46),
    ("2025-06-16","2025-06-20",5.48), ("2025-06-23","2025-06-30",5.49),
    ("2025-07-01","2025-07-04",5.54), ("2025-07-07","2025-07-11",5.53),
    ("2025-07-14","2025-07-18",5.57), ("2025-07-21","2025-07-25",5.54),
    ("2025-07-28","2025-07-31",5.59),
    ("2025-08-01","2025-08-08",5.56), ("2025-08-11","2025-08-15",5.73),
    ("2025-08-18","2025-08-22",5.55), ("2025-08-25","2025-08-29",5.59),
    ("2025-09-01","2025-09-05",5.56), ("2025-09-08","2025-09-12",5.70),
    ("2025-09-15","2025-09-19",5.64), ("2025-09-22","2025-09-26",5.58),
    ("2025-09-29","2025-09-30",5.68),
    ("2025-10-01","2025-10-10",5.55), ("2025-10-13","2025-10-17",5.45),
    ("2025-10-20","2025-10-24",5.58), ("2025-10-27","2025-10-31",5.62),
    ("2025-11-03","2025-11-07",5.51), ("2025-11-10","2025-11-14",5.56),
    ("2025-11-17","2025-11-21",5.41), ("2025-11-24","2025-11-28",5.56),
    ("2025-12-01","2025-12-05",5.62), ("2025-12-08","2025-12-12",5.59),
    ("2025-12-15","2025-12-19",5.56), ("2025-12-22","2025-12-31",5.65),
    # ── 2026 ──────────────────────────────────────────────────────────────────
    ("2026-01-05","2026-01-09",5.53), ("2026-01-12","2026-01-16",5.58),
    ("2026-01-19","2026-01-23",5.65), ("2026-01-26","2026-01-30",5.56),
    ("2026-02-02","2026-02-06",5.62), ("2026-02-09","2026-02-13",5.57),
    ("2026-02-16","2026-02-20",5.66), ("2026-02-23","2026-02-27",5.76),
    ("2026-03-02","2026-03-06",5.69), ("2026-03-09","2026-03-13",5.85),
    ("2026-03-16","2026-03-20",5.82), ("2026-03-23","2026-03-31",5.89),
]


# ══════════════════════════════════════════════════════════════════════════════
# DATABASE
# ══════════════════════════════════════════════════════════════════════════════
def init_db(conn):
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS _fx_raw (
        dt   TEXT PRIMARY KEY,
        fx   REAL
    );
    CREATE TABLE IF NOT EXISTS _cepea_raw (
        dt        TEXT PRIMARY KEY,
        r_arroba  REAL,
        r_kg      REAL
    );
    CREATE TABLE IF NOT EXISTS _secex_raw (
        year         INTEGER,
        month        INTEGER,
        rev_000usd   REAL,
        vol_tons     REAL,
        price_usd_kg REAL,
        PRIMARY KEY (year, month)
    );
    CREATE TABLE IF NOT EXISTS _weekly_raw (
        start_date   TEXT PRIMARY KEY,
        end_date     TEXT,
        price_usd_kg REAL
    );
    CREATE TABLE IF NOT EXISTS monthly (
        period       TEXT PRIMARY KEY,
        year         INTEGER,
        month        INTEGER,
        secex_usd_kg REAL,
        fx           REAL,
        secex_brl_kg REAL,
        cepea_r_kg   REAL,
        spread       REAL,
        updated_at   TEXT
    );
    CREATE TABLE IF NOT EXISTS weekly (
        start_date   TEXT PRIMARY KEY,
        end_date     TEXT,
        secex_usd_kg REAL,
        fx           REAL,
        secex_brl_kg REAL,
        cepea_r_kg   REAL,
        spread       REAL,
        updated_at   TEXT
    );
    """)
    conn.commit()


# ══════════════════════════════════════════════════════════════════════════════
# HTTP HELPER
# ══════════════════════════════════════════════════════════════════════════════
def get(url, **kwargs):
    hdrs = {"Accept": "application/json", "User-Agent": "Mozilla/5.0"}
    # Brazilian government servers use ICP-Brasil certs not trusted on Linux by default
    ssl_verify = not any(h in url for h in _NO_VERIFY_HOSTS)
    for attempt in range(RETRY):
        try:
            r = requests.get(url, headers=hdrs, timeout=TIMEOUT,
                             verify=ssl_verify, **kwargs)
            r.raise_for_status()
            return r
        except Exception as e:
            if attempt == RETRY - 1:
                print(f"  ✗ {url[:70]}…: {e}")
                return None
            time.sleep(2 ** attempt)
    return None


# ══════════════════════════════════════════════════════════════════════════════
# BCB PTAX FX
# ══════════════════════════════════════════════════════════════════════════════
def fetch_fx(conn):
    """Download BCB PTAX BRL/USD daily rates into _fx_raw."""
    start = datetime(ANO_INI, 1, 1).strftime("%m-%d-%Y")
    end   = datetime.now().strftime("%m-%d-%Y")
    rows  = []

    # Method A: BCB OLINDA
    url = (
        "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/"
        f"CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)"
        f"?@dataInicial='{start}'&@dataFinalCotacao='{end}'"
        "&$format=json&$select=cotacaoCompra,dataHoraCotacao"
    )
    r = get(url)
    if r:
        for item in r.json().get("value", []):
            try:
                rows.append((item["dataHoraCotacao"][:10], float(item["cotacaoCompra"])))
            except Exception:
                pass
        print(f"  [FX] BCB OLINDA: {len(rows)} rows")

    # Method B: BCB SGS series 1
    if not rows:
        url2 = (
            f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.1/dados"
            f"?formato=json&dataInicial=01/01/{ANO_INI}&dataFinal="
            + datetime.now().strftime("%d/%m/%Y")
        )
        r2 = get(url2)
        if r2:
            for item in r2.json():
                try:
                    dt = datetime.strptime(item["data"], "%d/%m/%Y").strftime("%Y-%m-%d")
                    rows.append((dt, float(item["valor"])))
                except Exception:
                    pass
            print(f"  [FX] BCB SGS: {len(rows)} rows")

    if not rows:
        print("  [FX] All methods failed — FX not updated.")
        return 0

    conn.executemany("INSERT OR REPLACE INTO _fx_raw(dt,fx) VALUES(?,?)", rows)
    conn.commit()
    print(f"  [FX] {len(rows)} rows stored.")
    return len(rows)


# ══════════════════════════════════════════════════════════════════════════════
# SECEX MONTHLY
# ══════════════════════════════════════════════════════════════════════════════
def fetch_secex(conn, years=None):
    """Download MDIC annual CSVs and upsert into _secex_raw."""
    from io import StringIO
    try:
        import pandas as pd
    except ImportError:
        sys.exit("Missing: pip install pandas openpyxl")

    if years is None:
        years = range(ANO_INI, datetime.now().year + 1)

    # The annual CSV uses 8-digit NCM codes (e.g. "02011000").
    # NCM_CODES contains 4-digit chapter codes ("0201", "0202"), so we
    # match by prefix — str[:4] — rather than exact equality.
    BASE = "https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/EXP_{year}.csv"
    total = 0
    for yr in years:
        r = get(BASE.format(year=yr))
        if not r:
            continue
        try:
            df = pd.read_csv(StringIO(r.text), sep=";", dtype=str, low_memory=False)
            # Normalise: strip whitespace and zero-pad to 8 chars
            df["CO_NCM"] = df["CO_NCM"].str.strip().str.zfill(8)
            # Keep rows whose 4-digit chapter prefix matches NCM_CODES
            df = df[df["CO_NCM"].str[:4].isin(NCM_CODES)].copy()
            if df.empty:
                print(f"  [SECEX] {yr}: no beef rows")
                continue
            df["CO_MES"]     = df["CO_MES"].astype(int)
            df["KG_LIQUIDO"] = df["KG_LIQUIDO"].astype(float)
            df["VL_FOB"]     = df["VL_FOB"].astype(float)
            grp = df.groupby("CO_MES").agg(
                vol_kg=("KG_LIQUIDO","sum"), rev_usd=("VL_FOB","sum")
            ).reset_index()
            rows = []
            for _, row in grp.iterrows():
                m    = int(row["CO_MES"])
                vol  = float(row["vol_kg"]) / 1000      # tons
                rev  = float(row["rev_usd"]) / 1000     # 000 USD
                p    = (rev * 1000 / (vol * 1000)) if vol > 0 else None  # USD/kg
                rows.append((yr, m, rev, vol, p))
            conn.executemany(
                "INSERT OR REPLACE INTO _secex_raw(year,month,rev_000usd,vol_tons,price_usd_kg)"
                " VALUES(?,?,?,?,?)", rows
            )
            conn.commit()
            total += len(rows)
            print(f"  [SECEX] {yr}: {len(rows)} months")
        except Exception as ex:
            print(f"  [SECEX] {yr}: {ex}")
    return total


# ══════════════════════════════════════════════════════════════════════════════
# CEPEA (local XLS — seeding only)
# ══════════════════════════════════════════════════════════════════════════════
def load_cepea_xls(conn, path):
    """Load CEPEA Boi Gordo R$/arroba from local XLS/XLSX file."""
    import subprocess, tempfile
    try:
        import pandas as pd
    except ImportError:
        sys.exit("Missing: pip install pandas openpyxl")

    p = Path(path)
    if not p.exists():
        print(f"  [CEPEA] Not found: {path}")
        return 0

    df = None
    # Try direct read (XLSX)
    try:
        df = pd.read_excel(p, header=None, engine="openpyxl")
    except Exception:
        pass

    # LibreOffice fallback (OLE2 .xls)
    if df is None:
        try:
            tmp = tempfile.mkdtemp()
            out = Path(tmp) / (p.stem + "_conv.xlsx")
            subprocess.run(
                ["libreoffice", "--headless", "--convert-to", "xlsx", str(p), "--outdir", tmp],
                capture_output=True, timeout=90
            )
            if out.exists():
                df = pd.read_excel(out, header=None, engine="openpyxl")
                print("  [CEPEA] LibreOffice conversion OK")
        except Exception as ex:
            print(f"  [CEPEA] LibreOffice failed: {ex}")

    if df is None:
        print("  [CEPEA] Could not read file.")
        return 0

    rows = []
    for _, row in df.iterrows():
        for c in range(len(row) - 1):
            cell = row.iloc[c]
            dt   = None
            if isinstance(cell, (datetime, date)):
                dt = cell.date() if isinstance(cell, datetime) else cell
            elif isinstance(cell, str):
                for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%m/%d/%Y"):
                    try:
                        dt = datetime.strptime(cell.strip(), fmt).date()
                        break
                    except Exception:
                        pass
            if dt is None or dt.year < 2000:
                continue
            try:
                val = float(str(row.iloc[c + 1]).replace(",", "."))
                if 50 < val < 2000:
                    rows.append((str(dt), val, round(val / 15.0, 6)))
            except Exception:
                pass

    if not rows:
        print("  [CEPEA] No valid rows parsed.")
        return 0

    conn.executemany(
        "INSERT OR REPLACE INTO _cepea_raw(dt,r_arroba,r_kg) VALUES(?,?,?)", rows
    )
    conn.commit()
    print(f"  [CEPEA] {len(rows)} rows loaded from {p.name}")
    return len(rows)


# ══════════════════════════════════════════════════════════════════════════════
# WEEKLY RAW SEED
# ══════════════════════════════════════════════════════════════════════════════
def seed_weekly_raw(conn):
    conn.executemany(
        "INSERT OR REPLACE INTO _weekly_raw(start_date,end_date,price_usd_kg)"
        " VALUES(?,?,?)",
        WEEKLY_SEED
    )
    conn.commit()
    print(f"  [WEEKLY] {len(WEEKLY_SEED)} rows seeded into _weekly_raw.")


# ══════════════════════════════════════════════════════════════════════════════
# FILL MISSING SECEX MONTHS FROM WEEKLY AVERAGES
# ══════════════════════════════════════════════════════════════════════════════
def fill_secex_from_weekly(conn):
    """
    For months after the last official SECEX entry, estimate price_usd_kg as the
    simple average of weekly prices that fall within that month.

    Uses INSERT OR IGNORE so that when real MDIC data arrives via fetch_secex()
    (which uses INSERT OR REPLACE), the official values automatically overwrite
    these estimates.

    Returns the number of newly inserted estimated rows.
    """
    last = conn.execute(
        "SELECT year, month FROM _secex_raw ORDER BY year DESC, month DESC LIMIT 1"
    ).fetchone()
    if not last:
        return 0

    ly, lm = last
    last_date = f"{ly}-{lm:02d}-{monthrange(ly, lm)[1]:02d}"

    weekly = conn.execute(
        """
        SELECT CAST(strftime('%Y', start_date) AS INTEGER),
               CAST(strftime('%m', start_date) AS INTEGER),
               AVG(price_usd_kg)
        FROM   _weekly_raw
        WHERE  start_date > ? AND price_usd_kg IS NOT NULL
        GROUP  BY 1, 2
        ORDER  BY 1, 2
        """,
        (last_date,),
    ).fetchall()

    filled = 0
    for yr, mo, avg_p in weekly:
        if avg_p is None:
            continue
        conn.execute(
            "INSERT OR IGNORE INTO _secex_raw(year,month,rev_000usd,vol_tons,price_usd_kg)"
            " VALUES(?,?,NULL,NULL,?)",
            (yr, mo, round(avg_p, 6)),
        )
        if conn.execute("SELECT changes()").fetchone()[0]:
            print(f"  [SECEX-est] {yr}-{mo:02d}: {avg_p:.4f} USD/kg  ← weekly avg (official MDIC pending)")
            filled += 1

    conn.commit()
    return filled


# ══════════════════════════════════════════════════════════════════════════════
# COMPUTE & MATERIALISE SPREAD TABLES
# ══════════════════════════════════════════════════════════════════════════════
def _avg(conn, table, col, s, e):
    r = conn.execute(
        f"SELECT AVG({col}) FROM {table} WHERE dt >= ? AND dt <= ?", (s, e)
    ).fetchone()
    return r[0] if r and r[0] is not None else None


def materialise(conn):
    """Compute monthly and weekly spread tables from raw data."""
    now_iso = datetime.now().isoformat(timespec="seconds")

    # ── Monthly ───────────────────────────────────────────────────────────────
    raw_m = conn.execute(
        "SELECT year, month, price_usd_kg FROM _secex_raw ORDER BY year, month"
    ).fetchall()
    monthly_rows = []
    for yr, mo, p_usd in raw_m:
        s  = f"{yr}-{mo:02d}-01"
        ld = monthrange(yr, mo)[1]
        e  = f"{yr}-{mo:02d}-{ld:02d}"
        fx = _avg(conn, "_fx_raw", "fx", s, e)
        ca = _avg(conn, "_cepea_raw", "r_kg", s, e)
        brl = (p_usd * fx) if p_usd and fx else None
        sp  = (brl / ca)  if brl and ca  else None
        monthly_rows.append((
            f"{yr}-{mo:02d}", yr, mo,
            round(p_usd, 6) if p_usd else None,
            round(fx,   6) if fx else None,
            round(brl,  6) if brl else None,
            round(ca,   6) if ca else None,
            round(sp,   6) if sp else None,
            now_iso,
        ))
    conn.executemany(
        "INSERT OR REPLACE INTO monthly"
        "(period,year,month,secex_usd_kg,fx,secex_brl_kg,cepea_r_kg,spread,updated_at)"
        " VALUES(?,?,?,?,?,?,?,?,?)",
        monthly_rows
    )

    # ── Weekly ────────────────────────────────────────────────────────────────
    raw_w = conn.execute(
        "SELECT start_date, end_date, price_usd_kg FROM _weekly_raw ORDER BY start_date"
    ).fetchall()
    weekly_rows = []
    for s, e, p_usd in raw_w:
        fx = _avg(conn, "_fx_raw", "fx", s, e)
        ca = _avg(conn, "_cepea_raw", "r_kg", s, e)
        brl = (p_usd * fx) if p_usd and fx else None
        sp  = (brl / ca)  if brl and ca  else None
        weekly_rows.append((
            s, e,
            round(p_usd, 6) if p_usd else None,
            round(fx,   6) if fx else None,
            round(brl,  6) if brl else None,
            round(ca,   6) if ca else None,
            round(sp,   6) if sp else None,
            now_iso,
        ))
    conn.executemany(
        "INSERT OR REPLACE INTO weekly"
        "(start_date,end_date,secex_usd_kg,fx,secex_brl_kg,cepea_r_kg,spread,updated_at)"
        " VALUES(?,?,?,?,?,?,?,?)",
        weekly_rows
    )
    conn.commit()

    nm = conn.execute("SELECT COUNT(*) FROM monthly WHERE spread IS NOT NULL").fetchone()[0]
    nw = conn.execute("SELECT COUNT(*) FROM weekly  WHERE spread IS NOT NULL").fetchone()[0]
    print(f"  [DB] monthly: {len(monthly_rows)} rows ({nm} with spread)")
    print(f"  [DB] weekly:  {len(weekly_rows)} rows ({nw} with spread)")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
def main():
    args = sys.argv[1:]
    do_init    = "--init" in args
    cepea_idx  = next((i for i, a in enumerate(args) if a == "--cepea"), None)
    cepea_path = args[cepea_idx + 1] if cepea_idx is not None and cepea_idx + 1 < len(args) else None

    print("=" * 60)
    print(f"  Brazil Beef Spread Extractor — {date.today().isoformat()}")
    print(f"  Mode: {'INIT (full seed)' if do_init else 'INCREMENTAL UPDATE'}")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    # Weekly raw always seeded (INSERT OR REPLACE → idempotent)
    print("\n[1] Seeding weekly raw data …")
    seed_weekly_raw(conn)

    if do_init:
        # Full SECEX history
        print("\n[2] Fetching SECEX monthly (all years) …")
        fetch_secex(conn)

        # CEPEA from local XLS
        print("\n[3] Loading CEPEA from local XLS …")
        if cepea_path:
            load_cepea_xls(conn, cepea_path)
        else:
            candidates = (
                list(Path(DB_PATH.parent).glob("CEPEA*.xls*")) +
                list(Path(DB_PATH.parent).glob("cepea*.xls*")) +
                list(Path(DB_PATH.parent.parent).glob("CEPEA*.xls*"))
            )
            if candidates:
                load_cepea_xls(conn, candidates[0])
            else:
                print("  [CEPEA] No XLS file found.")
                print("  Run with:  python extractor_bz.py --init --cepea /path/to/CEPEA.xls")
                print("  Download:  https://www.cepea.esalq.usp.br/br/indicador/boi-gordo.aspx")

        print("\n[4] Fetching BCB PTAX FX …")
        fetch_fx(conn)

    else:
        # Incremental: current + prior year SECEX only
        print("\n[2] Fetching SECEX monthly (recent years) …")
        yr = datetime.now().year
        fetch_secex(conn, years=[yr - 1, yr])

        print("\n[3] Fetching BCB PTAX FX …")
        fetch_fx(conn)

    print("\n[→] Computing spread tables …")
    materialise(conn)

    conn.close()
    print("\n" + "=" * 60)
    print("  Done. beef_bz.db updated.")
    print("=" * 60)


if __name__ == "__main__":
    main()
