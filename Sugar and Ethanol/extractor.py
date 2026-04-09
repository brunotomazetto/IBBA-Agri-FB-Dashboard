#!/usr/bin/env python3
"""
extractor.py — IBBA Agri Monitor · Unified Daily Extractor
===========================================================
Runs daily via GitHub Actions. Each section has its own schedule logic:

  S&E (Sugar NY11, Ethanol UDOP, FX PTAX) → every weekday
  Fuel Parity (ANP weekly prices)           → Thursdays only
  Supply/Demand (ANP monthly volumes)       → 5th of each month only

If it's not the right day for a section, it skips silently (no error).
If it IS the right day and the fetch fails, it raises so GitHub marks the run red.

Sources:
  NY11   → Yahoo Finance (SB=F)
  Etanol → UDOP (udop.com.br) via undetected-chromedriver + Xvfb
  FX     → BCB PTAX API (olinda.bcb.gov.br)
  Fuel   → ANP Série Histórica de Preços (semanal, xlsx)
  Vendas → ANP dados abertos (vendas-etanol-hidratado-m3-{Y}.csv, vendas-gasolina-c-m3-{Y}.csv)
  Produção → ANP dados abertos (producao-etanol-hidratado-m3.csv)
"""

import io
import logging
import sqlite3
import subprocess
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import requests

# ── Chrome / Selenium (only imported when needed) ──────────────────────────
try:
    import undetected_chromedriver as uc
    from selenium.webdriver.common.by import By
    HAS_CHROME = True
except ImportError:
    HAS_CHROME = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

DB_PATH       = Path(__file__).parent / "commodities.db"
HISTORY_START = "2010-01-01"
TODAY         = date.today()
NOW_STR       = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# ─────────────────────────────────────────────────────────────────────────────
# Schedule helpers — silent skip if not the right day
# ─────────────────────────────────────────────────────────────────────────────

def is_weekday()  -> bool: return TODAY.weekday() < 5           # Mon–Fri
def is_thursday() -> bool: return TODAY.weekday() == 3          # Thu
def is_month_5th()-> bool: return TODAY.day == 5                # 5th of month


# ─────────────────────────────────────────────────────────────────────────────
# DB helpers
# ─────────────────────────────────────────────────────────────────────────────

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS sugar_ny11 (
        id INTEGER PRIMARY KEY AUTOINCREMENT, data_referencia TEXT NOT NULL,
        ano INTEGER, mes INTEGER, preco_usdclb REAL NOT NULL,
        open_usdclb REAL, high_usdclb REAL, low_usdclb REAL, volume REAL,
        fonte TEXT DEFAULT 'Yahoo/SB=F', updated_at TEXT, UNIQUE(data_referencia));
    CREATE INDEX IF NOT EXISTS idx_sugar ON sugar_ny11(data_referencia);

    CREATE TABLE IF NOT EXISTS etanol_cepea (
        id INTEGER PRIMARY KEY AUTOINCREMENT, data_referencia TEXT NOT NULL,
        ano INTEGER, mes INTEGER, preco_brl_m3 REAL NOT NULL,
        fonte TEXT DEFAULT 'UDOP/CEPEA-Paulinia', updated_at TEXT,
        UNIQUE(data_referencia));
    CREATE INDEX IF NOT EXISTS idx_etanol ON etanol_cepea(data_referencia);

    CREATE TABLE IF NOT EXISTS fx_usdbrl (
        id INTEGER PRIMARY KEY AUTOINCREMENT, data_referencia TEXT NOT NULL,
        ano INTEGER, mes INTEGER, ptax_venda REAL NOT NULL,
        fonte TEXT DEFAULT 'BCB/PTAX', updated_at TEXT,
        UNIQUE(data_referencia));
    CREATE INDEX IF NOT EXISTS idx_fx ON fx_usdbrl(data_referencia);

    CREATE TABLE IF NOT EXISTS anp_estados (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        data_inicial TEXT NOT NULL, data_final TEXT NOT NULL,
        regiao TEXT, estado TEXT NOT NULL, produto TEXT NOT NULL,
        preco_medio_revenda REAL, updated_at TEXT,
        UNIQUE(data_inicial, estado, produto));
    CREATE INDEX IF NOT EXISTS idx_anp_est ON anp_estados(data_inicial, estado, produto);

    CREATE TABLE IF NOT EXISTS anp_brasil (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        data_inicial TEXT NOT NULL, data_final TEXT NOT NULL,
        produto TEXT NOT NULL, preco_medio_revenda REAL, updated_at TEXT,
        UNIQUE(data_inicial, produto));
    CREATE INDEX IF NOT EXISTS idx_anp_br ON anp_brasil(data_inicial, produto);

    CREATE TABLE IF NOT EXISTS anp_vendas_uf (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ano INTEGER NOT NULL, mes INTEGER NOT NULL, estado TEXT NOT NULL,
        eth_hid_m3 REAL, gas_c_m3 REAL, updated_at TEXT,
        UNIQUE(ano, mes, estado));
    CREATE INDEX IF NOT EXISTS idx_vendas ON anp_vendas_uf(ano, mes, estado);

    CREATE TABLE IF NOT EXISTS anp_producao_uf (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ano INTEGER NOT NULL, mes INTEGER NOT NULL, estado TEXT NOT NULL,
        eth_hid_m3 REAL, eth_ani_m3 REAL, updated_at TEXT,
        UNIQUE(ano, mes, estado));
    CREATE INDEX IF NOT EXISTS idx_prod ON anp_producao_uf(ano, mes, estado);
    """)
    conn.commit()


def last_date(conn, table, col="data_referencia"):
    r = conn.execute(f"SELECT MAX({col}) FROM {table}").fetchone()
    return r[0] if r and r[0] else None


def last_year_month(conn, table):
    r = conn.execute(
        f"SELECT MAX(ano), MAX(mes) FROM {table} "
        f"WHERE ano=(SELECT MAX(ano) FROM {table})"
    ).fetchone()
    return (int(r[0]), int(r[1])) if r and r[0] else None


def safe_float(val):
    try:
        f = float(val)
        return None if str(f) == "nan" else f
    except:
        return None


def parse_date(raw):
    for fmt in ("%d/%m/%Y", "%d/%m/%y", "%Y-%m-%d"):
        try:
            return datetime.strptime(str(raw).strip(), fmt).strftime("%Y-%m-%d")
        except:
            continue
    return None


# ─────────────────────────────────────────────────────────────────────────────
# HTTP helpers
# ─────────────────────────────────────────────────────────────────────────────

ANP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/csv,application/vnd.ms-excel,*/*",
    "Referer": "https://www.gov.br/anp/pt-br/",
}

def download(url: str, label: str, fatal: bool = True) -> bytes | None:
    for attempt in range(1, 4):
        try:
            log.info(f"[{label}] Downloading (attempt {attempt}): {url}")
            r = requests.get(url, headers=ANP_HEADERS, timeout=60)
            r.raise_for_status()
            log.info(f"[{label}] {len(r.content):,} bytes")
            return r.content
        except requests.RequestException as e:
            log.warning(f"[{label}] Attempt {attempt} failed: {e}")
            if attempt < 3:
                time.sleep(10 * attempt)
    msg = f"[{label}] All download attempts failed."
    if fatal:
        raise RuntimeError(msg)   # marks GitHub run red
    log.error(msg)
    return None


# ─────────────────────────────────────────────────────────────────────────────
# ══ SECTION 1: S&E  (runs every weekday) ════════════════════════════════════
# ─────────────────────────────────────────────────────────────────────────────

def run_se(conn: sqlite3.Connection) -> dict:
    if not is_weekday():
        log.info("[S&E] Not a weekday — skipping.")
        return {"skipped": True}

    log.info("=" * 60)
    log.info("S&E — Sugar NY11 · Ethanol UDOP · FX PTAX")
    log.info("=" * 60)

    results = {}
    results["ny11"] = fetch_sugar_ny11(conn)
    results["fx"]   = fetch_fx_usdbrl(conn)
    results["eth"]  = fetch_etanol_cepea(conn)   # Chrome — last, heaviest
    return results


# ── NY11 ──────────────────────────────────────────────────────────────────────

def fetch_sugar_ny11(conn) -> int:
    try:
        import yfinance as yf
    except ImportError:
        raise RuntimeError("[NY11] yfinance not installed")

    log.info("[NY11] Fetching Yahoo Finance (SB=F)...")
    ld = last_date(conn, "sugar_ny11")
    start = (datetime.strptime(ld, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d") \
            if ld else HISTORY_START
    if start > TODAY.strftime("%Y-%m-%d"):
        log.info("[NY11] Already up to date.")
        return 0

    df = yf.Ticker("SB=F").history(start=start, end=TODAY.strftime("%Y-%m-%d"),
                                   auto_adjust=False)
    if df is None or df.empty:
        log.info("[NY11] No new data.")
        return 0

    df.index = pd.to_datetime(df.index).tz_localize(None)
    inserted = 0
    for ts, row in df.iterrows():
        dr = ts.strftime("%Y-%m-%d")
        cl = safe_float(row.get("Close"))
        if not cl:
            continue
        conn.execute(
            "INSERT OR IGNORE INTO sugar_ny11 "
            "(data_referencia,ano,mes,preco_usdclb,open_usdclb,high_usdclb,low_usdclb,volume,updated_at) "
            "VALUES(?,?,?,?,?,?,?,?,?)",
            (dr, int(dr[:4]), int(dr[5:7]), cl,
             safe_float(row.get("Open")), safe_float(row.get("High")),
             safe_float(row.get("Low")),  safe_float(row.get("Volume")), NOW_STR))
        if conn.execute("SELECT changes()").fetchone()[0]:
            inserted += 1
    conn.commit()
    log.info(f"[NY11] {inserted} rows inserted.")
    return inserted


# ── FX PTAX ───────────────────────────────────────────────────────────────────

BCB_URL = (
    "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/"
    "CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)"
    "?@dataInicial='{di}'&@dataFinalCotacao='{df}'"
    "&$top=1000&$skip={skip}&$orderby=dataHoraCotacao%20asc"
    "&$format=json&$select=cotacaoVenda,dataHoraCotacao"
)

def fetch_fx_usdbrl(conn) -> int:
    log.info("[FX] Fetching BCB PTAX...")
    ld = last_date(conn, "fx_usdbrl")
    start = (datetime.strptime(ld, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d") \
            if ld else HISTORY_START
    if start > TODAY.strftime("%Y-%m-%d"):
        log.info("[FX] Already up to date.")
        return 0

    di = datetime.strptime(start, "%Y-%m-%d").strftime("%m-%d-%Y")
    df = TODAY.strftime("%m-%d-%Y")
    inserted = 0
    skip = 0
    while True:
        url = BCB_URL.format(di=di, df=df, skip=skip)
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            data = r.json().get("value", [])
        except Exception as e:
            raise RuntimeError(f"[FX] BCB API failed at skip={skip}: {e}")

        if not data:
            break
        for item in data:
            raw_dt = item.get("dataHoraCotacao", "")[:10]
            ptax   = item.get("cotacaoVenda")
            if not raw_dt or ptax is None:
                continue
            conn.execute(
                "INSERT OR IGNORE INTO fx_usdbrl "
                "(data_referencia,ano,mes,ptax_venda,updated_at) VALUES(?,?,?,?,?)",
                (raw_dt, int(raw_dt[:4]), int(raw_dt[5:7]), float(ptax), NOW_STR))
            if conn.execute("SELECT changes()").fetchone()[0]:
                inserted += 1
        log.info(f"[FX] skip={skip}: {len(data)} records")
        if len(data) < 1000:
            break
        skip += 1000
        time.sleep(0.3)

    conn.commit()
    log.info(f"[FX] {inserted} rows inserted.")
    return inserted


# ── Ethanol UDOP ──────────────────────────────────────────────────────────────

UDOP_URL = "https://www.udop.com.br/indicadores-etanol"

def make_driver():
    if not HAS_CHROME:
        raise RuntimeError("[ETANOL] undetected-chromedriver not installed")
    chrome = subprocess.run(["which", "google-chrome"], capture_output=True, text=True).stdout.strip()
    ver    = subprocess.run([chrome, "--version"], capture_output=True, text=True).stdout.strip()
    major  = int(ver.split()[-1].split(".")[0])
    log.info(f"[ETANOL] Chrome {ver} (major={major})")
    opts = uc.ChromeOptions()
    opts.binary_location = chrome
    for arg in ["--no-sandbox","--disable-dev-shm-usage","--disable-gpu",
                "--window-size=1280,900","--lang=pt-BR"]:
        opts.add_argument(arg)
    return uc.Chrome(options=opts, version_main=major)

def fetch_etanol_cepea(conn) -> int:
    ld = last_date(conn, "etanol_cepea")
    log.info(f"[ETANOL] Last in DB: {ld or 'none'}")
    driver, rows = None, []
    try:
        driver = make_driver()
        log.info(f"[ETANOL] Navigating to {UDOP_URL}")
        driver.get(UDOP_URL)
        time.sleep(8)
        try:
            driver.find_element(By.XPATH,
                "//button[contains(text(),'Diário') or contains(text(),'Di')]").click()
            time.sleep(2)
        except: pass
        try:
            driver.find_element(By.XPATH,
                "//button[contains(text(),'São Paulo')]").click()
            time.sleep(2)
        except: pass

        table = driver.find_element(By.CSS_SELECTOR, "table")
        for linha in table.find_elements(By.TAG_NAME, "tr"):
            cels = [c.text.strip() for c in linha.find_elements(By.TAG_NAME, "td")]
            if len(cels) < 2:
                continue
            dr = parse_date(cels[0])
            if not dr:
                continue
            try:
                val = float(cels[1].replace(".", "").replace(",", "."))
                if val > 0:
                    rows.append({"data_ref": dr, "preco_m3": val})
            except: continue

        log.info(f"[ETANOL] {len(rows)} rows read | "
                 f"{rows[-1]['data_ref'] if rows else '—'} → {rows[0]['data_ref'] if rows else '—'}")
    except Exception as e:
        raise RuntimeError(f"[ETANOL] Scraping failed: {e}")
    finally:
        if driver:
            try: driver.quit()
            except: pass

    if not rows:
        raise RuntimeError("[ETANOL] No data obtained from UDOP")

    if ld:
        rows = [r for r in rows if r["data_ref"] > ld]
    if not rows:
        log.info("[ETANOL] Nothing new.")
        return 0

    inserted = 0
    for r in rows:
        conn.execute(
            "INSERT OR IGNORE INTO etanol_cepea "
            "(data_referencia,ano,mes,preco_brl_m3,updated_at) VALUES(?,?,?,?,?)",
            (r["data_ref"], int(r["data_ref"][:4]), int(r["data_ref"][5:7]),
             r["preco_m3"], NOW_STR))
        if conn.execute("SELECT changes()").fetchone()[0]:
            inserted += 1
    conn.commit()
    log.info(f"[ETANOL] {inserted} rows inserted.")
    return inserted


# ─────────────────────────────────────────────────────────────────────────────
# ══ SECTION 2: Fuel Parity  (runs Thursdays only) ═══════════════════════════
# ─────────────────────────────────────────────────────────────────────────────

ANP_BASE     = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos"
FUEL_EST_URL = ANP_BASE + "/vdpb/semanal-estados-desde-2013.xlsx"
FUEL_BR_URL  = ANP_BASE + "/vdpb/semanal-brasil-desde-2013.xlsx"
PRODUTOS     = {"ETANOL HIDRATADO", "GASOLINA COMUM"}

def run_fuel(conn: sqlite3.Connection) -> dict:
    if not is_thursday():
        log.info("[Fuel] Not Thursday — skipping.")
        return {"skipped": True}

    log.info("=" * 60)
    log.info("Fuel Parity — ANP weekly prices (Etanol + Gasolina)")
    log.info("=" * 60)

    return {
        "estados": ingest_fuel_estados(conn),
        "brasil":  ingest_fuel_brasil(conn),
    }


def parse_anp_fuel_excel(content: bytes, label: str) -> pd.DataFrame | None:
    try:
        raw = pd.read_excel(io.BytesIO(content), sheet_name=0, header=None)
        header_row = next(
            (i for i, row in raw.iterrows() if "DATA INICIAL" in str(row.values)),
            None
        )
        if header_row is None:
            raise ValueError("'DATA INICIAL' header not found")
        df = pd.read_excel(io.BytesIO(content), sheet_name=0, header=header_row)
        df = df.dropna(subset=["DATA INICIAL"])
        df = df[df["PRODUTO"].isin(PRODUTOS)]
        df["DATA INICIAL"] = pd.to_datetime(df["DATA INICIAL"]).dt.strftime("%Y-%m-%d")
        df["DATA FINAL"]   = pd.to_datetime(df["DATA FINAL"]).dt.strftime("%Y-%m-%d")
        df["PREÇO MÉDIO REVENDA"] = pd.to_numeric(df["PREÇO MÉDIO REVENDA"], errors="coerce")
        log.info(f"[{label}] Parsed {len(df)} rows | "
                 f"{df['DATA INICIAL'].min()} → {df['DATA INICIAL'].max()}")
        return df
    except Exception as e:
        raise RuntimeError(f"[{label}] Excel parse failed: {e}")


def ingest_fuel_estados(conn) -> int:
    ld = last_date(conn, "anp_estados", "data_inicial")
    content = download(FUEL_EST_URL, "fuel-estados", fatal=True)
    df = parse_anp_fuel_excel(content, "fuel-estados")
    if ld:
        df = df[df["DATA INICIAL"] > ld]
    if df.empty:
        log.info("[fuel-estados] Nothing new.")
        return 0
    inserted = 0
    for _, r in df.iterrows():
        conn.execute(
            "INSERT OR IGNORE INTO anp_estados "
            "(data_inicial,data_final,regiao,estado,produto,preco_medio_revenda,updated_at) "
            "VALUES(?,?,?,?,?,?,?)",
            (r["DATA INICIAL"], r["DATA FINAL"],
             r.get("REGIÃO") or r.get("REGIAO"),
             r["ESTADO"], r["PRODUTO"],
             float(r["PREÇO MÉDIO REVENDA"]) if pd.notna(r["PREÇO MÉDIO REVENDA"]) else None,
             NOW_STR))
        if conn.execute("SELECT changes()").fetchone()[0]:
            inserted += 1
    conn.commit()
    log.info(f"[fuel-estados] {inserted} rows inserted.")
    return inserted


def ingest_fuel_brasil(conn) -> int:
    ld = last_date(conn, "anp_brasil", "data_inicial")
    content = download(FUEL_BR_URL, "fuel-brasil", fatal=True)
    df = parse_anp_fuel_excel(content, "fuel-brasil")
    if ld:
        df = df[df["DATA INICIAL"] > ld]
    if df.empty:
        log.info("[fuel-brasil] Nothing new.")
        return 0
    inserted = 0
    for _, r in df.iterrows():
        conn.execute(
            "INSERT OR IGNORE INTO anp_brasil "
            "(data_inicial,data_final,produto,preco_medio_revenda,updated_at) "
            "VALUES(?,?,?,?,?)",
            (r["DATA INICIAL"], r["DATA FINAL"], r["PRODUTO"],
             float(r["PREÇO MÉDIO REVENDA"]) if pd.notna(r["PREÇO MÉDIO REVENDA"]) else None,
             NOW_STR))
        if conn.execute("SELECT changes()").fetchone()[0]:
            inserted += 1
    conn.commit()
    log.info(f"[fuel-brasil] {inserted} rows inserted.")
    return inserted


# ─────────────────────────────────────────────────────────────────────────────
# ══ SECTION 3: Supply/Demand  (runs on 5th of each month) ═══════════════════
# ─────────────────────────────────────────────────────────────────────────────

VENDAS_ETH_URL = ANP_BASE + "/vdpb/vc/vendas-etanol-hidratado-m3-{year}.csv"
VENDAS_GAS_URL = ANP_BASE + "/vdpb/vc/vendas-gasolina-c-m3-{year}.csv"
PRODUCAO_URL   = ANP_BASE + "/peb/producao-etanol-hidratado-m3.csv"

MES_PT = {
    "JAN":1,"FEV":2,"MAR":3,"ABR":4,"MAI":5,"JUN":6,
    "JUL":7,"AGO":8,"SET":9,"OUT":10,"NOV":11,"DEZ":12,
}
ESTADO_NORM = {
    "Acre":"ACRE","Alagoas":"ALAGOAS","Amapá":"AMAPÁ","Amazonas":"AMAZONAS",
    "Bahia":"BAHIA","Ceará":"CEARÁ","Distrito Federal":"DISTRITO FEDERAL",
    "Espírito Santo":"ESPÍRITO SANTO","Goiás":"GOIÁS","Maranhão":"MARANHÃO",
    "Mato Grosso":"MATO GROSSO","Mato Grosso do Sul":"MATO GROSSO DO SUL",
    "Minas Gerais":"MINAS GERAIS","Pará":"PARÁ","Paraíba":"PARAÍBA",
    "Paraná":"PARANÁ","Pernambuco":"PERNAMBUCO","Piauí":"PIAUÍ",
    "Rio de Janeiro":"RIO DE JANEIRO","Rio Grande do Norte":"RIO GRANDE DO NORTE",
    "Rio Grande do Sul":"RIO GRANDE DO SUL","Rondônia":"RONDÔNIA",
    "Roraima":"RORAIMA","Santa Catarina":"SANTA CATARINA",
    "São Paulo":"SÃO PAULO","Sergipe":"SERGIPE","Tocantins":"TOCANTINS",
}

def run_supply_demand(conn: sqlite3.Connection) -> dict:
    if not is_month_5th():
        log.info("[Supply/Demand] Not the 5th of the month — skipping.")
        return {"skipped": True}

    log.info("=" * 60)
    log.info("Supply/Demand — ANP monthly volumes (Vendas + Produção)")
    log.info("=" * 60)

    return {
        "vendas":  ingest_vendas(conn),
        "producao": ingest_producao(conn),
    }


def parse_vendas_year(content: bytes, year: int, label: str) -> pd.DataFrame | None:
    for enc in ("latin-1", "utf-8-sig", "utf-8"):
        try:
            text = content.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    df = pd.read_csv(io.StringIO(text), sep=";", on_bad_lines="skip")
    df.columns = [c.strip().upper() for c in df.columns]
    uf_col = next(
        (c for c in df.columns if any(k in c for k in ("FEDERAÇÃO","FEDERACAO","ESTADO"," UF"))),
        None
    )
    if not uf_col:
        raise RuntimeError(f"[{label}] UF column not found. Cols: {list(df.columns)}")
    df = df[df[uf_col].notna()]
    df = df[~df[uf_col].str.upper().str.contains(r"TOTAL|BRASIL|REGIÃO|REGIAO|GRANDE",
                                                    na=False, regex=True)]
    mes_cols = {col: MES_PT[col[:3].upper()] for col in df.columns if col[:3].upper() in MES_PT}
    if not mes_cols:
        raise RuntimeError(f"[{label}] No month columns found")
    rows = []
    for _, row in df.iterrows():
        uf = str(row[uf_col]).strip().upper()
        for col, mes_num in mes_cols.items():
            val = row.get(col)
            if pd.isna(val):
                continue
            try:
                v = float(str(val).replace(".", "").replace(",", "."))
                rows.append({"ano": year, "mes": mes_num, "estado": uf, "volume": v})
            except: continue
    return pd.DataFrame(rows) if rows else None


def ingest_vendas(conn) -> int:
    last = last_year_month(conn, "anp_vendas_uf")
    last_ano = last[0] if last else 2013
    last_mes = last[1] if last else 0
    years = sorted({last_ano, last_ano + 1, TODAY.year})

    eth_frames, gas_frames = [], []
    for year in years:
        for url_tpl, frames, lbl in [
            (VENDAS_ETH_URL, eth_frames, f"eth-{year}"),
            (VENDAS_GAS_URL, gas_frames, f"gas-{year}"),
        ]:
            content = download(url_tpl.format(year=year), lbl, fatal=True)
            df = parse_vendas_year(content, year, lbl)
            if df is not None and not df.empty:
                frames.append(df)
            time.sleep(1)

    if not eth_frames and not gas_frames:
        raise RuntimeError("[vendas] No data fetched from ANP")

    def concat_rename(frames, col):
        if not frames:
            return pd.DataFrame(columns=["ano","mes","estado",col])
        return pd.concat(frames, ignore_index=True).rename(columns={"volume": col})

    eth = concat_rename(eth_frames, "eth_hid_m3")
    gas = concat_rename(gas_frames, "gas_c_m3")
    merged = eth.merge(gas, on=["ano","mes","estado"], how="outer") \
             if not eth.empty and not gas.empty else \
             (eth if not eth.empty else gas)
    if "eth_hid_m3" not in merged.columns: merged["eth_hid_m3"] = None
    if "gas_c_m3"   not in merged.columns: merged["gas_c_m3"]   = None

    merged = merged[
        (merged["ano"] > last_ano) |
        ((merged["ano"] == last_ano) & (merged["mes"] > last_mes))
    ]
    if merged.empty:
        log.info("[vendas] Nothing new.")
        return 0

    inserted = 0
    for _, r in merged.iterrows():
        conn.execute(
            "INSERT OR IGNORE INTO anp_vendas_uf "
            "(ano,mes,estado,eth_hid_m3,gas_c_m3,updated_at) VALUES(?,?,?,?,?,?)",
            (int(r.ano), int(r.mes), r.estado,
             float(r.eth_hid_m3) if pd.notna(r.get("eth_hid_m3")) else None,
             float(r.gas_c_m3)   if pd.notna(r.get("gas_c_m3"))   else None,
             NOW_STR))
        if conn.execute("SELECT changes()").fetchone()[0]:
            inserted += 1
    conn.commit()
    log.info(f"[vendas] {inserted} rows inserted.")
    return inserted


def ingest_producao(conn) -> int:
    last = last_year_month(conn, "anp_producao_uf")
    last_ano = last[0] if last else 2016
    last_mes = last[1] if last else 0

    content = download(PRODUCAO_URL, "producao", fatal=True)
    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            text = content.decode(enc); break
        except UnicodeDecodeError: continue

    df = pd.read_csv(io.StringIO(text), sep=",")
    df.columns = [c.strip() for c in df.columns]
    date_col = next((c for c in df.columns if "MÊS" in c.upper() or "MES" in c.upper()), None)
    hid_col  = next((c for c in df.columns if "HIDRATADO" in c.upper()), None)
    ani_col  = next((c for c in df.columns if "ANIDRO"   in c.upper()), None)
    est_col  = next((c for c in df.columns if "ESTADO"   in c.upper()), None)
    if not all([date_col, hid_col, est_col]):
        raise RuntimeError(f"[producao] Missing columns. Got: {list(df.columns)}")

    df["mes_ano"]    = pd.to_datetime(df[date_col], format="%m/%Y")
    df["ano"]        = df["mes_ano"].dt.year.astype(int)
    df["mes"]        = df["mes_ano"].dt.month.astype(int)
    df["estado"]     = df[est_col].str.strip().map(ESTADO_NORM).fillna(
                          df[est_col].str.strip().str.upper())
    df["eth_hid_m3"] = pd.to_numeric(df[hid_col], errors="coerce")
    df["eth_ani_m3"] = pd.to_numeric(df[ani_col], errors="coerce") if ani_col else None

    df = df[(df["ano"] > last_ano) | ((df["ano"] == last_ano) & (df["mes"] > last_mes))]
    if df.empty:
        log.info("[producao] Nothing new.")
        return 0

    inserted = 0
    for _, r in df.iterrows():
        conn.execute(
            "INSERT OR IGNORE INTO anp_producao_uf "
            "(ano,mes,estado,eth_hid_m3,eth_ani_m3,updated_at) VALUES(?,?,?,?,?,?)",
            (int(r.ano), int(r.mes), r.estado,
             float(r.eth_hid_m3) if pd.notna(r.eth_hid_m3) else None,
             float(r.eth_ani_m3) if pd.notna(r.eth_ani_m3) else None,
             NOW_STR))
        if conn.execute("SELECT changes()").fetchone()[0]:
            inserted += 1
    conn.commit()
    log.info(f"[producao] {inserted} rows inserted.")
    return inserted



# ─────────────────────────────────────────────────────────────────────────────
# ══ DASHBOARD GENERATION (runs after every scraper section) ═════════════════
# ─────────────────────────────────────────────────────────────────────────────

# HTML template stored as compressed base64 (before/after the data block)
_TMPL_BEFORE_B64 = "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"
_TMPL_AFTER_B64  = "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"

def generate_dashboard(conn: sqlite3.Connection) -> None:
    """Regenerate se_dashboard.html with latest data from DB."""
    import base64, gzip
    from collections import OrderedDict

    log.info("[Dashboard] Regenerating se_dashboard.html...")

    # ── Extract all data ──────────────────────────────────────────────────────
    ATR_VHP=1.05; ATR_HYD=1.68; FRETE=85.0; ELEVACAO=10.5; CONV_L_TON=1.04; CONV_TON_LB=22.0

    se_rows = conn.execute("""
        SELECT e.data_referencia, s.preco_usdclb, e.preco_brl_m3, f.ptax_venda
        FROM etanol_cepea e
        JOIN sugar_ny11  s ON s.data_referencia = e.data_referencia
        JOIN fx_usdbrl   f ON f.data_referencia = e.data_referencia
        ORDER BY e.data_referencia
    """).fetchall()
    se_data = []
    for dr, sugar, eth_m3, fx in se_rows:
        if not all([sugar, eth_m3, fx]): continue
        equiv = (((eth_m3*ATR_VHP/ATR_HYD)+FRETE+(ELEVACAO*fx))/CONV_L_TON/CONV_TON_LB)/fx
        se_data.append({"d":dr,"sugar":round(sugar,4),"eth":round(eth_m3,2),
                         "fx":round(fx,4),"equiv":round(equiv,2),"diff":round(equiv-sugar,2)})

    uf_series = {}
    for date, uf, parity in conn.execute("""
        SELECT e.data_inicial, e.estado, ROUND(e.preco_medio_revenda/g.preco_medio_revenda,4)
        FROM anp_estados e
        JOIN anp_estados g ON g.data_inicial=e.data_inicial AND g.estado=e.estado AND g.produto='GASOLINA COMUM'
        WHERE e.produto='ETANOL HIDRATADO' AND e.preco_medio_revenda IS NOT NULL AND g.preco_medio_revenda IS NOT NULL
        ORDER BY e.data_inicial
    """).fetchall():
        if uf not in uf_series: uf_series[uf] = []
        uf_series[uf].append({"d":date,"p":parity})

    br_series = [{"d":r[0],"p":r[1]} for r in conn.execute("""
        SELECT e.data_inicial, ROUND(e.preco_medio_revenda/g.preco_medio_revenda,4)
        FROM anp_brasil e
        JOIN anp_brasil g ON g.data_inicial=e.data_inicial AND g.produto='GASOLINA COMUM'
        WHERE e.produto='ETANOL HIDRATADO' AND e.preco_medio_revenda IS NOT NULL AND g.preco_medio_revenda IS NOT NULL
        ORDER BY e.data_inicial
    """).fetchall()]

    map_data = {}
    for date, uf, parity in conn.execute("""
        SELECT e.data_inicial, e.estado, ROUND(e.preco_medio_revenda/g.preco_medio_revenda,4)
        FROM anp_estados e
        JOIN anp_estados g ON g.data_inicial=e.data_inicial AND g.estado=e.estado AND g.produto='GASOLINA COMUM'
        WHERE e.produto='ETANOL HIDRATADO' AND e.preco_medio_revenda IS NOT NULL AND g.preco_medio_revenda IS NOT NULL
    """).fetchall():
        if date not in map_data: map_data[date] = {}
        map_data[date][uf] = parity

    map_dates = sorted(map_data.keys())
    month_map = OrderedDict()
    for dt in map_dates:
        month_map[dt[:7]] = dt
    MONTH_DATES  = list(month_map.values())
    MONTH_LABELS = list(month_map.keys())

    deficit_rows = conn.execute("""
        SELECT v.ano, v.mes, v.estado,
               ROUND(v.eth_hid_m3) AS vendas_m3,
               ROUND(COALESCE(p.eth_hid_m3,0)) AS prod_m3,
               ROUND(COALESCE(p.eth_hid_m3,0) - v.eth_hid_m3) AS saldo_m3
        FROM anp_vendas_uf v
        LEFT JOIN anp_producao_uf p ON p.ano=v.ano AND p.mes=v.mes AND p.estado=v.estado
        WHERE v.ano >= 2017 AND v.eth_hid_m3 IS NOT NULL
        ORDER BY v.ano, v.mes, v.estado
    """).fetchall()

    otto_rows = conn.execute("""
        SELECT ano, mes, estado,
               ROUND(eth_hid_m3*0.70/(eth_hid_m3*0.70+gas_c_m3),4)
        FROM anp_vendas_uf
        WHERE eth_hid_m3 IS NOT NULL AND gas_c_m3 IS NOT NULL
          AND (eth_hid_m3*0.70+gas_c_m3) > 0
        ORDER BY ano, mes, estado
    """).fetchall()

    deficit_series = {}; deficit_map = {}
    for ano, mes, estado, vendas, prod, saldo in deficit_rows:
        d = f"{ano}-{mes:02d}"
        if estado not in deficit_series: deficit_series[estado] = []
        deficit_series[estado].append({"d":d,"vendas":vendas,"prod":prod,"saldo":saldo})
        if d not in deficit_map: deficit_map[d] = {}
        deficit_map[d][estado] = {"s":saldo,"v":vendas,"p":prod}

    otto_series = {}; otto_map = {}
    for ano, mes, estado, pene in otto_rows:
        d = f"{ano}-{mes:02d}"
        if estado not in otto_series: otto_series[estado] = []
        otto_series[estado].append({"d":d,"p":float(pene)})
        if d not in otto_map: otto_map[d] = {}
        otto_map[d][estado] = float(pene)

    def_months  = sorted(deficit_map.keys())
    otto_months = sorted(otto_map.keys())

    def build_by_year(months):
        by_year = {}
        for m in months:
            y, mo = m[:4], m[5:7]
            if y not in by_year: by_year[y] = []
            by_year[y].append(mo)
        return by_year

    def_by_year = build_by_year(def_months)
    ott_by_year = build_by_year(otto_months)
    def_years   = sorted(def_by_year.keys(), reverse=True)
    ott_years   = sorted(ott_by_year.keys(), reverse=True)

    by_month2 = {}
    for uf, arr in deficit_series.items():
        for r in arr:
            d = r["d"]
            if d not in by_month2: by_month2[d] = {"vendas":0,"prod":0}
            by_month2[d]["vendas"] += (r.get("vendas") or 0)
            by_month2[d]["prod"]   += (r.get("prod") or 0)
    br_def = [{"d":d,"vendas":round(v["vendas"]),"prod":round(v["prod"]),"saldo":round(v["prod"]-v["vendas"])}
               for d,v in sorted(by_month2.items())]

    by_month3 = {}
    for uf, arr in deficit_series.items():
        for r in arr:
            d = r["d"]
            otto_val = otto_map.get(d, {}).get(uf)
            if otto_val is None or not r.get("vendas"): continue
            eth_eq = r["vendas"] * 0.70
            gas    = eth_eq * (1 - otto_val) / otto_val
            if d not in by_month3: by_month3[d] = {"eth_eq":0,"gas":0}
            by_month3[d]["eth_eq"] += eth_eq
            by_month3[d]["gas"]    += gas
    br_otto = [{"d":d,"p":round(v["eth_eq"]/(v["eth_eq"]+v["gas"]),4)}
                for d,v in sorted(by_month3.items()) if (v["eth_eq"]+v["gas"])>0]

    UF_CODE_SD = {
        'ACRE':'AC','ALAGOAS':'AL','AMAPÁ':'AP','AMAZONAS':'AM','BAHIA':'BA',
        'CEARÁ':'CE','DISTRITO FEDERAL':'DF','ESPÍRITO SANTO':'ES','GOIÁS':'GO',
        'MARANHÃO':'MA','MATO GROSSO':'MT','MATO GROSSO DO SUL':'MS','MINAS GERAIS':'MG',
        'PARÁ':'PA','PARAÍBA':'PB','PARANÁ':'PR','PERNAMBUCO':'PE','PIAUÍ':'PI',
        'RIO DE JANEIRO':'RJ','RIO GRANDE DO NORTE':'RN','RIO GRANDE DO SUL':'RS',
        'RONDÔNIA':'RO','RORAIMA':'RR','SANTA CATARINA':'SC','SÃO PAULO':'SP',
        'SERGIPE':'SE','TOCANTINS':'TO'
    }


    CODE_UF_PARITY = {"AC": "ACRE", "AL": "ALAGOAS", "AP": "AMAPA", "AM": "AMAZONAS", "BA": "BAHIA", "CE": "CEARA", "DF": "DISTRITO FEDERAL", "ES": "ESPIRITO SANTO", "GO": "GOIAS", "MA": "MARANHAO", "MT": "MATO GROSSO", "MS": "MATO GROSSO DO SUL", "MG": "MINAS GERAIS", "PA": "PARA", "PB": "PARAIBA", "PR": "PARANA", "PE": "PERNAMBUCO", "PI": "PIAUI", "RJ": "RIO DE JANEIRO", "RN": "RIO GRANDE DO NORTE", "RS": "RIO GRANDE DO SUL", "RO": "RONDONIA", "RR": "RORAIMA", "SC": "SANTA CATARINA", "SP": "SAO PAULO", "SE": "SERGIPE", "TO": "TOCANTINS"}
    CODE_NAME_MAP  = {"AC": "Acre", "AL": "Alagoas", "AP": "Amap\u00e1", "AM": "Amazonas", "BA": "Bahia", "CE": "Cear\u00e1", "DF": "Distrito Federal", "ES": "Esp\u00edrito Santo", "GO": "Goi\u00e1s", "MA": "Maranh\u00e3o", "MT": "Mato Grosso", "MS": "Mato Grosso do Sul", "MG": "Minas Gerais", "PA": "Par\u00e1", "PB": "Para\u00edba", "PR": "Paran\u00e1", "PE": "Pernambuco", "PI": "Piau\u00ed", "RJ": "Rio de Janeiro", "RN": "Rio Grande do Norte", "RS": "Rio Grande do Sul", "RO": "Rond\u00f4nia", "RR": "Roraima", "SC": "Santa Catarina", "SP": "S\u00e3o Paulo", "SE": "Sergipe", "TO": "Tocantins"}
    import json as _json
    J = lambda x: _json.dumps(x, separators=(',',':'))

    data_block = f"""
const SE_DATA      = {J(se_data)};
const UF_SERIES    = {J(uf_series)};
const BR_SERIES    = {J(br_series)};
const MAP_DATA     = {J(map_data)};
const MONTH_DATES  = {J(MONTH_DATES)};
const MONTH_LABELS = {J(MONTH_LABELS)};
const DEF_SERIES   = {J(deficit_series)};
const OTTO_SERIES  = {J(otto_series)};
const DEF_MAP      = {J(deficit_map)};
const OTTO_MAP     = {J(otto_map)};
const DEF_MONTHS   = {J(def_months)};
const OTTO_MONTHS  = {J(otto_months)};
const DEF_BY_YEAR  = {J(def_by_year)};
const OTT_BY_YEAR  = {J(ott_by_year)};
const DEF_YEARS    = {J(def_years)};
const OTT_YEARS    = {J(ott_years)};
const BR_DEF_SERIES  = {J(br_def)};
const BR_OTTO_SERIES = {J(br_otto)};
const UF_CODE_SD   = {J(UF_CODE_SD)};
const CODE_UF      = {J(CODE_UF_PARITY)};
const CODE_NAME    = {J(CODE_NAME_MAP)};
const MONTH_NAMES  = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];"""

    # Decompress templates and assemble
    tmpl_before = gzip.decompress(base64.b64decode(_TMPL_BEFORE_B64)).decode("utf-8")
    tmpl_after  = gzip.decompress(base64.b64decode(_TMPL_AFTER_B64)).decode("utf-8")
    html = tmpl_before + data_block + tmpl_after

    out_path = DB_PATH.parent / "se_dashboard.html"
    out_path.write_text(html, encoding="utf-8")
    log.info(f"[Dashboard] Written: {out_path} ({len(html):,} chars)")

# ─────────────────────────────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────────────────────────────

def summary(conn):
    log.info("=" * 60)
    log.info("DB SUMMARY")
    pairs = [
        ("sugar_ny11",      "data_referencia"),
        ("etanol_cepea",    "data_referencia"),
        ("fx_usdbrl",       "data_referencia"),
        ("anp_estados",     "data_inicial"),
        ("anp_brasil",      "data_inicial"),
    ]
    for tbl, col in pairs:
        r = conn.execute(f"SELECT COUNT(*), MIN({col}), MAX({col}) FROM {tbl}").fetchone()
        log.info(f"  {tbl:22}: {r[0]:7,} | {r[1] or '—'} → {r[2] or '—'}")
    for tbl in ["anp_vendas_uf","anp_producao_uf"]:
        r = conn.execute(f"SELECT COUNT(*), MIN(ano), MAX(ano) FROM {tbl}").fetchone()
        lm = conn.execute(
            f"SELECT MAX(ano), MAX(mes) FROM {tbl} WHERE ano=(SELECT MAX(ano) FROM {tbl})"
        ).fetchone()
        log.info(f"  {tbl:22}: {r[0]:7,} | {r[1]}→{r[2]} | latest: {lm[0]}-{lm[1]:02d}")
    log.info("=" * 60)


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    # Support --dashboard-only flag to regenerate dashboard without scraping
    dashboard_only = "--dashboard-only" in sys.argv

    log.info("=" * 60)
    if dashboard_only:
        log.info(f"IBBA Extractor | DASHBOARD-ONLY MODE | {NOW_STR}")
    else:
        log.info(f"IBBA Extractor | {TODAY} ({TODAY.strftime('%A')}) | {NOW_STR}")
        log.info(f"  Weekday: {is_weekday()} | Thursday: {is_thursday()} | 5th: {is_month_5th()}")
    log.info("=" * 60)

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = get_conn()
    ensure_schema(conn)

    errors = []

    if not dashboard_only:
        # S&E — daily
        try:
            run_se(conn)
        except Exception as e:
            log.error(f"[S&E] FAILED: {e}")
            errors.append(f"S&E: {e}")

        # Fuel — Thursdays
        try:
            run_fuel(conn)
        except Exception as e:
            log.error(f"[Fuel] FAILED: {e}")
            errors.append(f"Fuel: {e}")

        # Supply/Demand — 5th of month
        try:
            run_supply_demand(conn)
        except Exception as e:
            log.error(f"[Supply/Demand] FAILED: {e}")
            errors.append(f"Supply/Demand: {e}")

    # Regenerate dashboard with latest data
    try:
        generate_dashboard(conn)
    except Exception as e:
        log.error(f'[Dashboard] Generation failed: {e}')
        errors.append(f'Dashboard: {e}')

    summary(conn)
    conn.close()

    if errors:
        log.error(f"EXTRACTOR FINISHED WITH {len(errors)} ERROR(S):")
        for e in errors:
            log.error(f"  • {e}")
        sys.exit(1)
    else:
        log.info("All sections completed successfully.")


if __name__ == "__main__":
    main()
