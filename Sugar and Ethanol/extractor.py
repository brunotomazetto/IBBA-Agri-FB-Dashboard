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
    log.info("=" * 60)
    log.info(f"IBBA Extractor | {TODAY} ({TODAY.strftime('%A')}) | {NOW_STR}")
    log.info(f"  Weekday: {is_weekday()} | Thursday: {is_thursday()} | 5th: {is_month_5th()}")
    log.info("=" * 60)

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = get_conn()
    ensure_schema(conn)

    errors = []

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

    summary(conn)
    conn.close()

    if errors:
        log.error(f"EXTRACTOR FINISHED WITH {len(errors)} ERROR(S):")
        for e in errors:
            log.error(f"  • {e}")
        sys.exit(1)   # non-zero exit → GitHub marks run red ✗
    else:
        log.info("All sections completed successfully.")


if __name__ == "__main__":
    main()
