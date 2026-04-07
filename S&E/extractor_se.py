#!/usr/bin/env python3
"""
extractor_se.py — IBBA Agri Monitor · S&E Dashboard
"""

import io, logging, sqlite3, sys, time
from datetime import date, datetime, timedelta
from pathlib import Path

import requests

try:
    import pandas as pd
except ImportError:
    raise SystemExit("pip install pandas openpyxl xlrd yfinance")

try:
    import yfinance as yf
except ImportError:
    raise SystemExit("pip install yfinance")

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent / "commodities.db"

YF_TICKER     = "SB=F"
HISTORY_START = "2010-01-01"

UNICA_XLS_DIARIO = (
    "https://unicadata.com.br/xlsPrcProd.php"
    "?idioma=1&tipoHistorico=7&idTabela=2405"
    "&estado=Paulinia&produto=Etanol+hidratado+combust%C3%ADvel&frequencia=Di%C3%A1rio"
)
UNICA_XLS_MENSAL = (
    "https://unicadata.com.br/xlsPrcProd.php"
    "?idioma=1&tipoHistorico=7&idTabela=1433"
    "&estado=S%C3%A3o+Paulo&produto=Etanol+hidratado+combust%C3%ADvel&frequencia=Mensal"
)
UNICA_REFERER = "https://unicadata.com.br/preco-ao-produtor.php?idMn=42"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/123.0.0.0 Safari/537.36",
    "Accept-Language": "pt-BR,pt;q=0.9",
    "Referer": UNICA_REFERER,
}


# ── DB ────────────────────────────────────────────────────────────────────────
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def ensure_schema(conn):
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS sugar_ny11 (
        id INTEGER PRIMARY KEY AUTOINCREMENT, data_referencia TEXT NOT NULL,
        ano INTEGER, mes INTEGER, preco_usdclb REAL NOT NULL,
        open_usdclb REAL, high_usdclb REAL, low_usdclb REAL, volume REAL,
        fonte TEXT DEFAULT 'Yahoo/SB=F', updated_at TEXT, UNIQUE(data_referencia));
    CREATE INDEX IF NOT EXISTS idx_sugar_data ON sugar_ny11(data_referencia);
    CREATE TABLE IF NOT EXISTS etanol_cepea (
        id INTEGER PRIMARY KEY AUTOINCREMENT, data_referencia TEXT NOT NULL,
        ano INTEGER, mes INTEGER, preco_brl_l REAL NOT NULL,
        fonte TEXT DEFAULT 'UNICA/CEPEA-Paulinia', updated_at TEXT, UNIQUE(data_referencia));
    CREATE INDEX IF NOT EXISTS idx_etanol_data ON etanol_cepea(data_referencia);
    """)
    conn.commit()
    log.info(f"Schema OK — banco: {DB_PATH}")

def _last_date(conn, table):
    r = conn.execute(f"SELECT MAX(data_referencia) FROM {table}").fetchone()
    return r[0] if r and r[0] else None

def _safe_float(val):
    try:
        f = float(val)
        return None if str(f) == "nan" else f
    except: return None

def _parse_date_br(raw):
    raw = str(raw).strip()
    for fmt in ("%d/%m/%Y", "%d/%m/%y", "%Y-%m-%d", "%d-%m-%Y"):
        try: return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except: continue
    return None


# ── NY11 ──────────────────────────────────────────────────────────────────────
def fetch_sugar_ny11(conn, now_str):
    log.info("[NY11] Buscando Yahoo Finance (SB=F)...")
    last  = _last_date(conn, "sugar_ny11")
    start = (datetime.strptime(last, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d") if last else HISTORY_START
    today = date.today().strftime("%Y-%m-%d")
    if start > today:
        log.info("[NY11] Já atualizado.")
        return 0
    log.info(f"[NY11] {start} → hoje")
    try:
        df = yf.Ticker(YF_TICKER).history(start=start, end=today, auto_adjust=False)
    except Exception as e:
        log.error(f"[NY11] {e}"); return 0
    if df is None or df.empty:
        log.info("[NY11] Sem dados novos."); return 0
    df.index = pd.to_datetime(df.index).tz_localize(None)
    inserted = 0
    for ts, row in df.iterrows():
        dr = ts.strftime("%Y-%m-%d")
        cl = _safe_float(row.get("Close"))
        if not cl: continue
        conn.execute(
            "INSERT OR IGNORE INTO sugar_ny11 (data_referencia,ano,mes,preco_usdclb,open_usdclb,high_usdclb,low_usdclb,volume,updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
            (dr,int(dr[:4]),int(dr[5:7]),cl,_safe_float(row.get("Open")),_safe_float(row.get("High")),_safe_float(row.get("Low")),_safe_float(row.get("Volume")),now_str))
        if conn.execute("SELECT changes()").fetchone()[0]: inserted += 1
    conn.commit()
    log.info(f"[NY11] {inserted} linhas inseridas.")
    return inserted


# ── ETANOL ────────────────────────────────────────────────────────────────────
def fetch_etanol_cepea(conn, now_str):
    carga = "--carga-historica" in sys.argv
    last  = _last_date(conn, "etanol_cepea")
    log.info(f"[ETANOL] Último no banco: {last or 'nenhum'}")
    total = 0

    if carga or last is None:
        log.info("[ETANOL] Baixando histórico mensal (idTabela=1433)...")
        rows = _download_and_parse(UNICA_XLS_MENSAL, "mensal")
        if rows:
            n = _insert_rows(conn, rows, last, now_str)
            log.info(f"[ETANOL] Mensal: {n} inseridas.")
            total += n
            last = _last_date(conn, "etanol_cepea")

    log.info("[ETANOL] Baixando dados diários (idTabela=2405)...")
    rows = _download_and_parse(UNICA_XLS_DIARIO, "diário")
    if rows:
        n = _insert_rows(conn, rows, last, now_str)
        log.info(f"[ETANOL] Diário: {n} inseridas.")
        total += n
    else:
        log.error("[ETANOL] Falha nos dados diários.")
    return total


def _download_and_parse(url, label):
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        r.raise_for_status()
    except Exception as e:
        log.error(f"[ETANOL] Download '{label}': {e}")
        return []

    content = r.content
    log.info(f"[ETANOL] '{label}': {len(content):,} bytes | magic={content[:4].hex()}")

    is_xlsx = content[:4] == b"PK\x03\x04"
    is_xls  = content[:4] == b"\xd0\xcf\x11\xe0"
    if not (is_xlsx or is_xls):
        log.error(f"[ETANOL] '{label}': não é Excel")
        return []

    engine = "openpyxl" if is_xlsx else "xlrd"
    try:
        raw = pd.read_excel(io.BytesIO(content), engine=engine, header=None, dtype=str)
    except Exception as e:
        log.error(f"[ETANOL] parse erro: {e}")
        return []

    # ── Log das primeiras 20 linhas para ver o layout real ───────────────
    log.info(f"[ETANOL] '{label}': shape={raw.shape}")
    for i, row in raw.head(20).iterrows():
        vals = [str(v)[:35] for v in row.tolist()]
        log.info(f"[ETANOL]   row {i:2d}: {vals}")

    # ── Parse: varre todas as células buscando data + preço na mesma linha
    rows = []
    for _, row in raw.iterrows():
        data_ref = None
        preco    = None
        # Tenta cada célula como data
        for ci in range(len(row)):
            dr = _parse_date_br(str(row.iloc[ci]))
            if dr:
                data_ref = dr
                # Preço = próxima célula numérica válida
                for pi in range(ci + 1, len(row)):
                    pv = str(row.iloc[pi]).strip().replace(",", ".")
                    try:
                        p = float(pv)
                        if p > 0:
                            preco = p
                            break
                    except: continue
                break
        if data_ref and preco:
            rows.append({"data_ref": data_ref, "preco": preco})

    rows.sort(key=lambda r: r["data_ref"])
    log.info(f"[ETANOL] '{label}': {len(rows)} registros válidos")
    if rows:
        log.info(f"[ETANOL] Período: {rows[0]['data_ref']} → {rows[-1]['data_ref']}")
        log.info(f"[ETANOL] Amostra: {rows[:2]}")
    return rows


def _insert_rows(conn, rows, last, now_str):
    if last:
        rows = [r for r in rows if r["data_ref"] > last]
    if not rows:
        log.info("[ETANOL] Nada novo.")
        return 0
    inserted = 0
    for r in rows:
        conn.execute(
            "INSERT OR IGNORE INTO etanol_cepea (data_referencia,ano,mes,preco_brl_l,updated_at) VALUES(?,?,?,?,?)",
            (r["data_ref"], int(r["data_ref"][:4]), int(r["data_ref"][5:7]), r["preco"], now_str))
        if conn.execute("SELECT changes()").fetchone()[0]: inserted += 1
    conn.commit()
    return inserted


# ── SUMMARY ───────────────────────────────────────────────────────────────────
def _summary(conn):
    log.info("=" * 60)
    for table, label, col, unit in [
        ("sugar_ny11",   "Açúcar NY11",           "preco_usdclb", "USDc/lb"),
        ("etanol_cepea", "Etanol Hidratado UNICA", "preco_brl_l",  "R$/l"),
    ]:
        r = conn.execute(f"SELECT COUNT(*), MIN(data_referencia), MAX(data_referencia) FROM {table}").fetchone()
        n, dmin, dmax = r
        last = conn.execute(f"SELECT data_referencia, {col} FROM {table} ORDER BY data_referencia DESC LIMIT 1").fetchone()
        ls = f"{last[1]:.4f} {unit}" if last and last[1] else "—"
        log.info(f"  {label:28}: {n:6} | {dmin or '—'} → {dmax or '—'} | Último: {last[0] if last else '—'} = {ls}")
    log.info("=" * 60)


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log.info("=" * 60)
    log.info(f"S&E Extractor | Banco: {DB_PATH} | {now_str}")
    log.info("=" * 60)
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = get_conn()
    ensure_schema(conn)

    log.info("--- NY11 ---")
    n1 = fetch_sugar_ny11(conn, now_str)
    time.sleep(1)
    log.info("--- Etanol ---")
    n2 = fetch_etanol_cepea(conn, now_str)

    _summary(conn)
    conn.close()
    log.info(f"Fim — NY11: {n1} | Etanol: {n2}")

if __name__ == "__main__":
    main()
