#!/usr/bin/env python3
"""
extractor_se.py — IBBA Agri Monitor · S&E Dashboard
=====================================================
Execução diária via GitHub Actions.

Fontes confirmadas em testes (07/04/2026):
  NY11   → Yahoo Finance (SB=F), diário, sem bloqueio
  Etanol → UNICAdata/UNICA (xlsPrcProd.php)
             idTabela=2400 → diário, dados atuais (07/04/2026 confirmado)
             idTabela=1433 → mensal, janela de 3 anos (histórico em loop)
             Fonte primária: CEPEA/ESALQ republicado pela UNICA
             Preço ao produtor, CIF Paulínia-SP, sem frete, sem impostos
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

UNICA_BASE    = "https://unicadata.com.br/xlsPrcProd.php"
UNICA_REFERER = "https://unicadata.com.br/preco-ao-produtor.php?idMn=42"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/123.0.0.0 Safari/537.36",
    "Accept-Language": "pt-BR,pt;q=0.9",
    "Referer": UNICA_REFERER,
}

# idTabela=2400 → diário atual (confirmado: retorna 07/04/2026)
# idTabela=2405 → NÃO USAR (arquivo estático de 2019)
UNICA_ID_DIARIO  = 2400
# idTabela=1433 → mensal SP, janela de 3 anos (varia conforme o ano atual)
UNICA_ID_MENSAL  = 1433

# Anos para varredura histórica mensal (loop de 3 em 3 anos não é necessário
# pois o site já mostra os 3 anos mais recentes — para histórico completo
# usamos uma série de idTabelas de anos anteriores via parâmetro ano_base)
# Na prática o 1433 mostra sempre os últimos 3 anos disponíveis.
# Para histórico mais antigo, usaremos anos específicos se necessário.

MESES_PT = {
    "janeiro": 1, "fevereiro": 2, "março": 3, "abril": 4,
    "maio": 5, "junho": 6, "julho": 7, "agosto": 8,
    "setembro": 9, "outubro": 10, "novembro": 11, "dezembro": 12,
}


# ════════════════════════════════════════════════════════════════════════════════
# BANCO
# ════════════════════════════════════════════════════════════════════════════════
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
        fonte TEXT DEFAULT 'UNICA/CEPEA-Paulinia', updated_at TEXT,
        UNIQUE(data_referencia));
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


# ════════════════════════════════════════════════════════════════════════════════
# NY11
# ════════════════════════════════════════════════════════════════════════════════
def fetch_sugar_ny11(conn, now_str):
    log.info("[NY11] Buscando Yahoo Finance (SB=F)...")
    last  = _last_date(conn, "sugar_ny11")
    start = (datetime.strptime(last, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d") if last else HISTORY_START
    today = date.today().strftime("%Y-%m-%d")
    if start > today:
        log.info("[NY11] Já atualizado."); return 0
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
            "INSERT OR IGNORE INTO sugar_ny11 "
            "(data_referencia,ano,mes,preco_usdclb,open_usdclb,high_usdclb,low_usdclb,volume,updated_at) "
            "VALUES(?,?,?,?,?,?,?,?,?)",
            (dr,int(dr[:4]),int(dr[5:7]),cl,
             _safe_float(row.get("Open")),_safe_float(row.get("High")),
             _safe_float(row.get("Low")),_safe_float(row.get("Volume")),now_str))
        if conn.execute("SELECT changes()").fetchone()[0]: inserted += 1
    conn.commit()
    log.info(f"[NY11] {inserted} linhas inseridas.")
    return inserted


# ════════════════════════════════════════════════════════════════════════════════
# ETANOL — helpers
# ════════════════════════════════════════════════════════════════════════════════
def _get_xls(id_tabela, extra_params="", label=""):
    url = (f"{UNICA_BASE}?idioma=1&tipoHistorico=7&idTabela={id_tabela}"
           f"&estado=Paulinia&produto=Etanol+hidratado+combust%C3%ADvel"
           f"&frequencia=Di%C3%A1rio{extra_params}")
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        r.raise_for_status()
        if r.content[:4] in (b"PK\x03\x04", b"\xd0\xcf\x11\xe0"):
            log.info(f"[ETANOL] '{label}': {len(r.content):,} bytes OK")
            return r.content
        log.error(f"[ETANOL] '{label}': resposta não é Excel")
    except Exception as e:
        log.error(f"[ETANOL] '{label}': {e}")
    return None

def _get_xls_mensal(id_tabela, label=""):
    url = (f"{UNICA_BASE}?idioma=1&tipoHistorico=7&idTabela={id_tabela}"
           f"&estado=S%C3%A3o+Paulo&produto=Etanol+hidratado+combust%C3%ADvel"
           f"&frequencia=Mensal")
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        r.raise_for_status()
        if r.content[:4] in (b"PK\x03\x04", b"\xd0\xcf\x11\xe0"):
            log.info(f"[ETANOL] '{label}': {len(r.content):,} bytes OK")
            return r.content
        log.error(f"[ETANOL] '{label}': resposta não é Excel")
    except Exception as e:
        log.error(f"[ETANOL] '{label}': {e}")
    return None


def _parse_diario(content):
    """
    Layout confirmado: col 4 = data DD/MM/YYYY, col 5 = preço com vírgula.
    """
    engine = "openpyxl" if content[:4] == b"PK\x03\x04" else "xlrd"
    raw = pd.read_excel(io.BytesIO(content), engine=engine, header=None, dtype=str)
    rows = []
    for _, row in raw.iterrows():
        if len(row) < 6: continue
        dr = _parse_date_br(str(row.iloc[4]))
        if not dr: continue
        try:
            p = float(str(row.iloc[5]).replace(",", "."))
            if p > 0: rows.append({"data_ref": dr, "preco": p})
        except: continue
    rows.sort(key=lambda r: r["data_ref"])
    if rows:
        log.info(f"[ETANOL] diário: {len(rows)} registros | {rows[0]['data_ref']} → {rows[-1]['data_ref']}")
    return rows


def _parse_mensal(content):
    """
    Layout tabela cruzada: col 3 = mês (nome PT), cols 4..N = anos.
    Linha de header contém anos (4 dígitos).
    """
    engine = "openpyxl" if content[:4] == b"PK\x03\x04" else "xlrd"
    raw = pd.read_excel(io.BytesIO(content), engine=engine, header=None, dtype=str)

    # Localiza linha de header com anos
    header_row, col_ano = None, {}
    for i, row in raw.iterrows():
        anos = {}
        for ci, v in enumerate(row):
            try:
                a = int(str(v).strip())
                if 2000 <= a <= 2030: anos[ci] = a
            except: pass
        if len(anos) >= 2:
            header_row, col_ano = i, anos
            break

    if header_row is None:
        log.warning("[ETANOL] mensal: header com anos não encontrado"); return []

    # Localiza coluna dos meses
    col_mes = None
    for ci in range(len(raw.columns)):
        for ri in range(header_row + 1, min(header_row + 15, len(raw))):
            if str(raw.iloc[ri, ci]).strip().lower() in MESES_PT:
                col_mes = ci; break
        if col_mes is not None: break

    if col_mes is None:
        log.warning("[ETANOL] mensal: coluna de meses não encontrada"); return []

    log.info(f"[ETANOL] mensal: anos={sorted(col_ano.values())}")
    rows = []
    for ri in range(header_row + 1, len(raw)):
        row = raw.iloc[ri]
        mes_num = MESES_PT.get(str(row.iloc[col_mes]).strip().lower())
        if not mes_num: continue
        for ci, ano in col_ano.items():
            try:
                p = float(str(row.iloc[ci]).replace(",", "."))
                if p <= 0: continue
                rows.append({"data_ref": f"{ano:04d}-{mes_num:02d}-15", "preco": p})
            except: continue

    rows.sort(key=lambda r: r["data_ref"])
    if rows:
        log.info(f"[ETANOL] mensal: {len(rows)} registros | {rows[0]['data_ref']} → {rows[-1]['data_ref']}")
    return rows


def _insert_rows(conn, rows, last, now_str):
    if last:
        rows = [r for r in rows if r["data_ref"] > last]
    if not rows:
        log.info("[ETANOL] Nada novo."); return 0
    inserted = 0
    for r in rows:
        conn.execute(
            "INSERT OR IGNORE INTO etanol_cepea (data_referencia,ano,mes,preco_brl_l,updated_at) VALUES(?,?,?,?,?)",
            (r["data_ref"], int(r["data_ref"][:4]), int(r["data_ref"][5:7]), r["preco"], now_str))
        if conn.execute("SELECT changes()").fetchone()[0]: inserted += 1
    conn.commit()
    return inserted


# ════════════════════════════════════════════════════════════════════════════════
# ETANOL — orquestração
# ════════════════════════════════════════════════════════════════════════════════
def fetch_etanol_cepea(conn, now_str):
    carga = "--carga-historica" in sys.argv
    last  = _last_date(conn, "etanol_cepea")
    log.info(f"[ETANOL] Último no banco: {last or 'nenhum'}")
    total = 0

    # ── Carga histórica: mensal (tabela cruzada, janela dos últimos 3 anos) ───
    # A UNICAdata sempre mostra os 3 anos mais recentes no idTabela=1433.
    # Para histórico completo desde 2002, o site exigiria navegação com POST,
    # então usamos o mensal como complemento histórico e o diário para recência.
    if carga or last is None:
        log.info("[ETANOL] Baixando histórico mensal (idTabela=1433)...")
        c = _get_xls_mensal(UNICA_ID_MENSAL, "mensal")
        if c:
            rows = _parse_mensal(c)
            n = _insert_rows(conn, rows, None, now_str)
            log.info(f"[ETANOL] Mensal: {n} inseridas.")
            total += n
            last = _last_date(conn, "etanol_cepea")
        time.sleep(1)

    # ── Atualização diária (idTabela=2400 — confirmado com dados de hoje) ─────
    log.info(f"[ETANOL] Baixando diário (idTabela={UNICA_ID_DIARIO})...")
    c = _get_xls(UNICA_ID_DIARIO, label="diário 2400")
    if c:
        rows = _parse_diario(c)
        n = _insert_rows(conn, rows, last, now_str)
        log.info(f"[ETANOL] Diário: {n} inseridas.")
        total += n
    else:
        log.error("[ETANOL] Falha nos dados diários.")

    return total


# ════════════════════════════════════════════════════════════════════════════════
# SUMMARY + MAIN
# ════════════════════════════════════════════════════════════════════════════════
def _summary(conn):
    log.info("=" * 60)
    log.info("RESUMO DO BANCO")
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
