#!/usr/bin/env python3
"""
extractor_se.py — IBBA Agri Monitor · S&E Dashboard
=====================================================
Execução diária via GitHub Actions.

Fontes:
  NY11   → Yahoo Finance (SB=F), diário
  Etanol → UDOP (udop.com.br/indicadores-etanol)
             Indicador Diário ESALQ/BM&FBovespa Posto Paulínia (SP)
             Mesma série do CEPEA, sem Cloudflare, sem login
             Unidade armazenada: R$/m³ (valor original UDOP × 1000)
  Câmbio → BCB API (PTAX oficial), diário

═══════════════════════════════════════════════════════════════
RACIONAL — PREÇO EQUIVALENTE ETANOL vs AÇÚCAR
═══════════════════════════════════════════════════════════════

O dashboard vai comparar o preço do Açúcar NY11 (USDc/lb) com o
preço equivalente do Etanol Hidratado convertido para USDc/lb,
permitindo ver qual produto compensa mais produzir na usina.

Fórmula (replicada do Excel de referência IBBA):

  Etanol_equiv (USDc/lb) =
    (
      (Etanol_R$/m³ × ATR_VHP / ATR_Hydrous)
      + Frete_R$/ton
      + (Elevação_USD/ton × FX_R$/USD)
    )
    ÷ conv_l_ton        [l/ton  → 1.04]
    ÷ conv_ton_lb       [ton/lb → 22.0]
    ÷ FX_R$/USD

Parâmetros fixos (podem mudar por safra — ajustar aqui):
  ATR_VHP      = 1.05    (kg ATR / litro VHP)
  ATR_Hydrous  = 1.68    (kg ATR / litro hidratado)
  Frete        = 85.0    (R$/ton)
  Elevação     = 10.5    (USD/ton — custo FOB/porto)
  conv_l_ton   = 1.04    (litros por tonelada de etanol)
  conv_ton_lb  = 22.0    (conversão ton→lb no contexto açúcar)

IMPORTANTE: O preço equivalente NÃO é armazenado no banco.
É calculado on-the-fly no dashboard usando os parâmetros acima.
Assim, mudando um parâmetro, todo o histórico se atualiza.

Validação: 18/03/2026 → Etanol=3040 R$/m³, FX=5.20 → 17.14 USDc/lb ✓

═══════════════════════════════════════════════════════════════
TABELAS DO BANCO (commodities.db)
═══════════════════════════════════════════════════════════════
  sugar_ny11    → preço diário NY11 (USDc/lb)
  etanol_cepea  → preço diário etanol hidratado (R$/m³)
  fx_usdbrl     → câmbio diário PTAX USD/BRL (R$/USD)
"""

import logging, sqlite3, subprocess, time
from datetime import date, datetime, timedelta
from pathlib import Path

try:
    import pandas as pd
except ImportError:
    raise SystemExit("pip install pandas yfinance undetected-chromedriver selenium requests")

try:
    import yfinance as yf
except ImportError:
    raise SystemExit("pip install yfinance")

try:
    import requests
except ImportError:
    raise SystemExit("pip install requests")

try:
    import undetected_chromedriver as uc
    from selenium.webdriver.common.by import By
except ImportError:
    raise SystemExit("pip install undetected-chromedriver selenium")

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)

DB_PATH       = Path(__file__).parent / "commodities.db"
YF_TICKER     = "SB=F"
HISTORY_START = "2010-01-01"
UDOP_URL      = "https://www.udop.com.br/indicadores-etanol"
BCB_PTAX_URL  = "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?@dataInicial='{di}'&@dataFinalCotacao='{df}'&$top=1000&$orderby=dataHoraCotacao%20asc&$format=json&$select=cotacaoVenda,dataHoraCotacao"


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
        ano INTEGER, mes INTEGER,
        preco_brl_m3 REAL NOT NULL,
        fonte TEXT DEFAULT 'UDOP/CEPEA-Paulinia', updated_at TEXT,
        UNIQUE(data_referencia));
    CREATE INDEX IF NOT EXISTS idx_etanol_data ON etanol_cepea(data_referencia);

    CREATE TABLE IF NOT EXISTS fx_usdbrl (
        id INTEGER PRIMARY KEY AUTOINCREMENT, data_referencia TEXT NOT NULL,
        ano INTEGER, mes INTEGER,
        ptax_venda REAL NOT NULL,
        fonte TEXT DEFAULT 'BCB/PTAX', updated_at TEXT,
        UNIQUE(data_referencia));
    CREATE INDEX IF NOT EXISTS idx_fx_data ON fx_usdbrl(data_referencia);
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

def _parse_date(raw):
    raw = str(raw).strip()
    for fmt in ("%d/%m/%Y", "%d/%m/%y", "%Y-%m-%d"):
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
# CÂMBIO — BCB PTAX
# ════════════════════════════════════════════════════════════════════════════════
def fetch_fx_usdbrl(conn, now_str):
    """
    Coleta PTAX venda USD/BRL via API do Banco Central do Brasil.
    Frequência: diária (dias úteis).
    Histórico: desde HISTORY_START.
    """
    log.info("[FX] Buscando PTAX USD/BRL (BCB)...")
    last  = _last_date(conn, "fx_usdbrl")
    start = (datetime.strptime(last, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d") if last else HISTORY_START
    today = date.today().strftime("%Y-%m-%d")
    if start > today:
        log.info("[FX] Já atualizado."); return 0

    # BCB API usa formato MM-DD-YYYY
    di = datetime.strptime(start, "%Y-%m-%d").strftime("%m-%d-%Y")
    df = datetime.strptime(today,  "%Y-%m-%d").strftime("%m-%d-%Y")
    url = BCB_PTAX_URL.format(di=di, df=df)

    log.info(f"[FX] {start} → hoje")
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        data = r.json().get("value", [])
    except Exception as e:
        log.error(f"[FX] Falha BCB: {e}"); return 0

    inserted = 0
    for item in data:
        raw_dt   = item.get("dataHoraCotacao","")[:10]  # "YYYY-MM-DD"
        ptax     = item.get("cotacaoVenda")
        if not raw_dt or ptax is None: continue
        dr = raw_dt  # já em YYYY-MM-DD
        conn.execute(
            "INSERT OR IGNORE INTO fx_usdbrl (data_referencia,ano,mes,ptax_venda,updated_at) VALUES(?,?,?,?,?)",
            (dr, int(dr[:4]), int(dr[5:7]), float(ptax), now_str))
        if conn.execute("SELECT changes()").fetchone()[0]: inserted += 1

    conn.commit()
    log.info(f"[FX] {inserted} linhas inseridas.")
    return inserted


# ════════════════════════════════════════════════════════════════════════════════
# ETANOL — UDOP via undetected-chromedriver
# ════════════════════════════════════════════════════════════════════════════════
def make_driver():
    chrome_path = subprocess.run(["which","google-chrome"], capture_output=True, text=True).stdout.strip()
    ver_str     = subprocess.run([chrome_path,"--version"], capture_output=True, text=True).stdout.strip()
    major       = int(ver_str.split()[-1].split(".")[0])
    log.info(f"[ETANOL] Chrome: {ver_str} (major={major})")
    opts = uc.ChromeOptions()
    opts.binary_location = chrome_path
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1280,900")
    opts.add_argument("--lang=pt-BR")
    return uc.Chrome(options=opts, version_main=major)

def fetch_etanol_cepea(conn, now_str):
    """
    Coleta preço diário do etanol hidratado via UDOP.
    Armazena em R$/m³ (valor original — não divide por 1000).
    O dashboard converte para R$/l quando necessário (÷ 1000).
    """
    last = _last_date(conn, "etanol_cepea")
    log.info(f"[ETANOL] Último no banco: {last or 'nenhum'}")

    driver, rows = None, []
    try:
        driver = make_driver()
        log.info(f"[ETANOL] Navegando para {UDOP_URL}")
        driver.get(UDOP_URL)
        time.sleep(8)

        try:
            driver.find_element(By.XPATH, "//button[contains(text(),'Diário') or contains(text(),'Di')]").click()
            time.sleep(2)
        except: pass
        try:
            driver.find_element(By.XPATH, "//button[contains(text(),'São Paulo')]").click()
            time.sleep(2)
        except: pass

        tabela = driver.find_element(By.CSS_SELECTOR, "table")
        linhas = tabela.find_elements(By.TAG_NAME, "tr")
        log.info(f"[ETANOL] Linhas na tabela: {len(linhas)}")

        for linha in linhas:
            cels = [c.text.strip() for c in linha.find_elements(By.TAG_NAME, "td")]
            if len(cels) < 2: continue
            dr = _parse_date(cels[0])
            if not dr: continue
            try:
                # Valor da tabela UDOP já está em R$/m³ (ex: 2.948,50)
                val_m3 = float(cels[1].replace(".","").replace(",","."))
                if val_m3 <= 0: continue
                rows.append({"data_ref": dr, "preco_m3": val_m3})
            except: continue

        log.info(f"[ETANOL] {len(rows)} registros lidos | {rows[-1]['data_ref'] if rows else '—'} → {rows[0]['data_ref'] if rows else '—'}")

    except Exception as e:
        log.error(f"[ETANOL] Erro: {e}")
    finally:
        if driver:
            try: driver.quit()
            except: pass

    if not rows:
        log.error("[ETANOL] Nenhum dado obtido."); return 0

    if last:
        rows = [r for r in rows if r["data_ref"] > last]
    if not rows:
        log.info("[ETANOL] Nada novo."); return 0

    inserted = 0
    for r in rows:
        conn.execute(
            "INSERT OR IGNORE INTO etanol_cepea (data_referencia,ano,mes,preco_brl_m3,updated_at) VALUES(?,?,?,?,?)",
            (r["data_ref"], int(r["data_ref"][:4]), int(r["data_ref"][5:7]), r["preco_m3"], now_str))
        if conn.execute("SELECT changes()").fetchone()[0]: inserted += 1
    conn.commit()
    log.info(f"[ETANOL] {inserted} novas linhas inseridas.")
    return inserted


# ════════════════════════════════════════════════════════════════════════════════
# SUMMARY + MAIN
# ════════════════════════════════════════════════════════════════════════════════
def _summary(conn):
    log.info("=" * 62)
    log.info("RESUMO DO BANCO")
    for table, label, col, unit in [
        ("sugar_ny11",  "Açúcar NY11",       "preco_usdclb", "USDc/lb"),
        ("etanol_cepea","Etanol (UDOP/CEPEA)","preco_brl_m3", "R$/m³"),
        ("fx_usdbrl",   "Câmbio PTAX",        "ptax_venda",   "R$/USD"),
    ]:
        r = conn.execute(f"SELECT COUNT(*), MIN(data_referencia), MAX(data_referencia) FROM {table}").fetchone()
        n, dmin, dmax = r
        last = conn.execute(f"SELECT data_referencia, {col} FROM {table} ORDER BY data_referencia DESC LIMIT 1").fetchone()
        ls = f"{last[1]:.4f} {unit}" if last and last[1] else "—"
        log.info(f"  {label:26}: {n:6} | {dmin or '—'} → {dmax or '—'} | Último: {last[0] if last else '—'} = {ls}")
    log.info("=" * 62)

def main():
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log.info("=" * 62)
    log.info(f"S&E Extractor | Banco: {DB_PATH} | {now_str}")
    log.info("=" * 62)
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = get_conn()
    ensure_schema(conn)

    log.info("--- NY11 ---")
    n1 = fetch_sugar_ny11(conn, now_str)
    time.sleep(1)

    log.info("--- Câmbio PTAX (BCB) ---")
    n2 = fetch_fx_usdbrl(conn, now_str)
    time.sleep(1)

    log.info("--- Etanol (UDOP/CEPEA) ---")
    n3 = fetch_etanol_cepea(conn, now_str)

    _summary(conn)
    conn.close()
    log.info(f"Fim — NY11: {n1} | FX: {n2} | Etanol: {n3}")

if __name__ == "__main__":
    main()
