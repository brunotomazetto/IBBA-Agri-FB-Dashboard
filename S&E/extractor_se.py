#!/usr/bin/env python3
"""
extractor_se.py — IBBA Agri Monitor · S&E Dashboard
=====================================================
Execução diária via GitHub Actions.

Fontes:
  NY11   → Yahoo Finance (SB=F), diário, sem bloqueio
  Etanol → CEPEA/ESALQ via Playwright (Chromium headless)
             Abre a página, aguarda Turnstile resolver (~20s),
             intercepta o download do Excel e insere só os dias novos.
             Histórico inicial: carregado via planilha manual (jan/2010→abr/2026)
             Atualizações: apenas registros posteriores ao último no banco
"""

import io, logging, sqlite3, sys, time
from datetime import date, datetime, timedelta
from pathlib import Path

import requests

try:
    import pandas as pd
except ImportError:
    raise SystemExit("pip install pandas openpyxl xlrd yfinance playwright")

try:
    import yfinance as yf
except ImportError:
    raise SystemExit("pip install yfinance")

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
except ImportError:
    raise SystemExit("pip install playwright && playwright install chromium")

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)

DB_PATH      = Path(__file__).parent / "commodities.db"
DOWNLOAD_DIR = Path(__file__).parent / "_downloads"

YF_TICKER     = "SB=F"
HISTORY_START = "2010-01-01"

CEPEA_URL = "https://www.cepea.esalq.usp.br/br/indicador/etanol.aspx"


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
        fonte TEXT DEFAULT 'CEPEA/ESALQ-Paulinia', updated_at TEXT,
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
# ETANOL — Playwright
# ════════════════════════════════════════════════════════════════════════════════
def fetch_etanol_cepea(conn, now_str):
    last = _last_date(conn, "etanol_cepea")
    log.info(f"[ETANOL] Último no banco: {last or 'nenhum'}")

    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    xls_content = None

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(
                headless=True,
                args=["--no-sandbox","--disable-setuid-sandbox",
                      "--disable-dev-shm-usage","--disable-gpu"],
            )
            context = browser.new_context(
                locale="pt-BR",
                accept_downloads=True,
                viewport={"width": 1280, "height": 800},
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/123.0.0.0 Safari/537.36"
                ),
            )
            page = context.new_page()

            # Intercepta qualquer response que seja Excel
            intercepted = []
            def on_response(resp):
                ct  = resp.headers.get("content-type","")
                url = resp.url
                if ("spreadsheet" in ct or "excel" in ct or "octet-stream" in ct
                        or "widgetpastas" in url or url.endswith(".xls") or url.endswith(".xlsx")):
                    try:
                        body = resp.body()
                        if len(body) > 5000:
                            log.info(f"[ETANOL] Excel interceptado: {url[:80]} ({len(body):,} bytes)")
                            intercepted.append(body)
                    except: pass
            page.on("response", on_response)

            # Navega com domcontentloaded
            log.info(f"[ETANOL] Navegando para {CEPEA_URL}")
            try:
                page.goto(CEPEA_URL, wait_until="domcontentloaded", timeout=60_000)
                log.info("[ETANOL] DOM carregado.")
            except PWTimeout:
                log.warning("[ETANOL] Timeout no goto — continuando...")

            # ── Aguarda 25s para o Turnstile resolver automaticamente ──────────
            # O Cloudflare Turnstile em modo "managed" resolve sem interação
            # humana em browsers reais — leva entre 5-20 segundos
            log.info("[ETANOL] Aguardando Turnstile resolver (25s)...")
            time.sleep(25)

            # Verifica se a página agora tem conteúdo real (não só links CF)
            all_links = page.eval_on_selector_all(
                "a[href]", "els => els.map(e => ({text: e.innerText.trim().slice(0,50), href: e.href}))"
            )
            non_cf_links = [l for l in all_links if "cloudflare.com" not in l["href"]]
            log.info(f"[ETANOL] Links não-Cloudflare na página: {len(non_cf_links)}")
            for l in non_cf_links[:5]:
                log.info(f"  {l['text']!r} → {l['href'][:80]}")

            # Tenta clicar no link de download do Excel
            if not intercepted:
                xls_links = [l for l in non_cf_links
                             if any(k in l["href"].lower() for k in
                                    ["widgetpastas","excel",".xls","download","indicador"])]
                log.info(f"[ETANOL] Links candidatos ao download: {xls_links[:3]}")

                if xls_links:
                    try:
                        log.info(f"[ETANOL] Clicando: {xls_links[0]['href'][:80]}")
                        with page.expect_download(timeout=30_000) as dl_info:
                            page.click(f"a[href='{xls_links[0]['href']}']", timeout=10_000)
                        dl = dl_info.value
                        dl_path = DOWNLOAD_DIR / dl.suggested_filename
                        dl.save_as(dl_path)
                        xls_content = dl_path.read_bytes()
                        log.info(f"[ETANOL] Download via clique: {dl_path.name} ({len(xls_content):,} bytes)")
                    except Exception as e:
                        log.warning(f"[ETANOL] Clique falhou: {e}")

            # Usa Excel interceptado se disponível
            if not xls_content and intercepted:
                xls_content = intercepted[-1]
                log.info("[ETANOL] Usando Excel interceptado da rede.")

            # Screenshot para debug
            try:
                ss = DOWNLOAD_DIR / "cepea_screenshot.png"
                page.screenshot(path=str(ss), full_page=True)
                log.info(f"[ETANOL] Screenshot: {ss}")
            except: pass

            context.close()
            browser.close()

    except Exception as e:
        log.error(f"[ETANOL] Playwright erro: {e}")
        return 0

    if not xls_content:
        log.error("[ETANOL] Nenhum Excel obtido.")
        return 0

    # Valida magic bytes
    is_xlsx = xls_content[:4] == b"PK\x03\x04"
    is_xls  = xls_content[:4] == b"\xd0\xcf\x11\xe0"
    if not (is_xlsx or is_xls):
        log.error(f"[ETANOL] Conteúdo não é Excel. Magic: {xls_content[:8].hex()}")
        (DOWNLOAD_DIR / "debug.bin").write_bytes(xls_content)
        return 0

    # Parse
    try:
        rows = _parse_cepea_excel(xls_content, is_xlsx)
    except Exception as e:
        log.error(f"[ETANOL] Parse erro: {e}"); return 0

    if not rows:
        log.warning("[ETANOL] Planilha sem registros válidos."); return 0

    log.info(f"[ETANOL] {len(rows)} registros | {rows[0]['data_ref']} → {rows[-1]['data_ref']}")

    # Insere só o que é novo
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
    log.info(f"[ETANOL] {inserted} novas linhas inseridas.")
    return inserted


def _parse_cepea_excel(content, is_xlsx):
    """
    Parse da planilha CEPEA. Layout confirmado:
      Col A = Data (DD/MM/YYYY)
      Col B = À vista R$ (R$/m³ → ÷1000 = R$/litro)
    Tolera linhas de cabeçalho descritivo no topo.
    """
    engine = "openpyxl" if is_xlsx else "xlrd"
    buf    = io.BytesIO(content)
    raw    = pd.read_excel(buf, engine=engine, header=None, dtype=str)

    rows = []
    for _, row in raw.iterrows():
        # Busca data em qualquer coluna
        for ci in range(min(4, len(row))):
            dr = _parse_date_br(str(row.iloc[ci]))
            if not dr:
                continue
            # Preço na próxima coluna
            for pi in range(ci+1, min(ci+4, len(row))):
                pv = str(row.iloc[pi]).strip().replace(",",".")
                try:
                    p = float(pv)
                    if p <= 0:
                        continue
                    # Se valor > 100 assume R$/m³ e converte para R$/litro
                    preco_l = round(p / 1000, 6) if p > 100 else p
                    rows.append({"data_ref": dr, "preco": preco_l})
                    break
                except:
                    continue
            break

    rows.sort(key=lambda r: r["data_ref"])
    return rows


# ════════════════════════════════════════════════════════════════════════════════
# SUMMARY + MAIN
# ════════════════════════════════════════════════════════════════════════════════
def _summary(conn):
    log.info("=" * 60)
    log.info("RESUMO DO BANCO")
    for table, label, col, unit in [
        ("sugar_ny11",   "Açúcar NY11",            "preco_usdclb", "USDc/lb"),
        ("etanol_cepea", "Etanol Hidratado CEPEA",  "preco_brl_l",  "R$/l"),
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
    log.info("--- Etanol (Playwright + CEPEA) ---")
    n2 = fetch_etanol_cepea(conn, now_str)

    _summary(conn)
    conn.close()
    log.info(f"Fim — NY11: {n1} | Etanol: {n2}")

if __name__ == "__main__":
    main()
