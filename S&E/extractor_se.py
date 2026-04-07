#!/usr/bin/env python3
"""
extractor_se.py — IBBA Agri Monitor · S&E Dashboard
=====================================================
Execução diária via GitHub Actions.

Fluxo:
  1. Coleta histórico de preço spot de Açúcar NY11 (USDc/lb)
     → Fonte: Yahoo Finance (SB=F — Sugar No. 11 front-month continuous)
  2. Coleta histórico de preço de Etanol Hidratado Carburante
     → Fonte: CEPEA/ESALQ via Playwright (browser real — passa pelo WAF)
  3. Salva tudo em commodities.db (SQLite) dentro da pasta S&E

═══════════════════════════════════════════════════════════════
FONTES
═══════════════════════════════════════════════════════════════

  AÇÚCAR NY11  → Yahoo Finance (yfinance)
                  Ticker  : SB=F
                  Campo   : Close (USDc/lb)
                  Freq.   : diária (dias úteis)

  ETANOL HIDRATADO → CEPEA/ESALQ via Playwright
                  O CEPEA bloqueia IPs de datacenter via WAF (Imperva).
                  Playwright abre um Chromium real que passa pela verificação.
                  Baixa a planilha Excel do indicador Etanol Hidratado (338).
                  Unidade : R$/litro (à vista, posto usina São Paulo)
                  Freq.   : diária (dias úteis)

═══════════════════════════════════════════════════════════════
DEPENDÊNCIAS
═══════════════════════════════════════════════════════════════

  pip install requests pandas openpyxl xlrd yfinance playwright
  playwright install chromium

"""

import io
import logging
import sqlite3
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import requests

try:
    import pandas as pd
except ImportError:
    raise SystemExit("Execute: pip install pandas openpyxl xlrd yfinance playwright")

try:
    import yfinance as yf
except ImportError:
    raise SystemExit("Execute: pip install yfinance")

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
except ImportError:
    raise SystemExit("Execute: pip install playwright && playwright install chromium")

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Caminhos ──────────────────────────────────────────────────────────────────
DB_PATH      = Path(__file__).parent / "commodities.db"
DOWNLOAD_DIR = Path(__file__).parent / "_cepea_downloads"

# ── Config NY11 ───────────────────────────────────────────────────────────────
YF_TICKER     = "SB=F"
HISTORY_START = "2010-01-01"

# ── Config CEPEA ──────────────────────────────────────────────────────────────
CEPEA_ETANOL_URL = "https://www.cepea.esalq.usp.br/br/indicador/etanol.aspx"
# Seletor do botão de download da planilha Excel na página do CEPEA
# (inspecionado via DevTools — link com texto "Excel" ou href widgetpastas)
CEPEA_DOWNLOAD_SELECTOR = "a[href*='widgetpastas'][href*='338']"
# Timeout máximo para o browser carregar a página (ms)
PLAYWRIGHT_TIMEOUT = 60_000


# ════════════════════════════════════════════════════════════════════════════════
# BANCO DE DADOS
# ════════════════════════════════════════════════════════════════════════════════
def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS sugar_ny11 (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        data_referencia TEXT NOT NULL,
        ano             INTEGER,
        mes             INTEGER,
        preco_usdclb    REAL NOT NULL,
        open_usdclb     REAL,
        high_usdclb     REAL,
        low_usdclb      REAL,
        volume          REAL,
        fonte           TEXT DEFAULT 'Yahoo/SB=F',
        updated_at      TEXT,
        UNIQUE(data_referencia)
    );
    CREATE INDEX IF NOT EXISTS idx_sugar_data ON sugar_ny11(data_referencia);

    CREATE TABLE IF NOT EXISTS etanol_cepea (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        data_referencia TEXT NOT NULL,
        ano             INTEGER,
        mes             INTEGER,
        preco_brl_l     REAL NOT NULL,
        fonte           TEXT DEFAULT 'CEPEA/ESALQ',
        updated_at      TEXT,
        UNIQUE(data_referencia)
    );
    CREATE INDEX IF NOT EXISTS idx_etanol_data ON etanol_cepea(data_referencia);
    """)
    conn.commit()
    log.info(f"Schema OK — banco: {DB_PATH}")


# ════════════════════════════════════════════════════════════════════════════════
# HELPERS
# ════════════════════════════════════════════════════════════════════════════════
def _last_date(conn: sqlite3.Connection, table: str) -> str | None:
    row = conn.execute(f"SELECT MAX(data_referencia) FROM {table}").fetchone()
    return row[0] if row and row[0] else None


def _safe_float(val) -> float | None:
    try:
        f = float(val)
        return None if str(f) == "nan" else f
    except (TypeError, ValueError):
        return None


def _parse_date_br(raw: str) -> str | None:
    raw = raw.strip()
    for fmt in ("%d/%m/%Y", "%d/%m/%y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _insert_etanol_rows(
    conn: sqlite3.Connection,
    rows: list[dict],
    last: str | None,
    now_str: str,
) -> int:
    if last:
        rows = [r for r in rows if r["data_ref"] > last]
    if not rows:
        log.info("[ETANOL] Dados já atualizados. Nada a inserir.")
        return 0
    inserted = 0
    for row in rows:
        try:
            conn.execute(
                """INSERT OR IGNORE INTO etanol_cepea
                   (data_referencia, ano, mes, preco_brl_l, updated_at)
                   VALUES (?, ?, ?, ?, ?)""",
                (
                    row["data_ref"],
                    int(row["data_ref"][:4]),
                    int(row["data_ref"][5:7]),
                    float(row["preco"]),
                    now_str,
                ),
            )
            if conn.execute("SELECT changes()").fetchone()[0]:
                inserted += 1
        except Exception as exc:
            log.warning(f"[ETANOL] Erro ao inserir {row['data_ref']}: {exc}")
    conn.commit()
    return inserted


# ════════════════════════════════════════════════════════════════════════════════
# FETCH — AÇÚCAR NY11 (Yahoo Finance · SB=F)
# ════════════════════════════════════════════════════════════════════════════════
def fetch_sugar_ny11(conn: sqlite3.Connection, now_str: str) -> int:
    log.info("[NY11] Buscando dados Yahoo Finance (SB=F)...")

    last  = _last_date(conn, "sugar_ny11")
    start = (
        (datetime.strptime(last, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        if last else HISTORY_START
    )
    today = date.today().strftime("%Y-%m-%d")

    if start > today:
        log.info("[NY11] Dados já atualizados até hoje.")
        return 0

    log.info(f"[NY11] Buscando de {start} até hoje")

    try:
        ticker = yf.Ticker(YF_TICKER)
        df = ticker.history(start=start, end=today, auto_adjust=False)
    except Exception as exc:
        log.error(f"[NY11] Falha yfinance: {exc}")
        return 0

    if df is None or df.empty:
        log.info("[NY11] Nenhum dado novo.")
        return 0

    df.index = pd.to_datetime(df.index).tz_localize(None)

    inserted = 0
    for ts, row in df.iterrows():
        data_ref = ts.strftime("%Y-%m-%d")
        close    = _safe_float(row.get("Close"))
        if close is None:
            continue
        try:
            conn.execute(
                """INSERT OR IGNORE INTO sugar_ny11
                   (data_referencia, ano, mes, preco_usdclb,
                    open_usdclb, high_usdclb, low_usdclb, volume, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    data_ref, int(data_ref[:4]), int(data_ref[5:7]),
                    close,
                    _safe_float(row.get("Open")),
                    _safe_float(row.get("High")),
                    _safe_float(row.get("Low")),
                    _safe_float(row.get("Volume")),
                    now_str,
                ),
            )
            if conn.execute("SELECT changes()").fetchone()[0]:
                inserted += 1
        except Exception as exc:
            log.warning(f"[NY11] Erro ao inserir {data_ref}: {exc}")

    conn.commit()
    log.info(f"[NY11] {inserted} novas linhas inseridas.")
    return inserted


# ════════════════════════════════════════════════════════════════════════════════
# FETCH — ETANOL CEPEA via Playwright
# ════════════════════════════════════════════════════════════════════════════════
def fetch_etanol_cepea(conn: sqlite3.Connection, now_str: str) -> int:
    """
    Usa Playwright (Chromium headless) para:
      1. Abrir a página do indicador Etanol no CEPEA
      2. Aguardar o carregamento completo (passa pelo WAF)
      3. Interceptar o download da planilha Excel
      4. Parsear o Excel e inserir no banco
    """
    log.info("[ETANOL] Iniciando Playwright (Chromium headless)...")

    last = _last_date(conn, "etanol_cepea")
    log.info(f"[ETANOL] Último registro no banco: {last or 'nenhum'}")

    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    xls_content: bytes | None = None

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                ],
            )

            # Contexto com locale pt-BR e download habilitado
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

            # ── Abre a página do CEPEA ─────────────────────────────────────
            log.info(f"[ETANOL] Navegando para {CEPEA_ETANOL_URL}")
            page.goto(CEPEA_ETANOL_URL, wait_until="networkidle", timeout=PLAYWRIGHT_TIMEOUT)
            log.info("[ETANOL] Página carregada.")

            # Pequena pausa humana
            time.sleep(3)

            # ── Localiza o link de download do Excel ──────────────────────
            # Tenta pelo seletor CSS direto; se não achar, busca por texto
            download_link = None
            try:
                download_link = page.locator(CEPEA_DOWNLOAD_SELECTOR).first
                download_link.wait_for(timeout=10_000)
                log.info("[ETANOL] Link de download encontrado via seletor CSS.")
            except Exception:
                log.info("[ETANOL] Seletor CSS não encontrou — tentando por texto 'Excel'...")
                try:
                    download_link = page.get_by_text("Excel", exact=False).first
                    download_link.wait_for(timeout=10_000)
                    log.info("[ETANOL] Link de download encontrado via texto 'Excel'.")
                except Exception:
                    # Último recurso: pega todos os links e filtra por widgetpastas
                    log.info("[ETANOL] Buscando links href com 'widgetpastas'...")
                    hrefs = page.eval_on_selector_all(
                        "a[href]",
                        "els => els.map(e => e.href)"
                    )
                    xls_hrefs = [h for h in hrefs if "widgetpastas" in h or "indicador" in h]
                    log.info(f"[ETANOL] Links encontrados: {xls_hrefs}")
                    if xls_hrefs:
                        # Usa requests com os cookies do Playwright para baixar
                        cookies = context.cookies()
                        xls_content = _download_with_playwright_cookies(
                            xls_hrefs[0], cookies, CEPEA_ETANOL_URL
                        )

            # ── Faz o download clicando no link ───────────────────────────
            if download_link is not None and xls_content is None:
                log.info("[ETANOL] Clicando no link de download...")
                with page.expect_download(timeout=PLAYWRIGHT_TIMEOUT) as dl_info:
                    download_link.click()
                download = dl_info.value
                dl_path  = DOWNLOAD_DIR / download.suggested_filename
                download.save_as(dl_path)
                log.info(f"[ETANOL] Arquivo baixado: {dl_path} ({dl_path.stat().st_size:,} bytes)")
                xls_content = dl_path.read_bytes()

            context.close()
            browser.close()

    except PlaywrightTimeout as exc:
        log.error(f"[ETANOL] Timeout do Playwright: {exc}")
        return 0
    except Exception as exc:
        log.error(f"[ETANOL] Erro no Playwright: {exc}")
        return 0

    if not xls_content:
        log.error("[ETANOL] Nenhum conteúdo Excel obtido.")
        return 0

    # ── Parse do Excel ────────────────────────────────────────────────────────
    log.info(f"[ETANOL] Parseando Excel ({len(xls_content):,} bytes)...")
    try:
        rows = _parse_cepea_excel(xls_content)
    except Exception as exc:
        log.error(f"[ETANOL] Falha ao parsear Excel: {exc}")
        debug = Path(__file__).parent / "debug_cepea.bin"
        debug.write_bytes(xls_content)
        log.info(f"[ETANOL] Salvo para debug: {debug}")
        return 0

    if not rows:
        log.warning("[ETANOL] Planilha vazia ou não reconhecida.")
        return 0

    log.info(f"[ETANOL] {len(rows)} linhas lidas da planilha.")
    inserted = _insert_etanol_rows(conn, rows, last, now_str)
    log.info(f"[ETANOL] {inserted} novas linhas inseridas.")
    return inserted


def _download_with_playwright_cookies(
    url: str,
    pw_cookies: list[dict],
    referer: str,
) -> bytes | None:
    """
    Usa requests com os cookies capturados pelo Playwright para baixar
    diretamente uma URL que requer sessão autenticada.
    """
    log.info(f"[ETANOL] Baixando via cookies Playwright: {url}")
    session = requests.Session()
    for ck in pw_cookies:
        session.cookies.set(ck["name"], ck["value"], domain=ck.get("domain", ""))
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36"
        ),
        "Referer": referer,
        "Accept":  "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,*/*",
    })
    try:
        resp = session.get(url, timeout=60)
        resp.raise_for_status()
        if len(resp.content) > 1000:  # mínimo razoável para um Excel
            return resp.content
        log.warning(f"[ETANOL] Resposta muito pequena ({len(resp.content)} bytes), ignorando.")
    except Exception as exc:
        log.warning(f"[ETANOL] Download via cookies falhou: {exc}")
    return None


# ── Parser do Excel ───────────────────────────────────────────────────────────
def _parse_cepea_excel(content: bytes) -> list[dict]:
    """Lê planilha CEPEA e retorna lista de {'data_ref': str, 'preco': float}."""
    is_xlsx = content[:4] == b"PK\x03\x04"
    engine  = "openpyxl" if is_xlsx else "xlrd"
    buf     = io.BytesIO(content)

    # Lê sem header para localizar linha de cabeçalho real
    raw = pd.read_excel(buf, engine=engine, header=None, dtype=str)

    header_row = 0
    for i, row in raw.iterrows():
        row_str = " ".join(str(v).lower() for v in row.values if pd.notna(v))
        if "data" in row_str and any(k in row_str for k in ["vista", "valor", "preco", "preço", "r$"]):
            header_row = i
            break

    buf.seek(0)
    df = pd.read_excel(buf, engine=engine, header=header_row, dtype=str)
    df.columns = [str(c).strip().lower() for c in df.columns]

    col_data  = next((c for c in df.columns if "data" in c), None)
    col_preco = next(
        (c for c in df.columns if any(k in c for k in ["vista", "valor", "preço", "preco", "r$"])),
        None,
    )

    if not col_data or not col_preco:
        raise ValueError(f"Colunas não encontradas. Disponíveis: {list(df.columns)}")

    rows = []
    for _, row in df.iterrows():
        data_ref = _parse_date_br(str(row[col_data]).strip())
        if not data_ref:
            continue
        try:
            preco = float(str(row[col_preco]).strip().replace(",", "."))
            if preco <= 0:
                continue
        except (ValueError, TypeError):
            continue
        rows.append({"data_ref": data_ref, "preco": preco})

    rows.sort(key=lambda r: r["data_ref"])
    return rows


# ════════════════════════════════════════════════════════════════════════════════
# SUMMARY
# ════════════════════════════════════════════════════════════════════════════════
def _summary(conn: sqlite3.Connection) -> None:
    log.info("=" * 60)
    log.info("RESUMO DO BANCO")
    for table, label, col, unidade in [
        ("sugar_ny11",   "Açúcar NY11",            "preco_usdclb", "USDc/lb"),
        ("etanol_cepea", "Etanol Hidratado CEPEA", "preco_brl_l",  "R$/l"),
    ]:
        try:
            r = conn.execute(
                f"SELECT COUNT(*), MIN(data_referencia), MAX(data_referencia) FROM {table}"
            ).fetchone()
            n, dt_min, dt_max = r
            last = conn.execute(
                f"SELECT data_referencia, {col} FROM {table} "
                f"ORDER BY data_referencia DESC LIMIT 1"
            ).fetchone()
            last_str = f"{last[1]:.4f} {unidade}" if last and last[1] else "—"
            log.info(
                f"  {label:30}: {n:6} registros | "
                f"{dt_min or '—'} → {dt_max or '—'} | "
                f"Último: {last[0] if last else '—'} = {last_str}"
            )
        except Exception as exc:
            log.warning(f"  Erro ao resumir {table}: {exc}")
    log.info("=" * 60)


# ════════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════════
def main() -> None:
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log.info("=" * 60)
    log.info("S&E Extractor — iniciando")
    log.info(f"  Banco : {DB_PATH}")
    log.info(f"  Data  : {now_str}")
    log.info("=" * 60)

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    conn = get_conn()
    ensure_schema(conn)

    log.info("--- Açúcar NY11 (Yahoo Finance · SB=F) ---")
    n_sugar = fetch_sugar_ny11(conn, now_str)

    time.sleep(2)

    log.info("--- Etanol Hidratado CEPEA/ESALQ (Playwright) ---")
    n_etanol = fetch_etanol_cepea(conn, now_str)

    _summary(conn)
    conn.close()

    log.info(f"Coleta finalizada — NY11: {n_sugar} novas | Etanol: {n_etanol} novas")


if __name__ == "__main__":
    main()
