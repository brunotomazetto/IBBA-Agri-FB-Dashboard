#!/usr/bin/env python3
"""
extractor_se.py — IBBA Agri Monitor · S&E Dashboard
=====================================================
Execução diária via GitHub Actions.

Fluxo:
  1. Coleta histórico de preço spot de Açúcar NY11 (USDc/lb)
     → Fonte: Yahoo Finance (SB=F — Sugar No. 11 front-month continuous)
  2. Coleta histórico de preço de Etanol Hidratado Carburante
     → Fonte: CEPEA/ESALQ — download da planilha Excel oficial
  3. Salva tudo em commodities.db (SQLite) dentro da pasta S&E

═══════════════════════════════════════════════════════════════
FONTES
═══════════════════════════════════════════════════════════════

  AÇÚCAR NY11  → Yahoo Finance (yfinance)
                  Ticker  : SB=F  (ICE Sugar No. 11 front-month)
                  Campo   : Close (preço de fechamento em USDc/lb)
                  Unidade : USDc/lb
                  Freq.   : diária (dias úteis)
                  Sem API key — gratuito e sem restrição de acesso

  ETANOL HIDRATADO → CEPEA/ESALQ — Indicador Etanol Hidratado
                  Produto : 17 / Indicador : 338 (Hidratado Carburante)
                  Unidade : R$/litro (à vista, posto usina São Paulo)
                  Freq.   : diária (dias úteis)

═══════════════════════════════════════════════════════════════
ESTRUTURA DO BANCO (commodities.db)
═══════════════════════════════════════════════════════════════

  sugar_ny11   : preços diários NY11 (USDc/lb)
  etanol_cepea : preços diários etanol hidratado CEPEA (R$/l)

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
    raise SystemExit("pandas não instalado. Execute: pip install pandas openpyxl xlrd yfinance")

try:
    import yfinance as yf
except ImportError:
    raise SystemExit("yfinance não instalado. Execute: pip install yfinance")

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Caminhos ──────────────────────────────────────────────────────────────────
DB_PATH = Path(__file__).parent / "commodities.db"

# ── Config ────────────────────────────────────────────────────────────────────
YF_TICKER     = "SB=F"
HISTORY_START = "2010-01-01"

CEPEA_DOWNLOAD_URL = "https://www.cepea.esalq.usp.br/br/widgetpastas/17/indicador/338.aspx"
CEPEA_REFERER_URL  = "https://www.cepea.esalq.usp.br/br/indicador/etanol.aspx"

MAX_RETRIES = 3
RETRY_DELAY = 5


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


# ════════════════════════════════════════════════════════════════════════════════
# FETCH — AÇÚCAR NY11 (Yahoo Finance · SB=F)
# ════════════════════════════════════════════════════════════════════════════════
def fetch_sugar_ny11(conn: sqlite3.Connection, now_str: str) -> int:
    """
    Coleta histórico do NY11 via yfinance (ticker SB=F).
    Gratuito, sem API key, sem bloqueio em ambientes CI/CD.
    Close = preço de fechamento em USDc/lb.
    """
    log.info("[NY11] Buscando dados Yahoo Finance (SB=F)...")

    last  = _last_date(conn, "sugar_ny11")
    start = (
        (datetime.strptime(last, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        if last else HISTORY_START
    )
    today = date.today().strftime("%Y-%m-%d")

    if start > today:
        log.info("[NY11] Dados já atualizados até hoje. Nada a fazer.")
        return 0

    log.info(f"[NY11] Buscando de {start} até hoje")

    try:
        ticker = yf.Ticker(YF_TICKER)
        df = ticker.history(start=start, end=today, auto_adjust=False)
    except Exception as exc:
        log.error(f"[NY11] Falha ao buscar yfinance: {exc}")
        return 0

    if df is None or df.empty:
        log.info("[NY11] Nenhum dado novo retornado.")
        return 0

    # Normaliza índice tz-aware → naive date string
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
# FETCH — ETANOL HIDRATADO CEPEA/ESALQ
# ════════════════════════════════════════════════════════════════════════════════
def fetch_etanol_cepea(conn: sqlite3.Connection, now_str: str) -> int:
    """
    Baixa a planilha Excel do CEPEA (Indicador Etanol Hidratado).
    Usa sessão com cookies + headers completos de browser para evitar 403.
    """
    log.info("[ETANOL] Buscando planilha CEPEA/ESALQ...")

    last = _last_date(conn, "etanol_cepea")
    log.info(f"[ETANOL] Último registro no banco: {last or 'nenhum'}")

    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36"
        ),
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;q=0.9,"
            "image/avif,image/webp,image/apng,*/*;q=0.8"
        ),
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection":      "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    })

    # Passo 1 — visita a página de indicador para pegar cookies de sessão
    try:
        log.info("[ETANOL] Obtendo cookies de sessão...")
        session.get(CEPEA_REFERER_URL, timeout=30)
        time.sleep(2)
    except Exception as exc:
        log.warning(f"[ETANOL] Aviso ao obter cookies (continuando): {exc}")

    # Passo 2 — download do Excel com Referer correto
    try:
        log.info("[ETANOL] Baixando planilha Excel...")
        resp = session.get(
            CEPEA_DOWNLOAD_URL,
            headers={"Referer": CEPEA_REFERER_URL},
            timeout=60,
        )
        resp.raise_for_status()
    except Exception as exc:
        log.error(f"[ETANOL] Falha ao baixar planilha: {exc}")
        return 0

    content_type = resp.headers.get("Content-Type", "")
    log.info(f"[ETANOL] Content-Type: {content_type} | Tamanho: {len(resp.content):,} bytes")

    # Verifica magic bytes para confirmar que é um Excel
    is_xlsx = resp.content[:4] == b"PK\x03\x04"
    is_xls  = resp.content[:4] == b"\xd0\xcf\x11\xe0"

    if not (is_xlsx or is_xls or "spreadsheet" in content_type or "excel" in content_type):
        log.error(
            f"[ETANOL] Resposta não é Excel. "
            f"Primeiros 200 bytes: {resp.content[:200]}"
        )
        debug_path = Path(__file__).parent / "debug_cepea_response.bin"
        debug_path.write_bytes(resp.content)
        log.info(f"[ETANOL] Arquivo salvo para debug: {debug_path}")
        return 0

    # Parse da planilha
    try:
        df = _parse_cepea_excel(resp.content, is_xlsx)
    except Exception as exc:
        log.error(f"[ETANOL] Falha ao parsear Excel: {exc}")
        return 0

    if df is None or df.empty:
        log.warning("[ETANOL] Planilha vazia ou não reconhecida.")
        return 0

    log.info(f"[ETANOL] {len(df)} linhas lidas da planilha.")

    if last:
        df = df[df["data_ref"] > last]

    if df.empty:
        log.info("[ETANOL] Dados já atualizados. Nada a inserir.")
        return 0

    inserted = 0
    for _, row in df.iterrows():
        data_ref = row["data_ref"]
        preco    = row["preco"]
        if not data_ref or preco is None:
            continue
        try:
            conn.execute(
                """INSERT OR IGNORE INTO etanol_cepea
                   (data_referencia, ano, mes, preco_brl_l, updated_at)
                   VALUES (?, ?, ?, ?, ?)""",
                (data_ref, int(data_ref[:4]), int(data_ref[5:7]), float(preco), now_str),
            )
            if conn.execute("SELECT changes()").fetchone()[0]:
                inserted += 1
        except Exception as exc:
            log.warning(f"[ETANOL] Erro ao inserir {data_ref}: {exc}")

    conn.commit()
    log.info(f"[ETANOL] {inserted} novas linhas inseridas.")
    return inserted


def _parse_cepea_excel(content: bytes, is_xlsx: bool) -> "pd.DataFrame | None":
    """
    Lê planilha CEPEA e retorna DataFrame com ['data_ref', 'preco'].
    Tolerante a cabeçalhos descritivos e variações de layout.
    """
    engine = "openpyxl" if is_xlsx else "xlrd"
    buf    = io.BytesIO(content)

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
        log.error(f"[ETANOL] Colunas não identificadas. Disponíveis: {list(df.columns)}")
        return None

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

    if not rows:
        return None

    return pd.DataFrame(rows).sort_values("data_ref").reset_index(drop=True)


def _parse_date_br(raw: str) -> "str | None":
    for fmt in ("%d/%m/%Y", "%d/%m/%y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


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
            r    = conn.execute(
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

    log.info("--- Etanol Hidratado CEPEA/ESALQ ---")
    n_etanol = fetch_etanol_cepea(conn, now_str)

    _summary(conn)
    conn.close()

    log.info(f"Coleta finalizada — NY11: {n_sugar} novas | Etanol: {n_etanol} novas")


if __name__ == "__main__":
    main()
