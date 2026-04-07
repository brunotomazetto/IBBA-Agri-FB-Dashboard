#!/usr/bin/env python3
"""
extractor_se.py — IBBA Agri Monitor · S&E Dashboard
=====================================================
Execução diária via GitHub Actions.

Fluxo:
  1. Coleta histórico de preço spot de Açúcar NY11 (USDc/lb)
     → Fonte: Yahoo Finance (SB=F — Sugar No. 11 front-month continuous)
  2. Coleta histórico de preço de Etanol Hidratado Carburante
     → Fonte: CEPEA/ESALQ — API JSON interna (sem bloqueio de datacenter)
              Fallback: download da planilha Excel oficial
  3. Salva tudo em commodities.db (SQLite) dentro da pasta S&E

═══════════════════════════════════════════════════════════════
FONTES
═══════════════════════════════════════════════════════════════

  AÇÚCAR NY11  → Yahoo Finance (yfinance)
                  Ticker  : SB=F
                  Campo   : Close (USDc/lb)
                  Freq.   : diária (dias úteis)

  ETANOL HIDRATADO → CEPEA/ESALQ
                  Método 1: API JSON  esalqlog.esalq.usp.br  (preferido)
                  Método 2: API JSON  cepea.esalq.usp.br/api  (fallback)
                  Método 3: planilha Excel widgetpastas       (último recurso)
                  Unidade : R$/litro (à vista, posto usina São Paulo)
                  Freq.   : diária (dias úteis)

═══════════════════════════════════════════════════════════════
ESTRUTURA DO BANCO (commodities.db)
═══════════════════════════════════════════════════════════════

  sugar_ny11   : preços diários NY11 (USDc/lb)
  etanol_cepea : preços diários etanol hidratado CEPEA (R$/l)

"""

import io
import json
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

# ── Config NY11 ───────────────────────────────────────────────────────────────
YF_TICKER     = "SB=F"
HISTORY_START = "2010-01-01"

# ── Config CEPEA ──────────────────────────────────────────────────────────────
# Método 1 — API JSON interna usada pelo próprio widget do site CEPEA
# Retorna série completa em JSON sem autenticação
CEPEA_JSON_URL_1 = (
    "https://esalqlog.esalq.usp.br/RecebeIndicadorCepea"
    "?indicador_id=338&produto_id=17"
)
# Método 2 — endpoint alternativo documentado em projetos open-source que
# consultam o CEPEA (ex: pycepea, agrotools)
CEPEA_JSON_URL_2 = (
    "https://www.cepea.esalq.usp.br/br/indicador/etanol/ind_etanol.json.js"
)
# Método 3 — planilha Excel (funciona localmente, pode ser bloqueada em CI)
CEPEA_XLS_URL    = "https://www.cepea.esalq.usp.br/br/widgetpastas/17/indicador/338.aspx"
CEPEA_REFERER    = "https://www.cepea.esalq.usp.br/br/indicador/etanol.aspx"


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
    """Insere lista de {'data_ref': str, 'preco': float} filtrando já existentes."""
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
# FETCH — ETANOL CEPEA  (cadeia de métodos)
# ════════════════════════════════════════════════════════════════════════════════
def fetch_etanol_cepea(conn: sqlite3.Connection, now_str: str) -> int:
    last = _last_date(conn, "etanol_cepea")
    log.info(f"[ETANOL] Último registro no banco: {last or 'nenhum'}")

    # ── Método 1: API JSON esalqlog ───────────────────────────────────────────
    rows = _cepea_method_json_esalqlog()
    if rows:
        log.info(f"[ETANOL] Método 1 (esalqlog JSON): {len(rows)} registros brutos")
        inserted = _insert_etanol_rows(conn, rows, last, now_str)
        log.info(f"[ETANOL] {inserted} novas linhas inseridas.")
        return inserted

    # ── Método 2: JSON alternativo cepea.esalq.usp.br ────────────────────────
    log.info("[ETANOL] Método 1 falhou — tentando Método 2 (JSON alternativo)...")
    rows = _cepea_method_json_alt()
    if rows:
        log.info(f"[ETANOL] Método 2 (JSON alt): {len(rows)} registros brutos")
        inserted = _insert_etanol_rows(conn, rows, last, now_str)
        log.info(f"[ETANOL] {inserted} novas linhas inseridas.")
        return inserted

    # ── Método 3: planilha Excel ──────────────────────────────────────────────
    log.info("[ETANOL] Método 2 falhou — tentando Método 3 (Excel)...")
    rows = _cepea_method_excel()
    if rows:
        log.info(f"[ETANOL] Método 3 (Excel): {len(rows)} registros brutos")
        inserted = _insert_etanol_rows(conn, rows, last, now_str)
        log.info(f"[ETANOL] {inserted} novas linhas inseridas.")
        return inserted

    log.error("[ETANOL] Todos os métodos falharam. Verifique conectividade com o CEPEA.")
    return 0


# ── Método 1 — API JSON esalqlog (usada internamente pelo widget CEPEA) ───────
def _cepea_method_json_esalqlog() -> list[dict]:
    """
    Endpoint JSON interno do CEPEA descoberto via DevTools no site deles.
    Retorna array de objetos com campos Data e Preco (ou variantes).
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/123.0.0.0",
        "Accept":     "application/json, text/javascript, */*",
        "Referer":    CEPEA_REFERER,
        "Origin":     "https://www.cepea.esalq.usp.br",
    }
    try:
        resp = requests.get(CEPEA_JSON_URL_1, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return _parse_cepea_json(data)
    except Exception as exc:
        log.warning(f"[ETANOL] Método 1 falhou: {exc}")
        return []


# ── Método 2 — JSON alternativo ───────────────────────────────────────────────
def _cepea_method_json_alt() -> list[dict]:
    """
    Arquivo .json.js servido pelo CEPEA para o gráfico do indicador.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/123.0.0.0",
        "Accept":     "application/json, text/javascript, */*",
        "Referer":    CEPEA_REFERER,
    }
    try:
        resp = requests.get(CEPEA_JSON_URL_2, headers=headers, timeout=30)
        resp.raise_for_status()
        # Remove possível wrapper de função JS: callback({...}) → {...}
        text = resp.text.strip()
        if text.startswith("("):
            text = text[1:]
        if text.endswith(")"):
            text = text[:-1]
        # Tenta também remover prefixo de callback nomeado
        if "(" in text[:30]:
            text = text[text.index("(") + 1:]
            if text.endswith(")"):
                text = text[:-1]
        data = json.loads(text)
        return _parse_cepea_json(data)
    except Exception as exc:
        log.warning(f"[ETANOL] Método 2 falhou: {exc}")
        return []


# ── Método 3 — Excel via sessão com cookies ───────────────────────────────────
def _cepea_method_excel() -> list[dict]:
    """Download da planilha Excel com sessão autenticada por cookies."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "pt-BR,pt;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection":      "keep-alive",
    })
    try:
        # Obtém cookies visitando a página principal
        session.get(CEPEA_REFERER, timeout=30)
        time.sleep(2)
        resp = session.get(
            CEPEA_XLS_URL,
            headers={"Referer": CEPEA_REFERER},
            timeout=60,
        )
        resp.raise_for_status()
    except Exception as exc:
        log.warning(f"[ETANOL] Método 3 (Excel) falhou no download: {exc}")
        return []

    # Verifica magic bytes
    is_xlsx = resp.content[:4] == b"PK\x03\x04"
    is_xls  = resp.content[:4] == b"\xd0\xcf\x11\xe0"
    if not (is_xlsx or is_xls):
        log.warning(f"[ETANOL] Método 3: resposta não é Excel ({len(resp.content)} bytes)")
        debug = Path(__file__).parent / "debug_cepea.bin"
        debug.write_bytes(resp.content)
        log.info(f"[ETANOL] Salvo para debug: {debug}")
        return []

    try:
        return _parse_cepea_excel(resp.content, is_xlsx)
    except Exception as exc:
        log.warning(f"[ETANOL] Método 3: falha ao parsear Excel: {exc}")
        return []


# ── Parsers ───────────────────────────────────────────────────────────────────
def _parse_cepea_json(data) -> list[dict]:
    """
    Aceita múltiplos formatos de resposta JSON do CEPEA:
      - Lista de dicts: [{"Data": "01/01/2024", "Preco": 3.5}, ...]
      - Dict com chave de dados: {"data": [...], "values": [...]}
      - Lista de listas: [["01/01/2024", 3.5], ...]
    """
    rows = []

    # Normaliza para lista
    if isinstance(data, dict):
        # Procura a primeira chave que seja lista
        for key in ("data", "Data", "values", "series", "itens", "items"):
            if key in data and isinstance(data[key], list):
                data = data[key]
                break
        else:
            # Tenta a primeira chave com lista
            for v in data.values():
                if isinstance(v, list) and len(v) > 10:
                    data = v
                    break

    if not isinstance(data, list):
        log.warning(f"[ETANOL] JSON inesperado: tipo={type(data)}")
        return []

    for item in data:
        # Formato dict
        if isinstance(item, dict):
            # Chave de data
            raw_date = None
            for k in ("Data", "data", "date", "dt", "Dt", "DATE"):
                if k in item:
                    raw_date = str(item[k])
                    break
            # Chave de preço (preferência: à vista)
            raw_preco = None
            for k in ("Preco", "preco", "price", "Price",
                       "valor", "Valor", "close", "Close",
                       "AVistaReal", "a_vista_real", "AVista"):
                if k in item and item[k] not in (None, "", "-"):
                    raw_preco = item[k]
                    break

        # Formato lista [data, preco]
        elif isinstance(item, (list, tuple)) and len(item) >= 2:
            raw_date  = str(item[0])
            raw_preco = item[1]
        else:
            continue

        data_ref = _parse_date_br(raw_date) if raw_date else None
        if not data_ref:
            continue
        try:
            preco = float(str(raw_preco).replace(",", "."))
            if preco <= 0:
                continue
        except (ValueError, TypeError):
            continue

        rows.append({"data_ref": data_ref, "preco": preco})

    # Ordena por data
    rows.sort(key=lambda r: r["data_ref"])
    return rows


def _parse_cepea_excel(content: bytes, is_xlsx: bool) -> list[dict]:
    """Lê planilha Excel CEPEA e retorna lista de {'data_ref', 'preco'}."""
    engine = "openpyxl" if is_xlsx else "xlrd"
    buf    = io.BytesIO(content)
    raw    = pd.read_excel(buf, engine=engine, header=None, dtype=str)

    # Localiza linha de cabeçalho
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
        raise ValueError(f"Colunas não encontradas: {list(df.columns)}")

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

    log.info("--- Etanol Hidratado CEPEA/ESALQ ---")
    n_etanol = fetch_etanol_cepea(conn, now_str)

    _summary(conn)
    conn.close()

    log.info(f"Coleta finalizada — NY11: {n_sugar} novas | Etanol: {n_etanol} novas")


if __name__ == "__main__":
    main()
