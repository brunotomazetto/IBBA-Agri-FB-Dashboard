#!/usr/bin/env python3
"""
extractor_se.py — IBBA Agri Monitor · S&E Dashboard
=====================================================
Execução diária via GitHub Actions.

Fluxo:
  1. Coleta histórico de preço spot de Açúcar NY11 (USDc/lb)
     → Fonte: Yahoo Finance (SB=F — Sugar No. 11 front-month)
  2. Coleta histórico de preço de Etanol Hidratado ao produtor (R$/l)
     → Fonte: UNICAdata/UNICA — endpoint xlsPrcProd.php
               Dados CEPEA republicados pela UNICA (mesma série)
               Preço diário CIF Paulínia-SP, sem frete, sem impostos
               Testado e confirmado sem bloqueio em GitHub Actions

═══════════════════════════════════════════════════════════════
BANCO: S&E/commodities.db
  sugar_ny11   — preços diários NY11 (USDc/lb)
  etanol_cepea — preços diários etanol hidratado (R$/l)
═══════════════════════════════════════════════════════════════
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
    raise SystemExit("Execute: pip install pandas openpyxl xlrd yfinance")

try:
    import yfinance as yf
except ImportError:
    raise SystemExit("Execute: pip install yfinance")

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

# ── Config Etanol — UNICAdata ─────────────────────────────────────────────────
# Endpoint Excel confirmado em testes no GitHub Actions (200 OK, sem WAF)
# idTabela=2405 → Etanol Hidratado Combustível, frequência diária, Paulínia-SP
# Fonte primária: CEPEA/ESALQ republicado pela UNICA
UNICA_XLS_URL = (
    "https://unicadata.com.br/xlsPrcProd.php"
    "?idioma=1"
    "&tipoHistorico=7"
    "&idTabela=2405"
    "&estado=Paulinia"
    "&produto=Etanol+hidratado+combust%C3%ADvel"
    "&frequencia=Di%C3%A1rio"
)
UNICA_REFERER = (
    "https://unicadata.com.br/preco-ao-produtor.php"
    "?idMn=42&tipoHistorico=7&acao=visualizar"
    "&idTabela=2405&produto=Etanol+hidratado+combust%C3%ADvel"
    "&frequencia=Di%C3%A1rio&estado=Paulinia"
)
# Nota: esse endpoint retorna os ÚLTIMOS ~20 dias úteis.
# Para histórico completo, usamos também idTabela=1433 (mensal SP)
# e idTabela=2487 (semanal SP) como complemento, mas o diário já
# cobre as atualizações incrementais após a carga inicial.
UNICA_XLS_URL_HISTORICO = (
    "https://unicadata.com.br/xlsPrcProd.php"
    "?idioma=1"
    "&tipoHistorico=7"
    "&idTabela=1433"
    "&estado=S%C3%A3o+Paulo"
    "&produto=Etanol+hidratado+combust%C3%ADvel"
    "&frequencia=Mensal"
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "pt-BR,pt;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
}


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
        fonte           TEXT DEFAULT 'UNICA/CEPEA-Paulinia',
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
    """Converte data para YYYY-MM-DD. Aceita DD/MM/YYYY, YYYY-MM-DD, etc."""
    raw = str(raw).strip()
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
    """Insere apenas registros novos (posterior ao último no banco)."""
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
    """
    Coleta histórico diário do NY11 via yfinance (ticker SB=F).
    Incremental: busca apenas a partir do dia seguinte ao último registro.
    """
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
        df = yf.Ticker(YF_TICKER).history(start=start, end=today, auto_adjust=False)
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
# FETCH — ETANOL HIDRATADO (UNICAdata · xlsPrcProd.php)
# ════════════════════════════════════════════════════════════════════════════════
def fetch_etanol_cepea(conn: sqlite3.Connection, now_str: str) -> int:
    """
    Coleta o preço diário do Etanol Hidratado ao produtor via UNICAdata.

    Estratégia:
      - Se banco vazio → baixa tabela diária (últimos ~20 dias úteis)
        O histórico mais antigo precisa ser carregado uma única vez via
        execução local ou ajuste de parâmetros (ver UNICA_XLS_URL_HISTORICO).
      - Se banco já tem dados → baixa tabela diária (incremental, últimos 20 dias)
        Isso é suficiente para atualizações diárias normais.

    Nota sobre o histórico completo:
      O endpoint diário retorna apenas os últimos ~20 dias úteis.
      Para carregar o histórico desde 2010, execute uma vez localmente:
        python extractor_se.py --carga-historica
      Isso baixa a tabela mensal (desde 2002) como base e depois aplica
      o diário por cima.
    """
    import sys
    carga_historica = "--carga-historica" in sys.argv

    last = _last_date(conn, "etanol_cepea")
    log.info(f"[ETANOL] Último registro no banco: {last or 'nenhum'}")

    total_inserted = 0

    # ── Carga histórica (mensal desde 2002) ───────────────────────────────────
    if carga_historica or last is None:
        log.info("[ETANOL] Baixando histórico mensal (UNICAdata idTabela=1433)...")
        rows_hist = _download_unica_xls(UNICA_XLS_URL_HISTORICO, "histórico mensal")
        if rows_hist:
            n = _insert_etanol_rows(conn, rows_hist, last, now_str)
            log.info(f"[ETANOL] Histórico mensal: {n} linhas inseridas.")
            total_inserted += n
            last = _last_date(conn, "etanol_cepea")

    # ── Atualização diária (últimos ~20 dias úteis) ───────────────────────────
    log.info("[ETANOL] Baixando dados diários (UNICAdata idTabela=2405)...")
    rows_daily = _download_unica_xls(UNICA_XLS_URL, "diário Paulínia")
    if rows_daily:
        n = _insert_etanol_rows(conn, rows_daily, last, now_str)
        log.info(f"[ETANOL] Diário: {n} novas linhas inseridas.")
        total_inserted += n
    else:
        log.error("[ETANOL] Falha ao baixar dados diários da UNICAdata.")

    return total_inserted


def _download_unica_xls(url: str, label: str) -> list[dict]:
    """
    Baixa e parseia uma planilha Excel da UNICAdata.
    Retorna lista de {'data_ref': 'YYYY-MM-DD', 'preco': float}.
    """
    hdrs = {
        **HEADERS,
        "Referer": UNICA_REFERER,
        "Accept": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,*/*",
    }
    try:
        resp = requests.get(url, headers=hdrs, timeout=30)
        resp.raise_for_status()
    except Exception as exc:
        log.error(f"[ETANOL] Download '{label}' falhou: {exc}")
        return []

    content = resp.content
    log.info(f"[ETANOL] '{label}': {len(content):,} bytes baixados")

    is_xlsx = content[:4] == b"PK\x03\x04"
    is_xls  = content[:4] == b"\xd0\xcf\x11\xe0"
    if not (is_xlsx or is_xls):
        log.error(f"[ETANOL] '{label}': resposta não é Excel. Magic: {content[:8].hex()}")
        return []

    try:
        return _parse_unica_excel(content, is_xlsx, label)
    except Exception as exc:
        log.error(f"[ETANOL] '{label}': falha no parse: {exc}")
        return []


def _parse_unica_excel(content: bytes, is_xlsx: bool, label: str) -> list[dict]:
    """
    Parseia planilha da UNICAdata.

    Layout típico:
      Linhas 0-6 : cabeçalho descritivo
      Linha 7+   : DATA | VALOR (R$/l)

    O diagnóstico confirmou:
      row 5: ['Preço recebido pelo produtor -']
      row 7: ['Unidade: R$/l']

    Estratégia: localiza a primeira linha com uma data válida no col 0.
    """
    engine = "openpyxl" if is_xlsx else "xlrd"
    buf    = io.BytesIO(content)
    raw    = pd.read_excel(buf, engine=engine, header=None, dtype=str)

    rows = []
    for _, row in raw.iterrows():
        # Coluna 0 deve ser data, coluna 1 deve ser preço
        raw_date  = str(row.iloc[0]).strip() if len(row) > 0 else ""
        raw_preco = str(row.iloc[1]).strip() if len(row) > 1 else ""

        data_ref = _parse_date_br(raw_date)
        if not data_ref:
            continue  # linha de cabeçalho ou rodapé

        try:
            preco = float(raw_preco.replace(",", "."))
            if preco <= 0:
                continue
        except (ValueError, TypeError):
            continue

        rows.append({"data_ref": data_ref, "preco": preco})

    rows.sort(key=lambda r: r["data_ref"])
    log.info(f"[ETANOL] '{label}': {len(rows)} registros válidos parseados")
    if rows:
        log.info(f"[ETANOL] Período: {rows[0]['data_ref']} → {rows[-1]['data_ref']}")
    return rows


# ════════════════════════════════════════════════════════════════════════════════
# SUMMARY
# ════════════════════════════════════════════════════════════════════════════════
def _summary(conn: sqlite3.Connection) -> None:
    log.info("=" * 60)
    log.info("RESUMO DO BANCO")
    for table, label, col, unidade in [
        ("sugar_ny11",   "Açúcar NY11",            "preco_usdclb", "USDc/lb"),
        ("etanol_cepea", "Etanol Hidratado UNICA",  "preco_brl_l",  "R$/l"),
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
                f"  {label:28}: {n:6} registros | "
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

    time.sleep(1)

    log.info("--- Etanol Hidratado (UNICAdata · CEPEA Paulínia) ---")
    n_etanol = fetch_etanol_cepea(conn, now_str)

    _summary(conn)
    conn.close()

    log.info(f"Coleta finalizada — NY11: {n_sugar} novas | Etanol: {n_etanol} novas")


if __name__ == "__main__":
    main()
