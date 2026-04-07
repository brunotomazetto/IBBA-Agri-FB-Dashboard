#!/usr/bin/env python3
"""
extractor_se.py — IBBA Agri Monitor · S&E Dashboard
=====================================================
Execução diária via GitHub Actions.

Fluxo:
  1. Coleta histórico de preço spot de Açúcar NY11 (USDc/lb)
     → Fonte: Nasdaq Data Link (CHRIS/ICE_SB1 — ICE Sugar No. 11 Continuous)
  2. Coleta histórico de preço de Etanol Hidratado Carburante
     → Fonte: CEPEA/ESALQ — download da planilha Excel oficial
  3. Salva tudo em commodities.db (SQLite) dentro da pasta S&E

═══════════════════════════════════════════════════════════════
FONTES
═══════════════════════════════════════════════════════════════

  AÇÚCAR NY11  → Nasdaq Data Link (antigo Quandl)
                  Dataset : CHRIS/ICE_SB1
                  Série   : Settle (preço de fechamento contrato front-month)
                  Unidade : USDc/lb
                  Freq.   : diária (dias úteis)
                  API Key : env var NASDAQ_API_KEY (gratuita em data.nasdaq.com)

  ETANOL HIDRATADO → CEPEA/ESALQ — Indicador Etanol Hidratado
                  URL     : https://www.cepea.esalq.usp.br/br/indicador/etanol.aspx
                  Planilha: download XLS/XLSX via POST (form do site)
                  Unidade : R$/litro (à vista, posto usina São Paulo)
                  Freq.   : diária (dias úteis)

═══════════════════════════════════════════════════════════════
VARIÁVEIS DE AMBIENTE NECESSÁRIAS
═══════════════════════════════════════════════════════════════

  NASDAQ_API_KEY   → Chave gratuita em https://data.nasdaq.com/sign-up
                     Sem a chave, a coleta do NY11 é pulada com aviso.

═══════════════════════════════════════════════════════════════
ESTRUTURA DO BANCO (commodities.db)
═══════════════════════════════════════════════════════════════

  sugar_ny11   : preços diários NY11 (USDc/lb)
  etanol_cepea : preços diários etanol hidratado CEPEA (R$/l)

"""

import io
import logging
import os
import sqlite3
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import requests

# ── Dependências extras (pandas + openpyxl para o Excel do CEPEA) ─────────────
try:
    import pandas as pd
except ImportError:
    raise SystemExit("pandas não instalado. Execute: pip install pandas openpyxl xlrd")

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Caminhos ──────────────────────────────────────────────────────────────────
# Salva dentro da pasta S&E no mesmo repositório
DB_PATH = Path(__file__).parent / "commodities.db"

# ── Credenciais / Config ───────────────────────────────────────────────────────
NASDAQ_API_KEY = os.getenv("NASDAQ_API_KEY", "")

# Nasdaq Data Link — ICE Sugar No. 11 Continuous (front-month)
# Coluna "Settle" = preço de fechamento em USDc/lb
NASDAQ_NY11_URL = (
    "https://data.nasdaq.com/api/v3/datasets/CHRIS/ICE_SB1.json"
)

# CEPEA — Indicador Etanol Hidratado Carburante (posto usina SP, R$/litro)
# O site usa um POST com parâmetro de data para gerar o download Excel
CEPEA_ETANOL_URL = (
    "https://www.cepea.esalq.usp.br/br/indicador/etanol.aspx"
)
CEPEA_DOWNLOAD_URL = (
    "https://www.cepea.esalq.usp.br/br/widgetpastas/17/indicador/338.aspx"
)

# Data de início da coleta histórica (não buscar mais antigo que isso)
HISTORY_START = "2010-01-01"

# Retry config
MAX_RETRIES = 3
RETRY_DELAY = 5  # segundos


# ════════════════════════════════════════════════════════════════════════════════
# BANCO DE DADOS
# ════════════════════════════════════════════════════════════════════════════════
def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript("""
    -- ── Açúcar NY11 (ICE Sugar No. 11) ─────────────────────────────────────
    CREATE TABLE IF NOT EXISTS sugar_ny11 (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        data_referencia TEXT NOT NULL,   -- YYYY-MM-DD
        ano            INTEGER,
        mes            INTEGER,
        preco_usdclb   REAL NOT NULL,    -- USDc/lb (Settle)
        open_usdclb    REAL,
        high_usdclb    REAL,
        low_usdclb     REAL,
        volume         REAL,
        fonte          TEXT DEFAULT 'Nasdaq/ICE_SB1',
        updated_at     TEXT,
        UNIQUE(data_referencia)
    );
    CREATE INDEX IF NOT EXISTS idx_sugar_data
        ON sugar_ny11(data_referencia);

    -- ── Etanol Hidratado Carburante CEPEA/ESALQ ──────────────────────────────
    CREATE TABLE IF NOT EXISTS etanol_cepea (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        data_referencia TEXT NOT NULL,   -- YYYY-MM-DD
        ano            INTEGER,
        mes            INTEGER,
        preco_brl_l    REAL NOT NULL,    -- R$/litro (à vista posto usina SP)
        fonte          TEXT DEFAULT 'CEPEA/ESALQ',
        updated_at     TEXT,
        UNIQUE(data_referencia)
    );
    CREATE INDEX IF NOT EXISTS idx_etanol_data
        ON etanol_cepea(data_referencia);
    """)
    conn.commit()
    log.info(f"Schema OK — banco: {DB_PATH}")


# ════════════════════════════════════════════════════════════════════════════════
# HELPERS
# ════════════════════════════════════════════════════════════════════════════════
def _last_date_in_table(conn: sqlite3.Connection, table: str) -> str | None:
    """Retorna a data mais recente já gravada na tabela (ou None se vazia)."""
    row = conn.execute(
        f"SELECT MAX(data_referencia) FROM {table}"
    ).fetchone()
    return row[0] if row and row[0] else None


def _request_with_retry(
    method: str,
    url: str,
    **kwargs,
) -> requests.Response:
    """GET/POST com retry automático em caso de erro temporário."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.request(method, url, timeout=60, **kwargs)
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            log.warning(f"  Tentativa {attempt}/{MAX_RETRIES} falhou: {exc}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY * attempt)
    raise RuntimeError(f"Falha após {MAX_RETRIES} tentativas: {url}")


# ════════════════════════════════════════════════════════════════════════════════
# FETCH — AÇÚCAR NY11 (Nasdaq Data Link · CHRIS/ICE_SB1)
# ════════════════════════════════════════════════════════════════════════════════
def fetch_sugar_ny11(conn: sqlite3.Connection, now_str: str) -> int:
    """
    Coleta série histórica de fechamento do NY11 via Nasdaq Data Link.

    Campos retornados pela API (coluna order do dataset CHRIS/ICE_SB1):
      Date | Open | High | Low | Settle | Change | Wave | Volume | Prev. Day OI

    Armazena: Date, Open, High, Low, Settle (→ preco_usdclb), Volume.

    Estratégia incremental: se já há dados no banco, busca apenas
    a partir do dia seguinte ao último registro.
    """
    if not NASDAQ_API_KEY:
        log.error(
            "[NY11] NASDAQ_API_KEY não definida. "
            "Crie uma chave gratuita em https://data.nasdaq.com/sign-up "
            "e adicione como secret no GitHub Actions."
        )
        return 0

    log.info("[NY11] Buscando dados Nasdaq Data Link (CHRIS/ICE_SB1)...")

    last = _last_date_in_table(conn, "sugar_ny11")
    start_date = (
        (datetime.strptime(last, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        if last else HISTORY_START
    )

    if start_date > date.today().strftime("%Y-%m-%d"):
        log.info("[NY11] Dados já atualizados até hoje. Nada a fazer.")
        return 0

    log.info(f"[NY11] Buscando de {start_date} até hoje")

    params = {
        "api_key":   NASDAQ_API_KEY,
        "start_date": start_date,
        "order":      "asc",          # cronológico
    }

    try:
        resp = _request_with_retry("GET", NASDAQ_NY11_URL, params=params)
        payload = resp.json()
    except Exception as exc:
        log.error(f"[NY11] Falha na requisição: {exc}")
        return 0

    dataset   = payload.get("dataset", {})
    col_names = [c.lower() for c in dataset.get("column_names", [])]
    data_rows = dataset.get("data", [])

    if not data_rows:
        log.info("[NY11] Nenhum dado novo retornado pela API.")
        return 0

    # Mapear índices das colunas relevantes
    try:
        idx_date   = col_names.index("date")
        idx_settle = col_names.index("settle")
        idx_open   = col_names.index("open")   if "open"   in col_names else None
        idx_high   = col_names.index("high")   if "high"   in col_names else None
        idx_low    = col_names.index("low")    if "low"    in col_names else None
        idx_vol    = col_names.index("volume") if "volume" in col_names else None
    except ValueError as exc:
        log.error(f"[NY11] Coluna inesperada no dataset: {exc}. Colunas: {col_names}")
        return 0

    inserted = 0
    for row in data_rows:
        data_ref = str(row[idx_date])[:10]
        settle   = row[idx_settle]

        if settle is None:
            continue  # dia sem fechamento (feriado/dia sem liquidez)

        open_v  = row[idx_open]  if idx_open  is not None else None
        high_v  = row[idx_high]  if idx_high  is not None else None
        low_v   = row[idx_low]   if idx_low   is not None else None
        vol_v   = row[idx_vol]   if idx_vol   is not None else None

        try:
            conn.execute(
                """INSERT OR IGNORE INTO sugar_ny11
                   (data_referencia, ano, mes, preco_usdclb,
                    open_usdclb, high_usdclb, low_usdclb, volume, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    data_ref,
                    int(data_ref[:4]),
                    int(data_ref[5:7]),
                    float(settle),
                    float(open_v) if open_v is not None else None,
                    float(high_v) if high_v is not None else None,
                    float(low_v)  if low_v  is not None else None,
                    float(vol_v)  if vol_v  is not None else None,
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
    Coleta o indicador de Etanol Hidratado Carburante do CEPEA/ESALQ.

    O CEPEA disponibiliza a série em planilha Excel via URL direta.
    A planilha contém duas colunas: Data | Valor (R$/litro, à vista posto usina SP).

    Estratégia:
      1. Baixa a planilha Excel completa (histórico total)
      2. Filtra apenas registros novos (posterior ao último no banco)
      3. Insere os novos registros

    URL do arquivo: varia por produto; o ID 338 corresponde ao
    Indicador Etanol Hidratado (CEPEA/ESALQ - Hidratado).
    """
    log.info("[ETANOL] Buscando planilha CEPEA/ESALQ...")

    last = _last_date_in_table(conn, "etanol_cepea")
    log.info(f"[ETANOL] Último registro no banco: {last or 'nenhum'}")

    # ── Download da planilha ──────────────────────────────────────────────────
    # O CEPEA usa um formulário ASP.NET com ViewState. Para evitar complexidade
    # de parsing do ViewState, usamos o link direto da planilha de indicadores
    # que o CEPEA disponibiliza publicamente.
    #
    # URL do widget de download — produto 17 (Etanol), indicador 338 (Hidratado)
    # Retorna arquivo .xlsx com histórico completo.
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Referer": CEPEA_ETANOL_URL,
        "Accept": (
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,"
            "application/vnd.ms-excel,*/*"
        ),
    }

    try:
        resp = _request_with_retry("GET", CEPEA_DOWNLOAD_URL, headers=headers)
    except Exception as exc:
        log.error(f"[ETANOL] Falha ao baixar planilha CEPEA: {exc}")
        return 0

    content_type = resp.headers.get("Content-Type", "")
    log.info(f"[ETANOL] Content-Type recebido: {content_type}")

    # ── Parse da planilha ─────────────────────────────────────────────────────
    try:
        df = _parse_cepea_excel(resp.content)
    except Exception as exc:
        log.error(f"[ETANOL] Falha ao parsear Excel CEPEA: {exc}")
        # Tenta salvar o arquivo para debug
        debug_path = Path(__file__).parent / "debug_cepea_etanol.bin"
        debug_path.write_bytes(resp.content)
        log.info(f"[ETANOL] Arquivo salvo para debug: {debug_path}")
        return 0

    if df is None or df.empty:
        log.warning("[ETANOL] Planilha vazia ou não reconhecida.")
        return 0

    log.info(f"[ETANOL] {len(df)} linhas lidas da planilha.")

    # ── Filtrar apenas registros novos ────────────────────────────────────────
    if last:
        df = df[df["data_ref"] > last]

    if df.empty:
        log.info("[ETANOL] Dados já atualizados. Nada a inserir.")
        return 0

    # ── Inserir no banco ──────────────────────────────────────────────────────
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
                (
                    data_ref,
                    int(data_ref[:4]),
                    int(data_ref[5:7]),
                    float(preco),
                    now_str,
                ),
            )
            if conn.execute("SELECT changes()").fetchone()[0]:
                inserted += 1
        except Exception as exc:
            log.warning(f"[ETANOL] Erro ao inserir {data_ref}: {exc}")

    conn.commit()
    log.info(f"[ETANOL] {inserted} novas linhas inseridas.")
    return inserted


def _parse_cepea_excel(content: bytes) -> "pd.DataFrame | None":
    """
    Lê o Excel do CEPEA e retorna DataFrame com colunas ['data_ref', 'preco'].

    O CEPEA geralmente entrega planilhas com:
      - Algumas linhas de cabeçalho/rodapé descritivas
      - Colunas: 'Data' (DD/MM/YYYY) | 'À Vista(R$)' ou similar
    Esta função é tolerante a variações de layout.
    """
    buf = io.BytesIO(content)

    # Tenta xlsx primeiro, depois xls
    for engine in ("openpyxl", "xlrd"):
        try:
            raw = pd.read_excel(buf, engine=engine, header=None)
            break
        except Exception:
            buf.seek(0)
            continue
    else:
        raise ValueError("Não foi possível ler o Excel com openpyxl nem xlrd.")

    # ── Localizar linha de cabeçalho ──────────────────────────────────────────
    # Procura pela primeira linha que contenha "data" em alguma célula
    header_row = None
    for i, row in raw.iterrows():
        row_str = " ".join(str(v).lower() for v in row.values if pd.notna(v))
        if "data" in row_str and ("vista" in row_str or "valor" in row_str or "preço" in row_str or "preco" in row_str):
            header_row = i
            break

    if header_row is None:
        # Fallback: assume que a primeira linha é o header
        header_row = 0

    df = pd.read_excel(
        io.BytesIO(content),
        engine=engine,
        header=header_row,
        dtype=str,
    )

    # ── Normalizar nomes de colunas ───────────────────────────────────────────
    df.columns = [str(c).strip().lower() for c in df.columns]

    # Coluna de data
    col_data = next(
        (c for c in df.columns if "data" in c),
        None,
    )
    # Coluna de preço: "à vista", "valor", "preço", etc.
    col_preco = next(
        (c for c in df.columns if any(k in c for k in ["vista", "valor", "preço", "preco", "r$"])),
        None,
    )

    if col_data is None or col_preco is None:
        log.error(f"[ETANOL] Colunas não identificadas. Disponíveis: {list(df.columns)}")
        return None

    # ── Converter tipos ───────────────────────────────────────────────────────
    result_rows = []
    for _, row in df.iterrows():
        raw_data  = str(row[col_data]).strip()
        raw_preco = str(row[col_preco]).strip().replace(",", ".")

        # Parse de data: DD/MM/YYYY ou YYYY-MM-DD
        data_ref = _parse_date_br(raw_data)
        if data_ref is None:
            continue  # linha de cabeçalho/rodapé

        # Parse de preço
        try:
            preco = float(raw_preco)
            if preco <= 0:
                continue
        except (ValueError, TypeError):
            continue

        result_rows.append({"data_ref": data_ref, "preco": preco})

    if not result_rows:
        return None

    out = pd.DataFrame(result_rows).sort_values("data_ref").reset_index(drop=True)
    return out


def _parse_date_br(raw: str) -> "str | None":
    """
    Converte string de data para YYYY-MM-DD.
    Aceita: DD/MM/YYYY, DD/MM/YY, YYYY-MM-DD, e variantes.
    Retorna None se não conseguir parsear.
    """
    raw = raw.strip()
    for fmt in ("%d/%m/%Y", "%d/%m/%y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


# ════════════════════════════════════════════════════════════════════════════════
# SUMMARY HELPERS
# ════════════════════════════════════════════════════════════════════════════════
def _summary(conn: sqlite3.Connection) -> None:
    """Loga um resumo dos dados no banco após a coleta."""
    log.info("=" * 60)
    log.info("RESUMO DO BANCO")

    for table, label, col_preco, unidade in [
        ("sugar_ny11",   "Açúcar NY11",            "preco_usdclb", "USDc/lb"),
        ("etanol_cepea", "Etanol Hidratado CEPEA", "preco_brl_l",  "R$/l"),
    ]:
        try:
            row = conn.execute(
                f"""SELECT COUNT(*), MIN(data_referencia), MAX(data_referencia),
                           MIN({col_preco}), MAX({col_preco}), AVG({col_preco})
                    FROM {table}"""
            ).fetchone()
            n, dt_min, dt_max, v_min, v_max, v_avg = row
            last_row = conn.execute(
                f"SELECT data_referencia, {col_preco} FROM {table} "
                f"ORDER BY data_referencia DESC LIMIT 1"
            ).fetchone()
            last_dt, last_v = (last_row[0], last_row[1]) if last_row else ("—", None)
            last_str = f"{last_v:.4f} {unidade}" if last_v else "—"
            log.info(
                f"  {label:30}: {n:6} registros | "
                f"{dt_min} → {dt_max} | "
                f"Último: {last_dt} = {last_str}"
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
    log.info(f"  Banco   : {DB_PATH}")
    log.info(f"  Data    : {now_str}")
    log.info("=" * 60)

    # Garante que a pasta existe
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    conn = get_conn()
    ensure_schema(conn)

    # ── 1. Açúcar NY11 ────────────────────────────────────────────────────────
    log.info("--- Açúcar NY11 (Nasdaq Data Link) ---")
    n_sugar = fetch_sugar_ny11(conn, now_str)

    # Pequena pausa entre fontes
    time.sleep(2)

    # ── 2. Etanol Hidratado CEPEA ─────────────────────────────────────────────
    log.info("--- Etanol Hidratado CEPEA/ESALQ ---")
    n_etanol = fetch_etanol_cepea(conn, now_str)

    # ── Resumo ────────────────────────────────────────────────────────────────
    _summary(conn)
    conn.close()

    log.info(f"Coleta finalizada — NY11: {n_sugar} novas | Etanol: {n_etanol} novas")


if __name__ == "__main__":
    main()
