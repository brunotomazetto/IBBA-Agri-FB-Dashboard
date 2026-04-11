#!/usr/bin/env python3
"""
extractor_crushing_spread.py — Agri Monitor · Soy Crushing Spread
==================================================================
Calcula o crushing spread de soja para biodiesel no RS e no MT.

Fórmula (por tonelada de soja processada):
    Spread = (P_farelo × 0.77) + (P_biodiesel × 0.19) - (P_soja × 1000/60)

    P_soja      → R$/sc 60kg  — API CONAB precos ao produtor (RS e MT)
    P_farelo    → USD/kg FOB convertido para R$/ton via PTAX
                  CSV bulk MDIC/SECEX, NCM 23040090, filtrado por porto (URF)
    P_biodiesel → R$/m³       — ANP produtores B100 (Sul e Centro-Oeste)

Fontes (mesma abordagem dos outros extractors do projeto):
    CONAB preco soja  → API REST portaldeinformacoes.conab.gov.br (semanal)
    SECEX farelo      → CSV bulk balanca.economia.gov.br por ano (mensal)
    BCB PTAX          → API REST olinda.bcb.gov.br (diário)
    ANP biodiesel     → XLS download gov.br (semanal, ~12 dias defasagem)

Schedules (GitHub Actions — um único workflow):
    Quinta + Sexta  → CONAB soja + ANP biodiesel + PTAX + Spread
    Dias 8–15/mês   → SECEX farelo + PTAX + Spread
    Cada seção verifica freshness antes de buscar (retry automático).

Uso:
    python extractor_crushing_spread.py             # rodada normal
    python extractor_crushing_spread.py --force-all # ignora freshness
"""

import io
import logging
import sqlite3
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────

DB_DIR        = Path(__file__).parent
DB_PATH       = DB_DIR / "crushing_spread.db"
HISTORY_START = date(2013, 1, 1)
TODAY         = date.today()
NOW_STR       = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
FORCE_ALL     = False

# Freshness — dias sem atualização antes de tentar nova busca
SOJA_STALE_DAYS   = 5    # semanal
BIO_STALE_DAYS    = 10   # ANP publica ~12 dias após fechamento da semana
FARELO_STALE_DAYS = 25   # mensal

# Fatores de conversão do esmagamento
FATOR_FARELO    = 0.77
FATOR_BIODIESEL = 0.19
CONV_SC60_TON   = 1000 / 60  # R$/sc60kg → R$/ton

# ── CONAB — API preços ao produtor ───────────────────────────────────────────
# Mesmo endpoint usado no extractor_imea.py
CONAB_API = "https://portaldeinformacoes.conab.gov.br/index.php/api"

# Produtos e UFs de interesse (nomenclatura exata da API CONAB)
CONAB_SOJA_CONFIG = [
    {"uf": "RS", "produto": "SOJA EM GRÃOS   (60 kg)", "nivel": "PRODUTOR"},
    {"uf": "MT", "produto": "SOJA EM GRÃOS   (60 kg)", "nivel": "PRODUTOR"},
]

# ── SECEX — CSV bulk por ano (mesmo padrão do extractor_secex.py) ────────────
SECEX_BASE_URL = "https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/EXP_{ano}.csv"
FARELO_NCM     = 23040090   # Farelo e resíduos da extração de óleo de soja (int)

# Códigos URF dos portos de interesse (coluna CO_URF no CSV)
# Santos = 0809200 | Rio Grande = 0912600
URF_CONFIG = {
    "Santos":     "0809200",
    "Rio Grande": "0912600",
}

# ── BCB PTAX ─────────────────────────────────────────────────────────────────
BCB_PTAX_URL = (
    "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/"
    "CotacaoDolarPeriodo(dataInicial=@di,dataFinalCotacao=@df)"
    "?@di='{di}'&@df='{df}'&$top=1000&$orderby=dataHoraCotacao%20asc"
    "&$format=json&$select=cotacaoVenda,dataHoraCotacao"
)

# ── ANP — XLS preços biodiesel B100 produtor ─────────────────────────────────
# URL confirmada na página da ANP (arquivo .xls)
ANP_BIODIESEL_URL = (
    "https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/"
    "ppidp/precos-medios-ponderados-semanais-2013.xls"
)
ANP_REGIOES = {"SUL", "CENTRO-OESTE"}

# Mapeamento regional para o cálculo do spread
REGIAO_MAP = {
    "RS": {"uf": "RS", "porto": "Rio Grande", "bio": "SUL"},
    "MT": {"uf": "MT", "porto": "Santos",     "bio": "CENTRO-OESTE"},
}


# ─────────────────────────────────────────────────────────────────────────────
# Freshness check
# ─────────────────────────────────────────────────────────────────────────────

def is_stale(conn, table, stale_days, date_col="data_referencia", where=""):
    if FORCE_ALL:
        return True
    clause = f"WHERE {where}" if where else ""
    r = conn.execute(f"SELECT MAX({date_col}) FROM {table} {clause}").fetchone()
    last = r[0] if r and r[0] else None
    if not last:
        log.info(f"  [{table}] vazio — buscando historico completo")
        return True
    last_dt = datetime.strptime(str(last)[:10], "%Y-%m-%d").date()
    age = (TODAY - last_dt).days
    log.info(
        f"  [{table}] ultimo: {str(last)[:10]} ({age}d, limiar={stale_days}d) "
        f"→ {'DESATUALIZADO' if age > stale_days else 'OK'}"
    )
    return age > stale_days


# ─────────────────────────────────────────────────────────────────────────────
# DB helpers
# ─────────────────────────────────────────────────────────────────────────────

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_schema(conn):
    conn.executescript("""
    -- Preço soja ao produtor — API CONAB (semanal)
    CREATE TABLE IF NOT EXISTS soja_conab (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        data_referencia TEXT NOT NULL,
        uf              TEXT NOT NULL,
        produto_conab   TEXT NOT NULL,
        nivel           TEXT NOT NULL,
        preco_brl_kg    REAL,
        preco_brl_sc60  REAL,
        fonte           TEXT DEFAULT 'CONAB/API',
        updated_at      TEXT,
        UNIQUE(data_referencia, uf, produto_conab, nivel)
    );
    CREATE INDEX IF NOT EXISTS idx_soja_data ON soja_conab(data_referencia);
    CREATE INDEX IF NOT EXISTS idx_soja_uf   ON soja_conab(uf);

    -- Cambio PTAX — BCB (diario)
    CREATE TABLE IF NOT EXISTS fx_ptax (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        data_referencia TEXT NOT NULL UNIQUE,
        ptax_venda      REAL NOT NULL,
        fonte           TEXT DEFAULT 'BCB/PTAX',
        updated_at      TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_ptax ON fx_ptax(data_referencia);

    -- Farelo de soja FOB — CSV SECEX/MDIC (mensal, por porto)
    CREATE TABLE IF NOT EXISTS farelo_secex (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        ano          INTEGER NOT NULL,
        mes          INTEGER NOT NULL,
        porto        TEXT NOT NULL,
        co_urf       TEXT NOT NULL,
        kg_liquido   REAL,
        vl_fob_usd   REAL,
        preco_usd_kg REAL,
        fonte        TEXT DEFAULT 'SECEX/MDIC',
        updated_at   TEXT,
        UNIQUE(ano, mes, porto)
    );
    CREATE INDEX IF NOT EXISTS idx_farelo ON farelo_secex(ano, mes);

    -- Biodiesel B100 produtor — ANP (semanal)
    CREATE TABLE IF NOT EXISTS biodiesel_anp (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        data_inicial TEXT NOT NULL,
        data_final   TEXT NOT NULL,
        regiao       TEXT NOT NULL,
        preco_brl_m3 REAL,
        fonte        TEXT DEFAULT 'ANP/Produtores',
        updated_at   TEXT,
        UNIQUE(data_inicial, regiao)
    );
    CREATE INDEX IF NOT EXISTS idx_bio ON biodiesel_anp(data_inicial);

    -- Spread calculado
    CREATE TABLE IF NOT EXISTS crushing_spread (
        id                 INTEGER PRIMARY KEY AUTOINCREMENT,
        data_referencia    TEXT NOT NULL,
        regiao             TEXT NOT NULL,
        preco_soja_sc60    REAL,
        preco_soja_ton     REAL,
        preco_farelo_usdkg REAL,
        preco_farelo_ton   REAL,
        ptax               REAL,
        preco_bio_m3       REAL,
        receita_farelo     REAL,
        receita_biodiesel  REAL,
        custo_soja         REAL,
        spread_brl_ton     REAL,
        updated_at         TEXT,
        UNIQUE(data_referencia, regiao)
    );
    CREATE INDEX IF NOT EXISTS idx_spread ON crushing_spread(data_referencia);
    """)
    conn.commit()


def last_date(conn, table, col="data_referencia", where=""):
    clause = f"WHERE {where}" if where else ""
    r = conn.execute(f"SELECT MAX({col}) FROM {table} {clause}").fetchone()
    return r[0] if r and r[0] else None


def safe_float(val):
    try:
        f = float(str(val).replace(",", ".").strip())
        return None if str(f) in ("nan", "inf", "-inf") else f
    except Exception:
        return None


def parse_date_br(raw):
    for fmt in ("%d/%m/%Y", "%d/%m/%y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(str(raw).strip(), fmt).strftime("%Y-%m-%d")
        except Exception:
            continue
    return None


def _last_value(value_map, sorted_keys, target):
    """Forward-fill: retorna o último valor disponível em ou antes de target."""
    for k in reversed(sorted_keys):
        if k <= target:
            return value_map.get(k)
    return None


# ─────────────────────────────────────────────────────────────────────────────
# HTTP helper
# ─────────────────────────────────────────────────────────────────────────────

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "pt-BR,pt;q=0.9",
}


def download(url, label, fatal=True, extra_headers=None):
    hdrs = {**HEADERS, **(extra_headers or {})}
    for attempt in range(1, 4):
        try:
            log.info(f"[{label}] Download (tentativa {attempt}): {url}")
            r = requests.get(url, headers=hdrs, timeout=120, verify=False)
            r.raise_for_status()
            log.info(f"[{label}] {len(r.content):,} bytes")
            return r.content
        except requests.RequestException as e:
            log.warning(f"[{label}] Tentativa {attempt} falhou: {e}")
            if attempt < 3:
                time.sleep(10 * attempt)
    msg = f"[{label}] Todas as tentativas falharam."
    if fatal:
        raise RuntimeError(msg)
    log.error(msg)
    return None


# ─────────────────────────────────────────────────────────────────────────────
# SEÇÃO 1 — Soja CONAB (API REST — mesmo padrão do extractor_imea.py)
# ─────────────────────────────────────────────────────────────────────────────

def run_soja(conn):
    log.info("=" * 60)
    log.info("Soja — API CONAB preços ao produtor (RS e MT)")
    log.info("=" * 60)

    if not is_stale(conn, "soja_conab", SOJA_STALE_DAYS):
        log.info("[Soja] Dado fresco — pulando.")
        return {"skipped": True}

    total_inserted = 0
    for cfg in CONAB_SOJA_CONFIG:
        uf      = cfg["uf"]
        produto = cfg["produto"]
        nivel   = cfg["nivel"]

        ld = last_date(conn, "soja_conab", where=f"uf='{uf}'")
        log.info(f"[Soja-{uf}] Último dado no DB: {ld or 'nenhum'}")

        try:
            r = requests.get(
                f"{CONAB_API}/produto/serie-historica",
                params={"produto": produto, "nivel": nivel, "uf": uf},
                timeout=60,
                verify=False,
            )
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            log.error(f"[Soja-{uf}] API CONAB falhou: {e}")
            continue

        pontos = data.get("data", [])
        log.info(f"[Soja-{uf}] {len(pontos)} pontos retornados pela API")

        inserted = 0
        for pt in pontos:
            data_ref = str(pt.get("data") or "")[:10]
            valor_kg = safe_float(pt.get("valor"))
            if not data_ref or not valor_kg or valor_kg <= 0:
                continue
            if ld and data_ref <= ld:
                continue

            conn.execute(
                "INSERT OR IGNORE INTO soja_conab "
                "(data_referencia, uf, produto_conab, nivel, "
                " preco_brl_kg, preco_brl_sc60, updated_at) "
                "VALUES (?,?,?,?,?,?,?)",
                (data_ref, uf, produto, nivel,
                 valor_kg, round(valor_kg * 60, 4), NOW_STR),
            )
            if conn.execute("SELECT changes()").fetchone()[0]:
                inserted += 1

        conn.commit()
        log.info(f"[Soja-{uf}] {inserted} linhas inseridas.")
        total_inserted += inserted
        time.sleep(0.5)

    return {"inserido": total_inserted}


# ─────────────────────────────────────────────────────────────────────────────
# SEÇÃO 2 — PTAX BCB (incremental — mesmo padrão dos outros extractors)
# ─────────────────────────────────────────────────────────────────────────────

def run_ptax(conn):
    log.info("=" * 60)
    log.info("Cambio PTAX — BCB (incremental)")
    log.info("=" * 60)

    ld    = last_date(conn, "fx_ptax")
    start = (
        (datetime.strptime(ld, "%Y-%m-%d") + timedelta(days=1)).date()
        if ld else HISTORY_START
    )
    if start > TODAY:
        log.info("[PTAX] Ja atualizado.")
        return {"inserido": 0}

    inserted = 0
    current  = start
    while current <= TODAY:
        end_chunk = min(date(current.year, 12, 31), TODAY)
        url = BCB_PTAX_URL.format(
            di=current.strftime("%m-%d-%Y"),
            df=end_chunk.strftime("%m-%d-%Y"),
        )
        try:
            r    = requests.get(url, timeout=30)
            r.raise_for_status()
            data = r.json().get("value", [])
        except Exception as e:
            log.error(f"[PTAX] Falha em {current.year}: {e}")
            break

        for item in data:
            raw_dt = str(item.get("dataHoraCotacao", ""))[:10]
            ptax   = item.get("cotacaoVenda")
            if not raw_dt or ptax is None:
                continue
            conn.execute(
                "INSERT OR IGNORE INTO fx_ptax "
                "(data_referencia, ptax_venda, updated_at) VALUES (?,?,?)",
                (raw_dt, float(ptax), NOW_STR),
            )
            if conn.execute("SELECT changes()").fetchone()[0]:
                inserted += 1

        log.info(f"[PTAX] {current.year}: {len(data)} registros")
        current = date(current.year + 1, 1, 1)
        time.sleep(0.3)

    conn.commit()
    log.info(f"[PTAX] {inserted} linhas inseridas.")
    return {"inserido": inserted}


# ─────────────────────────────────────────────────────────────────────────────
# SEÇÃO 3 — Farelo SECEX via CSV bulk (mesmo padrão do extractor_secex.py)
# ─────────────────────────────────────────────────────────────────────────────

def run_farelo(conn):
    log.info("=" * 60)
    log.info("Farelo de Soja FOB — CSV SECEX/MDIC (Santos e Rio Grande)")
    log.info("=" * 60)

    # Freshness check via data sintética YYYY-MM-01
    r = conn.execute(
        "SELECT MAX(printf('%04d-%02d-01', ano, mes)) FROM farelo_secex"
    ).fetchone()
    last_ym = r[0] if r and r[0] else None

    if not FORCE_ALL and last_ym:
        last_dt = datetime.strptime(last_ym, "%Y-%m-%d").date()
        age     = (TODAY - last_dt).days
        log.info(
            f"  [farelo_secex] ultimo: {last_ym[:7]} ({age}d, "
            f"limiar={FARELO_STALE_DAYS}d) → "
            f"{'DESATUALIZADO' if age > FARELO_STALE_DAYS else 'OK'}"
        )
        if age <= FARELO_STALE_DAYS:
            log.info("[Farelo] Dado fresco — pulando.")
            return {"skipped": True}

    # Descobre a partir de qual ano buscar (mesmo padrão do extractor_secex.py)
    r2 = conn.execute(
        "SELECT MAX(ano) FROM farelo_secex"
    ).fetchone()
    ultimo_ano = int(r2[0]) if r2 and r2[0] else None

    if ultimo_ano:
        anos = list(range(ultimo_ano, TODAY.year + 1))
        log.info(f"[Farelo] Atualização incremental desde {ultimo_ano}")
    else:
        anos = list(range(HISTORY_START.year, TODAY.year + 1))
        log.info(f"[Farelo] Carga histórica desde {HISTORY_START.year}")

    total_inserted = 0

    for ano in anos:
        url = SECEX_BASE_URL.format(ano=ano)
        log.info(f"[Farelo] Baixando CSV {ano}...")

        try:
            r_http = requests.get(url, stream=True, verify=False, timeout=120)
            if r_http.status_code != 200:
                log.warning(f"[Farelo] {ano} nao disponivel (status {r_http.status_code})")
                continue

            df = pd.read_csv(
                io.StringIO(r_http.content.decode("latin1")),
                sep=";",
                dtype={"CO_NCM": int, "CO_URF": str},
            )

            # Filtra NCM de farelo de soja
            df = df[df["CO_NCM"] == FARELO_NCM].copy()
            if df.empty:
                log.info(f"[Farelo] {ano}: NCM {FARELO_NCM} nao encontrado")
                continue

            # Filtra pelos portos de interesse e agrega por mes/porto
            for porto, urf_code in URF_CONFIG.items():
                df_porto = df[df["CO_URF"] == urf_code].copy()
                if df_porto.empty:
                    log.info(f"[Farelo] {ano}/{porto}: sem dados")
                    continue

                df_agg = (
                    df_porto.groupby(["CO_ANO", "CO_MES"])[["VL_FOB", "KG_LIQUIDO"]]
                    .sum()
                    .reset_index()
                )

                for _, row in df_agg.iterrows():
                    a   = int(row["CO_ANO"])
                    m   = int(row["CO_MES"])
                    kg  = safe_float(row["KG_LIQUIDO"])
                    fob = safe_float(row["VL_FOB"])
                    if not kg or kg == 0 or not fob:
                        continue

                    conn.execute(
                        "INSERT OR REPLACE INTO farelo_secex "
                        "(ano, mes, porto, co_urf, kg_liquido, vl_fob_usd, "
                        " preco_usd_kg, updated_at) "
                        "VALUES (?,?,?,?,?,?,?,?)",
                        (a, m, porto, urf_code, kg, fob,
                         round(fob / kg, 6), NOW_STR),
                    )
                    if conn.execute("SELECT changes()").fetchone()[0]:
                        total_inserted += 1

            conn.commit()
            log.info(f"[Farelo] {ano}: processado")

        except Exception as e:
            log.error(f"[Farelo] Erro ao processar {ano}: {e}")

    log.info(f"[Farelo] {total_inserted} registros inseridos/atualizados.")
    return {"inserido": total_inserted}


# ─────────────────────────────────────────────────────────────────────────────
# SEÇÃO 4 — Biodiesel ANP
# ─────────────────────────────────────────────────────────────────────────────

def run_biodiesel(conn):
    log.info("=" * 60)
    log.info("Biodiesel B100 — ANP Produtores (Sul e Centro-Oeste)")
    log.info("=" * 60)

    if not is_stale(conn, "biodiesel_anp", BIO_STALE_DAYS, date_col="data_inicial"):
        log.info("[Biodiesel] Dado fresco — pulando.")
        return {"skipped": True}

    ld      = last_date(conn, "biodiesel_anp", col="data_inicial")
    content = download(ANP_BIODIESEL_URL, "biodiesel-anp", fatal=True)
    df      = _parse_anp_biodiesel(content)

    if df.empty or "regiao" not in df.columns:
        log.warning("[Biodiesel] Parser nao retornou dados com coluna 'regiao'.")
        return {"inserido": 0}

    df = df[df["regiao"].isin(ANP_REGIOES)]
    if ld:
        df = df[df["data_inicial"] > ld]
    if df.empty:
        log.info("[Biodiesel] Nenhum dado novo.")
        return {"inserido": 0}

    inserted = 0
    for _, row in df.iterrows():
        conn.execute(
            "INSERT OR IGNORE INTO biodiesel_anp "
            "(data_inicial, data_final, regiao, preco_brl_m3, updated_at) "
            "VALUES (?,?,?,?,?)",
            (row["data_inicial"], row["data_final"],
             row["regiao"], row["preco"], NOW_STR),
        )
        if conn.execute("SELECT changes()").fetchone()[0]:
            inserted += 1

    conn.commit()
    log.info(f"[Biodiesel] {inserted} linhas inseridas.")
    return {"inserido": inserted}


def _parse_anp_biodiesel(content):
    """
    Parseia a planilha .xls da ANP.
    O arquivo tem cabecalho institucional nas primeiras linhas.
    Varre todas as linhas ate encontrar o cabecalho real de dados.
    """
    try:
        xl = pd.ExcelFile(io.BytesIO(content), engine="xlrd")
        log.info(f"[Biodiesel] Abas disponiveis: {xl.sheet_names}")
    except Exception as e:
        raise RuntimeError(f"[Biodiesel] Nao foi possivel abrir .xls: {e}")

    all_rows = []

    for sheet in xl.sheet_names:
        try:
            raw = xl.parse(sheet, header=None)
        except Exception:
            continue

        # Loga as primeiras 15 linhas para diagnostico
        log.info(f"[Biodiesel] Aba '{sheet}' — primeiras 15 linhas:")
        for i, row in raw.head(15).iterrows():
            vals = [str(v) for v in row.values if pd.notna(v) and str(v).strip()]
            if vals:
                log.info(f"  linha {i}: {vals}")

        # Varre TODAS as linhas buscando o cabecalho real
        header_row = None
        for i, row in raw.iterrows():
            vals_upper = [str(v).upper() for v in row.values if pd.notna(v)]
            row_str    = " ".join(vals_upper)
            # Cabecalho real tem combinacoes de: DATA/SEMANA + REGIAO/PRODUTO + PRECO/VALOR
            if (
                ("INICIAL" in row_str and "FINAL" in row_str)
                or ("SEMANA" in row_str and ("PREC" in row_str or "REGI" in row_str))
                or ("DATA" in row_str and "REGI" in row_str and "PREC" in row_str)
                or ("PRODUTO" in row_str and "REGI" in row_str and "PREC" in row_str)
                or ("B100" in row_str and "REGI" in row_str)
            ):
                header_row = i
                log.info(f"[Biodiesel] Cabecalho encontrado na linha {i}: {vals_upper}")
                break

        if header_row is None:
            log.warning(f"[Biodiesel] Aba '{sheet}': cabecalho nao encontrado — pulando")
            continue

        df = xl.parse(sheet, header=header_row)
        df.columns = [str(c).strip().upper() for c in df.columns]
        df = df.dropna(how="all")
        log.info(f"[Biodiesel] Aba '{sheet}': colunas = {list(df.columns)}")

        if not df.empty:
            log.info(f"[Biodiesel] Primeira linha de dados: {dict(df.iloc[0])}")

        # Identifica colunas por nome
        di_col    = next((c for c in df.columns if "INICIAL" in c), None)
        df_col    = next((c for c in df.columns if "FINAL"   in c), None)
        reg_col   = next((c for c in df.columns if "REGI"    in c), None)
        prod_col  = next((c for c in df.columns if "PRODUTO" in c), None)
        preco_col = next(
            (c for c in df.columns
             if any(k in c for k in ("PRECO", "VALOR", "M3", "PONDERADO"))),
            None
        )

        log.info(
            f"[Biodiesel] Mapeamento: di={di_col}, df={df_col}, "
            f"reg={reg_col}, prod={prod_col}, preco={preco_col}"
        )

        if not all([di_col, df_col, preco_col]):
            log.warning(f"[Biodiesel] Aba '{sheet}': colunas insuficientes — pulando")
            continue

        for _, row in df.iterrows():
            # Filtra por produto se a coluna existir
            if prod_col:
                prod_val = str(row.get(prod_col, "")).upper()
                if "BIODIESEL" not in prod_val and "B100" not in prod_val:
                    continue

            di    = parse_date_br(row.get(di_col))
            df_   = parse_date_br(row.get(df_col))
            preco = safe_float(row.get(preco_col))

            # Regiao: da coluna ou do nome da aba
            reg = ""
            if reg_col:
                reg = str(row.get(reg_col, "")).strip().upper()
            if not reg:
                reg = sheet.strip().upper()
            reg = reg.replace("CENTRO OESTE", "CENTRO-OESTE")

            if di and df_ and reg and preco and preco > 0:
                all_rows.append({
                    "data_inicial": di, "data_final": df_,
                    "regiao": reg, "preco": preco,
                })

    result = pd.DataFrame(all_rows)
    if not result.empty:
        result = result.drop_duplicates(subset=["data_inicial", "regiao"])
        result = result.sort_values("data_inicial")
        log.info(
            f"[Biodiesel] {len(result)} registros | "
            f"{result['data_inicial'].min()} → {result['data_inicial'].max()}"
        )
    else:
        log.warning("[Biodiesel] Nenhum registro parseado.")
    return result


# ─────────────────────────────────────────────────────────────────────────────
# SEÇÃO 5 — Spread calculado
# ─────────────────────────────────────────────────────────────────────────────

def run_spread(conn):
    log.info("=" * 60)
    log.info("Crushing Spread — Calculando RS e MT")
    log.info("=" * 60)

    # Media por data/UF para consolidar diferentes pontos de coleta
    soja_df = pd.read_sql(
        "SELECT data_referencia, uf, AVG(preco_brl_sc60) AS preco "
        "FROM soja_conab WHERE preco_brl_sc60 > 0 "
        "GROUP BY data_referencia, uf ORDER BY data_referencia",
        conn,
    )
    farelo_df = pd.read_sql(
        "SELECT ano, mes, porto, preco_usd_kg "
        "FROM farelo_secex WHERE preco_usd_kg > 0 ORDER BY ano, mes",
        conn,
    )
    ptax_df = pd.read_sql(
        "SELECT data_referencia, ptax_venda FROM fx_ptax ORDER BY data_referencia",
        conn,
    )
    bio_df = pd.read_sql(
        "SELECT data_inicial, data_final, regiao, preco_brl_m3 "
        "FROM biodiesel_anp WHERE preco_brl_m3 > 0 ORDER BY data_inicial",
        conn,
    )

    missing = [n for n, df in [("soja", soja_df), ("farelo", farelo_df),
                                ("ptax", ptax_df), ("bio", bio_df)] if df.empty]
    if missing:
        log.warning(f"[Spread] Dados insuficientes — faltam: {missing}")
        return {"calculado": 0}

    ld_spread = last_date(conn, "crushing_spread")
    start = (
        datetime.strptime(ld_spread, "%Y-%m-%d").date() + timedelta(days=1)
        if ld_spread else HISTORY_START
    )

    ptax_map   = dict(zip(ptax_df["data_referencia"],
                          ptax_df["ptax_venda"].astype(float)))
    ptax_dates = sorted(ptax_map.keys())

    inserted = 0
    d = start

    while d <= TODAY:
        d_str = d.strftime("%Y-%m-%d")
        ano   = d.year
        mes   = d.month

        ptax = _last_value(ptax_map, ptax_dates, d_str)
        if ptax is None:
            d += timedelta(days=7)
            continue

        for regiao, cfg in REGIAO_MAP.items():

            # Soja — forward-fill por UF
            uf_df      = soja_df[soja_df["uf"] == cfg["uf"]]
            soja_map   = dict(zip(uf_df["data_referencia"],
                                  uf_df["preco"].astype(float)))
            soja_dates = sorted(soja_map.keys())
            p_soja_sc60 = _last_value(soja_map, soja_dates, d_str)
            if p_soja_sc60 is None:
                continue

            # Farelo — mes corrente, fallback mes anterior
            porto_df   = farelo_df[farelo_df["porto"] == cfg["porto"]]
            farelo_row = porto_df[
                (porto_df["ano"] == ano) & (porto_df["mes"] == mes)
            ]
            if farelo_row.empty:
                prev_mes = mes - 1 if mes > 1 else 12
                prev_ano = ano if mes > 1 else ano - 1
                farelo_row = porto_df[
                    (porto_df["ano"] == prev_ano) & (porto_df["mes"] == prev_mes)
                ]
            if farelo_row.empty:
                continue
            p_farelo_usdkg = float(farelo_row.iloc[-1]["preco_usd_kg"])

            # Biodiesel — semana ANP, fallback 21 dias
            bio_reg  = bio_df[bio_df["regiao"] == cfg["bio"]]
            bio_rows = bio_reg[
                (bio_reg["data_inicial"] <= d_str) &
                (bio_reg["data_final"]   >= d_str)
            ]
            if bio_rows.empty:
                cutoff   = (d - timedelta(days=21)).strftime("%Y-%m-%d")
                bio_rows = bio_reg[bio_reg["data_final"] >= cutoff]
                if bio_rows.empty:
                    continue
            p_bio_m3 = float(bio_rows.iloc[-1]["preco_brl_m3"])

            # Calculo do spread
            p_soja_ton    = p_soja_sc60 * CONV_SC60_TON
            p_farelo_ton  = p_farelo_usdkg * 1000 * ptax  # USD/kg → R$/ton

            receita_farelo    = p_farelo_ton * FATOR_FARELO
            receita_biodiesel = p_bio_m3     * FATOR_BIODIESEL
            custo_soja        = p_soja_ton
            spread            = receita_farelo + receita_biodiesel - custo_soja

            conn.execute(
                "INSERT OR REPLACE INTO crushing_spread "
                "(data_referencia, regiao, preco_soja_sc60, preco_soja_ton, "
                " preco_farelo_usdkg, preco_farelo_ton, ptax, preco_bio_m3, "
                " receita_farelo, receita_biodiesel, custo_soja, spread_brl_ton, "
                " updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (d_str, regiao,
                 round(p_soja_sc60,    4), round(p_soja_ton,    4),
                 round(p_farelo_usdkg, 6), round(p_farelo_ton,  4),
                 round(ptax,           4), round(p_bio_m3,      4),
                 round(receita_farelo, 4), round(receita_biodiesel, 4),
                 round(custo_soja,     4), round(spread,         4),
                 NOW_STR),
            )
            if conn.execute("SELECT changes()").fetchone()[0]:
                inserted += 1

        d += timedelta(days=7)

    conn.commit()
    log.info(f"[Spread] {inserted} registros calculados/atualizados.")
    return {"calculado": inserted}


# ─────────────────────────────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────────────────────────────

def summary(conn):
    log.info("=" * 60)
    log.info("RESUMO DO BANCO")

    for uf in ["RS", "MT"]:
        r = conn.execute(
            "SELECT COUNT(*), MIN(data_referencia), MAX(data_referencia), "
            "ROUND(AVG(preco_brl_sc60),2) FROM soja_conab WHERE uf=?", (uf,)
        ).fetchone()
        log.info(
            f"  soja_conab [{uf}]: {r[0]:5,} | {r[1]} → {r[2]} | avg R${r[3]}/sc"
        )

    for porto in ["Santos", "Rio Grande"]:
        r = conn.execute(
            "SELECT COUNT(*), MIN(ano), MAX(ano), ROUND(AVG(preco_usd_kg),4) "
            "FROM farelo_secex WHERE porto=?", (porto,)
        ).fetchone()
        log.info(
            f"  farelo_secex [{porto:10}]: {r[0]:5,} meses | "
            f"{r[1]}→{r[2]} | avg US${r[3]}/kg"
        )

    r = conn.execute(
        "SELECT COUNT(*), MIN(data_referencia), MAX(data_referencia) FROM fx_ptax"
    ).fetchone()
    log.info(f"  fx_ptax: {r[0]:5,} | {r[1]} → {r[2]}")

    for reg in ["SUL", "CENTRO-OESTE"]:
        r = conn.execute(
            "SELECT COUNT(*), MIN(data_inicial), MAX(data_inicial), "
            "ROUND(AVG(preco_brl_m3),2) FROM biodiesel_anp WHERE regiao=?", (reg,)
        ).fetchone()
        log.info(
            f"  biodiesel [{reg:12}]: {r[0]:5,} semanas | "
            f"{r[1]} → {r[2]} | avg R${r[3]}/m³"
        )

    log.info("-" * 60)
    log.info("ULTIMOS SPREADS:")
    for row in conn.execute(
        "SELECT regiao, data_referencia, preco_soja_sc60, "
        "preco_farelo_ton, preco_bio_m3, spread_brl_ton "
        "FROM crushing_spread "
        "WHERE data_referencia = (SELECT MAX(data_referencia) FROM crushing_spread) "
        "ORDER BY regiao"
    ).fetchall():
        log.info(
            f"  {row['regiao']} ({row['data_referencia']}): "
            f"soja={row['preco_soja_sc60']:.2f} R$/sc | "
            f"farelo={row['preco_farelo_ton']:.2f} R$/t | "
            f"bio={row['preco_bio_m3']:.2f} R$/m³ | "
            f"SPREAD = {row['spread_brl_ton']:.2f} R$/t"
        )
    log.info("=" * 60)


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    global FORCE_ALL
    FORCE_ALL = "--force-all" in sys.argv

    log.info("=" * 60)
    log.info(f"Crushing Spread Extractor | {TODAY} ({TODAY.strftime('%A')}) | {NOW_STR}")
    log.info(f"  Force-all: {FORCE_ALL}")
    log.info("=" * 60)

    DB_DIR.mkdir(parents=True, exist_ok=True)
    conn = get_conn()
    ensure_schema(conn)

    errors = []
    for label, fn in [
        ("Soja/CONAB",    lambda: run_soja(conn)),
        ("PTAX/BCB",      lambda: run_ptax(conn)),
        ("Farelo/SECEX",  lambda: run_farelo(conn)),
        ("Biodiesel/ANP", lambda: run_biodiesel(conn)),
        ("Spread",        lambda: run_spread(conn)),
    ]:
        try:
            fn()
        except Exception as e:
            log.error(f"[{label}] FALHOU: {e}")
            errors.append(f"{label}: {e}")

    summary(conn)
    conn.close()

    if errors:
        log.error(f"FINALIZADO COM {len(errors)} ERRO(S):")
        for e in errors:
            log.error(f"  * {e}")
        sys.exit(1)
    else:
        log.info("Todas as secoes concluidas com sucesso.")


if __name__ == "__main__":
    main()
