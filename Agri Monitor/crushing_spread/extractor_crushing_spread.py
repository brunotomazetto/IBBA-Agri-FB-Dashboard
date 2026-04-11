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

# ── CONAB — TXT precos semanais por produto/UF ───────────────────────────────
CONAB_PRECO_URL = (
    "https://portaldeinformacoes.conab.gov.br/downloads/arquivos/PrecosSemanalUF.txt"
)
CONAB_UFS     = {"RS", "MT"}
CONAB_PRODUTO = "SOJA"
CONAB_NIVEL   = "RECEBIDO"  # nivel = "PREÇO RECEBIDO P/ PR" (preço recebido pelo produtor)

# ── SECEX — CSV bulk por ano (mesmo padrão do extractor_secex.py) ────────────
SECEX_BASE_URL = "https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/EXP_{ano}.csv"
FARELO_NCM     = 23040090   # Farelo e resíduos da extração de óleo de soja (int)

# Filtro por UF de embarque (SG_UF_NCM) — mais robusto que filtrar por URF
# Santos → SP | Rio Grande → RS
UF_PORTO_CONFIG = {
    "Santos":     "SP",
    "Rio Grande": "RS",
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
    """
    Precos de soja ao produtor — CONAB PrecosSemanalUF.txt (update semanal)
    Fonte: portaldeinformacoes.conab.gov.br/downloads/arquivos/PrecosSemanalUF.txt
    Produto: 'SOJA' (match exato apos strip — exclui farelo, semente, oleo)
    Nivel: 'PREÇO RECEBIDO P/ PR' (preco recebido pelo produtor, campo DSC_NIVEL_COMERCIALIZACAO)
    UFs: RS e MT | Preco em R$/kg → converte para R$/sc60 (×60)

    Historico anterior a 2025 vem do portal Siagro (precos mensais):
    portaldeinformacoes.conab.gov.br → Mercado → Preços Agropecuários
    Produto: 'SOJA EM GRÃOS   (60 kg)' | Nivel: PRODUTOR | UF: MT, RS
    e foi carregado manualmente no DB via XLSX exportado do portal.
    """
    log.info("=" * 60)
    log.info("Soja — CONAB PrecosSemanalUF.txt (RS e MT)")
    log.info("=" * 60)

    if not is_stale(conn, "soja_conab", SOJA_STALE_DAYS):
        log.info("[Soja] Dado fresco — pulando.")
        return {"skipped": True}

    ld      = last_date(conn, "soja_conab")
    content = download(CONAB_PRECO_URL, "conab-soja", fatal=True)

    for enc in ("latin-1", "utf-8-sig", "utf-8"):
        try:
            text = content.decode(enc)
            break
        except UnicodeDecodeError:
            continue

    first_line = text.splitlines()[0] if text.splitlines() else ""
    sep = "\t" if "\t" in first_line else ";"
    df = pd.read_csv(io.StringIO(text), sep=sep, on_bad_lines="skip", dtype=str)
    df.columns = [c.strip().upper() for c in df.columns]
    log.info(f"[Soja] Colunas: {list(df.columns)} | Linhas: {len(df)}")

    uf_col    = next((c for c in df.columns if c in ("UF", "SIGLA_UF")), None)
    prod_col  = next((c for c in df.columns if "PRODUTO" in c), None)
    nivel_col = next((c for c in df.columns if "NIVEL" in c or "COMERCI" in c), None)
    date_col  = next((c for c in df.columns if "DATA" in c), None)
    preco_col = next((c for c in df.columns if "VALOR" in c or "PRECO" in c), None)

    if not all([uf_col, prod_col, date_col, preco_col]):
        raise RuntimeError(f"[Soja] Colunas nao encontradas: {list(df.columns)}")

    # Produto exato = 'SOJA' (apos strip)
    df = df[df[prod_col].str.strip().str.upper() == "SOJA"].copy()
    log.info(f"[Soja] Linhas produto='SOJA': {len(df)}")

    # UFs RS e MT
    df = df[df[uf_col].str.strip().str.upper().isin({"RS", "MT"})].copy()
    log.info(f"[Soja] Apos filtro UF: {len(df)} linhas")

    # Nivel: 'PREÇO RECEBIDO P/ PR' — contem 'RECEBIDO'
    if nivel_col:
        niveis = df[nivel_col].str.strip().str.upper().unique()
        log.info(f"[Soja] Niveis disponiveis: {list(niveis)}")
        df = df[df[nivel_col].str.strip().str.upper().str.contains("RECEBIDO", na=False)].copy()
        log.info(f"[Soja] Apos filtro nivel RECEBIDO: {len(df)} linhas")

    if df.empty:
        log.warning("[Soja] Nenhuma linha apos filtros.")
        return {"inserido": 0}

    # Log das primeiras linhas para diagnostico
    amostra = df.head(3)
    for _, row in amostra.iterrows():
        log.info(
            f"[Soja] Amostra: uf={row.get(uf_col,'?')} | "
            f"data_raw='{row.get(date_col,'?')}' | "
            f"preco={row.get(preco_col,'?')}"
        )
    log.info(f"[Soja] ld (ultimo no banco) = {ld}")

    inserted = 0
    n_date_fail = 0
    n_ld_skip   = 0
    for _, row in df.iterrows():
        # DATA_INICIAL_FINAL_SEMANA: "DD-MM-YYYY - DD-MM-YYYY" — pega a inicial
        raw_field = str(row.get(date_col, "")).strip()
        raw_date  = raw_field.split(" - ")[0].strip()
        # Converte DD-MM-YYYY para parse_date_br (que aceita DD/MM/YYYY)
        raw_date  = raw_date.replace("-", "/")
        dr = parse_date_br(raw_date)
        if not dr:
            n_date_fail += 1
            if n_date_fail <= 2:
                log.warning(f"[Soja] Parse falhou: raw_field='{raw_field}' → raw_date='{raw_date}'")
            continue
        if ld and dr <= ld:
            n_ld_skip += 1
            continue
        uf       = str(row.get(uf_col, "")).strip().upper()
        nivel    = str(row.get(nivel_col, "")).strip() if nivel_col else "PREÇO RECEBIDO P/ PR"
        preco_kg = safe_float(row.get(preco_col))
        if not preco_kg or preco_kg <= 0:
            continue
        conn.execute(
            "INSERT OR IGNORE INTO soja_conab "
            "(data_referencia, uf, produto_conab, nivel, "
            " preco_brl_kg, preco_brl_sc60, updated_at) "
            "VALUES (?,?,?,?,?,?,?)",
            (dr, uf, "SOJA", nivel, preco_kg, round(preco_kg * 60, 4), NOW_STR),
        )
        if conn.execute("SELECT changes()").fetchone()[0]:
            inserted += 1

    conn.commit()
    log.info(f"[Soja] {inserted} linhas inseridas | "
             f"date_fail={n_date_fail} | ld_skip={n_ld_skip}.")
    return {"inserido": inserted}


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

            # Filtra NCM de farelo de soja (CO_NCM lido como int)
            df_farelo = df[df["CO_NCM"] == FARELO_NCM].copy()
            if df_farelo.empty:
                log.info(f"[Farelo] {ano}: NCM {FARELO_NCM} nao encontrado")
                continue

            log.info(f"[Farelo] {ano}: {len(df_farelo)} linhas de farelo encontradas")
            # Mostra UFs de embarque disponiveis para diagnostico
            if "SG_UF_NCM" in df_farelo.columns:
                ufs = df_farelo["SG_UF_NCM"].dropna().unique()
                log.info(f"[Farelo] {ano}: UFs de embarque: {sorted(ufs)}")

            # Filtra por UF de embarque (SG_UF_NCM) — mais robusto que filtrar por URF
            # SP = Santos | RS = Rio Grande
            for porto, uf_emb in UF_PORTO_CONFIG.items():
                col_uf = next((c for c in df_farelo.columns
                               if "UF_NCM" in c or "SG_UF" in c), None)
                if col_uf is None:
                    log.warning(f"[Farelo] {ano}: coluna UF nao encontrada. "
                                f"Colunas: {list(df_farelo.columns[:8])}")
                    break
                df_porto = df_farelo[df_farelo[col_uf].str.strip().str.upper() == uf_emb].copy()
                if df_porto.empty:
                    log.info(f"[Farelo] {ano}/{porto} (UF={uf_emb}): sem dados")
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
                        (a, m, porto, uf_emb, kg, fob,
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
    Layout confirmado nos logs (linha 8 = regioes, linha 9+ = dados):
      ['(A partir de 2013)', 'Norte', 'Nordeste', 'Centro-Oeste', 'Sul', 'Sudeste']
      linha 9: [produto, data_ini, data_fim, norte, nordeste, centro-oeste, sul, sudeste, brasil]
      produto ex: "Biodiesel (B100) (R$/m3)"
      datas como objetos datetime pandas
      idx_sul=6, idx_co=5 (confirmados nos logs)
    """
    try:
        xl = pd.ExcelFile(io.BytesIO(content), engine="xlrd")
        log.info(f"[Biodiesel] Abas: {xl.sheet_names}")
    except Exception as e:
        raise RuntimeError(f"[Biodiesel] Nao foi possivel abrir .xls: {e}")

    all_rows = []

    for sheet in xl.sheet_names:
        try:
            # Le sem cabecalho — retorna DataFrame com indice 0..N
            raw = xl.parse(sheet, header=None)
        except Exception:
            continue

        # Reset index para garantir acesso por posicao numerica com .iloc
        raw = raw.reset_index(drop=True)

        # Procura linha de regioes (Norte + Nordeste + Sul juntos)
        regiao_pos = None  # posicao numerica no DataFrame resetado
        for pos in range(len(raw)):
            vals = [str(v).strip() for v in raw.iloc[pos].tolist() if pd.notna(v) and str(v).strip()]
            vals_up = [v.upper() for v in vals]
            if "NORTE" in vals_up and "NORDESTE" in vals_up and "SUL" in vals_up:
                regiao_pos = pos
                log.info(f"[Biodiesel] Linha de regioes na posicao {pos}: {vals}")
                break

        if regiao_pos is None:
            log.warning(f"[Biodiesel] Aba '{sheet}': linha de regioes nao encontrada")
            continue

        # Mapa de indices de coluna a partir da linha de regioes
        regiao_vals = raw.iloc[regiao_pos].tolist()
        log.info(f"[Biodiesel] Valores das colunas: {regiao_vals}")

        idx_sul = None
        idx_co  = None
        for j, v in enumerate(regiao_vals):
            v_str = str(v).strip().upper() if pd.notna(v) else ""
            if "SUL" in v_str and "SUDE" not in v_str:
                idx_sul = j
            if "CENTRO" in v_str:
                idx_co = j
        log.info(f"[Biodiesel] idx_sul={idx_sul}, idx_co={idx_co}")

        if idx_sul is None and idx_co is None:
            log.warning("[Biodiesel] SUL e CENTRO-OESTE nao mapeados")
            continue

        def parse_any_date(rv):
            if rv is None:
                return None
            if hasattr(rv, 'strftime'):
                return rv.strftime("%Y-%m-%d")
            s = str(rv).strip()
            if not s or s.lower() in ("nat", "nan", ""):
                return None
            # "2013-01-06 00:00:00" ou "2013-01-06"
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y", "%d/%m/%y"):
                try:
                    return datetime.strptime(s[:19], fmt).strftime("%Y-%m-%d")
                except Exception:
                    continue
            return None

        # Itera linhas de dados abaixo da linha de regioes (usa posicao numerica)
        n_parsed = 0
        n_bio    = 0
        for pos in range(regiao_pos + 1, len(raw)):
            row_vals = raw.iloc[pos].tolist()

            # Coluna 0 = produto
            prod_raw = str(row_vals[0]).strip() if pd.notna(row_vals[0]) else ""
            if not prod_raw:
                continue
            prod_up = prod_raw.upper()

            # Conta quantas linhas de biodiesel passou
            if "BIODIESEL" in prod_up or "B100" in prod_up:
                n_bio += 1
                if n_bio <= 3:
                    log.info(f"[Biodiesel] Linha {pos}: produto='{prod_raw}', "
                             f"col1={row_vals[1] if len(row_vals)>1 else '?'}, "
                             f"col2={row_vals[2] if len(row_vals)>2 else '?'}, "
                             f"col{idx_sul}={row_vals[idx_sul] if idx_sul and idx_sul<len(row_vals) else '?'}, "
                             f"col{idx_co}={row_vals[idx_co] if idx_co and idx_co<len(row_vals) else '?'}")
            else:
                continue

            di  = parse_any_date(row_vals[1] if len(row_vals) > 1 else None)
            df_ = parse_any_date(row_vals[2] if len(row_vals) > 2 else None)

            if not di or not df_:
                continue

            for idx, regiao_nome in [(idx_sul, "SUL"), (idx_co, "CENTRO-OESTE")]:
                if idx is None or idx >= len(row_vals):
                    continue
                val = row_vals[idx]
                if val is None or (isinstance(val, float) and pd.isna(val)):
                    continue
                val_str = str(val).strip()
                if val_str in ("***", "", "nan", "NaN"):
                    continue
                preco = safe_float(val_str)
                if preco and preco > 0:
                    # Planilha ANP usa R$/litro (~6-8 R$/l para biodiesel)
                    # Precisamos de R$/m³ (~6000-8000 R$/m³) → multiplica por 1000
                    if preco < 50:        # R$/litro
                        preco_m3 = round(preco * 1000, 2)
                    elif preco < 500:     # R$/100l (improvavel, mas cobre)
                        preco_m3 = round(preco * 10, 2)
                    else:                 # ja em R$/m³
                        preco_m3 = round(preco, 2)
                    all_rows.append({
                        "data_inicial": di,
                        "data_final":   df_,
                        "regiao":       regiao_nome,
                        "preco":        preco_m3,
                    })
                    n_parsed += 1

        log.info(f"[Biodiesel] Aba '{sheet}': {n_bio} linhas B100 encontradas, "
                 f"{n_parsed} registros extraidos")

    result = pd.DataFrame(all_rows)
    if not result.empty:
        result = result.drop_duplicates(subset=["data_inicial", "regiao"])
        result = result.sort_values("data_inicial")
        log.info(f"[Biodiesel] {len(result)} registros | "
                 f"{result['data_inicial'].min()} → {result['data_inicial'].max()}")
        log.info(f"[Biodiesel] Regioes: {result['regiao'].value_counts().to_dict()}")
    else:
        log.warning("[Biodiesel] Nenhum registro parseado.")
    return result


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
