#!/usr/bin/env python3
"""
extractor_crushing_spread.py — Agri Monitor · Soy Crushing Spread
==================================================================
Calcula o crushing spread de soja para biodiesel no RS e no MT.

Fórmula (por tonelada de soja processada):
    Spread = (P_farelo × 0.77) + (P_biodiesel × 0.19) - (P_soja × 1000/60)

    P_soja      -> R$/sc 60kg  — CONAB precos semanais por UF (RS e MT)
    P_farelo    -> USD/kg FOB convertido para R$/ton via PTAX
                  ComexStat/MDIC, NCM 23040090, URF Santos e Rio Grande
    P_biodiesel -> R$/m3       — ANP produtores B100, Sul e Centro-Oeste

Schedules (GitHub Actions):
    Quinta + Sexta  -> CONAB soja + ANP biodiesel + PTAX + Spread
    Dias 8-15/mes   -> ComexStat farelo + PTAX + Spread
    Cada secao so busca se o dado no banco estiver velho (freshness check).

Uso:
    python extractor_crushing_spread.py             # rodada normal
    python extractor_crushing_spread.py --force-all # rebusca tudo
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

SOJA_STALE_DAYS   = 5
BIO_STALE_DAYS    = 10
FARELO_STALE_DAYS = 25

FATOR_FARELO    = 0.77
FATOR_BIODIESEL = 0.19
CONV_SC60_TON   = 1000 / 60

# ── URLs ──────────────────────────────────────────────────────────────────────
CONAB_SEMANAL_UF = (
    "https://portaldeinformacoes.conab.gov.br/downloads/arquivos/PrecosSemanalUF.txt"
)
CONAB_UFS = {"RS", "MT"}

COMEXSTAT_API  = "https://api-comexstat.mdic.gov.br/general"
FARELO_NCM     = "23040090"
URF_SANTOS     = "0809200"
URF_RIO_GRANDE = "0912600"

BCB_PTAX_URL = (
    "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/"
    "CotacaoDolarPeriodo(dataInicial=@di,dataFinalCotacao=@df)"
    "?@di='{di}'&@df='{df}'&$top=1000&$orderby=dataHoraCotacao%20asc"
    "&$format=json&$select=cotacaoVenda,dataHoraCotacao"
)

# URL correta confirmada na pagina da ANP — arquivo .xls (nao .xlsx)
ANP_BIODIESEL_URL = (
    "https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/"
    "ppidp/precos-medios-ponderados-semanais-2013.xls"
)
ANP_REGIOES = {"SUL", "CENTRO-OESTE"}

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
        f"-> {'DESATUALIZADO' if age > stale_days else 'OK'}"
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
    CREATE TABLE IF NOT EXISTS soja_conab (
        id                    INTEGER PRIMARY KEY AUTOINCREMENT,
        data_referencia       TEXT NOT NULL,
        uf                    TEXT NOT NULL,
        municipio             TEXT,
        nivel_comercializacao TEXT,
        preco_brl_kg          REAL,
        preco_brl_sc60        REAL,
        fonte                 TEXT DEFAULT 'CONAB/Semanal',
        updated_at            TEXT,
        UNIQUE(data_referencia, uf, municipio, nivel_comercializacao)
    );
    CREATE INDEX IF NOT EXISTS idx_soja_data ON soja_conab(data_referencia);
    CREATE INDEX IF NOT EXISTS idx_soja_uf   ON soja_conab(uf);

    CREATE TABLE IF NOT EXISTS fx_ptax (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        data_referencia TEXT NOT NULL UNIQUE,
        ptax_venda      REAL NOT NULL,
        fonte           TEXT DEFAULT 'BCB/PTAX',
        updated_at      TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_ptax ON fx_ptax(data_referencia);

    CREATE TABLE IF NOT EXISTS farelo_comexstat (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        ano          INTEGER NOT NULL,
        mes          INTEGER NOT NULL,
        porto        TEXT NOT NULL,
        urf          TEXT NOT NULL,
        kg_liquido   REAL,
        vl_fob_usd   REAL,
        preco_usd_kg REAL,
        fonte        TEXT DEFAULT 'ComexStat/MDIC',
        updated_at   TEXT,
        UNIQUE(ano, mes, porto)
    );
    CREATE INDEX IF NOT EXISTS idx_farelo ON farelo_comexstat(ano, mes);

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
            r = requests.get(url, headers=hdrs, timeout=60, verify=False)
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
# SECAO 1 — Soja CONAB
# ─────────────────────────────────────────────────────────────────────────────

def run_soja(conn):
    log.info("=" * 60)
    log.info("Soja — CONAB Precos Semanais por UF (RS e MT)")
    log.info("=" * 60)

    if not is_stale(conn, "soja_conab", SOJA_STALE_DAYS):
        log.info("[Soja] Dado fresco — pulando.")
        return {"skipped": True}

    ld      = last_date(conn, "soja_conab")
    content = download(CONAB_SEMANAL_UF, "conab-soja", fatal=True)

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
    log.info(f"[Soja] Colunas: {list(df.columns)} | Linhas brutas: {len(df)}")

    # Colunas confirmadas nos logs anteriores
    uf_col    = "UF"
    date_col  = "DATA_INICIAL_FINAL_SEMANA"
    preco_col = "VALOR_PRODUTO_KG"
    prod_col  = "PRODUTO"
    nivel_col = "DSC_NIVEL_COMERCIALIZACAO" if "DSC_NIVEL_COMERCIALIZACAO" in df.columns else None
    mun_col   = next((c for c in df.columns if "MUNIC" in c), None)

    # Diagnostico: mostra produtos unicos para facilitar ajuste futuro
    if prod_col in df.columns:
        produtos_unicos = df[prod_col].dropna().unique()
        log.info(f"[Soja] Produtos disponiveis (primeiros 30): {list(produtos_unicos[:30])}")

    # Filtra por UF
    df = df[df[uf_col].str.upper().isin(CONAB_UFS)].copy()
    log.info(f"[Soja] Apos filtro UF (RS/MT): {len(df)} linhas")

    # Filtra por produto — "SOJA" no nome
    df = df[df[prod_col].str.upper().str.contains("SOJA", na=False)].copy()
    log.info(f"[Soja] Apos filtro SOJA: {len(df)} linhas")

    if df.empty:
        log.warning("[Soja] Nenhuma linha. Verifique os produtos disponiveis acima.")
        return {"inserido": 0}

    inserted = 0
    for _, row in df.iterrows():
        # DATA_INICIAL_FINAL_SEMANA pode ser "DD/MM/YYYY-DD/MM/YYYY" — pega a inicial
        raw_date = str(row.get(date_col, "")).split("-")[0].strip()
        dr = parse_date_br(raw_date)
        if not dr or (ld and dr <= ld):
            continue

        uf    = str(row.get(uf_col, "")).strip().upper()
        nivel = str(row.get(nivel_col, "")).strip() if nivel_col else None
        mun   = str(row.get(mun_col,  "")).strip() if mun_col else None

        # VALOR_PRODUTO_KG esta em R$/kg — converte para R$/sc 60kg
        preco_kg = safe_float(row.get(preco_col))
        if not preco_kg or preco_kg <= 0:
            continue
        preco_sc60 = preco_kg * 60

        conn.execute(
            "INSERT OR IGNORE INTO soja_conab "
            "(data_referencia, uf, municipio, nivel_comercializacao, "
            " preco_brl_kg, preco_brl_sc60, updated_at) "
            "VALUES (?,?,?,?,?,?,?)",
            (dr, uf, mun, nivel, preco_kg, preco_sc60, NOW_STR),
        )
        if conn.execute("SELECT changes()").fetchone()[0]:
            inserted += 1

    conn.commit()
    log.info(f"[Soja] {inserted} linhas inseridas.")
    return {"inserido": inserted}


# ─────────────────────────────────────────────────────────────────────────────
# SECAO 2 — PTAX BCB
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
# SECAO 3 — Farelo ComexStat
# ─────────────────────────────────────────────────────────────────────────────

def run_farelo(conn):
    log.info("=" * 60)
    log.info("Farelo de Soja FOB — ComexStat/MDIC (Santos e Rio Grande)")
    log.info("=" * 60)

    r = conn.execute(
        "SELECT MAX(printf('%04d-%02d-01', ano, mes)) FROM farelo_comexstat"
    ).fetchone()
    last_ym = r[0] if r and r[0] else None

    if not FORCE_ALL and last_ym:
        last_dt = datetime.strptime(last_ym, "%Y-%m-%d").date()
        age     = (TODAY - last_dt).days
        log.info(
            f"  [farelo_comexstat] ultimo: {last_ym[:7]} ({age}d, "
            f"limiar={FARELO_STALE_DAYS}d) -> "
            f"{'DESATUALIZADO' if age > FARELO_STALE_DAYS else 'OK'}"
        )
        if age <= FARELO_STALE_DAYS:
            log.info("[Farelo] Dado fresco — pulando.")
            return {"skipped": True}

    results = {}
    for porto, urf in [("Santos", URF_SANTOS), ("Rio Grande", URF_RIO_GRANDE)]:
        results[porto] = _ingest_farelo_porto(conn, porto, urf)
    return results


def _ingest_farelo_porto(conn, porto, urf):
    r = conn.execute(
        "SELECT ano, mes FROM farelo_comexstat "
        "WHERE porto=? ORDER BY ano DESC, mes DESC LIMIT 1",
        (porto,)
    ).fetchone()

    if r:
        start_ano, start_mes = int(r["ano"]), int(r["mes"])
    else:
        start_ano, start_mes = HISTORY_START.year, HISTORY_START.month

    end_ano = TODAY.year
    end_mes = TODAY.month - 1 if TODAY.month > 1 else 12
    if end_mes == 0:
        end_ano -= 1
        end_mes  = 12

    if start_ano > end_ano or (start_ano == end_ano and start_mes > end_mes):
        log.info(f"[Farelo-{porto}] Ja atualizado.")
        return 0

    log.info(
        f"[Farelo-{porto}] Consultando "
        f"{start_mes:02d}/{start_ano} -> {end_mes:02d}/{end_ano}"
    )

    payload = {
        "flow":       "export",
        "monthStart": f"{start_ano}-{start_mes:02d}",
        "monthEnd":   f"{end_ano}-{end_mes:02d}",
        "filters": [
            {"filter": "ncm", "values": [FARELO_NCM]},
            {"filter": "urf", "values": [urf]},
        ],
        "details":  ["month"],
        "metrics":  ["metricFOB", "metricKG"],
        "language": "pt",
    }

    try:
        resp = requests.post(
            COMEXSTAT_API,
            json=payload,
            headers={**HEADERS, "Content-Type": "application/json"},
            timeout=60,
            verify=False,
        )
        if not resp.ok:
            log.error(
                f"[Farelo-{porto}] API {resp.status_code}: {resp.text[:500]}"
            )
            resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        raise RuntimeError(f"[Farelo-{porto}] API ComexStat falhou: {e}")

    rows = data.get("data", data.get("list", []))
    log.info(
        f"[Farelo-{porto}] {len(rows)} meses | "
        f"Chaves do response: {list(data.keys())}"
    )
    if rows:
        log.info(f"[Farelo-{porto}] Exemplo de linha: {rows[0]}")

    inserted = 0
    for row in rows:
        raw_my = str(
            row.get("monthYear") or row.get("month") or
            row.get("periodo") or row.get("co_mes_ini") or ""
        )
        try:
            if "-" in raw_my and len(raw_my) >= 7:
                parts = raw_my.split("-")
                ano, mes = (int(parts[0]), int(parts[1])) if len(parts[0]) == 4 \
                           else (int(parts[1]), int(parts[0]))
            elif "/" in raw_my:
                parts    = raw_my.split("/")
                mes, ano = int(parts[0]), int(parts[1])
            else:
                log.warning(f"[Farelo-{porto}] Data nao reconhecida: {raw_my}")
                continue
        except Exception:
            continue

        kg  = safe_float(
            row.get("metricKG") or row.get("kgLiquido") or
            row.get("kg_liquido") or row.get("qtdKg")
        )
        fob = safe_float(
            row.get("metricFOB") or row.get("vlFob") or
            row.get("vl_fob") or row.get("valorFob")
        )

        if not kg or kg == 0 or not fob:
            continue

        conn.execute(
            "INSERT OR REPLACE INTO farelo_comexstat "
            "(ano, mes, porto, urf, kg_liquido, vl_fob_usd, preco_usd_kg, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (ano, mes, porto, urf, kg, fob, round(fob / kg, 6), NOW_STR),
        )
        if conn.execute("SELECT changes()").fetchone()[0]:
            inserted += 1

    conn.commit()
    log.info(f"[Farelo-{porto}] {inserted} meses inseridos/atualizados.")
    return inserted


# ─────────────────────────────────────────────────────────────────────────────
# SECAO 4 — Biodiesel ANP
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

        header_row = None
        for i, row in raw.iterrows():
            row_str = " ".join(str(v).upper() for v in row.values if pd.notna(v))
            if "DATA" in row_str and ("REGI" in row_str or "PRODUTO" in row_str):
                header_row = i
                break
        if header_row is None:
            continue

        df = xl.parse(sheet, header=header_row)
        df.columns = [str(c).strip().upper() for c in df.columns]
        df = df.dropna(how="all")
        log.info(f"[Biodiesel] Aba '{sheet}': colunas = {list(df.columns)}")

        di_col    = next((c for c in df.columns if "INICIAL" in c), None)
        df_col    = next((c for c in df.columns if "FINAL"   in c), None)
        reg_col   = next((c for c in df.columns if "REGI"    in c), None)
        prod_col  = next((c for c in df.columns if "PRODUTO" in c), None)
        preco_col = next((c for c in df.columns
                          if any(k in c for k in ("PRECO", "VALOR", "M3",
                                                   "PONDERADO"))), None)
        # Tenta tambem com caractere especial
        if not preco_col:
            preco_col = next((c for c in df.columns
                              if "PRE" in c or "M\u00b3" in c), None)

        if not all([di_col, df_col, preco_col]):
            log.warning(f"[Biodiesel] Aba '{sheet}': colunas insuficientes — pulando")
            continue

        for _, row in df.iterrows():
            if prod_col:
                prod_val = str(row.get(prod_col, "")).upper()
                if "BIODIESEL" not in prod_val and "B100" not in prod_val:
                    continue

            di    = parse_date_br(row.get(di_col))
            df_   = parse_date_br(row.get(df_col))
            preco = safe_float(row.get(preco_col))

            reg = ""
            if reg_col:
                reg = str(row.get(reg_col, "")).strip().upper()
                reg = reg.replace("CENTRO OESTE", "CENTRO-OESTE")
            else:
                reg = sheet.strip().upper().replace("CENTRO OESTE", "CENTRO-OESTE")

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
            f"{result['data_inicial'].min()} -> {result['data_inicial'].max()}"
        )
    else:
        log.warning("[Biodiesel] Nenhum registro parseado da planilha.")
    return result


# ─────────────────────────────────────────────────────────────────────────────
# SECAO 5 — Spread calculado
# ─────────────────────────────────────────────────────────────────────────────

def run_spread(conn):
    log.info("=" * 60)
    log.info("Crushing Spread — Calculando RS e MT")
    log.info("=" * 60)

    soja_df = pd.read_sql(
        "SELECT data_referencia, uf, AVG(preco_brl_sc60) AS preco "
        "FROM soja_conab WHERE preco_brl_sc60 > 0 "
        "GROUP BY data_referencia, uf ORDER BY data_referencia",
        conn,
    )
    farelo_df = pd.read_sql(
        "SELECT ano, mes, porto, preco_usd_kg "
        "FROM farelo_comexstat WHERE preco_usd_kg > 0 ORDER BY ano, mes",
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

    if any(df.empty for df in [soja_df, farelo_df, ptax_df, bio_df]):
        missing = [n for n, df in [("soja", soja_df), ("farelo", farelo_df),
                                    ("ptax", ptax_df), ("bio", bio_df)] if df.empty]
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
            uf_df      = soja_df[soja_df["uf"] == cfg["uf"]]
            soja_map   = dict(zip(uf_df["data_referencia"],
                                  uf_df["preco"].astype(float)))
            soja_dates = sorted(soja_map.keys())
            p_soja_sc60 = _last_value(soja_map, soja_dates, d_str)
            if p_soja_sc60 is None:
                continue

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

            p_soja_ton    = p_soja_sc60 * CONV_SC60_TON
            p_farelo_ton  = p_farelo_usdkg * 1000 * ptax
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
            f"  soja_conab [{uf}]: {r[0]:5,} | {r[1]} -> {r[2]} | avg R${r[3]}/sc"
        )

    for porto in ["Santos", "Rio Grande"]:
        r = conn.execute(
            "SELECT COUNT(*), MIN(ano), MAX(ano), ROUND(AVG(preco_usd_kg),4) "
            "FROM farelo_comexstat WHERE porto=?", (porto,)
        ).fetchone()
        log.info(
            f"  farelo [{porto:10}]: {r[0]:5,} meses | {r[1]}->{r[2]} | avg ${r[3]}/kg"
        )

    r = conn.execute(
        "SELECT COUNT(*), MIN(data_referencia), MAX(data_referencia) FROM fx_ptax"
    ).fetchone()
    log.info(f"  fx_ptax: {r[0]:5,} | {r[1]} -> {r[2]}")

    for reg in ["SUL", "CENTRO-OESTE"]:
        r = conn.execute(
            "SELECT COUNT(*), MIN(data_inicial), MAX(data_inicial), "
            "ROUND(AVG(preco_brl_m3),2) FROM biodiesel_anp WHERE regiao=?", (reg,)
        ).fetchone()
        log.info(
            f"  biodiesel [{reg:12}]: {r[0]:5,} semanas | {r[1]} -> {r[2]} | avg R${r[3]}/m3"
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
            f"bio={row['preco_bio_m3']:.2f} R$/m3 | "
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
        ("Soja/CONAB",       lambda: run_soja(conn)),
        ("PTAX/BCB",         lambda: run_ptax(conn)),
        ("Farelo/ComexStat", lambda: run_farelo(conn)),
        ("Biodiesel/ANP",    lambda: run_biodiesel(conn)),
        ("Spread",           lambda: run_spread(conn)),
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
