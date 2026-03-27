import hashlib
import io
import logging
import os
import re
import sqlite3
import time
from datetime import datetime
import pandas as pd
import requests
# ── Configurações ──────────────────────────────────────────────────────────────
DB_PATH      = os.path.join(os.path.dirname(__file__), "imea.db")
# API do IMEA (descoberta via DevTools — endpoint direto, sem necessidade de Playwright)
# Exemplo de chamada:
# GET https://api1.imea.com.br/api/arquivo?cadeia=4&tipo=696277432068079616&page=1&pageSize=100&nome=&sort=1
API_BASE  = "https://api1.imea.com.br/api/arquivo"
TIPO_ID   = "696277432068079616"   # subcategoria "Custo de Produção" (fixo)
PAGE_SIZE = 100                    # 100 por página garante histórico completo em 1 chamada
CULTURAS = {
    "SOJA":    "4",
    "MILHO":   "3",
    "ALGODAO": "2",
}
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer":    "https://imea.com.br/",
}
MESES_PT = {
    "jan": 1, "fev": 2, "mar": 3, "abr": 4,
    "mai": 5, "jun": 6, "jul": 7, "ago": 8,
    "set": 9, "out": 10, "nov": 11, "dez": 12,
}
# Palavras-chave para identificar bloco de custo no Excel
KW_COE = ("custo operacional efetivo", "coe", "sementes", "fertilizantes",
           "defensivos", "insumos", "operacoes mecanizadas", "despesas")
KW_COT = ("custo operacional total", "cot", "mao de obra", "mão de obra", "deprecia")
KW_CT  = ("custo total", "oportunidade", "arrendamento", "pro-labore", "pró-labore")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)
# ── Inicializa banco ───────────────────────────────────────────────────────────
conn = sqlite3.connect(DB_PATH)
conn.executescript("""
    CREATE TABLE IF NOT EXISTS relatorios (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        imea_id         TEXT    UNIQUE,
        cultura         TEXT    NOT NULL,
        nome            TEXT,
        safra           TEXT,
        mes_referencia  TEXT,
        data_publicacao TEXT,
        nome_arquivo    TEXT,
        url_s3          TEXT,
        hash_md5        TEXT    UNIQUE NOT NULL,
        coletado_em     TEXT    NOT NULL
    );
    CREATE TABLE IF NOT EXISTS custos_itens (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        relatorio_id    INTEGER NOT NULL REFERENCES relatorios(id),
        cultura         TEXT    NOT NULL,
        nome_relatorio  TEXT,
        safra           TEXT,
        mes_referencia  TEXT,
        regiao          TEXT,
        grupo_custo     TEXT,
        item            TEXT,
        unidade         TEXT,
        quantidade      REAL,
        preco_unitario  REAL,
        valor_ha        REAL,
        tipo_custo      TEXT,
        updated_at      TEXT
    );
    CREATE TABLE IF NOT EXISTS custos_resumo (
        id                   INTEGER PRIMARY KEY AUTOINCREMENT,
        relatorio_id         INTEGER NOT NULL REFERENCES relatorios(id),
        cultura              TEXT    NOT NULL,
        nome_relatorio       TEXT,
        safra                TEXT,
        mes_referencia       TEXT,
        regiao               TEXT,
        coe_ha               REAL,
        cot_ha               REAL,
        ct_ha                REAL,
        produtividade_sc_ha  REAL,
        pe_coe_sc_ha         REAL,
        pe_ct_sc_ha          REAL,
        updated_at           TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_itens_rel  ON custos_itens(relatorio_id);
    CREATE INDEX IF NOT EXISTS idx_resumo_rel ON custos_resumo(relatorio_id);
""")
conn.commit()
# ── Utilitários ────────────────────────────────────────────────────────────────
def parse_float(val):
    try:
        s = str(val).strip().replace("R$", "").replace(" ", "")
        if "," in s and "." in s:
            s = s.replace(".", "").replace(",", ".")
        elif "," in s:
            s = s.replace(",", ".")
        return float(s)
    except (ValueError, AttributeError):
        return None
def md5(content: bytes) -> str:
    return hashlib.md5(content).hexdigest()
def hash_existe(conn, h: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM relatorios WHERE hash_md5 = ?", (h,)
    ).fetchone() is not None
def extrai_mes_ref(texto: str) -> str | None:
    m = re.search(
        r"(jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)[a-z]*[\W_]?(\d{4})",
        texto, re.IGNORECASE
    )
    if m:
        mn = MESES_PT.get(m.group(1).lower()[:3], 1)
        return f"{m.group(2)}-{mn:02d}"
    return None
def extrai_safra(texto: str) -> str | None:
    m = re.search(r"(20\d{2})[/_\-](2\d|\d{2})", texto)
    return f"{m.group(1)}/{m.group(2)}" if m else None
def detecta_tipo_custo(row_str: str, tipo_atual: str) -> str:
    s = row_str.lower()
    if any(k in s for k in KW_CT):
        return "CT"
    if any(k in s for k in KW_COT):
        return "COT"
    if any(k in s for k in KW_COE):
        return "COE"
    return tipo_atual
def extrai_numericos(vals: list) -> list:
    return [f for v in vals if (f := parse_float(v)) is not None and f > 0]
def limpa(v) -> str:
    s = str(v).strip()
    return "" if s.lower() in ("nan", "none", "") else s
def upsert_itens(conn, rows: list[dict]):
    if not rows:
        return
    pd.DataFrame(rows).to_sql(
        "custos_itens", conn, if_exists="append", index=False, method="multi"
    )
    conn.execute("""
        DELETE FROM custos_itens WHERE id NOT IN (
            SELECT MAX(id) FROM custos_itens
            GROUP BY relatorio_id, regiao, grupo_custo, item, tipo_custo
        )
    """)
    conn.commit()
def upsert_resumo(conn, row: dict):
    pd.DataFrame([row]).to_sql(
        "custos_resumo", conn, if_exists="append", index=False, method="multi"
    )
    conn.execute("""
        DELETE FROM custos_resumo WHERE id NOT IN (
            SELECT MAX(id) FROM custos_resumo
            GROUP BY relatorio_id, regiao
        )
    """)
    conn.commit()
# ── API IMEA: lista todos os relatórios disponíveis ────────────────────────────
def lista_relatorios_api(cultura: str, cadeia_id: str) -> list[dict]:
    """
    Chama a API do IMEA e retorna todos os relatórios disponíveis para a cultura.
    Usa paginação para garantir histórico completo.
    """
    todos = []
    page  = 1
    while True:
        url = (
            f"{API_BASE}"
            f"?cadeia={cadeia_id}"
            f"&tipo={TIPO_ID}"
            f"&page={page}"
            f"&pageSize={PAGE_SIZE}"
            f"&nome=&sort=1"
        )
        try:
            resp = requests.get(url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            log.error(f"[{cultura}] Erro na API (page={page}): {e}")
            break
        results = data.get("Result", [])
        todos.extend(results)
        log.info(
            f"[{cultura}] API page={page} → {len(results)} itens "
            f"(total={data.get('TotalCount')})"
        )
        if page >= data.get("PageCount", 1):
            break
        page += 1
        time.sleep(0.5)
    return todos
# ── Parse do Excel ─────────────────────────────────────────────────────────────
def parse_excel(content: bytes, cultura: str, nome_relatorio: str) -> dict:
    """
    Lê todas as abas do Excel do IMEA e extrai:
    - itens de custo linha a linha (grupo, item, unidade, qtd, preço, R$/ha, COE/COT/CT)
    - resumo (COE/COT/CT totais, produtividade, ponto de equilíbrio)
    - safra e mês de referência
    O nome da aba é usado como campo 'regiao' (ex: "Médio Norte", "Sul MT").
    """
    xls = pd.ExcelFile(io.BytesIO(content))
    log.info(f"  Abas: {xls.sheet_names}")
    itens  = []
    resumo = {
        "coe_ha": None, "cot_ha": None, "ct_ha": None,
        "produtividade_sc_ha": None,
        "pe_coe_sc_ha": None, "pe_ct_sc_ha": None,
    }
    safra   = extrai_safra(nome_relatorio)
    mes_ref = extrai_mes_ref(nome_relatorio)
    SKIP = ("total", "subtotal", "nan", "", "item", "grupo", "descricao",
            "r$/ha", "r$", "unidade", "quantidade", "custo")
    for sheet in xls.sheet_names:
        try:
            df = pd.read_excel(io.BytesIO(content), sheet_name=sheet, header=None)
        except Exception as e:
            log.warning(f"  Erro aba '{sheet}': {e}")
            continue
        regiao           = sheet   # nome da aba = região
        tipo_custo_atual = "COE"
        for r in range(len(df)):
            row_vals = [limpa(v) for v in df.iloc[r]]
            row_str  = " ".join(row_vals).lower()
            # Captura safra/mês no cabeçalho
            if r < 20:
                for cell in row_vals:
                    safra   = safra   or extrai_safra(cell)
                    mes_ref = mes_ref or extrai_mes_ref(cell)
            # Detectar bloco COE / COT / CT
            tipo_custo_atual = detecta_tipo_custo(row_str, tipo_custo_atual)
            # Capturar totais para resumo
            if "custo operacional efetivo" in row_str or "total coe" in row_str:
                nums = extrai_numericos(row_vals)
                resumo["coe_ha"] = resumo["coe_ha"] or (nums[-1] if nums else None)
            if "custo operacional total" in row_str or "total cot" in row_str:
                nums = extrai_numericos(row_vals)
                resumo["cot_ha"] = resumo["cot_ha"] or (nums[-1] if nums else None)
            if re.search(r"\bcusto total\b", row_str) and "operacional" not in row_str:
                nums = extrai_numericos(row_vals)
                resumo["ct_ha"] = resumo["ct_ha"] or (nums[-1] if nums else None)
            if "produtividade" in row_str and "sc" in row_str:
                nums = extrai_numericos(row_vals)
                resumo["produtividade_sc_ha"] = resumo["produtividade_sc_ha"] or (nums[-1] if nums else None)
            if "ponto de equil" in row_str or "ponto equil" in row_str:
                nums = extrai_numericos(row_vals)
                if nums:
                    if not resumo["pe_coe_sc_ha"]:
                        resumo["pe_coe_sc_ha"] = nums[-1]
                    elif not resumo["pe_ct_sc_ha"]:
                        resumo["pe_ct_sc_ha"] = nums[-1]
            # Extrair linha de item de custo
            primeiro = row_vals[0].lower() if row_vals else ""
            if not primeiro or primeiro in SKIP or primeiro.startswith("custo"):
                continue
            nums = extrai_numericos(row_vals)
            if len(nums) < 2:
                continue
            grupo  = limpa(row_vals[0])
            item   = limpa(row_vals[1]) if len(row_vals) > 1 else grupo
            unidad = limpa(row_vals[2]) if len(row_vals) > 2 else ""
            if not item or len(item) < 3:
                continue
            itens.append({
                "cultura":        cultura,
                "nome_relatorio": nome_relatorio,
                "safra":          safra,
                "mes_referencia": mes_ref,
                "regiao":         regiao,
                "grupo_custo":    grupo,
                "item":           item,
                "unidade":        unidad,
                "quantidade":     nums[0] if len(nums) > 0 else None,
                "preco_unitario": nums[1] if len(nums) > 1 else None,
                "valor_ha":       nums[-1],
                "tipo_custo":     tipo_custo_atual,
                "updated_at":     datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "relatorio_id":   None,
            })
    log.info(f"  Itens: {len(itens)} | COE={resumo['coe_ha']} | CT={resumo['ct_ha']}")
    return {"safra": safra, "mes_ref": mes_ref, "itens": itens, "resumo": resumo}
# ── Pipeline por cultura ───────────────────────────────────────────────────────
def processa_cultura(cultura: str, cadeia_id: str):
    relatorios = lista_relatorios_api(cultura, cadeia_id)
    if not relatorios:
        log.info(f"[{cultura}] Nenhum relatório encontrado na API — nada a fazer.")
        return
    novos = 0
    for rel in relatorios:
        imea_id  = rel.get("Id", "")
        nome     = rel.get("Nome", "")
        url_s3   = rel.get("Path", "")
        data_pub = (rel.get("Data") or "")[:10]   # "2026-03-16T00:00:00" → "2026-03-16"
        if not url_s3:
            log.warning(f"  Sem URL para '{nome}' — pulando.")
            continue
        log.info(f"  → {nome} ({data_pub})")
        try:
            resp = requests.get(url_s3, headers=HEADERS, timeout=60)
            resp.raise_for_status()
            content = resp.content
        except Exception as e:
            log.error(f"  Erro no download: {e}")
            continue
        h = md5(content)
        if hash_existe(conn, h):
            log.info(f"  ↳ Já existe no banco (md5={h[:8]}…) — pulando.")
            continue
        nome_arquivo = f"{cultura}_{imea_id}.xlsx"
        # Parse do Excel
        try:
            parsed = parse_excel(content, cultura, nome)
        except Exception as e:
            log.error(f"  Erro no parse: {e}", exc_info=True)
            continue
        safra   = parsed["safra"]   or extrai_safra(data_pub)
        mes_ref = parsed["mes_ref"] or data_pub[:7]   # fallback: "2026-03"
        # Inserir meta do relatório
        cur = conn.execute(
            """INSERT OR IGNORE INTO relatorios
               (imea_id, cultura, nome, safra, mes_referencia, data_publicacao,
                nome_arquivo, url_s3, hash_md5, coletado_em)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (imea_id, cultura, nome, safra, mes_ref, data_pub,
             nome_arquivo, url_s3, h, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        )
        conn.commit()
        rel_id = cur.lastrowid
        log.info(f"  ↳ Inserido: id={rel_id} | safra={safra} | mes={mes_ref}")
        # Inserir itens
        itens = parsed["itens"]
        for item in itens:
            item["relatorio_id"] = rel_id
        upsert_itens(conn, itens)
        log.info(f"  ↳ {len(itens)} itens inseridos")
        # Inserir resumo
        resumo = parsed["resumo"]
        resumo.update({
            "relatorio_id":   rel_id,
            "cultura":        cultura,
            "nome_relatorio": nome,
            "safra":          safra,
            "mes_referencia": mes_ref,
            "regiao":         None,
            "updated_at":     datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })
        upsert_resumo(conn, resumo)
        novos += 1
        time.sleep(0.5)   # respeita o servidor
    if novos == 0:
        log.info(f"[{cultura}] Base já atualizada — nenhum arquivo novo.")
    else:
        log.info(f"[{cultura}] {novos} novo(s) relatório(s) inserido(s).")
# ── Main ───────────────────────────────────────────────────────────────────────
for cultura, cadeia_id in CULTURAS.items():
    try:
        processa_cultura(cultura, cadeia_id)
    except Exception as e:
        log.error(f"ERRO em {cultura}: {e}", exc_info=True)
conn.close()
print("\nConcluído.")
