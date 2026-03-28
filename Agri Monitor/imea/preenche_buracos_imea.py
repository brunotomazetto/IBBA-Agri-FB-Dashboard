import sqlite3
import os
import logging
import time
from datetime import datetime
import requests

DB_PATH = os.path.join(os.path.dirname(__file__), "imea.db")
IMEA_USER = os.environ["IMEA_USER"]
IMEA_PASS = os.environ["IMEA_PASS"]

API_TOKEN       = "https://api1.imea.com.br/token"
API_INDICADORES = "https://api1.imea.com.br/api/indicadorfinal/seriehistoricageral"
API_DADOS       = "https://api1.imea.com.br/api/seriehistorica"

GRUPO_CUSTO_ID  = "1121328740175912960"
ESTADO_MT       = "51"
TIPO_LOCALIDADE = "1"

# ── Meses exatamente faltantes por cultura ────────────────────────────────────
BURACOS = {
    "SOJA": {
        "cadeia_id": "4",
        "janelas": [
            # mai/23
            ("1335026912394682368", "23/24", "2023-05-01", "2023-05-31"),
            # abr/24
            ("1484351182193295360", "24/25", "2024-04-01", "2024-04-30"),
            # out/24 → nov/25
            ("1484351182193295360", "24/25", "2024-10-01", "2025-11-30"),
            ("1595648460215812096", "25/26", "2024-10-01", "2025-11-30"),
        ],
    },
    "ALGODAO": {
        "cadeia_id": "1",
        "janelas": [
            # dez/24 → nov/25
            ("1484351182193295360", "24/25", "2024-12-01", "2025-11-30"),
            ("1595648460215812096", "25/26", "2024-12-01", "2025-11-30"),
        ],
    },
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

conn = sqlite3.connect(DB_PATH)

def get_token():
    resp = requests.post(API_TOKEN,
        data={"username": IMEA_USER, "password": IMEA_PASS,
              "grant_type": "password", "client_id": "2"},
        headers={"Content-Type": "application/x-www-form-urlencoded",
                 "Referer": "https://portal.imea.com.br/"},
        timeout=30)
    resp.raise_for_status()
    return resp.json()["access_token"]

def hdrs(token):
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json",
            "Referer": "https://portal.imea.com.br/"}

def get_indicadores(token, cadeia_id, safra_ids):
    todos, page = [], 1
    while True:
        resp = requests.post(
            f"{API_INDICADORES}?nome=&pageSize=100&page={page}",
            json={"nome": "", "pageSize": 100, "page": page,
                  "cadeia": [cadeia_id], "grupo": [GRUPO_CUSTO_ID],
                  "indicador": [], "estado": [ESTADO_MT], "safra": safra_ids,
                  "tipolocalidade": [TIPO_LOCALIDADE], "regiao": [], "inicio": "", "fim": "",
                  "cidade": [], "cidadeDestino": [], "estadoDestino": [],
                  "regiaoDestino": [], "tipoDestino": []},
            headers=hdrs(token), timeout=30)
        resp.raise_for_status()
        data = resp.json()
        todos.extend(data.get("Result", []))
        if page >= data.get("PageCount", 1): break
        page += 1
    return todos

def get_dados(token, cadeia_id, ind_id, safra_id, inicio, fim):
    resp = requests.post(API_DADOS,
        json={"cadeia": [cadeia_id], "grupo": [GRUPO_CUSTO_ID],
              "indicador": [ind_id], "estado": [ESTADO_MT],
              "safra": [safra_id], "tipolocalidade": [TIPO_LOCALIDADE],
              "regiao": [], "inicio": inicio, "fim": fim,
              "cidade": [], "cidadeDestino": [], "estadoDestino": [],
              "regiaoDestino": [], "tipoDestino": []},
        headers=hdrs(token), timeout=60)
    resp.raise_for_status()
    data = resp.json()
    return data if isinstance(data, list) else data.get("Result", [])

def upsert(cultura, cadeia_id, rows):
    if not rows: return 0
    registros = []
    for r in rows:
        try: valor = float(r.get("Valor") or r.get("valor2") or 0)
        except: continue
        if valor <= 0: continue
        registros.append((
            cultura, cadeia_id,
            str(r.get("IndicadorFinalId", "")),
            r.get("IndicadorFinalNome", ""),
            r.get("SafraDescricao", ""),
            str(r.get("SafraId", "") or ""),
            r.get("SafraTipoDescricao", ""),
            (r.get("Data") or "")[:10],
            r.get("Ano"), r.get("Mes"),
            valor, r.get("UnidadeSigla", ""),
            r.get("EstadoSigla", "MT"),
            "CUSTO",
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        ))
    conn.executemany("""
        INSERT OR IGNORE INTO historico
        (cultura, cadeia_id, indicador_id, indicador_nome, safra, safra_id,
         safra_tipo, data_referencia, ano, mes, valor, unidade, estado,
         grupo, updated_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", registros)
    conn.commit()
    return len(registros)

# ── Main ───────────────────────────────────────────────────────────────────────
token = get_token()
log.info("Token obtido.")
total = 0

for cultura, cfg in BURACOS.items():
    cadeia_id = cfg["cadeia_id"]
    janelas   = cfg["janelas"]
    log.info(f"\n[{cultura}] {len(janelas)} janelas a preencher")

    safra_ids = list(dict.fromkeys(j[0] for j in janelas))
    indicadores = get_indicadores(token, cadeia_id, safra_ids)
    log.info(f"  {len(indicadores)} indicadores encontrados")

    for ind in indicadores:
        ind_id   = str(ind.get("Id", ""))
        ind_nome = ind.get("IndicadorNome", "")
        ind_total = 0

        for safra_id, safra_desc, inicio, fim in janelas:
            try:
                dados = get_dados(token, cadeia_id, ind_id, safra_id, inicio, fim)
            except Exception as e:
                log.error(f"    Erro {safra_desc} {inicio}: {e}")
                continue
            ins = upsert(cultura, cadeia_id, dados)
            ind_total += ins
            time.sleep(0.1)

        if ind_total > 0:
            log.info(f"  ✓ {ind_nome}: {ind_total} registros")
        total += ind_total

# Resumo
print("\n=== DATAS APÓS PREENCHIMENTO ===")
for cultura in ["SOJA", "ALGODAO"]:
    datas = [r[0] for r in conn.execute("""
        SELECT DISTINCT data_referencia FROM historico
        WHERE cultura=? AND grupo='CUSTO'
        AND data_referencia BETWEEN '2023-01-01' AND '2026-02-28'
        ORDER BY data_referencia
    """, (cultura,))]
    print(f"\n{cultura}: {[d[:7] for d in datas]}")

conn.close()
log.info(f"\n✓ Total inserido: {total} registros")
print("\nConcluído.")
