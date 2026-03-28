import sqlite3
import os
import logging
import time
from datetime import datetime
import requests

# ── Configurações ──────────────────────────────────────────────────────────────
DB_PATH = os.path.join(os.path.dirname(__file__), "imea.db")

IMEA_USER = os.environ["IMEA_USER"]
IMEA_PASS = os.environ["IMEA_PASS"]

API_TOKEN       = "https://api1.imea.com.br/token"
API_INDICADORES = "https://api1.imea.com.br/api/indicadorfinal/seriehistoricageral"
API_DADOS       = "https://api1.imea.com.br/api/seriehistorica"

GRUPO_CUSTO_ID  = "1121328740175912960"
ESTADO_MT       = "51"
TIPO_LOCALIDADE = "1"

# Safras com janelas de 6 meses para contornar limite de 10 registros
CULTURAS = {
    "SOJA": {
        "cadeia_id": "4",
        "safras": [
            # (safra_id, safra_desc, inicio, fim)
            ("1595648460215812096", "25/26", "2025-09-01", "2026-12-31"),
            ("1484351182193295360", "24/25", "2024-04-01", "2024-12-31"),
            ("1484351182193295360", "24/25", "2023-09-01", "2024-03-31"),
            ("1335026912394682368", "23/24", "2023-04-01", "2023-12-31"),
            ("1335026912394682368", "23/24", "2022-09-01", "2023-03-31"),
            ("1174122980756627456", "22/23", "2023-04-01", "2023-09-30"),
            ("1174122980756627456", "22/23", "2022-09-01", "2023-03-31"),
        ],
    },
    "MILHO": {
        "cadeia_id": "3",
        "safras": [
            ("1595648460215812096", "25/26", "2025-09-01", "2026-12-31"),
            ("1484351182193295360", "24/25", "2024-04-01", "2024-12-31"),
            ("1484351182193295360", "24/25", "2023-09-01", "2024-03-31"),
            ("1335026912394682368", "23/24", "2023-04-01", "2023-12-31"),
            ("1335026912394682368", "23/24", "2022-09-01", "2023-03-31"),
            ("1174122980756627456", "22/23", "2023-04-01", "2023-09-30"),
            ("1174122980756627456", "22/23", "2022-09-01", "2023-03-31"),
        ],
    },
    "ALGODAO": {
        "cadeia_id": "1",
        "safras": [
            ("1595648460215812096", "25/26", "2025-09-01", "2026-12-31"),
            ("1484351182193295360", "24/25", "2024-04-01", "2024-12-31"),
            ("1484351182193295360", "24/25", "2023-09-01", "2024-03-31"),
            ("1335026912394682368", "23/24", "2023-04-01", "2023-12-31"),
            ("1335026912394682368", "23/24", "2022-09-01", "2023-03-31"),
            ("1174122980756627456", "22/23", "2023-04-01", "2023-09-30"),
            ("1174122980756627456", "22/23", "2022-09-01", "2023-03-31"),
        ],
    },
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ── Inicializa banco ───────────────────────────────────────────────────────────
conn = sqlite3.connect(DB_PATH)
conn.executescript("""
    CREATE TABLE IF NOT EXISTS historico (
        id               INTEGER PRIMARY KEY AUTOINCREMENT,
        cultura          TEXT    NOT NULL,
        cadeia_id        TEXT,
        indicador_id     TEXT,
        indicador_nome   TEXT,
        safra            TEXT,
        safra_id         TEXT,
        data_referencia  TEXT,
        ano              INTEGER,
        mes              INTEGER,
        valor            REAL,
        unidade          TEXT,
        estado           TEXT,
        updated_at       TEXT,
        UNIQUE(indicador_id, safra_id, data_referencia, estado)
    );
    CREATE INDEX IF NOT EXISTS idx_hist_cultura ON historico(cultura);
    CREATE INDEX IF NOT EXISTS idx_hist_ind     ON historico(indicador_id);
    CREATE INDEX IF NOT EXISTS idx_hist_safra   ON historico(safra_id);
""")
conn.commit()

# ── Autenticação ───────────────────────────────────────────────────────────────
def get_token() -> str:
    log.info("Autenticando no portal IMEA...")
    resp = requests.post(
        API_TOKEN,
        data={"username": IMEA_USER, "password": IMEA_PASS,
              "grant_type": "password", "client_id": "2"},
        headers={"Content-Type": "application/x-www-form-urlencoded",
                 "Referer": "https://portal.imea.com.br/"},
        timeout=30,
    )
    resp.raise_for_status()
    log.info("  Token obtido.")
    return resp.json()["access_token"]


def hdrs(token):
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json",
            "Referer": "https://portal.imea.com.br/"}


# ── Buscar indicadores ─────────────────────────────────────────────────────────
def get_indicadores(token, cadeia_id, safra_ids) -> list:
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
            headers=hdrs(token), timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        todos.extend(data.get("Result", []))
        if page >= data.get("PageCount", 1):
            break
        page += 1
    return todos


# ── Buscar dados por indicador + safra + período ───────────────────────────────
def get_dados(token, cadeia_id, indicador_id, safra_id, inicio, fim) -> list:
    resp = requests.post(
        API_DADOS,
        json={"cadeia": [cadeia_id], "grupo": [GRUPO_CUSTO_ID],
              "indicador": [indicador_id], "estado": [ESTADO_MT],
              "safra": [safra_id], "tipolocalidade": [TIPO_LOCALIDADE],
              "regiao": [], "inicio": inicio, "fim": fim,
              "cidade": [], "cidadeDestino": [], "estadoDestino": [],
              "regiaoDestino": [], "tipoDestino": []},
        headers=hdrs(token), timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()
    return data if isinstance(data, list) else data.get("Result", [])


# ── Upsert ─────────────────────────────────────────────────────────────────────
def upsert(cultura, cadeia_id, rows) -> int:
    if not rows:
        return 0
    registros = []
    for r in rows:
        try:
            valor = float(r.get("Valor") or r.get("valor2") or 0)
        except (ValueError, TypeError):
            continue
        registros.append((
            cultura, cadeia_id,
            str(r.get("IndicadorFinalId", "")),
            r.get("IndicadorFinalNome", ""),
            r.get("SafraDescricao", ""),
            str(r.get("SafraId", "")),
            (r.get("Data") or "")[:10],
            r.get("Ano"), r.get("Mes"),
            valor, r.get("UnidadeSigla", ""),
            r.get("EstadoSigla", "MT"),
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        ))
    conn.executemany(
        """INSERT OR IGNORE INTO historico
           (cultura, cadeia_id, indicador_id, indicador_nome, safra, safra_id,
            data_referencia, ano, mes, valor, unidade, estado, updated_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        registros,
    )
    conn.commit()
    return len(registros)


# ── Main ───────────────────────────────────────────────────────────────────────
token = get_token()
total = 0

for cultura, cfg in CULTURAS.items():
    cadeia_id = cfg["cadeia_id"]
    safras    = cfg["safras"]

    log.info(f"\n{'='*50}\nProcessando: {cultura}\n{'='*50}")

    # Buscar lista de indicadores usando todas as safras únicas
    safra_ids_unicos = list(dict.fromkeys(s[0] for s in safras))
    indicadores = get_indicadores(token, cadeia_id, safra_ids_unicos)
    log.info(f"  {len(indicadores)} indicadores encontrados")

    for ind in indicadores:
        ind_id   = str(ind.get("Id", ""))
        ind_nome = ind.get("IndicadorNome", "")
        ind_total = 0

        for safra_id, safra_desc, inicio, fim in safras:
            try:
                dados = get_dados(token, cadeia_id, ind_id, safra_id, inicio, fim)
            except Exception as e:
                log.error(f"    Erro {safra_desc} {inicio}: {e}")
                continue
            inseridos = upsert(cultura, cadeia_id, dados)
            ind_total += inseridos

        if ind_total > 0:
            log.info(f"  ✓ {ind_nome}: {ind_total} registros")

    log.info(f"[{cultura}] Concluído.")

conn.close()
log.info(f"\n✓ Total inserido: {total} registros")
print("\nConcluído.")
