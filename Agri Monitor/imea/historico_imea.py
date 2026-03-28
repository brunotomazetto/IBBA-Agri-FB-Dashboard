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

ESTADO_MT       = "51"
TIPO_LOCALIDADE = "1"

# Grupos a coletar
GRUPOS = {
    "PRECO":        "697311317411758080",   # Preço — diário, sem safra
    "PRODUTIVIDADE":"697311317415952387",   # Safra — com safra
}

CULTURAS = {
    "SOJA":    "4",
    "MILHO":   "3",
    "ALGODAO": "1",
}

# Janelas de busca por ano (preço é diário — usar janelas anuais)
JANELAS_ANUAIS = [
    ("2020-01-01", "2020-12-31"),
    ("2021-01-01", "2021-12-31"),
    ("2022-01-01", "2022-12-31"),
    ("2023-01-01", "2023-12-31"),
    ("2024-01-01", "2024-12-31"),
    ("2025-01-01", "2025-12-31"),
    ("2026-01-01", "2026-12-31"),
]

# Safras para produtividade
SAFRAS = [
    "1595648460215812096",  # 25/26
    "1484351182193295360",  # 24/25
    "1335026912394682368",  # 23/24
    "1174122980756627456",  # 22/23
]

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
        safra_tipo       TEXT,
        data_referencia  TEXT,
        ano              INTEGER,
        mes              INTEGER,
        valor            REAL,
        unidade          TEXT,
        estado           TEXT,
        grupo            TEXT,
        updated_at       TEXT,
        UNIQUE(indicador_id, safra_id, data_referencia, estado)
    );
    CREATE INDEX IF NOT EXISTS idx_hist_cultura ON historico(cultura);
    CREATE INDEX IF NOT EXISTS idx_hist_grupo   ON historico(grupo);
""")
conn.commit()

# ── Autenticação ───────────────────────────────────────────────────────────────
def get_token() -> str:
    log.info("Autenticando...")
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
def get_indicadores(token, cadeia_id, grupo_id, safras=[]) -> list:
    todos, page = [], 1
    while True:
        resp = requests.post(
            f"{API_INDICADORES}?nome=&pageSize=100&page={page}",
            json={"nome": "", "pageSize": 100, "page": page,
                  "cadeia": [cadeia_id], "grupo": [grupo_id],
                  "indicador": [], "estado": [ESTADO_MT], "safra": safras,
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


# ── Buscar dados ───────────────────────────────────────────────────────────────
def get_dados(token, cadeia_id, grupo_id, indicador_id, inicio, fim, safras=[]) -> list:
    resp = requests.post(
        API_DADOS,
        json={"cadeia": [cadeia_id], "grupo": [grupo_id],
              "indicador": [indicador_id], "estado": [ESTADO_MT],
              "safra": safras, "tipolocalidade": [TIPO_LOCALIDADE],
              "regiao": [], "inicio": inicio, "fim": fim,
              "cidade": [], "cidadeDestino": [], "estadoDestino": [],
              "regiaoDestino": [], "tipoDestino": []},
        headers=hdrs(token), timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()
    return data if isinstance(data, list) else data.get("Result", [])


# ── Upsert ─────────────────────────────────────────────────────────────────────
def upsert(cultura, cadeia_id, grupo_nome, rows) -> int:
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
            str(r.get("SafraId", "") or ""),
            r.get("SafraTipoDescricao", ""),
            (r.get("Data") or "")[:10],
            r.get("Ano"), r.get("Mes"),
            valor, r.get("UnidadeSigla", ""),
            r.get("EstadoSigla", "MT"),
            grupo_nome,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        ))
    conn.executemany(
        """INSERT OR IGNORE INTO historico
           (cultura, cadeia_id, indicador_id, indicador_nome, safra, safra_id,
            safra_tipo, data_referencia, ano, mes, valor, unidade, estado,
            grupo, updated_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        registros,
    )
    conn.commit()
    return len(registros)


# ── Main ───────────────────────────────────────────────────────────────────────
token  = get_token()
total  = 0

for cultura, cadeia_id in CULTURAS.items():
    log.info(f"\n{'='*50}\n{cultura}\n{'='*50}")

    # ── PREÇO (diário, sem safra) ──────────────────────────────────────────────
    grupo_id   = GRUPOS["PRECO"]
    indicadores = get_indicadores(token, cadeia_id, grupo_id)
    log.info(f"  Preço: {len(indicadores)} indicadores")

    for ind in indicadores:
        ind_id   = str(ind.get("Id", ""))
        ind_nome = ind.get("IndicadorNome", "")
        ind_total = 0

        for inicio, fim in JANELAS_ANUAIS:
            try:
                dados = get_dados(token, cadeia_id, grupo_id, ind_id, inicio, fim)
            except Exception as e:
                log.error(f"    Erro {inicio}: {e}")
                continue
            inseridos = upsert(cultura, cadeia_id, "PRECO", dados)
            ind_total += inseridos

        if ind_total > 0:
            log.info(f"  ✓ {ind_nome}: {ind_total} registros")

    # ── PRODUTIVIDADE (por safra) ──────────────────────────────────────────────
    grupo_id    = GRUPOS["PRODUTIVIDADE"]
    indicadores = get_indicadores(token, cadeia_id, grupo_id, SAFRAS)
    log.info(f"  Produtividade: {len(indicadores)} indicadores")

    for ind in indicadores:
        ind_id   = str(ind.get("Id", ""))
        ind_nome = ind.get("IndicadorNome", "")
        ind_total = 0

        for safra_id in SAFRAS:
            for inicio, fim in JANELAS_ANUAIS:
                try:
                    dados = get_dados(token, cadeia_id, grupo_id, ind_id,
                                      inicio, fim, [safra_id])
                except Exception as e:
                    log.error(f"    Erro {safra_id} {inicio}: {e}")
                    continue
                inseridos = upsert(cultura, cadeia_id, "PRODUTIVIDADE", dados)
                ind_total += inseridos

        if ind_total > 0:
            log.info(f"  ✓ {ind_nome}: {ind_total} registros")

    log.info(f"[{cultura}] Concluído.")

conn.close()
log.info(f"\n✓ Total inserido: {total} registros")
print("\nConcluído.")
