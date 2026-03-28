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

API_TOKEN = "https://api1.imea.com.br/token"
API_DADOS = "https://api1.imea.com.br/api/seriehistorica"   # endpoint correto!

GRUPO_CUSTO_ID  = "1121328740175912960"
ESTADO_MT       = "51"
TIPO_LOCALIDADE = "1"

# cadeia_id → safras (descobertas via DevTools)
CULTURAS = {
    "SOJA": {
        "cadeia_id": "4",
        "safras": ["1595648460215812096", "1484351182193295360", "1335026912394682368", "1174122980756627456"],
    },
    "MILHO": {
        "cadeia_id": "3",
        "safras": ["1595648460215812096", "1335026912394682368", "1484351182193295360", "1174122980756627456"],
    },
    # Algodão: descomentar após obter safras via DevTools
    # "ALGODAO": {
    #     "cadeia_id": "1",
    #     "safras": [],
    # },
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
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
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Referer": "https://portal.imea.com.br/",
            "Origin":  "https://portal.imea.com.br",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        },
        timeout=30,
    )
    resp.raise_for_status()
    token = resp.json()["access_token"]
    log.info("  Token obtido com sucesso.")
    return token


# ── Buscar dados históricos (todos os indicadores de uma vez) ──────────────────
def get_dados(token: str, cadeia_id: str, safras: list[str], page: int = 1) -> dict:
    resp = requests.post(
        API_DADOS,
        json={
            "cadeia":         [cadeia_id],
            "grupo":          [GRUPO_CUSTO_ID],
            "indicador":      [],           # vazio = todos os indicadores
            "estado":         [ESTADO_MT],
            "regiao":         [],
            "safra":          safras,
            "tipolocalidade": [TIPO_LOCALIDADE],
            "inicio":         "",
            "fim":            "",
            "cidade":         [],
            "cidadeDestino":  [],
            "estadoDestino":  [],
            "regiaoDestino":  [],
            "tipoDestino":    [],
            "pageSize":       1000,
            "page":           page,
        },
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type":  "application/json",
            "Referer":       "https://portal.imea.com.br/",
            "Origin":        "https://portal.imea.com.br",
        },
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()


# ── Upsert no banco ────────────────────────────────────────────────────────────
def upsert_dados(cultura: str, cadeia_id: str, rows: list) -> int:
    if not rows:
        return 0
    registros = []
    for r in rows:
        try:
            valor = float(r.get("Valor") or r.get("valor2") or 0)
        except (ValueError, TypeError):
            continue
        data_str = (r.get("Data") or "")[:10]
        registros.append((
            cultura,
            cadeia_id,
            str(r.get("IndicadorFinalId", "")),
            r.get("IndicadorFinalNome", ""),
            r.get("SafraDescricao", ""),
            str(r.get("SafraId", "")),
            data_str,
            r.get("Ano"),
            r.get("Mes"),
            valor,
            r.get("UnidadeSigla", ""),
            r.get("EstadoSigla", "MT"),
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        ))
    conn.executemany(
        """INSERT OR IGNORE INTO historico
           (cultura, cadeia_id, indicador_id, indicador_nome,
            safra, safra_id, data_referencia, ano, mes,
            valor, unidade, estado, updated_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        registros,
    )
    conn.commit()
    return len(registros)


# ── Pipeline principal ─────────────────────────────────────────────────────────
token = get_token()
total = 0

for cultura, cfg in CULTURAS.items():
    cadeia_id = cfg["cadeia_id"]
    safras    = cfg["safras"]

    log.info(f"\n{'='*50}")
    log.info(f"Processando: {cultura} (cadeia={cadeia_id}, {len(safras)} safras)")
    log.info(f"{'='*50}")

    page = 1
    while True:
        try:
            data = get_dados(token, cadeia_id, safras, page)
        except Exception as e:
            log.error(f"  Erro ao buscar dados (page={page}): {e}")
            break

        rows       = data if isinstance(data, list) else data.get("Result", [])
        page_count = 1 if isinstance(data, list) else data.get("PageCount", 1)
        total_count = len(rows) if isinstance(data, list) else data.get("TotalCount", 0)

        log.info(f"  page={page}/{page_count} → {len(rows)} registros (total={total_count})")

        inseridos = upsert_dados(cultura, cadeia_id, rows)
        total += inseridos
        log.info(f"  {inseridos} inseridos no banco")

        if page >= page_count:
            break
        page += 1
        time.sleep(0.5)

    log.info(f"[{cultura}] Concluído.")

conn.close()
log.info(f"\n✓ Total inserido: {total} registros")
print("\nConcluído.")
