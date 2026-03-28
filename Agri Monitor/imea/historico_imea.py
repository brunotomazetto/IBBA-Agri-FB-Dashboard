import sqlite3
import os
import logging
import time
from datetime import datetime
import requests

# ── Configurações ──────────────────────────────────────────────────────────────
DB_PATH = os.path.join(os.path.dirname(__file__), "imea.db")

# Credenciais via GitHub Secrets (nunca hardcoded no código)
IMEA_USER = os.environ["IMEA_USER"]
IMEA_PASS = os.environ["IMEA_PASS"]

# Endpoints da API do portal IMEA
API_TOKEN      = "https://api1.imea.com.br/token"
API_INDICADORES = "https://api1.imea.com.br/api/indicadorfinal/seriehistoricageral"
API_DADOS      = "https://api1.imea.com.br/api/seriehistorica/dados"
API_SAFRAS     = "https://api1.imea.com.br/api/safra"

# Constantes fixas (descobertas via DevTools)
GRUPO_CUSTO_ID  = "1121328740175912960"   # "Custo de Produção"
ESTADO_MT       = "51"
TIPO_LOCALIDADE = "1"                     # "Estado"

# safras descobertas via DevTools do portal IMEA
CULTURAS = {
    "SOJA": {
        "cadeia_id": "4",
        "safras": ["1484351182193295360", "1335026912394682368", "1174122980756627456"],
    },
    # descomentar após validar soja:
    # "MILHO": {
    #     "cadeia_id": "3",
    #     "safras": ["1595648460215812096", "1335026912394682368", "1484351182193295360", "1174122980756627456"],
    # },
    # "ALGODAO": {
    #     "cadeia_id": "1",
    #     "safras": [],  # preencher após obter do DevTools
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
""")
conn.commit()

# ── Autenticação ───────────────────────────────────────────────────────────────
def get_token() -> str:
    log.info("Autenticando no portal IMEA...")
    resp = requests.post(
        API_TOKEN,
        data={
            "username":   IMEA_USER,
            "password":   IMEA_PASS,
            "grant_type": "password",
            "client_id":  "2",
        },
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Content-Type": "application/x-www-form-urlencoded",
            "Referer": "https://portal.imea.com.br/",
            "Origin":  "https://portal.imea.com.br",
        },
        timeout=30,
    )
    resp.raise_for_status()
    token = resp.json()["access_token"]
    log.info(f"  Token obtido — expira em {resp.json().get('.expires')}")
    return token


def auth_headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}



# ── Buscar safras disponíveis ──────────────────────────────────────────────────
def get_safras(token: str, cadeia_id: str) -> list[str]:
    """Retorna lista de IDs de todas as safras disponíveis para a cadeia."""
    resp = requests.get(
        f"{API_SAFRAS}?cadeia={cadeia_id}&pageSize=100&page=1",
        headers=auth_headers(token),
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    items = data if isinstance(data, list) else data.get("Result", [])
    ids = [str(s.get("Id") or s.get("id") or "") for s in items if s.get("Id") or s.get("id")]
    log.info(f"  {len(ids)} safras encontradas")
    return ids


# ── Buscar todos os indicadores de custo de uma cultura ───────────────────────
def get_indicadores(token: str, cadeia_id: str) -> list[dict]:
    """
    Busca todos os indicadores do grupo 'Custo de Produção' para a cadeia.
    Pagina até trazer tudo.
    """
    todos = []
    page  = 1
    while True:
        resp = requests.post(
            f"{API_INDICADORES}?nome=&pageSize=100&page={page}",
            json={
                "nome":           "",
                "pageSize":       100,
                "page":           page,
                "cadeia":         [cadeia_id],
                "grupo":          [GRUPO_CUSTO_ID],
                "indicador":      [],
                "estado":         [ESTADO_MT],
                "regiao":         [],
                "safra":          [],
                "tipolocalidade": [TIPO_LOCALIDADE],
                "inicio":        "",
                "fim":           "",
                "cidade":         [],
                "cidadeDestino":  [],
                "estadoDestino":  [],
                "regiaoDestino":  [],
                "tipoDestino":    [],
            },
            headers=auth_headers(token),
            timeout=30,
        )
        resp.raise_for_status()
        data    = resp.json()
        results = data.get("Result", [])
        todos.extend(results)
        if page >= data.get("PageCount", 1):
            break
        page += 1
        pass

    log.info(f"  {len(todos)} indicadores encontrados")
    return todos


# ── Buscar dados históricos de um indicador ────────────────────────────────────
def get_dados(token: str, cadeia_id: str, indicador_id: str, safras: list[str]) -> list[dict]:
    """
    Busca toda a série histórica de um indicador para MT.
    Passa todas as safras disponíveis para garantir retorno completo.
    """
    resp = requests.post(
        API_DADOS,
        json={
            "cadeia":         [cadeia_id],
            "grupo":          [GRUPO_CUSTO_ID],
            "indicador":      [indicador_id],
            "estado":         [ESTADO_MT],
            "regiao":         [],
            "safra":          safras,
            "tipolocalidade": [TIPO_LOCALIDADE],
            "inicio":        "",
            "fim":           "",
            "cidade":         [],
            "cidadeDestino":  [],
            "estadoDestino":  [],
            "regiaoDestino":  [],
            "tipoDestino":    [],
        },
        headers=auth_headers(token),
        timeout=60,
    )
    log.info(f"    HTTP {resp.status_code} | tamanho={len(resp.content)} bytes | body={resp.text[:200]}")
    resp.raise_for_status()
    if not resp.content:
        return []
    data = resp.json()
    return data if isinstance(data, list) else data.get("Result", [])


# ── Upsert no banco ────────────────────────────────────────────────────────────
def upsert_dados(cultura: str, cadeia_id: str, rows: list[dict]):
    if not rows:
        return 0

    registros = []
    for r in rows:
        try:
            valor = float(r.get("Valor") or r.get("valor2") or 0)
        except (ValueError, TypeError):
            continue

        data_str = (r.get("Data") or "")[:10]  # "2023-04-17"
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

    try:
        indicadores = get_indicadores(token, cadeia_id)
    except Exception as e:
        log.error(f"  Erro ao buscar safras/indicadores: {e}")
        continue

    for ind in indicadores[:1]:  # TESTE  # TESTE: remover [:3] para rodar completo
        ind_id   = str(ind.get("Id", ""))
        ind_nome = ind.get("IndicadorNome", "")

        log.info(f"  → [{ind_id}] {ind_nome}")

        try:
            dados = get_dados(token, cadeia_id, ind_id, safras)
        except Exception as e:
            log.error(f"    Erro ao buscar dados: {e}")
            pass
            continue

        inseridos = upsert_dados(cultura, cadeia_id, dados)
        total += inseridos
        log.info(f"    {inseridos} registros inseridos ({len(dados)} retornados)")
        pass

    log.info(f"[{cultura}] Concluído.")

conn.close()
log.info(f"\n✓ Histórico completo. Total de registros: {total}")
print("\nConcluído.")
