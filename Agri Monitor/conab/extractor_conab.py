import requests
import pandas as pd
import sqlite3
import io
import os
from datetime import datetime

# ── Configuracoes ──────────────────────────────────────────────────────────────
DB_PATH = "conab/conab.db"

URLS = {
    "graos": "https://portaldeinformacoes.conab.gov.br/downloads/arquivos/LevantamentoGraos.txt",
    "cana":  "https://portaldeinformacoes.conab.gov.br/downloads/arquivos/LevantamentoCana.txt",
}

# Nomes exatos como aparecem na coluna PRODUTO do arquivo da CONAB.
# Inclui as tres safras do milho separadamente + total (caso exista no arquivo).
PRODUTOS_GRAOS = [
    "SOJA",
    "MILHO 1ª SAFRA",
    "MILHO 2ª SAFRA",
    "MILHO 3ª SAFRA",
    "MILHO TOTAL",
    "MILHO TOTAL (1ª+2ª SAFRAS)",
    "MILHO TOTAL (1ª+2ª+3ª SAFRAS)",
    "ALGODÃO EM PLUMA",
    "ALGODÃO TOTAL (PLUMA)",
]

# ── Inicializa banco ───────────────────────────────────────────────────────────
os.makedirs("conab", exist_ok=True)

conn = sqlite3.connect(DB_PATH)
conn.execute("""
    CREATE TABLE IF NOT EXISTS safra (
        produto        TEXT,
        safra          TEXT,
        levantamento   INTEGER,
        estado         TEXT,
        area_mil_ha    REAL,
        produtividade  REAL,
        producao_mil_t REAL,
        updated_at     TEXT,
        PRIMARY KEY (produto, safra, levantamento, estado)
    )
""")
conn.commit()

# ── Utilitarios ────────────────────────────────────────────────────────────────
def baixa_txt(url):
    print(f"Baixando {url}...")
    r = requests.get(url, timeout=180, verify=False)
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.content.decode("latin1")), sep=";", dtype=str)
    df.columns = [c.strip() for c in df.columns]
    print(f"  -> {len(df)} linhas, colunas: {list(df.columns)}")
    return df

def parse_float(val):
    try:
        return float(str(val).replace(".", "").replace(",", ".").strip())
    except (ValueError, AttributeError):
        return None

def col(df, *palavras):
    for p in palavras:
        c = next((c for c in df.columns if p.upper() in c.upper()), None)
        if c:
            return c
    return None

def upsert(conn, df):
    df.to_sql("safra", conn, if_exists="append", index=False, method="multi")
    conn.execute("""
        DELETE FROM safra WHERE rowid NOT IN (
            SELECT MAX(rowid) FROM safra GROUP BY produto, safra, levantamento, estado
        )
    """)
    conn.commit()

# ── Graos (Soja, Milho 1a/2a/3a, Algodao) ─────────────────────────────────────
try:
    df_graos = baixa_txt(URLS["graos"])

    c_prod   = col(df_graos, "PRODUTO")
    c_safra  = col(df_graos, "SAFRA")
    c_lev    = col(df_graos, "LEVANTAMENTO")
    c_estado = col(df_graos, "ESTADO", "UF")
    c_area   = col(df_graos, "AREA")
    c_produ  = col(df_graos, "PRODUTIVIDADE")
    c_prodc  = col(df_graos, "PRODUCAO", "PRODUCAO")

    df_graos = df_graos[df_graos[c_prod].str.strip().isin(PRODUTOS_GRAOS)].copy()

    df_graos["produto"]        = df_graos[c_prod].str.strip()
    df_graos["safra"]          = df_graos[c_safra].str.strip()
    df_graos["levantamento"]   = pd.to_numeric(df_graos[c_lev], errors="coerce").astype("Int64")
    df_graos["estado"]         = df_graos[c_estado].str.strip() if c_estado else "BRASIL"
    df_graos["area_mil_ha"]    = df_graos[c_area].apply(parse_float)
    df_graos["produtividade"]  = df_graos[c_produ].apply(parse_float)
    df_graos["producao_mil_t"] = df_graos[c_prodc].apply(parse_float)
    df_graos["updated_at"]     = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    df_final = df_graos[["produto","safra","levantamento","estado",
                          "area_mil_ha","produtividade","producao_mil_t","updated_at"]]

    upsert(conn, df_final)
    print(f"  OK {len(df_final)} registros de graos inseridos/atualizados.")

except Exception as e:
    print(f"  ERRO ao processar graos: {e}")

# ── Cana-de-Acucar ─────────────────────────────────────────────────────────────
try:
    df_cana = baixa_txt(URLS["cana"])

    c_safra  = col(df_cana, "SAFRA")
    c_lev    = col(df_cana, "LEVANTAMENTO")
    c_estado = col(df_cana, "ESTADO", "UF")
    c_area   = col(df_cana, "AREA")
    c_produ  = col(df_cana, "PRODUTIVIDADE")
    c_prodc  = col(df_cana, "PRODUCAO", "PRODUCAO")

    df_cana["produto"]        = "CANA-DE-ACUCAR"
    df_cana["safra"]          = df_cana[c_safra].str.strip()
    df_cana["levantamento"]   = pd.to_numeric(df_cana[c_lev], errors="coerce").astype("Int64")
    df_cana["estado"]         = df_cana[c_estado].str.strip() if c_estado else "BRASIL"
    df_cana["area_mil_ha"]    = df_cana[c_area].apply(parse_float)
    df_cana["produtividade"]  = df_cana[c_produ].apply(parse_float)
    df_cana["producao_mil_t"] = df_cana[c_prodc].apply(parse_float)
    df_cana["updated_at"]     = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    df_final_c = df_cana[["produto","safra","levantamento","estado",
                           "area_mil_ha","produtividade","producao_mil_t","updated_at"]]

    upsert(conn, df_final_c)
    print(f"  OK {len(df_final_c)} registros de cana inseridos/atualizados.")

except Exception as e:
    print(f"  ERRO ao processar cana: {e}")

conn.close()
print("\nConcluido.")
