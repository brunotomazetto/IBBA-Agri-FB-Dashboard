import requests
import pandas as pd
import sqlite3
import io
import os
from datetime import datetime

# ── Configurações ──────────────────────────────────────────────────────────────
DB_PATH = "conab/conab.db"

URLS = {
    "graos": "https://portaldeinformacoes.conab.gov.br/downloads/arquivos/LevantamentoGraos.txt",
    "cana":  "https://portaldeinformacoes.conab.gov.br/downloads/arquivos/LevantamentoCana.txt",
}

# Nomes exatos da coluna 'produto' no arquivo da CONAB (sem acentos)
PRODUTOS_GRAOS = [
    "SOJA",
    "MILHO 1ª SAFRA",   "MILHO 1A SAFRA",
    "MILHO 2ª SAFRA",   "MILHO 2A SAFRA",
    "MILHO 3ª SAFRA",   "MILHO 3A SAFRA",
    "MILHO TOTAL",
    "MILHO TOTAL (1ª+2ª SAFRAS)",    "MILHO TOTAL (1A+2A SAFRAS)",
    "MILHO TOTAL (1ª+2ª+3ª SAFRAS)", "MILHO TOTAL (1A+2A+3A SAFRAS)",
    "ALGODAO EM PLUMA",
    "ALGODAO TOTAL (PLUMA)",
]

# ── Inicializa banco ───────────────────────────────────────────────────────────
os.makedirs("conab", exist_ok=True)

conn = sqlite3.connect(DB_PATH)
conn.execute("""
    CREATE TABLE IF NOT EXISTS safra (
        ano_agricola          TEXT,
        safra                 TEXT,
        uf                    TEXT,
        produto               TEXT,
        id_produto            TEXT,
        id_levantamento       TEXT,
        dsc_levantamento      TEXT,
        area_plantada_mil_ha  REAL,
        producao_mil_t        REAL,
        produtividade_t_ha    REAL,
        updated_at            TEXT,
        PRIMARY KEY (ano_agricola, safra, uf, produto, id_levantamento)
    )
""")
conn.commit()

# ── Utilitários ────────────────────────────────────────────────────────────────
def baixa_txt(url):
    print(f"Baixando {url}...")
    r = requests.get(url, timeout=180, verify=False)
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.content.decode("latin1")), sep=";", dtype=str)
    df.columns = [c.strip() for c in df.columns]
    for c in df.columns:
        df[c] = df[c].str.strip()
    print(f"  -> {len(df)} linhas, colunas: {list(df.columns)}")
    return df

def parse_float(val):
    try:
        s = str(val).strip()
        # Suporta tanto ponto decimal ('263.7') quanto separador BR ('1.868,7')
        if "," in s and "." in s:
            s = s.replace(".", "").replace(",", ".")   # '1.868,7' -> '1868.7'
        elif "," in s:
            s = s.replace(",", ".")                    # '1,7' -> '1.7'
        return float(s)
    except (ValueError, AttributeError):
        return None

def upsert(conn, df):
    df.to_sql("safra", conn, if_exists="append", index=False, method="multi")
    conn.execute("""
        DELETE FROM safra WHERE rowid NOT IN (
            SELECT MAX(rowid) FROM safra
            GROUP BY ano_agricola, safra, uf, produto, id_levantamento
        )
    """)
    conn.commit()

# ── Grãos (Soja, Milho, Algodão) ──────────────────────────────────────────────
try:
    df_graos = baixa_txt(URLS["graos"])

    df_graos = df_graos[df_graos["produto"].isin(PRODUTOS_GRAOS)].copy()

    df_graos["area_plantada_mil_ha"] = df_graos["area_plantada_mil_ha"].apply(parse_float)
    df_graos["producao_mil_t"]       = df_graos["producao_mil_t"].apply(parse_float)
    df_graos["produtividade_t_ha"]   = df_graos["produtividade_mil_ha_mil_t"].apply(parse_float)
    df_graos["updated_at"]           = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    df_final = df_graos[[
        "ano_agricola", "safra", "uf", "produto", "id_produto",
        "id_levantamento", "dsc_levantamento",
        "area_plantada_mil_ha", "producao_mil_t", "produtividade_t_ha", "updated_at"
    ]]

    upsert(conn, df_final)
    print(f"  OK {len(df_final)} registros de grãos inseridos/atualizados.")

except Exception as e:
    print(f"  ERRO ao processar grãos: {e}")

# ── Cana-de-Acucar ─────────────────────────────────────────────────────────────
# Estrutura real do arquivo:
# ano_agricola | dsc_safra_previsao | uf | produto | id_produto |
# dsc_levantamento | id_levantamento | area_plantada_mil_ha | producao_mil_t |
# producao_acucar_mil_t | producao_etanol_* | produtcao_atr_kg_t
# Nao tem produtividade — salva ATR (kg acucar/t cana) como proxy
try:
    df_cana = baixa_txt(URLS["cana"])

    col_area  = next((c for c in df_cana.columns if "AREA"         in c.upper()), None)
    col_prod  = next((c for c in df_cana.columns if c == "producao_mil_t"), None)
    col_atr   = next((c for c in df_cana.columns if "ATR"          in c.upper()), None)
    col_uf    = next((c for c in df_cana.columns if c.upper() == "UF"), None)
    col_ano   = next((c for c in df_cana.columns if "ANO"          in c.upper()), None)
    col_safra = next((c for c in df_cana.columns if "SAFRA"        in c.upper()), None)
    col_lev   = next((c for c in df_cana.columns if c == "id_levantamento"), None)
    col_dlev  = next((c for c in df_cana.columns if c == "dsc_levantamento"), None)
    col_idp   = next((c for c in df_cana.columns if c == "id_produto"), None)

    faltando = [nome for nome, c in [("area", col_area), ("producao", col_prod),
                                     ("uf", col_uf), ("ano", col_ano),
                                     ("safra", col_safra), ("levantamento", col_lev)]
                if c is None]
    if faltando:
        raise ValueError(f"Colunas nao encontradas no arquivo de cana: {faltando}. "
                         f"Colunas disponiveis: {list(df_cana.columns)}")

    df_cana["ano_agricola"]         = df_cana[col_ano]
    df_cana["safra"]                = df_cana[col_safra]
    df_cana["uf"]                   = df_cana[col_uf]
    df_cana["produto"]              = "CANA-DE-ACUCAR"
    df_cana["id_produto"]           = df_cana[col_idp] if col_idp else ""
    df_cana["id_levantamento"]      = df_cana[col_lev]
    df_cana["dsc_levantamento"]     = df_cana[col_dlev] if col_dlev else ""
    df_cana["area_plantada_mil_ha"] = df_cana[col_area].apply(parse_float)
    df_cana["producao_mil_t"]       = df_cana[col_prod].apply(parse_float)
    df_cana["produtividade_t_ha"]   = df_cana[col_atr].apply(parse_float) if col_atr else None
    df_cana["updated_at"]           = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    df_final_c = df_cana[[
        "ano_agricola", "safra", "uf", "produto", "id_produto",
        "id_levantamento", "dsc_levantamento",
        "area_plantada_mil_ha", "producao_mil_t", "produtividade_t_ha", "updated_at"
    ]]

    upsert(conn, df_final_c)
    print(f"  OK {len(df_final_c)} registros de cana inseridos/atualizados.")

except Exception as e:
    print(f"  ERRO ao processar cana: {e}")

conn.close()
print("\nConcluído.")
