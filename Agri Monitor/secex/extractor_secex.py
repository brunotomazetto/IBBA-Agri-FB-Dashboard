import requests
import pandas as pd
import sqlite3
import json
import io
import os
from datetime import datetime

# ── Configurações ──────────────────────────────────────────────────────────────
DB_PATH  = "secex/secex.db"
BASE_URL = "https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/EXP_{ano}.csv"

NCM_CONFIG = [
    {"ncm": 12011000, "produto": "Soja para semeadura",        "categoria": "Soja"},
    {"ncm": 12019000, "produto": "Soja em grão (outras)",      "categoria": "Soja"},
    {"ncm": 10051000, "produto": "Milho para semeadura",       "categoria": "Milho"},
    {"ncm": 10059010, "produto": "Milho em grão p/ moagem",    "categoria": "Milho"},
    {"ncm": 10059090, "produto": "Milho em grão (outros)",     "categoria": "Milho"},
    {"ncm": 52010010, "produto": "Algodão não cardado cru",    "categoria": "Algodão"},
    {"ncm": 52010020, "produto": "Algodão não cardado branq.", "categoria": "Algodão"},
    {"ncm": 52010090, "produto": "Algodão não cardado outros", "categoria": "Algodão"},
    {"ncm": 52030000, "produto": "Algodão cardado ou penteado","categoria": "Algodão"},
    {"ncm": 17011300, "produto": "Açúcar de cana industrial",  "categoria": "Açúcar"},
    {"ncm": 17011400, "produto": "Açúcar de cana outros",      "categoria": "Açúcar"},
    {"ncm": 17019900, "produto": "Outros açúcares",            "categoria": "Açúcar"},
]

ncm_list      = [item["ncm"]      for item in NCM_CONFIG]
ncm_produto   = {item["ncm"]: item["produto"]   for item in NCM_CONFIG}
ncm_categoria = {item["ncm"]: item["categoria"] for item in NCM_CONFIG}

# ── Inicializa banco ───────────────────────────────────────────────────────────
os.makedirs("secex", exist_ok=True)

conn = sqlite3.connect(DB_PATH)
conn.execute("""
    CREATE TABLE IF NOT EXISTS exportacoes (
        co_ano     INTEGER,
        co_mes     INTEGER,
        co_ncm     INTEGER,
        categoria  TEXT,
        produto    TEXT,
        vl_fob     REAL,
        kg_liquido REAL,
        updated_at TEXT,
        PRIMARY KEY (co_ano, co_mes, co_ncm)
    )
""")
conn.commit()

# ── Descobre a partir de qual ano buscar ───────────────────────────────────────
cursor = conn.execute("""
    SELECT co_ano, co_mes FROM exportacoes
    ORDER BY co_ano DESC, co_mes DESC
    LIMIT 1
""")
row = cursor.fetchone()

if row:
    ultimo_ano, ultimo_mes = row
    print(f"Banco existente — último dado: {ultimo_mes}/{ultimo_ano}")
    anos = list(range(ultimo_ano, datetime.now().year + 1))
else:
    print("Banco vazio — carga histórica completa desde 1997.")
    anos = list(range(1997, datetime.now().year + 1))
row = cursor.fetchone()

if row:
    ultimo_ano = row[0]
    print(f"Banco existente — buscando a partir de {ultimo_ano} (reprocessa ano corrente).")
    anos = list(range(ultimo_ano, datetime.now().year + 1))
else:
    print("Banco vazio — carga histórica completa desde 1997.")
    anos = list(range(1997, datetime.now().year + 1))

# ── Extração e carga ───────────────────────────────────────────────────────────
novos_registros = 0

for ano in anos:
    url = BASE_URL.format(ano=ano)
    print(f"Baixando {ano}...")

    try:
        r = requests.get(url, stream=True, verify=False, timeout=120)

        if r.status_code != 200:
            print(f"  ⚠️  {ano} não disponível (status {r.status_code}), pulando.")
            continue

        df = pd.read_csv(
            io.StringIO(r.content.decode("latin1")),
            sep=";",
            dtype={"CO_NCM": int}
        )

        df = df[df["CO_NCM"].isin(ncm_list)].copy()

        if df.empty:
            print(f"  Nenhum NCM encontrado em {ano}.")
            continue

        # Consolida por ano/mês/NCM (soma todas as UFs)
        df_agg = (
            df.groupby(["CO_ANO", "CO_MES", "CO_NCM"])[["VL_FOB", "KG_LIQUIDO"]]
            .sum()
            .reset_index()
        )

        df_agg["categoria"]  = df_agg["CO_NCM"].map(ncm_categoria)
        df_agg["produto"]    = df_agg["CO_NCM"].map(ncm_produto)
        df_agg["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df_agg.columns = [c.lower() for c in df_agg.columns]
        df_agg = df_agg[["co_ano", "co_mes", "co_ncm", "categoria", "produto", "vl_fob", "kg_liquido", "updated_at"]]

        # INSERT OR REPLACE respeita a PRIMARY KEY — não duplica
        df_agg.to_sql("exportacoes_temp", conn, if_exists="replace", index=False)
        conn.execute("""
            INSERT OR REPLACE INTO exportacoes
            SELECT * FROM exportacoes_temp
        """)
        conn.execute("DROP TABLE exportacoes_temp")
        conn.commit()

        novos_registros += len(df_agg)
        print(f"  ✅ {len(df_agg)} registros inseridos/atualizados.")

    except Exception as e:
        print(f"  ❌ Erro ao processar {ano}: {e}")

conn.close()
print(f"\nConcluído — {novos_registros} registros processados.")
