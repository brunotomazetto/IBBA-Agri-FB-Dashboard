#!/usr/bin/env python3
"""
test_sources.py v4 — Varre idTabelas da UNICAdata e testa semanal/histórico completo
"""
import io, requests, logging, re
import pandas as pd

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/123.0.0.0 Safari/537.36",
    "Referer": "https://unicadata.com.br/preco-ao-produtor.php?idMn=42",
}

def get_xls(url):
    r = requests.get(url, headers=HEADERS, timeout=20)
    if r.status_code == 200 and r.content[:4] in (b"PK\x03\x04", b"\xd0\xcf\x11\xe0"):
        return r.content
    return None

def anos_no_excel(content):
    try:
        raw = pd.read_excel(io.BytesIO(content), header=None, dtype=str)
        txt = raw.to_string()
        return sorted(set(re.findall(r'20[12]\d', txt)))
    except:
        return []

def datas_no_excel(content):
    """Extrai datas no formato DD/MM/YYYY do Excel"""
    try:
        raw = pd.read_excel(io.BytesIO(content), header=None, dtype=str)
        datas = []
        for _, row in raw.iterrows():
            for v in row:
                v = str(v).strip()
                if re.match(r'\d{2}/\d{2}/20\d{2}', v):
                    datas.append(v)
        return sorted(set(datas))
    except:
        return []

log.info("="*70)

# ── 1. Testa tabelas próximas à 2405 (diário) para achar versão mais recente
log.info("--- Varrendo idTabelas ao redor de 2405 (diário) ---")
BASE_DIARIO = "https://unicadata.com.br/xlsPrcProd.php?idioma=1&tipoHistorico=7&estado=Paulinia&produto=Etanol+hidratado+combust%C3%ADvel&frequencia=Di%C3%A1rio"
for tid in [2400, 2401, 2402, 2403, 2404, 2405, 2406, 2407, 2408, 2409, 2410,
            2480, 2485, 2487, 2490, 2500, 2510, 2520, 3000, 3100, 3200]:
    url = f"{BASE_DIARIO}&idTabela={tid}"
    c = get_xls(url)
    if c:
        anos = anos_no_excel(c)
        datas = datas_no_excel(c)
        log.info(f"  idTabela={tid}: {len(c):,} bytes | anos={anos} | datas={datas[-3:] if datas else '—'}")
    else:
        pass  # silencioso para tabelas que não retornam Excel

log.info("")

# ── 2. Testa semanal (idTabela=2487) — talvez seja mais recente que o diário
log.info("--- Semanal idTabela=2487 ---")
url_sem = "https://unicadata.com.br/xlsPrcProd.php?idioma=1&tipoHistorico=7&idTabela=2487&estado=S%C3%A3o+Paulo&produto=Etanol+hidratado+combust%C3%ADvel&frequencia=Semanal"
c = get_xls(url_sem)
if c:
    anos = anos_no_excel(c)
    datas = datas_no_excel(c)
    log.info(f"  Semanal 2487: {len(c):,} bytes | anos={anos} | datas recentes={datas[-5:]}")
    raw = pd.read_excel(io.BytesIO(c), header=None, dtype=str)
    for i, row in raw.head(25).iterrows():
        vals = [str(v)[:30] for v in row.tolist() if str(v) != 'nan']
        if vals: log.info(f"  row {i:2d}: {vals}")

log.info("")

# ── 3. Testa mensal com diferentes idTabelas para achar histórico mais amplo
log.info("--- Mensais com diferentes idTabelas ---")
BASE_MENSAL = "https://unicadata.com.br/xlsPrcProd.php?idioma=1&tipoHistorico=7&estado=S%C3%A3o+Paulo&produto=Etanol+hidratado+combust%C3%ADvel&frequencia=Mensal"
for tid in [1433, 1434, 1435, 1440, 1450, 1500, 1600, 1700, 1800, 2000, 2100, 2200, 2300, 2400]:
    url = f"{BASE_MENSAL}&idTabela={tid}"
    c = get_xls(url)
    if c:
        anos = anos_no_excel(c)
        log.info(f"  idTabela={tid}: {len(c):,} bytes | anos={anos}")

log.info("")

# ── 4. Inspeciona o HTML buscando outros links de XLS na página
log.info("--- Links XLS em outras páginas da UNICAdata ---")
pages = [
    "https://unicadata.com.br/preco-ao-produtor.php?idMn=42&tipoHistorico=7&acao=visualizar&idTabela=2487&produto=Etanol+hidratado+combust%C3%ADvel&frequencia=Semanal&estado=S%C3%A3o+Paulo",
    "https://unicadata.com.br/listagem.php?IdMn=42",
]
for url in pages:
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        links = re.findall(r'href=["\']([^"\']*xlsPrcProd[^"\']*)["\']', r.text, re.IGNORECASE)
        log.info(f"  {url[-60:]}: {links[:3]}")
    except Exception as e:
        log.error(f"  Erro: {e}")

log.info("="*70)
