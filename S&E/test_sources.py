#!/usr/bin/env python3
"""test_sources.py v5 — Verifica preços reais nas tabelas 2026"""
import io, requests, logging
import pandas as pd

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/123.0.0.0 Safari/537.36",
    "Referer": "https://unicadata.com.br/preco-ao-produtor.php?idMn=42",
}

# Todos os estados possíveis para o etanol hidratado
ESTADOS = ["Paulinia", "S%C3%A3o+Paulo", "Goias", "Goi%C3%A1s", ""]

# idTabelas que mostraram 2026 no diagnóstico
IDS_2026 = [2400, 2401, 2402, 2403, 2404, 2406, 2407, 2408, 2409, 2410,
            2480, 2485, 2490, 2500, 2510, 2520, 3000, 3100, 3200]

def get_precos(content):
    """Retorna lista de (data, preco) onde preco > 0"""
    try:
        engine = "openpyxl" if content[:4] == b"PK\x03\x04" else "xlrd"
        raw = pd.read_excel(io.BytesIO(content), engine=engine, header=None, dtype=str)
        results = []
        for _, row in raw.iterrows():
            for ci in range(len(row)):
                v = str(row.iloc[ci]).strip()
                # Procura data DD/MM/YYYY
                import re
                if re.match(r'\d{2}/\d{2}/20\d{2}', v):
                    # Preço na próxima coluna
                    for pi in range(ci+1, min(ci+4, len(row))):
                        pv = str(row.iloc[pi]).strip().replace(",",".")
                        try:
                            p = float(pv)
                            if p > 0:
                                results.append((v, p))
                                break
                        except: continue
                    break
        return results
    except:
        return []

log.info("Testando idTabelas com dados de 2026 — buscando preço > 0")
log.info("="*70)

# Testa cada idTabela com Paulinia (estado padrão do diário de etanol)
for tid in IDS_2026:
    url = (f"https://unicadata.com.br/xlsPrcProd.php"
           f"?idioma=1&tipoHistorico=7&idTabela={tid}"
           f"&estado=Paulinia&produto=Etanol+hidratado+combust%C3%ADvel&frequencia=Di%C3%A1rio")
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code == 200 and r.content[:4] in (b"PK\x03\x04", b"\xd0\xcf\x11\xe0"):
            precos = get_precos(r.content)
            precos_nz = [(d,p) for d,p in precos if p > 0]
            if precos_nz:
                log.info(f"✅ idTabela={tid}: PREÇO REAL! {precos_nz[:3]}")
            else:
                log.info(f"❌ idTabela={tid}: sem preço ({len(precos)} datas, tudo zero)")
        else:
            log.info(f"❌ idTabela={tid}: status={r.status_code}")
    except Exception as e:
        log.warning(f"⚠️  idTabela={tid}: {e}")

log.info("")
log.info("Testando 2406 com diferentes estados (tem tamanho diferente=48,339)")
for estado in ESTADOS:
    url = (f"https://unicadata.com.br/xlsPrcProd.php"
           f"?idioma=1&tipoHistorico=7&idTabela=2406"
           f"&estado={estado}&produto=Etanol+hidratado+combust%C3%ADvel&frequencia=Di%C3%A1rio")
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code == 200 and r.content[:4] in (b"PK\x03\x04", b"\xd0\xcf\x11\xe0"):
            precos = get_precos(r.content)
            precos_nz = [(d,p) for d,p in precos if p > 0]
            log.info(f"  estado='{estado}': {len(r.content):,}b | preços={precos_nz[:3]}")
    except Exception as e:
        log.warning(f"  estado='{estado}': {e}")

log.info("")
log.info("Testando produto=Etanol+anidro e outros produtos no idTabela=2400")
produtos = [
    "Etanol+hidratado+combust%C3%ADvel",
    "Etanol+anidro+combust%C3%ADvel",
    "Etanol+hidratado+outros+fins",
]
for prod in produtos:
    url = (f"https://unicadata.com.br/xlsPrcProd.php"
           f"?idioma=1&tipoHistorico=7&idTabela=2400"
           f"&estado=Paulinia&produto={prod}&frequencia=Di%C3%A1rio")
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code == 200 and r.content[:4] in (b"PK\x03\x04", b"\xd0\xcf\x11\xe0"):
            precos = get_precos(r.content)
            precos_nz = [(d,p) for d,p in precos if p > 0]
            log.info(f"  produto={prod[:30]}: preços={precos_nz[:3]}")
    except Exception as e:
        log.warning(f"  produto={prod[:30]}: {e}")

log.info("="*70)
