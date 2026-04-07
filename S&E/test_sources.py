#!/usr/bin/env python3
"""
test_sources.py v3 — Descobre os parâmetros corretos da UNICAdata
para buscar histórico completo e dados recentes.
"""
import requests, logging, re
logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/123.0.0.0 Safari/537.36",
    "Accept-Language": "pt-BR,pt;q=0.9",
    "Referer": "https://unicadata.com.br/preco-ao-produtor.php?idMn=42",
}

def test(label, url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        ct = r.headers.get("Content-Type","")
        is_xls = r.content[:4] in (b"PK\x03\x04", b"\xd0\xcf\x11\xe0")
        log.info(f"[{r.status_code}] {label}")
        log.info(f"  {len(r.content):,} bytes | XLS={is_xls} | CT={ct[:60]}")
        if not is_xls:
            log.info(f"  Snippet: {r.text[:300]}")
        return r.content if is_xls else None
    except Exception as e:
        log.error(f"ERRO [{label}]: {e}")
        return None

log.info("="*70)
log.info("TESTE DE PARÂMETROS UNICADATA")
log.info("="*70)

# 1. Inspeciona o HTML da página para ver quais parâmetros o form usa
log.info("--- Inspecionando HTML da página diária ---")
try:
    r = requests.get(
        "https://unicadata.com.br/preco-ao-produtor.php"
        "?idMn=42&tipoHistorico=7&acao=visualizar&idTabela=2405"
        "&produto=Etanol+hidratado+combust%C3%ADvel&frequencia=Di%C3%A1rio&estado=Paulinia",
        headers=HEADERS, timeout=30)
    log.info(f"Página status: {r.status_code} | {len(r.content):,} bytes")
    # Extrai action do form e campos hidden
    forms = re.findall(r'<form[^>]*>(.*?)</form>', r.text, re.DOTALL|re.IGNORECASE)
    for i, f in enumerate(forms[:3]):
        log.info(f"Form {i}: {f[:500]}")
    # Procura inputs com data
    inputs = re.findall(r'<input[^>]+>', r.text, re.IGNORECASE)
    for inp in inputs:
        if any(k in inp.lower() for k in ['data', 'date', 'ini', 'fim', 'start', 'end', 'period']):
            log.info(f"Input relevante: {inp[:200]}")
    # Procura o link do Excel/XLS no HTML
    xls_links = re.findall(r'href=["\']([^"\']*xls[^"\']*)["\']', r.text, re.IGNORECASE)
    log.info(f"Links XLS no HTML: {xls_links[:5]}")
    # Procura parâmetros dataIni/dataFim
    params_found = re.findall(r'(data\w*|period\w*|ini\w*|fim\w*)["\s]*[:=]["\s]*([^"&\s]+)', r.text, re.IGNORECASE)
    log.info(f"Params de data encontrados: {params_found[:10]}")
except Exception as e:
    log.error(f"Erro: {e}")

log.info("")
log.info("--- Testando variações de parâmetros de data ---")

from datetime import date, timedelta
hoje      = date.today().strftime("%d/%m/%Y")
ha30dias  = (date.today() - timedelta(days=30)).strftime("%d/%m/%Y")
ha365dias = (date.today() - timedelta(days=365)).strftime("%d/%m/%Y")
inicio    = "01/01/2010"

base = "https://unicadata.com.br/xlsPrcProd.php?idioma=1&tipoHistorico=7&idTabela=2405&estado=Paulinia&produto=Etanol+hidratado+combust%C3%ADvel&frequencia=Di%C3%A1rio"

# Testa variações de parâmetros de período
import urllib.parse
tests = [
    ("sem datas (atual)",        base),
    ("dataIni+dataFim recentes", base + f"&dataIni={urllib.parse.quote(ha30dias)}&dataFim={urllib.parse.quote(hoje)}"),
    ("inicio+fim",               base + f"&inicio={urllib.parse.quote(ha30dias)}&fim={urllib.parse.quote(hoje)}"),
    ("de+ate",                   base + f"&de={urllib.parse.quote(ha30dias)}&ate={urllib.parse.quote(hoje)}"),
    ("startDate+endDate",        base + f"&startDate={urllib.parse.quote(ha30dias)}&endDate={urllib.parse.quote(hoje)}"),
    ("periodoIni+periodoFim",    base + f"&periodoIni={urllib.parse.quote(ha30dias)}&periodoFim={urllib.parse.quote(hoje)}"),
    ("dataInicial+dataFinal",    base + f"&dataInicial={urllib.parse.quote(ha30dias)}&dataFinal={urllib.parse.quote(hoje)}"),
    # Testa histórico completo
    ("dataIni desde 2010",       base + f"&dataIni={urllib.parse.quote(inicio)}&dataFim={urllib.parse.quote(hoje)}"),
]

for label, url in tests:
    content = test(label, url)
    if content:
        # Verifica se tem datas recentes no conteúdo
        import io, pandas as pd
        try:
            raw = pd.read_excel(io.BytesIO(content), header=None, dtype=str)
            # Procura pelo ano atual no conteúdo
            txt = raw.to_string()
            anos_encontrados = set(re.findall(r'20[12]\d', txt))
            log.info(f"  Anos no Excel: {sorted(anos_encontrados)}")
            log.info(f"  Shape: {raw.shape}")
        except: pass
    log.info("")

log.info("="*70)
log.info("FIM")
log.info("="*70)
