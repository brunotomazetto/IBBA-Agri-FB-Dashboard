#!/usr/bin/env python3
"""
test_sources.py v2 — Diagnóstico focado:
  1. UNICAdata xlsPrcProd.php (equivalente Excel do PDF que apareceu)
  2. ANP — parseia página de dados estatísticos e testa todos os links de Excel
             relacionados a etanol/produtor/preço
"""

import io, re, logging, requests, sys
try:
    import pandas as pd
except ImportError:
    sys.exit("pip install pandas openpyxl xlrd")

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)

HDR = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/123.0.0.0 Safari/537.36",
    "Accept-Language": "pt-BR,pt;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
}

def test_url(label, url, ref=None, session=None):
    h = {**HDR}
    if ref: h["Referer"] = ref
    req = session or requests
    try:
        r = req.get(url, headers=h, timeout=25, allow_redirects=True)
        ct   = r.headers.get("Content-Type","")
        size = len(r.content)
        is_xlsx = r.content[:4] == b"PK\x03\x04"
        is_xls  = r.content[:4] == b"\xd0\xcf\x11\xe0"
        is_xls_any = is_xlsx or is_xls

        status_ok = "✅" if r.status_code == 200 else "❌"
        log.info(f"{status_ok} [{r.status_code}] {label}")
        log.info(f"   URL final : {r.url[:110]}")
        log.info(f"   CT        : {ct[:70]} | {size:,} bytes")

        if is_xls_any:
            engine = "openpyxl" if is_xlsx else "xlrd"
            try:
                df = pd.read_excel(io.BytesIO(r.content), engine=engine, header=None, nrows=8)
                log.info(f"   📊 EXCEL OK | shape amostra: {df.shape}")
                for i, row in df.iterrows():
                    vals = [str(v)[:30] for v in row.tolist() if str(v) not in ("nan","None","")]
                    if vals:
                        log.info(f"   row {i}: {vals}")
            except Exception as e:
                log.warning(f"   Excel parse falhou: {e}")
            return True, r.content
        else:
            snippet = r.text[:300].replace("\n"," ")
            log.info(f"   Snippet   : {snippet[:200]}")
            return False, r.content
    except Exception as e:
        log.error(f"❌ ERRO [{label}]: {e}")
        return False, b""
    finally:
        log.info("")


log.info("="*70)
log.info("DIAGNÓSTICO v2")
log.info("="*70)

# ══════════════════════════════════════════════════════════════════════
# 1. UNICAdata — testa xlsPrcProd.php (equivalente XLS do PDF que apareceu)
# ══════════════════════════════════════════════════════════════════════
log.info("─── UNICAdata XLS endpoints ────────────────────────────────────────")

UNICA_REF = "https://unicadata.com.br/preco-ao-produtor.php?idMn=42&tipoHistorico=7&acao=visualizar&idTabela=2405&produto=Etanol+hidratado+combust%C3%ADvel&frequencia=Di%C3%A1rio&estado=Paulinia"

# O PDF usa: pdfPrcProd.php?idioma=1&tipoHistorico=7&idTabela=2405&estado=Paulinia&...
# O XLS provavelmente usa: xlsPrcProd.php com os mesmos parâmetros
params = "idioma=1&tipoHistorico=7&idTabela=2405&estado=Paulinia&produto=Etanol+hidratado+combust%C3%ADvel&frequencia=Di%C3%A1rio"

unica_candidates = [
    ("UNICAdata xlsPrcProd.php",        f"https://unicadata.com.br/xlsPrcProd.php?{params}"),
    ("UNICAdata excelPrcProd.php",      f"https://unicadata.com.br/excelPrcProd.php?{params}"),
    ("UNICAdata downloadExcel.php",     f"https://unicadata.com.br/downloadExcel.php?{params}"),
    ("UNICAdata download.php tab 2405", f"https://unicadata.com.br/download.php?idTabela=2405&tipoHistorico=7&estado=Paulinia"),
    # Tenta também com a tabela mensal (idTabela=1433) para confirmar padrão
    ("UNICAdata xlsPrcProd mensal 1433",f"https://unicadata.com.br/xlsPrcProd.php?idioma=1&tipoHistorico=7&idTabela=1433&estado=S%C3%A3o+Paulo&produto=Etanol+hidratado+combust%C3%ADvel&frequencia=Mensal"),
]

for label, url in unica_candidates:
    ok, content = test_url(label, url, ref=UNICA_REF)
    if ok:
        log.info(f"   🎯 FUNCIONOU: {label}")
        break

# ══════════════════════════════════════════════════════════════════════
# 2. ANP — parseia a página e testa TODOS os links Excel de etanol/preço
# ══════════════════════════════════════════════════════════════════════
log.info("─── ANP — buscando links Excel na página de dados estatísticos ─────")

ANP_BASE = "https://www.gov.br"
try:
    r_anp = requests.get(
        "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-estatisticos",
        headers=HDR, timeout=25
    )
    log.info(f"ANP página: {r_anp.status_code} | {len(r_anp.content):,} bytes")

    # Extrai todos os links .xlsx e .xls da página
    all_xls = re.findall(r'https?://[^\s"\'<>]+\.xlsx?', r_anp.text, re.IGNORECASE)
    all_xls = list(dict.fromkeys(all_xls))  # dedup mantendo ordem
    log.info(f"Total links Excel na página ANP: {len(all_xls)}")

    # Filtra os relevantes: etanol + (produtor ou preço ou hidratado)
    keywords = ["etanol", "hidratado", "produtor", "preco", "preços", "biocombustivel"]
    relevant = [u for u in all_xls if any(k in u.lower() for k in keywords)]
    log.info(f"Links relevantes (etanol/produtor): {len(relevant)}")
    for u in relevant:
        log.info(f"  → {u}")

    log.info("")
    # Testa cada um
    for url in relevant:
        test_url(f"ANP {url.split('/')[-1]}", url)

    # Se não achou nada específico, lista os 10 primeiros para inspeção
    if not relevant:
        log.info("Nenhum link específico — primeiros 10 xlsx da página:")
        for u in all_xls[:10]:
            log.info(f"  {u}")

except Exception as e:
    log.error(f"ANP página ERRO: {e}")

log.info("="*70)
log.info("FIM DO DIAGNÓSTICO")
log.info("="*70)
