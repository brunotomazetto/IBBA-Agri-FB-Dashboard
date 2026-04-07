#!/usr/bin/env python3
"""
test_sources.py — Diagnóstico de fontes de Etanol Hidratado
============================================================
Roda no GitHub Actions e loga qual endpoint responde corretamente.
NÃO escreve no banco. Só testa conectividade e valida o conteúdo.
"""

import io
import logging
import requests
import sys

try:
    import pandas as pd
except ImportError:
    sys.exit("pip install pandas openpyxl xlrd")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

HEADERS_BROWSER = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "pt-BR,pt;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
}

results = []

def test(label, url, ref=None, method="GET", extra_headers=None):
    hdrs = {**HEADERS_BROWSER}
    if ref:
        hdrs["Referer"] = ref
    if extra_headers:
        hdrs.update(extra_headers)
    try:
        r = requests.request(method, url, headers=hdrs, timeout=20, allow_redirects=True)
        ct    = r.headers.get("Content-Type", "")
        size  = len(r.content)
        magic = r.content[:8].hex()
        is_xls  = r.content[:4] in (b"PK\x03\x04", b"\xd0\xcf\x11\xe0")
        is_html = b"<html" in r.content[:500].lower() or b"<!doc" in r.content[:500].lower()
        is_json = ct.startswith("application/json") or (r.content[:1] in (b"[", b"{"))

        status_emoji = "✅" if r.status_code == 200 else "❌"
        content_emoji = "📊 EXCEL" if is_xls else ("📄 HTML" if is_html else ("🔢 JSON" if is_json else "❓"))

        log.info(f"{status_emoji} [{r.status_code}] {label}")
        log.info(f"   URL final : {r.url[:100]}")
        log.info(f"   Conteúdo  : {content_emoji} | {size:,} bytes | CT: {ct[:60]}")
        log.info(f"   Magic hex : {magic}")

        if is_xls:
            try:
                engine = "openpyxl" if r.content[:4] == b"PK\x03\x04" else "xlrd"
                df = pd.read_excel(io.BytesIO(r.content), engine=engine, header=None, nrows=5)
                log.info(f"   Excel OK  : {df.shape[0]} linhas amostra | cols: {list(df.columns)}")
                log.info(f"   Amostra   : {df.iloc[0].tolist()}")
            except Exception as e:
                log.warning(f"   Excel parse FALHOU: {e}")
        elif is_html and r.status_code == 200:
            # Tenta extrair links de download do HTML
            import re
            links = re.findall(r'href=["\']([^"\']*(?:download|excel|xls|widgetpastas)[^"\']*)["\']',
                               r.text, re.IGNORECASE)
            if links:
                log.info(f"   Links XLS : {links[:3]}")
            # Verifica se tem Cloudflare Turnstile
            if "turnstile" in r.text.lower() or "challenge" in r.url:
                log.warning(f"   ⚠️  CLOUDFLARE TURNSTILE detectado!")
            # Verifica se tem dados de preço
            if "etanol" in r.text.lower() and ("r$/l" in r.text.lower() or "preço" in r.text.lower()):
                log.info(f"   ✅ Página contém dados de etanol/preço")
        elif is_json and r.status_code == 200:
            try:
                j = r.json()
                log.info(f"   JSON tipo : {type(j).__name__} | len={len(j) if hasattr(j,'__len__') else 'n/a'}")
                sample = j[:2] if isinstance(j, list) else list(j.items())[:3]
                log.info(f"   Amostra   : {sample}")
            except Exception as e:
                log.warning(f"   JSON parse FALHOU: {e}")

        results.append((label, r.status_code, content_emoji, size))

    except Exception as e:
        log.error(f"❌ ERRO [{label}]: {e}")
        results.append((label, "ERR", "❌", 0))

    log.info("")


log.info("=" * 70)
log.info("DIAGNÓSTICO DE FONTES — Etanol Hidratado ao Produtor")
log.info("=" * 70)
log.info("")

# ── 1. CEPEA /indicador/series/ (URL nova fornecida pelo usuário) ─────────────
log.info("─── CEPEA ─────────────────────────────────────────────────────────")
test(
    "CEPEA /series/etanol.aspx?id=103",
    "https://cepea.esalq.usp.br/br/indicador/series/etanol.aspx?id=103",
    ref="https://cepea.esalq.usp.br/br/indicador/etanol.aspx",
)
test(
    "CEPEA www /series/etanol.aspx?id=103",
    "https://www.cepea.esalq.usp.br/br/indicador/series/etanol.aspx?id=103",
    ref="https://www.cepea.esalq.usp.br/br/indicador/etanol.aspx",
)
# Tenta o download Excel direto pelo endpoint series
test(
    "CEPEA widgetpastas Excel direto",
    "https://www.cepea.esalq.usp.br/br/widgetpastas/17/indicador/338.aspx",
    ref="https://www.cepea.esalq.usp.br/br/indicador/series/etanol.aspx?id=103",
)

# ── 2. UNICAdata ──────────────────────────────────────────────────────────────
log.info("─── UNICAdata ──────────────────────────────────────────────────────")
# Tenta a página principal para ver se tem link de download
sess = requests.Session()
sess.headers.update(HEADERS_BROWSER)
try:
    r0 = sess.get(
        "https://unicadata.com.br/preco-ao-produtor.php"
        "?idMn=42&tipoHistorico=7&acao=visualizar"
        "&idTabela=2405&produto=Etanol+hidratado+combust%C3%ADvel"
        "&frequencia=Di%C3%A1rio&estado=Paulinia",
        timeout=20
    )
    log.info(f"UNICAdata página status: {r0.status_code} | {len(r0.content):,} bytes")
    if r0.status_code == 200:
        import re
        # Procura links de download (download_media.php?idM=XXXXX)
        idms = re.findall(r'download_media\.php\?idM=(\d+)', r0.text)
        log.info(f"UNICAdata idM encontrados na página: {idms}")
        if idms:
            for idm in idms[:3]:
                test(
                    f"UNICAdata download_media idM={idm}",
                    f"https://unicadata.com.br/download_media.php?idM={idm}",
                    ref="https://unicadata.com.br/preco-ao-produtor.php",
                )
        else:
            log.warning("UNICAdata: nenhum idM encontrado na página")
            # Mostra trecho do HTML para debug
            snippet = r0.text[r0.text.lower().find("download"):r0.text.lower().find("download")+200] if "download" in r0.text.lower() else r0.text[:500]
            log.info(f"Trecho HTML: {snippet[:300]}")
    if "turnstile" in r0.text.lower() or "cloudflare" in r0.text.lower():
        log.warning("UNICAdata: Cloudflare/Turnstile detectado!")
except Exception as e:
    log.error(f"UNICAdata página ERRO: {e}")

# ── 3. ANP — planilha semanal ao produtor ────────────────────────────────────
log.info("")
log.info("─── ANP (fallback semanal) ─────────────────────────────────────────")
# Testa a página de dados estatísticos da ANP para encontrar o Excel certo
test(
    "ANP precos-etanol-produtor.xlsx",
    "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-estatisticos/de/precos-de-etanol-hidratado-carburante/precos-etanol-produtor.xlsx",
)
test(
    "ANP precos-etanol-carburante-produtor.xlsx (alt)",
    "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-estatisticos/de/precos-ao-produtor/etanol-hidratado-carburante.xlsx",
)
# URL real que aparece nos relatórios ANP
test(
    "ANP SIMBIOSE preços produtor",
    "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-estatisticos",
    extra_headers={"Accept": "text/html"},
)

# ── Resumo ────────────────────────────────────────────────────────────────────
log.info("=" * 70)
log.info("RESUMO")
log.info("=" * 70)
for label, status, ctype, size in results:
    ok = "✅" if status == 200 else "❌"
    log.info(f"  {ok} [{status}] {label[:55]:55} {ctype} {size:>10,} bytes")
