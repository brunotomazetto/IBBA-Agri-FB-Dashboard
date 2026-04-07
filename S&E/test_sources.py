#!/usr/bin/env python3
"""
test_sources.py — Testa a URL /series/etanol.aspx?id=103 com Playwright
Captura todos os requests de rede para achar a API de dados por baixo
"""
import time, logging
from playwright.sync_api import sync_playwright

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)

TARGET = "https://cepea.esalq.usp.br/br/indicador/series/etanol.aspx?id=103"

with sync_playwright() as pw:
    browser = pw.chromium.launch(
        headless=True,
        args=["--no-sandbox","--disable-setuid-sandbox","--disable-dev-shm-usage","--disable-gpu"],
    )
    context = browser.new_context(
        locale="pt-BR",
        viewport={"width": 1280, "height": 800},
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    )
    page = context.new_page()

    # Loga TODOS os requests e responses
    all_requests  = []
    all_responses = []

    def on_request(req):
        all_requests.append({"url": req.url, "method": req.method})

    def on_response(resp):
        ct  = resp.headers.get("content-type","")
        all_responses.append({"url": resp.url, "status": resp.status, "ct": ct})
        # Captura responses JSON ou Excel
        if any(k in ct for k in ["json","excel","spreadsheet","octet"]):
            try:
                body = resp.body()
                log.info(f"🎯 DADOS [{resp.status}] {resp.url[:100]}")
                log.info(f"   CT={ct} | {len(body):,} bytes")
                if "json" in ct:
                    log.info(f"   JSON snippet: {resp.text()[:300]}")
            except: pass

    page.on("request",  on_request)
    page.on("response", on_response)

    log.info(f"Navegando para {TARGET}")
    try:
        page.goto(TARGET, wait_until="domcontentloaded", timeout=60_000)
        log.info("DOM carregado.")
    except Exception as e:
        log.warning(f"goto timeout: {e}")

    log.info("Aguardando 30s (Turnstile + carregamento de dados)...")
    time.sleep(30)

    # Screenshot
    page.screenshot(path="/tmp/series_screenshot.png", full_page=True)
    log.info("Screenshot salvo.")

    # HTML da página
    html = page.content()
    with open("/tmp/series_page.html","w") as f:
        f.write(html)
    log.info(f"HTML: {len(html):,} chars")

    # Links na página
    links = page.eval_on_selector_all("a[href]", "els => els.map(e => ({text: e.innerText.trim().slice(0,50), href: e.href}))")
    non_cf = [l for l in links if "cloudflare" not in l["href"]]
    log.info(f"Links não-Cloudflare: {len(non_cf)}")
    for l in non_cf[:10]:
        log.info(f"  {l['text']!r} → {l['href'][:100]}")

    # Requests feitos
    log.info(f"\nTotal requests: {len(all_requests)}")
    non_cf_req = [r for r in all_requests if "cloudflare" not in r["url"] and "cdn-cgi" not in r["url"]]
    log.info(f"Requests não-Cloudflare: {len(non_cf_req)}")
    for r in non_cf_req:
        log.info(f"  [{r['method']}] {r['url'][:120]}")

    # Responses com dados
    log.info(f"\nResponses com dados (JSON/Excel):")
    data_resp = [r for r in all_responses if any(k in r["ct"] for k in ["json","excel","spreadsheet","octet"])]
    for r in data_resp:
        log.info(f"  [{r['status']}] {r['url'][:120]} | {r['ct'][:60]}")

    context.close()
    browser.close()

log.info("FIM")
