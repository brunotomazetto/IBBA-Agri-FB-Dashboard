#!/usr/bin/env python3
"""
test_sources.py — Testa B3 e Novacana para preço diário etanol hidratado
"""
import time, logging, re, io
from pathlib import Path

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)

DOWNLOAD_DIR = Path("/tmp/downloads")
DOWNLOAD_DIR.mkdir(exist_ok=True)

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By

def make_driver(download_dir=None):
    options = uc.ChromeOptions()
    if download_dir:
        options.add_experimental_option("prefs", {
            "download.default_directory": str(download_dir),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
        })
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1280,900")
    options.add_argument("--lang=pt-BR")
    return uc.Chrome(options=options, version_main=None)

# ════════════════════════════════════════════════════════════════════════════════
# TESTE 1 — B3
# Indicador CEPEA/B3 publicado em b3.com.br
# ════════════════════════════════════════════════════════════════════════════════
log.info("="*70)
log.info("TESTE 1 — B3 (Indicador Etanol Hidratado CEPEA/B3)")
log.info("="*70)

B3_URLS = [
    "https://www.b3.com.br/pt_br/market-data-e-indices/indices/indicadores-agropecuarios/indicador-etanol-hidratado-paulinia-sp-esalq-b3.htm",
    "https://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/market-data/consultas/mercado-de-derivativos/precos-referenciais/etanol/",
    "https://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/market-data/historico/derivativos/indicador-etanol-hidratado/",
]

driver = make_driver(DOWNLOAD_DIR)
try:
    for url in B3_URLS:
        log.info(f"\nTestando: {url[:90]}")
        driver.get(url)
        time.sleep(8)
        title = driver.title
        curr  = driver.current_url
        log.info(f"  Title: '{title}' | URL: {curr[:90]}")

        # Cloudflare?
        cf = "just a moment" in title.lower() or "verify" in driver.page_source.lower()[:500]
        log.info(f"  Cloudflare: {cf}")

        if not cf:
            # Procura dados/tabelas/links de download
            driver.save_screenshot(f"/tmp/b3_{B3_URLS.index(url)}.png")
            
            # Links de download (Excel/CSV/PDF)
            links = driver.find_elements(By.CSS_SELECTOR, 
                "a[href*='.xls'], a[href*='.xlsx'], a[href*='.csv'], a[href*='download'], a[href*='excel']")
            log.info(f"  Links download: {len(links)}")
            for l in links[:5]:
                log.info(f"    '{l.text.strip()[:40]}' → {l.get_attribute('href')[:100]}")
            
            # Tabelas com números
            tables = driver.find_elements(By.TAG_NAME, "table")
            log.info(f"  Tabelas: {len(tables)}")
            for t in tables[:2]:
                rows = t.find_elements(By.TAG_NAME, "tr")
                for r in rows[:3]:
                    log.info(f"    {r.text.strip()[:100]}")

            # Todos os links da página
            all_links = driver.find_elements(By.TAG_NAME, "a")
            log.info(f"  Total links: {len(all_links)}")
            for l in all_links[:15]:
                href = l.get_attribute("href") or ""
                txt  = l.text.strip()[:40]
                if href and "cloudflare" not in href and txt:
                    log.info(f"    '{txt}' → {href[:100]}")
        
        # Screenshot mesmo se CF
        driver.save_screenshot(f"/tmp/b3_{B3_URLS.index(url)}.png")
        time.sleep(2)

finally:
    driver.quit()
    log.info("[B3] Driver fechado.")

# ════════════════════════════════════════════════════════════════════════════════
# TESTE 2 — Novacana
# Agrega dados do CEPEA, pode ter endpoint mais acessível
# ════════════════════════════════════════════════════════════════════════════════
log.info("\n" + "="*70)
log.info("TESTE 2 — Novacana (dados etanol hidratado)")
log.info("="*70)

NOVACANA_URLS = [
    "https://www.novacana.com/data/dados/",
    "https://www.novacana.com/preco/etanol/hidratado/sp/",
    "https://www.novacana.com/preco/etanol/",
]

driver2 = make_driver(DOWNLOAD_DIR)
try:
    for url in NOVACANA_URLS:
        log.info(f"\nTestando: {url}")
        driver2.get(url)
        time.sleep(8)
        title = driver2.title
        log.info(f"  Title: '{title}'")
        cf = "just a moment" in title.lower()
        log.info(f"  Cloudflare: {cf}")
        driver2.save_screenshot(f"/tmp/novacana_{NOVACANA_URLS.index(url)}.png")

        if not cf:
            # Procura preços na página
            page = driver2.page_source
            # Extrai números que parecem preços de etanol (R$1-5/l)
            precos = re.findall(r'R\$\s*[\d,\.]+|[\d]+[,\.][\d]{2,4}\s*/\s*[lL]', page)
            log.info(f"  Preços encontrados: {precos[:10]}")

            # Links de API/dados
            links = driver2.find_elements(By.CSS_SELECTOR,
                "a[href*='api'], a[href*='dados'], a[href*='.xls'], a[href*='.csv'], a[href*='download']")
            log.info(f"  Links dados/API: {len(links)}")
            for l in links[:5]:
                log.info(f"    '{l.text.strip()[:40]}' → {l.get_attribute('href')[:100]}")

            # Requests XHR/fetch (via performance logs)
            all_links = driver2.find_elements(By.TAG_NAME, "a")
            for l in all_links[:20]:
                href = l.get_attribute("href") or ""
                txt  = l.text.strip()[:40]
                if href and txt and "cloudflare" not in href:
                    log.info(f"    '{txt}' → {href[:100]}")

finally:
    driver2.quit()
    log.info("[Novacana] Driver fechado.")

log.info("\n" + "="*70)
log.info("FIM")
log.info("="*70)
