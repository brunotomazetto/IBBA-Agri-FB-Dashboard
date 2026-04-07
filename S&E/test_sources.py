#!/usr/bin/env python3
"""
test_sources.py — Testa MAPA/SIMBIOSE e UDOP para preço semanal etanol
"""
import time, logging, re
from pathlib import Path

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By

def make_driver():
    options = uc.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1280,900")
    options.add_argument("--lang=pt-BR")
    return uc.Chrome(options=options, version_main=None)

def check_page(driver, url, label, wait=8):
    log.info(f"\n[{label}] {url}")
    driver.get(url)
    time.sleep(wait)
    title = driver.title
    cf    = "just a moment" in title.lower()
    log.info(f"  Title: '{title}' | CF: {cf}")
    driver.save_screenshot(f"/tmp/{label}.png")
    if not cf:
        page = driver.page_source
        # Procura padrões de preço R$/l ou R$/m³
        precos = re.findall(r'\d+[.,]\d{2,4}', page)
        # Procura datas recentes
        datas  = re.findall(r'\d{2}/\d{2}/202[456]', page)
        log.info(f"  Datas recentes: {list(set(datas))[:8]}")
        log.info(f"  Valores numéricos amostra: {precos[:10]}")
        # Links de download
        links = driver.find_elements(By.CSS_SELECTOR,
            "a[href*='.xls'], a[href*='.xlsx'], a[href*='.csv'], a[href*='download'], a[href*='api']")
        log.info(f"  Links download/API: {len(links)}")
        for l in links[:5]:
            log.info(f"    '{l.text.strip()[:40]}' → {l.get_attribute('href')[:100]}")
        return True
    return False

log.info("="*70)

# ── MAPA / SIMBIOSE ────────────────────────────────────────────────────────────
log.info("TESTE — MAPA / SIMBIOSE (Ministério da Agricultura)")
log.info("="*70)

driver = make_driver()
try:
    # Portal de biocombustíveis do MAPA
    urls_mapa = [
        ("mapa_0", "https://indicadores.agricultura.gov.br/agrostat/index.htm"),
        ("mapa_1", "https://www.gov.br/agricultura/pt-br/assuntos/sustentabilidade/agroenergia/dados-do-setor"),
        ("mapa_2", "https://sistemas.agricultura.gov.br/simco/biocombustiveis"),
    ]
    for label, url in urls_mapa:
        check_page(driver, url, label)
        time.sleep(2)
finally:
    driver.quit()

# ── UDOP ──────────────────────────────────────────────────────────────────────
log.info("\n" + "="*70)
log.info("TESTE — UDOP (dados etanol hidratado ao produtor)")
log.info("="*70)

driver2 = make_driver()
try:
    urls_udop = [
        ("udop_0", "https://www.udop.com.br/index.php/cana/item/etanol"),
        ("udop_1", "https://www.udop.com.br/index.php/etanol/preco-etanol-hidratado"),
        ("udop_2", "https://www.udop.com.br/index.php/cana/tabela_consecana_saopaulo"),
    ]
    for label, url in urls_udop:
        check_page(driver2, url, label, wait=10)
        time.sleep(2)
finally:
    driver2.quit()

log.info("\n" + "="*70)
log.info("FIM")
