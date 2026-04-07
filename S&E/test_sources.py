#!/usr/bin/env python3
"""test_sources.py — Navega pelo menu UDOP para achar preço etanol hidratado ao produtor"""
import time, logging, re
from pathlib import Path

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By

options = uc.ChromeOptions()
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--disable-gpu")
options.add_argument("--window-size=1280,900")
options.add_argument("--lang=pt-BR")
driver = uc.Chrome(options=options, version_main=None)

try:
    # Abre a home do UDOP e coleta todos os links do menu
    log.info("Abrindo UDOP home...")
    driver.get("https://www.udop.com.br")
    time.sleep(8)
    driver.save_screenshot("/tmp/udop_home.png")

    # Coleta TODOS os links da página
    all_links = driver.find_elements(By.TAG_NAME, "a")
    log.info(f"Total links na home: {len(all_links)}")

    # Filtra links relevantes para etanol/preço/mercado
    keywords = ["etanol", "preço", "preco", "mercado", "hidratado", "produtor", "usina", "cotação"]
    relevant = []
    for l in all_links:
        href = (l.get_attribute("href") or "").lower()
        txt  = l.text.strip().lower()
        if any(k in href or k in txt for k in keywords):
            relevant.append((l.text.strip()[:50], l.get_attribute("href") or ""))

    log.info(f"\nLinks relevantes ({len(relevant)}):")
    for txt, href in relevant[:30]:
        log.info(f"  '{txt}' → {href[:100]}")

    # Testa as URLs mais promissoras
    log.info("\n--- Testando URLs UDOP candidatas ---")
    candidates = [h for _, h in relevant if h and "udop.com.br" in h]
    candidates = list(dict.fromkeys(candidates))  # dedup

    for url in candidates[:10]:
        log.info(f"\nTestando: {url}")
        driver.get(url)
        time.sleep(6)
        title = driver.title
        page  = driver.page_source

        # Procura preços de etanol (R$/l ou R$/m³)
        datas  = re.findall(r'\d{2}/\d{2}/202[456]', page)
        # Valores entre 1.5 e 5.0 (R$/litro) ou 1500-5000 (R$/m³)
        precos_l  = re.findall(r'[23][,\.]\d{3,4}', page)  # ~R$2-3/l
        precos_m3 = re.findall(r'[23]\d{3}[,\.]\d{2}', page)  # ~R$2000-3000/m³

        log.info(f"  Title: '{title[:60]}'")
        log.info(f"  Datas 2024-2026: {list(set(datas))[:5]}")
        log.info(f"  Preços ~R$/l: {list(set(precos_l))[:8]}")
        log.info(f"  Preços ~R$/m³: {list(set(precos_m3))[:5]}")
        driver.save_screenshot(f"/tmp/udop_cand_{candidates.index(url)}.png")

finally:
    driver.quit()

log.info("FIM")
