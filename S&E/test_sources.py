#!/usr/bin/env python3
"""test_sources.py — Navega UDOP para achar preço etanol hidratado ao produtor"""
import time, logging, re, subprocess, os

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By

# Acha o path real do Chrome e sua versão
chrome_path = subprocess.run(["which", "google-chrome"], capture_output=True, text=True).stdout.strip()
version_str = subprocess.run([chrome_path, "--version"], capture_output=True, text=True).stdout.strip()
major = int(version_str.split()[-1].split(".")[0])
log.info(f"Chrome: {chrome_path} | {version_str} | major={major}")

options = uc.ChromeOptions()
options.binary_location = chrome_path  # força o path correto
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--disable-gpu")
options.add_argument("--window-size=1280,900")
options.add_argument("--lang=pt-BR")

driver = uc.Chrome(options=options, version_main=major)

try:
    log.info("Abrindo UDOP home...")
    driver.get("https://www.udop.com.br")
    time.sleep(8)
    driver.save_screenshot("/tmp/udop_home.png")

    all_links = driver.find_elements(By.TAG_NAME, "a")
    log.info(f"Total links na home: {len(all_links)}")

    keywords = ["etanol", "preço", "preco", "mercado", "hidratado", "produtor", "usina", "cotação", "cana"]
    relevant = []
    for l in all_links:
        href = (l.get_attribute("href") or "").lower()
        txt  = l.text.strip().lower()
        if any(k in href or k in txt for k in keywords):
            relevant.append((l.text.strip()[:50], l.get_attribute("href") or ""))

    log.info(f"\nLinks relevantes ({len(relevant)}):")
    for txt, href in relevant[:30]:
        log.info(f"  '{txt}' → {href[:100]}")

    candidates = list(dict.fromkeys([h for _, h in relevant if h and "udop.com.br" in h]))
    log.info(f"\n--- Testando {len(candidates)} URLs candidatas ---")

    for i, url in enumerate(candidates[:8]):
        log.info(f"\n[{i}] {url}")
        driver.get(url)
        time.sleep(6)
        title = driver.title
        page  = driver.page_source
        datas  = re.findall(r'\d{2}/\d{2}/202[456]', page)
        precos = re.findall(r'[23][,\.]\d{3,4}', page)
        log.info(f"  Title: '{title[:60]}'")
        log.info(f"  Datas 2024-2026: {list(set(datas))[:5]}")
        log.info(f"  Preços ~R$/l: {list(set(precos))[:8]}")
        driver.save_screenshot(f"/tmp/udop_{i}.png")

finally:
    driver.quit()

log.info("FIM")
