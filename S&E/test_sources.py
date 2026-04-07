#!/usr/bin/env python3
"""
test_sources.py — Tenta clicar no Turnstile checkbox e acessar CEPEA
"""
import time, logging, os
from pathlib import Path

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)

DOWNLOAD_DIR = Path("/tmp/cepea_downloads")
DOWNLOAD_DIR.mkdir(exist_ok=True)
TARGET = "https://cepea.esalq.usp.br/br/indicador/etanol.aspx"

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
import random

log.info("Iniciando undetected Chrome...")
options = uc.ChromeOptions()
prefs = {
    "download.default_directory": str(DOWNLOAD_DIR),
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
}
options.add_experimental_option("prefs", prefs)
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--disable-gpu")
options.add_argument("--window-size=1280,900")
options.add_argument("--lang=pt-BR")

driver = uc.Chrome(options=options, version_main=None)

try:
    log.info(f"Navegando para {TARGET}")
    driver.get(TARGET)
    time.sleep(8)
    driver.save_screenshot("/tmp/s1_inicial.png")
    log.info(f"S1 | Title: '{driver.title}' | URL: {driver.current_url}")

    # Verifica se está no challenge
    if "moment" in driver.title.lower() or "verify" in driver.page_source.lower():
        log.info("Cloudflare detectado — tentando clicar no checkbox Turnstile...")

        # O Turnstile fica dentro de um iframe
        iframes = driver.find_elements(By.TAG_NAME, "iframe")
        log.info(f"Iframes encontrados: {len(iframes)}")
        for i, iframe in enumerate(iframes):
            src = iframe.get_attribute("src") or ""
            log.info(f"  iframe {i}: {src[:100]}")

        # Tenta clicar no checkbox dentro do iframe do Turnstile
        turnstile_iframe = None
        for iframe in iframes:
            src = iframe.get_attribute("src") or ""
            if "turnstile" in src or "challenge" in src:
                turnstile_iframe = iframe
                break

        if turnstile_iframe:
            log.info("Entrando no iframe do Turnstile...")
            driver.switch_to.frame(turnstile_iframe)
            time.sleep(2)

            # Procura o checkbox
            checkboxes = driver.find_elements(By.CSS_SELECTOR, "input[type='checkbox'], .ctp-checkbox, [role='checkbox']")
            log.info(f"Checkboxes no iframe: {len(checkboxes)}")

            if checkboxes:
                cb = checkboxes[0]
                # Simula movimento humano antes de clicar
                actions = ActionChains(driver)
                actions.move_to_element(cb)
                actions.pause(random.uniform(0.3, 0.8))
                actions.click(cb)
                actions.perform()
                log.info("Clique no checkbox realizado.")
            else:
                # Tenta clicar por coordenadas (centro do checkbox visível)
                log.info("Checkbox não encontrado por seletor — tentando por coordenadas...")
                try:
                    body = driver.find_element(By.TAG_NAME, "body")
                    actions = ActionChains(driver)
                    actions.move_to_element_with_offset(body, 30, 30)
                    actions.pause(random.uniform(0.5, 1.0))
                    actions.click()
                    actions.perform()
                    log.info("Clique por coordenadas realizado.")
                except Exception as e:
                    log.warning(f"Clique por coordenadas falhou: {e}")

            driver.switch_to.default_content()
        else:
            log.warning("iframe do Turnstile não encontrado — tentando clicar no body")
            # Fallback: clica onde o checkbox deveria estar na página principal
            try:
                cb_div = driver.find_element(By.CSS_SELECTOR, ".ctp-checkbox-label, [class*='checkbox'], [class*='turnstile']")
                ActionChains(driver).move_to_element(cb_div).pause(0.5).click().perform()
                log.info("Clique no div do checkbox.")
            except Exception as e:
                log.warning(f"Fallback falhou: {e}")

        # Aguarda resolução após o clique
        log.info("Aguardando resolução do Turnstile (20s)...")
        time.sleep(20)
        driver.save_screenshot("/tmp/s2_pos_clique.png")
        log.info(f"S2 | Title: '{driver.title}' | URL: {driver.current_url}")

    # Verifica se agora está na página real
    page = driver.page_source
    passou = "etanol" in page.lower() and "indicador" in page.lower() and "moment" not in driver.title.lower()
    log.info(f"Passou pelo Cloudflare: {passou}")

    if passou:
        log.info("✅ Página CEPEA carregada! Aguardando conteúdo...")
        time.sleep(5)
        driver.save_screenshot("/tmp/s3_cepea.png")

        # Procura links de download
        links = driver.find_elements(By.CSS_SELECTOR, "a[href*='widgetpastas'], a[href*='.xls'], a[href*='download']")
        log.info(f"Links de download: {len(links)}")
        for l in links[:5]:
            log.info(f"  '{l.text}' → {l.get_attribute('href')}")

        # Lista todos os links da página para debug
        all_links = driver.find_elements(By.TAG_NAME, "a")
        log.info(f"Total de links na página: {len(all_links)}")
        for l in all_links[:20]:
            href = l.get_attribute("href") or ""
            txt  = l.text.strip()[:40]
            if href and "cloudflare" not in href:
                log.info(f"  '{txt}' → {href[:100]}")

except Exception as e:
    log.error(f"Erro: {e}")
    import traceback; traceback.print_exc()
    try: driver.save_screenshot("/tmp/s_erro.png")
    except: pass
finally:
    try: driver.quit()
    except: pass

log.info("FIM")
