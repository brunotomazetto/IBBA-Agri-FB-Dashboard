#!/usr/bin/env python3
"""
test_sources.py — Testa undetected-chromedriver no CEPEA
"""
import time, logging, os
from pathlib import Path

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)

DOWNLOAD_DIR = Path("/tmp/cepea_downloads")
DOWNLOAD_DIR.mkdir(exist_ok=True)

TARGET = "https://cepea.esalq.usp.br/br/indicador/etanol.aspx"

try:
    import undetected_chromedriver as uc
except ImportError:
    raise SystemExit("pip install undetected-chromedriver")

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

log.info("Iniciando undetected Chrome...")

options = uc.ChromeOptions()

# Configura download automático para a pasta
prefs = {
    "download.default_directory": str(DOWNLOAD_DIR),
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True,
}
options.add_experimental_option("prefs", prefs)

# Argumentos para ambiente CI (sem GPU, sem sandbox)
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--disable-gpu")
options.add_argument("--window-size=1280,800")
options.add_argument("--lang=pt-BR")

# NÃO headless — roda com display virtual via Xvfb no workflow
# options.add_argument("--headless")  # comentado propositalmente

try:
    driver = uc.Chrome(options=options, version_main=None)
    log.info("Chrome iniciado com sucesso.")

    log.info(f"Navegando para {TARGET}")
    driver.get(TARGET)

    log.info("Aguardando 30s para Turnstile resolver...")
    time.sleep(30)

    # Screenshot do estado atual
    driver.save_screenshot("/tmp/uc_screenshot_1.png")
    log.info(f"Screenshot 1 salvo. Title: '{driver.title}'")
    log.info(f"URL atual: {driver.current_url}")

    # Verifica se passou do Cloudflare
    page_source = driver.page_source
    has_cf     = "cloudflare" in page_source.lower() and "confirme" in page_source.lower()
    has_cepea  = "etanol" in page_source.lower() and "cepea" in page_source.lower() and not has_cf
    log.info(f"Cloudflare challenge ativo: {has_cf}")
    log.info(f"Página CEPEA carregada:     {has_cepea}")

    if has_cepea:
        log.info("✅ Passou pelo Cloudflare! Procurando link de download...")
        # Procura o link de download do Excel
        try:
            links = driver.find_elements(By.CSS_SELECTOR, "a[href*='widgetpastas'], a[href*='.xls']")
            log.info(f"Links de download encontrados: {len(links)}")
            for l in links[:5]:
                log.info(f"  {l.text!r} → {l.get_attribute('href')}")

            if links:
                log.info(f"Clicando no download: {links[0].get_attribute('href')}")
                links[0].click()
                time.sleep(10)  # aguarda download

                # Verifica arquivos baixados
                files = list(DOWNLOAD_DIR.iterdir())
                log.info(f"Arquivos em {DOWNLOAD_DIR}: {[f.name for f in files]}")
        except Exception as e:
            log.error(f"Erro ao buscar download: {e}")
    else:
        log.warning("❌ Ainda no Cloudflare challenge após 30s")
        # Tenta mais 30s
        log.info("Aguardando mais 30s...")
        time.sleep(30)
        driver.save_screenshot("/tmp/uc_screenshot_2.png")
        log.info(f"Screenshot 2. Title: '{driver.title}'")
        page_source = driver.page_source
        has_cf2 = "confirme" in page_source.lower()
        log.info(f"Cloudflare ainda ativo: {has_cf2}")

except Exception as e:
    log.error(f"Erro: {e}")
    import traceback
    traceback.print_exc()
finally:
    try:
        driver.quit()
    except: pass

log.info("FIM")
