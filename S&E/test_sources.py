#!/usr/bin/env python3
"""test_sources.py v6 — Testa BCB SGS série 28045 (etanol hidratado CEPEA)"""
import requests, logging
logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)

# BCB SGS — séries de etanol hidratado
# 28045 = Etanol hidratado — preço ao produtor SP (CEPEA) — R$/litro — diário
# 1399  = Etanol hidratado — preço ao consumidor SP (ANP) — R$/litro — semanal
SERIES = {
    28045: "Etanol hidratado produtor SP - CEPEA (R$/l) diário",
    1399:  "Etanol hidratado consumidor SP - ANP (R$/l) semanal",
    28052: "Etanol hidratado produtor GO - CEPEA (R$/l) diário",
    28046: "Etanol hidratado produtor AL - CEPEA (R$/l) diário",
    28050: "Etanol anidro produtor SP - CEPEA (R$/l) diário",
}

BCB_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{cod}/dados?formato=json&dataInicial=01/01/2024&dataFinal=07/04/2026"

log.info("="*70)
log.info("TESTE BCB SGS — Séries de Etanol")
log.info("="*70)

for cod, desc in SERIES.items():
    url = BCB_URL.format(cod=cod)
    try:
        r = requests.get(url, timeout=20)
        log.info(f"\n[{r.status_code}] Série {cod}: {desc}")
        log.info(f"  URL: {url[:80]}")
        if r.status_code == 200:
            data = r.json()
            log.info(f"  Registros: {len(data)}")
            if data:
                log.info(f"  Primeiro: {data[0]}")
                log.info(f"  Último  : {data[-1]}")
                log.info(f"  Amostra : {data[-3:]}")
        else:
            log.warning(f"  Erro: {r.text[:200]}")
    except Exception as e:
        log.error(f"  Exceção: {e}")

log.info("\n" + "="*70)
log.info("Teste histórico completo — série 28045 desde 2010")
log.info("="*70)
try:
    url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.28045/dados?formato=json&dataInicial=01/01/2010&dataFinal=07/04/2026"
    r = requests.get(url, timeout=30)
    if r.status_code == 200:
        data = r.json()
        log.info(f"Total registros: {len(data)}")
        log.info(f"Primeiro: {data[0] if data else '—'}")
        log.info(f"Último  : {data[-1] if data else '—'}")
    else:
        log.warning(f"Status {r.status_code}: {r.text[:200]}")
except Exception as e:
    log.error(f"Erro: {e}")

log.info("="*70)
