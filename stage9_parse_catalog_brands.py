# stage1_parse_catalog_brands.py

from bs4 import BeautifulSoup
from urllib.parse import urljoin
import os
import json
from datetime import datetime
from threading import Lock
from utils import load_proxies, proxy_lock, fetch_with_proxies

# === Константы ===
URLS = {
    "foreign": "https://zapo.ru/auto2dV2/?action=marks&typeCatalog=CARS_FOREIGN",
    "native": "https://zapo.ru/auto2dV2/?action=marks&typeCatalog=CARS_NATIVE",
    "moto": "https://zapo.ru/auto2dV2/?action=marks&typeCatalog=MOTORCYCLE"
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

BASE_URL = "https://zapo.ru"

PROXY_FILE = "proxies_cleaned.txt"
PROXY_ALIVE_FILE = "proxies_alive.txt"
TMP_DIR = "stage9_temp_results"
LOG_DIR = "zapo_logs"
OUTPUT_FILE = "stage9_brands.json"
RETRIES = 10

# === Инициализация ===
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(TMP_DIR, exist_ok=True)
log_file_path = os.path.join(LOG_DIR, f"brands_parse_log_{datetime.now():%Y%m%d_%H%M%S}.txt")
log_lock = Lock()
save_lock = Lock()

# === Логирование ===
def log(message: str):
    print(message)
    with log_lock:
        with open(log_file_path, "a", encoding="utf-8") as f:
            f.write(message + "\n")

# === Загрузка прокси ===
proxies = load_proxies(PROXY_FILE, PROXY_ALIVE_FILE)
working_proxies = []

# === Получение HTML через SOCKS5 прокси ===
def fetch_html(url: str) -> str | None:
    """Load *url* using :func:`utils.fetch_with_proxies`."""
    html, _ = fetch_with_proxies(
        url,
        proxies,
        working_proxies,
        headers=HEADERS,
        retries=RETRIES,
        logger=log,
    )
    return html

# === Парсинг одной категории (foreign/native/moto) ===
def parse_catalog(url):
    for attempt in range(1, RETRIES + 1):
        html = fetch_html(url)
        if not html:
            log(f"[RETRY {attempt}] Не удалось получить HTML: {url}")
            continue

        soup = BeautifulSoup(html, "html.parser")
        blocks = soup.select("a.catalogAuto2dMarkLink")

        if blocks:
            results = []
            for a_tag in blocks:
                name_tag = a_tag.select_one("span.catalogAuto2dMarkName")
                img_tag = a_tag.select_one("img")

                results.append({
                    "name": name_tag.get_text(strip=True) if name_tag else "",
                    "image_url": img_tag["src"] if img_tag else "",
                    "link": urljoin(BASE_URL, a_tag["href"])
                })

            return results

        else:
            log(f"[RETRY {attempt}] Блоки брендов не найдены в HTML: {url}")

    log(f"[FAILED] Не удалось получить данные по ссылке: {url}")
    return []

# === Основной запуск ===
def main():
    final_result = {}

    for key, url in URLS.items():
        log(f"🔍 Парсим: {key}")
        data = parse_catalog(url)
        final_result[key] = data
        log(f"[OK] {key} — {len(data)} брендов")
        # Сохраняем промежуточный
        tmp_path = os.path.join(TMP_DIR, f"{key}.json")
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    # Финальный JSON
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(final_result, f, ensure_ascii=False, indent=2)

    log(f"✅ Финальный результат сохранён в {OUTPUT_FILE}")
    log(f"📝 Рабочие прокси: {len(working_proxies)}")

    with open(PROXY_ALIVE_FILE, "w", encoding="utf-8") as f:
        for proxy in working_proxies:
            f.write(proxy + "\n")

if __name__ == "__main__":
    main()
