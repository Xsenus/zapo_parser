import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import os
import json
import random
from datetime import datetime
from threading import Lock

INPUT_FILE = "stage9_brands.json"
OUTPUT_FILE = "stage10_models_detailed.json"
PROXY_FILE = "proxies_cleaned.txt"
PROXY_ALIVE_FILE = "proxies_alive.txt"
TMP_DIR = "stage10_temp_results"
LOG_DIR = "zapo_logs"
BASE_URL = "https://zapo.ru"
RETRIES = 15

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(TMP_DIR, exist_ok=True)
log_file_path = os.path.join(LOG_DIR, f"models_parse_log_{datetime.now():%Y%m%d_%H%M%S}.txt")
log_lock = Lock()
save_lock = Lock()

def log(message: str):
    print(message)
    with log_lock:
        with open(log_file_path, "a", encoding="utf-8") as f:
            f.write(message + "\n")

def load_proxies():
    if os.path.exists(PROXY_ALIVE_FILE):
        with open(PROXY_ALIVE_FILE, "r", encoding="utf-8") as f:
            proxies = [line.strip() for line in f if line.strip()]
        if proxies:
            log(f"[INFO] Используем {len(proxies)} живых прокси")
            return proxies
    if os.path.exists(PROXY_FILE):
        with open(PROXY_FILE, "r", encoding="utf-8") as f:
            proxies = [line.strip() for line in f if line.strip()]
        log(f"[INFO] Используем {len(proxies)} прокси")
        return proxies
    return []

proxies = load_proxies()
working_proxies = []

def fetch_html(url):
    global proxies, working_proxies
    for attempt in range(1, RETRIES + 1):
        local_proxies = proxies.copy()
        random.shuffle(local_proxies)

        while local_proxies:
            proxy = local_proxies.pop()
            try:
                response = requests.get(
                    url, headers=HEADERS,
                    proxies={"http": f"socks5h://{proxy}", "https": f"socks5h://{proxy}"},
                    timeout=10
                )
                response.raise_for_status()
                if proxy not in working_proxies:
                    working_proxies.append(proxy)
                return response.text
            except Exception as e:
                log(f"[PROXY ERROR] {proxy} — {e}")
                if proxy in proxies:
                    proxies.remove(proxy)

        try:
            log(f"[ATTEMPT {attempt}] Пробуем без прокси...")
            response = requests.get(url, headers=HEADERS, timeout=10)
            response.raise_for_status()
            return response.text
        except Exception as e:
            log(f"[ERROR] Попытка {attempt} без прокси не удалась: {e}")

    log(f"[FAILED] Не удалось загрузить: {url}")
    return None

def parse_models_page(url):
    for attempt in range(1, RETRIES + 1):
        html = fetch_html(url)
        if not html:
            continue

        soup = BeautifulSoup(html, "html.parser")
        blocks = soup.select("div.productTile a.goodDescriptionLink")
        if not blocks:
            log(f"[RETRY {attempt}] Блоки моделей не найдены в: {url}")
            continue

        results = []
        for a_tag in blocks:
            img_tag = a_tag.select_one("img.goodDescriptionImg")
            name_tag = a_tag.select_one("span.goodDescriptionName")

            results.append({
                "name": name_tag.get_text(strip=True) if name_tag else "",
                "image_url": img_tag["src"] if img_tag else "",
                "modification_url": urljoin(BASE_URL, a_tag["href"])
            })

        return results

    return []

def main():
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        brands_data = json.load(f)

    all_results = []

    for category in ["foreign", "native", "moto"]:
        for brand in brands_data.get(category, []):
            name = brand["name"]
            brand_url = brand["link"]
            image_url = brand["image_url"]
            safe_name = name.replace(" ", "_").replace("/", "_")
            tmp_file = os.path.join(TMP_DIR, f"{category}_{safe_name}.json")

            if os.path.exists(tmp_file):
                try:
                    with open(tmp_file, "r", encoding="utf-8") as f:
                        all_results.append(json.load(f))
                    log(f"[SKIP] Уже обработан: {name} ({category})")
                    continue
                except Exception as e:
                    log(f"[WARNING] Повреждённый файл удалён: {tmp_file}")
                    os.remove(tmp_file)

            log(f"🔍 {category.upper()} → {name}")
            models = parse_models_page(brand_url)

            brand_result = {
                "brand": name,
                "type": category,
                "image_url": image_url,
                "models": models
            }

            with open(tmp_file, "w", encoding="utf-8") as f:
                json.dump(brand_result, f, ensure_ascii=False, indent=2)

            all_results.append(brand_result)
            log(f"[OK] {name} — моделей: {len(models)}")

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2)

    log(f"✅ Финальный результат сохранён в {OUTPUT_FILE}")
    log(f"📝 Рабочие прокси: {len(working_proxies)}")

    with open(PROXY_ALIVE_FILE, "w", encoding="utf-8") as f:
        for proxy in working_proxies:
            f.write(proxy + "\n")

if __name__ == "__main__":
    main()
