from bs4 import BeautifulSoup
from urllib.parse import urljoin
import json
import os
import re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from threading import Lock
from utils import load_proxies, proxy_lock, fetch_with_proxies
import hashlib

INPUT_FILE = "stage5_carbase.json"
OUTPUT_FILE = "stage6_versions_detailed.json"
TEMP_DIR = "stage6_temp_results"
LOG_DIR = "zapo_logs"
PROXY_FILE = "proxies_cleaned.txt"
PROXY_ALIVE_FILE = "proxies_alive.txt"
THREADS = 100

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(TEMP_DIR, exist_ok=True)
log_file_path = os.path.join(LOG_DIR, f"carbase_versions_log_{datetime.now():%Y%m%d_%H%M%S}.txt")

log_lock = Lock()
alive_proxies = set()
used_proxies = []

def log(message: str):
    print(message)
    with log_lock:
        with open(log_file_path, "a", encoding="utf-8") as f:
            f.write(message + "\n")

proxies = load_proxies(PROXY_FILE, PROXY_ALIVE_FILE)

def fetch_html(url: str) -> tuple[str | None, str | None]:
    """Load *url* using :func:`utils.fetch_with_proxies` and track good proxies."""
    html, proxy_used = fetch_with_proxies(
        url,
        proxies,
        used_proxies,
        headers=HEADERS,
        retries=3,
        logger=log,
    )
    if proxy_used:
        with proxy_lock:
            if proxy_used not in alive_proxies:
                alive_proxies.add(proxy_used)
                with open(PROXY_ALIVE_FILE, "a", encoding="utf-8") as f:
                    f.write(proxy_used + "\n")
    return html, proxy_used

def parse_version_details(version_url, exclude_proxy=None):
    tried_proxies = set()
    attempt = 0
    while attempt < 3:
        html, proxy_used = fetch_html(version_url)
        if not html:
            attempt += 1
            continue

        soup = BeautifulSoup(html, "html.parser")
        rows = soup.select("table tr[onclick]")
        details = []

        for row in rows:
            cols = row.find_all("td")
            if len(cols) < 6:
                continue

            detail = {
                "modification": cols[0].get_text(strip=True),
                "production_years": cols[1].get_text(strip=True),
                "fuel": cols[2].get_text(strip=True),
                "power_hp": cols[3].get_text(strip=True),
                "engine_code": cols[4].get_text(strip=True),
                "engine_volume": cols[5].get_text(strip=True),
            }

            onclick = row.get("onclick", "")
            match = re.search(r"location\.href='([^']+)'", onclick)
            if match:
                detail["modification_url"] = urljoin("https://zapo.ru", match.group(1))

            details.append(detail)

        if details:
            return details

        # 0 модификаций — пробуем с другим прокси
        attempt += 1
        if proxy_used:
            tried_proxies.add(proxy_used)
            with proxy_lock:
                if proxy_used in proxies:
                    proxies.remove(proxy_used)

    return []

def hash_filename(url):
    return hashlib.md5(url.encode("utf-8")).hexdigest() + ".json"

def process_item(item):
    version_url = item.get("version_url")
    if not version_url:
        return

    file_name = os.path.join(TEMP_DIR, hash_filename(version_url))
    if os.path.exists(file_name):
        return  # Уже обработан

    details = parse_version_details(version_url)
    item["modifications"] = details

    with open(file_name, "w", encoding="utf-8") as f:
        json.dump(item, f, ensure_ascii=False, indent=2)

    log(f"[OK] {item['brand']} | {item['model']} | {item['version']} — {len(details)} модификаций")

def main():
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        versions = json.load(f)

    processed_files = set(os.listdir(TEMP_DIR))
    remaining = [v for v in versions if hash_filename(v["version_url"]) not in processed_files]

    log(f"🔍 Всего версий: {len(versions)}")
    log(f"➡️ Осталось обработать: {len(remaining)}")

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        list(tqdm(executor.map(process_item, remaining), total=len(remaining), desc="📦 Модификации"))

    # Финальное объединение
    all_data = []
    for file in os.listdir(TEMP_DIR):
        with open(os.path.join(TEMP_DIR, file), "r", encoding="utf-8") as f:
            all_data.append(json.load(f))

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)

    log(f"✅ Обработка завершена. Всего: {len(all_data)} записей")
    log(f"📝 Лог файл: {log_file_path}")

if __name__ == "__main__":
    main()
