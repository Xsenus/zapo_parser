import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import json
import os
import random
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from threading import Lock
from utils import load_proxies, get_proxy_dict, proxy_lock

# === Настройки ===
INPUT_FILE = "stage6_versions_detailed.json"
OUTPUT_FILE = "stage7_parts_detailed.json"
PROXY_FILE = "proxies_cleaned.txt"
PROXY_ALIVE_FILE = "proxies_alive.txt"
TMP_DIR = "stage7_temp_results"
LOG_DIR = "zapo_logs"
THREADS = 1000
RETRIES = 10

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

# === Инициализация ===
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(TMP_DIR, exist_ok=True)
log_file_path = os.path.join(LOG_DIR, f"parts_parse_log_{datetime.now():%Y%m%d_%H%M%S}.txt")
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

# === Получение HTML с прокси ===
def fetch_html(url):
    """Download a URL using available proxies with thread safety."""
    global proxies, working_proxies
    with proxy_lock:
        proxy_list = proxies.copy()
    random.shuffle(proxy_list)

    while proxy_list:
        proxy = proxy_list.pop()
        proxy_dict = get_proxy_dict(proxy)
        try:
            response = requests.get(url, headers=HEADERS, timeout=10, proxies=proxy_dict)
            response.raise_for_status()
            with proxy_lock:
                if proxy not in working_proxies:
                    working_proxies.append(proxy)
            return response.text
        except Exception as e:
            log(f"[PROXY ERROR] {proxy} — {e}")
            with proxy_lock:
                if proxy in proxies:
                    proxies.remove(proxy)

    try:
        log("[INFO] Переход к соединению без прокси")
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        return response.text
    except Exception as e:
        log(f"[ERROR] Ошибка без прокси при загрузке {url}: {str(e)}")
        return None

# === Парсинг деталей на странице ===
def parse_parts(modification_url):
    html = fetch_html(modification_url)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select("tr[data-goodsgroup]")
    parts = []

    for row in rows:
        img_tag = row.select_one("td img")
        image_url = img_tag['src'] if img_tag else ""

        name_tag = row.select_one("td:nth-of-type(2) b")
        name = name_tag.get_text(strip=True) if name_tag else ""

        group_tag = row.select_one("td:nth-of-type(3) a")
        group_name = group_tag.get_text(strip=True) if group_tag else ""

        search_tag = row.select_one("td a.fr-btn-primary")
        search_url = urljoin("https://zapo.ru", search_tag['href']) if search_tag else ""

        parts.append({
            "name": name,
            "group": group_name,
            "image_url": image_url,
            "search_url": search_url
        })

    return parts

# === Формирование имени временного файла ===
def get_tmp_filename(brand, model, version, mod_name):
    safe_name = f"{brand}_{model}_{version}_{mod_name}".replace(" ", "_")
    return os.path.join(TMP_DIR, f"{safe_name}.json")

# === Обработка одной модификации ===
def process_modification(mod, parent_item, max_retries=RETRIES):
    brand = parent_item["brand"]
    model = parent_item["model"]
    version = parent_item["version"]
    mod_name = mod["modification"]
    url = mod.get("modification_url")
    filename = get_tmp_filename(brand, model, version, mod_name)

    # Проверка на уже обработанный и валидный файл
    if os.path.exists(filename):
        try:
            with open(filename, "r", encoding="utf-8") as f:
                existing = json.load(f)
            mods = existing.get("modifications", [])
            if not mods or not mods[0].get("parts"):
                raise ValueError("Empty parts in cached result")
        except Exception:
            log(f"[WARNING] Удаляю повреждённый файл: {filename}")
            os.remove(filename)
        else:
            return None

    if not url:
        return None

    for attempt in range(1, max_retries + 1):
        parts = parse_parts(url)
        if parts:
            mod["parts"] = parts
            with save_lock:
                full_structure = parent_item.copy()
                full_structure["modifications"] = [mod]
                with open(filename, "w", encoding="utf-8") as f:
                    json.dump(full_structure, f, ensure_ascii=False, indent=2)

            log(f"[OK] {brand} | {model} | {version} | {mod_name} — {len(parts)} деталей")
            return True
        else:
            log(f"[RETRY {attempt}] {brand} | {model} | {version} | {mod_name} — нет деталей")

    log(f"[FAILED] {brand} | {model} | {version} | {mod_name} — не удалось после {max_retries} попыток (URL {url})")
    return False

# === Основной запуск ===
def main():
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        versions = json.load(f)

    total_modifications = sum(len(v.get("modifications", [])) for v in versions)
    log(f"🔍 Загружено моделей: {len(versions)}, модификаций: {total_modifications}")

    tasks = []
    for item in versions:
        for mod in item.get("modifications", []):
            fname = get_tmp_filename(item["brand"], item["model"], item["version"], mod["modification"])
            if not os.path.exists(fname):
                tasks.append((mod, item))

    log(f"➡️ К обработке осталось: {len(tasks)} модификаций")

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = [executor.submit(process_modification, mod, parent) for mod, parent in tasks]
        for _ in tqdm(as_completed(futures), total=len(futures), desc="🔧 Обработка модификаций"):
            pass

    # Сборка итогового JSON с защитой от повреждённых файлов
    final_data = []
    for fname in os.listdir(TMP_DIR):
        fpath = os.path.join(TMP_DIR, fname)
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                final_data.append(json.load(f))
        except Exception as e:
            log(f"[SKIPPED] {fname} — ошибка чтения: {e}")

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(final_data, f, ensure_ascii=False, indent=2)

    log(f"✅ Данные сохранены в {OUTPUT_FILE}")
    log(f"📝 Рабочие прокси: {len(working_proxies)}")

    with open(PROXY_ALIVE_FILE, "w", encoding="utf-8") as f:
        for proxy in working_proxies:
            f.write(proxy + "\n")

if __name__ == "__main__":
    main()
