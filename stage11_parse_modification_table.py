import json
import os
import re
import time
import requests
from datetime import datetime
from threading import Lock
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# ---------- Константы ----------
INPUT_FILE = "stage10_models_detailed.json"
FAILED_FILE = "stage11_failed.json"
OUTPUT_FILE = "stage11_modifications_detailed.json"
PROXY_FILE = "proxies_cleaned.txt"
TMP_DIR = "stage11_temp_results"
LOG_DIR = "zapo_logs"
THREADS_REQUESTS = 250
THREADS_SELENIUM = 15
PAGE_TIMEOUT = 30

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

# ---------- Подготовка ----------
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(TMP_DIR, exist_ok=True)
log_file_path = os.path.join(LOG_DIR, f"stage11_log_{datetime.now():%Y%m%d_%H%M%S}.txt")
log_lock = Lock()
save_lock = Lock()

good_proxies = []
used_proxies = set()
requests_phase_results = []
failed_items = []

def log(msg):
    print(msg)
    with log_lock:
        with open(log_file_path, "a", encoding="utf-8") as f:
            f.write(msg + "\n")

def safe_filename(name):
    return re.sub(r'[\\/:"*?<>|]+', "_", name)

def load_proxies():
    if not os.path.exists(PROXY_FILE):
        return []
    with open(PROXY_FILE, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]

def extract_rows(soup):
    result = []
    rows = soup.select("table tr")
    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 6:
            continue
        link_tag = cols[0].find("a")
        result.append({
            "name": link_tag.get_text(strip=True) if link_tag else "",
            "url": f"https://zapo.ru{link_tag['href']}" if link_tag and link_tag.has_attr("href") else "",
            "year": cols[1].get_text(strip=True),
            "gearbox": cols[3].get_text(strip=True),
            "country": cols[4].get_text(strip=True),
            "description": cols[5].get_text(strip=True),
        })
    return result

def setup_driver(proxy):
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(f'--proxy-server=socks5://{proxy}')
    log(f"[PROXY] Используется: {proxy}")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)

def try_requests_first(url, proxy):
    try:
        proxies = {
            "http": f"socks5h://{proxy}",
            "https": f"socks5h://{proxy}"
        }
        response = requests.get(url, headers=HEADERS, proxies=proxies, timeout=10)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")
            if soup.select("table tr"):
                log(f"[REQUESTS] Успешно загружено через proxy {proxy}")
                return extract_rows(soup), proxy
    except Exception as e:
        log(f"[REQUESTS ERROR] {proxy} — {e}")
    return [], None

def parse_with_selenium(url, proxy):
    all_rows = []
    visited_pages = set()
    try:
        driver = setup_driver(proxy)
        driver.set_page_load_timeout(PAGE_TIMEOUT)
        driver.get(url)
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "dataTable")))
        soup = BeautifulSoup(driver.page_source, "html.parser")
        all_rows.extend(extract_rows(soup))
        visited_pages.add("1")

        while True:
            current_li = soup.select_one("ul.fr-pagination li.active span")
            if current_li:
                visited_pages.add(current_li.text.strip())

            next_a = next((a for a in soup.select("ul.fr-pagination li a.selectFilterPage")
                           if a.text.strip() not in visited_pages), None)

            if next_a:
                page_text = next_a.text.strip()
                visited_pages.add(page_text)
                try:
                    clickable = driver.find_element(By.LINK_TEXT, page_text)
                    driver.execute_script("arguments[0].click();", clickable)
                    time.sleep(2)
                    soup = BeautifulSoup(driver.page_source, "html.parser")
                    all_rows.extend(extract_rows(soup))
                except Exception as e:
                    log(f"[ERROR] Не удалось перейти на страницу {page_text}: {e}")
                    break
            else:
                break

        driver.quit()
        return all_rows, True, len(visited_pages)
    except Exception as e:
        log(f"[SELENIUM ERROR] Прокси {proxy} — {e}")
        try:
            driver.quit()
        except:
            pass
    return [], False, 0

def save_temp_file(item, rows, all_pages_loaded, pages_loaded_count):
    enriched = {k: v for k, v in item.items() if k != "proxy"}  # исключаем 'proxy'
    enriched["modification_table"] = rows
    enriched["all_pages_loaded"] = all_pages_loaded
    enriched["pages_loaded"] = pages_loaded_count
    filename = os.path.join(TMP_DIR, f"{safe_filename(item['brand'])}_{safe_filename(item['model'])}.json")
    with save_lock:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(enriched, f, ensure_ascii=False, indent=2)

def prepare_requests_phase(item):
    filename = os.path.join(TMP_DIR, f"{safe_filename(item['brand'])}_{safe_filename(item['model'])}.json")
    if os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            existing = json.load(f)
        if existing.get("all_pages_loaded", False):
            log(f"[SKIP] {item['brand']} | {item['model']} — уже обработан.")
            return

    proxy_list = load_proxies()
    for proxy in proxy_list:
        if proxy in used_proxies:
            continue
        used_proxies.add(proxy)
        rows, ok_proxy = try_requests_first(item["modification_url"], proxy)
        if rows:
            item["proxy"] = ok_proxy
            requests_phase_results.append(item)
            if ok_proxy and ok_proxy not in good_proxies:
                good_proxies.append(ok_proxy)
            save_temp_file(item, rows, all_pages_loaded=False, pages_loaded_count=1)
            return

    # ⛔ Если не удалось ни с одним прокси
    failed_items.append(item)
    log(f"[FAILED REQUESTS] {item['brand']} | {item['model']} — не удалось получить первую страницу через requests")

def selenium_phase(item):
    url = item["modification_url"]
    proxy_list = []
    if "proxy" in item:
        proxy_list.append(item["proxy"])

    extra_proxies = [p for p in good_proxies + load_proxies() if p not in proxy_list]
    proxy_list.extend(extra_proxies)

    if not proxy_list:
        log(f"[SELENIUM SKIP] {item['brand']} | {item['model']} — нет доступных прокси")
        failed_items.append(item)
        return
    
    for proxy in proxy_list:
        if proxy in used_proxies:
            continue
        used_proxies.add(proxy)
        log(f"[SELENIUM] {item['brand']} | {item['model']} — пробуем через {proxy}")
        rows, success, page_count = parse_with_selenium(url, proxy)
        if rows:
            save_temp_file(item, rows, all_pages_loaded=success, pages_loaded_count=page_count)
            log(f"[FINAL] {item['brand']} | {item['model']} — {len(rows)} строк | pages={page_count} | success={success}")
            return
        else:
            log(f"[RETRY SELENIUM] {item['brand']} | {item['model']} — прокси {proxy} не дал результат")

    failed_items.append(item)
    log(f"[FAILED] {item['brand']} | {item['model']} — не удалось обработать Selenium")

def main():
    all_tasks = []

    # Загружаем из основного входного файла
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    for brand in data:
        for model in brand.get("models", []):
            all_tasks.append({
                "brand": brand.get("brand"),
                "type": brand.get("type"),
                "brand_image": brand.get("image_url"),
                "model": model.get("name"),
                "model_image": model.get("image_url"),
                "modification_url": model.get("modification_url")
            })

    # Добавляем ранее неудавшиеся (если есть)
    if os.path.exists(FAILED_FILE):
        with open(FAILED_FILE, "r", encoding="utf-8") as f:
            failed_previous = json.load(f)
            all_tasks.extend(failed_previous)
            log(f"🔁 Повторное добавление {len(failed_previous)} моделей из {FAILED_FILE}")

    log(f"🔍 Всего моделей для обработки: {len(all_tasks)}")

    # 1️⃣ Первичная обработка через requests
    with ThreadPoolExecutor(max_workers=THREADS_REQUESTS) as executor:
        list(tqdm(executor.map(prepare_requests_phase, all_tasks), total=len(all_tasks), desc="🌐 Requests-парсинг"))

    # 2️⃣ Добавляем модели с all_pages_loaded = false из TMP_DIR
    for fname in os.listdir(TMP_DIR):
        try:
            with open(os.path.join(TMP_DIR, fname), "r", encoding="utf-8") as f:
                model_data = json.load(f)
            if not model_data.get("all_pages_loaded", False):
                requests_phase_results.append(model_data)
        except Exception as e:
            log(f"[ERROR] Не удалось загрузить {fname}: {e}")

    log(f"🧠 Передано в Selenium-фазу: {len(requests_phase_results)} моделей")

    # 3️⃣ Selenium дообработка
    with ThreadPoolExecutor(max_workers=THREADS_SELENIUM) as executor:
        list(tqdm(executor.map(selenium_phase, requests_phase_results), total=len(requests_phase_results), desc="🧠 Selenium-парсинг"))

    # 4️⃣ Финальный сбор результатов
    final_data = []
    for fname in os.listdir(TMP_DIR):
        with open(os.path.join(TMP_DIR, fname), "r", encoding="utf-8") as f:
            final_data.append(json.load(f))

    total_rows = sum(len(d.get("modification_table", [])) for d in final_data)
    log(f"✅ Сохранено в {OUTPUT_FILE} — всего модификаций: {total_rows}")

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(final_data, f, ensure_ascii=False, indent=2)

    if failed_items:
        with open(FAILED_FILE, "w", encoding="utf-8") as f:
            json.dump(failed_items, f, ensure_ascii=False, indent=2)
        log(f"⚠️ Неудачных моделей: {len(failed_items)}. Сохранено в {FAILED_FILE}")
    else:
        if os.path.exists(FAILED_FILE):
            os.remove(FAILED_FILE)
        log("✅ Все модели успешно обработаны.")

if __name__ == "__main__":
    main()
