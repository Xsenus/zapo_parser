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
CHROME_DRIVER_PATH = ChromeDriverManager().install()

# ---------- Константы ----------
INPUT_FILE = "stage10_models_detailed.json"
FAILED_FILE = "stage11_failed.json"
OUTPUT_FILE = "stage11_modifications_detailed.json"
PROXY_FILE = "proxies_cleaned.txt"
TMP_DIR = "stage11_temp_results"
LOG_DIR = "zapo_logs"
THREADS_REQUESTS = 100
THREADS_SELENIUM = 10
PAGE_TIMEOUT = 30
RETRIES_REQUESTS = 10 
MIRRORS = [
    "https://part.avtomir.ru",
    "https://zapo.ru",
    "https://vindoc.ru",
    "https://autona88.ru",
    "https://b2b.autorus.ru",
    "https://xxauto.pro",
    "https://motexc.ru"
    ]

def with_mirror(url, mirror):
    return re.sub(r"https://[^/]+", mirror, url)

def is_rate_limited(html_text):
    return 'Превышен лимит запросов в день' in html_text

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(TMP_DIR, exist_ok=True)
log_file_path = os.path.join(LOG_DIR, f"stage11_log_{datetime.now():%Y%m%d_%H%M%S}.txt")
log_lock = Lock()
save_lock = Lock()

good_proxies = []
used_proxies = set()
requests_phase_results = []
failed_items = []

def extract_expected_modifications(soup):
    divs = soup.find_all("div")
    for div in divs:
        if "Модификаций:" in div.text:
            match = re.search(r"Модификаций:\s*(\d+)", div.text)
            if match:
                return int(match.group(1))
    return None

def get_pages_total_actual(soup):
    pages = soup.select("a.pageNumber.selectFilterPage")
    return max([int(a.text.strip()) for a in pages if a.text.strip().isdigit()], default=1)

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
    table = soup.select_one("table#dataTable")
    if not table:
        return []

    result = []
    rows = table.select("tbody > tr")

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

    log(f"[EXTRACT_ROWS] Найдено строк: {len(result)}")
    return result

def get_pages_total(soup):
    pages = soup.select("ul.fr-pagination li a.selectFilterPage")
    try:
        return max([int(p.text.strip()) for p in pages if p.text.strip().isdigit()], default=1)
    except:
        return 1

def setup_driver(proxy):
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(f'--proxy-server=socks5://{proxy}')
    log(f"[PROXY] Используется: {proxy}")
    service = Service(CHROME_DRIVER_PATH)
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
            rows = extract_rows(soup)
            pages_total = get_pages_total(soup)
            table_found = bool(soup.select("table tr"))
            log(f"[REQUESTS] Успешно загружено через proxy {proxy}, rows={len(rows)}")
            return rows, proxy, table_found, pages_total
    except Exception as e:
        log(f"[REQUESTS ERROR] {proxy} — {e}")
    return [], None, False, 0

def save_temp_file(item, rows, all_pages_loaded, pages_loaded, pages_total, table_found, modifications_expected=None):
    enriched = {k: v for k, v in item.items() if k != "proxy"}
    enriched.update({
        "modification_table": rows,
        "all_pages_loaded": all_pages_loaded,
        "pages_loaded": pages_loaded,
        "pages_total": pages_total,
        "table_found": table_found,
        "modifications_received": len(rows),
        "modifications_expected": modifications_expected
    })
    filename = os.path.join(TMP_DIR, f"{safe_filename(item['brand'])}_{safe_filename(item['model'])}.json")
    with save_lock:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(enriched, f, ensure_ascii=False, indent=2)
    log(f"[SAVE] Временный файл сохранён: {filename}")

def is_access_denied(html_text):
    return "Access denied to" in html_text or "<title>Access Denied</title>" in html_text

def prepare_requests_phase(item):
    filename = os.path.join(TMP_DIR, f"{safe_filename(item['brand'])}_{safe_filename(item['model'])}.json")
    if os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            existing = json.load(f)
        if existing.get("all_pages_loaded", False):
            log(f"[SKIP] {item['brand']} | {item['model']} — уже полностью обработан.")
            return

    proxy_list = load_proxies()
    used_proxies_per_item = set()
    tried_mirrors = set()

    for mirror in MIRRORS:
        url = with_mirror(item["modification_url"], mirror)
        mirror_limited = False

        for attempt in range(RETRIES_REQUESTS):
            for proxy in proxy_list:
                if proxy in used_proxies_per_item:
                    continue
                used_proxies_per_item.add(proxy)
                used_proxies.add(proxy)

                try:
                    proxies = {
                        "http": f"socks5h://{proxy}",
                        "https": f"socks5h://{proxy}"
                    }
                    
                    response = requests.get(url, headers=HEADERS, proxies=proxies, timeout=10)

                    if is_access_denied(response.text):
                        log(f"[ACCESS DENIED] {mirror} | {item['brand']} {item['model']} — доступ запрещён, пробуем другое зеркало.")
                        continue

                    if is_rate_limited(response.text):
                        log(f"[LIMIT] {mirror} | {item['brand']} {item['model']} — превышен лимит, пробуем другое зеркало.")
                        mirror_limited = True
                        break  # выход из прокси-цикла, но не всей функции

                    if response.status_code == 200:
                        soup = BeautifulSoup(response.text, "html.parser")
                        expected_modifications = extract_expected_modifications(soup)
                        rows = extract_rows(soup)
                        
                        if len(rows) == 0:
                            log(f"[EMPTY TABLE] {mirror} | {item['brand']} {item['model']} — таблица пуста, пробуем другое зеркало.")
                            mirror_limited = True
                            break
                                                
                        pages_total = get_pages_total(soup)
                        table_found = bool(soup.select("table tr"))
                        log(f"[REQUESTS] OK: {mirror} через {proxy}, rows={len(rows)}")

                        item["proxy"] = proxy
                        if proxy not in good_proxies:
                            good_proxies.append(proxy)

                        # Приводим URL к zapo.ru для унификации
                        item["modification_url"] = with_mirror(item["modification_url"], "https://zapo.ru")

                        save_temp_file(item, rows, all_pages_loaded=False,
                            pages_loaded=1, pages_total=pages_total,
                            table_found=table_found, modifications_expected=expected_modifications)
                        requests_phase_results.append(item)
                        return
                except Exception as e:
                    log(f"[REQUESTS ERROR] {mirror} | {proxy} — {e}")

            if mirror_limited:
                break  # переходим к следующему зеркалу

        log(f"[MIRROR FAIL] {mirror} не дал результат для {item['brand']} | {item['model']}")
        tried_mirrors.add(mirror)

    failed_items.append(item)
    log(f"[FAILED REQUESTS] {item['brand']} | {item['model']} — все зеркала и прокси не сработали")

def parse_with_selenium(url, proxy, start_page=2):
    all_rows = []
    visited_pages = set()
    driver = None
    try:
        driver = setup_driver(proxy)
        driver.set_page_load_timeout(PAGE_TIMEOUT)
        driver.get(url)
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "dataTable")))
        soup = BeautifulSoup(driver.page_source, "html.parser")
        expected_modifications = extract_expected_modifications(soup)
        pages_total = get_pages_total_actual(soup)
        visited_pages.add("1")
        all_rows.extend(extract_rows(soup))

        while True:
            current_li = soup.select_one("ul.fr-pagination li.active span")
            if current_li:
                visited_pages.add(current_li.text.strip())

            next_a = next((a for a in soup.select("ul.fr-pagination li a.selectFilterPage")
                           if a.text.strip() not in visited_pages and int(a.text.strip()) >= start_page), None)

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

        return all_rows, True, len(visited_pages), pages_total, expected_modifications
    except Exception as e:
        log(f"[SELENIUM ERROR] Прокси {proxy} — {e}")
        return [], False, 0, 0, None
    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass

def selenium_phase(item):
    original_url = item["modification_url"]
    pages_loaded = item.get("pages_loaded", 1)
    pages_total = item.get("pages_total", 1)
    existing_rows = item.get("modification_table", [])
    table_found = item.get("table_found", True)

    expected_modifications = None

    proxy_list_all = [item.get("proxy")] if "proxy" in item else []
    proxy_list_all += [p for p in good_proxies + load_proxies() if p not in proxy_list_all]

    for mirror in MIRRORS:
        url = with_mirror(original_url, mirror)
        mirror_limited = False

        for proxy in proxy_list_all:
            if proxy in used_proxies:
                continue
            used_proxies.add(proxy)

            log(f"[SELENIUM] {item['brand']} | {item['model']} — зеркало: {mirror}, прокси: {proxy}")
            rows, success, page_count, real_pages_total, expected_modifications = parse_with_selenium(url, proxy, start_page=pages_loaded + 1)

            if not success:
                log(f"[PROXY FAIL] Ошибка при подключении через {proxy}, пробуем следующий прокси.")
                continue  # ❗ Пробуем другой прокси, не выходим

            if rows is not None and len(rows) == 0:
                log(f"[EMPTY TABLE] {mirror} | {item['brand']} {item['model']} — Selenium получил 0 строк, переходим к другому зеркалу.")
                mirror_limited = True
                break

            if rows:
                total_rows = existing_rows + rows
                # Удаляем дубликаты по URL
                seen = set()
                total_rows = [r for r in total_rows if (r['url'] not in seen and not seen.add(r['url']))]
                modifications_received = len(total_rows)
                all_loaded = (
                    (page_count + pages_loaded) >= real_pages_total or
                    (expected_modifications is not None and modifications_received >= expected_modifications)
                )
                # Унифицируем URL
                item["modification_url"] = with_mirror(original_url, "https://zapo.ru")

                save_temp_file(item, total_rows, all_pages_loaded=all_loaded,
                               pages_loaded=page_count + pages_loaded,
                               pages_total=real_pages_total,
                               table_found=table_found,
                               modifications_expected=expected_modifications)
                log(f"[DEDUP] Удалено {len(existing_rows) + len(rows) - len(total_rows)} дублей.")
                log(f"[FINAL] {item['brand']} | {item['model']} — {modifications_received} строк | pages={page_count} | success={success}")
                return

            elif expected_modifications is None and page_count == 0:
                # Проверим лимит
                try:
                    driver = setup_driver(proxy)
                    driver.get(url)
                    html = driver.page_source
                    
                    if is_access_denied(html):
                        log(f"[ACCESS DENIED] {mirror} | {item['brand']} {item['model']} — Selenium получил страницу отказа.")
                        continue
                    
                    driver.quit()
                    if is_rate_limited(html):
                        log(f"[LIMIT] {mirror} | {item['brand']} {item['model']} — лимит по зеркалу в Selenium.")
                        mirror_limited = True
                        break
                except Exception as e:
                    log(f"[Selenium Check Error] {proxy} — {e}")

        if mirror_limited:
            continue  # переходим к следующему зеркалу

    failed_items.append(item)
    log(f"[FAILED SELENIUM] {item['brand']} | {item['model']} — все зеркала/прокси не сработали")

def main():
    all_tasks = []

    # Загрузка входных данных
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

    # Подгружаем ранее неудачные попытки
    if os.path.exists(FAILED_FILE):
        with open(FAILED_FILE, "r", encoding="utf-8") as f:
            failed_previous = json.load(f)
            all_tasks.extend(failed_previous)
            log(f"🔁 Повторное добавление {len(failed_previous)} моделей из {FAILED_FILE}")

    log(f"🔍 Всего моделей для обработки: {len(all_tasks)}")

    # 🔹 Фаза 1 — requests
    # with ThreadPoolExecutor(max_workers=THREADS_REQUESTS) as executor:
    #     list(tqdm(executor.map(prepare_requests_phase, all_tasks), total=len(all_tasks), desc="🌐 Requests-парсинг"))

    # 🔹 Фаза 2 — читаем TMP-файлы, обрабатываем не завершённые
    requests_phase_results.clear()
    for fname in os.listdir(TMP_DIR):
        try:
            with open(os.path.join(TMP_DIR, fname), "r", encoding="utf-8") as f:
                model_data = json.load(f)
            if not model_data.get("all_pages_loaded", False) and model_data.get("table_found", True):
                requests_phase_results.append(model_data)
        except Exception as e:
            log(f"[ERROR] Не удалось загрузить {fname}: {e}")
    log(f"🧠 Передано в Selenium-фазу: {len(requests_phase_results)} моделей")

    # 🔹 Фаза 3 — Selenium
    with ThreadPoolExecutor(max_workers=THREADS_SELENIUM) as executor:
        list(tqdm(executor.map(selenium_phase, requests_phase_results), total=len(requests_phase_results), desc="🧠 Selenium-парсинг"))

    # 🔹 Фаза 4 — сбор всех результатов
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
    else:
        if os.path.exists(FAILED_FILE):
            os.remove(FAILED_FILE)
        log("✅ Все модели успешно обработаны.")

if __name__ == "__main__":
    main()