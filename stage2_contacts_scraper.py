import os
import json
import time
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import re
import idna
from urllib.parse import urlparse, urlunparse
from utils import load_proxies, fetch_with_proxies

INPUT_FILE = 'brands.json'
OUTPUT_FILE = 'stage2_sites.json'
ERROR_LOG = 'stage2_errors.log'
MAX_WORKERS = 10
SAVE_EVERY = 5
MAX_RETRIES = 25
BASE_URL = 'https://zapo.ru'
PROXY_FILE = 'proxies_cleaned.txt'
PROXY_ALIVE_FILE = 'proxies_alive.txt'
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
proxies = load_proxies(PROXY_FILE, PROXY_ALIVE_FILE)
working_proxies: list[str] = []


def load_json_file(filename):
    if os.path.exists(filename):
        with open(filename, 'r', encoding='utf-8') as f:
            return json.load(f)
    return []


def save_json_file(filename, data):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def log_error(message):
    with open(ERROR_LOG, 'a', encoding='utf-8') as f:
        f.write(message + '\n')


def clean_url(text: str) -> str:
    """Очищает URL и нормализует кириллические домены"""
    text = re.sub(r'[\s\u00a0]+', '', text.strip())

    if not text.startswith(('http://', 'https://')):
        text = 'http://' + text

    try:
        parsed = urlparse(text)
        netloc = idna.encode(parsed.netloc).decode('ascii')
        return urlunparse(parsed._replace(netloc=netloc))
    except Exception:
        return text  # fallback на случай ошибок


def extract_company_site(html: str) -> str | None:
    soup = BeautifulSoup(html, 'html.parser')

    # Вариант 1: основной — <div class="getBrandFullInfoContent"> + <b>Сайт:</b>
    for block in soup.select("div.getBrandFullInfoContent"):
        label = block.find('b')
        if label and "Сайт" in label.text:
            link = block.find('a', href=True)
            if link:
                return clean_url(link['href'])

    # Вариант 2: fallback — <div class="col-md-8"> + <b>Сайт:</b>
    for b_tag in soup.select("div.col-md-8 b"):
        if "Сайт" in b_tag.text:
            parent = b_tag.parent
            if parent:
                link = parent.find('a', href=True)
                if link:
                    return clean_url(link['href'])

    # Вариант 3: универсальный — ищем "Сайт" + <a> после
    for block in soup.select("div.getBrandFullInfoContent"):
        for tag in block.find_all(['b', 'strong']):
            if "Сайт" in tag.get_text(strip=True):
                next_sibling = tag
                while next_sibling:
                    next_sibling = next_sibling.next_sibling
                    if not next_sibling:
                        break
                    if isinstance(next_sibling, str):
                        continue
                    link = next_sibling.find("a") if not next_sibling.name == "a" else next_sibling
                    if link and link.has_attr("href"):
                        return clean_url(link['href'])

    # Вариант 4: текстовый парсинг — ищем строку "Сайт: example.com"
    text = soup.get_text(separator=" ", strip=True)
    match = re.search(
        r"Сайт[:\s]*((https?://)?(www\.)?[a-zA-Zа-яА-Я0-9\-.]+\.[a-zA-Zа-яА-Я]{2,})",
        text
    )
    if match:
        site = match.group(1)
        return clean_url(site)

    return None


def fetch_with_retries(brand, retries=MAX_RETRIES) -> dict | None:
    name = brand['name']
    brand_url = clean_url(brand['brand_page'])
    if not brand_url.startswith("http"):
        brand_url = BASE_URL + brand_url

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/114.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://zapo.ru/brandslist",
        "Connection": "keep-alive",
        "DNT": "1",
        "Upgrade-Insecure-Requests": "1",
    }

    backoff = 2  # стартовая задержка

    for attempt in range(1, retries + 1):
        try:
            html, _ = fetch_with_proxies(
                brand_url, proxies, working_proxies, headers=headers, retries=1
            )
            if not html:
                raise Exception("empty response")
            site = extract_company_site(html)
            return {
                'name': name,
                'brand_page': brand_url,
                'company_site': site
            }

        except Exception as e:
            if attempt == retries:
                log_error(f"{name} | {brand_url} | {str(e)}")
                return None
            else:
                wait_time = backoff ** attempt
                tqdm.write(f"⚠️ Ошибка для '{name}', попытка {attempt}/{retries}, ожидание {wait_time} сек...")
                time.sleep(wait_time)


def merge_results(existing: list, new: list) -> list:
    existing_map = {b['name']: b for b in existing}
    for brand in new:
        if not brand:
            continue
        name = brand['name']
        if name not in existing_map or not existing_map[name].get('company_site'):
            existing_map[name] = brand
    return list(existing_map.values())


def main():
    all_brands = load_json_file(INPUT_FILE)
    processed = load_json_file(OUTPUT_FILE)
    processed_map = {b['name']: b for b in processed}

    to_process = [
        brand for brand in all_brands
        if brand['name'] not in processed_map or not processed_map[brand['name']].get('company_site')
    ]

    print(f"🔄 К обработке: {len(to_process)} брендов (из {len(all_brands)})")

    results = []
    counter = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_with_retries, brand): brand for brand in to_process}

        for future in tqdm(as_completed(futures), total=len(futures), desc="🔍 Сбор сайтов"):
            result = future.result()
            if result:
                results.append(result)
                counter += 1
                if counter % SAVE_EVERY == 0:
                    merged = merge_results(processed, results)
                    save_json_file(OUTPUT_FILE, merged)

    merged = merge_results(processed, results)
    save_json_file(OUTPUT_FILE, merged)
    print(f"✅ Завершено. Всего сайтов собрано: {len(merged)}")


if __name__ == '__main__':
    main()
