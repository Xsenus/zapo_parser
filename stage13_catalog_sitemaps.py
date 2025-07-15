import os
import re
import json
import gzip
import xml.etree.ElementTree as ET
from urllib.parse import quote
from itertools import product
from collections import defaultdict
from datetime import datetime
from typing import List, Dict, Any
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

from utils import load_proxies, fetch_with_proxies

# ---------- Константы ----------
GROUPS_FILE = "groups.json"
TEMP_DIR = "stage13_temp_results"
FILTERS_DIR = os.path.join(TEMP_DIR, "filters_json")
OUTPUT_DIR = os.path.join(TEMP_DIR, "sitemaps_output")
ALL_FILTERS_JSON = os.path.join(TEMP_DIR, "all_filters.json")
PROXY_FILE = "proxies_cleaned.txt"
PROXY_ALIVE_FILE = "proxies_alive.txt"
BASE_URL = "https://zapo.ru"
MAX_URLS = 50000
MAX_XML_SIZE = 8 * 1024 * 1024
REQUEST_TIMEOUT = 15
RETRIES = 25
HEADERS = {"User-Agent": "Mozilla/5.0"}

DEFAULT_FILTER_LIMIT = 5
THREADS = 5

os.makedirs(TEMP_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(FILTERS_DIR, exist_ok=True)

# ---------- Прокси ----------
proxies = load_proxies(PROXY_FILE, PROXY_ALIVE_FILE, logger=print)
working_proxies: List[str] = []

def reload_proxies():
    return load_proxies(PROXY_FILE, PROXY_ALIVE_FILE, check_alive=True, logger=print)

# ---------- Загрузка HTML ----------
def download_and_save_html(group_id: str) -> str:
    url = f"{BASE_URL}/{group_id}_catalog"
    html_path = os.path.join(TEMP_DIR, f"{group_id}.html")

    for attempt in range(1, RETRIES + 1):
        print(f"[{group_id}] Попытка загрузки #{attempt}")

        html, _ = fetch_with_proxies(
            url, proxies, working_proxies,
            headers=HEADERS, retries=RETRIES,
            timeout=REQUEST_TIMEOUT,
            logger=print,
            reload_proxies=reload_proxies,
        )

        if html and "<form" in html:
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(html)
            return html

    raise RuntimeError(f"❌ Пропуск {group_id}: Не удалось загрузить HTML")

# ---------- Парсинг фильтров ----------
def parse_filters(html: str) -> Dict[str, List[str]]:
    soup = BeautifulSoup(html, "lxml")
    form = soup.find("form", id="catalog-form")
    if not form:
        raise ValueError("Форма с id='catalog-form' не найдена")
    checkboxes = form.find_all("input", {"type": "checkbox", "name": True})
    filters = defaultdict(list)
    for cb in checkboxes:
        name, value = cb.get("name"), cb.get("value")
        m = re.search(r"property\[(.+?)\]", name or "")
        if m and value:
            filters[m.group(1)].append(value)
    return dict(filters)

# ---------- Загрузка или парсинг фильтров ----------
def load_or_parse_filters(group_id: str) -> Dict[str, List[str]]:
    json_path = os.path.join(FILTERS_DIR, f"{group_id}.json")

    for attempt in range(1, RETRIES + 1):
        if os.path.exists(json_path):
            with open(json_path, "r", encoding="utf-8") as f:
                return json.load(f)

        print(f"[{group_id}] Парсинг попытка #{attempt}")
        try:
            html = download_and_save_html(group_id)
            filters = parse_filters(html)
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(filters, f, indent=2, ensure_ascii=False)
            return filters
        except Exception as e:
            print(f"❌ Ошибка при парсинге фильтров для {group_id}: {e}")
            continue

    raise RuntimeError(f"❌ Пропуск {group_id}: Не удалось получить фильтры")

# ---------- Генерация ссылок ----------
def generate_links(filters: Dict[str, List[str]], keys: List[str], group_id: str) -> List[str]:
    values = [filters.get(k, []) for k in keys]
    links = []
    for combo in product(*values):
        url = f"{BASE_URL}/{group_id}_catalog?goods_group={group_id}&action=search&viewMode=tile&resultMode=5&hidePriceIn=1"
        for k, v in zip(keys, combo):
            url += f"&property[{k}][]={quote(v)}"
        links.append(url)
    return links

# ---------- Сохранение sitemap-файлов ----------
def save_sitemaps(urls: List[str], group_id: str) -> List[str]:
    now = datetime.now().isoformat(timespec="seconds") + "+03:00"
    files, chunk, size, index = [], [], 0, 1

    def write_chunk(part_urls: List[str], part_index: int) -> str:
        urlset = ET.Element("urlset", xmlns="http://www.sitemaps.org/schemas/sitemap/0.9")
        for u in part_urls:
            url_el = ET.SubElement(urlset, "url")
            ET.SubElement(url_el, "loc").text = u
            ET.SubElement(url_el, "lastmod").text = now
            ET.SubElement(url_el, "changefreq").text = "weekly"
        path = os.path.join(OUTPUT_DIR, f"sitemap_{group_id}_{part_index}.xml")
        ET.ElementTree(urlset).write(path, encoding="utf-8", xml_declaration=True)
        gz_path = path + ".gz"
        with open(path, "rb") as f_in, gzip.open(gz_path, "wb") as f_out:
            f_out.writelines(f_in)
        os.remove(path)
        return gz_path

    for url in urls:
        size += len(url.encode()) + 100
        chunk.append(url)
        if len(chunk) >= MAX_URLS or size >= MAX_XML_SIZE:
            files.append(write_chunk(chunk, index))
            chunk, size = [], 0
            index += 1

    if chunk:
        files.append(write_chunk(chunk, index))
    return files

# ---------- Индексный sitemap ----------
def generate_index(gz_files: List[str]):
    now = datetime.now().isoformat(timespec="seconds") + "+03:00"
    root = ET.Element("sitemapindex", xmlns="http://www.sitemaps.org/schemas/sitemap/0.9")
    for f in gz_files:
        sm = ET.SubElement(root, "sitemap")
        ET.SubElement(sm, "loc").text = f"{BASE_URL}/{os.path.basename(f)}"
        ET.SubElement(sm, "lastmod").text = now
    ET.ElementTree(root).write(
        os.path.join(OUTPUT_DIR, "sitemap_catalog_index.xml"),
        encoding="utf-8",
        xml_declaration=True
    )

# ---------- Главная функция ----------
def main():
    with open(GROUPS_FILE, "r", encoding="utf-8") as f:
        groups = json.load(f)

    all_filters = {}
    all_gz = []

    def fetch_and_cache(group: Dict[str, Any]) -> tuple[str, Dict[str, List[str]] | None]:
        gid = group["id"]
        try:
            filters = load_or_parse_filters(gid)
            return gid, filters
        except Exception as e:
            print(f"❌ Пропуск {gid}: {e}")
            return gid, None

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = {executor.submit(fetch_and_cache, g): g["id"] for g in groups}
        for future in as_completed(futures):
            gid, filters = future.result()
            if filters:
                all_filters[gid] = filters

    with open(ALL_FILTERS_JSON, "w", encoding="utf-8") as f:
        json.dump(all_filters, f, indent=2, ensure_ascii=False)

    def process_group(group: Dict[str, Any]) -> List[str]:
        gid = group["id"]
        if gid not in all_filters:
            return []
        expected_file = os.path.join(OUTPUT_DIR, f"sitemap_{gid}_1.xml.gz")
        if os.path.exists(expected_file):
            print(f"⏭️ {gid} уже обработан, пропуск...")
            return []
        try:
            filters = all_filters[gid]
            filter_limit = group.get("filter_limit", DEFAULT_FILTER_LIMIT)
            selected_keys = list(filters.keys())[:filter_limit] if filter_limit else list(filters.keys())
            urls = generate_links(filters, selected_keys, gid)
            print(f"✅ {gid}: {len(urls)} ссылок по фильтрам {selected_keys}")
            return save_sitemaps(urls, gid)
        except Exception as e:
            print(f"❌ Ошибка в группе {gid}: {e}")
            return []

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = {executor.submit(process_group, g): g["id"] for g in groups}
        for future in as_completed(futures):
            all_gz.extend(future.result())

    generate_index(all_gz)
    print("🏁 Sitemap генерация завершена.")

if __name__ == "__main__":
    main()
