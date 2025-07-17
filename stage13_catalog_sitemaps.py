import os
import re
import json
import gzip
from lxml import etree as ET
import time
from urllib.parse import quote, urlencode
from itertools import product, combinations
from collections import defaultdict
from datetime import datetime
from typing import List, Dict, Any
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
from tqdm import tqdm
from utils import load_proxies, fetch_with_proxies, MIRRORS, with_mirror

GROUPS_FILE = "groups.json"
TEMP_DIR = "stage13_temp_results"
FILTERS_DIR = os.path.join(TEMP_DIR, "filters_json")
OUTPUT_DIR = os.path.join(TEMP_DIR, "sitemaps_output")
ALL_FILTERS_JSON = os.path.join(TEMP_DIR, "all_filters.json")
DONE_GROUPS_FILE = os.path.join(TEMP_DIR, "done_groups.json")

THREADS = 4
LINK_VALIDATION_THREADS = 50

DEFAULT_FILTER_LIMIT = 7
MAX_DEPTH = 10
MAX_DYNAMIC_LINKS = 2_000_000

PROXY_FILE = "proxies_cleaned.txt"
PROXY_ALIVE_FILE = "proxies_alive.txt"
BASE_URL = "https://zapo.ru"
MAX_URLS = 50_000
MAX_XML_SIZE = 8 * 1024 * 1024
REQUEST_TIMEOUT = 10
RETRIES = 25
HEADERS = {"User-Agent": "Mozilla/5.0"}
USE_DYNAMIC_FILTERS = True
FALLBACK_TO_STATIC_FILTERS = True
VALIDATE_LINKS = False

os.makedirs(TEMP_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(FILTERS_DIR, exist_ok=True)

proxies = load_proxies(PROXY_FILE, PROXY_ALIVE_FILE, logger=print)
working_proxies: List[str] = []

def reload_proxies():
    return load_proxies(PROXY_FILE, PROXY_ALIVE_FILE, check_alive=True, logger=print)

def download_and_save_html(group_id: str) -> str:
    url = f"{BASE_URL}/{group_id}_catalog"
    html_path = os.path.join(TEMP_DIR, f"{group_id}.html")

    for attempt in range(1, RETRIES + 1):
        if os.path.exists(html_path):
            with open(html_path, "r", encoding="utf-8") as f:
                html = f.read()
                if "<form" in html:
                    return html
            os.remove(html_path)

        print(f"[{group_id}] Попытка загрузки #{attempt}")

        try:
            html, _ = fetch_with_proxies(
                url, proxies, working_proxies,
                headers=HEADERS,
                retries=1,
                timeout=REQUEST_TIMEOUT,
                logger=print,
                reload_proxies=reload_proxies,
            )
            if html and "<form" in html:
                with open(html_path, "w", encoding="utf-8") as f:
                    f.write(html)
                return html
        except Exception as e:
            print(f"[{group_id}] ❌ Ошибка загрузки: {e}")

    raise RuntimeError(f"❌ Пропуск {group_id}: Не удалось загрузить HTML после {RETRIES} попыток")

def is_valid_catalog_url_with_mirrors(url: str) -> bool:
    for mirror in MIRRORS:
        test_url = with_mirror(url, mirror)
        html, _ = fetch_with_proxies(
            test_url, proxies, working_proxies,
            headers=HEADERS,
            retries=3,
            timeout=REQUEST_TIMEOUT,
            logger=print,
            reload_proxies=reload_proxies,
        )
        if not html:
            continue
        soup = BeautifulSoup(html, "lxml")
        warning_div = soup.select_one("div.fr-alert.fr-alert-warning")
        if warning_div and "Товаров с указанными параметрами не найдено" in warning_div.text:
            continue
        return True
    return False

def validate_links_parallel(urls: List[str], max_workers: int = LINK_VALIDATION_THREADS) -> List[str]:
    if not urls:
        return []    
    valid = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(is_valid_catalog_url_with_mirrors, url): url for url in urls}
        for future in tqdm(as_completed(futures), total=len(futures), desc="🔍 Проверка ссылок"):
            url = futures[future]
            try:
                if future.result():
                    valid.append(url)
            except Exception as e:
                print(f"⚠️ Ошибка при проверке {url}: {e}")
    return valid

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


def generate_links_progressively(
    filters: Dict[str, List[str]],
    keys: List[str],
    group_id: str,
) -> List[str]:
    """Сгенерировать ссылки, перебирая сохранённые значения фильтров."""
    seen = set()
    links: List[str] = []
    for r in range(1, min(len(keys), MAX_DEPTH) + 1):
        for key_combo in combinations(keys, r):
            values = [filters.get(k, []) for k in key_combo]
            for combo in product(*values):
                url = (
                    f"{BASE_URL}/{group_id}_catalog?goods_group={group_id}"
                    f"&action=search&viewMode=tile&resultMode=5&hidePriceIn=1"
                )
                for k, v in zip(key_combo, combo):
                    url += f"&property[{k}][]={quote(v)}"
                if url not in seen:
                    seen.add(url)
                    links.append(url)
    return links

@lru_cache(maxsize=100_000)
def _fetch_dynamic_filters_cached(
    group_id: str,
    selected_tuple: tuple,
    exclude: str,
) -> Dict[str, Any]:
    from urllib.parse import urlencode

    params = {
        "goods_group": group_id,
        "action": "goods_catalog/goods_catalog/getFilters",
        "viewMode": "tile",
        "excluded": exclude,
    }

    for k, v in dict(selected_tuple).items():
        params.setdefault(f"property[{k}][]", []).append(v)

    # 👉 Сформировать полный URL с query-параметрами
    base_url = f"{BASE_URL}/{group_id}_catalog"
    full_url = f"{base_url}?{urlencode(params, doseq=True)}"

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": base_url,
    }

    html, _ = fetch_with_proxies(
        full_url,
        proxies,
        working_proxies,
        headers=headers,
        retries=RETRIES,
        timeout=REQUEST_TIMEOUT,
        logger=print,
        reload_proxies=reload_proxies,
    )

    if not html:
        print(f"⚠️ Пустой ответ на fetchFilters для {group_id} с {selected_tuple=}, {exclude=}")
        return {}

    try:
        return json.loads(html)
    except Exception as e:
        print(f"❌ Ошибка парсинга JSON для {group_id}: {e}")
        print(f"↩️ Ответ: {html[:200]}...")
        return {}

def fetch_dynamic_filters(
    group_id: str,
    selected: Dict[str, str],
    exclude: str,
) -> Dict[str, Any]:
    """Обёртка для кешированной функции."""
    selected_tuple = tuple(sorted(selected.items()))
    return _fetch_dynamic_filters_cached(group_id, selected_tuple, exclude)


def generate_links_dynamic(
    group_id: str,
    keys: List[str],
    max_depth: int = MAX_DEPTH,
) -> List[str]:
    """Рекурсивная генерация ссылок, используя API getFilters с контролем глубины и количества."""

    links: List[str] = []
    seen: set[str] = set()

    def build(selected: Dict[str, str], depth: int = 0):
        nonlocal links

        if selected:
            url = (
                f"{BASE_URL}/{group_id}_catalog?goods_group={group_id}"
                f"&action=search&viewMode=tile&resultMode=5&hidePriceIn=1"
            )
            for k, v in selected.items():
                url += f"&property[{k}][]={quote(v)}"

            if url not in seen:
                seen.add(url)
                links.append(url)
                if len(links) % 1000 == 0:
                    print(f"🔗 [{group_id}] Сгенерировано {len(links)} ссылок...")

            if len(links) >= MAX_DYNAMIC_LINKS:
                return

        if depth >= len(keys) or depth >= max_depth:
            return

        key = keys[depth]
        
        start = time.time()
        data = fetch_dynamic_filters(group_id, selected, key)
        elapsed = time.time() - start
        if elapsed > 5:
            print(f"🐢 [{group_id}] {key} — запрос длился {elapsed:.1f} сек.")
            
        values = data.get(key, [])

        if not values:
            return

        for val in values:
            selected[key] = val
            build(selected, depth + 1)
            del selected[key]

            if len(links) >= MAX_DYNAMIC_LINKS:
                return
    build({})
    return links

def generate_links(
    group_id: str,
    filters: Dict[str, List[str]],
    keys: List[str],
    max_depth: int = MAX_DEPTH,
) -> List[str]:
    """Выбрать способ генерации ссылок в зависимости от настроек."""
    if USE_DYNAMIC_FILTERS:
        urls = generate_links_dynamic(group_id, keys, max_depth)
        if urls or not FALLBACK_TO_STATIC_FILTERS:
            return urls
    return generate_links_progressively(filters, keys, group_id)

def save_sitemaps(urls: List[str], group_id: str) -> List[str]:
    now = datetime.now().isoformat(timespec="seconds") + "+03:00"
    xml_paths, chunk, size, index = [], [], 0, 1

    def write_xml_chunk(part_urls: List[str], part_index: int) -> str:
        urlset = ET.Element("urlset", nsmap={None: "http://www.sitemaps.org/schemas/sitemap/0.9"})
        for u in part_urls:
            url_el = ET.SubElement(urlset, "url")
            ET.SubElement(url_el, "loc").text = u
            ET.SubElement(url_el, "lastmod").text = now
            ET.SubElement(url_el, "changefreq").text = "weekly"
        path = os.path.join(OUTPUT_DIR, f"sitemap_{group_id}_{part_index}.xml")
        tree = ET.ElementTree(urlset)
        tree.write(path, encoding="utf-8", pretty_print=False, xml_declaration=True)
        return path

    def gzip_file(path: str) -> str:
        gz_path = path + ".gz"
        with open(path, "rb") as f_in, gzip.open(gz_path, "wb") as f_out:
            f_out.writelines(f_in)
        os.remove(path)
        print(f"📦 Архивирован: {gz_path}")
        return gz_path

    for url in urls:
        size += len(url.encode()) + 100
        chunk.append(url)
        if len(chunk) >= MAX_URLS or size >= MAX_XML_SIZE:
            xml_paths.append(write_xml_chunk(chunk, index))
            chunk, size = [], 0
            index += 1

    if chunk:
        xml_paths.append(write_xml_chunk(chunk, index))

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        gz_files = list(executor.map(gzip_file, xml_paths))

    return gz_files

def remove_old_sitemaps(group_id: str):
    for f in os.listdir(OUTPUT_DIR):
        if f.startswith(f"sitemap_{group_id}_") and f.endswith(".xml.gz"):
            try:
                os.remove(os.path.join(OUTPUT_DIR, f))
                print(f"🗑️ Удалён старый файл: {f}")
            except Exception as e:
                print(f"⚠️ Не удалось удалить {f}: {e}")

def process_group(
    group: Dict[str, Any],
    validate_links: bool = VALIDATE_LINKS,
    remove_old: bool = False,
) -> List[str]:
    gid = group["id"]
    print(f"\n🚧 Обработка группы: {gid}")

    if gid in done_groups:
        print(f"⏭️ {gid} уже завершена ранее, пропуск...")
        return []

    if gid not in all_filters:
        print(f"⚠️ {gid} отсутствует в all_filters, пропуск...")
        return []

    if remove_old:
        remove_old_sitemaps(gid)

    try:
        filters = all_filters[gid]
        if not filters or all(len(v) == 0 for v in filters.values()):
            raise ValueError(f"Пустые или некорректные фильтры для группы {gid}")

        filter_limit = group.get("filter_limit", DEFAULT_FILTER_LIMIT)
        selected_keys = list(filters.keys())[:filter_limit]
        print(f"🔑 Выбраны фильтры: {selected_keys}")

        raw_urls = generate_links(gid, filters, selected_keys)
        if len(raw_urls) >= MAX_DYNAMIC_LINKS:
            print(f"⚠️ Превышен лимит ссылок ({MAX_DYNAMIC_LINKS}), остановка.")
            
        print(f"🔗 Сгенерировано {len(raw_urls)} ссылок")
        
        if not raw_urls:
            print(f"⚠️ {gid} — Нет сгенерированных ссылок, пропуск...")
            return []

        valid_urls = validate_links_parallel(raw_urls) if validate_links else raw_urls
        print(f"✅ Валидных ссылок: {len(valid_urls)}")

        if not valid_urls:
            print(f"❌ Нет валидных ссылок для {gid}, пропуск...")
            return []

        gz_files = save_sitemaps(valid_urls, gid)
        print(f"📤 Успешно сохранено {len(gz_files)} sitemap-файлов для {gid}")

        done_groups.add(gid)
        with open(DONE_GROUPS_FILE + ".tmp", "w", encoding="utf-8") as f:
            json.dump(sorted(done_groups), f, indent=2, ensure_ascii=False)
        os.replace(DONE_GROUPS_FILE + ".tmp", DONE_GROUPS_FILE)

        return gz_files

    except Exception as e:
        print(f"❌ Ошибка в группе {gid}: {e}")
        return []

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

def main():
    global all_filters, done_groups    
    
    with open(GROUPS_FILE, "r", encoding="utf-8") as f:
        groups = json.load(f)

    if os.getenv("TEST_GROUP"):
        groups = [g for g in groups if g["id"] == os.getenv("TEST_GROUP")]

    all_filters = {}
    all_gz = []
    done_groups = set()

    if os.path.exists(DONE_GROUPS_FILE):
        with open(DONE_GROUPS_FILE, "r", encoding="utf-8") as f:
            done_groups = set(json.load(f))

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

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = {
            executor.submit(process_group, g, validate_links=VALIDATE_LINKS, remove_old=False): g["id"]
            for g in groups
        }
        for future in as_completed(futures):
            all_gz.extend(future.result())

    generate_index(all_gz)
    print("🏁 Sitemap генерация завершена.")

if __name__ == "__main__":
    main()
