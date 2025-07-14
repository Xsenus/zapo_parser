import os
import re
import gzip
from itertools import product
from collections import defaultdict
from urllib.parse import quote
from datetime import datetime
import xml.etree.ElementTree as ET

import requests
from bs4 import BeautifulSoup

from utils import load_proxies, fetch_with_proxies

# ---------- Константы ----------
INPUT_HTML = "oils_catalog.txt"
OUTPUT_DIR = "sitemaps_output"
PROXY_FILE = "proxies_cleaned.txt"
PROXY_ALIVE_FILE = "proxies_alive.txt"
BASE_URL = "https://zapo.ru"
TARGET_URL = "https://zapo.ru/oils_catalog"
MAX_URLS = 50000
MAX_XML_SIZE = 8 * 1024 * 1024
REQUEST_TIMEOUT = 15
RETRIES = 10

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ---------- Загрузка прокси ----------
proxies = load_proxies(PROXY_FILE, PROXY_ALIVE_FILE, logger=print)
working_proxies: list[str] = []


def reload_proxies() -> list[str]:
    """Перезагрузка прокси с проверкой живости."""
    return load_proxies(
        PROXY_FILE,
        PROXY_ALIVE_FILE,
        logger=print,
        check_alive=True,
    )


def fetch_catalog_page() -> str:
    """Загрузить HTML-каталог с использованием прокси и перезагрузки."""
    html, _ = fetch_with_proxies(
        TARGET_URL,
        proxies,
        working_proxies,
        headers=HEADERS,
        retries=RETRIES,
        timeout=REQUEST_TIMEOUT,
        logger=print,
        reload_proxies=reload_proxies,
    )
    if not html or '<form' not in html:
        raise RuntimeError('❌ Не удалось загрузить каталог масел')
    return html


def load_or_fetch_html() -> str:
    """Загрузить HTML из файла или с сайта, если файл отсутствует или повреждён."""
    if os.path.exists(INPUT_HTML):
        with open(INPUT_HTML, 'r', encoding='utf-8') as f:
            html = f.read()
            if '<form' in html:
                return html
            print("[HTML] ⚠️ Повреждённый кэш, пробуем заново...")
            os.remove(INPUT_HTML)

    html = fetch_catalog_page()
    with open(INPUT_HTML, 'w', encoding='utf-8') as f:
        f.write(html)
    return html


def parse_filters(html: str) -> dict[str, list[str]]:
    """Распарсить доступные фильтры из формы каталога."""
    soup = BeautifulSoup(html, 'lxml')
    form = soup.find('form', id='catalog-form')
    if not form:
        raise ValueError("<form id='catalog-form'> не найдена")

    checkboxes = form.find_all('input', {'type': 'checkbox', 'name': True})
    filters: defaultdict[str, list[str]] = defaultdict(list)

    for cb in checkboxes:
        name = cb.get('name')
        value = cb.get('value')
        if not name or not value or not name.startswith('property['):
            continue
        m = re.search(r'property\[(.+?)\]', name)
        if m:
            key = m.group(1)
            if key in {'brands', 'viscosity', 'oil_type'}:
                filters[key].append(value)
    return filters


def generate_links(filters: dict[str, list[str]]) -> list[str]:
    """Сгенерировать все возможные комбинации ссылок по фильтрам."""
    links = []
    for brand, viscosity, oil_type in product(
        filters.get('brands', []),
        filters.get('viscosity', []),
        filters.get('oil_type', []),
    ):
        url = (
            f"{BASE_URL}/oils_catalog?goods_group=oils&action=search&viewMode=tile&resultMode=5&hidePriceIn=1"
            f"&property[brands][]={quote(brand)}"
            f"&property[viscosity][]={quote(viscosity)}"
            f"&property[oil_type][]={quote(oil_type)}"
            f"&property[liquid_volume][from]=&property[liquid_volume][to]="
        )
        links.append(url)
    return links


def save_sitemaps(urls: list[str]) -> list[str]:
    """Разбить ссылки на части и сохранить в .xml.gz файлы."""
    files: list[str] = []
    chunk: list[str] = []
    size_estimate = 0
    index = 1
    now = datetime.now().isoformat(timespec='seconds') + '+03:00'

    def write_chunk(chunk_urls: list[str], chunk_index: int) -> str:
        urlset = ET.Element('urlset', xmlns='http://www.sitemaps.org/schemas/sitemap/0.9')
        for url in chunk_urls:
            url_el = ET.SubElement(urlset, 'url')
            ET.SubElement(url_el, 'loc').text = url
            ET.SubElement(url_el, 'lastmod').text = now
            ET.SubElement(url_el, 'changefreq').text = 'weekly'
        xml_path = os.path.join(OUTPUT_DIR, f'sitemap_catalog_{chunk_index}.xml')
        ET.ElementTree(urlset).write(xml_path, encoding='utf-8', xml_declaration=True)
        with open(xml_path, 'rb') as f_in, gzip.open(xml_path + '.gz', 'wb') as f_out:
            f_out.writelines(f_in)
        os.remove(xml_path)
        return xml_path + '.gz'

    for url in urls:
        entry = f"<url><loc>{url}</loc><lastmod>{now}</lastmod><changefreq>weekly</changefreq></url>"
        size_estimate += len(entry.encode('utf-8'))
        chunk.append(url)
        if len(chunk) >= MAX_URLS or size_estimate >= MAX_XML_SIZE:
            gz_file = write_chunk(chunk, index)
            files.append(gz_file)
            index += 1
            chunk = []
            size_estimate = 0

    if chunk:
        gz_file = write_chunk(chunk, index)
        files.append(gz_file)

    return files


def generate_index(gz_files: list[str]) -> None:
    """Сгенерировать индексный файл sitemap_catalog_index.xml"""
    index_root = ET.Element('sitemapindex', xmlns='http://www.sitemaps.org/schemas/sitemap/0.9')
    now = datetime.now().isoformat(timespec='seconds') + '+03:00'
    for f in gz_files:
        sitemap = ET.SubElement(index_root, 'sitemap')
        ET.SubElement(sitemap, 'loc').text = f"{BASE_URL}/{os.path.basename(f)}"
        ET.SubElement(sitemap, 'lastmod').text = now

    ET.ElementTree(index_root).write(
        os.path.join(OUTPUT_DIR, 'sitemap_catalog_index.xml'),
        encoding='utf-8',
        xml_declaration=True,
    )


def main() -> None:
    html = load_or_fetch_html()
    filters = parse_filters(html)
    print('🧪 Найденные фильтры:')
    for key in ['brands', 'viscosity', 'oil_type']:
        values = filters.get(key, [])
        preview = ", ".join(values[:5]) + ('...' if len(values) > 5 else '')
        print(f"  {key}: {len(values)} значений → {preview}")

    urls = generate_links(filters)
    print(f'✅ Сгенерировано ссылок: {len(urls)}')

    gz_files = save_sitemaps(urls)
    generate_index(gz_files)

    print('✅ Готово! Sitemap файлы находятся в:', OUTPUT_DIR)


if __name__ == '__main__':
    main()
