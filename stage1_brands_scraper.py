import os
import requests
from bs4 import BeautifulSoup
import json
from tqdm import tqdm
from utils import load_proxies, fetch_with_proxies, MIRRORS, with_mirror

LOCAL_HTML = "base.html"
REMOTE_URL = "https://zapo.ru/brandslist"
OUTPUT_JSON = "brands.json"
PROXY_FILE = "proxies_cleaned.txt"
PROXY_ALIVE_FILE = "proxies_alive.txt"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
proxies = load_proxies(PROXY_FILE, PROXY_ALIVE_FILE)
working_proxies: list[str] = []

def fetch_html_from_site():
    print("🌐 Файл не найден — пробуем загрузить с сайта...")
    for mirror in MIRRORS:
        url = with_mirror(REMOTE_URL, mirror)
        html, _ = fetch_with_proxies(
            url, proxies, working_proxies, headers=HEADERS, retries=3, logger=print
        )
        if html:
            return html
    print("❌ Ошибка загрузки с сайта через все зеркала")
    return None

def clean_url(url_part):
    # Удаляем пробелы, неразрывные пробелы и лишние символы
    return url_part.replace('\xa0', '').replace('\u200b', '').strip()

def parse_html(html):
    soup = BeautifulSoup(html, "html.parser")
    brand_links = soup.select("li.inline > a[href]")

    brands = []
    for a in tqdm(brand_links, desc="📦 Сбор брендов"):
        name = a.get_text(strip=True)
        href = clean_url(a["href"])
        full_url = f"https://zapo.ru{href}"
        brands.append({
            "name": name,
            "brand_page": full_url
        })
    return brands

def main():
    if os.path.exists(LOCAL_HTML):
        print("📄 Найден HTML-файл — парсим локально...")
        with open(LOCAL_HTML, "r", encoding="utf-8") as f:
            html = f.read()
    else:
        html = fetch_html_from_site()
        if not html:
            print("❌ Не удалось получить HTML")
            return

    brands = parse_html(html)

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(brands, f, ensure_ascii=False, indent=2)

    print(f"✅ Успешно сохранено брендов: {len(brands)} → {OUTPUT_JSON}")

if __name__ == "__main__":
    main()
