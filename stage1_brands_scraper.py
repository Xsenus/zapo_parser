import os
import requests
from bs4 import BeautifulSoup
import json
from tqdm import tqdm

LOCAL_HTML = "base.html"
REMOTE_URL = "https://zapo.ru/brandslist"
OUTPUT_JSON = "brands.json"

def fetch_html_from_site():
    try:
        print("🌐 Файл не найден — пробуем загрузить с сайта...")
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        }
        response = requests.get(REMOTE_URL, headers=headers, timeout=20)
        response.raise_for_status()
        return response.text
    except Exception as ex:
        print(f"❌ Ошибка загрузки с сайта: {ex}")
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
        full_url = f"http://zapo.ru{href}"
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
