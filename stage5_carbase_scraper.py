import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import json
from tqdm import tqdm
import os
from datetime import datetime
import re
from utils import load_proxies, fetch_with_proxies, MIRRORS, with_mirror

BASE_URL = "https://zapo.ru"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}
PROXY_FILE = "proxies_cleaned.txt"
PROXY_ALIVE_FILE = "proxies_alive.txt"
proxies = load_proxies(PROXY_FILE, PROXY_ALIVE_FILE)
working_proxies: list[str] = []
OUTPUT_FILE = "stage5_carbase.json"
LOG_DIR = "zapo_logs"
os.makedirs(LOG_DIR, exist_ok=True)
log_file_path = os.path.join(LOG_DIR, f"carbase_log_{datetime.now():%Y%m%d_%H%M%S}.txt")


def log(message: str):
    print(message)
    with open(log_file_path, "a", encoding="utf-8") as f:
        f.write(message + "\n")


def get_brands():
    for mirror in MIRRORS:
        url = with_mirror(f"{BASE_URL}/carbase", mirror)
        html, _ = fetch_with_proxies(
            url, proxies, working_proxies, headers=HEADERS, retries=3, logger=log
        )
        if html:
            soup = BeautifulSoup(html, 'html.parser')
            break
    else:
        raise RuntimeError("Failed to load brands from all mirrors")
    brand_divs = soup.select("div.carbase3Brands__brand a")

    brands = []
    for a in brand_divs:
        name = a.text.strip()
        link = urljoin(BASE_URL, a['href'])
        if link:
            link = with_mirror(link, "https://zapo.ru")
        brands.append((name, link))
    return brands


def get_models_and_versions(brand_name, brand_url):
    html, _ = fetch_with_proxies(
        brand_url, proxies, working_proxies, headers=HEADERS, retries=3, logger=log
    )
    if not html:
        log(f"[ERROR] {brand_name}: unable to load {brand_url}")
        return []
    soup = BeautifulSoup(html, 'html.parser')

    model_groups = {
        group['href'].replace("#group_", ""): group.get_text(strip=True)
        for group in soup.select(".carbase3Models__groups__name[href^='#group_']")
    }

    result = []
    wrappers = soup.select(".carbase3Models__models__tiles__wrapper")
    for wrapper in wrappers:
        model_id = wrapper.get("id", "").replace("group_", "")
        model_name = model_groups.get(model_id, "Неизвестная модель")

        tiles = wrapper.select(".carbase3Models__tile")
        for tile in tiles:
            version_name = tile.select_one(".carbase3Models__tile__name")
            date_range = tile.select_one(".carbase3Models__tile__text span:nth-of-type(2)")
            img_tag = tile.select_one("img")

            version = version_name.text.strip() if version_name else ""
            dates = re.sub(r"\s+", " ", date_range.text.strip()) if date_range else ""
            image = urljoin("https:", img_tag['src']) if img_tag and img_tag.get('src') else ""
            version_url = urljoin(BASE_URL, tile['href']) if tile and tile.has_attr('href') else ""
            if version_url:
                version_url = with_mirror(version_url, "https://zapo.ru")

            result.append({
                "brand": brand_name,
                "brand_url": brand_url,
                "model": model_name,
                "version": version,
                "dates": dates,
                "image": image,
                "version_url": version_url
            })
    return result


def main():
    all_data = []
    total_versions = 0

    brands = get_brands()
    log(f"🔍 Найдено брендов: {len(brands)}")

    for brand_name, brand_url in tqdm(brands, desc="📥 Обработка брендов"):
        try:
            models = get_models_and_versions(brand_name, brand_url)
            total_versions += len(models)
            log(f"[✔]  {brand_name:<20} {'.' * (30 - len(brand_name))} {len(models)} версий")
            all_data.extend(models)
        except Exception as e:
            log(f"[ERROR] {brand_name}: {e}")

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)

    model_count = len(set(f"{x['brand']}|{x['model']}" for x in all_data))
    brand_count = len(set(x['brand'] for x in all_data))

    log(f"📦 Всего брендов: {brand_count}")
    log(f"📦 Всего моделей: {model_count}")
    log(f"📦 Всего версий: {total_versions}")
    log(f"✅ Сохранено в {OUTPUT_FILE}")
    log(f"📝 Лог файл: {log_file_path}")


if __name__ == "__main__":
    main()
