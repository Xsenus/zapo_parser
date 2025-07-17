import os
from lxml import etree as ET
from glob import glob
from time import time

# --- Константы ---
INPUT_DIR = "stage13_temp_results/sitemaps_output"
OUTPUT_DIR = "stage13_temp_results/catalog_combined"
MAX_URLS = 50000
MAX_FILESIZE = 8 * 1024 * 1024
CHECK_EVERY = 100  

os.makedirs(OUTPUT_DIR, exist_ok=True)
input_files = sorted(glob(os.path.join(INPUT_DIR, "*.xml")))

def get_xml_size(urls):
    root = ET.Element("urlset", xmlns="http://www.sitemaps.org/schemas/sitemap/0.9")
    for u in urls:
        root.append(u)
    return len(ET.tostring(root, encoding="utf-8", xml_declaration=True))

def save_batch(batch, part):
    root = ET.Element("urlset", xmlns="http://www.sitemaps.org/schemas/sitemap/0.9")
    for e in batch:
        root.append(e)
    tree = ET.ElementTree(root)
    filename = os.path.join(OUTPUT_DIR, f"sitemap_catalog_{part}.xml")
    tree.write(filename, encoding="utf-8", xml_declaration=True, pretty_print=True)
    print(f"✅ Сохранено: {filename} ({len(batch)} ссылок)")
    return part + 1

part = 1
batch = []
size_cache = 0

start_time = time()

for path in input_files:
    print(f"📄 Обработка файла: {os.path.basename(path)}")
    try:
        tree = ET.parse(path)
        root = tree.getroot()
        urls = root.findall("{http://www.sitemaps.org/schemas/sitemap/0.9}url")

        for idx, elem in enumerate(urls):
            batch.append(elem)

            if len(batch) % CHECK_EVERY == 0:
                size_cache = get_xml_size(batch)

            if len(batch) >= MAX_URLS or size_cache >= MAX_FILESIZE:
                print(f"  🗂 Партия достигла лимита: {len(batch)} URL, {size_cache} байт")
                part = save_batch(batch, part)
                batch = []
                size_cache = 0

    except Exception as e:
        print(f"❌ Ошибка при обработке {path}: {e}")

if batch:
    print(f"🧾 Сохраняем финальную партию: {len(batch)} ссылок")
    part = save_batch(batch, part)

print(f"🏁 Завершено за {round(time() - start_time, 2)} сек.")