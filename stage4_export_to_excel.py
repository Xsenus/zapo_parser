import json
import pandas as pd
import re
from tqdm import tqdm

CONTACTS_FILE = 'stage3_contacts_processed.json'
BRANDS_FILE = 'brands.json'
OUTPUT_FILE_RU = 'contacts_ru.xlsx'
OUTPUT_FILE_NON_RU = 'contacts_non_ru.xlsx'
MAX_EMAILS = 2
MAX_PHONES = 5

def load_json(path):
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

def normalize_phone(phone):
    digits = re.sub(r'\D', '', phone)
    if digits.startswith('8') and len(digits) == 11:
        digits = '7' + digits[1:]
    elif digits.startswith('00'):
        digits = digits[2:]
    elif digits.startswith('0') and len(digits) > 10:
        digits = '7' + digits[1:]
    return digits

def split_and_export():
    contacts = load_json(CONTACTS_FILE)
    brands = load_json(BRANDS_FILE)
    brand_map = {b["name"]: b.get("brand_page", "") for b in brands}

    ru_rows = []
    non_ru_rows = []

    print("🔄 Обработка записей...")
    for item in tqdm(contacts, desc="📦 Экспорт"):
        name = item['name']
        site = item['site']
        page = brand_map.get(name, '')

        emails = item.get("emails", [])
        phones = [normalize_phone(p) for p in item.get("phones", [])]

        if MAX_EMAILS >= 0:
            emails = emails[:MAX_EMAILS]
        if MAX_PHONES >= 0:
            phones = phones[:MAX_PHONES]

        row = {
            "Название": name,
            "Страница": page,
            "Сайт": site,
            "Email": emails[0] if emails else "",
        }

        for i, phone in enumerate(phones):
            row[f"Телефон {i+1}"] = phone

        if ".ru" in site.lower():
            ru_rows.append(row)
        else:
            non_ru_rows.append(row)

    print(f"✅ Сохранение в {OUTPUT_FILE_RU} ({len(ru_rows)} записей)")
    pd.DataFrame(ru_rows).to_excel(OUTPUT_FILE_RU, index=False)

    print(f"✅ Сохранение в {OUTPUT_FILE_NON_RU} ({len(non_ru_rows)} записей)")
    pd.DataFrame(non_ru_rows).to_excel(OUTPUT_FILE_NON_RU, index=False)

if __name__ == '__main__':
    split_and_export()
