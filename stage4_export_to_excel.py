import json
import pandas as pd
import re
from tqdm import tqdm
from datetime import datetime
import os

CONTACTS_FILE = 'stage3_contacts_processed.json'
BRANDS_FILE = 'brands.json'
LOG_FILE = 'stage4_export_log.txt'
MAX_EMAILS = -1
MAX_PHONES = -1

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

def prepare_rows(items, brand_map):
    rows = []
    max_emails = 0
    max_phones = 0

    for item in tqdm(items, desc="📦 Подготовка строк"):
        name = item['name']
        site = item['site']
        page = brand_map.get(name, '')

        emails = list(set(item.get("emails", [])))
        phones = list({normalize_phone(p) for p in item.get("phones", [])})

        if MAX_EMAILS >= 0:
            emails = emails[:MAX_EMAILS]
        if MAX_PHONES >= 0:
            phones = phones[:MAX_PHONES]

        max_emails = max(max_emails, len(emails))
        max_phones = max(max_phones, len(phones))

        rows.append({
            "name": name,
            "site": site,
            "page": page,
            "emails": emails,
            "phones": phones
        })

    return rows, max_emails, max_phones

def export_to_excel(rows, max_emails, max_phones, output_file):
    export_rows = []

    for item in rows:
        row = {
            "Название": item["name"],
            "Страница": item["page"],
            "Сайт": item["site"]
        }

        for i in range(max_emails):
            row[f"Email {i+1}"] = item["emails"][i] if i < len(item["emails"]) else ""

        for i in range(max_phones):
            row[f"Телефон {i+1}"] = item["phones"][i] if i < len(item["phones"]) else ""

        export_rows.append(row)

    pd.DataFrame(export_rows).to_excel(output_file, index=False)
    print(f"✅ Сохранено в {output_file} ({len(export_rows)} записей)")
    return len(export_rows)

def write_log(log_data):
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(f"[{datetime.now()}]\n")
        f.write(log_data + "\n\n")

def split_and_export():
    contacts = load_json(CONTACTS_FILE)
    brands = load_json(BRANDS_FILE)
    brand_map = {b["name"]: b.get("brand_page", "") for b in brands}
    date_str = datetime.now().strftime('%Y-%m-%d')

    ru_items = [item for item in contacts if '.ru' in item.get('site', '').lower()]
    non_ru_items = [item for item in contacts if '.ru' not in item.get('site', '').lower()]

    output_ru = f'contacts_ru_{date_str}.xlsx'
    output_non_ru = f'contacts_non_ru_{date_str}.xlsx'

    print("📊 Обработка .ru доменов...")
    ru_rows, ru_max_emails, ru_max_phones = prepare_rows(ru_items, brand_map)
    count_ru = export_to_excel(ru_rows, ru_max_emails, ru_max_phones, output_ru)

    print("📊 Обработка остальных доменов...")
    non_ru_rows, non_ru_max_emails, non_ru_max_phones = prepare_rows(non_ru_items, brand_map)
    count_non_ru = export_to_excel(non_ru_rows, non_ru_max_emails, non_ru_max_phones, output_non_ru)

    log_text = (
        f"Файл: {output_ru} — {count_ru} записей (max email: {ru_max_emails}, max телефоны: {ru_max_phones})\n"
        f"Файл: {output_non_ru} — {count_non_ru} записей (max email: {non_ru_max_emails}, max телефоны: {non_ru_max_phones})"
    )
    write_log(log_text)

if __name__ == '__main__':
    split_and_export()
