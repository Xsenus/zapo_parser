import os
import json
import re
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import phonenumbers

INPUT_FILE = 'stage2_sites.json'
OUTPUT_FILE = 'stage3_contacts.json'
PROCESSED_LOG = 'stage3_contacts_processed.json'
ERROR_LOG = 'stage3_errors.log'
MAX_WORKERS = 25
SAVE_EVERY = 5


def load_json(filename):
    if os.path.exists(filename):
        with open(filename, 'r', encoding='utf-8') as f:
            return json.load(f)
    return []


def save_json(filename, data):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def log_error(message):
    with open(ERROR_LOG, 'a', encoding='utf-8') as f:
        f.write(message + '\n')


def extract_contacts(html: str) -> dict:
    soup = BeautifulSoup(html, 'html.parser')
    text = soup.get_text(" ", strip=True)

    emails = set(re.findall(r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+', text))

    phones = set()
    for match in phonenumbers.PhoneNumberMatcher(text, None):
        raw = match.raw_string
        if raw:
            phones.add(raw.strip())

    return {
        'emails': list(emails),
        'phones': list(phones)
    }


def try_fetch(url):
    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/114.0.0.0 Safari/537.36"
            )
        }
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        return response.text
    except Exception:
        return None


def find_contact_page(base_url, html):
    soup = BeautifulSoup(html, 'html.parser')
    contact_keywords = [
    'контакт', 'контакты', 'связь', 'обратная связь', 'о нас', 'о компании',
    'contact', 'contacts', 'contact us', 'get in touch', 'contactez', 'contatti', 'contato',
    'kontakt', 'kontakte', 'impressum', 'impress', 'about', 'about us', 'company',
    'info', 'support', 'service', 'customer service', 'help' ]
    
    for a in soup.find_all('a', href=True):
        text = a.get_text(strip=True).lower()
        href = a['href']
        if any(kw in text for kw in contact_keywords):
            return urljoin(base_url, href)

    return None

def process_site(brand):
    name = brand['name']
    site = brand.get('company_site')
    if not site:
        return None

    try:
        html_main = try_fetch(site)
        if not html_main:
            raise Exception('Главная страница недоступна')

        contact_data = extract_contacts(html_main)

        contact_url = find_contact_page(site, html_main)
        if contact_url and contact_url != site:
            html_contact = try_fetch(contact_url)
            if html_contact:
                extra_data = extract_contacts(html_contact)
                contact_data['emails'].extend(extra_data['emails'])
                contact_data['phones'].extend(extra_data['phones'])

        return {
            'name': name,
            'site': site,
            'emails': list(set(contact_data['emails'])),
            'phones': list(set(contact_data['phones']))
        }

    except Exception as e:
        log_error(f"{name} | {site} | {str(e)}")
        return None


def main():
    all_sites = load_json(INPUT_FILE)
    processed = load_json(PROCESSED_LOG)
    processed_names = {b['name'] for b in processed}
    to_process = [b for b in all_sites if b.get('company_site') and b['name'] not in processed_names]

    print(f"📄 Всего брендов: {len(all_sites)}")
    print(f"✅ Уже обработано: {len(processed)}")
    print(f"🔁 К обработке: {len(to_process)}")

    results = []
    counter = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_site, brand): brand for brand in to_process}
        for future in tqdm(as_completed(futures), total=len(futures), desc="📥 Сбор контактов"):
            try:
                result = future.result(timeout=60)
                if result:
                    results.append(result)
                    counter += 1
                    if counter % SAVE_EVERY == 0:
                        save_json(OUTPUT_FILE, results)
                        save_json(PROCESSED_LOG, processed + results)
            except Exception as e:
                brand = futures[future]
                log_error(f"{brand['name']} | {brand.get('company_site')} | future timeout/error: {str(e)}")

    save_json(OUTPUT_FILE, results)
    save_json(PROCESSED_LOG, processed + results)
    print(f"✅ Завершено. Собрано новых записей: {len(results)}")


if __name__ == '__main__':
    main()
