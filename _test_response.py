import requests
import json

url = "https://zapo.ru/tires_catalog"

params = {
    "goods_group": "tires",
    "action": "goods_catalog/goods_catalog/getFilters",
    "viewMode": "tile",
    "property[brands][]": "AMTEL",
    "excluded": "brands"
}

headers = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": "https://zapo.ru/tires_catalog"
}

try:
    response = requests.get(url, params=params, headers=headers, timeout=10)
    response.raise_for_status()
    print(f"✅ Статус: {response.status_code}")
    print(f"📦 Content-Type: {response.headers.get('Content-Type')}")
    data = response.json()
    print("🔍 Ответ JSON:")
    print(json.dumps(data, indent=2, ensure_ascii=False))

except Exception as e:
    print(f"❌ Ошибка запроса: {e}")
    print("Ответ сервера:")
    print(response.text)
