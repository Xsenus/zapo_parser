import json
from datetime import datetime

with open("groups.json", "r", encoding="utf-8") as f:
    groups = json.load(f)

base_url = "https://zapo.ru"
today = datetime.now().strftime("%Y-%m-%dT%H:%M:%S+03:00")

# Пропускаем повторяющиеся URL из XML, если такие были, можно будет вычесть вручную
sitemap_entries = []

for group in groups:
    url = f"{base_url}/{group['id']}"
    entry = {
        "loc": url,
        "lastmod": today,
        "changefreq": "weekly"
    }
    sitemap_entries.append(entry)
    url = f"{base_url}/{group['id']}_catalog"
    entry = {
        "loc": url,
        "lastmod": today,
        "changefreq": "weekly"
    }
    sitemap_entries.append(entry)

# Пример: как сохранить как sitemap XML
from lxml import etree as ET

urlset = ET.Element("urlset", xmlns="http://www.sitemaps.org/schemas/sitemap/0.9")

for entry in sitemap_entries:
    url_el = ET.SubElement(urlset, "url")
    ET.SubElement(url_el, "loc").text = entry["loc"]
    ET.SubElement(url_el, "lastmod").text = entry["lastmod"]
    ET.SubElement(url_el, "changefreq").text = entry["changefreq"]

tree = ET.ElementTree(urlset)
tree.write("sitemap_generated.xml", encoding="utf-8", xml_declaration=True, pretty_print=True)
