import json
import pandas as pd
from datetime import datetime
import os

INPUT_DATA_FILE = "stage11_modifications_detailed.json"
INPUT_BRANDS_FILE = "stage9_brands.json"
OUTPUT_EXCEL_FILE = "stage12_modifications_export.xlsx"
LOG_DIR = "zapo_logs"

def load_json(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)

def build_brands_lookup(brands_data):
    lookup = {}
    for category in ["foreign", "native", "moto"]:
        for brand in brands_data.get(category, []):
            name = brand["name"].strip().lower()
            if name not in lookup:
                lookup[name] = brand["link"]
    return lookup

def get_brand_link(brand, brands_lookup):
    return brands_lookup.get(brand.strip().lower(), "")

def flatten_modifications(data, brands_lookup):
    rows = []
    for car in data:
        brand = car.get("brand", "")
        model = car.get("model", "")
        image = car.get("model_image", "")
        model_url = car.get("modification_url", "")
        mod_table = car.get("modification_table", [])

        brand_link = get_brand_link(brand, brands_lookup)

        # Строка — только марка
        rows.append({
            "Марка": brand,
            "Модель": "",
            "Модификация": "",
            "Серия": "",
            "Год выпуска": "",
            "Ссылка": brand_link,
            "Изображение": image
        })

        # Строка — марка + модель
        rows.append({
            "Марка": brand,
            "Модель": model,
            "Модификация": "",
            "Серия": "",
            "Год выпуска": "",
            "Ссылка": model_url if model else brand_link,
            "Изображение": image
        })

        # Строки — модификации
        for mod in mod_table:
            raw_desc = mod.get("description", "")
            series = raw_desc.replace("Серия:", "").strip()
            rows.append({
                "Марка": brand,
                "Модель": model,
                "Модификация": mod.get("name", ""),
                "Серия": series,
                "Год выпуска": mod.get("year", ""),
                "Ссылка": mod.get("url", ""),
                "Изображение": image
            })
    return rows

def export_to_excel(rows, output_path):
    df = pd.DataFrame(rows)
    df.drop_duplicates(inplace=True)
    df.sort_values(by=["Марка", "Модель", "Модификация", "Серия", "Год выпуска"], inplace=True)
    df.to_excel(output_path, index=False)
    print(f"✅ Сохранено: {output_path} ({len(df)} строк)")
    return df

def write_log(df, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(output_dir, f"stage12_log_{timestamp}.txt")

    with open(log_file, "w", encoding="utf-8") as f:
        f.write(f"[{timestamp}]\n")
        f.write(f"Файл: {OUTPUT_EXCEL_FILE}\n")
        f.write(f"Строк: {len(df)}\n")
        f.write(f"Уникальных марок: {df['Марка'].nunique()}\n")
        f.write(f"Уникальных моделей: {df['Модель'].nunique()}\n")
        f.write(f"Уникальных модификаций: {df['Модификация'].nunique()}\n")

    print(f"📝 Лог записан: {log_file}")

def main():
    brands_data = load_json(INPUT_BRANDS_FILE)
    mods_data = load_json(INPUT_DATA_FILE)

    brands_lookup = build_brands_lookup(brands_data)
    rows = flatten_modifications(mods_data, brands_lookup)
    df = export_to_excel(rows, OUTPUT_EXCEL_FILE)
    write_log(df, LOG_DIR)

if __name__ == "__main__":
    main()
