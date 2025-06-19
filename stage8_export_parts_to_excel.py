# stage8_export_parts_to_excel.py

import json
import pandas as pd
from tqdm import tqdm
from datetime import datetime
import os

STAGE6_FILE = "stage6_versions_detailed.json"
STAGE7_FILE = "stage7_parts_detailed.json"
OUTPUT_FILE = "stage8_parts_export.xlsx"
LOGS_DIR = "zapo_logs"

def load_data(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)

def build_parts_lookup(parts_data):
    """Создание словаря {modification_url: parts[]}"""
    lookup = {}
    for car in parts_data:
        for mod in car.get("modifications", []):
            mod_url = mod.get("modification_url")
            if mod_url:
                lookup[mod_url] = mod.get("parts", [])
    return lookup

def flatten_full_data(full_data, parts_lookup):
    rows = []
    for car in tqdm(full_data, desc="🚗 Обработка авто"):
        brand = car.get("brand", "")
        brand_url = car.get("brand_url", "")
        model = car.get("model", "")
        version = car.get("version", "")
        version_url = car.get("version_url", "")
        image_car = car.get("image", "")
        modifications = car.get("modifications", [])

        # Уровень 1 — только марка
        rows.append({
            "Марка": brand,
            "Группа": "",
            "Модель": "",
            "Модификация": "",
            "Товарная группа": "",
            "Ссылка": brand_url,
            "Название товара": "",
            "Ссылка на товар": "",
            "Изображение автомобиля": image_car,
            "Изображение товара": "",
        })

        # Уровень 2 — марка + группа
        group_link = f"{brand_url}#group_{model}"
        rows.append({
            "Марка": brand,
            "Группа": model,
            "Модель": "",
            "Модификация": "",
            "Товарная группа": "",
            "Ссылка": group_link,
            "Название товара": "",
            "Ссылка на товар": "",
            "Изображение автомобиля": image_car,
            "Изображение товара": "",
        })

        # Уровень 3 — марка + группа + модель
        rows.append({
            "Марка": brand,
            "Группа": model,
            "Модель": version,
            "Модификация": "",
            "Товарная группа": "",
            "Ссылка": version_url,
            "Название товара": "",
            "Ссылка на товар": "",
            "Изображение автомобиля": image_car,
            "Изображение товара": "",
        })

        for mod in modifications:
            mod_name = mod.get("modification", "")
            mod_url = mod.get("modification_url", "")
            parts = parts_lookup.get(mod_url, [])

            if parts:
                for part in parts:
                    rows.append({
                        "Марка": brand,
                        "Группа": model,
                        "Модель": version,
                        "Модификация": mod_name,
                        "Товарная группа": part.get("group", ""),
                        "Ссылка": mod_url,
                        "Название товара": part.get("name", ""),
                        "Ссылка на товар": part.get("search_url", ""),
                        "Изображение автомобиля": image_car,
                        "Изображение товара": part.get("image_url", ""),
                    })
            else:
                # Если нет деталей — просто строка модификации
                rows.append({
                    "Марка": brand,
                    "Группа": model,
                    "Модель": version,
                    "Модификация": mod_name,
                    "Товарная группа": "",
                    "Ссылка": mod_url,
                    "Название товара": "",
                    "Ссылка на товар": "",
                    "Изображение автомобиля": image_car,
                    "Изображение товара": "",
                })
    return rows

def export_to_excel(rows, output_path):
    df = pd.DataFrame(rows)
    df.sort_values(by=["Марка", "Группа", "Модель", "Модификация"], inplace=True)
    df.to_excel(output_path, index=False)
    print(f"✅ Сохранено: {output_path} ({len(df)} строк)")
    return df

def write_log(df, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(output_dir, f"parts_export_log_{timestamp}.txt")

    brands = df["Марка"].nunique()
    models = df["Группа"].nunique()
    mods = df["Модификация"].nunique()

    with open(log_file, "w", encoding="utf-8") as f:
        f.write(f"[{timestamp}]\n")
        f.write(f"Файл: {OUTPUT_FILE}\n")
        f.write(f"Строк: {len(df)}\n")
        f.write(f"Марок: {brands}, Моделей: {models}, Модификаций: {mods}\n")

    print(f"📝 Лог записан: {log_file}")

def main():
    full_data = load_data(STAGE6_FILE)
    parts_data = load_data(STAGE7_FILE)
    parts_lookup = build_parts_lookup(parts_data)
    rows = flatten_full_data(full_data, parts_lookup)
    df = export_to_excel(rows, OUTPUT_FILE)
    write_log(df, LOGS_DIR)

if __name__ == "__main__":
    main()
