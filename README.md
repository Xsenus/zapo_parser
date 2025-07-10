# Zapo Parser

Скрипты для парсинга сайтов брендов с целью сбора контактной информации и автокаталога с сайта zapo.ru.

## 📁 Структура

- `stage1_brands_scraper.py` — сбор информации о брендах и страницах  
- `stage2_contacts_scraper.py` — поиск сайтов брендов  
- `stage3_contacts_collector.py` — сбор email и телефонов с главной и контактных страниц  
- `stage4_export_to_excel.py` — экспорт в Excel, деление на `.ru` и остальные домены  
- `stage5_carbase_scraper.py` — парсинг автокаталога брендов, моделей и версий автомобилей  
- `stage6_parse_modifications.py` — сбор модификаций автомобилей по version_url  
- `stage7_parse_parts.py` — сбор деталей по modification_url  
- `stage8_export_parts_to_excel.py` — экспорт деталей в Excel  
- `stage9_parse_catalog_brands.py` — парсинг брендов по категориям (CARS_FOREIGN, CARS_NATIVE, MOTORCYCLE)  
- `stage10_parse_models.py` — парсинг моделей по брендам  
- `stage11_parse_modification_table.py` — постраничный парсинг таблицы модификаций
- `stage12_export_modifications_to_excel.py` — экспорт модификаций в Excel
- `stage13_oils_sitemaps.py` — генерация sitemap для каталога масел

## ⚙️ Установка

```bash
pip install -r requirements.txt
```

## 🚀 Запуск экспорта

```bash
python stage4_export_to_excel.py          # Контакты
python stage8_export_parts_to_excel.py    # Детали
```

## 📦 Результат

### Контактные данные

- `contacts_ru_YYYY-MM-DD_HH-MM-SS.xlsx` — бренды с сайтами в зоне `.ru`  
- `contacts_non_ru_YYYY-MM-DD_HH-MM-SS.xlsx` — все остальные  
- `stage4_export_log.txt` — лог выполнения  

### Автокаталог и детали

- `stage5_carbase.json` — автокаталог с zapo.ru  
- `stage6_versions_detailed.json`, `stage7_parts_detailed.json`, `stage11_modifications_detailed.json` — JSON с модификациями и деталями
- `stage12_modifications_export.xlsx` — итоговая таблица модификаций
- `sitemaps_output/sitemap_catalog_index.xml` — sitemap каталога масел

## 📌 Параметры экспорта

В `stage4_export_to_excel.py`:

```python
MAX_EMAILS = 2     # Сколько email добавлять (или -1 для всех)
MAX_PHONES = 5     # Сколько телефонов добавлять (или -1 для всех)
```

## 📝 Формат таблиц

| Название | Страница | Сайт | Email 1 | Email 2 | ... | Телефон 1 | Телефон 2 | ... |
|----------|----------|------|---------|---------|-----|------------|-------------|-----|

- Кол-во колонок для email/телефонов — по максимальному числу на группу
- Email и телефоны очищаются от дублей
- Телефоны нормализуются (например, `+7`, `8`, `00` → `7XXXXXXXXXX`)

## 🚘 Об автокаталоге

`stage5_carbase_scraper.py` → `stage11_parse_modification_table.py`

- Сохраняются: бренд, модель, версия, годы выпуска, модификации, детали
- Ведётся лог: `zapo_logs/`
- Все этапы используют SOCKS5-прокси и логирование. Список зеркал хранится в `utils.MIRRORS` и применяется во всех скриптах.
- Промежуточные файлы сохраняются в `stageX_temp_results/`

## 🔧 TODO

- [ ] Добавить экспорт названия страны
- [ ] Определение страны по телефонному коду
