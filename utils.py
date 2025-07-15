from threading import Lock
import os
import random
import re
from typing import Callable, Tuple
import requests
from concurrent.futures import ThreadPoolExecutor

__all__ = [
    "proxy_lock",
    "load_proxies",
    "download_proxies",
    "get_proxy_dict",
    "fetch_with_proxies",
    "MIRRORS",
    "with_mirror",
]

# 🔒 Глобальный лок для потокобезопасной работы с прокси
proxy_lock = Lock()

# 🔗 Шаблон URL для загрузки SOCKS5 прокси с best-proxies.ru
PROXY_API_URL = (
    "https://api.best-proxies.ru/proxylist.txt"
    "?key={key}&type=socks5&level=1&speed=1&limit=0"
)

def download_proxies(api_key: str) -> list[str]:
    """Загрузить список SOCKS5-прокси по API-ключу с best-proxies.ru."""
    try:
        response = requests.get(PROXY_API_URL.format(key=api_key), timeout=10)
        response.raise_for_status()
        return [line.strip() for line in response.text.splitlines() if line.strip()]
    except Exception:
        return []

def check_proxy_alive(proxy: str, timeout: int = 5) -> bool:
    """Проверить, работает ли прокси через запрос к Google."""
    test_url = "https://www.google.com"
    try:
        response = requests.get(
            test_url,
            proxies=get_proxy_dict(proxy),
            timeout=timeout
        )
        return response.ok
    except Exception:
        return False

def filter_alive_proxies(proxies: list[str], threads: int = 50) -> list[str]:
    """Отфильтровать только рабочие прокси (многопоточно)."""
    with ThreadPoolExecutor(max_workers=threads) as executor:
        results = list(executor.map(check_proxy_alive, proxies))
    return [proxy for proxy, ok in zip(proxies, results) if ok]

def load_proxies(
    proxy_file: str,
    alive_file: str | None = None,
    *,
    api_key: str | None = None,
    logger: Callable[[str], None] | None = None,
    check_alive: bool = False,
) -> list[str]:
    """
    Загрузка прокси с приоритетом API.
    При check_alive=True — оставляются только рабочие и сохраняются в alive_file.
    """
    proxies: set[str] = set()
    api_key = api_key or os.getenv("PROXY_API_KEY")

    # 🔁 Попробовать загрузку с API
    if api_key:
        api_proxies = download_proxies(api_key)
        if api_proxies:
            proxies.update(api_proxies)
            # 📁 Объединить с локальными (если есть)
            if os.path.exists(proxy_file):
                with open(proxy_file, "r", encoding="utf-8") as f:
                    proxies.update(line.strip() for line in f if line.strip())

            proxies = sorted(proxies)

            # ✅ Проверить живость, если требуется
            if check_alive:
                if logger:
                    logger(f"[PROXIES] 🔍 Проверка {len(proxies)} прокси...")
                alive = filter_alive_proxies(proxies)
                if logger:
                    logger(f"[PROXIES] ✅ Живых: {len(alive)}")

                if alive_file:
                    with open(alive_file, "w", encoding="utf-8") as f:
                        f.write("\n".join(alive))
                return alive

            # 💾 Сохранить объединённые прокси
            with open(proxy_file, "w", encoding="utf-8") as f:
                f.write("\n".join(proxies))
            if logger:
                logger(f"[PROXIES] Загружено с API ({len(api_proxies)} новых), всего: {len(proxies)}")
            return list(proxies)

    # 📂 Попробовать alive-файл
    if alive_file and os.path.exists(alive_file):
        with open(alive_file, "r", encoding="utf-8") as f:
            proxies = {line.strip() for line in f if line.strip()}
        if proxies:
            if logger:
                logger(f"[PROXIES] Загружено из alive-файла: {len(proxies)}")
            return list(proxies)

    # 📂 Попробовать основной файл
    if os.path.exists(proxy_file):
        with open(proxy_file, "r", encoding="utf-8") as f:
            proxies = {line.strip() for line in f if line.strip()}
        if proxies:
            if check_alive:
                if logger:
                    logger(f"[PROXIES] 🔍 Проверка {len(proxies)} прокси из proxy-файла...")
                alive = filter_alive_proxies(list(proxies))
                if logger:
                    logger(f"[PROXIES] ✅ Живых: {len(alive)}")
                if alive_file:
                    with open(alive_file, "w", encoding="utf-8") as f:
                        f.write("\n".join(alive))
                return alive

            if logger:
                logger(f"[PROXIES] Загружено из proxy-файла: {len(proxies)}")
            return list(proxies)

    if logger:
        logger("[PROXIES] ❌ Прокси не найдены — ни API, ни локальные файлы.")
    return []

def get_proxy_dict(proxy: str) -> dict:
    """Вернуть словарь прокси для requests с SOCKS5."""
    return {"http": f"socks5h://{proxy}", "https": f"socks5h://{proxy}"}

# 🔁 Список зеркал для обхода ограничений
MIRRORS = [
    "https://part.avtomir.ru",
    "https://zapo.ru",
    "https://vindoc.ru",
    "https://autona88.ru",
    "https://b2b.autorus.ru",
    "https://xxauto.pro",
    "https://motexc.ru",
]

def with_mirror(url: str, mirror: str) -> str:
    """Заменить домен в URL на указанный mirror."""
    return re.sub(r"https://[^/]+", mirror, url)

def fetch_with_proxies(
    url: str,
    proxies: list[str],
    working: list[str] | None = None,
    *,
    headers: dict | None = None,
    retries: int = 3,
    timeout: int = 10,
    logger: Callable[[str], None] | None = None,
    reload_proxies: Callable[[], list[str]] | None = None,
) -> Tuple[str | None, str | None]:
    """
    Загружает страницу с использованием списка прокси. При удачном подключении
    прокси добавляется в начало списка working для приоритетного использования.
    При неудаче — прокси исключается из списка. Возможна повторная загрузка
    списка через reload_proxies().
    """
    working = working or []

    for attempt in range(1, retries + 1):
        with proxy_lock:
            proxy_list = working + [p for p in proxies if p not in working]
            random.shuffle(proxy_list[len(working):])

        while proxy_list:
            proxy = proxy_list.pop(0)
            try:
                response = requests.get(
                    url,
                    headers=headers,
                    timeout=timeout,
                    proxies=get_proxy_dict(proxy),
                )
                response.raise_for_status()
                with proxy_lock:
                    if proxy in working:
                        working.remove(proxy)
                    working.insert(0, proxy)
                return response.text, proxy
            except Exception as e:
                if logger:
                    logger(f"[ПРОКСИ ОШИБКА] {proxy} — {e}")
                with proxy_lock:
                    if proxy in proxies:
                        proxies.remove(proxy)

        if reload_proxies and attempt < retries:
            if logger:
                logger("[ПРОКСИ] 🔁 Все прокси исчерпаны. Пробуем загрузить новые...")
            with proxy_lock:
                new_proxies = reload_proxies()
                if new_proxies:
                    if logger:
                        logger(f"[ПРОКСИ] Получено новых прокси: {len(new_proxies)}")
                    proxies.clear()
                    proxies.extend(new_proxies)
                    continue
                else:
                    if logger:
                        logger("[ПРОКСИ] ❌ Не удалось получить новые прокси.")
                    break

        # 📡 Последняя попытка — без прокси
        try:
            if logger:
                logger(f"[ПОПЫТКА {attempt}] Пробуем загрузить без прокси...")
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            return response.text, None
        except Exception as e:
            if logger:
                logger(f"[ОШИБКА] Попытка {attempt} без прокси не удалась: {e}")

    if logger:
        logger(f"[ОШИБКА] ❌ Все попытки загрузки неудачны: {url}")
    return None, None