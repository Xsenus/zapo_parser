from threading import Lock
import os
import random
import re
from typing import Callable, Iterable, Tuple
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

# Shared lock for thread-safe proxy operations
proxy_lock = Lock()

# URL template for downloading SOCKS5 proxies from best-proxies.ru
PROXY_API_URL = (
    "https://api.best-proxies.ru/proxylist.txt"
    "?key={key}&type=socks5&level=1&speed=1&limit=0"
)

def download_proxies(api_key: str) -> list[str]:
    """Download a list of proxies using the provided *api_key*."""
    try:
        response = requests.get(PROXY_API_URL.format(key=api_key), timeout=10)
        response.raise_for_status()
        return [line.strip() for line in response.text.splitlines() if line.strip()]
    except Exception:
        return []

def check_proxy_alive(proxy: str, timeout: int = 5) -> bool:
    """Проверка, работает ли SOCKS5-прокси через запрос к Google."""
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
    """Вернуть список только живых прокси."""
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
    Загружаем прокси с приоритетом API. При check_alive=True —
    отфильтровываются живые и сохраняются в alive_file.
    """
    proxies: set[str] = set()
    api_key = api_key or os.getenv("PROXY_API_KEY")

    if api_key:
        api_proxies = download_proxies(api_key)
        if api_proxies:
            proxies.update(api_proxies)
            if os.path.exists(proxy_file):
                with open(proxy_file, "r", encoding="utf-8") as f:
                    proxies.update(line.strip() for line in f if line.strip())

            proxies = sorted(proxies)

            if check_alive:
                if logger:
                    logger(f"[PROXIES] 🔍 Проверка {len(proxies)} прокси...")
                alive = filter_alive_proxies(list(proxies))
                if logger:
                    logger(f"[PROXIES] ✅ Живых: {len(alive)}")

                if alive_file:
                    with open(alive_file, "w", encoding="utf-8") as f:
                        f.write("\n".join(alive))
                return alive

            with open(proxy_file, "w", encoding="utf-8") as f:
                f.write("\n".join(proxies))
            if logger:
                logger(f"[PROXIES] Загружено с API ({len(api_proxies)} новых), всего {len(proxies)}")
            return list(proxies)

    # Fallback to alive file
    if alive_file and os.path.exists(alive_file):
        with open(alive_file, "r", encoding="utf-8") as f:
            proxies = {line.strip() for line in f if line.strip()}
        if proxies:
            if logger:
                logger(f"[PROXIES] Загружено из alive-файла ({alive_file}): {len(proxies)}")
            return list(proxies)

    # Fallback to proxy file
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
                logger(f"[PROXIES] Загружено из proxy-файла ({proxy_file}): {len(proxies)}")
            return list(proxies)

    if logger:
        logger("[PROXIES] Прокси не найдены — ни API, ни локальные файлы")
    return []

def get_proxy_dict(proxy: str) -> dict:
    """Return a requests-compatible proxy dictionary for SOCKS5 proxies."""
    return {"http": f"socks5h://{proxy}", "https": f"socks5h://{proxy}"}


# Default list of mirrors for zapo-like sites
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
    """Replace the host in *url* with the provided *mirror*."""
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
) -> Tuple[str | None, str | None]:
    """Fetch *url* trying the given proxies and return ``(text, used_proxy)``."""

    working = working or []
    for attempt in range(1, retries + 1):
        with proxy_lock:
            proxy_list = proxies.copy()
        random.shuffle(proxy_list)

        while proxy_list:
            proxy = proxy_list.pop()
            try:
                response = requests.get(
                    url,
                    headers=headers,
                    timeout=timeout,
                    proxies=get_proxy_dict(proxy),
                )
                response.raise_for_status()
                with proxy_lock:
                    if proxy not in working:
                        working.append(proxy)
                return response.text, proxy
            except Exception as e:
                if logger:
                    logger(f"[PROXY ERROR] {proxy} — {e}")
                with proxy_lock:
                    if proxy in proxies:
                        proxies.remove(proxy)

        try:
            if logger:
                logger(f"[ATTEMPT {attempt}] Пробуем без прокси...")
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            return response.text, None
        except Exception as e:
            if logger:
                logger(f"[ERROR] Попытка {attempt} без прокси не удалась: {e}")

    if logger:
        logger(f"[FAILED] Не удалось загрузить: {url}")
    return None, None
