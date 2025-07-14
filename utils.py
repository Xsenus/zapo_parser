"""Common utilities for Zapo parsers."""

from threading import Lock
import os
import random
import re
from typing import Callable, Iterable, Tuple
import requests

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


def load_proxies(
    proxy_file: str,
    alive_file: str | None = None,
    *,
    api_key: str | None = None,
) -> list[str]:
    """Load proxies from ``alive_file`` or ``proxy_file``.

    If files are missing and ``api_key`` (or ``PROXY_API_KEY`` env var)
    is provided, proxies will be downloaded from best-proxies.ru and saved to
    ``proxy_file``.
    """

    if alive_file and os.path.exists(alive_file):
        with open(alive_file, "r", encoding="utf-8") as f:
            proxies = [line.strip() for line in f if line.strip()]
        if proxies:
            return proxies

    if os.path.exists(proxy_file):
        with open(proxy_file, "r", encoding="utf-8") as f:
            proxies = [line.strip() for line in f if line.strip()]
        if proxies:
            return proxies

    api_key = api_key or os.getenv("PROXY_API_KEY")
    if api_key:
        proxies = download_proxies(api_key)
        if proxies:
            with open(proxy_file, "w", encoding="utf-8") as f:
                f.write("\n".join(proxies))
            return proxies

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
