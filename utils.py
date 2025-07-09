"""Common utilities for Zapo parsers."""

from threading import Lock
import os

__all__ = ["proxy_lock", "load_proxies", "get_proxy_dict"]

# Shared lock for thread-safe proxy operations
proxy_lock = Lock()


def load_proxies(proxy_file: str, alive_file: str | None = None):
    """Load a list of proxies, preferring the alive file if provided."""
    if alive_file and os.path.exists(alive_file):
        with open(alive_file, "r", encoding="utf-8") as f:
            proxies = [line.strip() for line in f if line.strip()]
        if proxies:
            return proxies
    if os.path.exists(proxy_file):
        with open(proxy_file, "r", encoding="utf-8") as f:
            return [line.strip() for line in f if line.strip()]
    return []


def get_proxy_dict(proxy: str):
    """Return a requests-compatible proxy dictionary for SOCKS5 proxies."""
    return {"http": f"socks5h://{proxy}", "https": f"socks5h://{proxy}"}
