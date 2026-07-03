import time
from typing import Any

import requests

from settings import HTTP_RETRIES, HTTP_TIMEOUT_SECONDS


def request(method: str, url: str, **kwargs: Any) -> requests.Response:
    kwargs.setdefault("timeout", HTTP_TIMEOUT_SECONDS)
    last_error: Exception | None = None

    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            response = requests.request(method, url, **kwargs)
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            last_error = exc
            if attempt >= HTTP_RETRIES:
                break
            time.sleep(min(2 ** (attempt - 1), 10))

    raise last_error or RuntimeError(f"HTTP request failed: {method} {url}")


def get(url: str, **kwargs: Any) -> requests.Response:
    return request("GET", url, **kwargs)


def post(url: str, **kwargs: Any) -> requests.Response:
    return request("POST", url, **kwargs)
