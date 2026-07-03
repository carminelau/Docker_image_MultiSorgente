from traceback import format_exc

from http_client import post
from settings import DRY_RUN, STATUS_API_KEY, STATUS_API_URL


def send_status(processo: str, delta: str, timestamp: str, status: str, traceback: str | None = None) -> bool:
    payload = {
        "apikey": STATUS_API_KEY,
        "processo": processo,
        "delta": delta,
        "timestamp": timestamp,
        "status": status,
    }
    if traceback:
        payload["traceback"] = traceback
    if DRY_RUN:
        print(f"DRY_RUN status skipped: {payload}", flush=True)
        return True
    try:
        post(STATUS_API_URL, data=payload)
        return True
    except Exception:
        print(f"Errore invio status {processo}: {format_exc()}", flush=True)
        return False
