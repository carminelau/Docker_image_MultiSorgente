import os
import sys
from typing import Iterable


_WARNED: set[str] = set()


def _warn_once(name: str, fallback: str) -> None:
    if name in _WARNED:
        return
    _WARNED.add(name)
    print(
        f"WARNING config env {name} missing, using compatibility fallback",
        file=sys.stderr,
        flush=True,
    )


def get_env(name: str, fallback: str = "", warn: bool = True) -> str:
    value = os.getenv(name)
    if value is not None and value != "":
        return value
    if warn and fallback != "":
        _warn_once(name, fallback)
    return fallback


def get_int_env(name: str, fallback: int, warn: bool = True) -> int:
    value = get_env(name, str(fallback), warn=warn)
    try:
        return int(value)
    except ValueError:
        print(
            f"WARNING config env {name} invalid integer '{value}', using {fallback}",
            file=sys.stderr,
            flush=True,
        )
        return fallback


def get_bool_env(name: str, fallback: bool = False) -> bool:
    value = os.getenv(name)
    if value is None or value == "":
        return fallback
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def get_csv_env(name: str, fallback: Iterable[str]) -> list[str]:
    value = os.getenv(name)
    if not value:
        return list(fallback)
    return [item.strip() for item in value.split(",") if item.strip()]


LOG_LEVEL = get_env("LOG_LEVEL", "INFO", warn=False).upper()
DRY_RUN = get_bool_env("DRY_RUN", False)

MONGO_HOST = get_env("MONGO_HOST", "mongo_multisorgente")
MONGO_PORT = get_int_env("MONGO_PORT", 27017)
MONGO_USERNAME = get_env("MONGO_INITDB_ROOT_USERNAME", "multisorgente")
MONGO_PASSWORD = get_env("MONGO_INITDB_ROOT_PASSWORD", "multisorgente")
MONGO_AUTH_DB = get_env("MONGO_AUTH_DB", "admin")
MONGO_DB_MS = get_env("MONGO_DB_MS", "MS", warn=False)
MONGO_DB_SSDB = get_env("MONGO_DB_SSDB", "SSDB", warn=False)

STATUS_API_URL = get_env(
    "STATUS_API_URL",
    "https://square.sensesquare.eu:5002/inserisci_status_processi",
)
STATUS_API_KEY = get_env("STATUS_API_KEY", "WDBNX4IUF66C")
TOKEN_WRITE = get_env("TOKEN_WRITE", "1FPCPQCZPV7G")
DATATIME_URL = get_env("DATATIME_URL", "http://188.166.29.27:5000/datatime")

SERVER_DO_URL = get_env("SERVER_DO_URL", "http://188.166.29.27:5004")
SERVER_BETA_URL = get_env("SERVER_BETA_URL", "http://193.205.184.54:5004")
SERVER_UBU_URL = get_env("SERVER_UBU_URL", "http://192.168.1.150:5004")

OPENMETEO_BASE_URL = get_env("OPENMETEO_BASE_URL", "http://192.168.1.112:8080/v1/forecast")
OPENMETEO_FALLBACK_URL = get_env(
    "OPENMETEO_FALLBACK_URL",
    "https://api.open-meteo.com/v1/forecast",
)

HTTP_TIMEOUT_SECONDS = get_int_env("HTTP_TIMEOUT_SECONDS", 30, warn=False)
HTTP_RETRIES = get_int_env("HTTP_RETRIES", 3, warn=False)
