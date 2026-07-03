import logging
import sys
from pathlib import Path

from settings import LOG_LEVEL


def get_logger(name: str, logfile: str = "log.txt") -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    if logfile:
        file_handler = logging.FileHandler(Path(logfile), encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    logger.propagate = False
    return logger


def append_log(message: str, logfile: str = "log.txt") -> None:
    print(message, flush=True)
    with open(logfile, "a", encoding="utf-8") as handle:
        handle.write(message.rstrip() + "\n")
