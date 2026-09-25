"""Console and private per-run full/error log configuration."""
from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from uuid import uuid4

from . import SCRIPT_DIR


class ConsoleFilter(logging.Filter):
    """Command lines are already printed live; avoid a second console copy."""
    def filter(self, record: logging.LogRecord) -> bool:
        return not getattr(record, "command_output", False)


class PrivateFileHandler(logging.FileHandler):
    """Create run logs readable/writable only by the running account."""
    def _open(self):
        fd = os.open(self.baseFilename, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        return os.fdopen(fd, "a", encoding="utf-8", errors="replace")


def is_error_line(line: str) -> bool:
    """Recognize explicit severity prefixes without mistaking progress for errors."""
    # PBS output can prepend an ISO-style timestamp before TASK ERROR / Error.
    text = re.sub(r"^\d{4}-\d{2}-\d{2}[T ][0-9:.+Z-]+\s*(?:[\]:-]\s*)?", "", line.strip())
    return bool(re.match(r"^(?:TASK ERROR\b|(?:ERROR|FATAL|CRITICAL|FAILED)(?:\s*:|\s+-|$)|"
                         r"\[(?:ERROR|FATAL|CRITICAL)\])", text, re.IGNORECASE))


def close_logger(logger: logging.Logger) -> None:
    """Flush and close every handler so tests and repeated runs release files."""
    for handler in logger.handlers[:]:
        handler.close()
        logger.removeHandler(handler)


def build_logger(verbose: bool, log_dir: Optional[Path] = None) -> logging.Logger:
    """Create per-run full/error logs beneath the script, independent of cwd."""
    logger = logging.getLogger("pbs_sync_verifyjob_gc_mqtt")
    close_logger(logger)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    folder = log_dir if log_dir is not None else SCRIPT_DIR / "logs"
    folder.mkdir(parents=True, exist_ok=True, mode=0o700)
    stem = datetime.now(timezone.utc).strftime("pbs-maintenance-%Y%m%dT%H%M%S.%fZ")
    stem += f"-{os.getpid()}-{uuid4().hex[:8]}"
    logger.log_file = folder / (stem + ".log")
    logger.err_file = folder / (stem + ".err")
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    try:
        console = logging.StreamHandler()
        console.setLevel(logging.DEBUG if verbose else logging.INFO)
        console.addFilter(ConsoleFilter())
        logger.addHandler(console)
        full = PrivateFileHandler(logger.log_file, encoding="utf-8")
        full.setLevel(logging.DEBUG)
        logger.addHandler(full)
        errors = PrivateFileHandler(logger.err_file, encoding="utf-8", delay=True)
        errors.setLevel(logging.ERROR)
        logger.addHandler(errors)
        for handler in logger.handlers:
            handler.setFormatter(formatter)
    except OSError:
        close_logger(logger)
        raise
    return logger

