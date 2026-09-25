"""Local sendmail/Postfix email delivery for PBS maintenance outcomes."""
from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
from email.message import EmailMessage
from pathlib import Path
from typing import Dict, Any, Optional


def find_sendmail(configured_path: str = "") -> Optional[str]:
    """Find a usable sendmail-compatible executable, honoring an explicit override first."""
    if configured_path:
        candidate = Path(configured_path).expanduser()
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)
        return None

    for candidate in (Path("/usr/sbin/sendmail"), Path("/usr/bin/sendmail")):
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)

    return shutil.which("sendmail")


def email_send(email_settings: Dict[str, Any], sendmail_settings: Dict[str, Any],
               payload: Dict[str, Any], logger: logging.Logger) -> None:
    """Build the JSON outcome email and hand it to local sendmail/Postfix with ``sendmail -t``."""
    message = EmailMessage()
    message["From"] = email_settings["from_address"]
    message["To"] = ", ".join(email_settings["to_addresses"])
    label = "DRY RUN" if payload["dry_run"] else (
        "FAILED" if payload["event"] == "pbs_maintenance_failed" else "SUCCESS"
    )
    message["Subject"] = f'{email_settings["subject_prefix"]} {label} - {payload["hostname"]}'
    message.set_content(json.dumps(payload, ensure_ascii=False, indent=2))

    sendmail_bin = find_sendmail(sendmail_settings["path"])
    if not sendmail_bin:
        raise FileNotFoundError(
            "Could not find a sendmail-compatible executable; install Postfix/sendmail or set sendmail.path."
        )

    try:
        subprocess.run(
            [sendmail_bin, "-t"],
            input=message.as_bytes(),
            check=True,
            capture_output=True,
        )
    except subprocess.CalledProcessError as exc:
        stderr = exc.stderr.decode(errors="replace").strip() if exc.stderr else ""
        detail = f"; stderr: {stderr}" if stderr else ""
        raise RuntimeError(f"sendmail exited with status {exc.returncode}{detail}") from exc

    logger.info("Email handed to local sendmail: %s -t", sendmail_bin)
