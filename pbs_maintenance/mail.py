"""SMTP email delivery with optional authentication and verified TLS."""
from __future__ import annotations

import json
import logging
import smtplib
import ssl
from email.message import EmailMessage
from typing import Dict, Any


def email_send(settings: Dict[str, Any], payload: Dict[str, Any], logger: logging.Logger) -> None:
    """Send an outcome through SMTP with verified TLS when selected; no attachments."""
    message = EmailMessage()
    message["From"] = settings["from_address"]
    message["To"] = ", ".join(settings["to_addresses"])
    label = "DRY RUN" if payload["dry_run"] else ("FAILED" if payload["event"] == "pbs_maintenance_failed" else "SUCCESS")
    message["Subject"] = f'{settings["subject_prefix"]} {label} - {payload["hostname"]}'
    message.set_content(json.dumps(payload, ensure_ascii=False, indent=2))
    context = ssl.create_default_context(cafile=settings["cafile"] or None) if settings["security"] != "none" else None
    kwargs = dict(host=settings["host"], port=settings["port"], timeout=settings["timeout_sec"])
    connection = (smtplib.SMTP_SSL(context=context, **kwargs) if settings["security"] == "ssl"
                  else smtplib.SMTP(**kwargs))
    with connection as client:
        if settings["security"] == "starttls":
            client.ehlo()
            client.starttls(context=context)
            client.ehlo()
        if settings["username"]:
            client.login(settings["username"], settings["password"])
        refused = client.send_message(message, from_addr=settings["from_address"], to_addrs=settings["to_addresses"])
        if refused:
            raise RuntimeError("SMTP refused one or more recipients.")
    logger.info("Email accepted by SMTP server.")

