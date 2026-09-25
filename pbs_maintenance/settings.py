"""TOML defaults, parsing, validation and notification selection."""
from __future__ import annotations

import argparse
from copy import deepcopy
from pathlib import Path
from typing import Dict, Any

from . import mail as mail_client
from . import mqtt as mqtt_client

try:
    import tomllib
except ImportError:  # Python 3.9/3.10 support without inventing a TOML parser.
    try:
        import tomli as tomllib
    except ImportError:
        tomllib = None


DEFAULT_CONFIG = {
    "steps": {"sync": True, "verify": True, "prune": True, "gc": True},
    "jobs": {"sync_job": "", "verify_job": "", "gc_datastore": ""},
    "prune": {"mode": "job", "job": "", "datastore": "", "keep_last": "",
              "keep_daily": "", "keep_weekly": "", "keep_monthly": "", "keep_yearly": ""},
    "logging": {"verbose": False},
    "dry_run": {"enabled": True, "send_mqtt": False, "send_email": False},
    "mqtt": {"enabled": True, "on_success": False, "host": "", "port": 1883, "topic": "",
             "username": "", "password": "", "tls": False, "cafile": "",
             "insecure": False, "client_id": "", "retain": False,
             "max_output_chars": 4000, "timeout_sec": 15},
    "email": {"enabled": False, "on_success": False, "from_address": "", "to_addresses": [],
              "subject_prefix": "[PBS maintenance]"},
    "sendmail": {"path": ""},
}


def load_config(path: Path) -> Dict[str, Any]:
    """Read TOML, reject unknown/mistyped settings, and fill documented defaults."""
    if tomllib is None:
        raise ValueError("Python 3.9/3.10 needs tomli: install requirements.txt, or use Python 3.11+.")
    try:
        with path.open("rb") as stream:
            supplied = tomllib.load(stream)
    except (OSError, ValueError) as exc:
        # Avoid echoing TOML source fragments which may contain credentials.
        raise ValueError(f"Cannot read valid TOML from {path} ({type(exc).__name__}).") from exc
    config = deepcopy(DEFAULT_CONFIG)
    for section, options in supplied.items():
        if section not in config or not isinstance(options, dict):
            raise ValueError(f"Unknown section or invalid table: {section}.")
        for key, value in options.items():
            if key not in config[section]:
                raise ValueError(f"Unknown setting: {section}.{key}.")
            if section == "prune" and key.startswith("keep_"):
                valid = (type(value) is int and value >= 0) or value == ""
            else:
                valid = type(value) is type(config[section][key])
            if not valid:
                raise ValueError(f"Invalid type/value for {section}.{key}; see config.toml comments.")
            config[section][key] = value
    # Resolve optional local file paths relative to the config, never to the caller's cwd.
    value = config["mqtt"]["cafile"]
    if value:
        candidate = Path(value).expanduser()
        config["mqtt"]["cafile"] = str(candidate if candidate.is_absolute() else path.parent / candidate)
    value = config["sendmail"]["path"]
    if value:
        candidate = Path(value).expanduser()
        config["sendmail"]["path"] = str(candidate if candidate.is_absolute() else path.parent / candidate)
    return config


def config_to_args(config: Dict[str, Any]) -> argparse.Namespace:
    """Adapt TOML to the original prune/payload helpers instead of duplicating them."""
    args = argparse.Namespace(
        sync_job=config["jobs"]["sync_job"], verify_job=config["jobs"]["verify_job"],
        datastore=config["jobs"]["gc_datastore"], prune=config["prune"]["mode"] == "manual",
        prune_job=config["prune"]["job"] if config["prune"]["mode"] == "job" else None,
        prune_datastore=config["prune"]["datastore"],
    )
    for period in ("last", "daily", "weekly", "monthly", "yearly"):
        value = config["prune"]["keep_" + period]
        setattr(args, "prune_keep_" + period, None if value == "" else value)
    return args


def _any_keep_set(args: argparse.Namespace) -> bool:
    return any(
        v is not None
        for v in (
            args.prune_keep_last,
            args.prune_keep_daily,
            args.prune_keep_weekly,
            args.prune_keep_monthly,
            args.prune_keep_yearly,
        )
    )


def notification_channels(config: Dict[str, Any], event: str | None = None) -> Dict[str, bool]:
    """Select transports for validation/dry-run/failure, with success explicitly opt-in."""
    if config["dry_run"]["enabled"]:
        return {name: config["dry_run"]["send_" + name] for name in ("mqtt", "email")}

    channels = {name: config[name]["enabled"] for name in ("mqtt", "email")}
    if event == "pbs_maintenance_success":
        return {name: channels[name] and config[name]["on_success"] for name in ("mqtt", "email")}

    # Validation (event=None) and failure delivery both use the master channel
    # switches. This guarantees that disabling success messages does not suppress
    # a later failure notification.
    return channels


def validate_config(config: Dict[str, Any], args: argparse.Namespace) -> None:
    """Validate selection and active notifications before maintenance or connections."""
    steps = config["steps"]
    if not any(steps.values()):
        raise ValueError("Nothing to do: enable at least one setting in [steps].")
    for step, key in (("sync", "sync_job"), ("verify", "verify_job"), ("gc", "gc_datastore")):
        if steps[step] and not config["jobs"][key].strip():
            raise ValueError(f"jobs.{key} is required when steps.{step} is true.")
    if config["prune"]["mode"] not in ("job", "manual"):
        raise ValueError('prune.mode must be "job" or "manual".')
    if steps["prune"]:
        if args.prune:
            if not args.prune_datastore.strip() or not _any_keep_set(args):
                raise ValueError("Manual prune requires prune.datastore and at least one prune.keep_* value.")
            if config["prune"]["job"]:
                raise ValueError('Clear prune.job when selecting prune.mode = "manual".')
        elif not args.prune_job.strip():
            raise ValueError('prune.job is required when prune.mode = "job" and pruning is enabled.')
    channels = notification_channels(config)
    mqtt = config["mqtt"]
    if not 1 <= mqtt["port"] <= 65535 or mqtt["timeout_sec"] <= 0:
        raise ValueError("mqtt.port must be 1..65535 and timeout_sec must be positive.")
    if mqtt["max_output_chars"] <= 0:
        raise ValueError("mqtt.max_output_chars must be a positive integer.")
    if channels["mqtt"]:
        if not mqtt["host"].strip():
            raise ValueError("mqtt.host is required for the selected notification channel.")
        if mqtt["password"] and not mqtt["username"]:
            raise ValueError("mqtt.password requires mqtt.username.")
        if mqtt["cafile"] and not Path(mqtt["cafile"]).is_file():
            raise ValueError("mqtt.cafile does not name an existing certificate file.")
        if not mqtt["topic"] or any(c in mqtt["topic"] for c in ("+", "#", "\x00")):
            raise ValueError("mqtt.topic must be a nonempty publish topic without wildcard/NUL characters.")
        if mqtt_client.mqtt is None:
            raise ValueError("MQTT sending requires paho-mqtt; install requirements.txt first.")
    email = config["email"]
    if any(type(item) is not str for item in email["to_addresses"]):
        raise ValueError("email.to_addresses must be an array of address strings.")
    if channels["email"]:
        addresses = [email["from_address"]] + email["to_addresses"]
        if not email["to_addresses"] or any(not a.strip() or "@" not in a or any(c in a for c in "\r\n") for a in addresses):
            raise ValueError("Email requires a from_address and nonempty to_addresses with valid mailbox addresses.")
        if any(c in email["subject_prefix"] for c in "\r\n"):
            raise ValueError("email.subject_prefix must be a single line.")
        if mail_client.find_sendmail(config["sendmail"]["path"]) is None:
            raise ValueError("Email sending requires sendmail; install Postfix/sendmail or set sendmail.path.")

