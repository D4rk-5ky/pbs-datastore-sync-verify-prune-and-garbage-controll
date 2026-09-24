#!/usr/bin/env python3
"""PBS maintenance configured by config.toml; ordered execution, local logs,
MQTT/SMTP notifications and a dry-run that never starts a PBS command.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import shlex
import shutil
import smtplib
import ssl
import socket
import subprocess
import sys
import threading
from copy import deepcopy
from email.message import EmailMessage
from pathlib import Path
from uuid import uuid4
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any

try:
    import paho.mqtt.client as mqtt
except ImportError:
    mqtt = None


try:
    import tomllib
except ImportError:  # Python 3.9/3.10 support without inventing a TOML parser.
    try:
        import tomli as tomllib
    except ImportError:
        tomllib = None


__version__ = "0.0.3"
SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG = {
    "steps": {"sync": True, "verify": True, "prune": True, "gc": True},
    "jobs": {"sync_job": "", "verify_job": "", "gc_datastore": ""},
    "prune": {"mode": "job", "job": "", "datastore": "", "keep_last": "",
              "keep_daily": "", "keep_weekly": "", "keep_monthly": "", "keep_yearly": ""},
    "logging": {"verbose": False},
    "dry_run": {"enabled": True, "send_mqtt": False, "send_email": False},
    "mqtt": {"enabled": True, "host": "", "port": 1883, "topic": "",
             "username": "", "password": "", "tls": False, "cafile": "",
             "insecure": False, "client_id": "", "retain": False,
             "max_output_chars": 4000, "timeout_sec": 15},
    "email": {"enabled": False, "host": "", "port": 587, "security": "starttls",
              "username": "", "password": "", "from_address": "", "to_addresses": [],
              "subject_prefix": "[PBS maintenance]", "cafile": "", "timeout_sec": 15},
}


@dataclass
class CmdResult:
    argv: List[str]
    returncode: int
    stdout: str
    stderr: str


def tail_text(s: str, max_chars: int) -> str:
    s = (s or "").strip()
    if len(s) <= max_chars:
        return s
    return s[-max_chars:]


def _reader_thread(stream, sink_lines: List[str], *, to_stderr: bool, logger: logging.Logger) -> None:
    """
    Read lines from a stream, append to sink_lines, and print live.
    """
    try:
        for line in iter(stream.readline, ""):
            if not line:
                break
            line = line.rstrip("\n")
            sink_lines.append(line)
            if to_stderr:
                print(line, file=sys.stderr)
            else:
                print(line)
            # stderr is a transport stream, not a severity level. Keep progress
            # in the full log; only explicitly labelled errors reach .err.
            level = logging.ERROR if is_error_line(line) else logging.INFO
            logger.log(level, "[%s] %s", "stderr" if to_stderr else "stdout", line,
                       extra={"command_output": True})
    finally:
        try:
            stream.close()
        except Exception:
            pass


def run_cmd_stream(argv: List[str], *, env: Optional[dict], logger: logging.Logger) -> CmdResult:
    """
    Run a command and stream stdout/stderr live to terminal while capturing output.
    Uses threads to avoid deadlocks from pipe buffering.
    """
    logger.info("Running: %s", shlex.join(argv))

    proc = subprocess.Popen(
        argv,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env if env is not None else os.environ.copy(),
        bufsize=1,
        errors="replace",
    )

    assert proc.stdout is not None
    assert proc.stderr is not None

    stdout_lines: List[str] = []
    stderr_lines: List[str] = []

    t_out = threading.Thread(
        target=_reader_thread,
        args=(proc.stdout, stdout_lines),
        kwargs={"to_stderr": False, "logger": logger},
        daemon=True,
    )
    t_err = threading.Thread(
        target=_reader_thread,
        args=(proc.stderr, stderr_lines),
        kwargs={"to_stderr": True, "logger": logger},
        daemon=True,
    )

    t_out.start()
    t_err.start()

    rc = proc.wait()
    t_out.join(timeout=5)
    t_err.join(timeout=5)

    stdout = "\n".join(stdout_lines)
    stderr = "\n".join(stderr_lines)

    if rc != 0:
        logger.error("FAILED (rc=%s): %s", rc, shlex.join(argv))
    else:
        logger.info("OK: %s", shlex.join(argv))

    return CmdResult(argv=argv, returncode=rc, stdout=stdout, stderr=stderr)


def mqtt_publish(
    *,
    host: str,
    port: int,
    topic: str,
    payload: dict,
    username: Optional[str],
    password: Optional[str],
    tls: bool,
    cafile: Optional[str],
    insecure: bool,
    client_id: str,
    retain: bool,
    logger: logging.Logger,
    timeout_sec: int = 15,
) -> None:
    if mqtt is None:
        raise RuntimeError("paho-mqtt not installed. Install with: pip install paho-mqtt")

    # Paho-mqtt 2.x supports callback_api_version; older versions don't.
    try:
        client = mqtt.Client(
            client_id=client_id,
            protocol=mqtt.MQTTv311,
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        )
    except Exception:
        client = mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv311)

    if username:
        client.username_pw_set(username, password=password)

    if tls:
        client.tls_set(ca_certs=cafile if cafile else None)
        if insecure:
            client.tls_insecure_set(True)

    published = {"ok": False}

    def on_publish(_client, _userdata, mid, *args, **kwargs):
        published["ok"] = True
        logger.info("MQTT publish acknowledged (mid=%s)", mid)

    def on_disconnect(_client, _userdata, *args, **kwargs):
        # args may contain rc/reason_code, properties, flags, etc depending on paho version
        reason = None
        if args:
            reason = args[0]
        reason = kwargs.get("reason_code", reason)
        logger.info("MQTT disconnected (reason=%s)", reason)

    try:
        client.on_publish = on_publish
        client.on_disconnect = on_disconnect
    except Exception:
        pass

    logger.info("Connecting MQTT %s:%d ...", host, port)
    client.connect(host, port, keepalive=30)

    client.loop_start()
    try:
        msg = json.dumps(payload, ensure_ascii=False)
        logger.info("Publishing MQTT topic=%s retain=%s", topic, retain)

        info = client.publish(topic, msg, qos=1, retain=retain)

        # Wait for publish (works on most paho versions). Guard with our own timeout via poll.
        try:
            info.wait_for_publish(timeout=timeout_sec)  # type: ignore[arg-type]
        except TypeError:
            pass

        import time

        deadline = time.time() + timeout_sec
        while time.time() < deadline and not info.is_published() and not published["ok"]:
            time.sleep(0.1)

        if not info.is_published() and not published["ok"]:
            raise TimeoutError(f"MQTT publish not acknowledged within {timeout_sec}s")

    finally:
        try:
            client.disconnect()
        finally:
            client.loop_stop()

    logger.info("MQTT published OK")


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


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _manual_prune_keep_dict(args: argparse.Namespace) -> Dict[str, Optional[int]]:
    return {
        "last": args.prune_keep_last,
        "daily": args.prune_keep_daily,
        "weekly": args.prune_keep_weekly,
        "monthly": args.prune_keep_monthly,
        "yearly": args.prune_keep_yearly,
    }


def base_payload(*, hostname: str, steps: Dict[str, bool], args: argparse.Namespace) -> Dict[str, Any]:
    # Represent prune mode in payload (helps HA automations)
    if steps.get("prune"):
        if args.prune_job:
            prune_mode = "prune-job"
        elif args.prune:
            prune_mode = "manual"
        else:
            prune_mode = None
    else:
        prune_mode = None

    return {
        "hostname": hostname,
        "time_utc": utc_now_iso(),
        "steps": steps,
        "sync_job": args.sync_job if steps.get("sync") else None,
        "verify_job": args.verify_job if steps.get("verify") else None,

        # Prune info
        "prune_mode": prune_mode,
        "prune_job": args.prune_job if (steps.get("prune") and args.prune_job) else None,
        "prune_datastore": args.prune_datastore if (steps.get("prune") and args.prune) else None,
        "prune_keep": _manual_prune_keep_dict(args) if (steps.get("prune") and args.prune) else None,

        # GC
        "datastore": args.datastore if steps.get("gc") else None,
    }


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


def _build_manual_prune_argv(args: argparse.Namespace) -> List[str]:
    argv = ["proxmox-backup-manager", "prune", "run", args.prune_datastore]

    if args.prune_keep_last is not None:
        argv += ["--keep-last", str(args.prune_keep_last)]
    if args.prune_keep_daily is not None:
        argv += ["--keep-daily", str(args.prune_keep_daily)]
    if args.prune_keep_weekly is not None:
        argv += ["--keep-weekly", str(args.prune_keep_weekly)]
    if args.prune_keep_monthly is not None:
        argv += ["--keep-monthly", str(args.prune_keep_monthly)]
    if args.prune_keep_yearly is not None:
        argv += ["--keep-yearly", str(args.prune_keep_yearly)]

    return argv


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
    # Resolve optional CA files relative to the config, never to the caller's cwd.
    for channel in ("mqtt", "email"):
        value = config[channel]["cafile"]
        if value:
            candidate = Path(value).expanduser()
            config[channel]["cafile"] = str(candidate if candidate.is_absolute() else path.parent / candidate)
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


def notification_channels(config: Dict[str, Any]) -> Dict[str, bool]:
    """Dry-run opt-ins are independent of normal-run channel enable switches."""
    if config["dry_run"]["enabled"]:
        return {name: config["dry_run"]["send_" + name] for name in ("mqtt", "email")}
    return {name: config[name]["enabled"] for name in ("mqtt", "email")}


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
    for channel in ("mqtt", "email"):
        settings = config[channel]
        if not 1 <= settings["port"] <= 65535 or settings["timeout_sec"] <= 0:
            raise ValueError(f"{channel}.port must be 1..65535 and timeout_sec must be positive.")
        if channels[channel]:
            if not settings["host"].strip():
                raise ValueError(f"{channel}.host is required for the selected notification channel.")
            if settings["password"] and not settings["username"]:
                raise ValueError(f"{channel}.password requires {channel}.username.")
            if settings["cafile"] and not Path(settings["cafile"]).is_file():
                raise ValueError(f"{channel}.cafile does not name an existing certificate file.")
    if config["mqtt"]["max_output_chars"] <= 0:
        raise ValueError("mqtt.max_output_chars must be a positive integer.")
    if channels["mqtt"]:
        if not config["mqtt"]["topic"] or any(c in config["mqtt"]["topic"] for c in ("+", "#", "\x00")):
            raise ValueError("mqtt.topic must be a nonempty publish topic without wildcard/NUL characters.")
        if mqtt is None:
            raise ValueError("MQTT sending requires paho-mqtt; install requirements.txt first.")
    email = config["email"]
    if email["security"] not in ("starttls", "ssl", "none"):
        raise ValueError('email.security must be "starttls", "ssl", or "none".')
    if any(type(item) is not str for item in email["to_addresses"]):
        raise ValueError("email.to_addresses must be an array of address strings.")
    if channels["email"]:
        addresses = [email["from_address"]] + email["to_addresses"]
        if not email["to_addresses"] or any(not a.strip() or "@" not in a or any(c in a for c in "\r\n") for a in addresses):
            raise ValueError("Email requires a from_address and nonempty to_addresses with valid mailbox addresses.")
        if any(c in email["subject_prefix"] for c in "\r\n"):
            raise ValueError("email.subject_prefix must be a single line.")


def build_commands(args: argparse.Namespace, steps: Dict[str, bool]) -> List[tuple]:
    """One plan supplies both dry-run display and actual ordered execution."""
    commands = []
    if steps["sync"]:
        commands.append(("sync", ["proxmox-backup-manager", "sync-job", "run", args.sync_job]))
    if steps["verify"]:
        commands.append(("verify", ["proxmox-backup-manager", "verify-job", "run", args.verify_job]))
    if steps["prune"]:
        argv = (["proxmox-backup-manager", "prune-job", "run", args.prune_job]
                if args.prune_job else _build_manual_prune_argv(args))
        commands.append(("prune", argv))
    if steps["gc"]:
        commands.append(("gc", ["proxmox-backup-manager", "garbage-collection", "start", args.datastore]))
    return commands


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


def send_notifications(config: Dict[str, Any], payload: Dict[str, Any], logger: logging.Logger) -> bool:
    """Attempt each selected channel independently; one failure must not suppress the other."""
    channels = notification_channels(config)
    success = True
    if channels["mqtt"]:
        settings = config["mqtt"]
        try:
            mqtt_publish(
                host=settings["host"], port=settings["port"], topic=settings["topic"], payload=payload,
                username=settings["username"] or None, password=settings["password"] or None,
                tls=settings["tls"], cafile=settings["cafile"] or None, insecure=settings["insecure"],
                client_id=settings["client_id"] or f"pbs-maint-{socket.gethostname()}-{os.getpid()}",
                # Never overwrite a retained live status with a rehearsal event.
                retain=settings["retain"] and not payload["dry_run"], logger=logger,
                timeout_sec=settings["timeout_sec"],
            )
        except Exception as exc:
            logger.error("MQTT notification failed (%s). Check broker, credentials and TLS settings.", type(exc).__name__)
            success = False
    if channels["email"]:
        try:
            email_send(config["email"], payload, logger)
        except Exception as exc:
            logger.error("Email notification failed (%s). Check SMTP, recipients, credentials and TLS settings.", type(exc).__name__)
            success = False
    return success


def run_workflow(config: Dict[str, Any], args: argparse.Namespace, logger: logging.Logger) -> int:
    """Run or preview the validated plan, retaining the original failure-stop semantics."""
    steps = config["steps"]
    commands = build_commands(args, steps)
    payload = base_payload(hostname=socket.gethostname(), steps=steps, args=args)
    payload.update(version=__version__, dry_run=config["dry_run"]["enabled"],
                   log_file=str(logger.log_file), err_file=None)
    if payload["dry_run"]:
        for step, argv in commands:
            logger.info("DRY RUN [%s]: %s", step, shlex.join(argv))
        payload.update(event="pbs_maintenance_dry_run", commands=[argv for _, argv in commands])
        logger.info("Dry run: no PBS commands executed. Notification opt-ins: %s", notification_channels(config))
        return 0 if send_notifications(config, payload, logger) else 1
    for step, argv in commands:
        try:
            result = run_cmd_stream(argv, env=os.environ.copy(), logger=logger)
        except OSError as exc:
            logger.error("Could not start %s (%s).", step, type(exc).__name__)
            result = CmdResult(argv, 127, "", f"Could not start command ({type(exc).__name__}).")
        if result.returncode != 0:
            logger.error("Maintenance failed at %s (rc=%s); later steps skipped.", step, result.returncode)
            payload.update(event="pbs_maintenance_failed", failed_step=step, returncode=result.returncode,
                           command=shlex.join(result.argv),
                           stdout_tail=tail_text(result.stdout, config["mqtt"]["max_output_chars"]),
                           stderr_tail=tail_text(result.stderr, config["mqtt"]["max_output_chars"]),
                           time_utc=utc_now_iso(), err_file=str(logger.err_file))
            send_notifications(config, payload, logger)
            return 1
    payload.update(event="pbs_maintenance_success", time_utc=utc_now_iso(),
                   err_file=str(logger.err_file) if logger.err_file.exists() else None)
    if not send_notifications(config, payload, logger):
        return 1
    logger.info("All selected steps succeeded; selected notifications completed.")
    return 0


def main() -> int:
    """Load script-local configuration and logs; operational settings live in TOML."""
    parser = argparse.ArgumentParser(description="PBS maintenance configured by config.toml beside the script.")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}",
                        help="Show version and exit without configuration, logs or connections.")
    parser.add_argument("--config", type=Path, default=SCRIPT_DIR / "config.toml", metavar="PATH",
                        help="Alternate TOML file (relative to current directory). Default: config.toml beside the script.")
    cli = parser.parse_args()
    try:
        logger = build_logger(False)
    except OSError as exc:
        print(f"ERROR: Cannot create logs beside the script ({type(exc).__name__}); no maintenance started.", file=sys.stderr)
        return 2
    try:
        logger.info("PBS maintenance %s; full log: %s", __version__, logger.log_file)
        try:
            config = load_config(cli.config.expanduser().resolve())
            logger.handlers[0].setLevel(logging.DEBUG if config["logging"]["verbose"] else logging.INFO)
            logger.debug("Configuration loaded from %s; selected steps: %s", cli.config, config["steps"])
            args = config_to_args(config)
            validate_config(config, args)
            if not config["dry_run"]["enabled"] and shutil.which("proxmox-backup-manager") is None:
                raise ValueError("proxmox-backup-manager is not in PATH; real runs require the PBS host.")
        except ValueError as exc:
            logger.error("Configuration/preflight error: %s", exc)
            return 2
        return run_workflow(config, args, logger)
    finally:
        close_logger(logger)


if __name__ == "__main__":
    raise SystemExit(main())
