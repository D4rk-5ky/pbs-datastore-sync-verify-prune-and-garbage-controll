"""Shared PBS command planning, streaming, outcomes and ordered maintenance.

Sync, verify, prune and GC use one execution path for consistent safety behavior.
Transport implementations live in mail.py and mqtt.py.
"""
from __future__ import annotations

import argparse
import logging
import os
import shlex
import socket
import subprocess
import sys
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any

from . import __version__, logging_config, mail, mqtt, settings


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
            level = logging.ERROR if logging_config.is_error_line(line) else logging.INFO
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


def send_notifications(config: Dict[str, Any], payload: Dict[str, Any], logger: logging.Logger) -> bool:
    """Attempt each selected channel independently; one failure must not suppress the other."""
    channels = settings.notification_channels(config)
    success = True
    if channels["mqtt"]:
        mqtt_settings = config["mqtt"]
        try:
            mqtt.mqtt_publish(
                host=mqtt_settings["host"], port=mqtt_settings["port"], topic=mqtt_settings["topic"], payload=payload,
                username=mqtt_settings["username"] or None, password=mqtt_settings["password"] or None,
                tls=mqtt_settings["tls"], cafile=mqtt_settings["cafile"] or None, insecure=mqtt_settings["insecure"],
                client_id=mqtt_settings["client_id"] or f"pbs-maint-{socket.gethostname()}-{os.getpid()}",
                # Never overwrite a retained live status with a rehearsal event.
                retain=mqtt_settings["retain"] and not payload["dry_run"], logger=logger,
                timeout_sec=mqtt_settings["timeout_sec"],
            )
        except Exception as exc:
            logger.error("MQTT notification failed (%s). Check broker, credentials and TLS settings.", type(exc).__name__)
            success = False
    if channels["email"]:
        try:
            mail.email_send(config["email"], payload, logger)
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
        logger.info("Dry run: no PBS commands executed. Notification opt-ins: %s", settings.notification_channels(config))
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

