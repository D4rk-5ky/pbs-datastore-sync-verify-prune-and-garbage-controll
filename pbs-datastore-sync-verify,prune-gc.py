#!/usr/bin/env python3
"""
pbs_sync_verifyjob_gc_mqtt.py

Runs selected PBS maintenance steps in this order:
  1) sync-job run
  2) verify-job run
  3) prune  (either manual prune with retention OR prune-job run)
  4) garbage-collection start

Prune behavior:
  - If you pass --prune ... -> runs manual prune using keep-* retention flags
  - If you pass --prune-job <id> -> runs the configured PBS prune job instead
  - If prune step is enabled (default) you MUST choose exactly one of the above.

Streams stdout/stderr live to terminal while also capturing output.

Publishes MQTT on:
  - success (all selected steps succeeded)
  - failure (first selected step that fails), including which step failed and rc/output.

Requirements:
  - Run on PBS server (proxmox-backup-manager available)
  - pip install paho-mqtt
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import socket
import subprocess
import sys
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any

try:
    import paho.mqtt.client as mqtt
except ImportError:
    mqtt = None


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
            logger.debug(line)
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
    logger.info("Running: %s", " ".join(argv))

    proc = subprocess.Popen(
        argv,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env if env is not None else os.environ.copy(),
        bufsize=1,
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
        logger.error("FAILED (rc=%s): %s", rc, " ".join(argv))
    else:
        logger.info("OK: %s", " ".join(argv))

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


def build_logger(verbose: bool) -> logging.Logger:
    logger = logging.getLogger("pbs_sync_verifyjob_gc_mqtt")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    logger.propagate = False

    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG if verbose else logging.INFO)
    ch.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(ch)

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


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Run selected PBS steps (sync/verify/prune/gc); publish MQTT on success or failure."
    )

    # Steps selection (order is fixed: sync -> verify -> prune -> gc)
    ap.add_argument("--no-sync", action="store_true", help="Skip sync-job step.")
    ap.add_argument("--no-verify", action="store_true", help="Skip verify-job step.")
    ap.add_argument("--no-prune", action="store_true", help="Skip prune step.")
    ap.add_argument("--no-gc", action="store_true", help="Skip garbage-collection step.")

    # IDs / datastore (only required if their step is enabled)
    ap.add_argument("--sync-job", default=None, help="PBS sync job ID (e.g. s-xxxx...).")
    ap.add_argument("--verify-job", default=None, help="PBS verify job ID (e.g. v-xxxx...).")
    ap.add_argument("--datastore", default=None, help="Datastore name for garbage-collection start.")

    # Prune mode:
    #   --prune (manual prune + keep-* retention)
    #   --prune-job <id> (run PBS prune job)
    prune_mode = ap.add_mutually_exclusive_group(required=False)
    prune_mode.add_argument(
        "--prune",
        action="store_true",
        help="Use manual prune with keep-* retention (requires --prune-datastore + at least one keep-*).",
    )
    prune_mode.add_argument(
        "--prune-job",
        default=None,
        help="Use PBS prune-job instead of manual retention (e.g. p-xxxx...).",
    )

    # Manual prune settings (used only with --prune)
    ap.add_argument("--prune-datastore", default=None, help="Datastore name to prune (manual prune).")
    ap.add_argument("--prune-keep-last", type=int, default=None, help="(manual prune) Keep last N backups.")
    ap.add_argument("--prune-keep-daily", type=int, default=None, help="(manual prune) Keep daily N backups.")
    ap.add_argument("--prune-keep-weekly", type=int, default=None, help="(manual prune) Keep weekly N backups.")
    ap.add_argument("--prune-keep-monthly", type=int, default=None, help="(manual prune) Keep monthly N backups.")
    ap.add_argument("--prune-keep-yearly", type=int, default=None, help="(manual prune) Keep yearly N backups.")

    # MQTT
    ap.add_argument("--mqtt-host", required=True)
    ap.add_argument("--mqtt-port", type=int, default=1883)
    ap.add_argument("--mqtt-topic", required=True)
    ap.add_argument("--mqtt-username", default=None)
    ap.add_argument("--mqtt-password", default=None)
    ap.add_argument("--mqtt-tls", action="store_true")
    ap.add_argument("--mqtt-cafile", default=None)
    ap.add_argument("--mqtt-insecure", action="store_true")
    ap.add_argument("--mqtt-client-id", default=None)
    ap.add_argument("--mqtt-retain", action="store_true", help="Publish MQTT message with retain=true.")

    # Payload sizing for failures
    ap.add_argument(
        "--mqtt-max-output-chars",
        type=int,
        default=4000,
        help="Max chars to include from stdout/stderr in failure payload (tail).",
    )

    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    logger = build_logger(args.verbose)
    env = os.environ.copy()

    run_sync = not args.no_sync
    run_verify = not args.no_verify
    run_prune = not args.no_prune
    run_gc = not args.no_gc

    steps = {"sync": run_sync, "verify": run_verify, "prune": run_prune, "gc": run_gc}

    if not any(steps.values()):
        logger.error("Nothing to do: you disabled sync, verify, prune, and gc.")
        return 2

    # Validate required args based on enabled steps
    if run_sync and not args.sync_job:
        logger.error("Missing --sync-job (required unless you pass --no-sync)")
        return 2
    if run_verify and not args.verify_job:
        logger.error("Missing --verify-job (required unless you pass --no-verify)")
        return 2
    if run_gc and not args.datastore:
        logger.error("Missing --datastore (required unless you pass --no-gc)")
        return 2

    # Prune validation (only if prune step enabled)
    if run_prune:
        # Must choose exactly one prune mode: manual (--prune) OR prune-job (--prune-job)
        if bool(args.prune) == bool(args.prune_job):
            logger.error(
                "Prune enabled, but prune mode is invalid. Choose exactly one: "
                "--prune (manual retention) OR --prune-job <id>."
            )
            return 2

        if args.prune:
            if not args.prune_datastore:
                logger.error("Manual prune selected: missing --prune-datastore")
                return 2
            if not _any_keep_set(args):
                logger.error(
                    "Manual prune selected but no retention set. Provide at least one of: "
                    "--prune-keep-last/--prune-keep-daily/--prune-keep-weekly/--prune-keep-monthly/--prune-keep-yearly"
                )
                return 2
        else:
            # prune-job mode
            if not args.prune_job:
                logger.error("Prune-job selected: missing --prune-job <id>")
                return 2

    hostname = socket.gethostname()
    client_id = args.mqtt_client_id or f"pbs-maint-{hostname}-{os.getpid()}"

    def publish_event(payload: Dict[str, Any]) -> None:
        mqtt_publish(
            host=args.mqtt_host,
            port=args.mqtt_port,
            topic=args.mqtt_topic,
            payload=payload,
            username=args.mqtt_username,
            password=args.mqtt_password,
            tls=args.mqtt_tls,
            cafile=args.mqtt_cafile,
            insecure=args.mqtt_insecure,
            client_id=client_id,
            retain=args.mqtt_retain,
            logger=logger,
        )

    def publish_failure(step_name: str, result: CmdResult) -> None:
        payload = base_payload(hostname=hostname, steps=steps, args=args)
        payload.update(
            {
                "event": "pbs_maintenance_failed",
                "failed_step": step_name,
                "returncode": result.returncode,
                "command": " ".join(result.argv),
                "stdout_tail": tail_text(result.stdout, args.mqtt_max_output_chars),
                "stderr_tail": tail_text(result.stderr, args.mqtt_max_output_chars),
            }
        )
        try:
            publish_event(payload)
        except Exception as e:
            logger.error("Failed to publish MQTT failure message: %s", e)

    # Execute selected steps in order; on first failure publish MQTT failure and exit.

    if run_sync:
        r = run_cmd_stream(
            ["proxmox-backup-manager", "sync-job", "run", args.sync_job],
            env=env,
            logger=logger,
        )
        if r.returncode != 0:
            publish_failure("sync", r)
            return 1

    if run_verify:
        r = run_cmd_stream(
            ["proxmox-backup-manager", "verify-job", "run", args.verify_job],
            env=env,
            logger=logger,
        )
        if r.returncode != 0:
            publish_failure("verify", r)
            return 1

    if run_prune:
        if args.prune_job:
            # Prune job mode
            prune_argv = ["proxmox-backup-manager", "prune-job", "run", args.prune_job]
        else:
            # Manual retention mode
            prune_argv = _build_manual_prune_argv(args)

        r = run_cmd_stream(prune_argv, env=env, logger=logger)
        if r.returncode != 0:
            publish_failure("prune", r)
            return 1

    if run_gc:
        r = run_cmd_stream(
            ["proxmox-backup-manager", "garbage-collection", "start", args.datastore],
            env=env,
            logger=logger,
        )
        if r.returncode != 0:
            publish_failure("gc", r)
            return 1

    # Success MQTT
    payload = base_payload(hostname=hostname, steps=steps, args=args)
    payload.update({"event": "pbs_maintenance_success"})

    try:
        publish_event(payload)
    except Exception as e:
        logger.error("All selected steps succeeded, but MQTT publish failed: %s", e)
        return 1

    logger.info("All selected steps succeeded; MQTT sent.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
