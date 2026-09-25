#!/usr/bin/env python3
"""Command-line entry point; implementation lives in the pbs_maintenance package."""
from __future__ import annotations

import argparse
import logging
import shutil
import sys
from pathlib import Path

from pbs_maintenance import SCRIPT_DIR, __version__, logging_config, maintenance, settings


def main() -> int:
    """Load script-local configuration and logs; operational settings live in TOML."""
    parser = argparse.ArgumentParser(description="PBS maintenance configured by config.toml beside the script.")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}",
                        help="Show version and exit without configuration, logs or connections.")
    parser.add_argument("--config", type=Path, default=SCRIPT_DIR / "config.toml", metavar="PATH",
                        help="Alternate TOML file (relative to current directory). Default: config.toml beside the script.")
    cli = parser.parse_args()
    try:
        logger = logging_config.build_logger(False)
    except OSError as exc:
        print(f"ERROR: Cannot create logs beside the script ({type(exc).__name__}); no maintenance started.", file=sys.stderr)
        return 2
    try:
        logger.info("PBS maintenance %s; full log: %s", __version__, logger.log_file)
        try:
            config = settings.load_config(cli.config.expanduser().resolve())
            logger.handlers[0].setLevel(logging.DEBUG if config["logging"]["verbose"] else logging.INFO)
            logger.debug("Configuration loaded from %s; selected steps: %s", cli.config, config["steps"])
            args = settings.config_to_args(config)
            settings.validate_config(config, args)
            if not config["dry_run"]["enabled"] and shutil.which("proxmox-backup-manager") is None:
                raise ValueError("proxmox-backup-manager is not in PATH; real runs require the PBS host.")
        except ValueError as exc:
            logger.error("Configuration/preflight error: %s", exc)
            return 2
        return maintenance.run_workflow(config, args, logger)
    finally:
        logging_config.close_logger(logger)


if __name__ == "__main__":
    raise SystemExit(main())
