# Verification — 0.0.6

Checked on 2026-09-25 in the supplied execution environment. No real PBS datastore maintenance, MQTT broker publication, or real sendmail/Postfix delivery was performed.

## Baseline inspection

- Re-extracted and inspected the complete supplied 0.0.5 project before finalizing 0.0.6 changes: launcher, package modules, TOML files, README, code map, version history, CLI examples, tests, requirements, verification notes, ignore rules, and manifest.
- Re-ran the untouched 0.0.5 offline suite: **30/30 tests pass**.
- Used the supplied 0.0.5 ZIP as the immediate path/content baseline. All 19 project paths are retained in 0.0.6; no production module/file was removed or added.

## 0.0.6 change verification

- Confirm `-c PATH` and `--config PATH` are aliases on the same argparse option and both preserve the existing `Path` type, default config path, current-working-directory resolution for relative alternate paths, and script-local log placement.
- Run the real cross-module CLI fixture once with `-c site.toml` and once with `--config site.toml`; both select the alternate config and preserve command ordering/failure behavior.
- Replace direct SMTP code with local sendmail/Postfix delivery only. `mail.py` now builds an `EmailMessage` and executes `[sendmail_bin, "-t"]` with message bytes on stdin, `check=True`, `capture_output=True`, and no shell.
- Verify sendmail discovery honors an explicit path, otherwise checks `/usr/sbin/sendmail`, `/usr/bin/sendmail`, then `sendmail` in `PATH`. Active email fails preflight if no usable sendmail executable is found; silent dry-run does not require sendmail.
- Verify nonzero sendmail exit becomes a notification failure. No real mail is handed to the host MTA during tests.
- Preserve existing notification selection: real runs use `email.enabled`; dry-run uses the independent `dry_run.send_email`. MQTT behavior/payloads and maintenance ordering remain unchanged.
- Confirm current config schema is **37 settings** and both `config.toml` and `config-example.toml` parse to exactly `settings.DEFAULT_CONFIG`. SMTP host/port/security/login/password/CA/timeout settings are removed; `[sendmail] path` is added. Relative `sendmail.path` resolves from the TOML directory.
- Confirm README documents every current dotted config key and no longer documents direct SMTP as current behavior.
- Confirm the user-provided disclaimer block is byte-for-byte unchanged from the supplied 0.0.5 README.
- Confirm every manually defined application/test function and class is represented in `commented_code_map.md`.
- Confirm no `smtplib`/direct-SMTP implementation remains in current production/config/test files.

## Regression and integration checks

The complete 0.0.6 offline suite passes: **31/31 tests**.

Coverage includes fixed sync → verify → prune → GC order; stop-on-first-command-failure; manual-retention guards/forwarding; disabled-step payloads; dry-run non-execution; independent MQTT/email sends; notification failures; command-launch failure handling; full/error logging; TOML validation; config-relative file paths; Paho dependency checks; sendmail message/argv construction; sendmail nonzero exits; sendmail discovery/preflight; email-header validation; error classification; launcher/symlink path resolution; both `-c` and `--config`; simulated cross-module CLI execution; and import-side-effect prevention.

Additional release-tree checks pass:

- Compile all eight Python source/test files in memory with Python `compile()` so no release bytecode is created.
- Direct `--help` and `--version` checks; help displays `-c, --config PATH`, and version reports `0.0.6`.
- TOML schema/default parity for both bundled config files.
- README config-key coverage for all 37 current settings.
- AST definition inventory versus `commented_code_map.md`.
- Disclaimer equality against 0.0.5.
- Prior-path preservation: 19 prior paths, 19 current paths, zero missing, zero added.
- Generated cache/bytecode cleanup before packaging.

## Package verification

A clean versioned ZIP was built and checked as an archive rather than only as a working tree. The package contains **19 project files** beneath one top-level `pbs-datastore-sync-verify-prune-and-garbage-controll-0.0.6/` directory; all 18 non-manifest files are hash-tracked by `RELEASE_MANIFEST.json`.

Package checks pass for duplicate/unsafe ZIP members, CRC integrity, exact manifest file set, every manifest size/SHA-256, preservation of every 0.0.5 path, and exclusion of `__pycache__`, `.pyc`, `.pyo`, runtime `logs/`, virtual environments, build/dist output and temporary files. A clean extraction compiles all eight Python source/test files in memory, reports version `0.0.6`, displays `-c, --config PATH` in help, and passes the full **31/31** offline tests.

## Limits

Real PBS command compatibility and datastore effects were not tested. No real sync, verify, prune or garbage-collection operation was performed. No real MQTT broker was contacted. Sendmail command behavior is unit-tested with mocked subprocesses, but this environment did not deliver mail through a real Postfix/sendmail installation; local MTA configuration, relay authentication/TLS, queueing and final inbox delivery therefore remain unverified. Production scheduling, concurrency, long-running output, interruption, full-disk behavior and actual PBS-host permissions remain untested.

Existing workflow limitations remain: no overlap lock, PBS subprocess timeout, rollback or independent PBS task polling; captured command output is unbounded in memory; pipe-reader joins use the existing five-second timeout; and unexpected failures can still terminate without an outcome notification. Review README.md and source before production use.
