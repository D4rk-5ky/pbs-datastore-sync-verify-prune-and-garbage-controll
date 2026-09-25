# Commented code map — 0.0.7

This map explains every manually defined application/test function and class, plus every external command the project can invoke. The application keeps the original launcher and five functional modules inside `pbs_maintenance/`.

## Layout and dependency choices

- `pbs-datastore-sync-verify,prune-gc.py` is the only user-facing Python entry point. It parses `-c`/`--config`, help and version, then delegates to package code.
- `pbs_maintenance/__init__.py` contains only `__version__` and `SCRIPT_DIR`, keeping config/log paths anchored beside the launcher.
- `settings.py` owns TOML defaults, strict loading, path resolution, validation and notification-channel selection. It checks Paho availability through `mqtt.py` and local sendmail availability through `mail.py` without running maintenance.
- `maintenance.py` owns the common sync → verify → prune → GC plan, one command executor, event construction and notification coordination.
- `mail.py` implements only local sendmail/Postfix delivery. It does not implement direct SMTP.
- `mqtt.py` implements MQTT publication. `logging_config.py` implements console/full/error logging.
- Importing modules has no runtime side effects: no config load, log creation, child process, PBS work or network connection.

## Application definitions

### `pbs-datastore-sync-verify,prune-gc.py`

| Definition | What it does and why |
| --- | --- |
| `main` | Create the parser, expose `-c` and `--config` as equivalent names for the same `Path` option, expose side-effect-free help/version, create script-local logging, load/validate TOML, enable console debug when configured, require `proxmox-backup-manager` only for real runs, call `maintenance.run_workflow`, and always close handlers. Startup/preflight failures return 2. |

### `pbs_maintenance/__init__.py`

No functions/classes. `__version__` is the package release marker. `SCRIPT_DIR` resolves to the project root so default config and logs remain beside the launcher.

### `pbs_maintenance/logging_config.py`

| Definition | What it does and why |
| --- | --- |
| `ConsoleFilter` | Filter type used to avoid duplicate terminal copies of command-output lines that the pipe readers already print live. |
| `ConsoleFilter.filter` | Reject records marked `command_output` from the console handler while still allowing file handlers to receive them. |
| `PrivateFileHandler` | File-handler subclass used for private per-run logs. |
| `PrivateFileHandler._open` | Open/create log files with mode `0600`; lazy error-file opening means a successful run does not leave an empty `.err`. |
| `is_error_line` | Classify only explicit error prefixes such as `Error:`, `TASK ERROR`, `FATAL`, `CRITICAL`, `FAILED`, `[ERROR]`, `[FATAL]`, `[CRITICAL]`, optionally after an ISO-style timestamp. Ordinary stderr/progress is not automatically treated as an error. |
| `close_logger` | Flush, close and remove handlers so repeated tests/runs do not leak descriptors or duplicate handlers. |
| `build_logger` | Create `logs/` with private permissions, create unique full/error paths, attach console/full/error handlers, and expose `log_file`/`err_file` on the logger for event payloads. |

### `pbs_maintenance/mail.py`

| Definition | What it does and why |
| --- | --- |
| `find_sendmail` | Honor an explicit configured executable first. With no override, check `/usr/sbin/sendmail`, `/usr/bin/sendmail`, then `sendmail` in `PATH`. Return only an executable file/path or `None`. This matches the local sendmail/Postfix pattern used by the related Proxmox backup project. |
| `email_send` | Build an `EmailMessage` containing the JSON outcome payload, add `SUCCESS`, `FAILED` or `DRY RUN` to the configured subject prefix, locate sendmail, and run `[sendmail_bin, "-t"]` with the full RFC message bytes on stdin, `check=True`, and captured output. No shell and no direct SMTP are used. A nonzero sendmail exit is raised as a notification failure; successful handoff is logged. |

### `pbs_maintenance/maintenance.py`

| Definition | What it does and why |
| --- | --- |
| `CmdResult` | Dataclass holding argv, return code, stdout and stderr for every PBS step. |
| `tail_text` | Strip surrounding whitespace and retain at most the final configured number of characters for failure notification fields. |
| `_reader_thread` | Drain one child stdout/stderr pipe, capture each line, print it live to the matching terminal stream, classify explicit errors for logging, and close the stream. |
| `run_cmd_stream` | Launch an argument array without a shell, drain stdout/stderr concurrently to avoid pipe deadlocks, wait for completion, join reader threads, log success/failure and return `CmdResult`. |
| `utc_now_iso` | Produce timezone-aware UTC ISO timestamps for outcome events. |
| `_manual_prune_keep_dict` | Convert the five manual retention arguments into the event payload structure. |
| `base_payload` | Build common host/time/step/job/prune/GC metadata. Disabled targets are represented as `None`; `steps` describes selected steps rather than completed steps. |
| `_build_manual_prune_argv` | Build `proxmox-backup-manager prune run DATASTORE` and append only explicitly supplied retention switches. Zero remains an explicit supplied value. |
| `build_commands` | Build one ordered plan for sync → verify → prune → GC. Dry-run and real execution use the same plan so preview cannot drift from actual command construction. |
| `send_notifications` | Resolve event-specific channels, then independently attempt MQTT and email. Dry-run uses its explicit opt-ins; real failures use the channel master `enabled` switches; real success additionally requires each channel's `on_success = true`. MQTT dry-run is never retained. Email delegates to `mail.email_send(email, sendmail, payload, logger)`. A failure in one channel does not suppress the other; the combined result controls the process exit code. |
| `run_workflow` | Build the plan/payload, perform dry-run logging without PBS execution, or run enabled PBS commands in fixed order. Stop after the first command/launch failure, report that failure, otherwise report success. Notification failure returns 1 without rerunning maintenance. |

### `pbs_maintenance/mqtt.py`

| Definition | What it does and why |
| --- | --- |
| `mqtt_publish` | Publish JSON with Paho MQTT 3.1.1/QoS 1, optional auth/TLS/custom CA/insecure test mode, wait for publish acknowledgement, then stop/disconnect cleanly. |
| `mqtt_publish.on_publish` | Mark the publish as acknowledged and log the message ID. |
| `mqtt_publish.on_disconnect` | Log disconnect information while tolerating callback-signature variation. |

### `pbs_maintenance/settings.py`

| Definition | What it does and why |
| --- | --- |
| `load_config` | Load TOML through `tomllib`/Tomli, reject unknown sections/keys and wrong types, merge documented defaults, resolve `mqtt.cafile` and `sendmail.path` relative to the TOML directory, and avoid echoing malformed TOML source that could contain credentials. |
| `config_to_args` | Adapt TOML values to the existing `argparse.Namespace` shape reused by prune/payload helpers, avoiding duplicate maintenance logic. |
| `_any_keep_set` | Enforce that manual prune has at least one explicitly supplied retention value; `0` counts as supplied. |
| `notification_channels` | Select transport activity by run/event. Dry-run uses independent `dry_run.send_mqtt`/`dry_run.send_email`; validation and real failures use `mqtt.enabled`/`email.enabled`; real success requires both the master `enabled` switch and the corresponding `on_success` switch. This keeps failures active when success notifications are disabled. |
| `validate_config` | Fail closed on no selected steps, missing job/datastore targets, invalid/ambiguous prune settings, invalid MQTT ranges/topics/auth/CA/dependency, invalid email addresses/subject headers, or missing active sendmail executable. Silent dry-run does not require MQTT or sendmail. |

## External commands and guards

| Command | Purpose / guard |
| --- | --- |
| `proxmox-backup-manager sync-job run ID` | First enabled maintenance step; `ID = jobs.sync_job`. |
| `proxmox-backup-manager verify-job run ID` | Second enabled maintenance step; `ID = jobs.verify_job`. |
| `proxmox-backup-manager prune-job run ID` | Job-based prune branch when `prune.mode = "job"`. |
| `proxmox-backup-manager prune run DATASTORE` | Manual prune branch when `prune.mode = "manual"`. |
| `--keep-last N`, `--keep-daily N`, `--keep-weekly N`, `--keep-monthly N`, `--keep-yearly N` | PBS arguments appended only for manually supplied retention values; these are not Python CLI options. |
| `proxmox-backup-manager garbage-collection start DATASTORE` | Final enabled maintenance step; target is `jobs.gc_datastore`. |
| `SENDMAIL_PATH -t` | Email delivery only. Message bytes are supplied on stdin; no shell or SMTP connection is created by the script. Active email preflight requires a usable executable first. |

Both dry-run and real work use `build_commands`; only real work enters `run_cmd_stream`. Any nonzero PBS command stops later maintenance steps. The project still has no rollback, overlap lock, PBS subprocess timeout or separate PBS-task polling.

## Test definitions — `tests/test_maintenance.py`

| Definition | What it verifies / why |
| --- | --- |
| `write_toml` | Serialize the simple test config dictionaries without adding a runtime TOML-writer dependency. |
| `MaintenanceTests` | Container for isolated offline unit/integration tests. |
| `MaintenanceTests.setUp` | Create a private temp project/config/log area and valid baseline config for each test. |
| `MaintenanceTests.run_app` | Exercise real config loading/validation/logging/workflow while mocking only external PBS/MQTT/sendmail effects. |
| `MaintenanceTests.run_app.run` | Record PBS argv and inject deterministic exit/launch failures. |
| `MaintenanceTests.run_app.mqtt_send` | Record MQTT calls and optionally raise a broker-style failure. |
| `MaintenanceTests.run_app.email_send` | Record email payloads and optionally raise a sendmail-style notification failure. |
| `test_fixed_order_and_success` | Verify sync → verify → prune → GC order and success payload/log behavior. |
| `test_each_failure_stops_later_commands` | Verify each possible step failure stops all later steps and reports the failed step/rc/tail. |
| `test_validation_has_no_external_side_effects` | Verify malformed/unsafe config is rejected before PBS or notifications. |
| `test_manual_retention_forwarding_and_payload` | Verify explicit retention guard, manual argv construction and payload representation. |
| `test_single_enabled_step_and_disabled_payload_fields` | Verify a single selected step runs alone and inactive payload targets are null. |
| `test_notification_failures_do_not_suppress_other_channel` | Verify MQTT and email attempts are independent and notification failure returns 1. |
| `test_failure_tail_limit` | Verify configured failure tail truncation. |
| `test_dry_run_is_silent_by_default_and_never_executes` | Verify safe default dry-run logs commands, launches no PBS process and sends nothing unless opted in. |
| `test_dry_run_channels_are_independent_explicit_opt_ins` | Verify all MQTT/email dry-run opt-in combinations and non-retained MQTT rehearsal events. |
| `test_dry_run_notification_errors_and_missing_settings` | Verify dry-run notification failures and invalid email recipients fail safely without PBS execution. |
| `test_real_notifications_can_both_be_disabled` | Verify real maintenance can complete with both notification channels disabled. |
| `test_launch_failure_is_logged_and_notified` | Verify PBS executable launch failure becomes rc 127, creates error log and reports failure. |
| `test_help_and_version_require_no_configuration` | Verify help/version are side-effect free and expose current version/config semantics. |
| `test_local_stream_capture_and_error_only_file` | Verify stdout/stderr capture, live streaming, explicit error classification and separate `.err`. |
| `test_stderr_progress_alone_does_not_create_err` | Verify routine stderr progress is not automatically an error. |
| `test_config_errors_are_logged_without_toml_source` | Verify TOML/preflight errors are logged without leaking malformed source contents. |
| `test_bundled_config_covers_defaults_and_is_safe` | Verify the tracked example exactly represents every default and retains safe dry-run defaults. |
| `test_config_relative_file_paths` | Verify relative `mqtt.cafile` and `sendmail.path` resolve from the config directory. |
| `test_script_local_paths_ignore_caller_working_directory` | Verify default config/log roots stay beside the resolved launcher. |
| `test_log_creation_failure_prevents_work` | Verify inability to create the log directory aborts before maintenance. |
| `test_missing_paho_prevents_real_work_but_not_silent_dry_run` | Verify active MQTT requires Paho while silent dry-run does not. |
| `test_missing_toml_parser_is_actionable` | Verify Python 3.9/3.10 missing Tomli gets an actionable startup error. |
| `test_sendmail_message_and_command` | Verify email headers/body and the exact `[sendmail, "-t"]` subprocess contract with bytes on stdin and no real delivery. |
| `test_sendmail_nonzero_exit_is_an_error` | Verify nonzero sendmail exit becomes an email notification failure. |
| `test_find_sendmail_override_standard_paths_and_path` | Verify explicit executable override and PATH fallback behavior without using system mail. |
| `test_active_email_requires_sendmail` | Verify an enabled email channel fails preflight before maintenance when sendmail cannot be found. |
| `test_active_email_header_validation` | Verify unsafe/invalid From/To/Subject header values are rejected. |
| `test_error_classifier_does_not_match_routine_mentions` | Verify only explicit severity prefixes are treated as error records. |
| `prepare_cli_fixture` | Copy the real launcher/package and create a harmless fake PBS executable for cross-module CLI tests. |
| `test_cli_dry_run_from_other_directory_and_symlink` | Verify real imports, symlink launch, script-root config/log paths and no PBS execution in dry-run. |
| `test_cli_simulated_commands_cross_module_boundaries` | Verify real CLI/package execution with fake PBS and explicitly exercise both `-c` and `--config` as equivalent alternate-config selectors. |
| `test_package_imports_have_no_runtime_side_effects` | Verify package imports cannot create child processes/network activity/logs. |

## User/setup commands and entry points

- `python3 -m venv .venv` — create an isolated Python environment.
- `.venv/bin/python -m pip install -r requirements.txt` — install Paho and conditional Tomli; sendmail/Postfix is a system service/binary, not a Python dependency.
- `test -e config.toml || cp config-example.toml config.toml` — create local config without overwriting an existing file.
- `chmod 600 config.toml` — restrict access to config containing site details/MQTT credentials.
- `.venv/bin/python 'pbs-datastore-sync-verify,prune-gc.py'` — use default config beside launcher.
- `-c PATH` / `--config PATH` — exact aliases selecting an alternate TOML; relative paths use the caller's current working directory.
- `-h` / `--help` — parser help with no config/log/PBS/notification side effects.
- `--version` — print version and exit with no runtime setup.
- `sudo apt install postfix` — example Debian/PBS system installation for a local sendmail-compatible interface; the script does not configure Postfix.
- `command -v sendmail` — inspect which sendmail-compatible executable the environment exposes.
- `python3 -B -m unittest discover -s tests -v` — run offline tests without interpreter bytecode writes.
- Launcher `if __name__ == "__main__"` — convert `main()` return value to process exit code.
- Test `if __name__ == '__main__'` — run unittest directly.

## Release-document scope

`README.md` documents only current use. `config-example.toml` and `config.toml` expose every current option. `VERSIONING.md` records release history. `VERIFICATION.md` records actual checks/limitations. `RELEASE_MANIFEST.json` records prior-path preservation and final hashes. Generated logs, caches, bytecode, environments, build output and temporary files are excluded from the release ZIP.
