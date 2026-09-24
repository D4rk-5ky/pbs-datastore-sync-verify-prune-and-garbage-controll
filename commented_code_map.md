# Commented code map — 0.0.3

Every function/method in the shipped application and test module is listed below. Source names remain unchanged where reused. Maintenance commands, config loading, logging and transport roles are explained separately.

## Application data and modules

- `__version__` and `VERSION`: current release, both 0.0.3.
- `SCRIPT_DIR`: resolved Python script directory; anchors default config and logs even from another working directory.
- `DEFAULT_CONFIG`: the full typed configuration schema and safe defaults. The tracked `config-example.toml` has every setting, with comments; the tests require it to match this schema exactly.
- `CmdResult`: existing dataclass carrying argv, return code, stdout and stderr for reporting. Standard dataclass methods are generated, not manually defined.
- `ConsoleFilter` and `PrivateFileHandler`: small logging extensions to avoid duplicate console output and restrict file permissions.
- Optional `mqtt` import: allows quiet dry-run/help without Paho. Active sending validates availability before maintenance.
- `tomllib` / `tomli` fallback: real TOML parsers; Python 3.9/3.10 needs the conditional dependency in requirements.txt. Help/version remain available without it.
- Standard-library SMTP, TLS and email modules: no external mail library or shell mail command.

## Every application function

| Function | What it does and why |
| --- | --- |
| `tail_text` | Strip outer whitespace, returning up to the final configured number of characters. Shared failure reporting limits both MQTT and email output fields; config validation now requires a positive limit. |
| `_reader_thread` | Drain one stdout/stderr pipe, capture lines, stream them live, classify explicitly labelled errors, and log with stream tags. Ordinary stderr is INFO so progress does not contaminate .err. Always attempts to close the stream. |
| `run_cmd_stream` | Run an argument array without a shell, use two pipe-reader threads to avoid pipe-buffer deadlocks, wait for the child, join each reader for up to five seconds, log outcome, and return CmdResult. Nonzero exit always generates an error summary. Output decoding replaces invalid characters. The workflow handles launch OSError. |
| `mqtt_publish` | Reuse the original MQTT 3.1.1/Paho publisher: optional authentication/TLS, QoS 1 JSON publishing, callback acknowledgment and timeout polling, then disconnect/loop cleanup. Constructor fallback supports older Paho APIs. Config now supplies its existing timeout parameter. |
| `mqtt_publish.on_publish` | Set the acknowledgment flag and log the message ID, supporting the existing publish-wait fallback. |
| `mqtt_publish.on_disconnect` | Log disconnect information while tolerating multiple callback signatures. Its original first-positional-argument interpretation is unchanged and may be imperfect for newer callback APIs. |
| `ConsoleFilter.filter` | Suppress records already printed by the pipe readers on the console; full/error file handlers still receive them. This prevents duplicate console lines. |
| `PrivateFileHandler._open` | Create/open the run file with mode 0600 and UTF-8 replacement handling. Lazy opening of the error handler means successful runs do not leave empty error files. |
| `is_error_line` | Recognize explicit error severity prefixes, optionally following an unbracketed ISO timestamp. Do not infer error severity from stderr or arbitrary mentions of errors; unlabelled details remain in the full log. |
| `close_logger` | Close and remove all handlers, flushing logs and avoiding leaked descriptors or duplicate handlers during repeated runs/tests. |
| `build_logger` | Create script-local logs/ with a unique UTC/PID/random filename stem. Attach an INFO/DEBUG console handler, full DEBUG file handler and lazy ERROR-only file handler. Expose log_file/err_file paths for event metadata. Abort setup on file errors. |
| `utc_now_iso` | Generate an aware UTC ISO timestamp for event consumers, including retained-message freshness checks. |
| `_manual_prune_keep_dict` | Map the five retention fields into the original event format. Reused for both notification channels. |
| `base_payload` | Build shared hostname/time/selection/target/prune metadata. Unselected targets are null and steps describes selection, not completed work. The workflow adds version, dry-run and log paths. |
| `_any_keep_set` | Check that at least one retention value is not None, retaining the original explicit-policy guard. Zero remains a supplied value; PBS determines its semantics. |
| `_build_manual_prune_argv` | Reuse the original command builder to append only supplied last/daily/weekly/monthly/yearly counts, in that order. No second builder exists for dry-run. |
| `load_config` | Load a binary TOML file through tomllib or Tomli, reject unknown keys/tables and wrong types, merge documented defaults, and resolve CA paths against the config directory. Report parser failures without echoing secret source text. |
| `config_to_args` | Adapt TOML values into the argparse.Namespace shape expected by original retention/payload helpers. Empty retention strings become None; the selected prune mode controls job/manual fields. This avoids rewriting established helpers. |
| `notification_channels` | Select real-run mqtt.enabled/email.enabled, or independent dry_run.send_mqtt/send_email opt-ins during dry-run. A dry-run never inherits active live notifications implicitly. |
| `validate_config` | Reject empty selection, missing enabled targets, invalid/ambiguous prune mode, missing manual retention, invalid ports/timeouts/tail limits, wrong recipient types and invalid active-channel requirements. Check active MQTT library availability and supplied CA file existence before maintenance; do not connect to servers. |
| `build_commands` | Build one ordered plan for sync → verify → prune → GC, reusing the manual-prune helper. The same argument arrays are displayed by dry-run or executed by real runs. |
| `email_send` | Build a plain-text JSON EmailMessage with SUCCESS/FAILED/DRY RUN subject, use SMTP/STARTTLS or SMTP_SSL as configured, validate TLS with the system/custom CA, optionally log in and send to all recipients. Treat partial refusal as failure; use context-manager cleanup and no automatic retry. |
| `send_notifications` | Send the shared event over each selected channel independently and return whether all requested sends completed. Force MQTT retain=false on dry-run. Catch/report transport error types without including credentials or server replies; still attempt email after MQTT failure. |
| `run_workflow` | Construct shared metadata and the single command plan. Dry-run logs commands and optionally notifies, then returns without any process execution. Real runs execute in order, turn launch OSError into code 127, stop and report the first failed command, or publish success. Notifications failing produce exit 1 without repeating maintenance. |
| `main` | Expose only help/version/config selection, initialize script-local logs, load/validate config, enable console debug if requested, and require the PBS executable only for real work. Return 2 on startup errors; otherwise delegate to run_workflow and close handlers in finally. |

## PBS commands and execution guards

| Command | Purpose and selection |
| --- | --- |
| `proxmox-backup-manager sync-job run ID` | First enabled step; ID is jobs.sync_job and the policy comes from PBS. |
| `proxmox-backup-manager verify-job run ID` | Next enabled step; ID is jobs.verify_job. |
| `proxmox-backup-manager prune-job run ID` | Configured prune branch; ID is prune.job when prune.mode is job. |
| `proxmox-backup-manager prune run DATASTORE` | Manual prune branch; target is prune.datastore, not the GC target. |
| `--keep-last N`, `--keep-daily N`, `--keep-weekly N`, `--keep-monthly N`, `--keep-yearly N` | Append only supplied manual retention values. PBS interprets them; they are not Python CLI flags. |
| `proxmox-backup-manager garbage-collection start DATASTORE` | Final enabled step; target is jobs.gc_datastore. |

Both preview and real work use build_commands. Only the real-work branch invokes run_cmd_stream; dry-run returns before reaching that loop. Nonzero exit stops later steps. Notification transports do not call maintenance. Config/active-dependency validation happens before either kind of work. Subprocesses receive argument lists with no shell; shlex.join is only for readable logs/event fields.

## Every test function

Tests create isolated temporary directories, replace all workflow PBS/MQTT/SMTP effects, and use actual harmless local Python children only for stream capture. No test connects to a broker or mail server.

| Function | What it verifies / why |
| --- | --- |
| `write_toml` | Serialize these simple test fixtures without a production TOML writer dependency. |
| `MaintenanceTests.setUp` | Give every test a private script/config/log directory. |
| `MaintenanceTests.run_app` | Run actual config loading/validation/logging with only external effects mocked. |
| `MaintenanceTests.run_app.run` | Record command order and inject controlled nonzero or launch failures instead of executing PBS. |
| `MaintenanceTests.run_app.mqtt_send` | Record MQTT events and optionally simulate broker failure without a connection. |
| `MaintenanceTests.run_app.email_send` | Record email events and optionally simulate SMTP failure without sending mail. |
| `MaintenanceTests.test_fixed_order_and_success` | Verify fixed order and success, preserving the corresponding behavior without production side effects. |
| `MaintenanceTests.test_each_failure_stops_later_commands` | Verify each failure stops later commands, preserving the corresponding behavior without production side effects. |
| `MaintenanceTests.test_validation_has_no_external_side_effects` | Verify validation has no external side effects, preserving the corresponding behavior without production side effects. |
| `MaintenanceTests.test_manual_retention_forwarding_and_payload` | Verify manual retention forwarding and payload, preserving the corresponding behavior without production side effects. |
| `MaintenanceTests.test_single_enabled_step_and_disabled_payload_fields` | Verify single enabled step and disabled payload fields, preserving the corresponding behavior without production side effects. |
| `MaintenanceTests.test_notification_failures_do_not_suppress_other_channel` | Verify notification failures do not suppress other channel, preserving the corresponding behavior without production side effects. |
| `MaintenanceTests.test_failure_tail_limit` | Verify failure tail limit, preserving the corresponding behavior without production side effects. |
| `MaintenanceTests.test_dry_run_is_silent_by_default_and_never_executes` | Verify dry run is silent by default and never executes, preserving the corresponding behavior without production side effects. |
| `MaintenanceTests.test_dry_run_channels_are_independent_explicit_opt_ins` | Verify dry run channels are independent explicit opt ins, preserving the corresponding behavior without production side effects. |
| `MaintenanceTests.test_dry_run_notification_errors_and_missing_settings` | Verify dry run notification errors and missing settings, preserving the corresponding behavior without production side effects. |
| `MaintenanceTests.test_real_notifications_can_both_be_disabled` | Verify real notifications can both be disabled, preserving the corresponding behavior without production side effects. |
| `MaintenanceTests.test_launch_failure_is_logged_and_notified` | Verify launch failure is logged and notified, preserving the corresponding behavior without production side effects. |
| `MaintenanceTests.test_help_and_version_require_no_configuration` | Verify help and version require no configuration, preserving the corresponding behavior without production side effects. |
| `MaintenanceTests.test_local_stream_capture_and_error_only_file` | Verify local stream capture and error only file, preserving the corresponding behavior without production side effects. |
| `MaintenanceTests.test_stderr_progress_alone_does_not_create_err` | Verify stderr progress alone does not create err, preserving the corresponding behavior without production side effects. |
| `MaintenanceTests.test_config_errors_are_logged_without_toml_source` | Verify config errors are logged without toml source, preserving the corresponding behavior without production side effects. |
| `MaintenanceTests.test_bundled_config_covers_defaults_and_is_safe` | Load the tracked config-example.toml and verify its complete schema/defaults and safe dry-run settings; this also works in Git checkouts without a local config.toml. |
| `MaintenanceTests.test_config_relative_certificate_paths` | Verify config relative certificate paths, preserving the corresponding behavior without production side effects. |
| `MaintenanceTests.test_script_local_paths_ignore_caller_working_directory` | Verify script local paths ignore caller working directory, preserving the corresponding behavior without production side effects. |
| `MaintenanceTests.test_log_creation_failure_prevents_work` | Verify log creation failure prevents work, preserving the corresponding behavior without production side effects. |
| `MaintenanceTests.test_missing_paho_prevents_real_work_but_not_silent_dry_run` | Verify missing paho prevents real work but not silent dry run, preserving the corresponding behavior without production side effects. |
| `MaintenanceTests.test_missing_toml_parser_is_actionable` | Verify missing toml parser is actionable, preserving the corresponding behavior without production side effects. |
| `MaintenanceTests.test_smtp_modes_authentication_and_subject` | Verify smtp modes authentication and subject, preserving the corresponding behavior without production side effects. |
| `MaintenanceTests.test_smtp_partial_refusal_is_an_error` | Verify smtp partial refusal is an error, preserving the corresponding behavior without production side effects. |
| `MaintenanceTests.test_smtp_tls_failure_does_not_send_or_fallback` | Verify smtp tls failure does not send or fallback, preserving the corresponding behavior without production side effects. |
| `MaintenanceTests.test_active_email_header_validation` | Verify active email header validation, preserving the corresponding behavior without production side effects. |
| `MaintenanceTests.test_error_classifier_does_not_match_routine_mentions` | Verify error classifier does not match routine mentions, preserving the corresponding behavior without production side effects. |

## User/setup commands and entry points

- `python3 -m venv .venv`: create an isolated installation environment.
- `.venv/bin/python -m pip install -r requirements.txt`: install MQTT support and conditional TOML parsing dependency.
- `test -e config.toml || cp config-example.toml config.toml`: create a local config only if none exists.
- `git rm --cached -- config.toml`: untrack an already tracked config without deleting the local file; does not erase history.
- `chmod 600 config.toml`: restrict credential-bearing config access.
- `.venv/bin/python 'pbs-datastore-sync-verify,prune-gc.py'`: load script-local config and perform the selected workflow.
- `--config PATH`: select another TOML file; a relative path uses cwd. Config remains the source of all operational settings.
- `-h`/`--help` and `--version`: information-only parser exits before config/log setup.
- `python3 -B -m unittest discover -s tests -v`: run offline tests without bytecode writes.
- Application `__main__` guard: return main's result as the process exit code; importing does not run maintenance.
- Test `__main__` guard: invoke unittest for direct execution.

## Release documentation and scope

`README.md` covers current use and every config setting; `config-example.toml` contains the full annotated defaults; `config.toml` is the ignored local config; `.gitignore` excludes `config*.*` except `config-example.toml`; `examples/cli-options.txt` covers the remaining selection/information commands. `VERSIONING.md` records all changes, `VERIFICATION.md` states checks/limits, and `RELEASE_MANIFEST.json` accounts for original and prior-release paths and hashes. Packaging tools and generated logs/caches stay outside the deliverable.

Existing limits include no PBS command timeout/overlap lock/task polling, unbounded in-memory capture, five-second reader joins and version-dependent MQTT callback behavior. The error-only log recognizes labels, not arbitrary text semantics. See README for operational implications.
