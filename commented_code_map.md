# Commented code map — 0.0.5

The application uses five functional modules inside `pbs_maintenance/`, a small package initializer, and the original command-line entry point. Functions are moved and reused, not duplicated. This map lists every manually defined application/test function and class.

## Dependency and layout decisions

- `__init__.py` defines `__version__` and `SCRIPT_DIR`. SCRIPT_DIR resolves to the package's parent, keeping config/logs next to the launcher.
- The launcher imports settings, logging configuration and maintenance, then handles argument parsing and startup checks.
- `settings.py` owns DEFAULT_CONFIG, the tomllib/Tomli fallback, config adaptation, retention validation and notification selection. It only depends on the MQTT module for Paho availability; it does not import maintenance, avoiding a circular dependency.
- `maintenance.py` imports settings, logging configuration and the two transports. It owns the ordered plan and shared executor, event metadata and dispatch. It does not implement either transport again.
- `mail.py` and `mqtt.py` are independent protocol implementations. `logging_config.py` is independent of settings and maintenance.
- The planner and manual-retention builder total about 30 lines; sync/verify/GC each need only a tiny command branch. Keeping all four operations together preserves one execution and safety path without four trivial modules.
- `settings.py` is a conventional configuration-module name that remains trackable under the existing `config*.*` ignore rule. Package names avoid shadowing top-level standard-library modules such as email/logging.
- Importing the package or a module does not read config, create logs, launch maintenance, or connect to notification services.

## All application definitions by file

### pbs-datastore-sync-verify,prune-gc.py

| Definition | What it does and why |
| --- | --- |
| `main` | Expose only help/version/config selection. Help explicitly states that operational settings live in TOML, that relative `--config` paths use the caller working directory, that logs remain beside the resolved launcher, and that `--version` has no runtime side effects. After parsing, initialize script-local logs, load/validate config, enable console debug if requested, and require the PBS executable only for real work. Return 2 on startup errors; otherwise delegate to run_workflow and close handlers in finally. |

### pbs_maintenance/__init__.py

Defines package version and root-path constants only; no functions or commands.

### pbs_maintenance/logging_config.py

| Definition | What it does and why |
| --- | --- |
| `ConsoleFilter` | Command lines are already printed live; avoid a second console copy. |
| `ConsoleFilter.filter` | Suppress records already printed by the pipe readers on the console; full/error file handlers still receive them. This prevents duplicate console lines. |
| `PrivateFileHandler` | Create run logs readable/writable only by the running account. |
| `PrivateFileHandler._open` | Create/open the run file with mode 0600 and UTF-8 replacement handling. Lazy opening of the error handler means successful runs do not leave empty error files. |
| `is_error_line` | Recognize explicit error severity prefixes, optionally following an unbracketed ISO timestamp. Do not infer error severity from stderr or arbitrary mentions of errors; unlabelled details remain in the full log. |
| `close_logger` | Close and remove all handlers, flushing logs and avoiding leaked descriptors or duplicate handlers during repeated runs/tests. |
| `build_logger` | Create script-local logs/ with a unique UTC/PID/random filename stem. Attach an INFO/DEBUG console handler, full DEBUG file handler and lazy ERROR-only file handler. Expose log_file/err_file paths for event metadata. Abort setup on file errors. |

### pbs_maintenance/mail.py

| Definition | What it does and why |
| --- | --- |
| `email_send` | Build a plain-text JSON EmailMessage with SUCCESS/FAILED/DRY RUN subject, use SMTP/STARTTLS or SMTP_SSL as configured, validate TLS with the system/custom CA, optionally log in and send to all recipients. Treat partial refusal as failure; use context-manager cleanup and no automatic retry. |

### pbs_maintenance/maintenance.py

| Definition | What it does and why |
| --- | --- |
| `CmdResult` | Dataclass containing argument list, return code, stdout and stderr; one result representation serves all maintenance steps. |
| `tail_text` | Strip outer whitespace, returning up to the final configured number of characters. Shared failure reporting limits both MQTT and email output fields; config validation now requires a positive limit. |
| `_reader_thread` | Drain one stdout/stderr pipe, capture lines, stream them live, classify explicitly labelled errors, and log with stream tags. Ordinary stderr is INFO so progress does not contaminate .err. Always attempts to close the stream. |
| `run_cmd_stream` | Run an argument array without a shell, use two pipe-reader threads to avoid pipe-buffer deadlocks, wait for the child, join each reader for up to five seconds, log outcome, and return CmdResult. Nonzero exit always generates an error summary. Output decoding replaces invalid characters. The workflow handles launch OSError. |
| `utc_now_iso` | Generate an aware UTC ISO timestamp for event consumers, including retained-message freshness checks. |
| `_manual_prune_keep_dict` | Map the five retention fields into the original event format. Reused for both notification channels. |
| `base_payload` | Build shared hostname/time/selection/target/prune metadata. Unselected targets are null and steps describes selection, not completed work. The workflow adds version, dry-run and log paths. |
| `_build_manual_prune_argv` | Reuse the original command builder to append only supplied last/daily/weekly/monthly/yearly counts, in that order. No second builder exists for dry-run. |
| `build_commands` | Build one ordered plan for sync → verify → prune → GC, reusing the manual-prune helper. The same argument arrays are displayed by dry-run or executed by real runs. |
| `send_notifications` | Send the shared event over each selected channel independently and return whether all requested sends completed. Force MQTT retain=false on dry-run. Catch/report transport error types without including credentials or server replies; still attempt email after MQTT failure. Transport work is delegated to mqtt.mqtt_publish and mail.email_send. |
| `run_workflow` | Construct shared metadata and the single command plan. Dry-run logs commands and optionally notifies, then returns without any process execution. Real runs execute in order, turn launch OSError into code 127, stop and report the first failed command, or publish success. Notifications failing produce exit 1 without repeating maintenance. |

### pbs_maintenance/mqtt.py

| Definition | What it does and why |
| --- | --- |
| `mqtt_publish` | Reuse the original MQTT 3.1.1/Paho publisher: optional authentication/TLS, QoS 1 JSON publishing, callback acknowledgment and timeout polling, then disconnect/loop cleanup. Constructor fallback supports older Paho APIs. Config now supplies its existing timeout parameter. |
| `mqtt_publish.on_publish` | Set the acknowledgment flag and log the message ID, supporting the existing publish-wait fallback. |
| `mqtt_publish.on_disconnect` | Log disconnect information while tolerating multiple callback signatures. Its original first-positional-argument interpretation is unchanged and may be imperfect for newer callback APIs. |

### pbs_maintenance/settings.py

| Definition | What it does and why |
| --- | --- |
| `load_config` | Load a binary TOML file through tomllib or Tomli, reject unknown keys/tables and wrong types, merge documented defaults, and resolve CA paths against the config directory. Report parser failures without echoing secret source text. |
| `config_to_args` | Adapt TOML values into the argparse.Namespace shape expected by original retention/payload helpers. Empty retention strings become None; the selected prune mode controls job/manual fields. This avoids rewriting established helpers. |
| `_any_keep_set` | Check that at least one retention value is not None, retaining the original explicit-policy guard. Zero remains a supplied value; PBS determines its semantics. |
| `notification_channels` | Select real-run mqtt.enabled/email.enabled, or independent dry_run.send_mqtt/send_email opt-ins during dry-run. A dry-run never inherits active live notifications implicitly. |
| `validate_config` | Reject empty selection, missing enabled targets, invalid/ambiguous prune mode, missing manual retention, invalid ports/timeouts/tail limits, wrong recipient types and invalid active-channel requirements. Check active MQTT library availability through mqtt.py and supplied CA file existence before maintenance; do not connect to servers. |

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

## Every test definition — tests/test_maintenance.py

Tests import the package under its real name and the original launcher as `pbs_cli`; patches target the modules that own the external effects. Temporary copies exercise real package imports and script-root path resolution.

| Definition | What it verifies / why |
| --- | --- |
| `write_toml` | Serialize these simple test fixtures without a production TOML writer dependency. |
| `MaintenanceTests` | Groups offline regression and CLI integration checks, using isolated temporary directories. |
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
| `MaintenanceTests.prepare_cli_fixture` | Copy the runtime and substitute a harmless PBS executable for CLI tests. |
| `MaintenanceTests.test_cli_dry_run_from_other_directory_and_symlink` | Check real package imports and root-relative config/log paths via both launch paths. |
| `MaintenanceTests.test_cli_simulated_commands_cross_module_boundaries` | Run the real CLI against a fake executable to check success, stop order and file logs. |
| `MaintenanceTests.test_package_imports_have_no_runtime_side_effects` | Import every module in a fresh process with process/network creation forbidden. |

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

README.md describes current use, every TOML option, module responsibilities and keeping the package beside the launcher. config-example.toml contains the full commented defaults; config.toml is the local config, retained in the ZIP and ignored by Git. .gitignore permits the example and every Python module. VERSIONING.md records changes; VERIFICATION.md records test outcomes/limits; RELEASE_MANIFEST.json accounts for original/prior paths and current content hashes. No generated caches or logs are shipped.

Runtime settings and safety behavior remain the same: no PBS command timeout, overlap lock, rollback or independent task polling; full command output stays in memory; reader joins use the existing five-second timeout. Error-only logs identify explicit labels and command failure summaries. Transports are optional and dry-run sends require separate opt-ins.
