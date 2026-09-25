# PBS datastore maintenance

Version **0.0.6**. Configure PBS sync, verification, pruning and garbage collection in `config.toml`; run the Python script without operational flags. Each run writes local logs. MQTT and local sendmail/Postfix can report outcomes, including explicitly requested dry-run notifications.

## ⚠️ Disclaimer / Liability



**Use this script at your own risk.**

The author takes **no responsibility or liability** for any data loss, service disruption, misconfiguration, service outage, missed backups, credential exposure, or other damage that may occur from using this script.

Before running it in production, you **must**:

- Read the entire source code
- Understand exactly what it does (and what it does *not* do)
- Review and adapt it to your own environment
- Test it carefully in a non‑production setup

By using this script, **you accept full responsibility** for its effects.

⚠️ AI-assisted / vibe-coded experimental software. Use at your own risk.

## Disclaimer



This project is AI-assisted / vibe-coded software created as a hobby project. It has not been professionally audited and may contain bugs, unsafe behavior, data-loss issues, security problems, or incorrect assumptions.

You are responsible for reviewing the code, testing it in a safe environment, making backups, and understanding what it does before using it on real data. The author is not responsible for damage, data loss, broken systems, security issues, or other problems caused by using this software.

---

## Install and start

Use Python **3.9+**. Real maintenance runs require `proxmox-backup-manager` in PATH on the **PBS server**, and an account allowed to perform the selected operations. A dry run can run on a machine without PBS installed.

Keep the complete extracted project together, including the `pbs_maintenance/` folder beside the original Python script. Copying the launcher alone is not sufficient.

From the extracted project directory:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
test -e config.toml || cp config-example.toml config.toml
chmod 600 config.toml
.venv/bin/python 'pbs-datastore-sync-verify,prune-gc.py' --help
```

The first command creates an isolated environment. The second installs Paho for MQTT and, on Python 3.9/3.10, Tomli for TOML parsing; Python 3.11+ uses built-in `tomllib`. Email uses the host's local sendmail-compatible interface rather than direct SMTP. On Debian/PBS, Postfix commonly provides `/usr/sbin/sendmail`; install/configure a local mail transfer agent separately if you want email notifications. The `test`/`cp` command creates `config.toml` from the example only when it is absent, preserving an existing configuration. `chmod` restricts the config because it can contain MQTT credentials. Help explains the few informational/selection flags and does not create logs or connect to anything.

1. Copy **[config-example.toml](config-example.toml)** to `config.toml` if needed, then edit your local `config.toml`. Fill in the IDs/targets for enabled steps. Every setting has comments, defaults and relevant examples.
2. Leave `[dry_run] enabled = true`, `send_mqtt = false`, and `send_email = false`. The supplied file deliberately leaves your job IDs/targets empty: it will report a configuration error until you fill them in.
3. Run the script, inspect the planned commands in the terminal and `logs/`, and review the selected PBS jobs and retention policy.
4. To test notifications, configure the desired transport and explicitly enable its dry-run send setting.
5. To perform actual maintenance, set `[dry_run] enabled = false` and configure or disable real-run MQTT/email as appropriate. Run the same command.

```bash
.venv/bin/python 'pbs-datastore-sync-verify,prune-gc.py'
```

The default config and log folder are located beside the **resolved script file**, regardless of the working directory or a symlink used to launch it. The script directory must be writable to create logs. There is no fallback to another log directory if this fails; maintenance does not start.

## Git configuration files

`.gitignore` ignores `config*.*` files at any directory level, with an exception for **`config-example.toml`** so the complete commented defaults remain available in Git. Store your local settings and credentials in `config.toml`; keep the example free of real credentials.

The release ZIP still includes the original `config.toml` with blank site-specific values. A Git checkout can use the copy command above to create its ignored local config from the example. The application continues to load `config.toml` by default.

Ignore rules do not affect files already tracked by Git. If your repository already tracks `config.toml`, run `git rm --cached -- config.toml` to remove it from the index while retaining your local file, then commit that change. This does not remove it from past commits.

## Commands

| Invocation | What it does |
| --- | --- |
| `python3 'pbs-datastore-sync-verify,prune-gc.py'` | Load `config.toml` beside the script, create logs, validate, then preview or execute the selected plan. Use `.venv/bin/python` when installed in the environment above. |
| `python3 'pbs-datastore-sync-verify,prune-gc.py' -c /path/to/site.toml` or `--config /path/to/site.toml` | Load an alternate TOML file. `-c` and `--config` are exact equivalents. A relative config path is relative to the caller's current directory. Logs still go beside the script. |
| `python3 'pbs-datastore-sync-verify,prune-gc.py' -h` or `--help` | Print all available CLI flags and path/side-effect notes, then exit without loading config, creating logs, running PBS commands, or connecting. |
| `python3 'pbs-datastore-sync-verify,prune-gc.py' --version` | Print version and exit without loading config, creating logs, or connecting. |
| `python3 -B -m unittest discover -s tests -v` | Run offline tests. `-B` avoids bytecode writes and `-v` lists tests. |

All operational options, including dry-run and verbose logging, live in TOML. There are no positional arguments or operational flag overrides. [examples/cli-options.txt](examples/cli-options.txt) lists the current command interface.

## Config format and complete option reference

TOML strings are quoted, booleans are `true`/`false`, integers are unquoted, and recipient lists use `["a@example.com", "b@example.com"]`. TOML has no null: the retention settings use `""` for unset. Use `7`, not `"7"`, for a retention count. Unknown tables/keys, wrong types and invalid numeric ranges are rejected before maintenance or notifications. Omitted settings take the defaults below; notably, omitting `dry_run.enabled` selects **dry-run**.

Relative `mqtt.cafile` and configured `sendmail.path` values are resolved against the **config file's directory**. Secrets are not passed to PBS commands or dumped to logs. Protect the config and logs because they can contain MQTT credentials or backup metadata. The app does not provide environment-variable substitution or secret-file references.

### Steps, jobs and pruning

All four steps default to enabled. At least one must remain enabled, also in dry-run.

| Setting | Default | Meaning / example |
| --- | --- | --- |
| `steps.sync` | `true` | Run sync first; `false` skips it. Requires `jobs.sync_job` when enabled. |
| `steps.verify` | `true` | Run verification next; `false` skips it. Requires `jobs.verify_job`. |
| `steps.prune` | `true` | Run the selected prune mode; `false` skips it and its target/retention requirements. |
| `steps.gc` | `true` | Start garbage collection last; `false` skips it. Requires `jobs.gc_datastore`. |
| `jobs.sync_job` | `""` | Existing PBS sync job ID, e.g. `"sync-nightly"`. |
| `jobs.verify_job` | `""` | Existing PBS verify job ID, e.g. `"verify-nightly"`. |
| `jobs.gc_datastore` | `""` | GC datastore, e.g. `"backup-store"`; independent of prune targets. |
| `prune.mode` | `"job"` | Exactly `"job"` or `"manual"`. Job mode runs an existing policy; manual mode uses the counts below. |
| `prune.job` | `""` | Existing prune job ID, e.g. `"prune-nightly"`; required for enabled job mode. Leave empty for enabled manual mode. |
| `prune.datastore` | `""` | Required manual target, e.g. `"backup-store"`. Ignored in job mode; never inferred from the GC target. |
| `prune.keep_last` | `""` | Manual latest-backup retention, forwarded as `--keep-last N`. Example: `7`. |
| `prune.keep_daily` | `""` | Manual daily bucket count via `--keep-daily N`. Example: `14`. |
| `prune.keep_weekly` | `""` | Manual weekly bucket count via `--keep-weekly N`. Example: `8`. |
| `prune.keep_monthly` | `""` | Manual monthly bucket count via `--keep-monthly N`. Example: `12`. |
| `prune.keep_yearly` | `""` | Manual yearly bucket count via `--keep-yearly N`. Example: `3`. |

Manual mode requires a datastore and **at least one supplied retention integer**. Counts must be nonnegative; zero is explicitly passed through, while `""` omits the rule. This preserves the explicit-retention guard, but it is not a guarantee that a chosen policy is safe. PBS interprets counts and bucket retention. In job mode, manual datastore/counts are unused. The script does not check that selected jobs and GC/manual-prune targets refer to the same datastore.

For a full configured-job run, fill these values into the corresponding existing tables in the supplied file:

```toml
[jobs]
sync_job = "sync-nightly"
verify_job = "verify-nightly"
gc_datastore = "backup-store"

[prune]
mode = "job"
job = "prune-nightly"
```

For manual pruning, edit the existing `[prune]` table (these counts illustrate syntax, not a recommended policy):

```toml
[prune]
mode = "manual"
job = ""
datastore = "backup-store"
keep_last = 7
keep_daily = 14
keep_weekly = 8
keep_monthly = 12
keep_yearly = 3
```

For verification only, set `steps.sync = false`, `steps.verify = true`, `steps.prune = false`, and `steps.gc = false`; supply `jobs.verify_job`. Do not append duplicate tables when editing the sample: replace the existing values.

### Logging

| Setting | Default | Meaning |
| --- | --- | --- |
| `logging.verbose` | `false` | Enable DEBUG application messages on the terminal. Full file logging is always enabled. |

The script creates **`logs/` beside itself** automatically. A unique UTC timestamp, PID and random suffix prevent overlapping filenames:

```text
logs/pbs-maintenance-YYYYMMDDTHHMMSS.microsecondsZ-PID-suffix.log
logs/pbs-maintenance-YYYYMMDDTHHMMSS.microsecondsZ-PID-suffix.err
```

- **`.log`**: application status and every captured stdout/stderr line, including progress, warnings and errors. File timestamps use the host's local logging time; filenames use UTC. Concurrent stdout/stderr order reflects arrival order, not a guaranteed total order.
- **`.err`**: ERROR/CRITICAL records only. It is created lazily when the first error occurs; a clean run leaves no empty `.err` file. Each run has its own file, so previous errors cannot leak into the current run's log.
- The app recognizes explicit subprocess error prefixes, case-insensitively: `Error:`, `TASK ERROR`, `FATAL:`, `CRITICAL:`, `FAILED:`, `[ERROR]`, `[FATAL]`, and `[CRITICAL]`. It also recognizes an unbracketed ISO-style timestamp before the prefix. Normal stderr progress and warnings remain in `.log` only.
- A nonzero command exit always adds a failure summary to `.err`, even if output has no recognizable severity label. Unlabelled diagnostic details remain in the full `.log`; the script cannot reliably infer the meaning of arbitrary free-form text. Error-labelled output alone does not override a zero command exit code.
- Config/preflight, command-launch and notification failures also go to `.err`. Malformed TOML is reported without echoing its source text; notification exceptions are logged by type to avoid exposing credentials in server replies.
- New log files use mode `0600`; a new log directory uses `0700`. Existing directory permissions are not changed. Logs are not rotated/deleted automatically.

Normal stdout/stderr continues streaming live. Each line appears once on its corresponding console stream; logging does not add a second copy. Help/version and argument-parser errors do not create logs; config errors do because logging is set up before loading TOML.

### Dry-run

| Setting | Default | Meaning |
| --- | --- | --- |
| `dry_run.enabled` | `true` | Validate and log exact planned commands without starting **any PBS process**. `false` enables real work. |
| `dry_run.send_mqtt` | `false` | Send a real MQTT dry-run event using `[mqtt]`; independent of `mqtt.enabled`. |
| `dry_run.send_email` | `false` | Send a real local-sendmail dry-run message using `[email]` and `[sendmail]`; independent of `email.enabled`. |

Dry-run requires valid local step/target configuration, but does not inspect PBS jobs/datastores, run a PBS validation command, or require the PBS executable. It writes logs. With both send settings false, it makes no broker/mail connections and does not require Paho. With a send setting true, the relevant library/settings are checked and a real notification is sent.

To test both transports while leaving maintenance untouched:

```toml
[dry_run]
enabled = true
send_mqtt = true
send_email = true
```

MQTT uses the distinct event `pbs_maintenance_dry_run` with `dry_run: true` and a `commands` array. It is **always non-retained**, even if `mqtt.retain = true`. Subscribers should distinguish this event from maintenance success. Email subjects contain **DRY RUN** and bodies include the planned commands. A notification failure makes the dry run return 1; it never triggers maintenance.

### MQTT

| Setting | Default | Meaning / example |
| --- | --- | --- |
| `mqtt.enabled` | `true` | Send real-run outcomes; dry-run is controlled separately. `false` permits real runs without MQTT. |
| `mqtt.host` | `""` | Broker hostname/IP, required when selected, e.g. `"mqtt.example.lan"`. |
| `mqtt.port` | `1883` | TCP port, 1–65535. Set `8883` explicitly if your TLS broker uses it. |
| `mqtt.topic` | `""` | Required publish topic, e.g. `"pbs/maintenance/status"`; no `+`/`#` wildcards. |
| `mqtt.username` | `""` | Optional broker authentication username. Empty means no authentication. |
| `mqtt.password` | `""` | Password; a nonempty password requires a username when sending. |
| `mqtt.tls` | `false` | Enable TLS; does not change the port automatically. |
| `mqtt.cafile` | `""` | TLS CA certificate file, or system trust when empty. Transport ignores it without TLS; a supplied path must exist when MQTT sending is selected. |
| `mqtt.insecure` | `false` | Disable TLS hostname checking. TLS only; weakens identity verification. Keep false for normal use. |
| `mqtt.client_id` | `""` | Empty generates `pbs-maint-<hostname>-<PID>`; use distinct IDs for concurrent clients. |
| `mqtt.retain` | `false` | Retain a real-run outcome on the broker; dry-run overrides this to false. Check timestamps when consuming retained events. |
| `mqtt.max_output_chars` | `4000` | Positive tail length per stdout/stderr field in failure events, also used by email. Does not bound full logs or captured output in memory. |
| `mqtt.timeout_sec` | `15` | Positive publish wait; the existing helper can wait twice this duration, plus connection time. Not an overall runtime deadline. |

MQTT uses protocol 3.1.1 and fixed QoS 1. A missing Paho library for an active MQTT channel is detected before maintenance. Broker connectivity/authentication is checked when publishing, so it can still fail after work has completed.

### Email through local sendmail/Postfix

Email delivery follows the same local-sendmail pattern as the related Proxmox backup tooling: the script builds a standard `EmailMessage`, finds a sendmail-compatible executable, and feeds the complete message to `sendmail -t` on stdin. It does **not** connect directly to an SMTP server and stores no SMTP username/password/TLS settings. Your local Postfix/sendmail configuration is responsible for relay, authentication, TLS, queueing, and final delivery.

| Setting | Default | Meaning / example |
| --- | --- | --- |
| `email.enabled` | `false` | Send real-run success/failure messages; dry-run uses `dry_run.send_email` instead. |
| `email.from_address` | `""` | Required sender mailbox when email is selected, e.g. `"pbs@example.com"`. |
| `email.to_addresses` | `[]` | Required nonempty array of recipient mailboxes, e.g. `["admin@example.com"]`. |
| `email.subject_prefix` | `"[PBS maintenance]"` | Single-line prefix. The app appends `SUCCESS`, `FAILED` or `DRY RUN`, plus hostname. |
| `sendmail.path` | `""` | Optional explicit sendmail executable. Empty auto-detects `/usr/sbin/sendmail`, `/usr/bin/sendmail`, then `sendmail` in `PATH`. Relative configured paths resolve from the TOML directory. |

When email is selected, preflight requires a usable sendmail-compatible executable before maintenance starts. The send command is exactly `SENDMAIL_PATH -t`; the RFC message bytes are supplied on stdin with no shell. A nonzero sendmail exit makes the requested notification fail. The email body contains the same JSON outcome data as MQTT; logs are referenced by path, not attached. MQTT and email are attempted independently, so one notification transport failing does not suppress the other. No automatic retry is performed by this script; a local MTA may queue/retry according to its own configuration.

Typical Debian/PBS setup uses Postfix. For example:

```bash
sudo apt install postfix
command -v sendmail
```

Configure Postfix for your environment before relying on notifications. `command -v sendmail` should normally resolve to a sendmail-compatible binary after installation.

## Maintenance commands and safety

Enabled steps always run in this order. Each CLI process must exit successfully before the next step starts.

| Step | Command |
| --- | --- |
| Sync | `proxmox-backup-manager sync-job run JOB_ID` |
| Verify | `proxmox-backup-manager verify-job run JOB_ID` |
| Prune with job | `proxmox-backup-manager prune-job run JOB_ID` |
| Prune manually (alternative) | `proxmox-backup-manager prune run DATASTORE [--keep-last N] [--keep-daily N] [--keep-weekly N] [--keep-monthly N] [--keep-yearly N]` |
| GC | `proxmox-backup-manager garbage-collection start DATASTORE` |

Sync/verify/prune jobs use their existing PBS configuration. Manual pruning forwards only supplied retention counts. GC uses its independently selected datastore. Commands are passed as argument arrays, without a shell; shell-quoted text in logs is for inspection.

Any nonzero command exit stops the sequence and skips later steps; a command-launch error does the same. Prune requires one selected mode and manual mode requires explicit retention. Invalid local config, missing active MQTT dependency, missing active sendmail executable, missing PBS executable for a real run, or failure to create logs aborts before maintenance. Local validation failures do not send notifications.

Pruning and GC can remove data. The app does not create jobs, check cross-job datastore consistency, provide rollback, prevent concurrent runs, enforce a PBS subprocess timeout, or poll separate PBS task records. Success is based on the CLI exit codes. Confirm command support and completion semantics on the PBS release you use, and arrange overlap prevention externally when scheduling. Use absolute interpreter/script paths for scheduled runs.

## Events, exit codes and troubleshooting

All outcome events include `hostname`, `time_utc` (UTC ISO timestamp), selected `steps`, active `sync_job`/`verify_job`, `prune_mode`, `prune_job`, `prune_datastore`, `prune_keep`, GC `datastore`, `version`, `dry_run`, `log_file`, and `err_file`. Inactive targets are null. `steps` is a selection map, not a completed-step ledger. The error path is null if no error file existed when the event was composed; a later notification error can create that file afterward.

- `pbs_maintenance_success`: all selected CLI commands returned zero.
- `pbs_maintenance_failed`: adds `failed_step`, underlying `returncode`, shell-quoted `command`, `stdout_tail`, and `stderr_tail`. A caught launch error uses code 127. Tail fields can include contextual non-error output; the `.err` file remains error-only.
- `pbs_maintenance_dry_run`: adds planned `commands` arrays; no PBS commands were executed.

| Exit | Meaning |
| --- | --- |
| `0` | Maintenance/preview succeeded and all selected sends completed; also used by help/version. |
| `1` | A maintenance command/launch or requested notification failed. |
| `2` | Arguments, TOML, settings, dependencies, executable preflight, or log creation prevented startup. No maintenance ran. |

Unexpected exceptions and interruptions may terminate without an outcome notification. If logging cannot be initialized, the error appears on the terminal. Inspect both local logs and PBS task status before retrying a real run: earlier steps may already have completed. No notification channel guarantees delivery for every kind of failure.

## Project layout

The original `pbs-datastore-sync-verify,prune-gc.py` remains the command-line entry point. Keep its sibling `pbs_maintenance/` package when installing or moving the application. All runtime options still come from TOML; package modules are imported by the launcher and are not separate commands.

| Module | Responsibility |
| --- | --- |
| `pbs_maintenance/settings.py` | TOML defaults, loading, validation, and choosing notification channels. |
| `pbs_maintenance/maintenance.py` | Sync, verify, prune and garbage collection; shared command execution, dry-run, outcome data, and notification coordination. |
| `pbs_maintenance/logging_config.py` | Console output, private full/error log files, and error classification. |
| `pbs_maintenance/mqtt.py` | MQTT connection, authentication, TLS and acknowledged publication. |
| `pbs_maintenance/mail.py` | Build outcome email, locate local sendmail/Postfix, and deliver with `sendmail -t`. |

A small `pbs_maintenance/__init__.py` holds the version and project-directory location. Default config and logs remain in the project root beside the launcher, not inside the package.

The four PBS operations share one module because their command planner plus the manual-prune helper total about 30 lines. Splitting them into four files would mostly separate tiny command builders that depend on the same executor and stop-on-failure workflow. Email/sendmail and MQTT have distinct protocols and dependencies, so each has its own module. No separate utility, model, per-step, or notification-dispatch modules are needed.

## Project and verification files

`config-example.toml` is the complete commented example; `config.toml` holds the local configuration. `.gitignore` excludes local config files while allowing the example. `requirements.txt` defines installation dependencies. `commented_code_map.md` explains functions/commands; `VERSIONING.md` records releases; `VERIFICATION.md` records tests and their limits. `RELEASE_MANIFEST.json` records original/prior-file preservation and release hashes. The ZIP excludes generated logs, bytecode, environments and temporary files.

Implementation references: [Python TOML parsing](https://docs.python.org/3/library/tomllib.html), [Python `EmailMessage`](https://docs.python.org/3/library/email.message.html), [Python subprocesses](https://docs.python.org/3/library/subprocess.html), and [Python file logging](https://docs.python.org/3/library/logging.handlers.html).

## License

No warranty is provided.

You may modify and use this script freely, but **you alone are responsible** for its usage and impact.
