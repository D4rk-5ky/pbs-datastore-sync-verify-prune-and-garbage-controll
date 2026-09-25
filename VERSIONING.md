# Versioning and complete change record

`VERSION` and the Python `__version__` constant identify the current release. Each created release advances by one patch unit. Patch values range from 0 to 99: `0.0.98 → 0.0.99 → 0.1.0`; never `0.0.100`. After rollover, patch increments resume (`0.1.1`). A release must update this file, current usage documentation, the code map, and all option examples together.

Use the single canonical filename `VERSIONING.md`. Do not create a case-only `versioning.md` duplicate: it conflicts on case-insensitive filesystems.

## 0.0.5 — 2026-09-25

- Audit the complete 0.0.4 archive before modification and keep the existing maintenance, validation, logging, notification, prune-safety, command ordering, and failure-stop behavior unchanged.
- Clarify the launcher help text so every available CLI flag is explicit about side effects and path behavior: `--version` is information-only, `--config PATH` selects only the TOML file, relative paths use the caller working directory, logs remain beside the resolved launcher, and operational settings stay in TOML. No operational CLI flags are added.
- Increment `VERSION` and package `__version__` from 0.0.4 to 0.0.5. Update the current README, both complete TOML examples, CLI command reference, code map, test version expectation, verification record, and release manifest together. Preserve the user-provided disclaimer text.
- Retain every 0.0.4 project path. Add no runtime dependencies, no new operational settings, and no new production modules. Package without logs, bytecode, caches, environments, build output, or temporary files.
- Re-run compilation, all offline regression/integration tests, help/version checks, config/default parity checks, definition/code-map coverage checks, archive integrity checks, and final extracted-ZIP tests. Live PBS, MQTT, and SMTP services remain outside the test scope.

## 0.0.4 — 2026-09-25

- Refactor the single script into the `pbs_maintenance` package with five functional modules: `settings.py`, `maintenance.py`, `logging_config.py`, `mqtt.py`, and `mail.py`. Add only a small package `__init__.py` for version/root-location constants; retain the original filename and CLI in the launcher.
- Keep sync, verify, prune and GC together: the shared planner plus manual-retention builder total about 30 lines, and all steps use the same executor and failure-stop logic. Keep notification coordination and outcome construction in maintenance rather than adding another tiny module.
- Move existing functions/classes without rewriting their behavior. Qualify cross-module calls; rename the local notification settings variable to `mqtt_settings` to avoid shadowing the settings module. Read optional Paho availability through its owning module during validation.
- Centralize version and project-root resolution in the package. Keep config.toml and logs/ beside the launcher even though logging code moved into a subdirectory. Increment VERSION and package __version__ from 0.0.3 to 0.0.4; the launcher imports the same version.
- Use `settings.py` rather than `config.py` so the existing config*.* ignore rule cannot hide required Python source. No .gitignore rules, TOML keys/defaults, dependencies, command flags, payload structure, or safety policies change.
- Adapt the 27 existing tests to import and patch the owning modules. Add three integration tests covering real CLI/package imports, direct and symlink launch from another directory, simulated PBS success/failure across module boundaries, and imports without runtime side effects. Add one shared disposable CLI-fixture helper; no production test services are contacted.
- Document module responsibilities and installation layout in README, explain every moved function and command in commented_code_map.md, refresh both commented TOML version headers and command-reference header, and preserve the exact disclaimer.
- Refresh verification and archive manifests. Preserve all original and 0.0.3 paths; package all six new package files without caches, runtime logs, environments, or temporary files.

## 0.0.3 — 2026-09-24

- Add `.gitignore` with `config*.*` and `!config-example.toml`. Use the requested example filename consistently; no misspelled duplicate is created.
- Add `config-example.toml` with all 43 options, complete existing comments, blank site-specific fields, and safe dry-run defaults. Preserve the original `config.toml` in the full release ZIP.
- Document non-overwriting template setup, the unchanged default config path, and the distinction between ignored and already tracked files in README and the command/code references. Preserve the exact disclaimer.
- Increment VERSION and the application's version constant from 0.0.2 to 0.0.3; no maintenance, logging, notification, or validation behavior changes.
- Update the existing schema/default test to read the tracked example, so it works in Git checkouts without an ignored local config. Update its version expectation.
- Refresh verification and manifest; preserve every original and prior-release path; exclude runtime logs, caches, bytecode, environments, and temporary files from the ZIP.

## 0.0.2 — 2026-09-24

Code and configuration changes:

- Replace operational CLI flags with a fully commented `config.toml` beside the resolved script. Keep informational `-h`/`--help`, `--version`, and alternate-file `--config PATH`. Add a typed default schema, strict unknown-key/type validation, and config-relative CA path handling. Reuse existing prune/metadata helpers through a Namespace adapter.
- Add TOML support using built-in `tomllib` on Python 3.11+ and conditional Tomli on Python 3.9/3.10; add `requirements.txt` for these dependencies and Paho.
- Create unique per-run `.log` files in script-local `logs/`, with complete captured stdout/stderr and application records. Create `.err` lazily for ERROR/CRITICAL records only. Classify explicit error labels instead of treating all stderr as errors; nonzero command exits always generate error summaries. Avoid duplicate streamed console lines, replace undecodable output bytes, close log handlers at exit, use 0600 file / 0700 new-directory permissions, and abort if log creation fails.
- Add safe-default dry-run: validate/configure the same ordered plan, log exact shell-quoted commands, and return before any PBS execution. Add separate opt-in MQTT/email sends independent of real-run transport switches. Mark events/subjects as dry-run and force non-retained MQTT dry-run sends.
- Add optional SMTP email notifications, with STARTTLS, implicit TLS or explicit plaintext relay modes, verified TLS/custom CA support, authentication, recipient list, subject prefix and socket timeout. Report partial recipient refusal as failure. Attempt MQTT and SMTP independently without retrying maintenance.
- Add real-run MQTT enable/disable and configurable publish wait using the existing mqtt_publish parameter. Preserve original QoS 1, MQTT 3.1.1, helper implementation and real-run retained-message setting.
- Keep sync → verify → prune → GC order and stop-on-first-failure semantics. Preserve explicit manual-retention requirements and one prune mode; reject ambiguous configured-job/manual inputs. Add negative retention rejection, positive output-limit/timeouts and port/type checks. Zero retention remains explicitly forwarded to PBS.
- Check active Paho dependency and real-run PBS executable before work. Catch command-launch OSError to log/report failure with code 127 and stop later steps. Config/preflight failures return 2 without notifications. Mask TOML source in parser errors and log transport exception types rather than possibly sensitive server replies.
- Add version, dry_run and log paths to shared events; add a distinct dry-run event with planned command arrays. Reuse base_payload and failure tails across notification channels. Replace old nested notification wrappers with shared send_notifications; factor command planning, config validation and workflow execution into named functions documented in the code map.
- Increment both version markers from 0.0.1 to 0.0.2 under the patch rollover rule.

Documentation and verification:

- Rewrite README for current TOML/logging/dry-run/email behavior and all options, preserving the user's exact latest disclaimer and the original license text. Update the retained command-reference file, complete code map, version record, verification notes and release manifest.
- Adapt and expand regression tests to cover the config interface, original sequencing/prune guards, error-only logs, dry-run combinations, notification independence, SMTP modes/refusals, paths, preflights and dependency handling.
- Preserve both original file paths and every 0.0.1 project file path; add only config and dependency files. Package the full project without generated logs, environments, bytecode, caches or temporary files. Check the archive against the original and prior release; rerun tests from the extracted package.

See VERIFICATION.md and the external archive report for actual checks and limitations. No live PBS, MQTT or SMTP service was exercised.

## 0.0.1 — 2026-09-24

Initial numbered release. The supplied archive has no version marker, version history, config examples, or code map. Its unversioned state is treated as the baseline before 0.0.1, not as a previously published 0.0.0 release.

Code changes:

- Correct the module docstring's filename to the preserved original script filename.
- Add `__version__ = "0.0.1"` and an informational `--version` action that exits before validation, maintenance, or MQTT publication.
- Use `ArgumentDefaultsHelpFormatter` to show option defaults in CLI help.
- Expand help for step selectors, job/GC targets, all retention counts, MQTT host/port/topic/authentication/TLS/client identity, failure output length, and verbose logging.
- No maintenance function, subprocess command, execution ordering, prune guard, error-handling path, MQTT payload, retention default, or existing argument parsing constraint changed.

Documentation and supporting files:

- Replace the unrelated vzdump README content with current PBS maintenance usage, every CLI flag with defaults and examples, exact invoked commands, both prune modes, MQTT fields, exit codes, and actual limitations.
- Include the user-provided disclaimer verbatim and retain the original license section.
- Add `VERSION`, `commented_code_map.md`, `examples/cli-options.txt`, offline regression tests, and `VERIFICATION.md`.
- Include a release manifest comparing every original file path with the final package, content hashes, and explicit new-file inventory. Preserve both original filenames.
- Package the complete release without bytecode, caches, local environments, intermediate scripts, or temporary files. The untouched input archive is preserved outside the package.

Validation outcomes and limitations are recorded in `VERIFICATION.md` and the external archive verification report.
