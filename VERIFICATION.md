# Verification — 0.0.2

Checked on 2026-09-24 on macOS, using Python 3.9.6 with Paho 2.1.0 / Tomli 2.4.1 and Python 3.14.7 with built-in tomllib. Dependencies were installed into a temporary test environment outside the project. No live PBS, MQTT or SMTP connection was used.

## Completed checks

- Inspect the prior release source, documentation, config-reference and tests before changes; preserve both supplied original file paths and all nine 0.0.1 file paths.
- Compile both Python files on Python 3.9 and 3.14, storing bytecode outside the release tree.
- Run **27 offline test methods**, with parameterized cases, on both runtimes from the final extracted ZIP. Tests cover the original command order, every step's failure/stop behavior, manual-prune arguments/guards, disabled steps and shared payloads, output tails, all four dry-run channel combinations, absent dry-run process execution, independent notifications and their errors, command-launch errors, CLI info exits, config parsing/type/range/unknown-key errors, header validation, CA paths, dependency handling, and log setup failures.
- Exercise real harmless local Python subprocesses for stdout/stderr capture. Confirm ordinary stderr progress/warnings stay out of `.err`, explicit error output and failed command summaries enter it, successful progress does not create an empty `.err`, console lines are not duplicated, and files are created with mode 0600.
- Test SMTP with mocked clients for STARTTLS, implicit TLS, plaintext relay, login, recipient refusal and dry-run subjects; check TLS verification contexts and ensure STARTTLS failure never falls back or sends credentials/mail.
- Execute actual CLI help/version and no-flag dry-run smoke checks from a different working directory using disposable script/config copies. Confirm the default config/logs resolve beside the script, no .err is created for a clean preview, and a sentinel PBS executable is never invoked. Check relative `--config` selection and missing-executable preflight for real mode.
- Require all **43 config options** to match the documented schema/defaults, have preceding explanatory comments, and appear by qualified name in README. Confirm the packaged template defaults to dry-run with both send options false.
- Compare every application/test function name with the code map and compare the disclaimer block byte-for-byte with the user's previously approved text.
- Compare ASTs for seven reused helpers: MQTT publishing, tail trimming, UTC time, retention payload construction, base payload, explicit retention detection and manual prune command building remain unchanged.
- Build a full ZIP, verify CRC integrity, exact member list and bytes, all manifest hashes, both original paths and all prior-release paths, and absence of logs/caches/bytecode/environments/temp files. Rerun tests from the extracted ZIP before delivery. The external archive report records counts and SHA-256.

## Not fully tested

- Live PBS command compatibility, account privileges, sync/verify outcomes, prune deletion, GC behavior, datastore relationships, and whether the installed CLI waits for its server-side tasks.
- Real MQTT connectivity, credentials, retained/QoS delivery and all Paho callback versions. The original publishing helper is reused unchanged; workflows mock it during tests.
- Real SMTP server compatibility, credentials/certificates, DNS/network failures, spam filtering and final mailbox delivery. SMTP calls are mocked.
- Production scheduling, concurrent runs, interruptions, very large/long-running command output, full-disk/log-write failures after startup, or delayed child pipe closure.

## Limits intentionally documented

No overlap lock, PBS process timeout, rollback or independent task-status polling was added. Error-only logging identifies explicit labels and generates failure summaries; arbitrary unlabelled diagnostics stay in the full log. Captured output remains unbounded in memory and reader joins retain the original five-second timeout. The existing MQTT helper can wait twice its publish timeout and has no overall connection/run deadline. Notification failures do not undo completed maintenance. Unexpected exceptions/interruptions can still end without an outcome event.
