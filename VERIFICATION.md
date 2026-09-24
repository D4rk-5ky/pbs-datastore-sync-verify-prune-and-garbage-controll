# Verification — 0.0.3

Checked on 2026-09-24 with Python 3.14.7 on macOS. No live PBS, MQTT, or SMTP service was used.

## Current release checks

- Use actual `git check-ignore` in an isolated temporary repository to confirm that `config.toml`, other `config*.*` names, and nested configs are ignored, while `config-example.toml` (including in a nested directory) and unrelated files remain eligible for tracking.
- Parse both TOML files and require all 43 settings/defaults to match. Check every example option has preceding explanatory comments and appears in README.
- Compare the application AST with 0.0.2: only the version constant changed. Compile both Python files with bytecode directed outside the project.
- Run actual CLI help/version checks; confirm clean exit and no logs created.
- Run the existing 27 offline regression tests, including config validation, maintenance ordering, prune guards, dry-run notification combinations, log filtering, and mocked SMTP behavior. The bundled-config check now reads the tracked example instead of depending on an ignored local config.
- Compare the disclaimer byte-for-byte with 0.0.2; it is unchanged.
- Check the ZIP CRC, exact file inventory, hashes, original two paths, and all eleven 0.0.2 paths. Exclude generated logs, caches, bytecode, environments, and temporary files. Extract the final ZIP and run the tests in a disposable Git-checkout-style copy without config.toml to verify the example supports that workflow.

## Scope and limits

Maintenance, dry-run, logging, notifications, and validation code are unchanged from 0.0.2. Only version metadata, the existing test's template/version references, documentation, and the two new Git/template files changed.

Live PBS operations, MQTT delivery, SMTP delivery, and production scheduling remain untested. The original download ZIP is no longer at its original path; original-file preservation is checked against the untouched extracted original files and their recorded hashes. The preceding complete release ZIP supplies the immediate packaging baseline.

The release ZIP retains config.toml with blank site-specific values. Git ignore rules only affect untracked files; previously tracked configs require an explicit index removal, as documented in README.
