# Proxmox → PBS Backup with MQTT Status

## ⚠️ Disclaimer / Responsibility

**Use this script entirely at your own risk.**

The author/coder of this script takes **no responsibility or liability** for any damage, data loss, corruption, downtime, misconfiguration, missed backups, security exposure, or other issues that may arise from using this script.

By using this script, **you accept full responsibility** for its behavior and consequences.

Before using it, you are explicitly expected to:

- Read the **entire source code**
- Understand exactly what the script does
- Review how it interacts with your Proxmox and PBS environment
- Test it in a **non‑production environment first**

If you do not understand the code, **do not run it**.

---

## Overview

This script runs a **full Proxmox VE backup** (VMs and containers) to a **Proxmox Backup Server (PBS)** datastore using `vzdump`, and then publishes the backup result to **MQTT**.

It is intended for automation, monitoring, and integration with systems such as **Home Assistant**, **Node‑RED**, or other MQTT consumers.

---

## Features

- Runs `vzdump` on a Proxmox VE node
- Backup **all VMs/CTs** or selected VMIDs
- Live streaming of backup output to terminal
- Writes `.log` and `.err` files per run
- Publishes MQTT JSON payload on **success and failure**
- Supports MQTT authentication, QoS, retain, TLS
- Optional vzdump mail options
- Dry‑run mode for validation/testing

---

## Requirements

### System

- Proxmox VE node (must run on the Proxmox host, **not** on PBS)
- Root privileges
- `vzdump` available in PATH

### Python

- Python 3.9+
- Dependency:

```bash
pip install paho-mqtt
```

---

## What the Script Does

1. Validates environment (root, `vzdump`, MQTT library)
2. Builds a `vzdump` command from CLI flags
3. Executes the backup with live output streaming
4. Writes logs:
   - `.log` → full output
   - `.err` → combined output (referenced only if non‑empty)
5. Publishes MQTT status payload **regardless of success or failure**

---

## MQTT Payload Example

```json
{
  "status": "success",
  "rc": 0,
  "node": "pve-node1",
  "storage": "PBS-Backup:Datastore1",
  "duration_s": 842,
  "ts": "2026-01-21T05:10:22",
  "log_file": "/var/log/pbs-backup/pbs-backup-2026-01-21_05-00-01.log",
  "err_file": null
}
```

---

## CLI Flags

### Selection

- `--all` (default)
- `--no-all`
- `--vmid <id...>`
- `--exclude <id...>`

### Backup Options

- `--storage <storage>` **(required)**
- `--mode snapshot|suspend|stop`
- `--compress <algo>`
- `--bwlimit <KB/s>`
- `--only-running`
- `--quiet`
- `--timeout <seconds>`
- `--notes-template <template>`
- `--dry-run`

### vzdump Mail (optional)

- `--mailto <email>`
- `--mailnotification <value>`

### Logging

- `--log-dir <path>`
- `--log-prefix <prefix>`

### MQTT

- `--mqtt-host <host>` **(required)**
- `--mqtt-port <port>`
- `--mqtt-topic <topic>` **(required)**
- `--mqtt-user <user>`
- `--mqtt-pass <pass>`
- `--mqtt-qos 0|1|2`
- `--mqtt-retain`
- `--mqtt-timeout <sec>`
- `--mqtt-client-id <id>`

### MQTT TLS

- `--mqtt-tls`
- `--mqtt-cafile <path>`
- `--mqtt-insecure`

---

## Usage Examples (All Flag Combinations Covered)

### Minimal Required

```bash
sudo ./pbs_backup.py \
  --storage "PBS-Backup:Datastore1" \
  --mqtt-host 10.0.0.10 \
  --mqtt-topic proxmox/backup/pbs
```

---

### All Guests (Explicit)

```bash
sudo ./pbs_backup.py \
  --all \
  --storage "PBS-Backup:Datastore1" \
  --mqtt-host 10.0.0.10 \
  --mqtt-topic proxmox/backup/pbs
```

---

### All Guests With Exclusions

```bash
sudo ./pbs_backup.py \
  --all \
  --exclude 101 105 \
  --storage "PBS-Backup:Datastore1" \
  --mqtt-host 10.0.0.10 \
  --mqtt-topic proxmox/backup/pbs
```

---

### Specific VMIDs

```bash
sudo ./pbs_backup.py \
  --no-all \
  --vmid 100 101 102 \
  --storage "PBS-Backup:Datastore1" \
  --mqtt-host 10.0.0.10 \
  --mqtt-topic proxmox/backup/pbs
```

---

### Mode, Compression, Bandwidth

```bash
sudo ./pbs_backup.py \
  --mode snapshot \
  --compress zstd \
  --bwlimit 250000 \
  --storage "PBS-Backup:Datastore1" \
  --mqtt-host 10.0.0.10 \
  --mqtt-topic proxmox/backup/pbs
```

---

### Only Running + Quiet

```bash
sudo ./pbs_backup.py \
  --only-running \
  --quiet \
  --storage "PBS-Backup:Datastore1" \
  --mqtt-host 10.0.0.10 \
  --mqtt-topic proxmox/backup/pbs
```

---

### Dry‑Run

```bash
sudo ./pbs_backup.py \
  --dry-run \
  --storage "PBS-Backup:Datastore1" \
  --mqtt-host 10.0.0.10 \
  --mqtt-topic proxmox/backup/pbs
```

---

### vzdump Email Options

```bash
sudo ./pbs_backup.py \
  --mailto you@example.com \
  --mailnotification always \
  --storage "PBS-Backup:Datastore1" \
  --mqtt-host 10.0.0.10 \
  --mqtt-topic proxmox/backup/pbs
```

---

### MQTT Authentication / QoS / Retain

```bash
sudo ./pbs_backup.py \
  --mqtt-user backupbot \
  --mqtt-pass CHANGE_ME \
  --mqtt-qos 1 \
  --mqtt-retain \
  --storage "PBS-Backup:Datastore1" \
  --mqtt-host 10.0.0.10 \
  --mqtt-topic proxmox/backup/pbs
```

---

### MQTT TLS

```bash
sudo ./pbs_backup.py \
  --mqtt-tls \
  --mqtt-port 8883 \
  --mqtt-cafile /etc/ssl/certs/ca.pem \
  --storage "PBS-Backup:Datastore1" \
  --mqtt-host mqtt.example.lan \
  --mqtt-topic proxmox/backup/pbs
```

---

### Full Example (All Major Flags)

```bash
sudo ./pbs_backup.py \
  --no-all \
  --vmid 100 101 102 105 \
  --exclude 101 \
  --mode snapshot \
  --compress zstd \
  --bwlimit 250000 \
  --only-running \
  --timeout 14400 \
  --notes-template "Backup {node} {date}" \
  --mailto you@example.com \
  --mailnotification always \
  --log-dir /root/logs/pbs-backup \
  --log-prefix nightly \
  --mqtt-host mqtt.example.lan \
  --mqtt-port 8883 \
  --mqtt-topic proxmox/backup/pbs \
  --mqtt-user backupbot \
  --mqtt-pass CHANGE_ME \
  --mqtt-qos 1 \
  --mqtt-retain \
  --mqtt-timeout 30 \
  --mqtt-client-id pve1-backup-job \
  --mqtt-tls \
  --mqtt-cafile /etc/ssl/certs/ca.pem
```

---

## Security Notes

- MQTT credentials on CLI may appear in `ps` output
- Prefer TLS and restricted broker users
- Logs may contain sensitive metadata
- Restrict file permissions on the script and log directory

---

## License

No warranty is provided.

You may modify and use this script freely, but **you alone are responsible** for its usage and impact.
