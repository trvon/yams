# YAMS Backup & Recovery (Operations)

Back up and restore YAMS safely. Follow the steps below.

## TL;DR

- What to back up:
  - Data root: `$YAMS_STORAGE` (contains `yams.db` and `storage/`)
  - Config: `~/.config/yams` (or your `XDG_CONFIG_HOME/yams`)
- Hot (online) backup (SQLite-safe + files):
  ```bash
  # Set locations
  export YAMS_STORAGE="${YAMS_STORAGE:-$HOME/.local/share/yams}"
  export BKP_DIR="/var/backups/yams/$(date +%F)"

  # 1) Backup SQLite DB atomically
  mkdir -p "$BKP_DIR"
  sqlite3 "$YAMS_STORAGE/yams.db" ".backup '$BKP_DIR/yams.db'"

  # 2) Sync content store files
  rsync -a --delete "$YAMS_STORAGE/storage/" "$BKP_DIR/storage/"

  # (Optional) Config backup
  rsync -a --delete "${XDG_CONFIG_HOME:-$HOME/.config}/yams/" "$BKP_DIR/config/"
  ```
- Cold (offline) backup (max safety):
  ```bash
  sudo systemctl stop yams
  rsync -a --delete "$YAMS_STORAGE/" "/var/backups/yams/$(date +%F)/"
  sudo systemctl start yams
  ```
- Restore:
  ```bash
  sudo systemctl stop yams
  export YAMS_STORAGE="${YAMS_STORAGE:-/var/lib/yams}"
  export BKP_DIR="/var/backups/yams/2025-08-11"

  # Restore DB and storage
  rsync -a --delete "$BKP_DIR/storage/" "$YAMS_STORAGE/storage/"
  install -d "$(dirname "$YAMS_STORAGE/yams.db")"
  cp -f "$BKP_DIR/yams.db" "$YAMS_STORAGE/yams.db"

  # Restore config (if backed up)
  rsync -a --delete "$BKP_DIR/config/" "${XDG_CONFIG_HOME:-/etc}/yams/"

  # Fix ownership/permissions
  sudo chown -R yams:yams "$YAMS_STORAGE" "${XDG_CONFIG_HOME:-/etc}/yams"
  sudo chmod 600 "${XDG_CONFIG_HOME:-/etc}/yams/keys/ed25519.pem" 2>/dev/null || true

  sudo systemctl start yams
  yams stats
  ```

---

## What to back up (and why)

- `$YAMS_STORAGE`
  - `yams.db` (SQLite metadata & search indices)
  - `storage/` (content-addressed files/chunks)
- Config (XDG config dir)
  - `${XDG_CONFIG_HOME:-$HOME/.config}/yams/`
  - Includes keys under `keys/` (private key should be 0600)
- Optional: logs, operational scripts, deployment/unit files

Restoring `yams.db`, `storage/`, and config/keys is sufficient to recover.

---

## Backup strategies

Options:

1) Online (no downtime)
- Use `sqlite3 .backup` for a transactionally consistent DB snapshot
- rsync the `storage/` directory
- For very high write rates, consider running rsync twice to reduce window of changed files

2) Offline (brief downtime, simpler)
- Stop service → rsync entire `$YAMS_STORAGE/` → start service

3) Filesystem snapshots (advanced)
- Use ZFS/Btrfs/LVM snapshots for consistent point-in-time images
- Ensure snapshot includes both DB and `storage/`

---

