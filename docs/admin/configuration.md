# Configuration

Concise guidance for configuring YAMS in production. This document focuses on filesystem layout, runtime execution, environment isolation, and operational hygiene. Keep configurations minimal, explicit, and reproducible.

## 1. Filesystem and data directory

YAMS persists content, indexes, and metadata in a data directory. Use a dedicated volume with strict permissions.

Recommended locations:
- Linux: /var/lib/yams
- macOS (standalone): /usr/local/var/yams or /opt/yams/data
- Containers: bind mount a host path into /var/lib/yams

Permissions:
- Create a dedicated system user/group (e.g., yams).
- Chown the data directory to yams:yams and set 0750.
- Avoid running as root.

Example (Linux):
```bash
sudo useradd --system --home /var/lib/yams --shell /usr/sbin/nologin yams || true
sudo mkdir -p /var/lib/yams
sudo chown -R yams:yams /var/lib/yams
sudo chmod 0750 /var/lib/yams
```

## 2. Runtime patterns

YAMS is commonly invoked as a CLI in batch or automation contexts. If you deploy a long-running API component in your environment, use a supervisor (systemd or a process manager) and apply the same isolation and logging practices.

### 2.1 Batch/automation (cron/systemd timer)
- Encapsulate commands in a script (with set -eu -o pipefail).
- Export environment in the script or use an EnvironmentFile.
- Log stdout/stderr for auditing.

Example wrapper:
```bash
#!/usr/bin/env bash
set -euo pipefail
export PATH="/usr/local/bin:$PATH"
export YAMS_STORAGE="/var/lib/yams"
# Example job: ingest a directory
yams add /srv/docs --recursive --include="*.md" --tags "docs,import"
```

### 2.2 Systemd service (long-running component)
If your deployment includes a long-running YAMS-based service (e.g., API layer):

```ini
[Unit]
Description=YAMS Service
After=network-online.target
Wants=network-online.target

[Service]
User=yams
Group=yams
Environment="YAMS_STORAGE=/var/lib/yams"
# EnvironmentFile=-/etc/yams/yams.env       # optional env file
WorkingDirectory=/var/lib/yams
ExecStart=/usr/local/bin/yams serve
Restart=on-failure
RestartSec=3
NoNewPrivileges=true
ProtectSystem=full
ProtectHome=true
PrivateTmp=true
LimitNOFILE=16384

[Install]
WantedBy=multi-user.target
```

Notes:
- If your build does not expose a server entrypoint, prefer batch/cron usage.
- Adjust LimitNOFILE based on workload (see Performance Tuning).

## 3. Containers

Use a bind mount for durability and run as a non-root UID/GID.

```bash
mkdir -p $HOME/yams-data
docker run --rm -it \
  -u 10001:10001 \
  -v $HOME/yams-data:/var/lib/yams \
  ghcr.io/trvon/yams:latest yams init --non-interactive
```

Operational run:
```bash
docker run --rm -it \
  -u 10001:10001 \
  -v $HOME/yams-data:/var/lib/yams \
  ghcr.io/trvon/yams:latest yams add /data --recursive --include="*.md"
```

Resource controls:
- CPU/memory limits per container (e.g., --cpus, --memory).
- Mount read-only code/config; write-only data directories.

## 4. Environment and parameters

Discover supported flags and environment variables with:
```bash
yams --help
```
Guidelines:
- Prefer explicit CLI flags in automation for clarity.
- Use environment variables for secrets and deployment-specific paths.
- Keep site-specific values in an EnvironmentFile (systemd) or a .env (container orchestrators).

Common categories to review in your version:
- Data directory/path
- Concurrency/threads
- Compression options (e.g., zstd, LZMA)
- Deduplication/segmentation settings
- Full-text and vector indexing behavior
- Logging verbosity and output destination
- API service bind address/port (if applicable)

Only configure options supported by your version; defaults are generally safe to start.

## 5. Logging

- Production: capture stdout/stderr via journald (systemd) or container logs.
- Set log verbosity via CLI flags or environment if supported by your version.
- Retain logs according to your compliance policy; rotate at the platform layer.

Minimal examples:
- systemd: journalctl -u yams.service
- Docker: docker logs <container>

## 6. Security

- Run as a non-privileged user.
- Restrict filesystem permissions on the data directory (0750).
- Avoid embedding secrets in command lines (process listing); prefer environment or secret managers.
- If exposing a network endpoint (API), bind to a private interface or use a reverse proxy/WAF; enforce TLS termination and authentication upstream.

## 7. OS resource limits

Set sane defaults for file descriptors and process memory:
- NOFILE: ≥ 16384 (increase with concurrency and open-file patterns)
- Consider vm.swappiness and I/O scheduling based on workload
- Pin versions for reproducibility; avoid untested rolling updates

Example (systemd):
```
LimitNOFILE=16384
```

## 8. Backups and recovery

- Snapshot/copy the entire data directory at a consistent point.
- Quiesce writes when feasible (low-traffic windows or maintenance).
- Store backups off-site with retention and integrity checks.
- Periodically test restore procedures.

See: Operations → [Backup](../operations/backup.md)

## 9. Upgrades

- Pin the container tag or package version; upgrade deliberately.
- Take a fresh backup before upgrades.
- Review release notes for any migrations or compatibility notes.
- Validate in staging with representative data and queries.

## 10. Observability

- Health checks: implement a periodic read/search probe and alert on failures.
- Metrics: wrap CLI/API calls with timing and exit code logging.
- Surface slow queries and error rates in your monitoring stack.

See: Operations → [Monitoring](../operations/monitoring.md)

## 11. References

- Admin → [Performance Tuning](./performance_tuning.md)
- Admin → [Vector Search Tuning](./vector_search_tuning.md)
- Operations → [Deployment](../operations/deployment.md)
- Operations → [Monitoring](../operations/monitoring.md)
- Operations → [Backup](../operations/backup.md)

## Appendix: Vector DB Schema and Sentinel

YAMS stores semantic embeddings in `vectors.db` using a fixed embedding dimension. When you switch models (e.g., 384‑dim MiniLM to 768‑dim MPNet), you must align the vector schema to the new dimension.

Convergence mechanism (in order of precedence):
- Config (`~/.config/yams/config.toml`): `embeddings.embedding_dim` (preferred), then `vector_database.embedding_dim`, then `vector_index.dimension`.
- Sentinel (`$YAMS_STORAGE/vectors_sentinel.json`): written by `yams doctor` after a schema recreation and used by the daemon to acknowledge the chosen dimension.
- Generator/Model: model‑reported dims are used only when neither config nor sentinel are available.

Recommended workflow to change dimensions:
- `yams model download <name> --apply-config` — updates config dims and recreates `vectors.db` schema to the detected model dim.
- or: `yams doctor` → accept “Recreate vectors.db” → accept “Restart daemon now.” The doctor updates config dims and writes the sentinel.
- Then: `yams repair --embeddings` to regenerate vectors.

Notes:
- The daemon reads the sentinel at startup to avoid transient “stored vs configured” warnings and adopts the DB’s stored dim for the session.
- Repair paths avoid dropping tables; destructive changes are handled explicitly by `yams doctor` or `--apply-config` on model download.
