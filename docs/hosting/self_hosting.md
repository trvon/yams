---
title: Self-hosting
---

# Self‑hosting YAMS

YAMS is open source and designed to run anywhere you can run a binary or a container. This guide gives you a practical, minimal path to run YAMS yourself, plus links to deeper docs.

!!! tip
    If you want zero‑ops, backups, and usage metrics out of the box, consider the upcoming managed service. See [Hosting Overview](README.md) and join the [Early Access](early-access.md).

---

## Who should self‑host?

Self‑hosting is a great fit if you:

- Need full control over infrastructure and data locality
- Prefer your own backup/monitoring stack
- Have security/compliance requirements that limit SaaS usage
- Want to evaluate YAMS internally before choosing managed hosting

If you just want to get productive immediately without operating infra, the hosted option will be a better fit once available.

---

## Quick start

The fastest way to try YAMS locally is with Docker, or grab a native binary.

### Option A — Docker (recommended for first run)

```bash
# Check the CLI runs
docker run --rm -it ghcr.io/trvon/yams:latest --version

# Create a local data directory on the host
mkdir -p $HOME/yams-data

# Use a bind mount so data persists across container runs
docker run --rm -it \
  -v $HOME/yams-data:/var/lib/yams \
  --name yams \
  ghcr.io/trvon/yams:latest \
  yams init --non-interactive

# Add and search data
echo "hello from self-hosted yams" | docker run --rm -i \
  -v $HOME/yams-data:/var/lib/yams \
  ghcr.io/trvon/yams:latest yams add - --tags "example,selfhosted"

docker run --rm -it \
  -v $HOME/yams-data:/var/lib/yams \
  ghcr.io/trvon/yams:latest yams search "self-hosted"
```

Notes:
- The example persists YAMS state under `/var/lib/yams` inside the container, mapped to `$HOME/yams-data` on your host.
- Use a stable host path (e.g., `/srv/yams`) for servers.

### Option B — Native binary

See the platform‑specific install steps in [Installation](../user_guide/installation.md). After installing:

```bash
# One-time storage init (default location, see Configuration)
yams init --non-interactive

# Add content
echo "hello world" | yams add - --tags example

# Search and retrieve
yams search "hello"
yams list --format minimal --limit 1 | xargs yams get
```

---

## Data directories and configuration

YAMS stores its durable state in a data directory on disk (content store, indexes, metadata).

- Default paths and tunables are documented in [Configuration](../admin/configuration.md).
- For a service deployment, choose a dedicated volume such as:
  - Linux: `/var/lib/yams`
  - macOS: `/usr/local/var/yams` or `/opt/yams/data`
- Backups: snapshot the entire data directory; see the Backup section below.

!!! note
    Keep your data directory on a persistent volume. If you use containers, mount it from the host or a managed volume to avoid accidental data loss.

---

## Minimal docker-compose (optional)

If you prefer Compose to keep a running container around:

```yaml
version: "3.9"
services:
  yams:
    image: ghcr.io/trvon/yams:latest
    container_name: yams
    command: ["sleep", "infinity"]  # keep container running; exec yams commands as needed
    volumes:
      - /srv/yams:/var/lib/yams
    restart: unless-stopped
```

Usage:

```bash
# Start the container
docker compose up -d

# Initialize storage
docker exec -it yams yams init --non-interactive

# Run commands on demand
docker exec -i yams sh -lc 'echo "compose example" | yams add - --tags demo'
docker exec -it yams yams search "compose"

# Stop when done
docker compose down
```

This pattern is useful when you want a long‑lived environment you can exec into for scheduled jobs, imports, or admin operations.

---

## Backups and recovery

A simple, reliable backup strategy:

1. Quiesce writes (optional but ideal for busy systems)
2. Snapshot or copy the entire YAMS data directory
3. Store backups off‑site with retention policies

Example host‑level backup:

```bash
# Stop writes (if possible) or run during low activity
systemctl stop yams || true

# Snapshot (example using rsync; adapt to your tooling)
rsync -a --delete /var/lib/yams/ /backups/yams/$(date +%F)/

# Restart if you stopped it
systemctl start yams || true
```

Logical grouping and checkpoints:

- Use tags and metadata to mark important states: `yams add file --tags "release,2024Q4"`
- Use collections and snapshots during ingest:
  - `--collection release-v1.0`
  - `--snapshot-id 2024Q4`

Restore is symmetric: provision a clean data directory, then replace it with the backed‑up copy and restart your runner/container.

!!! warning
    Always test restores. An untested backup is a potential outage.

---

## Upgrades

- Container: pin a version tag, then roll forward deliberately (e.g., `ghcr.io/trvon/yams:v0.3.x`)
- Native: download the new binary and replace the old one atomically
- Before upgrading:
  - Take a fresh backup of the data directory
  - Review release notes (breaking changes, migrations)
- After upgrading:
  - Run a sanity check: `yams --version`, `yams search "<known doc>"`

---

## Observability and operations

Even in self‑hosted mode, you can keep ops light:

- Health check: run a read query on a schedule and alert on failures
  - Example: `yams search "healthcheck-probe" --limit 1`
- Metrics: wrap YAMS commands with your own timing/exit‑code logging
- Logs: capture stdout/stderr from your service runner (systemd, Docker)

For more deployment guidance, see [Deployment](../operations/deployment.md) and [Performance Tuning](../admin/performance_tuning.md).

---

## Security checklist

- Run as a non‑root user where possible
- Restrict access to the data directory (filesystem permissions)
- Isolate credentials/tokens used by automation around YAMS
- Encrypt backups at rest; store off‑site with limited access
- For remote workflows, tunnel or proxy over mutually authenticated channels (VPN/SSH/zero‑trust)

If you plan to expose YAMS‑backed APIs to the public internet, place them behind a reverse proxy or WAF you control.

---

## Common tasks

Initialize storage (one‑time):

```bash
yams init --non-interactive
```

Add content:

```bash
echo "notes" | yams add - --tags "notes,example"
yams add ./docs/ --recursive --include="*.md" --tags "docs,import"
```

Search and retrieve:

```bash
yams search "vector search" --limit 5
yams list --format minimal --limit 1 | xargs yams get
```

Versioning and organization:

```bash
yams add ./CHANGELOG.md --metadata "release=1.0.0" --tags "release,changelog" --collection "release-v1.0"
```

---

## Troubleshooting

- “Command not found” → confirm install path or Docker image/pull policy
- “Permission denied” → check data directory ownership and permissions
- “No results” → verify ingest worked: `yams list --limit 5`
- Performance issues → see [Performance Tuning](../admin/performance_tuning.md)

If you get stuck, open an issue on GitHub with details about your environment and what you tried.

---

## Related docs

- Install: [Installation](../user_guide/installation.md)
- Configure: [Configuration](../admin/configuration.md)
- Deploy/operate: [Deployment](../operations/deployment.md)
- Search usage: [Search Guide](../user_guide/search_guide.md)

---

If you later decide to move to managed hosting, we’ll document export/import guides to migrate collections and metadata with minimal friction.
