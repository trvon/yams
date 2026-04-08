---
title: Self-hosting
---

# Self-hosting

Run YAMS on your own infrastructure with full control over data, backups, and operations.

**Consider managed hosting if** you want zero-ops, automated backups, and metrics. See [Managed Hosting](managed.md).

## Who Should Self-host

- Need full control over infrastructure and data locality
- Prefer your own backup/monitoring stack
- Have security/compliance requirements limiting SaaS
- Want to evaluate internally before choosing managed hosting

## Quick Start

### Docker (Recommended)

```bash
docker run --rm -it ghcr.io/trvon/yams:latest --version
mkdir -p $HOME/yams-data

docker run --rm -it \
  -v $HOME/yams-data:/var/lib/yams \
  ghcr.io/trvon/yams:latest yams init --non-interactive

echo "hello from self-hosted yams" | docker run --rm -i \
  -v $HOME/yams-data:/var/lib/yams \
  ghcr.io/trvon/yams:latest yams add - --tags "example,selfhosted"

docker run --rm -it \
  -v $HOME/yams-data:/var/lib/yams \
  ghcr.io/trvon/yams:latest yams search "self-hosted"
```

**Note:** Use stable host path (e.g., `/srv/yams`) for servers.

### Native Binary

See [Installation](../user_guide/installation.md) for platform-specific download and setup steps.

### Docker Compose (Optional)

```yaml
version: "3.9"
services:
  yams:
    image: ghcr.io/trvon/yams:latest
    container_name: yams
    command: ["sleep", "infinity"]
    volumes:
      - /srv/yams:/var/lib/yams
    restart: unless-stopped
```

```bash
docker compose up -d
docker exec -it yams yams init --non-interactive
docker exec -i yams sh -lc 'echo "compose example" | yams add - --tags demo'
docker exec -it yams yams search "compose"
```

## Data Directory

YAMS stores content, indexes, and metadata in a single directory.

**Paths:**
- Linux: `/var/lib/yams`
- macOS: `/usr/local/var/yams` or `/opt/yams/data`
- Containers: mount from host to `/var/lib/yams`

See [Admin: Operations](../admin/operations.md) for configuration details.

## Backups

```bash
systemctl stop yams-daemon || true
rsync -a --delete /var/lib/yams/ /backups/yams/$(date +%F)/
systemctl start yams-daemon || true
```

**Best practices:**
- Quiesce writes during backup (low-traffic windows)
- Store backups off-site with retention policies
- Test restores periodically

**Logical grouping:**
```bash
yams add file --tags "release,2024Q4" --collection "release-v1.0" --snapshot-id "2024Q4"
```

## Upgrades

```bash
# Pin container version
docker pull ghcr.io/trvon/yams:v0.3.x

# Backup before upgrading
rsync -a /var/lib/yams/ /backups/yams-pre-upgrade/

# Review release notes
# Upgrade binary/container
# Sanity check
yams --version
yams search "<known doc>"
```

## Operations

**Health checks:**
```bash
yams search "healthcheck-probe" --limit 1  # Run on schedule, alert on failure
```

**Metrics:** Wrap commands with timing/exit-code logging

**Logs:** Capture stdout/stderr (systemd, Docker logs)

See [Admin: Operations](../admin/operations.md) for deployment and tuning.

## Security

- Run as non-root user
- Restrict data directory permissions (0750)
- Isolate credentials/tokens
- Encrypt backups at rest, store off-site
- For remote access: VPN/SSH/zero-trust tunnel
- For public APIs: reverse proxy or WAF with TLS

## Common Tasks

See the [Quick Start](../user_guide/README.md) and [CLI Reference](../user_guide/cli.md) for usage examples.

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Command not found | Check install path or Docker image |
| Permission denied | Check data directory ownership/permissions |
| No results | Verify ingest: `yams list --limit 5` |
| Performance | See [Admin: Operations](../admin/operations.md) |

Open issues on GitHub with environment details and steps to reproduce.

---

**Related:**
- [Installation](../user_guide/installation.md)
- [Admin: Operations](../admin/operations.md)
- [Managed Hosting](managed.md)
- [Pricing](pricing.md)
