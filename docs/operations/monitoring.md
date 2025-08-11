# YAMS Monitoring (Operations)

Monitor YAMS in production with the checks below.

## TL;DR

- Service health:
  - systemd: `systemctl is-active yams && echo OK || echo FAIL`
  - Logs (last 100): `journalctl -u yams -n 100 --no-pager`
- Storage health:
  - Stats: `yams stats --json | jq`
  - Capacity: `du -sh "${YAMS_STORAGE:-$HOME/.local/share/yams}"`
- TCP (WebSocket transport): `nc -z 127.0.0.1 8080 || exit 1`
- Alert quickly on:
  - Service down
  - Disk usage > 85% on $YAMS_STORAGE
  - Error spikes in logs
  - Stats report errors or unhealthy state

---

## What to monitor

1) Process and service
- yams process present and running
- systemd unit active and restarting rarely

2) Storage and capacity
- Disk space under `$YAMS_STORAGE`
- File descriptor limits (if you expect many connections)
- yams stats outputs “healthy”

3) Logs and errors
- Rate of ERROR/WARN lines
- Repeated migrations or init attempts (misconfiguration)
- Transport failures (WebSocket binds/connects)

4) Query/search health (optional)
- yams search timing in app logs (if you instrument upstream)
- Rate of empty/errored searches in your integration

---

## Quick checks

```bash
# Service
systemctl is-active yams && echo "yams: active" || (echo "yams: down" && exit 1)

# Logs (errors in the last 5 minutes)
journalctl -u yams --since "5 min ago" --no-pager | grep -E "ERROR|Error|Failed" || true

# Storage capacity
du -sh "${YAMS_STORAGE:-$HOME/.local/share/yams}"

# Basic stats JSON sanity
yams stats --json | jq '{ok: (has("error")|not), total: .total_documents, size_bytes: .total_size_bytes}'

# TCP reachability (WebSocket)
nc -z 127.0.0.1 8080 || echo "port 8080 not reachable"
```

---

## Prometheus exporter (textfile)

If you run node_exporter with the textfile collector, translate `yams stats --json` to a `.prom` file.

```bash
#!/usr/bin/env bash
# /usr/local/bin/yams_stats_exporter.sh
# Requires: jq, yams, and node_exporter textfile collector enabled

set -euo pipefail

OUT_DIR="/var/lib/node_exporter/textfile_collector"
OUT_FILE="${OUT_DIR}/yams.prom"

TMP=$(mktemp)
trap 'rm -f "$TMP"' EXIT

STATS_JSON=$(yams stats --json || echo '{}')

# Extract fields (adjust keys to match your yams stats payload)
total_docs=$(echo "$STATS_JSON" | jq -r '.total_documents // 0')
total_bytes=$(echo "$STATS_JSON" | jq -r '.total_size_bytes // 0')
unique_blocks=$(echo "$STATS_JSON" | jq -r '.unique_blocks // 0')
compression_ratio=$(echo "$STATS_JSON" | jq -r '.compression_ratio // 0')
health=$(echo "$STATS_JSON" | jq -r 'if .error then 0 else 1 end')

cat > "$TMP" <<EOF
# HELP yams_total_documents Total number of documents known to YAMS.
# TYPE yams_total_documents gauge
yams_total_documents $total_docs

# HELP yams_total_size_bytes Total size of stored data in bytes.
# TYPE yams_total_size_bytes gauge
yams_total_size_bytes $total_bytes

# HELP yams_unique_blocks Number of unique storage blocks.
# TYPE yams_unique_blocks gauge
yams_unique_blocks $unique_blocks

# HELP yams_compression_ratio Effective compression ratio (>= 1.0 means compressed).
# TYPE yams_compression_ratio gauge
yams_compression_ratio $compression_ratio

# HELP yams_health Overall YAMS health (1=ok, 0=error).
# TYPE yams_health gauge
yams_health $health
EOF

mv "$TMP" "$OUT_FILE"
```

Systemd timer to run every minute:

```ini
# /etc/systemd/system/yams-stats-exporter.service
[Unit]
Description=YAMS Stats Exporter

[Service]
Type=oneshot
Environment=YAMS_STORAGE=/var/lib/yams
ExecStart=/usr/local/bin/yams_stats_exporter.sh
User=node-exporter
Group=node-exporter

# /etc/systemd/system/yams-stats-exporter.timer
[Unit]
Description=Run YAMS Stats Exporter every minute

[Timer]
OnCalendar=*:0/1
AccuracySec=10s
Persistent=true

[Install]
WantedBy=timers.target
```

Enable:
```bash
systemctl daemon-reload
systemctl enable --now yams-stats-exporter.timer
```

---

## Suggested alerts

- Service down:
  - yams_active == 0 for 2m
- Health flag:
  - yams_health == 0 for 1m
- Disk capacity:
  - node_filesystem_avail_bytes / node_filesystem_size_bytes < 0.15 on $YAMS_STORAGE mount
- Error rate:
  - “ERROR|Failed” lines > threshold in last 5m (via log pipeline or Loki)
- Exporter stale:
  - `time() - yams_stats_exporter_timestamp_seconds > 180`

Tune thresholds to your environment.

---

## Logs and levels

- Default logging goes to stdout/stderr (systemd → journald).
- Tail:
  - `journalctl -u yams -f -o short-iso`
- Grep errors:
  - `journalctl -u yams --since "1 hour ago" | grep -E "ERROR|Error|Failed"`
- Consider a log shipper (journald → syslog, Loki, etc.) for centralization.

---

## Health checks (app + infra)

- App CLI:
  - `yams stats` non‑zero exit or missing fields → unhealthy
- TCP:
  - `nc -z 127.0.0.1 8080`
- Systemd:
  - Restart count > N per hour → investigate flapping

---

## Dashboards (examples)

- Storage
  - Documents over time, total bytes, compression ratio, unique blocks
- Health
  - yams_health, restart count, error rate
- Capacity
  - Filesystem usage for `$YAMS_STORAGE`
- Queries (if you instrument upstream)
  - Search latency p50/p95, result counts

---

## Troubleshooting

- Service won’t start:
  - `journalctl -u yams -n 200 --no-pager`
  - Verify `Environment=YAMS_STORAGE=...` and directory permissions
- Stats empty or failing:
  - Run `yams stats --json` manually with the same user/env as the service
- Port conflicts:
  - `ss -lntp | grep 8080` (Linux); adjust `--port` or stop the conflicting service
- Disk full:
  - Expand or rotate; keep >15% free headroom

---

## See also

- Deployment: `./deployment.md`
- Backup & Recovery: `./backup.md` (coming soon)
- Troubleshooting: `./troubleshooting.md` (coming soon)
- Performance Tuning: `./performance.md` (coming soon)
- CLI Reference: `../user_guide/cli.md`
- Admin Configuration: `../admin/configuration.md`
