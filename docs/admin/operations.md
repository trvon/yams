# Operations

Configuration, tuning, and deployment guidance for production YAMS deployments.

## Data Directory

YAMS persists content, indexes, and metadata in a single data directory.

**Recommended locations:**
- Linux: `/var/lib/yams`
- macOS: `/usr/local/var/yams` or `/opt/yams/data`
- Containers: bind mount host path to `/var/lib/yams`

**Permissions:**
```bash
sudo useradd --system --home /var/lib/yams --shell /usr/sbin/nologin yams
sudo mkdir -p /var/lib/yams
sudo chown -R yams:yams /var/lib/yams
sudo chmod 0750 /var/lib/yams
```

## Deployment

### Batch/Automation (cron/systemd timer)

```bash
#!/usr/bin/env bash
set -euo pipefail
export YAMS_STORAGE="/var/lib/yams"
yams add /srv/docs --recursive --include="*.md" --tags "docs,import"
```

### Systemd Service

```ini
[Unit]
Description=YAMS Service
After=network-online.target

[Service]
User=yams
Group=yams
Environment="YAMS_STORAGE=/var/lib/yams"
WorkingDirectory=/var/lib/yams
ExecStart=/usr/local/bin/yams serve
Restart=on-failure
NoNewPrivileges=true
ProtectSystem=full
ProtectHome=true
LimitNOFILE=16384

[Install]
WantedBy=multi-user.target
```

### Containers

```bash
mkdir -p $HOME/yams-data
docker run --rm -it \
  -u 10001:10001 \
  -v $HOME/yams-data:/var/lib/yams \
  ghcr.io/trvon/yams:latest yams init --non-interactive
```

**Resource controls:**
- Use `--cpus` and `--memory` limits
- Mount code/config read-only; data directories write-only

## Configuration

Discover options: `yams --help`

**Guidelines:**
- Prefer explicit CLI flags for clarity
- Use environment variables for secrets and deployment paths
- Store site-specific values in EnvironmentFile (systemd) or .env (containers)

**Key environment variables:**
- `YAMS_STORAGE`: data directory path
- `YAMS_CONFIG`: config file override
- `YAMS_TUNING_PROFILE`: efficient|balanced|aggressive
- `YAMS_MAX_ACTIVE_CONN`: connection limit
- `YAMS_IO_THREADS`: dedicated IPC I/O thread count
- `YAMS_SERVER_MAX_INFLIGHT`: max in-flight requests per connection
- `YAMS_CONNECTION_LIFETIME_S`: absolute lifetime for main IPC connections (`0` disables)
- `YAMS_KEEPALIVE_MS`: daemon keepalive interval

## Performance Tuning

### Auto-Tuning (Daemon)

YAMS includes centralized TuneAdvisor and ResourceTuner for adaptive behavior:

- **Metrics:** CPU utilization (/proc deltas), memory (PSS/RSS), connection counts
- **Worker pools:** Shrink when idle, grow under pressure
- **Auto-embed policy:** Idle (default), Never, Always
  - Idle: embed only when daemon is idle; defer otherwise to background repair
  - Never: always defer to `yams repair --embeddings`
  - Always: embed immediately (increases ingest CPU)

### Embedding Batch Controls

Adaptive DynamicBatcher with conservative defaults:
- Safety factor: 0.90 (reserves token budget headroom)
- Inter-batch pause: 0ms (add delay to reduce CPU spikes)
- Advisory doc cap: unset (limits documents per batch)

### Tuning Profiles

Profiles bundle multiple tuning heuristics:

| Profile | Use Case | Behavior |
|---------|----------|----------|
| `efficient` | Resource-constrained hosts | Slower pool growth, higher thresholds |
| `balanced` | General purpose (default) | Moderate ramp-up |
| `aggressive` | High throughput under load | Faster growth, lower thresholds |

**Set profile:**
```bash
yams config set tuning.profile aggressive
yams daemon restart
```

Or via environment:
```bash
export YAMS_TUNING_PROFILE=aggressive
```

### Quick Wins

- **Storage:** NVMe SSD, ext4/xfs with `relatime`
- **Concurrency:** ~physical core count for workers
- **Compression:** zstd (balanced), LZMA (cold archives)
- **Chunking:** 8-16 KiB (text), 32-64 KiB (binaries)
- **SQLite:** WAL enabled, `synchronous=NORMAL` (bulk ingest), `FULL` (steady-state)
- **FTS5:** Index only queried fields, run `optimize` after bulk ingest
- **OS limits:** `nofile >= 16384`

### Multi-Client Responsiveness

When daemon responsiveness degrades under many concurrent clients, prefer these overrides first:

```bash
export YAMS_IO_THREADS=10
export YAMS_MAX_ACTIVE_CONN=2048
export YAMS_SERVER_MAX_INFLIGHT=64
export YAMS_IPC_TIMEOUT_MS=15000
export YAMS_MAX_IDLE_TIMEOUTS=12
```

If long-lived main-socket clients are being reaped by lifetime limits, increase or disable lifetime
enforcement:

```bash
# 30 minutes
export YAMS_CONNECTION_LIFETIME_S=1800

# disable absolute lifetime close for main socket connections
export YAMS_CONNECTION_LIFETIME_S=0
```

### Workload-Specific

**Ingest-heavy:**
- Batch files (100-1000 docs)
- Lower fsync cost (`synchronous=NORMAL`)
- Defer FTS optimize until after ingest

**Query-heavy:**
- Higher fsync levels (`synchronous=FULL`)
- Increase caches (OS + DB)
- Optimize query shapes (prefix/suffix, filters)

**Mixed:**
- Schedule maintenance during off-peak
- Conservative concurrency, throttle ingest during peak queries

### Configuration Reference

See [include/yams/config/config_defaults.h](../../include/yams/config/config_defaults.h) for available keys.

| Area | Config Key | Effect | When to Adjust |
|------|-----------|--------|----------------|
| Worker pools | `[performance].num_worker_threads` | CPU parallelism | Increase for bursts, decrease on constrained hosts |
| Request backlog | `[performance].max_concurrent_operations` | Task limit | Lower for latency-sensitive, raise for long-running ingest |
| Chunking | `[chunking].min/max/average_chunk_size` | Dedupe granularity | Smaller for text diffs, larger for binaries |
| Compression | `[compression].algorithm`, `zstd_level` | CPU vs storage | Lower levels for real-time, raise for archival |
| Embeddings | `[embeddings].auto_on_add`, `batch_size` | Embedding cost | Disable auto for cold ingest, trim batch if memory tight |
| Vector index | `[vector_database].index_type`, `num_partitions` | Recall vs latency | Start with defaults, tune after baseline data |
| WAL | `[wal].sync_interval`, `enable_group_commit` | Fsync cadence | Relax during bulk, tighten for steady-state |

### Vector Search Tuning

**Embeddings:**
- Precompute offline to reduce ingest latency
- Normalize vectors if metric requires (cosine)

**Index parameters:**
- HNSW: tune `M`/`efConstruction` (build), `efSearch` (query)
- IVF/PQ: select `nlist`/`nprobe`, quantization settings

**Hybrid search:**
- Filter with keyword first to reduce vector candidates

### Dimension Changes

When switching embedding models, align vector schema:

**Convergence order:**
1. Config: `embeddings.embedding_dim` (preferred)
2. Sentinel: `$YAMS_STORAGE/vectors_sentinel.json`
3. Model: reported dimensions (fallback)

**Recommended workflow:**
```bash
yams model download <name> --apply-config  # Updates config + recreates vectors.db
yams repair --embeddings                   # Regenerate vectors
```

Or use doctor:
```bash
yams doctor  # Accept "Recreate vectors.db" → "Restart daemon"
yams repair --embeddings
```

## Maintenance

- **FTS optimize:** After bulk ingest
- **WAL checkpoint:** During low traffic
- **Integrity checks:** Periodic verification on sample sets
- **Backups:** Snapshot entire data directory at consistent points

## Monitoring

**Observability:**
```bash
yams status --json  # Check pool sizes, connections
yams stats -v       # Detailed metrics
```

**Health checks:**
- Implement periodic read/search probes
- Alert on failures, slow queries, error rates

**Key metrics:**
- Throughput: docs/s, bytes/s
- Latency: p50/p95/p99
- Resources: CPU, RSS, IO wait, disk latency (p99)

See [src/daemon/components/DaemonMetrics.cpp](../../src/daemon/components/DaemonMetrics.cpp) for implementation.

## Security

- Run as non-privileged user
- Restrict data directory permissions (0750)
- Avoid secrets in command lines; use environment or secret managers
- For network endpoints: bind to private interface, use reverse proxy, enforce TLS upstream

## Backups

- Snapshot entire data directory
- Quiesce writes during backup (low-traffic windows)
- Store backups off-site with integrity checks
- Test restore procedures periodically

## Troubleshooting

**Slow ingest:**
- Reduce fsync cost (`synchronous=NORMAL`)
- Increase batch and transaction size
- Check disk latency (NVMe performance)

**Slow queries:**
- Run FTS optimize
- Check cache sizing (hot set in memory)
- For vector search: increase search params or pre-filtering

**WAL growth:**
- Schedule checkpoints
- Ensure consumers aren't holding readers open

**Memory pressure:**
- Reduce caches and concurrency
- Lower embedding batch safety, increase inter-batch pauses
- Use Idle auto-embed policy

## Upgrades

- Pin container tags or package versions
- Backup before upgrades
- Review release notes for migrations
- Validate in staging first

---

**References:**
- [User Guide: CLI](../user_guide/cli.md)
- [Developer: Build System](../developer/build_system.md)
- [Architecture: Daemon](../architecture/daemon_architecture.md)
