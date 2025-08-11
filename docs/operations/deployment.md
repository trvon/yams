# YAMS Deployment (Operations)

Deploy YAMS with the steps below.

## TL;DR (Linux + systemd, MCP over WebSocket)

```bash
# 1) Install (example: local prefix)
cmake -S . -B build -DYAMS_BUILD_PROFILE=release -DYAMS_BUILD_DOCS=ON
cmake --build build -j
sudo cmake --install build --prefix /usr/local

# 2) Create runtime user and directories
sudo useradd --system --create-home --home-dir /var/lib/yams --shell /usr/sbin/nologin yams || true
sudo install -d -o yams -g yams -m 0750 /var/lib/yams         # data root (YAMS_STORAGE)
sudo install -d -o yams -g yams -m 0700 /etc/yams             # config
sudo install -d -o yams -g yams -m 0750 /var/log/yams         # logs (optional)

# 3) Initialize storage (one-time)
sudo -u yams env YAMS_STORAGE=/var/lib/yams yams init --non-interactive

# 4) Systemd unit (WebSocket transport on localhost:8080)
cat <<'UNIT' | sudo tee /etc/systemd/system/yams.service >/dev/null
[Unit]
Description=YAMS MCP Server
After=network-online.target
Wants=network-online.target

[Service]
User=yams
Group=yams
Environment=YAMS_STORAGE=/var/lib/yams
WorkingDirectory=/var/lib/yams
ExecStart=/usr/local/bin/yams serve --transport websocket --host 127.0.0.1 --port 8080 --path /mcp
Restart=on-failure
RestartSec=2s
LimitNOFILE=65536
# Hardening (tighten as needed)
ProtectSystem=full
ProtectHome=true
PrivateTmp=true
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
UNIT

# 5) Start and verify
sudo systemctl daemon-reload
sudo systemctl enable --now yams
sudo systemctl status yams --no-pager
journalctl -u yams -n 50 --no-pager
```

Notes
- For macOS or containers, see the sections below.
- Keep YAMS_STORAGE owned by yams:yams; keys are 0600 (created by init).

---

## Deployment models

- Bare metal (systemd): recommended for simplicity and journald logs.
- macOS (launchd): use a LaunchDaemon/LaunchAgent with a plist.
- Container: run `yams serve` within a container; mount persistent storage and config.

Choose a model appropriate for your environment.

---

## Paths, users, and permissions

- Data root (required):
  - Env: `YAMS_STORAGE=/var/lib/yams`
  - Created by you; `yams init` ensures subdirs + SQLite DB
- Config (optional explicit path):
  - `~/.config/yams` for the service user (yams)
  - Or set `XDG_CONFIG_HOME=/etc/yams` for a system config location
- Ownership:
  - Everything under `YAMS_STORAGE` should be owned by `yams:yams`
  - Private keys are created 0600

Quick audit
```bash
sudo tree -pug /var/lib/yams | head -50
sudo ls -l /etc/yams || true
```

---

## Systemd hardening (recommended)

Append to the unit if your distro permits:
```
ProtectKernelTunables=true
ProtectKernelModules=true
ProtectControlGroups=true
RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX
AmbientCapabilities=
CapabilityBoundingSet=
```

Tighten incrementally; test before rolling to prod.

---

## macOS (launchd) sketch

- Create a LaunchDaemon plist at `/Library/LaunchDaemons/com.yams.mcp.plist`
- Run as a dedicated user; set `EnvironmentVariables` for `YAMS_STORAGE`
- ProgramArguments example:
  - `/usr/local/bin/yams`, `serve`, `--transport`, `websocket`, `--host`, `127.0.0.1`, `--port`, `8080`, `--path`, `/mcp`
- Load: `sudo launchctl load -w /Library/LaunchDaemons/com.yams.mcp.plist`
- Logs via `log stream --predicate 'process == "yams"'`

Convert the working systemd flags to plist arrays.

---

## Container (example)

Dockerfile (minimal)
```dockerfile
FROM debian:stable-slim
RUN apt-get update && apt-get install -y ca-certificates libssl3 sqlite3 && rm -rf /var/lib/apt/lists/*
# Copy your locally built yams binary
COPY yams /usr/local/bin/yams
RUN useradd -r -m -d /var/lib/yams yams
USER yams
ENV YAMS_STORAGE=/var/lib/yams
WORKDIR /var/lib/yams
# Initialize on first run if needed
ENTRYPOINT ["/usr/local/bin/yams", "serve", "--transport", "websocket", "--host", "0.0.0.0", "--port", "8080", "--path", "/mcp"]
```

Run:
```bash
docker build -t yams:local .
docker run -d --name yams -p 8080:8080 -v yams_data:/var/lib/yams yams:local
```

---

## Security recommendations

- Run as a dedicated non‑login user (yams)
- Limit scope: bind `--host 127.0.0.1` and put a reverse proxy in front if needed
- File permissions:
  - data root 0750
  - keys 0600
- Network: open only required ports (e.g., 8080 if using WebSocket)
- Keep OpenSSL up to date (OS packages)
- Rotate API keys/tokens periodically (when applicable in your setup)

---

## Upgrades and rollbacks

- Zero‑downtime aim:
  - If single instance, plan a short maintenance window
  - If fronted by a proxy/load‑balancer, rotate instances
- Minimal flow:
```bash
sudo systemctl stop yams
# Optional backup (see Backup section)
sudo cmake --install build --prefix /usr/local
sudo systemctl start yams
journalctl -u yams -n 50 --no-pager
```
- Rollback:
  - Keep previous artifact or package
  - `sudo cmake --install <old_build> --prefix /usr/local` then restart

---

## Health checks (basic)

- Process/service:
  - `systemctl is-active yams`
- Logical:
  - `yams stats` (run as the same user with `YAMS_STORAGE` set)
- TCP (for WebSocket):
  - `nc -z 127.0.0.1 8080 || exit 1`

Integrate with your monitoring to page on failures.

---

## Reverse proxy (optional)

Nginx (example)
```nginx
server {
  listen 80;
  server_name yams.local;

  location /mcp {
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_set_header Host $host;

    proxy_pass http://127.0.0.1:8080/mcp;
  }
}
```

Use TLS (letsencrypt/certbot) for internet‑facing endpoints.

---

## Backup & Recovery (overview)

- What to back up:
  - Data root: `$YAMS_STORAGE` (includes storage/ and yams.db)
  - Config: `~/.config/yams` (or `/etc/yams` if you use that)
- Safe SQLite backup (online):
```bash
sqlite3 /var/lib/yams/yams.db ".backup '/var/backups/yams-$(date +%F).db'"
```
- Offline backup (simple):
  - `systemctl stop yams`
  - `rsync -a /var/lib/yams/ /var/backups/yams/$(date +%F)/`
  - `systemctl start yams`

Document your restore drills. Test them.

---

## Monitoring (overview)

- Logs:
  - `journalctl -u yams -f`
- Metrics:
  - Wrap `yams stats` with a small exporter script or sidecar
- Alerts:
  - Process liveness, port checks, error rates in logs, disk usage on `$YAMS_STORAGE`

---

## Performance (overview)

- Disk: SSD recommended (metadata + content)
- Filesystem:
  - Ensure sufficient inodes and `noatime` if appropriate
- Concurrency:
  - Default settings are conservative; tune only after measuring
- Snippets/preview trade‑offs:
  - Larger snippets improve UX but increase CPU/I/O

---

## Troubleshooting (quick)

- Service won’t start:
  - `journalctl -u yams -n 200 --no-pager`
  - Verify `YAMS_STORAGE` ownership and permissions
- Cannot write data:
  - Check free disk space and directory perms
- Port in use:
  - `ss -lntp | grep 8080` (Linux)
  - Adjust `--port` or stop the conflicting service
- Reset a bad init:
  - Stop service → move/backup `/var/lib/yams` → re‑`yams init`

---

## See also

- Monitoring: `./monitoring.md` (coming soon)
- Backup & Recovery: `./backup.md` (coming soon)
- Troubleshooting: `./troubleshooting.md` (coming soon)
- Performance Tuning: `./performance.md` (coming soon)
- CLI Reference (authoritative for flags): `../user_guide/cli.md`
- Admin Configuration: `../admin/configuration.md`
