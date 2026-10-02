# Install and set up YAMS

YAMS runs entirely on your machine. Pick one install channel, run `yams init`,
and your corpus lives under your own data directory. Nothing here needs an
account or a hosted service.

## Install

### macOS (Homebrew)

```bash
brew install trvon/yams/yams
```

The formula installs prebuilt `yams`, `yams-daemon` and `yams-mcp-server`
binaries plus shell completions for bash, zsh and fish. To run the daemon as a
login service:

```bash
brew services start yams      # stop with: brew services stop yams
```

The Homebrew service logs to `$(brew --prefix)/var/log/yams-daemon.log`.
A nightly formula, `yams-nightly`, tracks the experimental branch and conflicts
with `yams`.

### Debian and Ubuntu (APT)

```bash
curl -fsSL https://repo.yamsmemory.ai/gpg.key \
  | sudo gpg --dearmor -o /usr/share/keyrings/yams-stable.gpg
echo "deb [arch=amd64,arm64 signed-by=/usr/share/keyrings/yams-stable.gpg] https://repo.yamsmemory.ai/aptrepo stable main" \
  | sudo tee /etc/apt/sources.list.d/yams-stable.list
sudo apt-get update && sudo apt-get install yams
```

The repository metadata (`Release`, `InRelease`) is signed with the
`YAMS Memory <packages@yamsmemory.ai>` key.

### Fedora, RHEL and derivatives (DNF/YUM)

```bash
sudo tee /etc/yum.repos.d/yams.repo <<'REPO'
[yams]
name=YAMS Repository
baseurl=https://repo.yamsmemory.ai/yumrepo/
enabled=1
gpgcheck=0
repo_gpgcheck=1
gpgkey=https://repo.yamsmemory.ai/gpg.key
REPO
sudo dnf makecache && sudo dnf install yams
```

The repository metadata is signed; the RPM files themselves are not, hence
`gpgcheck=0`.

### Arch Linux (pacman)

```bash
sudo tee -a /etc/pacman.conf <<'REPO'

[yams]
SigLevel = Optional TrustAll
Server = https://repo.yamsmemory.ai/archrepo/os/$arch
REPO
sudo pacman -Sy yams
```

Packages are built for `x86_64` and `aarch64`. This repository is unsigned.

### Docker

```bash
docker pull ghcr.io/trvon/yams:latest
docker run --rm -it \
  -v "$HOME/.local/share/yams:/home/yams/.local/share/yams" \
  -v "$HOME/.config/yams:/home/yams/.config/yams" \
  ghcr.io/trvon/yams:latest init
```

The image's entrypoint is `yams`. It runs as the `yams` user and keeps its
corpus in `/home/yams/.local/share/yams` and its configuration in
`/home/yams/.config/yams`; mount both to keep data between runs. Release tags
are published as `X.Y.Z`, `X.Y` and `X`.

### Windows

Download `yams-<version>-windows-x86_64.msi` from the
[latest release](https://github.com/trvon/yams/releases/latest) and run it.
It installs per machine to `%ProgramFiles%\YAMS` and adds `bin` to `PATH`.

> The MSI currently ships the `yams` CLI and plugins but not `yams-daemon.exe`
> or `yams-mcp-server.exe`, so daemon-backed commands and `yams serve` do not
> work from the MSI yet.

### Release archives and source

Every [release](https://github.com/trvon/yams/releases) also carries
`yams-<version>-linux-{x86_64,arm64}.tar.gz`, `.deb`, `.rpm`, Arch
`.pkg.tar.zst`, and `yams-<version>-macos-{arm64,x86_64}.zip`, with
`SHA256SUMS` and a signature (`SHA256SUMS.asc`). To build from source, see
[BUILD.md](BUILD.md).

### Experimental channel

Package repositories for the `experimental` channel are not published yet;
only the stable repositories above are live. To try experimental builds, use
the Homebrew `yams-nightly` formula or build from source.

## First run

```bash
yams init                     # writes config.toml and a signing key pair
yams add ./notes --recursive --tags notes
yams search "what did I decide about caching"
yams daemon status
```

`yams init` accepts `--non-interactive`, `--auto`, `--force` and
`--no-keygen`. The CLI starts a per-user daemon on demand; you do not need to
run a service for normal use.

## Where things live

YAMS resolves every path once, in the same order for the CLI, daemon and MCP
server. On Linux and macOS it follows the XDG base directories; macOS uses the
same locations as Linux.

| What | Linux and macOS | Windows |
|---|---|---|
| Config file | `$XDG_CONFIG_HOME/yams/config.toml` (default `~/.config/yams/config.toml`) | `%APPDATA%\yams\config.toml` |
| Data (corpus, databases) | `$XDG_DATA_HOME/yams` (default `~/.local/share/yams`) | `%LOCALAPPDATA%\yams` |
| State (sessions) | `$XDG_STATE_HOME/yams` (default `~/.local/state/yams`) | `%LOCALAPPDATA%\yams\state` |
| Cache | `$XDG_CACHE_HOME/yams` (default `~/.cache/yams`) | `%LOCALAPPDATA%\yams\cache` |
| Daemon socket | `$XDG_RUNTIME_DIR/yams-daemon.sock`, else `/tmp/yams-daemon-<uid>.sock` | `%LOCALAPPDATA%\yams\yams-daemon.sock` |
| Daemon log | `$XDG_STATE_HOME/yams/daemon.log` (default `~/.local/state/yams/daemon.log`), rotated at 10 MB × 5 | `%LOCALAPPDATA%\yams\daemon.log` |
| User-installed plugins | `~/.local/lib/yams/plugins` | `%LOCALAPPDATA%\yams\plugins` |
| Plugin trust list | `<data dir>/plugins.trust` | same |

Packaged binaries are installed under `/usr/bin` on Linux packages
(`yams`, `yams-daemon`, `yams-mcp-server`), with bundled plugins in
`/usr/lib/yams/plugins`.

### Overriding paths

Highest precedence first:

- **Config file:** `--config`, then `YAMS_CONFIG`, then the default.
- **Data directory:** `--data-dir`, then `core.data_dir` in `config.toml`, then
  `YAMS_DATA_DIR` (or the older `YAMS_STORAGE`), then the default. A data
  directory set in `config.toml` wins over the environment variables.
- **Daemon socket:** `--socket`, then `YAMS_DAEMON_SOCKET`, then
  `daemon.socket_path` in `config.toml`, then the default. Clients (not the
  daemon) may use the packaged system daemon's socket instead of the default;
  see [Linux packages](#linux-packages).
- **PID file:** `--pid-file`, then `daemon.pid_file`, then the default.

If two aliases disagree (`YAMS_DATA_DIR` vs `YAMS_STORAGE`), YAMS refuses to
guess unless a higher-precedence source decides. The full policy is in
[architecture/runtime-paths.md](architecture/runtime-paths.md).

## Running the daemon as a service

### Linux packages

The deb, rpm and Arch packages install, enable and start a **system** service,
`yams-daemon.service`. It runs as the unprivileged `yams` system account
(created from `/usr/lib/sysusers.d/yams.conf`) with:

- data in `/var/lib/yams`
- socket at `/run/yams/yams-daemon.sock`, group `yams` read/write, no access for others
- log at `/var/log/yams/daemon.log`

Root and members of the `yams` group can use it. Add your account and log in
again so the new group applies:

```bash
sudo usermod -aG yams "$USER"
```

The CLI then finds the system daemon on its own, with no environment
variables, whenever it has nothing more specific to use. The order is:

1. a socket you chose: `--socket`, `YAMS_DAEMON_SOCKET`, or
   `daemon.socket_path` in `config.toml`;
2. your per-user daemon, if its socket exists;
3. your per-user daemon, if you named a personal data directory
   (`--data-dir`, `YAMS_DATA_DIR`, or `core.data_dir`, which `yams init`
   writes);
4. the system daemon, if `/run/yams/yams-daemon.sock` exists and you can open
   it;
5. otherwise the per-user default, started on demand.

So after `yams init` you keep a personal corpus. To use the shared system
corpus from such an account, set `socket_path = "/run/yams/yams-daemon.sock"`
under `[daemon]` in your `config.toml`. Anyone in the `yams` group can read,
add and delete documents in the system corpus, so add only users you trust
with all of it.

The service is sandboxed: it cannot see `/home`, `/root` or the real `/tmp`,
and it runs as a different user from you. When the CLI talks to a daemon that
runs as another user, `yams add <file>` sends the file's content (up to 15 MiB
per file) instead of its path. Directories cannot be added that way, because
the daemon walks them itself; add their files individually, place them where
the `yams` account can read them (for example under `/srv`), or use a per-user
daemon. `yams daemon stop` and `restart` do not touch the system daemon; use
`sudo systemctl stop|restart yams-daemon.service`. The service has no device
access (`PrivateDevices=yes`); to use a GPU embedding backend from it, relax
that in a drop-in (`sudo systemctl edit yams-daemon.service`).

If you prefer your own per-user daemon and data directory, turn the system
service off:

```bash
sudo systemctl disable --now yams-daemon.service
```

Removing the package stops and disables the service. `apt purge yams` also
deletes `/var/lib/yams` and `/var/log/yams`; `dnf remove` and `pacman -R`
keep them. The `yams` account stays in every case.

Upgrading from 0.20.x, which ran the service as a dynamic user, keeps the
corpus: systemd moves `/var/lib/private/yams` back to `/var/lib/yams` and
hands it to the `yams` account on the first start.

### Per-user systemd service

```bash
yams daemon install --user      # writes ~/.config/systemd/user/yams-daemon.service
yams daemon uninstall --user
```

`yams daemon install` accepts `--socket`, `--data-dir`, `--config` and
`--daemon-binary`. Run as root without `--user`, it writes
`/etc/systemd/system/yams-daemon.service`, which takes precedence over the
packaged unit of the same name.

### Everyday daemon commands

```bash
yams daemon start | stop | restart
yams daemon status -d          # detailed status, including startup phases
yams daemon log -f             # follow the daemon log
yams daemon doctor
```

## Settings worth knowing

All settings live in `config.toml`. [`examples/config.toml`](../examples/config.toml)
lists the full set; copy only the sections you change, because a copied
`data_dir` or socket path replaces the defaults above.

| Setting | Default | What it does |
|---|---|---|
| `[core] data_dir` | platform data dir | Where the corpus lives. `yams init` writes it. |
| `[daemon] log_level` | `info` | Daemon log verbosity. |
| `[embeddings] enable` | `true` | Set `false` to run text and grep search without embeddings. |
| `[daemon.maintenance] vector_vacuum_interval_hours` | `24` | How often idle maintenance checks whether `vectors.db` is worth compacting. `0` disables. |
| `[daemon.maintenance] session_expiry_days` | `30` | Idle session files older than this are removed. Sessions that are current, watch an existing directory, or still tag documents are kept. `0` disables. |
| `[tuning.resource] host_pressure` | `true` | Defer background work (topology rebuilds, repair scans, vacuum, backfills) while the machine is busy. |
| `[tuning.resource] host_cpu_pressure_pct` | `40` | CPU pressure (Linux PSI `some avg10`) above which the host counts as busy. Raise it on shared build hosts. |
| `[tuning.resource] background_max_deferral_s` | `300` | Longest background work waits before it runs anyway. `0` never defers. |
| `[tuning] profile` | `balanced` | `efficient`, `balanced` or `aggressive` resource use. |

`yams daemon status` shows the current host load and which background work is
deferred.

## Connect an agent over MCP

```bash
yams serve
```

```json
{
  "mcpServers": {
    "yams": { "command": "yams", "args": ["serve"] }
  }
}
```

`yams serve` speaks MCP over stdio and talks to the local daemon; pass
`--daemon-socket` (or set `YAMS_DAEMON_SOCKET`) to use a specific daemon, such
as the packaged system service.

## Uninstall and removing your data

Removing the package leaves your corpus in place. To delete it, remove the data,
state and config directories listed above (for example
`~/.local/share/yams`, `~/.local/state/yams` and `~/.config/yams`). For the
packaged system service, the data is in `/var/lib/yams`.
