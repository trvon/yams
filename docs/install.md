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

The MSI installs `yams.exe`, `yams-daemon.exe` and `yams-mcp-server.exe` in
`bin`. It registers no Windows service; the CLI starts the daemon on demand.
MSIs up to 0.20.3 lack the daemon and the MCP server. The 0.20.3 MSI also
installed a `yams.exe` that could not start because `yams_onnx_resource-0.dll`
was missing. Reinstall from the 0.20.5 MSI.

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
  `daemon.socket_path` in `config.toml`, then the default.
- **PID file:** `--pid-file`, then `daemon.pid_file`, then the default.

If two aliases disagree (`YAMS_DATA_DIR` vs `YAMS_STORAGE`), YAMS refuses to
guess unless a higher-precedence source decides. The full policy is in
[architecture/runtime-paths.md](architecture/runtime-paths.md).

## Running the daemon as a service

### Linux packages

The deb, rpm and Arch packages ship a systemd **user** unit,
`/usr/lib/systemd/user/yams-daemon.service`, and a user preset that enables it
for every user (`systemctl --global preset` on install). The daemon runs as you,
with the same paths as the CLI:

- config `$XDG_CONFIG_HOME/yams/config.toml` (default `~/.config/yams/config.toml`)
- data `$XDG_DATA_HOME/yams` (default `~/.local/share/yams`, or `core.data_dir`)
- socket `$XDG_RUNTIME_DIR/yams-daemon.sock` (or `daemon.socket_path`)
- log `$XDG_STATE_HOME/yams/daemon.log` (default `~/.local/state/yams/daemon.log`)

When upgrading from 0.20.3, follow the [upgrade steps](#upgrading-from-0203)
before starting the unit.

It starts with your next login session. To start it now:

```bash
systemctl --user daemon-reload
systemctl --user enable --now yams-daemon.service
```

The CLI needs no configuration to use it. `yams daemon start`, `stop` and
`restart` drive the unit through `systemctl --user` when it is installed, and
clients that start a daemon on demand start the unit (`systemctl --user
start`) rather than a separate copy, as long as you have not pointed them
elsewhere with `--socket`, `--data-dir`, `YAMS_DAEMON_SOCKET`, `YAMS_DATA_DIR`
or `YAMS_CONFIG`. To keep the daemon running without a login session (for
example on a server), enable lingering once: `sudo loginctl enable-linger "$USER"`.

Plugins (ONNX embeddings, the glint entity extractor) load when your config
enables them; `yams init` writes `auto_load_plugins = true` under `[daemon]`.
Restart the unit after changing the config.

The ONNX and glint plugins load the first ONNX Runtime 1.23 or newer from:

1. `runtime_library` (a file or directory) under `[plugins.onnx]` or
   `[plugins.glint]` in `config.toml`;
2. a system copy: `libonnxruntime.so.1` on the loader path, then the system
   library directories and `/opt/onnxruntime/lib`;
3. the bundled copy in `lib/yams/onnxruntime/` (`/usr/lib/yams/onnxruntime/`
   in the packages).

`YAMS_ONNX_RUNTIME_LIB` (a library file) or `YAMS_ONNX_RUNTIME_DIR` (a
directory) overrides all three. `yams plugin health` and the daemon log line
`[ONNX] Using <source> ONNX Runtime <version> from <path>` show which copy
loaded.

The unit keeps its hardening light (`NoNewPrivileges`, `LockPersonality`,
`RestrictRealtime`, `RestrictSUIDSGID`) so the daemon can read the files you
add from your home directory and `/tmp`.

To opt out for your account: `systemctl --user mask yams-daemon.service`. An
administrator can turn it off for everyone with
`sudo systemctl --global disable yams-daemon.service`, or with a preset file
in `/etc/systemd/user-preset/` containing `disable yams-daemon.service`.

Upgrading reloads running user managers and restarts running user daemons.
Removing the package stops running user daemons and disables the unit; it
never touches `~/.local/share/yams`.

### Upgrading from 0.20.3

Linux package upgrades stop, disable and remove the old system unit. The corpus
in `/var/lib/yams` is kept. The CLI corpus stays in your configured data
directory (default `~/.local/share/yams`).

Configs written by 0.20.3 `yams init` may pin shared `/tmp` paths. In
`~/.config/yams/config.toml`, remove only these two settings from `[daemon]`
if they have the values below:

```toml
socket_path = "/tmp/yams-daemon.sock"
pid_file = "/tmp/yams-daemon.pid"
```

These are lines to remove, not add. A shared socket at `/tmp/yams-daemon.sock`
collides between accounts on the same host. Keep any custom `socket_path` or
`pid_file` you set on purpose.

Then reload and start the user unit:

```bash
systemctl --user daemon-reload
systemctl --user enable --now yams-daemon.service
systemctl --user restart yams-daemon.service
```

`restart` applies the revised config if the daemon is already active. The
default socket is `$XDG_RUNTIME_DIR/yams-daemon.sock`.

### Per-user systemd service without the packages

```bash
yams daemon install --user      # writes ~/.config/systemd/user/yams-daemon.service
yams daemon uninstall --user
```

When the packaged unit is installed, `yams daemon install --user` with no
options just enables it (`systemctl --user enable --now`). With `--socket`,
`--data-dir`, `--config` or `--daemon-binary` it writes
`~/.config/systemd/user/yams-daemon.service`, which overrides the packaged
unit until `yams daemon uninstall --user` removes it. Run as root without
`--user`, it writes a system unit in `/etc/systemd/system`.

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
`--daemon-socket` (or set `YAMS_DAEMON_SOCKET`) to use a specific daemon.

## Uninstall and removing your data

Removing the package leaves your corpus in place. To delete it, remove the data,
state and config directories listed above (for example
`~/.local/share/yams`, `~/.local/state/yams` and `~/.config/yams`). Releases up
to 0.20.3 also left an unused system corpus in `/var/lib/yams`.
