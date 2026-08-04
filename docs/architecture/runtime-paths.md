# Runtime path authority

YAMS resolves process paths once through `yams::config::resolve_runtime_paths()` and passes the resulting `ResolvedRuntimePaths` snapshot to daemon, client, CLI, MCP, and service code.

## Precedence

| Path | Precedence (highest first) |
|---|---|
| Config file | explicit `--config`/typed override, `YAMS_CONFIG`, platform config default |
| Data directory | explicit `--data-dir`/typed override, TOML `core.data_dir`, compatible `[daemon]` data keys, `YAMS_DATA_DIR` or legacy `YAMS_STORAGE`, platform data default |
| Runtime directory | explicit typed override, `XDG_RUNTIME_DIR`/Windows runtime default, per-user temporary fallback |
| Daemon socket | explicit `--socket`/typed override, `YAMS_DAEMON_SOCKET` or compatibility `YAMS_DAEMON_SOCKET_PATH`, TOML `daemon.socket_path`, platform socket default |
| PID file | explicit `--pid-file`/typed override, TOML `daemon.pid_file`, platform PID default |

Every resolved field records its `RuntimePathSource` and source name. The snapshot also carries diagnostics for inactive lower-precedence conflicts.

## Alias conflicts

Compatibility aliases remain supported. Equal aliases are accepted. Unequal aliases fail closed when they would decide the effective path:

- `YAMS_DATA_DIR` versus `YAMS_STORAGE`
- `YAMS_DAEMON_SOCKET` versus `YAMS_DAEMON_SOCKET_PATH`

An explicit or configured data directory safely disambiguates conflicting data aliases; YAMS uses the higher-precedence value and emits a warning. An explicit socket similarly disambiguates socket aliases. This prevents daemon and client processes from silently selecting different corpora or sockets while preserving existing isolated fixtures that export equal aliases.

## Platform defaults

Unix follows XDG config/data/runtime homes and then `HOME`; daemon socket and PID fallbacks retain per-user `/tmp` behavior, while root retains `/var/run`. Windows uses `APPDATA`, `LOCALAPPDATA`, and `USERPROFILE` fallbacks. Callers should not reconstruct these branches locally.
