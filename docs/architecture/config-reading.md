# Configuration reading boundaries

YAMS has two intentionally different TOML surfaces.

## Ordinary lookups

Runtime scalar lookups use `yams::config::get_config_path()` and
`yams::config::parse_simple_toml()` from `yams_config`.

`get_config_path()` applies one precedence:

1. explicit function argument or typed runtime-path override;
2. an existing `YAMS_CONFIG_PATH` compatibility file;
3. canonical `YAMS_CONFIG`;
4. the platform config default.

The flat reader returns `section.key` strings. It preserves single- and
double-quoted scalar values, ignores quote-aware inline comments (including
mixed and escaped quotes), retains dotted keys, and leaves arrays as raw strings
for typed callers to decode. Path-list decoding splits only on unquoted commas.
It is not a
complete TOML implementation: multiline strings, quoted keys, nested objects,
escape interpretation, and schema validation are outside this seam.

Daemon policies may retain typed resolver methods, but those methods read the
same flat map. An explicit daemon config normalizes both `YAMS_CONFIG` and the
compatibility alias before dependent components are constructed, then restores
the caller's environment at teardown. App services, CLI commands, MCP prompt/downloader settings,
storage bootstrap, and content-store compression settings must not implement
local line parsers or reconstruct XDG/Windows config paths.

## Migration and schema operations

`ConfigMigrator::parseTomlConfig()` remains separate for migration, schema
validation, version upgrades, generated defaults, and complete daemon config
reconstruction. Ordinary MCP or service lookups must not use the migrator.

## Intentional exceptions

- Prompt discovery keeps prompt-specific precedence: `YAMS_MCP_PROMPTS_DIR`,
  `mcp_server.prompts_dir`, the resolved data directory's `prompts/` folder,
  then repository `docs/prompts` for development.
- Storage bootstrap receives an explicit config path from its caller; only its
  file parsing is shared.
- Config writers and migration code preserve comments/layout through their own
  update machinery rather than round-tripping the flat map.
- Benchmark controls and product tuning use dedicated typed configuration surfaces; this reader
  does not create new tuning authority.
