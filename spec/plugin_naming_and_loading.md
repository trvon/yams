# YAMS Plugin Naming & Loading Spec (Linux/macOS)

Status: draft
Applies to: Native ABI plugins loaded by the YAMS daemon (first‑class support on Linux and macOS)

## Goals
- Provide a deterministic, secure, cross‑platform policy for discovering, trusting, deduplicating,
  and naming plugins at daemon startup and during explicit loads.
- Avoid duplicate providers when both `libyams_foo_plugin.*` and `yams_foo_plugin.*` variants exist.
- Keep Linux and macOS behavior aligned while respecting platform conventions.

## Terminology
- Provider: Logical capability implemented by a plugin (e.g., `model_provider_v1`).
- Variant: Multiple filesystem artifacts that implement the same provider (e.g., different file
  names or directories).

## Discovery Order
The daemon builds a search list in this priority order (earlier is higher priority):
1) Configured plugin directories (daemon config `plugin_dirs`, if present)
2) Environment override (deprecated but supported): `YAMS_PLUGIN_DIR`
3) User: `$HOME/.local/lib/yams/plugins`
4) System: `/usr/local/lib/yams/plugins`, `/usr/lib/yams/plugins`
5) Install prefix (when compiled in): `${CMAKE_INSTALL_PREFIX}/lib/yams/plugins`

Note: Discovery is additive; the daemon attempts to load plugins from each existing directory.

## Trust Model
- Default deny: Plugins must be trusted before loading.
- Trust file: `~/.config/yams/plugins_trust.txt` (one absolute path per line, directory or file).
- CLI helpers: `yams plugin trust add|remove PATH`
- Dev override (not for production): `YAMS_PLUGIN_TRUST_ALL=1` trusts paths discovered by the search list.
- Additional safety checks: world‑writable plugin paths (or parents) are refused.

## Deduplication (Linux/macOS)
To avoid duplicate providers, the daemon deduplicates by provider name with path/file heuristics:
- Prefer non‑`lib` prefixed filenames when both variants exist in the same directory.
- Path priority: user directories outrank system directories; among equals, prefer non‑`lib` prefix.
- If an existing provider is already loaded from a higher‑priority location, lower‑priority variants
  are skipped.

Rationale: This avoids double loading like `libyams_onnx_plugin` + `yams_onnx_plugin` and keeps
behavior consistent across Linux and macOS.

## Name Policy
Controls how a plugin’s display identity is chosen and how file variants are merged.

Modes:
- `relaxed` (default)
  - If no manifest `name` is provided, derive a canonical name from the filename and normalize by
    stripping a leading `lib` from `libyams_*` on UNIX‑like systems.
  - Deduplicate variants by canonical name using the deduplication rules above.
- `spec`
  - Respect the manifest `name` verbatim for display and selection.
  - File variants that implement the same provider are still deduplicated according to the rules
    above; the manifest name remains authoritative for display.

Configuration:
- Env: `YAMS_PLUGIN_NAME_POLICY=spec|relaxed`
- Daemon config: `plugin_name_policy: spec|relaxed`

## Startup Loading Algorithm (Simplified)
1) Build the directory list per Discovery Order.
2) For each directory:
   - Enumerate regular files with extensions: `.so` (Linux), `.dylib` (macOS), `.dll` (Windows).
   - Skip files that do not match `*yams_*_plugin*` (when policy is `spec`, loosen to manifest name).
   - On UNIX, if both `lib<name>` and `<name>` exist, consider only `<name>`.
   - Trust check: refuse untrusted/world‑writable paths.
   - Dlopen + symbol probe (or preflight on macOS): obtain provider identity; apply Name Policy.
   - If provider already loaded, apply path priority and filename preference to keep or replace.
3) Finalize the provider map; only one instance per provider is active.

## Examples
Linux:
```
~/.local/lib/yams/plugins/
  libyams_foo_plugin.so
/usr/local/lib/yams/plugins/
  yams_foo_plugin.so

# With relaxed policy: the user copy wins, canonical name ‘yams_foo_plugin’.
```

macOS:
```
/usr/local/lib/yams/plugins/
  libyams_bar_plugin.dylib
  yams_bar_plugin.dylib

# The non-lib filename is preferred; the lib-prefixed sibling is skipped.
```

## Security Considerations
- Only trusted paths are loaded; world‑writable locations are refused.
- Dev overrides (`YAMS_PLUGIN_TRUST_ALL`) must not be used in production.
- Name Policy does not affect trust; it only affects identity and deduplication.

## Conformance Notes
- Implementations should behave identically on Linux and macOS, with only filename extensions and
  platform preflight checks differing.
- Any divergence must be documented and guarded by platform feature tests.

