# Environment override policy

Production code reads process environment values through `yams::config`:

- `getenv_optional()` / `getenv_nonempty()` copy string values under the shared environment lock.
- `read_env_bool()`, `read_env_int()`, `read_env_size()`, `read_env_u32()`, `read_env_float()`, `read_env_double()`, and `read_env_milliseconds()` provide strict typed parsing.
- `set_environment()` serializes compatibility writes and unsets through the same boundary.

Boolean overrides accept `1/0`, `true/false`, `yes/no`, and `on/off`, case-insensitively. Unset or empty values preserve the typed default. Unknown values emit a bounded warning and also preserve the typed default; a typo must not silently enable a feature.

## Vector compatibility aliases

`resolve_vector_environment()` is the single compatibility boundary for:

- `YAMS_DISABLE_VECTORS`
- `YAMS_DISABLE_VECTOR`
- `YAMS_DISABLE_VECTOR_DB`

A valid true value from any alias disables vectors for initialization, search, repair, and health probes. Valid false values do not override an already-disabled typed default. If true and false aliases conflict, disable wins conservatively and the policy reports a diagnostic. Invalid values are ignored with a warning.

Typed configuration remains the product authority. Compatibility environment overlays are retained for existing deployments and isolated test fixtures; new tuning surfaces belong in typed configuration rather than new `YAMS_*` variables.
