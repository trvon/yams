#pragma once

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <yams/core/types.h>

namespace yams::config {

/// A copied process-environment value with strict parse state.
///
/// `present && !value` is an invalid configured value. `!present` means the variable was not set.
template <typename T> struct ParsedEnvironmentValue {
    bool present{false};
    std::optional<T> value;
    std::string raw;

    [[nodiscard]] bool invalid() const noexcept { return present && !value.has_value(); }
    [[nodiscard]] T valueOr(T fallback) const { return value.value_or(fallback); }
};

/// Copy one environment value at the process-environment boundary, preserving an explicitly empty
/// value. All production environment reads should pass through this shared boundary.
std::optional<std::string> getenv_optional(std::string_view key);

/// Copy a non-empty environment value; unset and explicitly empty values return nullopt.
std::optional<std::string> getenv_nonempty(std::string_view key);

/// Copy every present process-environment entry whose name starts with `prefix` while holding the
/// shared boundary lock. The returned map is an immutable point-in-time lifecycle input.
std::map<std::string, std::string> snapshot_environment_prefix(std::string_view prefix);

/// Set or unset one process-environment value under the same boundary lock used by readers.
/// A null value unsets the key. Returns false when the platform mutation fails. All first-party
/// writers must use this boundary so same-value ABA writes remain observable to lease ownership.
bool set_environment(const char* key, const char* value) noexcept;

/// Lease a value and return its monotonic ownership token. Nested leases form a per-key chain, so
/// restoring an inner lease resumes its predecessor without allowing unrelated writers to be
/// overwritten.
std::optional<std::uint64_t> set_environment_owned(const char* key, const char* value) noexcept;

enum class EnvironmentRestoreResult { Restored, Released, OwnershipLost, Error };

/// Release one lease by token. A current lease restores its captured predecessor; an outer lease
/// released out of order is spliced from the ownership chain without changing the newer value.
/// Writers using `set_environment()` invalidate the chain, including same-value ABA writes. A raw
/// writer that bypasses the required boundary is detectable only when it changes the text.
EnvironmentRestoreResult restore_environment_if_owned(const char* key,
                                                      std::uint64_t installedGeneration) noexcept;

#ifdef YAMS_TESTING
/// Fail one owned lease acquisition after `successfulLeases` successful acquisitions.
/// The one-shot hook resets when it fires.
void testing_fail_owned_environment_lease_after(std::size_t successfulLeases) noexcept;

/// Make the next owned-lease release return Error after invalidating its ownership chain.
void testing_fail_owned_environment_restore_once() noexcept;
#endif

/// Compatibility accessor that maps unset and explicitly empty values to an empty string.
std::string getenv_copy(const char* key);

/// Strict typed environment readers. Invalid values emit a bounded warning and leave `value`
/// empty so callers can preserve their typed default.
ParsedEnvironmentValue<bool> read_env_bool(std::string_view key);
ParsedEnvironmentValue<std::size_t> read_env_size(std::string_view key);
ParsedEnvironmentValue<int> read_env_int(std::string_view key);
ParsedEnvironmentValue<std::uint32_t> read_env_u32(std::string_view key);
ParsedEnvironmentValue<float> read_env_float(std::string_view key);
ParsedEnvironmentValue<double> read_env_double(std::string_view key);
ParsedEnvironmentValue<std::chrono::milliseconds> read_env_milliseconds(std::string_view key);

/// Effective vector compatibility policy. A valid true value from any disable alias wins
/// conservatively; false and invalid aliases do not override a disabled typed default.
struct VectorEnvironmentPolicy {
    bool enabled{true};
    std::vector<std::string> disableSources;
    std::vector<std::string> diagnostics;
};

VectorEnvironmentPolicy resolve_vector_environment(bool configuredEnabled = true);

// String trimming utilities
inline void ltrim(std::string& s) {
    s.erase(s.begin(),
            std::find_if(s.begin(), s.end(), [](unsigned char ch) { return !std::isspace(ch); }));
}

inline void rtrim(std::string& s) {
    s.erase(std::find_if(s.rbegin(), s.rend(), [](unsigned char ch) { return !std::isspace(ch); })
                .base(),
            s.end());
}

inline void trim(std::string& s) {
    ltrim(s);
    rtrim(s);
}

// Quote handling
inline std::string unquote(std::string val) {
    trim(val);
    if (val.size() >= 2 && ((val.front() == '"' && val.back() == '"') ||
                            (val.front() == '\'' && val.back() == '\''))) {
        return val.substr(1, val.size() - 2);
    }
    return val;
}

// Tilde expansion
std::filesystem::path expand_tilde(const std::string& path);

// Terminal sanitization
std::string sanitize_for_terminal(std::string_view in);

// Time parsing
inline std::chrono::milliseconds parse_ms(std::string_view s) {
    try {
        return std::chrono::milliseconds(std::stol(std::string(s)));
    } catch (...) {
        return std::chrono::milliseconds(0);
    }
}

/// Parse common config boolean spellings. Unknown or empty values return fallback.
inline bool parse_bool(std::string_view value, bool fallback = false) {
    std::string normalized(value);
    trim(normalized);
    std::transform(normalized.begin(), normalized.end(), normalized.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    if (normalized.empty()) {
        return fallback;
    }
    if (normalized == "1" || normalized == "true" || normalized == "yes" || normalized == "on") {
        return true;
    }
    if (normalized == "0" || normalized == "false" || normalized == "no" || normalized == "off") {
        return false;
    }
    return fallback;
}

// Read one ordinary scalar through the shared flat-TOML parser.
std::string parse_config_value(const std::filesystem::path& config_path, const std::string& section,
                               const std::string& key);

// Parse a comma- or TOML-array-separated list of paths into filesystem paths.
// Accepts forms like "a,b" or ["a", "b"]. Tilde expansion is applied.
std::vector<std::filesystem::path> parse_path_list(const std::string& raw);

// Resolve the ordinary config path: explicit argument, existing YAMS_CONFIG_PATH compatibility
// override, YAMS_CONFIG, then the platform config default. The path need not exist.
std::filesystem::path get_config_path(const std::string& override_path = "");

/// Origin of one effective runtime path.
enum class RuntimePathSource {
    Explicit,
    ConfigFile,
    Environment,
    PlatformDefault,
};

/// One resolved path plus the authority that selected it.
struct ResolvedRuntimePath {
    std::filesystem::path value;
    RuntimePathSource source{RuntimePathSource::PlatformDefault};
    std::string sourceName;
};

/// Explicit typed overrides. A populated field has precedence over config, environment aliases,
/// and platform defaults.
struct RuntimePathOverrides {
    std::optional<std::filesystem::path> configFile;
    std::optional<std::filesystem::path> dataDir;
    std::optional<std::filesystem::path> runtimeDir;
    std::optional<std::filesystem::path> socketPath;
    std::optional<std::filesystem::path> pidFile;
};

/// Immutable effective path snapshot shared by daemon, client, CLI, MCP, and services.
///
/// Precedence is explicit typed override, supported config key, compatibility environment alias,
/// then platform default. Data config intentionally precedes environment aliases for compatibility
/// with the established CLI contract. Conflicting aliases fail unless a higher-precedence explicit
/// or configured data path makes them inactive; inactive conflicts are retained in diagnostics.
struct ResolvedRuntimePaths {
    ResolvedRuntimePath configFile;
    ResolvedRuntimePath dataDir;
    ResolvedRuntimePath runtimeDir;
    ResolvedRuntimePath socketPath;
    ResolvedRuntimePath pidFile;
    std::vector<std::string> diagnostics;
};

Result<ResolvedRuntimePaths>
resolve_runtime_paths(const RuntimePathOverrides& overrides = RuntimePathOverrides{});

// Platform-specific directory resolution (follows OS best practices)
// Windows: APPDATA (roaming) for config, LOCALAPPDATA for data/cache/runtime
// Unix/macOS: XDG Base Directory Specification

/// Returns the user config directory
/// Windows: %APPDATA%\yams
/// Unix: $XDG_CONFIG_HOME/yams or ~/.config/yams
std::filesystem::path get_config_dir();

/// Returns the user data directory (databases, indices, persistent storage)
/// Windows: %LOCALAPPDATA%\yams
/// Unix: $XDG_DATA_HOME/yams or ~/.local/share/yams
std::filesystem::path get_data_dir();

/// Returns the user cache directory (temporary files, caches)
/// Windows: %LOCALAPPDATA%\yams\cache
/// Unix: $XDG_CACHE_HOME/yams or ~/.cache/yams
std::filesystem::path get_cache_dir();

/// Returns the runtime directory (sockets, PIDs, ephemeral files)
/// Windows: %LOCALAPPDATA%\yams
/// Unix: $XDG_RUNTIME_DIR/yams or /tmp/yams-$UID
std::filesystem::path get_runtime_dir();

/// Returns the state directory (logs and durable runtime metadata)
/// Windows: %LOCALAPPDATA%\yams\state
/// Unix: $XDG_STATE_HOME/yams or ~/.local/state/yams
std::filesystem::path get_state_dir();

/// Returns the canonical daemon plugin trust file path.
/// Default: <data_dir>/plugins.trust (or YAMS_PLUGIN_TRUST_FILE override).
std::filesystem::path get_daemon_plugin_trust_file();

/// Returns the legacy plugin trust file path used by older CLI tooling.
/// Default: <config_dir>/plugins_trust.txt
std::filesystem::path get_legacy_plugin_trust_file();

// Compatibility path accessors backed by resolve_runtime_paths(). They throw
// std::invalid_argument when active aliases conflict instead of returning an empty/CWD-relative
// path.
std::filesystem::path resolve_socket_path_from_config();
std::filesystem::path resolve_pid_file_from_config();
std::filesystem::path resolve_data_dir_from_config();
std::string resolve_daemon_mode_from_config();

/// Returns the daemon runtime directory, creating it when possible.
std::filesystem::path get_daemon_runtime_dir();

/// Returns the daemon status JSON path under the runtime directory.
std::filesystem::path get_daemon_status_file();

/// Returns the effective daemon PID file path, including platform defaults.
std::filesystem::path resolve_daemon_pid_file_path();

/// Returns the effective daemon log file path, including platform defaults.
std::filesystem::path resolve_daemon_log_file_path();

// ============================================================================
// Ordinary flat-TOML config parsing and writing utilities
// ============================================================================

/// Parse ordinary scalar lookups into a map with section.key format.
/// e.g., [embeddings] dim = 384 becomes "embeddings.dim" -> "384". Single/double quoted
/// values and quote-aware inline comments are supported; arrays remain raw strings. Migration,
/// multiline TOML, nested objects, and schema validation belong to ConfigMigrator.
std::map<std::string, std::string> parse_simple_toml(const std::filesystem::path& path);

/// Dimension configuration from config file
struct DimensionConfig {
    std::optional<size_t> embeddings; // embeddings.embedding_dim
    std::optional<size_t> vectorDb;   // vector_database.embedding_dim
    std::optional<size_t> index;      // vector_index.dimension
};

/// Read dimension-related config values
DimensionConfig read_dimension_config(const std::filesystem::path& config_path);

/// Write dimension config values (updates all dimension keys)
bool write_dimension_config(const std::filesystem::path& config_path, size_t dim);

/// Write or update a single config value (section.key format)
/// Creates the file and parent directories if they don't exist
bool write_config_value(const std::filesystem::path& config_path, const std::string& key,
                        const std::string& value);

/// Write multiple config values atomically
bool write_config_values(const std::filesystem::path& config_path,
                         const std::map<std::string, std::string>& values);

} // namespace yams::config
