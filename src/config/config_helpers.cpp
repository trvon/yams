#include <array>
#include <charconv>
#include <fstream>
#include <limits>
#include <map>
#include <mutex>
#include <stdexcept>
#include <unordered_set>

#include <spdlog/spdlog.h>

#include <yams/common/fs_utils.h>
#include <yams/common/string_utils.h>
#include <yams/config/config_helpers.h>
#include <yams/config/detail/config_parse_utils.h>

#ifdef _WIN32
#if defined(__has_include)
#if __has_include(<process.h>)
#include <process.h>
#endif
#else
#include <process.h>
#endif
#include <shlobj.h>
#include <windows.h>
#else
#include <unistd.h> // For geteuid(), getpid(), getuid()
#endif

namespace yams::config {
namespace {

std::mutex& processEnvironmentMutex() {
    // Environment reads occur from process-lifetime singleton teardown paths. Keep this boundary
    // alive until process exit rather than depending on cross-translation-unit destruction order.
    static auto* mutex = new std::mutex();
    return *mutex;
}

void warnEnvironmentOnce(const std::string& message) {
    static auto* warningMutex = new std::mutex();
    static auto* warnings = new std::unordered_set<std::string>();
    bool firstOccurrence = false;
    {
        std::lock_guard lock(*warningMutex);
        firstOccurrence = warnings->insert(message).second;
    }
    if (firstOccurrence) {
        spdlog::warn("{}", message);
    }
}

std::size_t findFlatTomlComment(std::string_view value) {
    char activeQuote = '\0';
    bool escaped = false;
    for (std::size_t i = 0; i < value.size(); ++i) {
        const char current = value[i];
        if (activeQuote == '"' && escaped) {
            escaped = false;
            continue;
        }
        if (activeQuote == '"' && current == '\\') {
            escaped = true;
            continue;
        }
        if (activeQuote != '\0') {
            if (current == activeQuote) {
                activeQuote = '\0';
            }
            continue;
        }
        if (current == '"' || current == '\'') {
            activeQuote = current;
        } else if (current == '#') {
            return i;
        }
    }
    return std::string_view::npos;
}

template <typename T, typename Parser>
ParsedEnvironmentValue<T> readTypedEnvironment(std::string_view key, std::string_view expected,
                                               Parser&& parser) {
    ParsedEnvironmentValue<T> result;
    const auto raw = getenv_optional(key);
    if (!raw || detail::trimView(*raw).empty()) {
        return result;
    }

    result.present = true;
    result.raw = *raw;
    result.value = parser(*raw);
    if (!result.value) {
        warnEnvironmentOnce("Ignoring invalid environment " + std::string(key) + "='" +
                            yams::common::sanitizeForTerminal(*raw) + "'; expected " +
                            std::string(expected));
    }
    return result;
}

} // namespace

std::optional<std::string> getenv_optional(std::string_view key) {
    const std::string name(key);
    std::lock_guard lock(processEnvironmentMutex());
    const char* raw = std::getenv(name.c_str()); // NOLINT(concurrency-mt-unsafe)
    if (raw != nullptr) {
        return std::string(raw);
    }
    return std::nullopt;
}

std::optional<std::string> getenv_nonempty(std::string_view key) {
    const auto value = getenv_optional(key);
    return value && !value->empty() ? value : std::nullopt;
}

bool set_environment(const char* key, const char* value) noexcept {
    if (key == nullptr || *key == '\0') {
        return false;
    }
    try {
        std::lock_guard lock(processEnvironmentMutex());
#ifdef _WIN32
        return _putenv_s(key, value != nullptr ? value : "") == 0;
#else
        if (value != nullptr) {
            // NOLINTNEXTLINE(concurrency-mt-unsafe): serialized by processEnvironmentMutex().
            return ::setenv(key, value, 1) == 0;
        }
        // NOLINTNEXTLINE(concurrency-mt-unsafe): serialized by processEnvironmentMutex().
        return ::unsetenv(key) == 0;
#endif
    } catch (...) {
        return false;
    }
}

std::string getenv_copy(const char* key) {
    const auto value = getenv_nonempty(key);
    return value.value_or(std::string{});
}

ParsedEnvironmentValue<bool> read_env_bool(std::string_view key) {
    return readTypedEnvironment<bool>(
        key, "one of 1/0, true/false, yes/no, on/off",
        [](std::string_view raw) { return detail::parseTomlBool(detail::trimView(raw)); });
}

ParsedEnvironmentValue<std::size_t> read_env_size(std::string_view key) {
    return readTypedEnvironment<std::size_t>(
        key, "a non-negative integer",
        [](std::string_view raw) { return detail::parseUnsignedIntegral<std::size_t>(raw); });
}

ParsedEnvironmentValue<int> read_env_int(std::string_view key) {
    return readTypedEnvironment<int>(key, "an integer", [](std::string_view raw) {
        raw = detail::trimView(raw);
        int value{};
        const char* begin = raw.data();
        const char* end = begin + raw.size();
        const auto [parsedEnd, error] = std::from_chars(begin, end, value);
        return error == std::errc{} && parsedEnd == end ? std::optional<int>{value}
                                                        : std::optional<int>{};
    });
}

ParsedEnvironmentValue<std::uint32_t> read_env_u32(std::string_view key) {
    return readTypedEnvironment<std::uint32_t>(key, "a uint32 integer", [](std::string_view raw) {
        return detail::parseUnsignedIntegral<std::uint32_t>(raw);
    });
}

ParsedEnvironmentValue<float> read_env_float(std::string_view key) {
    return readTypedEnvironment<float>(key, "a finite number", [](std::string_view raw) {
        const auto parsed = detail::parseDouble(raw);
        if (!parsed || *parsed > std::numeric_limits<float>::max() ||
            *parsed < std::numeric_limits<float>::lowest()) {
            return std::optional<float>{};
        }
        return std::optional<float>{static_cast<float>(*parsed)};
    });
}

ParsedEnvironmentValue<double> read_env_double(std::string_view key) {
    return readTypedEnvironment<double>(key, "a finite number", detail::parseDouble);
}

ParsedEnvironmentValue<std::chrono::milliseconds> read_env_milliseconds(std::string_view key) {
    return readTypedEnvironment<std::chrono::milliseconds>(
        key, "a non-negative integer number of milliseconds", [](std::string_view raw) {
            const auto parsed = detail::parseUnsignedIntegral<std::chrono::milliseconds::rep>(raw);
            return parsed ? std::optional{std::chrono::milliseconds{*parsed}}
                          : std::optional<std::chrono::milliseconds>{};
        });
}

VectorEnvironmentPolicy resolve_vector_environment(bool configuredEnabled) {
    constexpr std::array<std::string_view, 3> kDisableAliases{
        "YAMS_DISABLE_VECTORS", "YAMS_DISABLE_VECTOR", "YAMS_DISABLE_VECTOR_DB"};

    VectorEnvironmentPolicy policy;
    policy.enabled = configuredEnabled;
    bool sawFalse = false;
    for (const auto alias : kDisableAliases) {
        const auto parsed = read_env_bool(alias);
        if (parsed.invalid()) {
            policy.diagnostics.push_back("Ignoring invalid vector-disable alias " +
                                         std::string(alias) + "='" +
                                         yams::common::sanitizeForTerminal(parsed.raw) + "'");
            continue;
        }
        if (!parsed.value) {
            continue;
        }
        if (*parsed.value) {
            policy.disableSources.emplace_back(alias);
        } else {
            sawFalse = true;
        }
    }

    if (!policy.disableSources.empty()) {
        policy.enabled = false;
        if (sawFalse) {
            const std::string diagnostic =
                "Conflicting vector-disable aliases; a valid disable=true value wins";
            policy.diagnostics.push_back(diagnostic);
            warnEnvironmentOnce(diagnostic);
        }
    }
    return policy;
}

std::string sanitize_for_terminal(std::string_view in) {
    return yams::common::sanitizeForTerminal(in);
}

std::filesystem::path expand_tilde(const std::string& path) {
    if (!path.empty() && path[0] == '~') {
        if (const auto home = getenv_copy("HOME"); !home.empty()) {
            // path may be "~" (bare home) or "~/..." (relative to home).
            // path.substr(2) would throw std::out_of_range for a bare "~".
            if (path.size() >= 2 && (path[1] == '/' || path[1] == '\\')) {
                return std::filesystem::path(home) / path.substr(2);
            }
            if (path.size() == 1) {
                return std::filesystem::path(home);
            }
            // "~user" (non-home-directory tilde expansion) — not expanded.
        }
    }
    return path;
}

// Platform-specific directory helpers
std::filesystem::path get_config_dir() {
#ifdef _WIN32
    // Windows: Use APPDATA (roaming) for config that should sync
    if (const auto appData = getenv_copy("APPDATA"); !appData.empty()) {
        return std::filesystem::path(appData) / "yams";
    }
    // Fallback to USERPROFILE
    if (const auto userProfile = getenv_copy("USERPROFILE"); !userProfile.empty()) {
        return std::filesystem::path(userProfile) / "AppData" / "Roaming" / "yams";
    }
#else
    // Unix: Follow XDG Base Directory Specification
    if (const auto xdgConfig = getenv_copy("XDG_CONFIG_HOME"); !xdgConfig.empty()) {
        return std::filesystem::path(xdgConfig) / "yams";
    }
    if (const auto home = getenv_copy("HOME"); !home.empty()) {
        return std::filesystem::path(home) / ".config" / "yams";
    }
#endif
    // Last resort: current directory
    return std::filesystem::current_path() / ".yams";
}

static std::filesystem::path get_platform_data_dir() {
#ifdef _WIN32
    if (const auto localAppData = getenv_copy("LOCALAPPDATA"); !localAppData.empty()) {
        return std::filesystem::path(localAppData) / "yams";
    }
    if (const auto userProfile = getenv_copy("USERPROFILE"); !userProfile.empty()) {
        return std::filesystem::path(userProfile) / "AppData" / "Local" / "yams";
    }
#else
    if (const auto xdgData = getenv_copy("XDG_DATA_HOME"); !xdgData.empty()) {
        return std::filesystem::path(xdgData) / "yams";
    }
    if (const auto home = getenv_copy("HOME"); !home.empty()) {
        return std::filesystem::path(home) / ".local" / "share" / "yams";
    }
#endif
    return std::filesystem::current_path() / "yams_data";
}

std::filesystem::path get_data_dir() {
    auto resolved = resolve_runtime_paths();
    if (resolved) {
        return resolved.value().dataDir.value;
    }
    throw std::invalid_argument(resolved.error().message);
}

std::filesystem::path get_cache_dir() {
#ifdef _WIN32
    // Windows: Use LOCALAPPDATA\yams\cache
    if (const auto localAppData = getenv_copy("LOCALAPPDATA"); !localAppData.empty()) {
        return std::filesystem::path(localAppData) / "yams" / "cache";
    }
    // Fallback to USERPROFILE
    if (const auto userProfile = getenv_copy("USERPROFILE"); !userProfile.empty()) {
        return std::filesystem::path(userProfile) / "AppData" / "Local" / "yams" / "cache";
    }
#else
    // Unix: Follow XDG Base Directory Specification
    if (const auto xdgCache = getenv_copy("XDG_CACHE_HOME"); !xdgCache.empty()) {
        return std::filesystem::path(xdgCache) / "yams";
    }
    if (const auto home = getenv_copy("HOME"); !home.empty()) {
        return std::filesystem::path(home) / ".cache" / "yams";
    }
#endif
    // Last resort
    return std::filesystem::temp_directory_path() / "yams-cache";
}

std::filesystem::path get_runtime_dir() {
#ifdef _WIN32
    // Windows: Use LOCALAPPDATA for runtime (sockets, PIDs)
    // Note: Windows doesn't have XDG_RUNTIME_DIR equivalent, so we use LOCALAPPDATA
    if (const auto localAppData = getenv_copy("LOCALAPPDATA"); !localAppData.empty()) {
        return std::filesystem::path(localAppData) / "yams";
    }
    // Fallback to USERPROFILE
    if (const auto userProfile = getenv_copy("USERPROFILE"); !userProfile.empty()) {
        return std::filesystem::path(userProfile) / "AppData" / "Local" / "yams";
    }
#else
    // Unix: Use XDG_RUNTIME_DIR (typically /run/user/$UID)
    if (const auto xdgRuntime = getenv_copy("XDG_RUNTIME_DIR"); !xdgRuntime.empty()) {
        return std::filesystem::path(xdgRuntime) / "yams";
    }
    // Fallback to /tmp with user-specific subdirectory
    std::string tmpDir = "/tmp/yams-" + std::to_string(getuid());
    return std::filesystem::path(tmpDir);
#endif
    // Last resort
    return std::filesystem::temp_directory_path() / "yams-runtime";
}

std::filesystem::path get_state_dir() {
#ifdef _WIN32
    if (const auto localAppData = getenv_copy("LOCALAPPDATA"); !localAppData.empty()) {
        return std::filesystem::path(localAppData) / "yams" / "state";
    }
    if (const auto userProfile = getenv_copy("USERPROFILE"); !userProfile.empty()) {
        return std::filesystem::path(userProfile) / "AppData" / "Local" / "yams" / "state";
    }
#else
    if (const auto xdgState = getenv_copy("XDG_STATE_HOME"); !xdgState.empty()) {
        return std::filesystem::path(xdgState) / "yams";
    }
    if (const auto home = getenv_copy("HOME"); !home.empty()) {
        return std::filesystem::path(home) / ".local" / "state" / "yams";
    }
#endif
    return std::filesystem::temp_directory_path() / "yams-state";
}

namespace {

bool can_write_to_directory(const std::filesystem::path& dir) {
    namespace fs = std::filesystem;
    if (!fs::exists(dir)) {
        return false;
    }
#ifdef _WIN32
    const auto pid = _getpid();
#else
    const auto pid = getpid();
#endif
    const auto testFile = dir / (".yams-test-" + std::to_string(pid));
    std::ofstream test(testFile);
    if (!test.good()) {
        return false;
    }
    test.close();
    std::error_code ec;
    fs::remove(testFile, ec);
    return true;
}

std::filesystem::path
resolve_default_socket_path(const std::optional<std::filesystem::path>& runtimeOverride) {
    namespace fs = std::filesystem;
    if (runtimeOverride && !runtimeOverride->empty()) {
        return *runtimeOverride / "yams-daemon.sock";
    }
#ifdef _WIN32
    if (const auto localAppData = getenv_copy("LOCALAPPDATA"); !localAppData.empty()) {
        const auto runtimeDir = fs::path(localAppData) / "yams";
        std::error_code error;
        yams::common::ensureDirectories(runtimeDir, error);
        if (!error && can_write_to_directory(runtimeDir)) {
            return runtimeDir / "yams-daemon.sock";
        }
    }
    return fs::temp_directory_path() / "yams-daemon.sock";
#else
    if (geteuid() == 0) {
        return fs::path("/var/run/yams-daemon.sock");
    }
    if (const auto xdgRuntime = getenv_copy("XDG_RUNTIME_DIR"); !xdgRuntime.empty()) {
        const fs::path runtimeDir(xdgRuntime);
        if (can_write_to_directory(runtimeDir)) {
            return runtimeDir / "yams-daemon.sock";
        }
    }
    return fs::path("/tmp") /
           ("yams-daemon-" + std::to_string(static_cast<unsigned long long>(getuid())) + ".sock");
#endif
}

std::filesystem::path
resolve_default_pid_path(const std::optional<std::filesystem::path>& runtimeOverride) {
    namespace fs = std::filesystem;
    if (runtimeOverride && !runtimeOverride->empty()) {
        return *runtimeOverride / "yams-daemon.pid";
    }
#ifdef _WIN32
    if (auto runtimeDir = get_daemon_runtime_dir();
        !runtimeDir.empty() && can_write_to_directory(runtimeDir)) {
        return runtimeDir / "yams-daemon.pid";
    }
    return fs::temp_directory_path() / "yams-daemon.pid";
#else
    if (geteuid() == 0) {
        return fs::path("/var/run/yams-daemon.pid");
    }
    if (auto runtimeDir = get_daemon_runtime_dir();
        !runtimeDir.empty() && can_write_to_directory(runtimeDir)) {
        return runtimeDir / "yams-daemon.pid";
    }
    return fs::path("/tmp") /
           ("yams-daemon-" + std::to_string(static_cast<unsigned long long>(getuid())) + ".pid");
#endif
}

std::filesystem::path get_state_home_for_daemon_log() {
    auto stateDir = get_state_dir();
    if (stateDir.empty()) {
        return {};
    }
#ifdef _WIN32
    // get_state_dir() returns %LOCALAPPDATA%\yams\state. Return
    // %LOCALAPPDATA% so callers can append /yams without doubling it.
    return stateDir.parent_path().parent_path();
#else
    return stateDir.parent_path();
#endif
}

} // namespace

std::filesystem::path get_daemon_runtime_dir() {
    auto runtimeDir = get_runtime_dir();
    std::error_code ec;
    yams::common::ensureDirectories(runtimeDir, ec);
    return runtimeDir;
}

std::filesystem::path get_daemon_status_file() {
    return get_daemon_runtime_dir() / "yams-daemon.status.json";
}

std::filesystem::path resolve_daemon_pid_file_path() {
    auto resolved = resolve_runtime_paths();
    if (resolved) {
        return resolved.value().pidFile.value;
    }
    throw std::invalid_argument(resolved.error().message);
}

std::filesystem::path resolve_daemon_log_file_path() {
#ifdef _WIN32
    if (auto stateHome = get_state_home_for_daemon_log(); !stateHome.empty()) {
        auto logDir = stateHome / "yams";
        yams::common::ensureDirectories(logDir);
        if (can_write_to_directory(logDir)) {
            return logDir / "daemon.log";
        }
    }
    return std::filesystem::temp_directory_path() / "yams-daemon.log";
#else
    if (geteuid() == 0 && can_write_to_directory("/var/log")) {
        return std::filesystem::path("/var/log/yams-daemon.log");
    }
    if (auto stateHome = get_state_home_for_daemon_log(); !stateHome.empty()) {
        auto logDir = stateHome / "yams";
        yams::common::ensureDirectories(logDir);
        if (can_write_to_directory(logDir)) {
            return logDir / "daemon.log";
        }
    }
    return std::filesystem::path("/tmp") /
           ("yams-daemon-" + std::to_string(static_cast<unsigned long long>(getuid())) + ".log");
#endif
}

std::filesystem::path get_daemon_plugin_trust_file() {
    if (const auto env = getenv_copy("YAMS_PLUGIN_TRUST_FILE"); !env.empty()) {
        return std::filesystem::path(env);
    }

    auto dataDir = resolve_data_dir_from_config();
    if (dataDir.empty()) {
        dataDir = get_data_dir();
    }
    return dataDir / "plugins.trust";
}

std::filesystem::path get_legacy_plugin_trust_file() {
    return get_config_dir() / "plugins_trust.txt";
}

std::string parse_config_value(const std::filesystem::path& config_path, const std::string& section,
                               const std::string& key) {
    const auto values = parse_simple_toml(config_path);
    const auto qualifiedKey = section.empty() ? key : section + "." + key;
    if (const auto it = values.find(qualifiedKey); it != values.end()) {
        return it->second;
    }
    return {};
}

std::filesystem::path get_config_path(const std::string& override_path) {
    if (!override_path.empty()) {
        return std::filesystem::path(override_path);
    }
    // Compatibility override retained for ConfigResolver callers and older harnesses. A stale
    // compatibility path falls through to the canonical path instead of masking it.
    if (const auto compatibilityEnv = getenv_copy("YAMS_CONFIG_PATH"); !compatibilityEnv.empty()) {
        const auto compatibilityPath = expand_tilde(compatibilityEnv);
        std::error_code error;
        if (std::filesystem::exists(compatibilityPath, error) && !error) {
            return compatibilityPath;
        }
    }
    if (const auto configEnv = getenv_copy("YAMS_CONFIG"); !configEnv.empty()) {
        return expand_tilde(configEnv);
    }
    return get_config_dir() / "config.toml";
}

// Helper to parse path-specific config values with tilde expansion.
static std::filesystem::path parse_config_path_value(const std::filesystem::path& configPath,
                                                     const std::string& targetSection,
                                                     const std::string& targetKey) {
    const auto value = parse_config_value(configPath, targetSection, targetKey);
    return value.empty() ? std::filesystem::path{} : expand_tilde(value);
}

namespace {

bool paths_equal(const std::filesystem::path& lhs, const std::filesystem::path& rhs) {
    return lhs.lexically_normal() == rhs.lexically_normal();
}

ResolvedRuntimePath make_resolved_path(std::filesystem::path value, RuntimePathSource source,
                                       std::string sourceName) {
    return ResolvedRuntimePath{std::move(value), source, std::move(sourceName)};
}

std::string alias_conflict_message(std::string_view firstName,
                                   const std::filesystem::path& firstValue,
                                   std::string_view secondName,
                                   const std::filesystem::path& secondValue) {
    return "Conflicting runtime path aliases " + std::string(firstName) + "='" +
           firstValue.string() + "' and " + std::string(secondName) + "='" + secondValue.string() +
           "'";
}

} // namespace

Result<ResolvedRuntimePaths> resolve_runtime_paths(const RuntimePathOverrides& overrides) {
    ResolvedRuntimePaths resolved;

    if (overrides.configFile && !overrides.configFile->empty()) {
        resolved.configFile = make_resolved_path(expand_tilde(overrides.configFile->string()),
                                                 RuntimePathSource::Explicit, "explicit config");
    } else {
        const auto compatibilityConfig = getenv_copy("YAMS_CONFIG_PATH");
        const auto canonicalConfig = getenv_copy("YAMS_CONFIG");
        std::error_code compatibilityError;
        const bool compatibilityActive =
            !compatibilityConfig.empty() &&
            std::filesystem::exists(expand_tilde(compatibilityConfig), compatibilityError) &&
            !compatibilityError;
        const auto configSource = compatibilityActive || !canonicalConfig.empty()
                                      ? RuntimePathSource::Environment
                                      : RuntimePathSource::PlatformDefault;
        const auto configSourceName =
            compatibilityActive
                ? "YAMS_CONFIG_PATH"
                : (!canonicalConfig.empty() ? "YAMS_CONFIG" : "platform config default");
        const auto configPath = compatibilityActive
                                    ? expand_tilde(compatibilityConfig)
                                    : (!canonicalConfig.empty() ? expand_tilde(canonicalConfig)
                                                                : get_config_dir() / "config.toml");
        resolved.configFile = make_resolved_path(configPath, configSource, configSourceName);
    }

    std::vector<std::pair<std::string, std::filesystem::path>> configuredDataPaths;
    const auto addConfiguredDataPath = [&](std::string section, std::string key) {
        if (auto value = parse_config_path_value(resolved.configFile.value, section, key);
            !value.empty()) {
            configuredDataPaths.emplace_back(section + "." + key, std::move(value));
        }
    };
    addConfiguredDataPath("core", "data_dir");
    addConfiguredDataPath("daemon", "data_dir");
    addConfiguredDataPath("daemon", "storage");
    addConfiguredDataPath("daemon", "storage_path");

    const auto explicitData = overrides.dataDir && !overrides.dataDir->empty();
    if (configuredDataPaths.size() > 1) {
        const auto& [firstName, firstValue] = configuredDataPaths.front();
        for (auto it = std::next(configuredDataPaths.begin()); it != configuredDataPaths.end();
             ++it) {
            if (!paths_equal(firstValue, it->second)) {
                const auto message =
                    alias_conflict_message(firstName, firstValue, it->first, it->second);
                if (!explicitData) {
                    return Error{ErrorCode::InvalidData, message};
                }
                resolved.diagnostics.push_back(message + " (ignored by explicit data path)");
            }
        }
    }

    const auto legacyStorage = getenv_copy("YAMS_STORAGE");
    const auto canonicalData = getenv_copy("YAMS_DATA_DIR");
    const bool dataEnvConflict = !legacyStorage.empty() && !canonicalData.empty() &&
                                 !paths_equal(legacyStorage, canonicalData);
    const bool configuredData = !configuredDataPaths.empty();
    if (dataEnvConflict) {
        const auto message =
            alias_conflict_message("YAMS_STORAGE", legacyStorage, "YAMS_DATA_DIR", canonicalData);
        if (!explicitData && !configuredData) {
            return Error{ErrorCode::InvalidData, message};
        }
        resolved.diagnostics.push_back(message + " (ignored by higher-precedence data path)");
    }

    if (explicitData) {
        resolved.dataDir = make_resolved_path(expand_tilde(overrides.dataDir->string()),
                                              RuntimePathSource::Explicit, "explicit data path");
    } else if (configuredData) {
        resolved.dataDir =
            make_resolved_path(configuredDataPaths.front().second, RuntimePathSource::ConfigFile,
                               configuredDataPaths.front().first);
    } else if (!canonicalData.empty()) {
        resolved.dataDir = make_resolved_path(expand_tilde(canonicalData),
                                              RuntimePathSource::Environment, "YAMS_DATA_DIR");
    } else if (!legacyStorage.empty()) {
        resolved.dataDir = make_resolved_path(expand_tilde(legacyStorage),
                                              RuntimePathSource::Environment, "YAMS_STORAGE");
    } else {
        resolved.dataDir = make_resolved_path(
            get_platform_data_dir(), RuntimePathSource::PlatformDefault, "platform data default");
    }

    const auto explicitRuntime = overrides.runtimeDir && !overrides.runtimeDir->empty();
    if (explicitRuntime) {
        resolved.runtimeDir =
            make_resolved_path(expand_tilde(overrides.runtimeDir->string()),
                               RuntimePathSource::Explicit, "explicit runtime path");
    } else {
        resolved.runtimeDir = make_resolved_path(
            get_runtime_dir(), RuntimePathSource::PlatformDefault, "platform runtime default");
    }

    const auto canonicalSocket = getenv_copy("YAMS_DAEMON_SOCKET");
    const auto compatibilitySocket = getenv_copy("YAMS_DAEMON_SOCKET_PATH");
    const bool explicitSocket = overrides.socketPath && !overrides.socketPath->empty();
    if (!canonicalSocket.empty() && !compatibilitySocket.empty() &&
        !paths_equal(canonicalSocket, compatibilitySocket)) {
        const auto message = alias_conflict_message("YAMS_DAEMON_SOCKET", canonicalSocket,
                                                    "YAMS_DAEMON_SOCKET_PATH", compatibilitySocket);
        if (!explicitSocket) {
            return Error{ErrorCode::InvalidData, message};
        }
        resolved.diagnostics.push_back(message + " (ignored by explicit socket path)");
    }

    if (explicitSocket) {
        resolved.socketPath =
            make_resolved_path(expand_tilde(overrides.socketPath->string()),
                               RuntimePathSource::Explicit, "explicit socket path");
    } else if (!canonicalSocket.empty()) {
        resolved.socketPath = make_resolved_path(
            expand_tilde(canonicalSocket), RuntimePathSource::Environment, "YAMS_DAEMON_SOCKET");
    } else if (!compatibilitySocket.empty()) {
        resolved.socketPath =
            make_resolved_path(expand_tilde(compatibilitySocket), RuntimePathSource::Environment,
                               "YAMS_DAEMON_SOCKET_PATH");
    } else if (auto configured =
                   parse_config_path_value(resolved.configFile.value, "daemon", "socket_path");
               !configured.empty()) {
        resolved.socketPath = make_resolved_path(
            std::move(configured), RuntimePathSource::ConfigFile, "daemon.socket_path");
    } else {
        resolved.socketPath = make_resolved_path(
            resolve_default_socket_path(explicitRuntime ? std::optional{resolved.runtimeDir.value}
                                                        : std::nullopt),
            RuntimePathSource::PlatformDefault, "platform socket default");
    }

    const bool explicitPid = overrides.pidFile && !overrides.pidFile->empty();
    if (explicitPid) {
        resolved.pidFile = make_resolved_path(expand_tilde(overrides.pidFile->string()),
                                              RuntimePathSource::Explicit, "explicit PID path");
    } else if (auto configured =
                   parse_config_path_value(resolved.configFile.value, "daemon", "pid_file");
               !configured.empty()) {
        resolved.pidFile = make_resolved_path(std::move(configured), RuntimePathSource::ConfigFile,
                                              "daemon.pid_file");
    } else {
        resolved.pidFile = make_resolved_path(
            resolve_default_pid_path(explicitRuntime ? std::optional{resolved.runtimeDir.value}
                                                     : std::nullopt),
            RuntimePathSource::PlatformDefault, "platform PID default");
    }

    return resolved;
}

std::filesystem::path resolve_socket_path_from_config() {
    auto resolved = resolve_runtime_paths();
    if (resolved) {
        return resolved.value().socketPath.value;
    }
    throw std::invalid_argument(resolved.error().message);
}

std::filesystem::path resolve_pid_file_from_config() {
    auto resolved = resolve_runtime_paths();
    if (resolved) {
        return resolved.value().pidFile.value;
    }
    throw std::invalid_argument(resolved.error().message);
}

std::filesystem::path resolve_data_dir_from_config() {
    auto resolved = resolve_runtime_paths();
    if (resolved) {
        return resolved.value().dataDir.value;
    }
    throw std::invalid_argument(resolved.error().message);
}

std::string resolve_daemon_mode_from_config() {
    if (const auto env = getenv_copy("YAMS_EMBEDDED"); !env.empty()) {
        std::string value = env;
        trim(value);
        if (!value.empty()) {
            return value;
        }
    }

    const std::filesystem::path config_path = get_config_path();

    if (!config_path.empty()) {
        auto value = parse_config_value(config_path, "daemon", "mode");
        trim(value);
        return value;
    }

    return {};
}

std::vector<std::filesystem::path> parse_path_list(const std::string& raw) {
    std::vector<std::filesystem::path> out;
    if (raw.empty())
        return out;

    std::string s = raw;
    trim(s);
    if (s.empty())
        return out;

    // Strip surrounding brackets for TOML arrays
    if (s.front() == '[' && s.back() == ']') {
        s = s.substr(1, s.size() - 2);
    }

    char activeQuote = '\0';
    bool escaped = false;
    std::string item;
    const auto appendItem = [&] {
        trim(item);
        item = unquote(item);
        if (!item.empty()) {
            out.emplace_back(expand_tilde(item));
        }
        item.clear();
    };
    for (const char current : s) {
        if (activeQuote == '"' && escaped) {
            item.push_back(current);
            escaped = false;
            continue;
        }
        if (activeQuote == '"' && current == '\\') {
            item.push_back(current);
            escaped = true;
            continue;
        }
        if (activeQuote != '\0') {
            item.push_back(current);
            if (current == activeQuote) {
                activeQuote = '\0';
            }
            continue;
        }
        if (current == '"' || current == '\'') {
            activeQuote = current;
            item.push_back(current);
        } else if (current == ',') {
            appendItem();
        } else {
            item.push_back(current);
        }
    }
    appendItem();

    return out;
}

// ============================================================================
// Full TOML config parsing and writing utilities
// ============================================================================

std::map<std::string, std::string> parse_simple_toml(const std::filesystem::path& path) {
    std::map<std::string, std::string> config;
    std::ifstream file(path);
    if (!file) {
        return config;
    }

    std::string line;
    std::string currentSection;

    while (std::getline(file, line)) {
        trim(line);

        // Skip comments and empty lines
        if (line.empty() || line[0] == '#') {
            continue;
        }

        // Check for section headers [section]
        if (line[0] == '[') {
            size_t end = line.find(']');
            if (end != std::string::npos) {
                currentSection = line.substr(1, end - 1);
                trim(currentSection);
                if (!currentSection.empty()) {
                    currentSection += ".";
                }
            }
            continue;
        }

        // Parse key-value pairs
        size_t eq = line.find('=');
        if (eq != std::string::npos) {
            std::string key = line.substr(0, eq);
            std::string value = line.substr(eq + 1);

            trim(key);
            trim(value);

            // Remove inline comments while respecting the active quote type and escaped double
            // quotes. The flat reader deliberately preserves escape sequences in returned values.
            if (const auto comment = findFlatTomlComment(value);
                comment != std::string_view::npos) {
                value.resize(comment);
                trim(value);
            }

            value = unquote(value);
            config[currentSection + key] = value;
        }
    }

    return config;
}

DimensionConfig read_dimension_config(const std::filesystem::path& config_path) {
    DimensionConfig result{};

    if (!std::filesystem::exists(config_path)) {
        return result;
    }

    auto config = parse_simple_toml(config_path);

    auto parseSize = [](const std::string& str) -> std::optional<size_t> {
        if (str.empty()) {
            return std::nullopt;
        }
        try {
            return static_cast<size_t>(std::stoul(str));
        } catch (...) {
            return std::nullopt;
        }
    };

    // Check various keys for embedding dimension
    if (auto it = config.find("embeddings.embedding_dim"); it != config.end()) {
        result.embeddings = parseSize(it->second);
    }
    if (auto it = config.find("vector_database.embedding_dim"); it != config.end()) {
        result.vectorDb = parseSize(it->second);
    }
    if (auto it = config.find("vector_index.dimension"); it != config.end()) {
        result.index = parseSize(it->second);
    }
    // Also check for typo variant
    if (!result.index) {
        if (auto it = config.find("vector_index.dimenions"); it != config.end()) {
            result.index = parseSize(it->second);
        }
    }

    return result;
}

bool write_dimension_config(const std::filesystem::path& config_path, size_t dim) {
    std::map<std::string, std::string> values;
    values["embeddings.embedding_dim"] = std::to_string(dim);
    values["vector_database.embedding_dim"] = std::to_string(dim);
    values["vector_index.dimension"] = std::to_string(dim);
    return write_config_values(config_path, values);
}

bool write_config_value(const std::filesystem::path& config_path, const std::string& key,
                        const std::string& value) {
    std::map<std::string, std::string> values;
    values[key] = value;
    return write_config_values(config_path, values);
}

bool write_config_values(const std::filesystem::path& config_path,
                         const std::map<std::string, std::string>& values) {
    try {
        // Ensure parent directories exist
        if (!yams::common::ensureDirectories(config_path.parent_path())) {
            return false;
        }

        // Read existing config
        auto config = parse_simple_toml(config_path);

        // Merge new values
        for (const auto& [key, value] : values) {
            config[key] = value;
        }

        // Group by section for writing
        std::map<std::string, std::map<std::string, std::string>> sections;
        for (const auto& [fullKey, val] : config) {
            size_t dot = fullKey.find('.');
            if (dot != std::string::npos) {
                std::string section = fullKey.substr(0, dot);
                std::string subkey = fullKey.substr(dot + 1);
                sections[section][subkey] = val;
            } else {
                // Top-level key (no section)
                sections[""][fullKey] = val;
            }
        }

        // Write back to file
        std::ofstream file(config_path, std::ios::trunc);
        if (!file) {
            return false;
        }

        // Write top-level keys first (if any)
        if (auto it = sections.find(""); it != sections.end()) {
            for (const auto& [key, val] : it->second) {
                file << key << " = \"" << val << "\"\n";
            }
            sections.erase(it);
        }

        // Write each section
        for (const auto& [section, kvs] : sections) {
            file << "\n[" << section << "]\n";
            for (const auto& [key, val] : kvs) {
                file << key << " = \"" << val << "\"\n";
            }
        }

        return true;
    } catch (...) {
        return false;
    }
}

} // namespace yams::config
