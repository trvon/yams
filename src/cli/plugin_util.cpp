#include <yams/cli/plugin_util.h>
#include <yams/config/config_helpers.h>
#include <yams/daemon/resource/plugin_trust.h>
#include <yams/plugins/model_provider_v1.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <set>

#ifdef _WIN32
#include <windows.h>
#define RTLD_LAZY 0
#define RTLD_LOCAL 0

static void* dlopen_impl(const char* filename, int /*flags*/) {
    return LoadLibraryA(filename);
}

static void* dlsym_impl(void* handle, const char* symbol) {
    return reinterpret_cast<void*>(GetProcAddress(static_cast<HMODULE>(handle), symbol));
}

static int dlclose_impl(void* handle) {
    return FreeLibrary(static_cast<HMODULE>(handle)) ? 0 : -1;
}

static const char* dlerror_impl() {
    static char buf[256];
    FormatMessageA(FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS, nullptr,
                   GetLastError(), MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), buf, sizeof(buf),
                   nullptr);
    return buf;
}

#define dlopen dlopen_impl
#define dlsym dlsym_impl
#define dlclose dlclose_impl
#define dlerror dlerror_impl

#else
#include <dlfcn.h>
#endif

namespace yams::cli::plugin {

namespace fs = std::filesystem;

// ============================================================================
// Plugin Trust Management
// ============================================================================

std::set<fs::path> readTrustedRoots() {
    const fs::path canonicalTrust = yams::config::get_daemon_plugin_trust_file();
    return yams::daemon::plugin_trust::loadTrustStore(canonicalTrust, canonicalTrust,
                                                      yams::config::get_legacy_plugin_trust_file())
        .entries;
}

bool isPathTrusted(const fs::path& pluginPath, const std::set<fs::path>& trustedRoots) {
    if (trustedRoots.empty()) {
        return false;
    }

    std::error_code ec;
    auto candidate = fs::weakly_canonical(pluginPath, ec);
    if (ec) {
        candidate = pluginPath.lexically_normal();
    }

    for (const auto& root : trustedRoots) {
        std::error_code rec;
        auto canonRoot = fs::weakly_canonical(root, rec);
        if (rec) {
            canonRoot = root.lexically_normal();
        }

        if (yams::daemon::plugin_trust::isPathWithin(canonRoot, candidate)) {
            return true;
        }
    }

    return false;
}

// ============================================================================
// Plugin Resolution
// ============================================================================

std::vector<fs::path> dedupePluginRoots(std::vector<fs::path> roots) {
    std::set<std::string> seen;
    std::vector<fs::path> unique;
    unique.reserve(roots.size());
    for (const auto& root : roots) {
        const auto key = root.lexically_normal().string();
        if (seen.insert(key).second) {
            unique.push_back(root);
        }
    }
    return unique;
}

std::vector<fs::path> getPluginSearchDirs() {
    std::vector<fs::path> dirs;

    // User-local plugins
    if (const char* home = std::getenv("HOME")) {
        dirs.emplace_back(fs::path(home) / ".local" / "lib" / "yams" / "plugins");
    }

#ifdef __APPLE__
    // Homebrew on macOS
    dirs.emplace_back("/opt/homebrew/lib/yams/plugins");
#endif

    // System paths
    dirs.emplace_back("/usr/local/lib/yams/plugins");
    dirs.emplace_back("/usr/lib/yams/plugins");

#ifdef YAMS_INSTALL_PREFIX
    dirs.emplace_back(fs::path(YAMS_INSTALL_PREFIX) / "lib" / "yams" / "plugins");
#endif

    return dirs;
}

std::string getPluginExtension() {
#ifdef _WIN32
    return ".dll";
#elif __APPLE__
    return ".dylib";
#else
    return ".so";
#endif
}

bool hasPluginExtension(const fs::path& path) {
    std::string ext = path.extension().string();
    return ext == ".so" || ext == ".dylib" || ext == ".dll" || ext == ".wasm";
}

std::optional<fs::path> resolvePlugin(const std::string& nameOrPath) {
    // If it's a path that exists, return it directly
    fs::path target(nameOrPath);
    if (fs::exists(target)) {
        return target;
    }

    auto searchDirs = getPluginSearchDirs();
    std::error_code ec;

    // Strategy 1: Exact filename/stem match
    for (const auto& dir : searchDirs) {
        if (!fs::exists(dir, ec) || !fs::is_directory(dir, ec)) {
            continue;
        }

        for (const auto& entry : fs::directory_iterator(dir, ec)) {
            if (!entry.is_regular_file(ec)) {
                continue;
            }

            const auto& path = entry.path();
            if (!hasPluginExtension(path)) {
                continue;
            }

            if (path.stem().string() == nameOrPath || path.filename().string() == nameOrPath) {
                return path;
            }
        }
    }

    // Strategy 2: Heuristic - filename contains the token
    // Keep this cheap pass ahead of ABI probing so obviously-missing names do not
    // require dlopen() across every installed plugin.
    for (const auto& dir : searchDirs) {
        if (!fs::exists(dir, ec) || !fs::is_directory(dir, ec)) {
            continue;
        }

        for (const auto& entry : fs::directory_iterator(dir, ec)) {
            if (!entry.is_regular_file(ec)) {
                continue;
            }

            const auto& path = entry.path();
            if (!hasPluginExtension(path)) {
                continue;
            }

            std::string filename = path.filename().string();
            if (filename.find(nameOrPath) != std::string::npos) {
                return path;
            }
        }
    }

    // Strategy 3: ABI name match via dlopen + yams_plugin_get_name
    for (const auto& dir : searchDirs) {
        if (!fs::exists(dir, ec) || !fs::is_directory(dir, ec)) {
            continue;
        }

        for (const auto& entry : fs::directory_iterator(dir, ec)) {
            if (!entry.is_regular_file(ec)) {
                continue;
            }

            const auto& path = entry.path();
            if (!hasPluginExtension(path)) {
                continue;
            }

            std::string ext = path.extension().string();
            if (ext != ".so" && ext != ".dylib" && ext != ".dll") {
                continue; // Skip WASM for ABI probing
            }

            void* handle = dlopen(path.string().c_str(), RTLD_LAZY | RTLD_LOCAL);
            if (!handle) {
                continue;
            }

            auto getName =
                reinterpret_cast<const char* (*)()>(dlsym(handle, "yams_plugin_get_name"));
            const char* pluginName = getName ? getName() : nullptr;
            bool match = (pluginName && std::string(pluginName) == nameOrPath);

            dlclose(handle);

            if (match) {
                return path;
            }
        }
    }

    return std::nullopt;
}

// ============================================================================
// Plugin ABI Probing
// ============================================================================

} // namespace yams::cli::plugin
