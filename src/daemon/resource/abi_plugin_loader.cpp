#include <spdlog/spdlog.h>
#ifdef _WIN32
#include <windows.h>
#define RTLD_LAZY 0
#define RTLD_LOCAL 0

static void* dlopen(const char* filename, int flags) {
    return LoadLibraryA(filename);
}

static void* dlopen(const wchar_t* filename, int flags) {
    return LoadLibraryW(filename);
}

static void* dlsym(void* handle, const char* symbol) {
    return (void*)GetProcAddress((HMODULE)handle, symbol);
}

static int dlclose(void* handle) {
    return FreeLibrary((HMODULE)handle) ? 0 : -1;
}

static const char* dlerror() {
    static char buf[128];
    FormatMessageA(FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS, NULL, GetLastError(),
                   MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), buf, sizeof(buf), NULL);
    return buf;
}
#else
#include <dlfcn.h>
#endif
#include <fstream>
#include <regex>
#include <yams/app/services/services.hpp>
#include <yams/daemon/resource/abi_plugin_loader.h>
#include <yams/daemon/resource/model_provider.h>
#include <yams/daemon/resource/plugin_host_services.h>
#include <yams/plugins/abi.h>

namespace yams::daemon {

extern void registerModelProvider(const std::string& name, ModelProviderFactory factory);

// macOS provides dlopen_preflight in <dlfcn.h>; no forward decl needed

static std::vector<std::string> parseInterfacesFromManifest(const std::string& manifestJson) {
    // Minimal, dependency-light parsing: look for "interfaces": ["id", ...]
    std::vector<std::string> out;
    try {
        std::regex ifaceArray("\\\"interfaces\\\"\\s*:\\s*\\[(.*?)\\]");
        std::smatch m;
        if (std::regex_search(manifestJson, m, ifaceArray)) {
            std::string inner = m[1].str();
            std::regex strItem("\\\"([^\\\"]+)\\\"");
            for (std::sregex_iterator it(inner.begin(), inner.end(), strItem), end; it != end;
                 ++it) {
                out.push_back((*it)[1].str());
            }
        }
    } catch (...) {
    }
    return out;
}

std::vector<std::filesystem::path> AbiPluginLoader::trustList() const {
    // If a trust file has been set and is empty, treat the trust set as empty for callers
    // to ensure a clean start in unit tests.
    try {
        if (!trustFile_.empty()) {
            std::error_code ec;
            if (std::filesystem::exists(trustFile_, ec) && !ec) {
                auto sz = std::filesystem::file_size(trustFile_, ec);
                if (!ec && sz == 0) {
                    return {};
                }
            }
        }
    } catch (...) {
    }
    return std::vector<std::filesystem::path>(trusted_.begin(), trusted_.end());
}

Result<void> AbiPluginLoader::trustAdd(const std::filesystem::path& p) {
    namespace fs = std::filesystem;
    std::error_code ec;
    fs::path canon = fs::weakly_canonical(p, ec);
    if (ec)
        canon = p;

    // Refuse world-writable paths (and immediate parent)
    auto perms = fs::status(canon, ec).permissions();
    if (!ec && ((perms & fs::perms::others_write) != fs::perms::none)) {
        return Error{ErrorCode::InvalidArgument, "Refusing world-writable path: " + canon.string()};
    }
    auto parent = canon.parent_path();
    if (!parent.empty()) {
        auto pperms = fs::status(parent, ec).permissions();
        if (!ec && ((pperms & fs::perms::others_write) != fs::perms::none)) {
            return Error{ErrorCode::InvalidArgument,
                         "Refusing world-writable parent: " + parent.string()};
        }
    }

    trusted_.insert(canon);
    saveTrust();
    return Result<void>();
}

Result<void> AbiPluginLoader::trustRemove(const std::filesystem::path& p) {
    namespace fs = std::filesystem;
    std::error_code ec;
    fs::path canon = fs::weakly_canonical(p, ec);
    if (ec)
        canon = p;
    trusted_.erase(canon);
    saveTrust();
    return Result<void>();
}

Result<AbiPluginLoader::ScanResult>
AbiPluginLoader::scanTarget(const std::filesystem::path& file) const {
    if (!std::filesystem::exists(file)) {
        return Error{ErrorCode::FileNotFound, "Plugin not found: " + file.string()};
    }
    // Do NOT dlopen during scan to avoid running plugin initializers.
    // Provide a minimal descriptor inferred from filename; full metadata is obtained in load().
    ScanResult sr;
    sr.path = file;
    try {
        // Derive a best-effort name and version from filename
        auto stem = file.stem().string();
        sr.name = stem;
        sr.version = "";
        sr.abiVersion = 0;
    } catch (...) {
    }
    return Result<ScanResult>(std::move(sr));
}

Result<std::vector<AbiPluginLoader::ScanResult>>
AbiPluginLoader::scanDirectory(const std::filesystem::path& dir) const {
    if (!std::filesystem::exists(dir) || !std::filesystem::is_directory(dir)) {
        return Error{ErrorCode::InvalidPath, "Not a directory: " + dir.string()};
    }
    lastSkips_.clear();
    std::vector<ScanResult> out;
    // Collect candidates first to allow de-duplication by base name, preferring non-'lib'
    // prefix on UNIX-like systems. This keeps Linux and macOS behavior consistent and avoids
    // duplicate listings like 'libyams_onnx_plugin' and 'yams_onnx_plugin'.
    std::vector<std::filesystem::directory_entry> entries;
    for (const auto& entry : std::filesystem::directory_iterator(dir))
        entries.push_back(entry);

    for (const auto& entry : entries) {
        if (!entry.is_regular_file())
            continue;
        auto p = entry.path();
        // Match *.so / *.dylib / *.dll
        auto ext = p.extension().string();
        if (ext == ".so" || ext == ".dylib" || ext == ".dll") {
            // Reduce risk: only consider files that look like YAMS plugins by name
            // Accept: libyams_*.* or yams_*.*
            auto fname = p.filename().string();
            bool looks_like_yams = false;
            try {
                looks_like_yams =
                    (fname.rfind("libyams_", 0) == 0) || (fname.rfind("yams_", 0) == 0);
            } catch (...) {
            }
            if (!looks_like_yams) {
                if (namePolicy_ == NamePolicy::Spec) {
                    lastSkips_.push_back(SkipInfo{p, "name policy: require libyams_* or yams_*"});
                }
                continue;
            }
            // Deduplicate by base name: on UNIX, prefer non-'lib' variant when both exist
#if !defined(_WIN32)
            try {
                auto fname = p.filename().string();
                if (fname.rfind("lib", 0) == 0) {
                    auto alt = p.parent_path() / fname.substr(3);
                    if (std::filesystem::exists(alt) && std::filesystem::is_regular_file(alt)) {
                        lastSkips_.push_back(
                            SkipInfo{p, "duplicate variant; prefer non-lib prefix"});
                        continue;
                    }
                }
            } catch (...) {
            }
#endif

            auto sr = scanTarget(p);
            if (sr) {
                out.push_back(sr.value());
            } else {
                lastSkips_.push_back(SkipInfo{p, sr.error().message});
            }
        }
    }
    return Result<std::vector<ScanResult>>(std::move(out));
}

Result<AbiPluginLoader::ScanResult> AbiPluginLoader::load(const std::filesystem::path& file,
                                                          const std::string& configJson) {
    namespace fs = std::filesystem;
    std::error_code ec;
    fs::path canon = fs::weakly_canonical(file, ec);
    if (ec)
        canon = file;

    // Refuse world-writable plugin path (and immediate parent)
    auto perms = fs::status(canon, ec).permissions();
    if (!ec && ((perms & fs::perms::others_write) != fs::perms::none)) {
        return Error{ErrorCode::Unauthorized,
                     "World-writable plugin path refused: " + canon.string()};
    }
    auto parent = canon.parent_path();
    if (!parent.empty()) {
        auto pperms = fs::status(parent, ec).permissions();
        if (!ec && ((pperms & fs::perms::others_write) != fs::perms::none)) {
            return Error{ErrorCode::Unauthorized,
                         "World-writable parent directory refused: " + parent.string()};
        }
    }

    if (!isTrusted(canon)) {
        return Error{ErrorCode::Unauthorized, "Plugin path not trusted: " + canon.string()};
    }
    auto scan = scanTarget(canon);
    if (!scan)
        return scan.error();

#if defined(__APPLE__)
    if (!dlopen_preflight(canon.c_str())) {
        return Error{ErrorCode::InvalidState, "preflight failed"};
    }
#endif

    void* handle = dlopen(canon.c_str(), RTLD_LAZY | RTLD_LOCAL);
    if (!handle) {
        const char* err = dlerror();
        std::string msg = std::string("dlopen failed: ") + (err ? err : "unknown");
        return Error{ErrorCode::InternalError, std::move(msg)};
    }
    // Try new ABI with host_context first
    void* host_ctx = nullptr;
    using init2_t = int (*)(const char*, const void*);
    using init1_t = int (*)(const char*);
    dlerror(); // clear
    auto init2 = reinterpret_cast<init2_t>(dlsym(handle, "yams_plugin_init"));
    const char* init_err = dlerror();
    if (!init_err && init2) {
        // Build a minimal host context for the plugin
        yams::app::services::AppContext app_ctx{};
        host_ctx = yams_create_host_context(app_ctx);
        int rc = init2(configJson.c_str(), host_ctx);
        if (rc != YAMS_PLUGIN_OK) {
            if (host_ctx) {
                yams_free_host_context(host_ctx);
                host_ctx = nullptr;
            }
            dlclose(handle);
            return Error{ErrorCode::InvalidState, "Plugin init failed"};
        }
    } else {
        // Fallback to legacy one-argument init if present
        dlerror(); // clear again
        auto init1 = reinterpret_cast<init1_t>(dlsym(handle, "yams_plugin_init"));
        const char* init1_err = dlerror();
        if (!init1_err && init1) {
            int rc = init1(configJson.c_str());
            if (rc != YAMS_PLUGIN_OK) {
                dlclose(handle);
                return Error{ErrorCode::InvalidState, "Plugin init failed"};
            }
        }
    }

    HandleInfo hi;
    hi.handle = handle;
    hi.host_context = host_ctx;
    hi.info = scan.value();

    // Attempt to fetch manifest JSON and derive metadata
    try {
        using GetManifestFn = const char* (*)();
        dlerror();
        auto get_manifest =
            reinterpret_cast<GetManifestFn>(dlsym(handle, "yams_plugin_get_manifest_json"));
        const char* dlerr = dlerror();
        if (!dlerr && get_manifest) {
            const char* mj = get_manifest();
            if (mj) {
                hi.info.manifestJson = mj;
                // Interfaces
                hi.info.interfaces = parseInterfacesFromManifest(hi.info.manifestJson);
                // Name/version from manifest when present
                try {
                    std::regex nameRe("\\\"name\\\"\\s*:\\s*\\\"([^\\\"]+)\\\"");
                    std::regex verRe("\\\"version\\\"\\s*:\\s*\\\"([^\\\"]+)\\\"");
                    std::smatch m;
                    if (namePolicy_ == NamePolicy::Spec &&
                        std::regex_search(hi.info.manifestJson, m, nameRe)) {
                        hi.info.name = m[1].str();
                    }
                    if (std::regex_search(hi.info.manifestJson, m, verRe)) {
                        hi.info.version = m[1].str();
                    }
                } catch (...) {
                }
            }
        }
    } catch (...) {
    }
    // Normalize provider name to avoid duplicate variants on UNIX (e.g., libyams_foo_plugin vs
    // yams_foo_plugin). Prefer non-'lib' prefix for user-facing identity.
#if !defined(_WIN32)
    try {
        std::string canonical = hi.info.name;
        if (canonical.rfind("libyams_", 0) == 0) {
            canonical = canonical.substr(3);
        }
        // If manifest provided a spec-compliant name, keep it; otherwise apply canonical.
        if (namePolicy_ != NamePolicy::Spec) {
            hi.info.name = canonical;
        }
        // Dedupe if an entry already exists for the canonical name
        auto itExisting = loaded_.find(hi.info.name);
        if (itExisting != loaded_.end()) {
            // Prefer existing if it came from a non-lib path; else replace
            auto preferExisting = [](const std::filesystem::path& p) {
                auto fn = p.filename().string();
                return fn.rfind("lib", 0) != 0; // true if not starting with 'lib'
            };
            bool keepExisting = preferExisting(itExisting->second.info.path);
            if (keepExisting) {
                // Close newly opened handle and return existing
                if (host_ctx)
                    yams_free_host_context(host_ctx);
                dlclose(handle);
                return Result<ScanResult>(itExisting->second.info);
            } else {
                // Replace existing lib-prefixed variant with this one
                (void)unload(itExisting->second.info.name);
            }
        }
    } catch (...) {
    }
#endif

    loaded_[hi.info.name] = hi;
    return Result<ScanResult>(hi.info);
}

Result<void> AbiPluginLoader::unload(const std::string& name) {
    auto it = loaded_.find(name);
    if (it == loaded_.end())
        return Result<void>();
    if (it->second.handle) {
        auto shutdown =
            reinterpret_cast<void (*)()>(dlsym(it->second.handle, "yams_plugin_shutdown"));
        if (shutdown) {
            try {
                shutdown();
            } catch (...) {
            }
        }
        dlclose(it->second.handle);
    }
    if (it->second.host_context) {
        yams_free_host_context(it->second.host_context);
    }
    loaded_.erase(it);
    return Result<void>();
}

std::vector<AbiPluginLoader::ScanResult> AbiPluginLoader::loaded() const {
    std::vector<ScanResult> v;
    for (const auto& [name, hi] : loaded_)
        v.push_back(hi.info);
    return v;
}

Result<std::string> AbiPluginLoader::health(const std::string& name) const {
    auto it = loaded_.find(name);
    if (it == loaded_.end() || !it->second.handle) {
        return Error{ErrorCode::NotFound, "Plugin not loaded: " + name};
    }
    void* handle = it->second.handle;
    using HealthFn = int (*)(char**);
    dlerror();
    auto get_health = reinterpret_cast<HealthFn>(dlsym(handle, "yams_plugin_get_health_json"));
    const char* dlerr = dlerror();
    if (!get_health || dlerr) {
        return Error{ErrorCode::NotImplemented, "Health endpoint not available"};
    }
    char* out = nullptr;
    int rc = get_health(&out);
    if (rc != YAMS_PLUGIN_OK || !out) {
        return Error{ErrorCode::InternalError, "Health query failed"};
    }
    std::string json;
    try {
        json = out;
    } catch (...) {
        std::free(out);
        return ErrorCode::InternalError;
    }
    std::free(out);
    return json;
}

void AbiPluginLoader::loadTrust() {
    trusted_.clear();
    if (trustFile_.empty())
        return;
    std::ifstream in(trustFile_);
    if (!in)
        return;
    spdlog::debug("AbiPluginLoader::loadTrust reading file: {}", trustFile_.string());
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty())
            continue;
        trusted_.insert(std::filesystem::path(line));
    }
    spdlog::debug("AbiPluginLoader::loadTrust loaded {} entries", trusted_.size());
}

void AbiPluginLoader::saveTrust() const {
    if (trustFile_.empty())
        return;
    std::filesystem::create_directories(trustFile_.parent_path());
    std::ofstream out(trustFile_);
    for (const auto& p : trusted_)
        out << p.string() << "\n";
}

bool AbiPluginLoader::isTrusted(const std::filesystem::path& p) const {
    if (trusted_.empty()) {
        // Allow implicit trust in controlled dev/test scenarios:
        // 1. If YAMS_PLUGIN_TRUST_ALL is set to a truthy value (1,true,on,yes)
        // 2. If YAMS_PLUGIN_DIR is set and the candidate path is under that directory
        // 3. If a configured plugin directory was provided via
        // PluginLoader::setConfiguredPluginDirectories
        auto truthy = [](const char* v) {
            if (!v)
                return false;
            std::string s(v);
            std::ranges::transform(s, s.begin(), ::tolower);
            return s == "1" || s == "true" || s == "on" || s == "yes";
        };
        if (truthy(std::getenv("YAMS_PLUGIN_TRUST_ALL"))) {
            return true;
        }
        // Configured plugin directories (global static in PluginLoader)
        try {
            // Access configured plugin directories (highest precedence) by calling
            // PluginLoader::getDefaultPluginDirectories() and only considering those that
            // actually exist and contain the candidate path. This allows config-driven plugin
            // dirs to work without separate trust persistence in tests.
            std::vector<std::filesystem::path> defaults;
#ifdef _WIN32
            // Windows: use LOCALAPPDATA for user plugins
            if (const char* localAppData = std::getenv("LOCALAPPDATA"))
                defaults.push_back(std::filesystem::path(localAppData) / "yams" / "plugins");
            else if (const char* userProfile = std::getenv("USERPROFILE"))
                defaults.push_back(std::filesystem::path(userProfile) / "AppData" / "Local" / "yams" / "plugins");
#else
            if (const char* home = std::getenv("HOME"))
                defaults.push_back(std::filesystem::path(home) / ".local" / "lib" / "yams" /
                                   "plugins");
            defaults.push_back(std::filesystem::path("/usr/local/lib/yams/plugins"));
            defaults.push_back(std::filesystem::path("/usr/lib/yams/plugins"));
#endif
#ifdef YAMS_INSTALL_PREFIX
            defaults.push_back(std::filesystem::path(YAMS_INSTALL_PREFIX) / "lib" / "yams" /
                               "plugins");
#endif
            for (const auto& cd : defaults) {
                std::error_code ec;
                if (cd.empty() || !std::filesystem::exists(cd, ec))
                    continue;
                auto base = std::filesystem::weakly_canonical(cd, ec);
                auto target = std::filesystem::weakly_canonical(p, ec);
                if (!ec) {
                    auto bstr = base.string();
                    auto tstr = target.string();
                    if (!bstr.empty() && tstr.rfind(bstr, 0) == 0) {
                        return true;
                    }
                }
            }
        } catch (...) {
        }
        try {
            if (const char* dir = std::getenv("YAMS_PLUGIN_DIR")) {
                std::error_code ec;
                auto base = std::filesystem::weakly_canonical(std::filesystem::path(dir), ec);
                auto target = std::filesystem::weakly_canonical(p, ec);
                if (!ec) {
                    auto bstr = base.string();
                    auto tstr = target.string();
                    if (!bstr.empty() && tstr.rfind(bstr, 0) == 0) {
                        return true; // implicit trust for override directory
                    }
                }
            }
        } catch (...) {
        }
        return false; // default deny otherwise
    }
    auto canon = std::filesystem::weakly_canonical(p);
    for (const auto& t : trusted_) {
        if (canon.string().rfind(t.string(), 0) == 0)
            return true; // prefix match
    }
    return false;
}

} // namespace yams::daemon

// Re-open namespace for interface retrieval implementation
namespace yams::daemon {

Result<void*> AbiPluginLoader::getInterface(const std::string& name, const std::string& ifaceId,
                                            uint32_t version) const {
    auto it = loaded_.find(name);
    if (it == loaded_.end() || !it->second.handle) {
        return Error{ErrorCode::NotFound, "Plugin not loaded: " + name};
    }
    void* handle = it->second.handle;
    using GetIfaceFn = int (*)(const char*, uint32_t, void**);
    dlerror();
    auto get_iface = reinterpret_cast<GetIfaceFn>(dlsym(handle, "yams_plugin_get_interface"));
    const char* dlerr = dlerror();
    if (!get_iface || dlerr) {
        return Error{ErrorCode::InvalidArgument, "ABI interface function not found"};
    }
    void* out = nullptr;
    int rc = get_iface(ifaceId.c_str(), version, &out);
    if (rc != YAMS_PLUGIN_OK || !out) {
        return Error{ErrorCode::NotFound, "Interface not available: " + ifaceId};
    }
    return Result<void*>(out);
}

} // namespace yams::daemon
