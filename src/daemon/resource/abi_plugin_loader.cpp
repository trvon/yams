#include <spdlog/spdlog.h>
#include <dlfcn.h>
#include <fstream>
#include <regex>
#include <yams/app/services/services.hpp>
#include <yams/daemon/resource/abi_plugin_loader.h>
#include <yams/daemon/resource/plugin_host_services.h>
#include <yams/plugins/abi.h>

namespace yams::daemon {

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
    trusted_.erase(p);
    saveTrust();
    return Result<void>();
}

Result<AbiPluginLoader::ScanResult>
AbiPluginLoader::scanTarget(const std::filesystem::path& file) const {
    if (!std::filesystem::exists(file)) {
        return Error{ErrorCode::FileNotFound, "Plugin not found: " + file.string()};
    }
    void* handle = dlopen(file.c_str(), RTLD_LAZY | RTLD_LOCAL);
    if (!handle) {
        return Error{ErrorCode::InternalError,
                     std::string("dlopen failed: ") + (dlerror() ? dlerror() : "unknown")};
    }
    auto close = [&]() { dlclose(handle); };

    auto get_abi = reinterpret_cast<int (*)()>(dlsym(handle, "yams_plugin_get_abi_version"));
    auto get_name = reinterpret_cast<const char* (*)()>(dlsym(handle, "yams_plugin_get_name"));
    auto get_ver = reinterpret_cast<const char* (*)()>(dlsym(handle, "yams_plugin_get_version"));
    auto get_manifest =
        reinterpret_cast<const char* (*)()>(dlsym(handle, "yams_plugin_get_manifest_json"));

    if (!get_abi || !get_name || !get_ver) {
        close();
        return Error{ErrorCode::InvalidArgument, "Missing required ABI symbols"};
    }

    ScanResult sr;
    sr.abiVersion = static_cast<uint32_t>(get_abi());
    sr.name = get_name() ? get_name() : "";
    sr.version = get_ver() ? get_ver() : "";
    sr.path = file;
    if (get_manifest) {
        const char* mj = get_manifest();
        if (mj)
            sr.manifestJson = mj;
        sr.interfaces = parseInterfacesFromManifest(sr.manifestJson);
    }

    close();
    return Result<ScanResult>(sr);
}

Result<std::vector<AbiPluginLoader::ScanResult>>
AbiPluginLoader::scanDirectory(const std::filesystem::path& dir) const {
    if (!std::filesystem::exists(dir) || !std::filesystem::is_directory(dir)) {
        return Error{ErrorCode::InvalidPath, "Not a directory: " + dir.string()};
    }
    std::vector<ScanResult> out;
    for (const auto& entry : std::filesystem::directory_iterator(dir)) {
        if (!entry.is_regular_file())
            continue;
        auto p = entry.path();
        // Match *.so / *.dylib / *.dll
        auto ext = p.extension().string();
        if (ext == ".so" || ext == ".dylib" || ext == ".dll") {
            auto sr = scanTarget(p);
            if (sr)
                out.push_back(sr.value());
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

    void* handle = dlopen(canon.c_str(), RTLD_LAZY | RTLD_LOCAL);
    if (!handle) {
        return Error{ErrorCode::InternalError,
                     std::string("dlopen failed: ") + (dlerror() ? dlerror() : "unknown")};
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
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty())
            continue;
        trusted_.insert(std::filesystem::path(line));
    }
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
    if (trusted_.empty())
        return false; // default deny
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
