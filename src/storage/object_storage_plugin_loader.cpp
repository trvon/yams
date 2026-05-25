#define _CRT_SECURE_NO_WARNINGS
#include <yams/storage/storage_backend.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <filesystem>
#include <string>
#include <vector>

#ifdef _WIN32
#include <windows.h>
#else
#include <dlfcn.h>
#endif

namespace yams::storage {

namespace fs = std::filesystem;

using CreateFn = IStorageBackend* (*)();
using DestroyFn = void (*)(IStorageBackend*);

class PluginBackendProxy : public IStorageBackend {
public:
    PluginBackendProxy(void* handle, DestroyFn destroyFn, IStorageBackend* inner)
        : handle_(handle), destroyFn_(destroyFn), inner_(inner) {}

    ~PluginBackendProxy() override {
        if (inner_ && destroyFn_) {
            destroyFn_(inner_);
            inner_ = nullptr;
        }
        if (handle_) {
#ifdef _WIN32
            FreeLibrary(static_cast<HMODULE>(handle_));
#else
            dlclose(handle_);
#endif
            handle_ = nullptr;
        }
    }

    Result<void> initialize(const BackendConfig& config) override {
        return inner_->initialize(config);
    }
    Result<void> store(std::string_view key, std::span<const std::byte> data) override {
        return inner_->store(key, data);
    }
    Result<std::vector<std::byte>> retrieve(std::string_view key) const override {
        return inner_->retrieve(key);
    }
    Result<bool> exists(std::string_view key) const override { return inner_->exists(key); }
    Result<void> remove(std::string_view key) override { return inner_->remove(key); }
    Result<std::vector<std::string>> list(std::string_view prefix) const override {
        return inner_->list(prefix);
    }
    Result<::yams::StorageStats> getStats() const override { return inner_->getStats(); }
    std::future<Result<void>> storeAsync(std::string_view key,
                                         std::span<const std::byte> data) override {
        return inner_->storeAsync(key, data);
    }
    std::future<Result<std::vector<std::byte>>> retrieveAsync(std::string_view key) const override {
        return inner_->retrieveAsync(key);
    }
    std::string getType() const override { return inner_->getType(); }
    bool isRemote() const override { return inner_->isRemote(); }
    Result<void> flush() override { return inner_->flush(); }

private:
    void* handle_{};
    DestroyFn destroyFn_{};
    IStorageBackend* inner_{};
};

static std::string libExtension() {
#if defined(__APPLE__)
    return "dylib";
#elif defined(__linux__)
    return "so";
#elif defined(_WIN32)
    return "dll";
#else
    return "so";
#endif
}

static bool endsWith(const std::string& value, const std::string& suffix) {
    return value.size() >= suffix.size() &&
           value.compare(value.size() - suffix.size(), suffix.size(), suffix) == 0;
}

static void addUnique(std::vector<std::string>& values, std::string value) {
    if (value.empty()) {
        return;
    }
    if (std::find(values.begin(), values.end(), value) == values.end()) {
        values.push_back(std::move(value));
    }
}

static std::vector<std::string> candidatePluginFileNames(const std::vector<std::string>& names) {
    std::vector<std::string> files;
    const std::string extension = "." + libExtension();

    for (const auto& name : names) {
        addUnique(files, name);
        if (!endsWith(name, extension)) {
            addUnique(files, name + extension);
        }
#ifndef _WIN32
        if (!name.starts_with("lib")) {
            addUnique(files, "lib" + name);
            if (!endsWith(name, extension)) {
                addUnique(files, "lib" + name + extension);
            }
        }
#endif
    }

    return files;
}

static std::vector<fs::path> defaultPluginDirs() { // unchanged
    std::vector<fs::path> dirs;
    if (const char* env = std::getenv("YAMS_PLUGIN_DIR")) {
        dirs.emplace_back(env);
        return dirs;
    }
    if (const char* home = std::getenv("HOME")) {
        dirs.emplace_back(fs::path(home) / ".local" / "lib" / "yams" / "plugins");
    }
#if defined(__APPLE__)
    dirs.emplace_back("/opt/homebrew/lib/yams/plugins");
#endif
    dirs.emplace_back("/usr/local/lib/yams/plugins");
    dirs.emplace_back("/usr/lib/yams/plugins");
    return dirs;
}

static fs::path resolvePluginPath(const std::vector<std::string>& names) {
    for (const auto& name : candidatePluginFileNames(names)) {
        fs::path direct{name};
        if ((direct.is_absolute() || direct.has_parent_path()) && fs::exists(direct)) {
            return direct;
        }

        // Prefer repo-local build artifacts first to avoid loading an older globally-installed
        // plugin when running from a development checkout.
        fs::path local = fs::path("plugins") / "object_storage_s3" / name;
        if (fs::exists(local)) {
            return local;
        }

        fs::path buildPlugin = fs::path("build") / "plugins" / "object_storage_s3" / name;
        if (fs::exists(buildPlugin)) {
            return buildPlugin;
        }

        fs::path builddirPlugin = fs::path("builddir") / "plugins" / "object_storage_s3" / name;
        if (fs::exists(builddirPlugin)) {
            return builddirPlugin;
        }

        fs::path buildFlat = fs::path("build") / name;
        if (fs::exists(buildFlat)) {
            return buildFlat;
        }

        fs::path builddirFlat = fs::path("builddir") / name;
        if (fs::exists(builddirFlat)) {
            return builddirFlat;
        }

        for (const auto& dir : defaultPluginDirs()) {
            fs::path p = dir / name;
            if (fs::exists(p)) {
                return p;
            }
        }
    }
    return {};
}

static std::unique_ptr<IStorageBackend> loadSpecific(const char* createSym, const char* destroySym,
                                                     const std::vector<std::string>& names) {
    fs::path path = resolvePluginPath(names);
    if (path.empty()) {
        return nullptr;
    }

#ifdef _WIN32
    HMODULE handle = LoadLibraryA(path.string().c_str());
    if (!handle) {
        spdlog::warn("Failed to open {}: GetLastError={}", path.string(), GetLastError());
        return nullptr;
    }

    auto create = reinterpret_cast<CreateFn>(GetProcAddress(handle, createSym));
    auto destroy = reinterpret_cast<DestroyFn>(GetProcAddress(handle, destroySym));
#else
    void* handle = dlopen(path.string().c_str(), RTLD_LAZY | RTLD_LOCAL);
    if (!handle) {
        spdlog::warn("Failed to open {}: {}", path.string(), dlerror() ? dlerror() : "");
        return nullptr;
    }
    dlerror();
    auto create = reinterpret_cast<CreateFn>(dlsym(handle, createSym));
    auto destroy = reinterpret_cast<DestroyFn>(dlsym(handle, destroySym));
#endif

    if (!create || !destroy) {
        spdlog::warn("Missing create/destroy in {}", path.string());
#ifdef _WIN32
        FreeLibrary(handle);
#else
        dlclose(handle);
#endif
        return nullptr;
    }
    IStorageBackend* inner = create();
    if (!inner) {
#ifdef _WIN32
        FreeLibrary(handle);
#else
        dlclose(handle);
#endif
        return nullptr;
    }
    return std::unique_ptr<IStorageBackend>(new PluginBackendProxy(handle, destroy, inner));
}

std::unique_ptr<IStorageBackend> tryCreatePluginBackendByName(const std::string& name,
                                                              const BackendConfig& config) {
    std::vector<std::string> candidates;
    candidates.push_back(name);
    if (!name.starts_with("yams_"))
        candidates.push_back("yams_" + name);
    auto backend = loadSpecific("yams_plugin_create_object_storage",
                                "yams_plugin_destroy_object_storage", candidates);
    if (!backend)
        return nullptr;
    if (auto r = backend->initialize(config); !r) {
        spdlog::warn("Plugin '{}' initialize failed: {}", name, r.error().message);
        return nullptr;
    }
    return backend;
}

std::unique_ptr<IStorageBackend> tryCreateS3PluginBackend(const BackendConfig& config) {
    return tryCreatePluginBackendByName("object_storage_s3", config);
}

} // namespace yams::storage
