#define _CRT_SECURE_NO_WARNINGS
#include <yams/storage/storage_backend.h>

#include <spdlog/spdlog.h>

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

static std::vector<fs::path> defaultPluginDirs() { // unchanged
    std::vector<fs::path> dirs;
    if (const char* env = std::getenv("YAMS_PLUGIN_DIR")) {
        dirs.emplace_back(env);
        return dirs;
    }
    if (const char* home = std::getenv("HOME")) {
        dirs.emplace_back(fs::path(home) / ".local" / "lib" / "yams" / "plugins");
    }
    dirs.emplace_back("/usr/local/lib/yams/plugins");
    dirs.emplace_back("/usr/lib/yams/plugins");
    return dirs;
}

static fs::path resolveS3PluginPath() {
    std::string base = "libyams_object_storage_s3." + libExtension();
#ifndef _WIN32
    std::vector<std::string> candidates{base};
#else
    std::vector<std::string> candidates{"yams_object_storage_s3." + libExtension()};
#endif
    for (const auto& name : candidates) {
        for (const auto& dir : defaultPluginDirs()) {
            fs::path p = dir / name;
            if (fs::exists(p))
                return p;
        }
        fs::path local = fs::path("plugins") / "object_storage_s3" / name;
        if (fs::exists(local))
            return local;
        fs::path build1 = fs::path("build") / name;
        if (fs::exists(build1))
            return build1;
    }
    return {};
}

static std::unique_ptr<IStorageBackend> loadSpecific(const char* /*createSym*/,
                                                     const char* /*destroySym*/,
                                                     const std::vector<std::string>& /*names*/) {
    fs::path path = resolveS3PluginPath();
    if (path.empty())
        return nullptr;

#ifdef _WIN32
    HMODULE handle = LoadLibraryA(path.string().c_str());
    if (!handle) {
        spdlog::warn("Failed to open {}: GetLastError={}", path.string(), GetLastError());
        return nullptr;
    }

    auto create =
        reinterpret_cast<CreateFn>(GetProcAddress(handle, "yams_plugin_create_object_storage"));
    auto destroy =
        reinterpret_cast<DestroyFn>(GetProcAddress(handle, "yams_plugin_destroy_object_storage"));
#else
    void* handle = dlopen(path.c_str(), RTLD_LAZY | RTLD_LOCAL);
    if (!handle) {
        spdlog::warn("Failed to open {}: {}", path.string(), dlerror() ? dlerror() : "");
        return nullptr;
    }
    dlerror();
    auto create = reinterpret_cast<CreateFn>(dlsym(handle, "yams_plugin_create_object_storage"));
    auto destroy = reinterpret_cast<DestroyFn>(dlsym(handle, "yams_plugin_destroy_object_storage"));
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
