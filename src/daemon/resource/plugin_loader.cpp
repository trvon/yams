#include <spdlog/spdlog.h>
#include <yams/daemon/resource/graph_adapter.h>
#include <yams/daemon/resource/model_provider.h>
#include <yams/daemon/resource/plugin_loader.h>

#include <climits> // For PATH_MAX
#include <cstring>
#include <dlfcn.h> // For dlopen, dlsym, dlclose
#include <filesystem>
#include <regex>
#ifndef _WIN32
#include <unistd.h> // For readlink on Unix-like systems
#endif

#ifdef __APPLE__
#include <mach-o/dyld.h> // For _NSGetExecutablePath
#endif

namespace yams::daemon {

namespace fs = std::filesystem;

// External function to register model providers
extern void registerModelProvider(const std::string& name, ModelProviderFactory factory);

// Simple in-process registry for GraphAdapters (backed by this TU)
static std::unordered_map<std::string, GraphAdapterFactory>& _graph_registry() {
    static std::unordered_map<std::string, GraphAdapterFactory> m;
    return m;
}

void registerGraphAdapter(const std::string& name, GraphAdapterFactory factory) {
    _graph_registry()[name] = std::move(factory);
}
std::vector<std::string> getRegisteredGraphAdapters() {
    std::vector<std::string> out;
    for (const auto& kv : _graph_registry())
        out.push_back(kv.first);
    return out;
}
std::unique_ptr<IGraphAdapter> createGraphAdapter(const std::string& preferred) {
    if (!preferred.empty()) {
        auto it = _graph_registry().find(preferred);
        if (it != _graph_registry().end())
            return it->second();
    }
    if (!_graph_registry().empty())
        return _graph_registry().begin()->second();
    return nullptr;
}

PluginLoader::PluginLoader() = default;

PluginLoader::~PluginLoader() {
    unloadAllPlugins();
}

Result<void> PluginLoader::loadPlugin(const std::filesystem::path& pluginPath) {
    try {
        // Check if file exists
        if (!fs::exists(pluginPath)) {
            return Error{ErrorCode::FileNotFound, "Plugin file not found: " + pluginPath.string()};
        }

        // Note: Do not pre-deduplicate by filename; multiple filenames may provide the
        // same provider. We'll deduplicate by provider name after dlopen/dlsym.

        spdlog::info("Loading plugin: {}", pluginPath.string());

        // Load the shared library with error handling
        void* handle = nullptr;

        // Clear any existing errors
        dlerror();

        // Try to load the plugin
        handle = dlopen(pluginPath.c_str(), RTLD_LAZY | RTLD_LOCAL);

        if (!handle) {
            const char* error_msg = dlerror();
            std::string error = error_msg ? error_msg : "Unknown error";

            // Don't treat as fatal error in test mode
            if (std::getenv("YAMS_TEST_MODE") != nullptr) {
                spdlog::debug("Plugin load failed in test mode: {}", error);
                return Error{ErrorCode::InternalError,
                             "Failed to load plugin (test mode): " + error};
            }

            return Error{ErrorCode::InternalError, "Failed to load plugin: " + error};
        }

        // Reset errors
        dlerror();

        // Detect interfaces exposed by this shared object
        bool registeredSomething = false;

        // Model Provider (legacy)
        typedef const char* (*GetProviderNameFunc)();
        GetProviderNameFunc getProviderName =
            reinterpret_cast<GetProviderNameFunc>(dlsym(handle, "getProviderName"));
        typedef IModelProvider* (*CreateProviderFunc)();
        CreateProviderFunc createProvider = nullptr;
        std::string modelName;
        if (getProviderName) {
            createProvider =
                reinterpret_cast<CreateProviderFunc>(dlsym(handle, "createOnnxProvider"));
            if (!createProvider)
                createProvider =
                    reinterpret_cast<CreateProviderFunc>(dlsym(handle, "createProvider"));
            if (createProvider) {
                const char* pn = getProviderName();
                if (pn && *pn)
                    modelName = pn;
            }
        }

        // Graph Adapter v1
        typedef const char* (*GetGraphAdapterNameFunc)();
        typedef IGraphAdapter* (*CreateGraphAdapterFunc)();
        GetGraphAdapterNameFunc getGraphAdapterName =
            reinterpret_cast<GetGraphAdapterNameFunc>(dlsym(handle, "getGraphAdapterName"));
        CreateGraphAdapterFunc createGraphAdapterFn = nullptr;
        std::string graphName;
        if (getGraphAdapterName) {
            createGraphAdapterFn =
                reinterpret_cast<CreateGraphAdapterFunc>(dlsym(handle, "createGraphAdapterV1"));
            if (!createGraphAdapterFn)
                createGraphAdapterFn =
                    reinterpret_cast<CreateGraphAdapterFunc>(dlsym(handle, "createGraphAdapter"));
            if (createGraphAdapterFn) {
                const char* gn = getGraphAdapterName();
                if (gn && *gn)
                    graphName = gn;
            }
        }

        if (modelName.empty() && graphName.empty()) {
            dlclose(handle);
            return Error{ErrorCode::InvalidArgument,
                         "Plugin does not expose a known interface: " + pluginPath.string()};
        }

        // Decide precedence when the same provider is found in multiple files/dirs.
        auto scorePath = [](const fs::path& p) -> int {
            // Lower score means higher priority.
            int s = 0;
            auto fname = p.filename().string();
            if (fname.rfind("lib", 0) == 0) {
                s += 10; // prefer non-lib prefixed names
            }
            std::string str = p.string();
            if (const char* home = std::getenv("HOME")) {
                fs::path userDir = fs::path(home) / ".local" / "lib" / "yams" / "plugins";
                if (str.rfind(userDir.string(), 0) == 0)
                    s += 0; // highest
            }
            if (str.find("/usr/local/lib/yams/plugins") != std::string::npos)
                s += 5;
            if (str.find("/usr/lib/yams/plugins") != std::string::npos)
                s += 8;
            return s;
        };

        int newScore = scorePath(pluginPath);

        auto ensureRecord = [&](const std::string& logicalKey, const std::string& humanName,
                                const std::string& ifaceTag) {
            auto it = plugins_.find(logicalKey);
            if (it != plugins_.end() && it->second.loaded) {
                int oldScore = scorePath(it->second.path);
                if (newScore >= oldScore) {
                    dlclose(handle);
                    spdlog::info("Skipping duplicate '{}' from {} (kept {} with higher priority)",
                                 humanName, pluginPath.string(), it->second.path.string());
                    return false;
                }
                (void)unloadPlugin(logicalKey);
            }
            PluginInfo info;
            info.name = humanName;
            info.path = pluginPath;
            info.handle = handle;
            info.loaded = true;
            info.interfaces.push_back(ifaceTag);
            plugins_[logicalKey] = info;
            return true;
        };

        if (!modelName.empty()) {
            if (ensureRecord(std::string("model:") + modelName, modelName, "model_provider")) {
                registerModelProvider(modelName,
                                      [createProvider]() -> std::unique_ptr<IModelProvider> {
                                          return std::unique_ptr<IModelProvider>(createProvider());
                                      });
                spdlog::info("Loaded model provider '{}' from {}", modelName, pluginPath.string());
                registeredSomething = true;
            }
        }
        if (!graphName.empty()) {
            if (ensureRecord(std::string("graph:") + graphName, graphName, "graph_adapter_v1")) {
                registerGraphAdapter(
                    graphName, [createGraphAdapterFn]() -> std::unique_ptr<IGraphAdapter> {
                        return std::unique_ptr<IGraphAdapter>(createGraphAdapterFn());
                    });
                spdlog::info("Loaded graph adapter '{}' from {}", graphName, pluginPath.string());
                registeredSomething = true;
            }
        }

        if (!registeredSomething) {
            dlclose(handle);
            return Error{ErrorCode::InvalidArgument,
                         "Plugin did not register any known interface: " + pluginPath.string()};
        }

        return Result<void>();

    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError,
                     std::string("Exception loading plugin: ") + e.what()};
    }
}

Result<size_t> PluginLoader::loadPluginsFromDirectory(const std::filesystem::path& directory,
                                                      [[maybe_unused]] const std::string& pattern) {
    try {
        if (!fs::exists(directory)) {
            spdlog::debug("Plugin directory does not exist: {}", directory.string());
            return Result<size_t>(0);
        }

        if (!fs::is_directory(directory)) {
            return Error{ErrorCode::InvalidArgument,
                         "Path is not a directory: " + directory.string()};
        }

        // Determine acceptable extensions (macOS dev builds sometimes produce .so for MODULE libs)
        std::vector<std::string> allowedExts;
#ifdef __APPLE__
        allowedExts = {".dylib", ".so"};
#elif defined(_WIN32)
        allowedExts = {".dll"};
#else
        allowedExts = {".so"};
#endif

        size_t loadedCount = 0;
        size_t totalFiles = 0;

        spdlog::info("Scanning for plugins in: {}", directory.string());

        // First, collect file names to help detect duplicate variants (e.g., 'libfoo.dylib' and
        // 'foo.dylib' both present). We prefer the non-'lib' prefixed variant on Apple to avoid
        // stale duplicates from legacy builds.
        std::vector<fs::directory_entry> entries;
        for (const auto& entry : fs::directory_iterator(directory)) {
            entries.push_back(entry);
        }

        for (const auto& entry : entries) {
            if (!entry.is_regular_file()) {
                continue;
            }
            totalFiles++;

            std::string filename = entry.path().filename().string();

            // Manual extension check instead of regex
            bool matchesExt = false;
            for (const auto& ext : allowedExts) {
                if (filename.size() > ext.size() &&
                    filename.compare(filename.size() - ext.size(), ext.size(), ext) == 0) {
                    matchesExt = true;
                    break;
                }
            }
            if (!matchesExt) {
                continue;
            }

            // Skip symlinks to avoid duplicate loading
            if (fs::is_symlink(entry.path())) {
                continue;
            }

#ifdef __APPLE__
            // Prefer non-'lib' prefixed variant when both exist (helps avoid legacy duplicates)
            if (filename.rfind("lib", 0) == 0) {
                // Candidate without 'lib' prefix
                std::string alt = filename.substr(3);
                fs::path altPath = entry.path().parent_path() / alt;
                if (fs::exists(altPath) && fs::is_regular_file(altPath)) {
                    spdlog::info("Skipping duplicate plugin '{}' in favor of '{}'", filename, alt);
                    continue;
                }
            }
#endif

            // Try to load the plugin
            auto result = loadPlugin(entry.path());
            if (result) {
                loadedCount++;
            } else {
                spdlog::warn("Failed to load plugin {}: {}", filename, result.error().message);
            }
        }

        spdlog::info("Loaded {} out of {} potential plugins from {}", loadedCount, totalFiles,
                     directory.string());
        return Result<size_t>(loadedCount);

    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError,
                     std::string("Exception scanning plugin directory: ") + e.what()};
    }
}

Result<void> PluginLoader::unloadPlugin(const std::string& pluginName) {
    try {
        auto it = plugins_.find(pluginName);
        if (it == plugins_.end()) {
            return Error{ErrorCode::NotFound, "Plugin not found: " + pluginName};
        }

        if (!it->second.loaded) {
            return Result<void>(); // Already unloaded
        }

        if (it->second.handle) {
            if (dlclose(it->second.handle) != 0) {
                std::string error = dlerror() ? dlerror() : "Unknown error";
                return Error{ErrorCode::InternalError, "Failed to unload plugin: " + error};
            }
        }

        it->second.loaded = false;
        it->second.handle = nullptr;

        spdlog::info("Unloaded plugin: {}", pluginName);
        return Result<void>();
    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError,
                     std::string("Exception unloading plugin: ") + e.what()};
    }
}

void PluginLoader::unloadAllPlugins() {
    for (auto& [name, info] : plugins_) {
        if (info.loaded && info.handle) {
            if (dlclose(info.handle) == 0) {
                spdlog::debug("Unloaded plugin: {}", name);
            } else {
                spdlog::warn("Failed to unload plugin {}: {}", name,
                             dlerror() ? dlerror() : "Unknown error");
            }
            info.loaded = false;
            info.handle = nullptr;
        }
    }
    plugins_.clear();
}

std::vector<PluginInfo> PluginLoader::getLoadedPlugins() const {
    std::vector<PluginInfo> result;
    for (const auto& [name, info] : plugins_) {
        if (info.loaded) {
            result.push_back(info);
        }
    }
    return result;
}

bool PluginLoader::isPluginLoaded(const std::string& pluginName) const {
    auto it = plugins_.find(pluginName);
    return it != plugins_.end() && it->second.loaded;
}

// static storage for configured directories
std::vector<std::filesystem::path> PluginLoader::configuredDirs_{};

void PluginLoader::setConfiguredPluginDirectories(const std::vector<std::filesystem::path>& dirs) {
    configuredDirs_.clear();
    for (const auto& d : dirs) {
        if (!d.empty()) {
            configuredDirs_.push_back(d);
        }
    }
}

std::vector<std::filesystem::path> PluginLoader::getDefaultPluginDirectories() {
    std::vector<fs::path> directories;

    // 1. Configured directories (highest precedence; stable order; may include multiple)
    for (const auto& c : configuredDirs_) {
        directories.push_back(c);
    }

    // 2. Environment variable (deprecated; additive not exclusive)
    if (const char* pluginDir = std::getenv("YAMS_PLUGIN_DIR")) {
        directories.push_back(fs::path(pluginDir));
    }

    // 3. Build directory (for development)
    // Look for plugin next to the daemon executable
    try {
#ifdef __APPLE__
        // macOS doesn't have /proc/self/exe
        char exePath[PATH_MAX];
        uint32_t size = sizeof(exePath);
        if (_NSGetExecutablePath(exePath, &size) == 0) {
            fs::path exeDir = fs::path(exePath).parent_path();
#else
        char exePath[PATH_MAX];
        ssize_t len = readlink("/proc/self/exe", exePath, sizeof(exePath) - 1);
        if (len != -1) {
            exePath[len] = '\0';
            fs::path exeDir = fs::path(exePath).parent_path();
#endif

            // Check common build layouts: plugin adjacent to binaries
            if (fs::exists(exeDir / "libyams_onnx_plugin.dylib") ||
                fs::exists(exeDir / "libyams_onnx_plugin.so")) {
                directories.push_back(exeDir);
            }

            // Dev convenience: look for build plugins/ tree next to the executable
            fs::path devPluginsDir = exeDir / ".." / "plugins";
            if (fs::exists(devPluginsDir)) {
                directories.push_back(devPluginsDir);
                if (fs::exists(devPluginsDir / "onnx")) {
                    directories.push_back(devPluginsDir / "onnx");
                }
            }
        }
    } catch (...) {
        // Ignore errors in finding exe path
    }

    // 4. User plugin directory
    if (const char* home = std::getenv("HOME")) {
        directories.push_back(fs::path(home) / ".local" / "lib" / "yams" / "plugins");
    }

    // 5. System plugin directories
    directories.push_back(fs::path("/usr/local/lib/yams/plugins"));
    directories.push_back(fs::path("/usr/lib/yams/plugins"));

    // 6. Installation prefix (if configured via compile definition)
    // Note: CMAKE_INSTALL_PREFIX is not defined as a preprocessor macro by default.
    // We pass YAMS_INSTALL_PREFIX from CMake to enable runtime discovery of
    // installed plugins.
#ifdef YAMS_INSTALL_PREFIX
    directories.push_back(fs::path(YAMS_INSTALL_PREFIX) / "lib" / "yams" / "plugins");
#endif

    return directories;
}

Result<size_t> PluginLoader::autoLoadPlugins() {
    auto directories = getDefaultPluginDirectories();
    size_t totalLoaded = 0;

    for (const auto& dir : directories) {
        if (fs::exists(dir) && fs::is_directory(dir)) {
            auto result = loadPluginsFromDirectory(dir);
            if (result) {
                totalLoaded += result.value();
            }
        }
    }

    // Also try to load specific known plugins from build directory if in
    // development
    if (totalLoaded == 0) {
        // Try to find and load the ONNX plugin specifically
        std::vector<fs::path> searchPaths = {fs::path("build/yams-release/src/daemon"),
                                             fs::path("build/src/daemon"), fs::path("src/daemon")};

        std::string pluginName = getPluginLibraryName("yams_onnx_plugin");

        for (const auto& searchPath : searchPaths) {
            fs::path pluginPath = searchPath / pluginName;
            if (fs::exists(pluginPath)) {
                auto result = loadPlugin(pluginPath);
                if (result) {
                    totalLoaded++;
                    break;
                }
            }
        }
    }

    return Result<size_t>(totalLoaded);
}

std::string PluginLoader::getPlatformLibraryExtension() {
#ifdef __APPLE__
    return "dylib";
#elif defined(__linux__)
    return "so";
#elif defined(_WIN32)
    return "dll";
#else
    return "so"; // Default to Linux
#endif
}

std::string PluginLoader::getPluginLibraryName(const std::string& pluginName) {
    std::string prefix = "";
    std::string extension = getPlatformLibraryExtension();

#ifndef _WIN32
    prefix = "lib"; // Unix-like systems use lib prefix
#endif

    return prefix + pluginName + "." + extension;
}

} // namespace yams::daemon
