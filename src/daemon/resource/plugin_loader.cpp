#include <spdlog/spdlog.h>
#include <yams/daemon/resource/model_provider.h>
#include <yams/daemon/resource/plugin_loader.h>

#include <climits> // For PATH_MAX
#include <dlfcn.h> // For dlopen, dlsym, dlclose
#include <filesystem>
#include <regex>

#ifdef __APPLE__
#include <mach-o/dyld.h> // For _NSGetExecutablePath
#endif

namespace yams::daemon {

namespace fs = std::filesystem;

// External function to register model providers
extern void registerModelProvider(const std::string& name, ModelProviderFactory factory);

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

        // Check if already loaded
        std::string pluginName = pluginPath.stem().string();
        if (plugins_.find(pluginName) != plugins_.end() && plugins_[pluginName].loaded) {
            spdlog::debug("Plugin already loaded: {}", pluginName);
            return Result<void>();
        }

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

        // Look for the required symbols
        // Reset errors
        dlerror();

        // Get the provider name function
        typedef const char* (*GetProviderNameFunc)();
        GetProviderNameFunc getProviderName =
            reinterpret_cast<GetProviderNameFunc>(dlsym(handle, "getProviderName"));

        if (!getProviderName) {
            dlclose(handle);
            return Error{ErrorCode::InvalidArgument,
                         "Plugin missing getProviderName function: " + pluginPath.string()};
        }

        // Get the factory function
        typedef IModelProvider* (*CreateProviderFunc)();
        CreateProviderFunc createProvider =
            reinterpret_cast<CreateProviderFunc>(dlsym(handle, "createOnnxProvider"));

        if (!createProvider) {
            // Try generic name
            createProvider = reinterpret_cast<CreateProviderFunc>(dlsym(handle, "createProvider"));
        }

        if (!createProvider) {
            dlclose(handle);
            return Error{ErrorCode::InvalidArgument,
                         "Plugin missing create function: " + pluginPath.string()};
        }

        // Get the provider name
        const char* providerName = getProviderName();
        if (!providerName || strlen(providerName) == 0) {
            dlclose(handle);
            return Error{ErrorCode::InvalidArgument, "Plugin returned invalid provider name"};
        }

        // Register the provider with the factory
        std::string name(providerName);

        // Register using a lambda that captures the function pointer
        registerModelProvider(name, [createProvider]() -> std::unique_ptr<IModelProvider> {
            return std::unique_ptr<IModelProvider>(createProvider());
        });

        // Store plugin info
        PluginInfo info;
        info.name = name;
        info.path = pluginPath;
        info.handle = handle;
        info.loaded = true;
        plugins_[name] = info;

        spdlog::info("Successfully loaded {} plugin from {}", name, pluginPath.string());
        return Result<void>();

    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError,
                     std::string("Exception loading plugin: ") + e.what()};
    }
}

Result<size_t> PluginLoader::loadPluginsFromDirectory(const std::filesystem::path& directory,
                                                      const std::string& pattern) {
    try {
        if (!fs::exists(directory)) {
            spdlog::debug("Plugin directory does not exist: {}", directory.string());
            return Result<size_t>(0);
        }

        if (!fs::is_directory(directory)) {
            return Error{ErrorCode::InvalidArgument,
                         "Path is not a directory: " + directory.string()};
        }

        // Determine pattern based on platform if not specified
        std::string filePattern = pattern;
        if (filePattern.empty()) {
            filePattern = std::string(".*\\.") + getPlatformLibraryExtension() + "$";
        }

        std::regex pluginRegex(filePattern);
        size_t loadedCount = 0;

        spdlog::info("Scanning for plugins in: {}", directory.string());

        for (const auto& entry : fs::directory_iterator(directory)) {
            if (!entry.is_regular_file()) {
                continue;
            }

            std::string filename = entry.path().filename().string();

            // Check if filename matches pattern
            if (!std::regex_match(filename, pluginRegex)) {
                continue;
            }

            // Skip symlinks to avoid duplicate loading
            if (fs::is_symlink(entry.path())) {
                continue;
            }

            // Try to load the plugin
            auto result = loadPlugin(entry.path());
            if (result) {
                loadedCount++;
            } else {
                spdlog::warn("Failed to load plugin {}: {}", filename, result.error().message);
            }
        }

        spdlog::info("Loaded {} plugins from {}", loadedCount, directory.string());
        return Result<size_t>(loadedCount);

    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError,
                     std::string("Exception scanning plugin directory: ") + e.what()};
    }
}

Result<void> PluginLoader::unloadPlugin(const std::string& pluginName) {
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

std::vector<std::filesystem::path> PluginLoader::getDefaultPluginDirectories() {
    std::vector<fs::path> directories;

    // 1. Environment variable override (exclusive - if set, only use this)
    if (const char* pluginDir = std::getenv("YAMS_PLUGIN_DIR")) {
        directories.push_back(fs::path(pluginDir));
        return directories; // Return early to make this an exclusive override
    }

    // 2. Build directory (for development)
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

            // Check common build layouts
            if (fs::exists(exeDir / "libyams_onnx_plugin.dylib") ||
                fs::exists(exeDir / "libyams_onnx_plugin.so")) {
                directories.push_back(exeDir);
            }

            // Check relative paths from build directory
            fs::path buildPluginDir = exeDir / ".." / "src" / "daemon";
            if (fs::exists(buildPluginDir)) {
                directories.push_back(buildPluginDir);
            }
        }
    } catch (...) {
        // Ignore errors in finding exe path
    }

    // 3. User plugin directory
    if (const char* home = std::getenv("HOME")) {
        directories.push_back(fs::path(home) / ".local" / "lib" / "yams" / "plugins");
    }

    // 4. System plugin directories
    directories.push_back(fs::path("/usr/local/lib/yams/plugins"));
    directories.push_back(fs::path("/usr/lib/yams/plugins"));

    // 5. Installation prefix (if configured)
#ifdef CMAKE_INSTALL_PREFIX
    directories.push_back(fs::path(CMAKE_INSTALL_PREFIX) / "lib" / "yams" / "plugins");
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

    // Also try to load specific known plugins from build directory if in development
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