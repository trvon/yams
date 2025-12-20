#include <cstdlib>
#include <yams/config/config_helpers.h>
#include <yams/daemon/components/dispatch_utils.hpp>

namespace yams::daemon::dispatch {
const std::vector<std::filesystem::path>& defaultAbiPluginDirs() noexcept {
    static std::vector<std::filesystem::path> dirs = [] {
        std::vector<std::filesystem::path> d;
#ifdef _WIN32
        // Windows: user plugins in LOCALAPPDATA
        if (const char* localAppData = std::getenv("LOCALAPPDATA"))
            d.push_back(std::filesystem::path(localAppData) / "yams" / "plugins");
        else if (const char* userProfile = std::getenv("USERPROFILE"))
            d.push_back(std::filesystem::path(userProfile) / "AppData" / "Local" / "yams" /
                        "plugins");
        // Windows: system plugins in Program Files (MSI install location)
        if (const char* programFiles = std::getenv("ProgramFiles"))
            d.push_back(std::filesystem::path(programFiles) / "YAMS" / "lib" / "yams" / "plugins");
#else
        if (const char* home = std::getenv("HOME"))
            d.push_back(std::filesystem::path(home) / ".local" / "lib" / "yams" / "plugins");
#ifdef __APPLE__
        // macOS: Homebrew default install location
        d.push_back(std::filesystem::path("/opt/homebrew/lib/yams/plugins"));
#endif
        d.push_back(std::filesystem::path("/usr/local/lib/yams/plugins"));
        d.push_back(std::filesystem::path("/usr/lib/yams/plugins"));
#endif
#ifdef YAMS_INSTALL_PREFIX
        d.push_back(std::filesystem::path(YAMS_INSTALL_PREFIX) / "lib" / "yams" / "plugins");
#endif
        return d;
    }();
    return dirs;
}

const std::vector<std::filesystem::path>& defaultExternalPluginDirs() noexcept {
    static std::vector<std::filesystem::path> dirs = [] {
        std::vector<std::filesystem::path> d;
#ifdef _WIN32
        // Windows: user plugins in LOCALAPPDATA
        if (const char* localAppData = std::getenv("LOCALAPPDATA"))
            d.push_back(std::filesystem::path(localAppData) / "yams" / "external-plugins");
        else if (const char* userProfile = std::getenv("USERPROFILE"))
            d.push_back(std::filesystem::path(userProfile) / "AppData" / "Local" / "yams" /
                        "external-plugins");
        // Windows: system plugins in Program Files (MSI install location)
        if (const char* programFiles = std::getenv("ProgramFiles"))
            d.push_back(std::filesystem::path(programFiles) / "YAMS" / "lib" / "yams" /
                        "external-plugins");
#else
        if (const char* home = std::getenv("HOME"))
            d.push_back(std::filesystem::path(home) / ".local" / "lib" / "yams" /
                        "external-plugins");
#ifdef __APPLE__
        // macOS: Homebrew default install location
        d.push_back(std::filesystem::path("/opt/homebrew/lib/yams/external-plugins"));
#endif
        d.push_back(std::filesystem::path("/usr/local/lib/yams/external-plugins"));
        d.push_back(std::filesystem::path("/usr/lib/yams/external-plugins"));
#endif
#ifdef YAMS_INSTALL_PREFIX
        d.push_back(std::filesystem::path(YAMS_INSTALL_PREFIX) / "lib" / "yams" /
                    "external-plugins");
#endif
        return d;
    }();
    return dirs;
}
} // namespace yams::daemon::dispatch
