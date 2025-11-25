#include <cstdlib>
#include <yams/daemon/components/dispatch_utils.hpp>

namespace yams::daemon::dispatch {
const std::vector<std::filesystem::path>& defaultAbiPluginDirs() noexcept {
    static std::vector<std::filesystem::path> dirs = [] {
        std::vector<std::filesystem::path> d;
#ifdef _WIN32
        // Windows: use LOCALAPPDATA for user plugins
        if (const char* localAppData = std::getenv("LOCALAPPDATA"))
            d.push_back(std::filesystem::path(localAppData) / "yams" / "plugins");
        else if (const char* userProfile = std::getenv("USERPROFILE"))
            d.push_back(std::filesystem::path(userProfile) / "AppData" / "Local" / "yams" /
                        "plugins");
#else
        if (const char* home = std::getenv("HOME"))
            d.push_back(std::filesystem::path(home) / ".local" / "lib" / "yams" / "plugins");
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
} // namespace yams::daemon::dispatch
