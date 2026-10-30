#include <yams/config/static_embedding_table.h>

#include <algorithm>
#include <string>
#include <system_error>
#include <vector>

#if defined(__APPLE__)
#include <mach-o/dyld.h>
#elif defined(_WIN32)
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#elif defined(__linux__)
#include <climits>
#include <unistd.h>
#endif

namespace yams::config {

namespace {

namespace fs = std::filesystem;

std::optional<fs::path> executablePath() {
#if defined(__APPLE__)
    char buf[4096]{};
    uint32_t size = sizeof(buf);
    if (_NSGetExecutablePath(buf, &size) == 0) {
        return fs::path(buf);
    }
#elif defined(_WIN32)
    wchar_t buf[MAX_PATH]{};
    const DWORD n = GetModuleFileNameW(nullptr, buf, MAX_PATH);
    if (n > 0 && n < MAX_PATH) {
        return fs::path(std::wstring(buf, n));
    }
#elif defined(__linux__)
    char buf[PATH_MAX]{};
    const ssize_t n = readlink("/proc/self/exe", buf, sizeof(buf));
    if (n > 0) {
        return fs::path(std::string(buf, static_cast<std::size_t>(n)));
    }
#endif
    return std::nullopt;
}

std::optional<fs::path> firstTableIn(const fs::path& dir) {
    std::error_code ec;
    if (dir.empty() || !fs::is_directory(dir, ec)) {
        return std::nullopt;
    }
    std::vector<fs::path> tables;
    for (const auto& entry : fs::directory_iterator(dir, ec)) {
        if (entry.is_regular_file(ec) && entry.path().extension() == ".smstat") {
            tables.push_back(entry.path());
        }
    }
    if (tables.empty()) {
        return std::nullopt;
    }
    return *std::min_element(tables.begin(), tables.end());
}

} // namespace

std::optional<fs::path> findStaticEmbeddingTable(const std::optional<fs::path>& configured,
                                                 const fs::path& dataDir) {
    std::error_code ec;
    if (configured && !configured->empty() && fs::is_regular_file(*configured, ec)) {
        return *configured;
    }
    if (!dataDir.empty()) {
        if (auto found = firstTableIn(dataDir / "models" / "static")) {
            return found;
        }
    }
    if (auto exe = executablePath()) {
        if (auto found = firstTableIn(exe->parent_path().parent_path() / "share" / "yams" /
                                      "models" / "static")) {
            return found;
        }
    }
#ifdef YAMS_INSTALL_PREFIX
    if (auto found =
            firstTableIn(fs::path(YAMS_INSTALL_PREFIX) / "share" / "yams" / "models" / "static")) {
        return found;
    }
#endif
    return std::nullopt;
}

} // namespace yams::config
