// TuneAdvisor platform-specific implementation
// Most of TuneAdvisor is header-only, but detectSystemMemory() requires
// platform headers that would pollute the public interface.

#include <yams/daemon/components/TuneAdvisor.h>

#include <cstdint>
#include <cstdio>
#include <cstring>

#if defined(_WIN32)
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#elif defined(__APPLE__)
#include <sys/sysctl.h>
#include <sys/types.h>
#endif

namespace yams::daemon {

uint64_t TuneAdvisor::detectSystemMemory() {
    constexpr uint64_t kFallback = 4ull * 1024ull * 1024ull * 1024ull; // 4 GiB

#if defined(_WIN32)
    MEMORYSTATUSEX memInfo{};
    memInfo.dwLength = sizeof(MEMORYSTATUSEX);
    if (GlobalMemoryStatusEx(&memInfo)) {
        return memInfo.ullTotalPhys;
    }
    return kFallback;

#elif defined(__APPLE__)
    uint64_t memSize = 0;
    size_t len = sizeof(memSize);
    // Use sysctlbyname which is more portable than mib array
    if (sysctlbyname("hw.memsize", &memSize, &len, nullptr, 0) == 0) {
        return memSize;
    }
    return kFallback;

#else
    // Linux: read /proc/meminfo
    std::FILE* f = std::fopen("/proc/meminfo", "r");
    if (f) {
        char line[256];
        while (std::fgets(line, sizeof(line), f)) {
            if (std::strncmp(line, "MemTotal:", 9) == 0) {
                unsigned long kb = 0;
                if (std::sscanf(line + 9, " %lu", &kb) == 1) {
                    std::fclose(f);
                    return static_cast<uint64_t>(kb) * 1024ull;
                }
            }
        }
        std::fclose(f);
    }
    return kFallback;
#endif
}

} // namespace yams::daemon
