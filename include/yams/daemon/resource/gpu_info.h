#pragma once

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <mutex>
#include <string>

#if defined(__linux__)
#include <filesystem>
#endif

#if defined(_WIN32)
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <dxgi.h>
#include <wrl/client.h>
#endif

namespace yams::daemon::resource {

struct GpuInfo {
    std::string name;      // e.g. "AMD Radeon RX 7900 XTX"
    uint64_t vramBytes{0}; // Total VRAM in bytes
    std::string provider;  // "migraphx", "cuda", "coreml", "directml", ""
    // True when GPU and CPU share a unified memory pool (e.g., Apple Silicon/CoreML).
    // In this mode, vramBytes is a heuristic budget signal, not dedicated VRAM.
    bool unifiedMemory{false};
    bool detected{false};
};

namespace detail {

#if defined(__linux__)
inline void detectLinuxGpu(GpuInfo& info) {
    namespace fs = std::filesystem;

    // AMD: /sys/class/drm/card*/device/mem_info_vram_total
    try {
        if (fs::exists("/sys/class/drm")) {
            for (const auto& entry : fs::directory_iterator("/sys/class/drm")) {
                auto cardName = entry.path().filename().string();
                if (cardName.find("card") != 0)
                    continue;
                // Skip render nodes (card0-render)
                if (cardName.find('-') != std::string::npos)
                    continue;

                fs::path vramPath = entry.path() / "device" / "mem_info_vram_total";
                if (!fs::exists(vramPath))
                    continue;

                std::ifstream vf(vramPath);
                if (!vf)
                    continue;
                uint64_t vram = 0;
                vf >> vram;
                if (vram == 0)
                    continue;

                info.vramBytes = vram;
                info.detected = true;
                info.provider = "migraphx";

                // --- GPU Name: dynamic cascade ---
                // 1) product_name (exists on some AMD cards)
                fs::path namePath = entry.path() / "device" / "product_name";
                if (fs::exists(namePath)) {
                    std::ifstream nf(namePath);
                    if (nf) {
                        std::getline(nf, info.name);
                        while (!info.name.empty() &&
                               std::isspace(static_cast<unsigned char>(info.name.back())))
                            info.name.pop_back();
                    }
                }
                // 2) uevent: DRIVER + PCI_ID
                if (info.name.empty()) {
                    fs::path ueventPath = entry.path() / "device" / "uevent";
                    if (fs::exists(ueventPath)) {
                        std::ifstream uf(ueventPath);
                        std::string line, driver, pciId;
                        while (std::getline(uf, line)) {
                            if (line.rfind("DRIVER=", 0) == 0)
                                driver = line.substr(7);
                            else if (line.rfind("PCI_ID=", 0) == 0)
                                pciId = line.substr(7);
                        }
                        if (!driver.empty() || !pciId.empty()) {
                            info.name = driver.empty() ? "GPU" : driver;
                            if (!pciId.empty())
                                info.name += " [" + pciId + "]";
                        }
                    }
                }
                // 3) vendor + device sysfs files
                if (info.name.empty()) {
                    std::string vendor, device;
                    fs::path vendorPath = entry.path() / "device" / "vendor";
                    fs::path devicePath = entry.path() / "device" / "device";
                    if (fs::exists(vendorPath)) {
                        std::ifstream vf2(vendorPath);
                        if (vf2) {
                            std::getline(vf2, vendor);
                        }
                    }
                    if (fs::exists(devicePath)) {
                        std::ifstream df(devicePath);
                        if (df) {
                            std::getline(df, device);
                        }
                    }
                    if (!vendor.empty() || !device.empty()) {
                        auto strip0x = [](std::string& s) {
                            while (!s.empty() && std::isspace(static_cast<unsigned char>(s.back())))
                                s.pop_back();
                            if (s.size() > 2 && s[0] == '0' && (s[1] == 'x' || s[1] == 'X'))
                                s = s.substr(2);
                        };
                        strip0x(vendor);
                        strip0x(device);
                        info.name = "GPU [" + vendor + ":" + device + "]";
                    }
                }
                // 4) Last resort (vendor-agnostic)
                if (info.name.empty())
                    info.name = "Unknown GPU";
                return;
            }
        }
    } catch (...) {
    }

    // NVIDIA: /proc/driver/nvidia/gpus/*/information
    try {
        if (fs::exists("/proc/driver/nvidia/gpus")) {
            for (const auto& entry : fs::directory_iterator("/proc/driver/nvidia/gpus")) {
                fs::path infoPath = entry.path() / "information";
                if (!fs::exists(infoPath))
                    continue;

                std::ifstream inf(infoPath);
                if (!inf)
                    continue;

                std::string line;
                while (std::getline(inf, line)) {
                    if (line.find("Model:") == 0) {
                        auto pos = line.find_first_not_of(" \t", 6);
                        if (pos != std::string::npos)
                            info.name = line.substr(pos);
                    }
                }
                if (!info.name.empty()) {
                    info.detected = true;
                    info.provider = "cuda";
                    break;
                }
            }
        }
    } catch (...) {
    }

    // NVIDIA VRAM: nvidia-smi (proc information file does NOT contain memory info)
    if (info.detected && info.vramBytes == 0) {
        try {
            FILE* pipe = popen(
                "nvidia-smi --query-gpu=memory.total --format=csv,noheader,nounits 2>/dev/null",
                "r");
            if (pipe) {
                char buf[128];
                if (fgets(buf, sizeof(buf), pipe)) {
                    try {
                        uint64_t mib = std::stoull(buf);
                        if (mib > 0)
                            info.vramBytes = mib * 1024ULL * 1024ULL;
                    } catch (...) {
                    }
                }
                pclose(pipe);
            }
        } catch (...) {
        }
    }
}
#endif // __linux__

#if defined(__APPLE__)
#include <sys/sysctl.h>

// Helper: read a sysctl string value by name. Returns empty on failure.
inline std::string sysctlString(const char* name) {
    size_t len = 0;
    if (sysctlbyname(name, nullptr, &len, nullptr, 0) != 0 || len == 0)
        return {};
    std::string val(len, '\0');
    if (sysctlbyname(name, &val[0], &len, nullptr, 0) != 0)
        return {};
    // Strip trailing NUL (sysctl includes it in len)
    while (!val.empty() && val.back() == '\0')
        val.pop_back();
    return val;
}

// Tier 1: Pure sysctl detection for Apple Silicon (~0ms, no subprocess).
// Returns true if Apple Silicon was detected (caller can skip slower paths).
inline bool detectAppleSiliconGpu(GpuInfo& info) {
    // Check for ARM64 (Apple Silicon)
    int64_t arm64 = 0;
    size_t len = sizeof(arm64);
    if (sysctlbyname("hw.optional.arm64", &arm64, &len, nullptr, 0) != 0 || arm64 == 0)
        return false;

    // Chip name: e.g. "Apple M3 Max"
    info.name = sysctlString("machdep.cpu.brand_string");
    if (info.name.empty())
        info.name = "Apple Silicon GPU";

    // Total system memory → estimate 75% as GPU-accessible (unified memory).
    // Matches Metal's recommendedMaxWorkingSetSize heuristic.
    uint64_t memsize = 0;
    len = sizeof(memsize);
    if (sysctlbyname("hw.memsize", &memsize, &len, nullptr, 0) == 0 && memsize > 0) {
        info.vramBytes = static_cast<uint64_t>(static_cast<double>(memsize) * 0.75);
    }

    info.provider = "coreml";
    info.unifiedMemory = true;
    info.detected = true;
    return true;
}

inline void detectMacGpu(GpuInfo& info) {
    // Tier 1: Apple Silicon via sysctl (instant, works in sandboxed envs)
    if (detectAppleSiliconGpu(info))
        return;

    // Tier 2: Intel Macs — use system_profiler for discrete GPU VRAM
    try {
        FILE* pipe = popen("system_profiler SPDisplaysDataType 2>/dev/null", "r");
        if (pipe) {
            char buf[512];
            std::string output;
            while (fgets(buf, sizeof(buf), pipe)) {
                output += buf;
            }
            pclose(pipe);

            // Parse chipset/model name
            auto chipPos = output.find("Chipset Model:");
            if (chipPos == std::string::npos)
                chipPos = output.find("Chip:");
            if (chipPos != std::string::npos) {
                auto lineEnd = output.find('\n', chipPos);
                auto colonPos = output.find(':', chipPos);
                if (colonPos != std::string::npos && lineEnd != std::string::npos) {
                    auto nameStart = output.find_first_not_of(" \t", colonPos + 1);
                    if (nameStart < lineEnd)
                        info.name = output.substr(nameStart, lineEnd - nameStart);
                }
            }

            // Parse VRAM
            auto vramPos = output.find("VRAM");
            if (vramPos != std::string::npos) {
                auto lineEnd = output.find('\n', vramPos);
                std::string vramLine = output.substr(vramPos, lineEnd - vramPos);
                auto numPos = vramLine.find_first_of("0123456789");
                if (numPos != std::string::npos) {
                    try {
                        uint64_t val = std::stoull(vramLine.substr(numPos));
                        if (vramLine.find("GB") != std::string::npos)
                            val *= 1024ULL * 1024ULL * 1024ULL;
                        else if (vramLine.find("MB") != std::string::npos)
                            val *= 1024ULL * 1024ULL;
                        info.vramBytes = val;
                        info.detected = true;
                        info.provider = "coreml";
                    } catch (...) {
                    }
                }
            }

            if (info.detected)
                return;
        }
    } catch (...) {
    }

    // Tier 3: Intel fallback — estimate from CPU brand when system_profiler fails
    std::string cpuBrand = sysctlString("machdep.cpu.brand_string");
    if (!cpuBrand.empty() && cpuBrand.find("Intel") != std::string::npos) {
        info.name = "Intel Integrated Graphics";
        // Conservative estimate for Intel Iris/UHD shared VRAM
        info.vramBytes = 1536ULL * 1024ULL * 1024ULL; // 1.5 GB
        info.provider = "coreml";
        info.unifiedMemory = true;
        info.detected = true;
    }
}
#endif // __APPLE__

#if defined(_WIN32)
inline void detectWindowsGpu(GpuInfo& info) {
    try {
        Microsoft::WRL::ComPtr<IDXGIFactory1> factory;
        if (FAILED(CreateDXGIFactory1(__uuidof(IDXGIFactory1), &factory)))
            return;

        Microsoft::WRL::ComPtr<IDXGIAdapter1> adapter;
        if (FAILED(factory->EnumAdapters1(0, &adapter)))
            return;

        DXGI_ADAPTER_DESC1 desc{};
        if (FAILED(adapter->GetDesc1(&desc)))
            return;

        // Skip software adapters
        if (desc.Flags & DXGI_ADAPTER_FLAG_SOFTWARE)
            return;

        info.vramBytes = desc.DedicatedVideoMemory;
        info.detected = info.vramBytes > 0;
        info.provider = "directml";

        // Convert wide string name to narrow
        char narrowName[256]{};
        WideCharToMultiByte(CP_UTF8, 0, desc.Description, -1, narrowName, sizeof(narrowName),
                            nullptr, nullptr);
        info.name = narrowName;
    } catch (...) {
    }
}
#endif // _WIN32

} // namespace detail

/// Effective embedding batch budget for GPU-backed inference.
///
/// For dedicated-memory GPUs this returns the detected VRAM budget unchanged.
/// For unified-memory GPUs (CoreML on Apple Silicon), this applies a conservative
/// safety cap to avoid treating shared system RAM as freely available VRAM.
///
/// Environment overrides:
/// - YAMS_GPU_BATCH_BUDGET_MB (global hard override)
/// - YAMS_GPU_UNIFIED_BUDGET_MB (unified-memory override)
/// - YAMS_GPU_UNIFIED_BUDGET_FRACTION (0.01-1.00 fraction of detected budget)
inline uint64_t effectiveGpuBatchBudgetBytes(const GpuInfo& info) {
    constexpr uint64_t kMiB = 1024ULL * 1024ULL;
    constexpr uint64_t kGiB = 1024ULL * kMiB;
    constexpr uint64_t kMinBudgetBytes = 256ULL * kMiB;

    if (!info.detected || info.vramBytes == 0) {
        return 0;
    }

    auto parseMbEnv = [](const char* name, uint64_t& outBytes) -> bool {
        const char* s = std::getenv(name);
        if (!s || !*s) {
            return false;
        }
        try {
            uint64_t mb = std::stoull(s);
            if (mb == 0) {
                return false;
            }
            outBytes = mb * 1024ULL * 1024ULL;
            return true;
        } catch (...) {
            return false;
        }
    };

    auto parseFractionEnv = [](const char* name, double& outFraction) -> bool {
        const char* s = std::getenv(name);
        if (!s || !*s) {
            return false;
        }
        try {
            double v = std::stod(s);
            if (v < 0.01 || v > 1.0) {
                return false;
            }
            outFraction = v;
            return true;
        } catch (...) {
            return false;
        }
    };

    uint64_t budgetBytes = 0;
    if (parseMbEnv("YAMS_GPU_BATCH_BUDGET_MB", budgetBytes)) {
        return std::clamp(budgetBytes, kMinBudgetBytes, info.vramBytes);
    }

    if (!info.unifiedMemory) {
        return info.vramBytes;
    }

    if (parseMbEnv("YAMS_GPU_UNIFIED_BUDGET_MB", budgetBytes)) {
        return std::clamp(budgetBytes, kMinBudgetBytes, info.vramBytes);
    }

    double fraction = 0.0;
    if (parseFractionEnv("YAMS_GPU_UNIFIED_BUDGET_FRACTION", fraction)) {
        budgetBytes = static_cast<uint64_t>(static_cast<long double>(info.vramBytes) * fraction);
        return std::clamp(budgetBytes, kMinBudgetBytes, info.vramBytes);
    }

    // Safety-first CoreML default: cap effective budget to 4 GiB.
    return std::clamp(std::min<uint64_t>(info.vramBytes, 4ULL * kGiB), kMinBudgetBytes,
                      info.vramBytes);
}

/// Cached, thread-safe GPU detection. Runs detection once at first call.
/// Use YAMS_GPU_VRAM_MB environment variable to override on any platform.
inline const GpuInfo& detectGpu() {
    static GpuInfo info;
    static std::once_flag flag;
    std::call_once(flag, []() {
#if defined(__linux__)
        detail::detectLinuxGpu(info);
#elif defined(__APPLE__)
        detail::detectMacGpu(info);
#elif defined(_WIN32)
        detail::detectWindowsGpu(info);
#endif
        // Environment override (works on all platforms as manual fallback)
        if (const char* env = std::getenv("YAMS_GPU_VRAM_MB")) {
            try {
                uint64_t mb = std::stoull(env);
                if (mb > 0) {
                    info.vramBytes = mb * 1024ULL * 1024ULL;
                    info.detected = true;
                    if (info.name.empty())
                        info.name = "GPU (env override)";
                }
            } catch (...) {
            }
        }
    });
    return info;
}

/// Convenience overload using cached detection state.
inline uint64_t effectiveGpuBatchBudgetBytes() {
    return effectiveGpuBatchBudgetBytes(detectGpu());
}

} // namespace yams::daemon::resource
