#pragma once

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
                        if (vf2) { std::getline(vf2, vendor); }
                    }
                    if (fs::exists(devicePath)) {
                        std::ifstream df(devicePath);
                        if (df) { std::getline(df, device); }
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
            FILE* pipe = popen("nvidia-smi --query-gpu=memory.total --format=csv,noheader,nounits 2>/dev/null", "r");
            if (pipe) {
                char buf[128];
                if (fgets(buf, sizeof(buf), pipe)) {
                    try {
                        uint64_t mib = std::stoull(buf);
                        if (mib > 0)
                            info.vramBytes = mib * 1024ULL * 1024ULL;
                    } catch (...) {}
                }
                pclose(pipe);
            }
        } catch (...) {}
    }
}
#endif // __linux__

#if defined(__APPLE__)
#include <sys/sysctl.h>
inline void detectMacGpu(GpuInfo& info) {
    // macOS: Use system_profiler to get GPU info
    // We avoid linking Metal framework directly to keep this header-only
    // system_profiler SPDisplaysDataType outputs human-readable text
    // Parse VRAM from lines like: "VRAM (Total):  16 GB"
    try {
        // Use popen for simplicity (runs once at startup)
        FILE* pipe = popen("system_profiler SPDisplaysDataType 2>/dev/null", "r");
        if (!pipe)
            return;

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
            // Find number and unit
            auto numPos = vramLine.find_first_of("0123456789");
            if (numPos != std::string::npos) {
                try {
                    uint64_t val = std::stoull(vramLine.substr(numPos));
                    // Check unit (GB vs MB)
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

        // Apple Silicon unified memory
        if (!info.detected && output.find("Apple") != std::string::npos) {
            info.detected = true;
            info.provider = "coreml";
            if (info.name.empty())
                info.name = "Apple Silicon GPU";

            // Estimate GPU memory budget: ~75% of system RAM
            // (matches Metal's recommendedMaxWorkingSetSize heuristic)
            uint64_t memsize = 0;
            size_t len = sizeof(memsize);
            if (sysctlbyname("hw.memsize", &memsize, &len, nullptr, 0) == 0 && memsize > 0) {
                info.vramBytes = static_cast<uint64_t>(static_cast<double>(memsize) * 0.75);
            }
        }
    } catch (...) {
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

} // namespace yams::daemon::resource
