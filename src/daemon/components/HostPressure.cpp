#include <yams/daemon/components/HostPressure.h>

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <string>
#include <thread>

#if defined(__linux__)
#include <unistd.h>
#endif

#if defined(__APPLE__)
#include <stdlib.h>
#include <sys/sysctl.h>
#include <sys/types.h>
#endif

namespace yams::daemon {

namespace {

std::optional<double> parseLeadingDouble(std::string_view text) noexcept {
    // strtod needs a terminated buffer; the fields parsed here are short numbers.
    char buffer[64];
    const std::size_t n = std::min(text.size(), sizeof(buffer) - 1);
    std::memcpy(buffer, text.data(), n);
    buffer[n] = '\0';
    char* end = nullptr;
    const double value = std::strtod(buffer, &end);
    if (end == buffer || !std::isfinite(value)) {
        return std::nullopt;
    }
    return value;
}

#if defined(__linux__)
/// Read a small procfs file into `out`. procfs reports size 0, so read until EOF.
bool readSmallFile(const char* path, std::string& out, std::size_t maxBytes = 4096) noexcept {
    std::FILE* file = std::fopen(path, "r");
    if (!file) {
        return false;
    }
    char buffer[512];
    out.clear();
    std::size_t n = 0;
    while ((n = std::fread(buffer, 1, sizeof(buffer), file)) > 0) {
        out.append(buffer, n);
        if (out.size() > maxBytes || n < sizeof(buffer)) {
            break;
        }
    }
    std::fclose(file);
    return !out.empty();
}

/// Whole physical disks have a backing device; partitions, dm-crypt/LVM, loop, zram and ZFS
/// zvols do not. Their IO lands on the physical disk, so counting them would double-count.
bool isPhysicalDisk(const std::string& name) {
    if (name.empty() || name.find('/') != std::string::npos) {
        return false;
    }
    const std::string path = "/sys/block/" + name + "/device";
    return ::access(path.c_str(), F_OK) == 0;
}

/// Busiest physical disk since the previous call. The first call measures against boot.
double sampleDiskBusyPct() noexcept {
    static std::mutex mutex;
    static std::vector<DiskIoTicks> previous;
    static std::chrono::steady_clock::time_point previousAt{};
    try {
        std::string buffer;
        if (!readSmallFile("/proc/diskstats", buffer, 256 * 1024)) {
            return -1.0;
        }
        auto current = parseDiskstatsIoTicks(buffer);
        std::erase_if(current, [](const DiskIoTicks& d) { return !isPhysicalDisk(d.name); });
        const auto now = std::chrono::steady_clock::now();

        std::lock_guard lock(mutex);
        std::vector<DiskIoTicks> before = previous;
        std::chrono::milliseconds elapsed{0};
        if (previousAt == std::chrono::steady_clock::time_point{}) {
            std::string uptime;
            if (readSmallFile("/proc/uptime", uptime)) {
                if (auto seconds = parseLeadingDouble(uptime)) {
                    elapsed = std::chrono::milliseconds{static_cast<std::int64_t>(*seconds * 1e3)};
                }
            }
            before = current;
            for (auto& d : before) {
                d.ioTicksMs = 0;
            }
        } else {
            elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - previousAt);
        }
        previous = std::move(current);
        previousAt = now;
        return diskBusyPct(before, previous, elapsed);
    } catch (...) {
        return -1.0;
    }
}
#endif

#if defined(__linux__) || defined(__APPLE__)
double onlineCpus() noexcept {
    const unsigned hc = std::thread::hardware_concurrency();
    return hc > 0 ? static_cast<double>(hc) : 1.0;
}
#endif

} // namespace

std::optional<double> parsePsiSomeAvg10(std::string_view content) noexcept {
    constexpr std::string_view kLinePrefix = "some ";
    constexpr std::string_view kField = "avg10=";
    std::size_t lineStart = 0;
    while (lineStart < content.size()) {
        std::size_t lineEnd = content.find('\n', lineStart);
        if (lineEnd == std::string_view::npos) {
            lineEnd = content.size();
        }
        const auto line = content.substr(lineStart, lineEnd - lineStart);
        if (line.starts_with(kLinePrefix)) {
            const auto field = line.find(kField);
            if (field == std::string_view::npos) {
                return std::nullopt;
            }
            return parseLeadingDouble(line.substr(field + kField.size()));
        }
        lineStart = lineEnd + 1;
    }
    return std::nullopt;
}

std::optional<double> parseLoadAvg1(std::string_view content) noexcept {
    return parseLeadingDouble(content);
}

std::vector<DiskIoTicks> parseDiskstatsIoTicks(std::string_view content) {
    // Fields: major minor name reads rmerged rsectors rms writes wmerged wsectors wms
    //         in_flight io_ticks ... io_ticks is the 13th.
    constexpr std::size_t kNameField = 2;
    constexpr std::size_t kIoTicksField = 12;
    std::vector<DiskIoTicks> devices;
    std::size_t lineStart = 0;
    while (lineStart < content.size()) {
        std::size_t lineEnd = content.find('\n', lineStart);
        if (lineEnd == std::string_view::npos) {
            lineEnd = content.size();
        }
        const auto line = content.substr(lineStart, lineEnd - lineStart);
        lineStart = lineEnd + 1;

        std::string_view name;
        std::optional<std::uint64_t> ticks;
        std::size_t field = 0;
        std::size_t pos = 0;
        bool numeric = true;
        while (pos < line.size() && field <= kIoTicksField) {
            while (pos < line.size() && line[pos] == ' ') {
                ++pos;
            }
            const std::size_t start = pos;
            while (pos < line.size() && line[pos] != ' ') {
                ++pos;
            }
            if (start == pos) {
                break;
            }
            const auto token = line.substr(start, pos - start);
            if (field == kNameField) {
                name = token;
            } else {
                std::uint64_t value = 0;
                for (const char c : token) {
                    if (c < '0' || c > '9') {
                        numeric = false;
                        break;
                    }
                    value = value * 10 + static_cast<std::uint64_t>(c - '0');
                }
                if (!numeric) {
                    break;
                }
                if (field == kIoTicksField) {
                    ticks = value;
                }
            }
            ++field;
        }
        if (numeric && ticks && !name.empty()) {
            devices.push_back(DiskIoTicks{std::string{name}, *ticks});
        }
    }
    return devices;
}

double diskBusyPct(const std::vector<DiskIoTicks>& before, const std::vector<DiskIoTicks>& after,
                   std::chrono::milliseconds elapsed) noexcept {
    if (elapsed.count() <= 0) {
        return -1.0;
    }
    double busiest = -1.0;
    for (const auto& now : after) {
        for (const auto& then : before) {
            if (then.name != now.name || now.ioTicksMs < then.ioTicksMs) {
                continue;
            }
            const double pct = 100.0 * static_cast<double>(now.ioTicksMs - then.ioTicksMs) /
                               static_cast<double>(elapsed.count());
            busiest = std::max(busiest, std::min(pct, 100.0));
            break;
        }
    }
    return busiest;
}

bool hostPressureElevated(const HostPressureSample& sample,
                          const HostPressureThresholds& thresholds,
                          bool currentlyElevated) noexcept {
    const double scale = currentlyElevated ? thresholds.releaseRatio : 1.0;
    const auto above = [scale](double value, double threshold) {
        return value >= 0.0 && threshold > 0.0 && value >= threshold * scale;
    };
    switch (sample.source) {
        case HostPressureSource::Psi:
            return above(sample.cpuSomeAvg10, thresholds.cpuSomePct) ||
                   (above(sample.ioSomeAvg10, thresholds.ioSomePct) &&
                    (sample.ioBusyPct < 0.0 || above(sample.ioBusyPct, thresholds.ioBusyPct))) ||
                   above(sample.memorySomeAvg10, thresholds.memorySomePct);
        case HostPressureSource::LoadAverage:
            return sample.vmPressureElevated || above(sample.loadPerCpu, thresholds.loadPerCpu);
        case HostPressureSource::Unavailable:
            return false;
    }
    return false;
}

HostPressureSample sampleHostPressure() noexcept {
    HostPressureSample sample;
#if defined(__linux__)
    std::string buffer;
    double loadAvg = -1.0;
    if (readSmallFile("/proc/loadavg", buffer)) {
        if (auto value = parseLoadAvg1(buffer)) {
            loadAvg = *value;
        }
    }
    if (loadAvg >= 0.0) {
        sample.loadPerCpu = loadAvg / onlineCpus();
    }

    const auto readPsi = [&buffer](const char* path) -> std::optional<double> {
        if (!readSmallFile(path, buffer)) {
            return std::nullopt;
        }
        return parsePsiSomeAvg10(buffer);
    };
    const auto cpu = readPsi("/proc/pressure/cpu");
    const auto io = readPsi("/proc/pressure/io");
    const auto memory = readPsi("/proc/pressure/memory");
    if (cpu || io || memory) {
        sample.source = HostPressureSource::Psi;
        sample.cpuSomeAvg10 = cpu.value_or(0.0);
        sample.ioSomeAvg10 = io.value_or(0.0);
        sample.memorySomeAvg10 = memory.value_or(0.0);
        sample.ioBusyPct = sampleDiskBusyPct();
    } else if (loadAvg >= 0.0) {
        sample.source = HostPressureSource::LoadAverage;
    }
#elif defined(__APPLE__)
    double loads[1] = {0.0};
    if (::getloadavg(loads, 1) == 1 && loads[0] >= 0.0) {
        sample.source = HostPressureSource::LoadAverage;
        sample.loadPerCpu = loads[0] / onlineCpus();
    }
    // kern.memorystatus_vm_pressure_level: 1 = normal, 2 = warn, 4 = critical.
    int vmLevel = 0;
    std::size_t len = sizeof(vmLevel);
    if (::sysctlbyname("kern.memorystatus_vm_pressure_level", &vmLevel, &len, nullptr, 0) == 0) {
        sample.vmPressureElevated = vmLevel >= 2;
        if (sample.source == HostPressureSource::Unavailable) {
            sample.source = HostPressureSource::LoadAverage;
            sample.loadPerCpu = 0.0;
        }
    }
#endif
    return sample;
}

bool DeferralGate::admit(bool hostBusy, Clock::time_point now,
                         std::chrono::milliseconds maxDeferral) noexcept {
    if (!hostBusy || maxDeferral.count() <= 0) {
        deferredSince_.reset();
        return true;
    }
    if (!deferredSince_) {
        deferredSince_ = now;
        ++deferrals_;
        return false;
    }
    if (now - *deferredSince_ >= maxDeferral) {
        // Starvation guard: let one pass through and start a new wait window.
        ++forcedRuns_;
        deferredSince_ = now;
        return true;
    }
    ++deferrals_;
    return false;
}

} // namespace yams::daemon
