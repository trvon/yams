#include <yams/daemon/components/HostPressure.h>

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>

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
bool readSmallFile(const char* path, std::string& out) noexcept {
    std::FILE* file = std::fopen(path, "r");
    if (!file) {
        return false;
    }
    char buffer[512];
    out.clear();
    std::size_t n = 0;
    while ((n = std::fread(buffer, 1, sizeof(buffer), file)) > 0) {
        out.append(buffer, n);
        if (out.size() > 4096) {
            break;
        }
    }
    std::fclose(file);
    return !out.empty();
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
                   above(sample.ioSomeAvg10, thresholds.ioSomePct) ||
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
