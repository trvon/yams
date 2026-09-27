#pragma once

// Host pressure signal
// --------------------
// The ResourceGovernor's own metrics (RSS, process CPU) cannot tell whether the *machine* is
// busy. A daemon that only watches itself happily starts a topology rebuild or a repair scan
// while the user is compiling, and both lose. This header samples a cheap host-wide signal:
//
//   Linux: PSI (/proc/pressure/{cpu,io,memory}, "some avg10"), the share of wall time in which
//          at least one task stalled on the resource. Falls back to loadavg / online CPUs when
//          PSI is unavailable (kernels before 4.20, or CONFIG_PSI=n).
//   macOS: loadavg / online CPUs, plus the kernel VM pressure level (kern.memorystatus).
//
// Parsing and classification are pure functions so they can be tested without a busy host.

#include <chrono>
#include <cstdint>
#include <optional>
#include <string_view>

namespace yams::daemon {

/// Where a host pressure sample came from.
enum class HostPressureSource : std::uint8_t {
    Unavailable = 0, // no host signal on this platform (or sampling disabled)
    Psi = 1,         // Linux pressure stall information
    LoadAverage = 2, // 1-minute load average normalized by online CPUs
};

constexpr std::string_view hostPressureSourceName(HostPressureSource source) noexcept {
    switch (source) {
        case HostPressureSource::Psi:
            return "psi";
        case HostPressureSource::LoadAverage:
            return "loadavg";
        case HostPressureSource::Unavailable:
            return "unavailable";
    }
    return "unavailable";
}

/// One host pressure reading. Negative values mean "not measured".
struct HostPressureSample {
    HostPressureSource source{HostPressureSource::Unavailable};
    double cpuSomeAvg10{-1.0};      // PSI cpu "some avg10", percent of wall time
    double ioSomeAvg10{-1.0};       // PSI io "some avg10", percent of wall time
    double memorySomeAvg10{-1.0};   // PSI memory "some avg10", percent of wall time
    double loadPerCpu{-1.0};        // 1-minute load average / online CPUs
    bool vmPressureElevated{false}; // macOS kernel VM pressure at warn or critical
};

/// Thresholds above which the host counts as busy. Resolved from [tuning.resource].
struct HostPressureThresholds {
    double cpuSomePct{40.0};
    double ioSomePct{30.0};
    double memorySomePct{10.0};
    double loadPerCpu{1.5};
    /// Fraction of each threshold the signal must drop below before the host counts as calm
    /// again. Keeps work from flapping on and off around a threshold.
    double releaseRatio{0.75};
};

/// Parse the "some avg10=" value from a /proc/pressure/<resource> file body.
std::optional<double> parsePsiSomeAvg10(std::string_view content) noexcept;

/// Parse the 1-minute load average from a /proc/loadavg file body.
std::optional<double> parseLoadAvg1(std::string_view content) noexcept;

/// Whether a sample is above the thresholds. `currentlyElevated` selects the release band:
/// once elevated, every signal must fall below threshold * releaseRatio to clear.
bool hostPressureElevated(const HostPressureSample& sample,
                          const HostPressureThresholds& thresholds,
                          bool currentlyElevated) noexcept;

/// Read the host signal for this platform. Cheap (a few small file reads or sysctls); callers
/// still rate-limit it because PSI avg10 only changes every two seconds.
HostPressureSample sampleHostPressure() noexcept;

} // namespace yams::daemon
