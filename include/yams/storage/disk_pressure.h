#pragma once

#include <yams/core/types.h>

#include <cmath>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <string_view>
#include <system_error>

namespace yams::storage {

/// Daemon-local filesystem pressure. Remote object-store capacity is intentionally excluded.
enum class DiskPressureLevel : std::uint8_t { Unknown = 0, Normal = 1, Warning = 2, Emergency = 3 };

[[nodiscard]] constexpr std::string_view toString(DiskPressureLevel level) noexcept {
    if (level == DiskPressureLevel::Normal) {
        return "normal";
    }
    if (level == DiskPressureLevel::Warning) {
        return "warning";
    }
    if (level == DiskPressureLevel::Emergency) {
        return "emergency";
    }
    return "unknown";
}

struct DiskPressurePolicy {
    static constexpr std::uint64_t kMiB = 1024ULL * 1024ULL;

    double warningFreePercent{10.0};
    std::uint64_t minimumWriteAdmissionBytes{256ULL * kMiB};
    std::uint64_t emergencyReserveBytes{100ULL * kMiB};

    [[nodiscard]] Result<void> validate() const {
        if (!std::isfinite(warningFreePercent) || warningFreePercent <= 0.0 ||
            warningFreePercent > 100.0) {
            return Error{ErrorCode::InvalidArgument,
                         "storage.disk_pressure.warning_free_percent must be in (0, 100]"};
        }
        const double basisPoints = warningFreePercent * 100.0;
        if (std::abs(basisPoints - std::round(basisPoints)) > 1e-9) {
            return Error{ErrorCode::InvalidArgument,
                         "storage.disk_pressure.warning_free_percent supports at most two "
                         "decimal places"};
        }
        if (emergencyReserveBytes == 0) {
            return Error{ErrorCode::InvalidArgument,
                         "storage.disk_pressure.emergency_reserve_bytes must be positive"};
        }
        if (minimumWriteAdmissionBytes < emergencyReserveBytes) {
            return Error{ErrorCode::InvalidArgument,
                         "storage.disk_pressure.minimum_write_admission_bytes must be at least "
                         "emergency_reserve_bytes"};
        }
        return {};
    }
};

struct DiskSpaceSnapshot {
    std::uint64_t capacityBytes{0};
    std::uint64_t availableBytes{0};
};

struct DiskPressureStatus {
    DiskSpaceSnapshot space;
    // pi-lens-ignore: no-bit-fields
    DiskPressureLevel level{DiskPressureLevel::Unknown};
};

using DiskSpaceProbe =
    std::function<Result<DiskSpaceSnapshot>(const std::filesystem::path& storagePath)>;

[[nodiscard]] inline Result<DiskSpaceSnapshot>
probeDiskSpace(const std::filesystem::path& storagePath) {
    std::error_code error;
    const auto space = std::filesystem::space(storagePath, error);
    if (error) {
        return Error{ErrorCode::IOError,
                     "failed to query storage filesystem space: " + error.message()};
    }
    return DiskSpaceSnapshot{.capacityBytes = space.capacity, .availableBytes = space.available};
}

[[nodiscard]] inline DiskPressureLevel classifyDiskPressure(const DiskSpaceSnapshot& snapshot,
                                                            const DiskPressurePolicy& policy) {
    if (snapshot.capacityBytes == 0) {
        return DiskPressureLevel::Unknown;
    }
    if (snapshot.availableBytes <= policy.emergencyReserveBytes) {
        return DiskPressureLevel::Emergency;
    }
    const long double freePercent = (static_cast<long double>(snapshot.availableBytes) * 100.0L) /
                                    static_cast<long double>(snapshot.capacityBytes);
    return freePercent < static_cast<long double>(policy.warningFreePercent)
               ? DiskPressureLevel::Warning
               : DiskPressureLevel::Normal;
}

[[nodiscard]] inline Result<DiskPressureStatus>
inspectDiskPressure(const std::filesystem::path& storagePath, const DiskPressurePolicy& policy,
                    const DiskSpaceProbe& probe = probeDiskSpace) {
    if (!probe) {
        return Error{ErrorCode::InvalidArgument, "disk-space probe is not configured"};
    }
    if (const auto valid = policy.validate(); !valid) {
        return valid.error();
    }
    auto snapshot = probe(storagePath);
    if (!snapshot) {
        return snapshot.error();
    }
    return DiskPressureStatus{.space = snapshot.value(),
                              .level = classifyDiskPressure(snapshot.value(), policy)};
}

} // namespace yams::storage
