#pragma once

#include <chrono>
#include <cstdint>
#include <ctime>
#include <iomanip>
#include <random>
#include <sstream>
#include <string>

namespace yams::core {

/**
 * Generate a UUID v4 string (xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx).
 * Uses std::random_device + mt19937_64. No external dependencies.
 */
inline std::string generateUUID() {
    static thread_local std::mt19937_64 rng{std::random_device{}()};
    std::uniform_int_distribution<uint64_t> dist;
    uint64_t a = dist(rng);
    uint64_t b = dist(rng);

    // Set version 4 (bits 12-15 of time_hi_and_version)
    a = (a & 0xFFFFFFFFFFFF0FFFull) | 0x0000000000004000ull;
    // Set variant 1 (bits 6-7 of clock_seq_hi_and_reserved)
    b = (b & 0x3FFFFFFFFFFFFFFFull) | 0x8000000000000000ull;

    char buf[37];
    std::snprintf(buf, sizeof(buf), "%08x-%04x-%04x-%04x-%012llx", static_cast<uint32_t>(a >> 32),
                  static_cast<uint16_t>((a >> 16) & 0xFFFF), static_cast<uint16_t>(a & 0xFFFF),
                  static_cast<uint16_t>(b >> 48),
                  static_cast<unsigned long long>(b & 0x0000FFFFFFFFFFFFull));
    return std::string(buf);
}

/**
 * Generate a prefixed ID: prefix-timestamp_ms-random6chars.
 * Similar to blackboard's genId pattern.
 */
inline std::string generateId(const std::string& prefix = "s") {
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::system_clock::now().time_since_epoch())
                  .count();

    static thread_local std::mt19937_64 rng{std::random_device{}()};
    std::uniform_int_distribution<uint32_t> dist(0, 0xFFFFFF);
    uint32_t r = dist(rng);

    char buf[64];
    std::snprintf(buf, sizeof(buf), "%s-%lld-%06x", prefix.c_str(), static_cast<long long>(ms), r);
    return std::string(buf);
}

/**
 * FNV-1a 64-bit short hash, returning lower 32 bits as hex string.
 * Consolidated from mcp_server.cpp, watch_command.cpp, init_command.cpp.
 */
inline std::string shortHash(const std::string& s) {
    std::uint64_t h = 1469598103934665603ull;
    for (unsigned char c : s) {
        h ^= static_cast<std::uint64_t>(c);
        h *= 1099511628211ull;
    }
    std::ostringstream oss;
    oss << std::hex << std::nouppercase << (h & 0xffffffffull);
    return oss.str();
}

/**
 * Generate ISO 8601 timestamp-based snapshot ID with microsecond precision.
 * Format: 2025-10-01T14:30:00.123456Z
 * Consolidated from document_service.cpp, indexing_service.cpp.
 */
inline std::string generateSnapshotId() {
    auto now = std::chrono::system_clock::now();
    auto time_t_now = std::chrono::system_clock::to_time_t(now);
    auto micros =
        std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count() %
        1000000;

    std::tm tm_utc;
#ifdef _WIN32
    gmtime_s(&tm_utc, &time_t_now);
#else
    gmtime_r(&time_t_now, &tm_utc);
#endif

    std::ostringstream oss;
    oss << std::put_time(&tm_utc, "%Y-%m-%dT%H:%M:%S") << '.' << std::setfill('0') << std::setw(6)
        << micros << 'Z';
    return oss.str();
}

} // namespace yams::core
