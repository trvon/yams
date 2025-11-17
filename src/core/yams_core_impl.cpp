// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

// Module implementation for yams.core
// This provides implementations for functions declared in the module interface

module yams.core;

#include <algorithm>
#include <cctype>
#include <iomanip>
#include <sstream>

namespace yams::core {

// ============================================================================
// Hash Implementation
// ============================================================================

std::string Hash::to_hex() const {
    std::ostringstream oss;
    oss << std::hex << std::setfill('0');
    for (auto byte : data) {
        oss << std::setw(2) << static_cast<unsigned>(byte);
    }
    return oss.str();
}

tl::expected<Hash, std::string> Hash::from_hex(std::string_view hex) {
    if (hex.length() != 64) {
        return tl::unexpected("Invalid hash length: expected 64 hex chars");
    }

    Hash result;
    for (std::size_t i = 0; i < 32; ++i) {
        auto high = hex[i * 2];
        auto low = hex[i * 2 + 1];

        if (!std::isxdigit(high) || !std::isxdigit(low)) {
            return tl::unexpected("Invalid hex character in hash");
        }

        auto hex_to_int = [](char c) -> uint8_t {
            if (c >= '0' && c <= '9')
                return c - '0';
            if (c >= 'a' && c <= 'f')
                return c - 'a' + 10;
            if (c >= 'A' && c <= 'F')
                return c - 'A' + 10;
            return 0;
        };

        result.data[i] = (hex_to_int(high) << 4) | hex_to_int(low);
    }

    return result;
}

// ============================================================================
// Error Code Strings
// ============================================================================

const char* error_code_string(ErrorCode code) noexcept {
    switch (code) {
        case ErrorCode::Success:
            return "Success";
        case ErrorCode::NotFound:
            return "Not found";
        case ErrorCode::AlreadyExists:
            return "Already exists";
        case ErrorCode::InvalidArgument:
            return "Invalid argument";
        case ErrorCode::IoError:
            return "I/O error";
        case ErrorCode::CorruptData:
            return "Corrupt data";
        case ErrorCode::OutOfMemory:
            return "Out of memory";
        case ErrorCode::PermissionDenied:
            return "Permission denied";
        case ErrorCode::NetworkError:
            return "Network error";
        case ErrorCode::Timeout:
            return "Timeout";
        case ErrorCode::InternalError:
            return "Internal error";
        default:
            return "Unknown error";
    }
}

} // namespace yams::core
