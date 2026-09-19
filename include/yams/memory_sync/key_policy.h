// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors
#pragma once

#include <array>
#include <cctype>
#include <cstddef>
#include <string>
#include <string_view>

#include <yams/core/types.h>

namespace yams::memory_sync {

inline constexpr std::size_t kMaxLogicalKeyBytes = 1024;
inline constexpr std::string_view kUserNamespace = "user";

/// Validate the canonical slash-delimited key consumed by MemorySyncLoop.
/// Typed adapter keys may contain multiple segments; filesystem syntax may not.
inline Result<void> validateLogicalKey(std::string_view key) {
    if (key.empty()) {
        return Error{ErrorCode::InvalidArgument, "memory sync key must not be empty"};
    }
    if (key.size() > kMaxLogicalKeyBytes) {
        return Error{ErrorCode::InvalidArgument, "memory sync key exceeds 1024 bytes"};
    }
    if (key.front() == '/' || key.back() == '/') {
        return Error{ErrorCode::InvalidArgument, "memory sync key must be relative"};
    }
    if (key.size() >= 2 && std::isalpha(static_cast<unsigned char>(key[0])) && key[1] == ':') {
        return Error{ErrorCode::InvalidArgument, "memory sync key must not have a drive root"};
    }

    std::size_t segmentStart = 0;
    for (std::size_t index = 0; index <= key.size(); ++index) {
        if (index < key.size()) {
            const auto ch = static_cast<unsigned char>(key[index]);
            if (ch == '\\' || ch < 0x20 || ch == 0x7f) {
                return Error{ErrorCode::InvalidArgument,
                             "memory sync key contains a forbidden character"};
            }
            if (ch != '/') {
                continue;
            }
        }

        const auto segment = key.substr(segmentStart, index - segmentStart);
        if (segment.empty() || segment == "." || segment == "..") {
            return Error{ErrorCode::InvalidArgument,
                         "memory sync key contains an invalid path segment"};
        }
        segmentStart = index + 1;
    }
    return {};
}

inline bool isReservedLogicalKey(std::string_view key) {
    static constexpr std::array<std::string_view, 10> kReservedNamespaces = {
        "document", "embedding", "topology-node", "topology-edge", "content-blob",
        "index",    "blob",      "checkpoint",    "quarantine",    "tombstone",
    };
    const auto separator = key.find('/');
    const auto first = key.substr(0, separator);
    for (const auto reserved : kReservedNamespaces) {
        if (first == reserved) {
            return true;
        }
    }
    return false;
}

/// Percent-encode one user or adapter identity into an object-store path segment.
inline std::string escapeKeySegment(std::string_view value) {
    static constexpr char kHex[] = "0123456789ABCDEF";
    std::string encoded;
    encoded.reserve(value.size());
    for (const unsigned char ch : value) {
        const bool unreserved = (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') ||
                                (ch >= '0' && ch <= '9') || ch == '-' || ch == '.' || ch == '_' ||
                                ch == '~';
        if (unreserved) {
            encoded.push_back(static_cast<char>(ch));
        } else {
            encoded.push_back('%');
            encoded.push_back(kHex[ch >> 4]);
            encoded.push_back(kHex[ch & 0x0f]);
        }
    }
    return encoded;
}

/// Map an untrusted generic publish/read key into the isolated user namespace.
inline Result<std::string> userLogicalKey(std::string_view key) {
    if (auto valid = validateLogicalKey(key); !valid) {
        return valid.error();
    }
    if (isReservedLogicalKey(key)) {
        return Error{ErrorCode::InvalidArgument,
                     "generic memory sync access cannot address a reserved namespace"};
    }
    return std::string(kUserNamespace) + "/" + escapeKeySegment(key);
}

} // namespace yams::memory_sync
