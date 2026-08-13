// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors
#pragma once

#include <cstddef>
#include <fstream>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include <nlohmann/json.hpp>

#include <yams/core/types.h>
#include <yams/memory_sync/memory_sync_service.h>

namespace yams::memory_sync {

/// CLI-facing helpers for the memory-sync service. Each returns the text to
/// print (raw value, or JSON when `json` is set) so they are trivially testable
/// without a full CLI harness.

/// Publish `valueOrFile` under `key`. A value starting with '@' is read from
/// the named file (binary-safe); otherwise the literal string is published.
inline Result<std::string> publishValue(MemorySyncService& svc, std::string_view key,
                                        std::string_view valueOrFile, bool json) {
    std::vector<std::byte> content;
    if (!valueOrFile.empty() && valueOrFile.front() == '@') {
        std::ifstream in{std::string(valueOrFile.substr(1)), std::ios::binary};
        if (!in) {
            return Error{ErrorCode::IOError,
                         std::string("cannot open file: ") + std::string(valueOrFile.substr(1))};
        }
        std::ostringstream oss;
        oss << in.rdbuf();
        const auto bytes = oss.str();
        content.assign(reinterpret_cast<const std::byte*>(bytes.data()),
                       reinterpret_cast<const std::byte*>(bytes.data()) + bytes.size());
    } else if (!valueOrFile.empty()) {
        content.assign(reinterpret_cast<const std::byte*>(valueOrFile.data()),
                       reinterpret_cast<const std::byte*>(valueOrFile.data()) + valueOrFile.size());
    }
    // Empty valueOrFile publishes an empty blob (valid).

    if (auto r = svc.publish(key, content); !r) {
        return r.error();
    }

    if (json) {
        nlohmann::json j;
        j["key"] = key;
        j["published"] = true;
        j["size"] = content.size();
        try {
            return j.dump(2);
        } catch (const std::exception& e) {
            return Error{ErrorCode::SerializationError, e.what()};
        }
    }
    return std::string("published ") + std::string(key) + " (" + std::to_string(content.size()) +
           " bytes)";
}

/// Read the merged value for `key`. Returns the raw bytes (non-JSON) or a JSON
/// object with key/size/value_hex (JSON, hex-encoded so arbitrary bytes are safe).
inline Result<std::string> readValue(MemorySyncService& svc, std::string_view key, bool json) {
    auto r = svc.read(key);
    if (!r) {
        return r.error();
    }
    const auto& data = r.value();
    if (json) {
        static constexpr char kHex[] = "0123456789abcdef";
        std::string hex;
        hex.reserve(data.size() * 2);
        for (const auto b : data) {
            const auto v = static_cast<unsigned char>(b);
            hex.push_back(kHex[v >> 4]);
            hex.push_back(kHex[v & 0xf]);
        }
        nlohmann::json j;
        j["key"] = key;
        j["size"] = data.size();
        j["value_hex"] = hex;
        try {
            return j.dump(2);
        } catch (const std::exception& e) {
            return Error{ErrorCode::SerializationError, e.what()};
        }
    }
    if (data.empty()) {
        return std::string{};
    }
    return std::string(reinterpret_cast<const char*>(data.data()), data.size());
}

/// Merged index summary + backend identity.
inline Result<std::string> statusSummary(MemorySyncService& svc, std::string_view backend,
                                         std::string_view nodeId, bool json) {
    auto index = svc.syncOnce();
    if (!index) {
        return index.error();
    }
    const std::size_t count = index.value().size();
    if (json) {
        nlohmann::json j;
        j["backend"] = backend;
        j["node_id"] = nodeId;
        j["records"] = count;
        try {
            return j.dump(2);
        } catch (const std::exception& e) {
            return Error{ErrorCode::SerializationError, e.what()};
        }
    }
    std::ostringstream oss;
    oss << "backend=" << backend << " node_id=" << nodeId << " records=" << count;
    return oss.str();
}

} // namespace yams::memory_sync
