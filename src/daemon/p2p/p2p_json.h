// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors
#pragma once

// pi-lens-ignore: fatal error
#include <nlohmann/json.hpp>

// pi-lens-ignore: fatal error
#include <yams/daemon/p2p/p2p_transport.h>

#include <cstring>
#include <string_view>

namespace yams::daemon::p2p::detail {

using Json = nlohmann::json;

inline Result<void> writeJson(P2pConnection& connection, const Json& json,
                              std::chrono::milliseconds timeout) {
    try {
        const std::string text = json.dump();
        return connection.writeFrame(
            std::span<const std::byte>(reinterpret_cast<const std::byte*>(text.data()),
                                       text.size()),
            timeout);
    } catch (const std::exception& error) {
        return Error{ErrorCode::SerializationError, error.what()};
    }
}

/// Control frames are structurally flat (<= 5 nesting levels). Anything deeper is a
/// pre-authentication stack-exhaustion probe and is rejected before nlohmann descends it.
inline constexpr std::size_t kMaxP2pJsonNestingDepth = 32;

inline bool exceedsJsonNestingDepth(std::string_view text, std::size_t maxDepth) {
    std::size_t depth = 0;
    bool inString = false;
    bool escaped = false;
    for (const char c : text) {
        if (inString) {
            if (escaped) {
                escaped = false;
            } else if (c == '\\') {
                escaped = true;
            } else if (c == '"') {
                inString = false;
            }
            continue;
        }
        if (c == '"') {
            inString = true;
        } else if (c == '[' || c == '{') {
            if (++depth > maxDepth) {
                return true;
            }
        } else if (c == ']' || c == '}') {
            if (depth > 0) {
                --depth;
            }
        }
    }
    return false;
}

inline Result<Json> parseJsonFrame(std::span<const std::byte> frame) {
    try {
        const std::string_view text(reinterpret_cast<const char*>(frame.data()), frame.size());
        if (exceedsJsonNestingDepth(text, kMaxP2pJsonNestingDepth)) {
            return Error{ErrorCode::ValidationError, "p2p JSON exceeds maximum nesting depth"};
        }
        return Json::parse(text);
    } catch (const std::exception& error) {
        return Error{ErrorCode::SerializationError,
                     std::string("invalid p2p JSON: ") + error.what()};
    }
}

struct JsonFrame {
    Json json;
    std::size_t payloadBytes{0};
};

/// Read and parse one control frame, retaining the exact wire payload size so callers can
/// account for bytes actually received rather than a compact re-serialization.
inline Result<JsonFrame> readJsonFrame(P2pConnection& connection,
                                       std::chrono::milliseconds timeout) {
    auto frame = connection.readFrame(timeout);
    if (!frame) {
        return frame.error();
    }
    auto parsed = parseJsonFrame(frame.value());
    if (!parsed) {
        return parsed.error();
    }
    return JsonFrame{std::move(parsed.value()), frame.value().size()};
}

inline Result<Json> readJson(P2pConnection& connection, std::chrono::milliseconds timeout) {
    auto frame = connection.readFrame(timeout);
    if (!frame) {
        return frame.error();
    }
    return parseJsonFrame(frame.value());
}

} // namespace yams::daemon::p2p::detail
