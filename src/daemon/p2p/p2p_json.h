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

inline Result<Json> readJson(P2pConnection& connection, std::chrono::milliseconds timeout) {
    auto frame = connection.readFrame(timeout);
    if (!frame) {
        return frame.error();
    }
    try {
        const std::string_view text(reinterpret_cast<const char*>(frame.value().data()),
                                    frame.value().size());
        return Json::parse(text);
    } catch (const std::exception& error) {
        return Error{ErrorCode::SerializationError,
                     std::string("invalid p2p JSON: ") + error.what()};
    }
}

} // namespace yams::daemon::p2p::detail
