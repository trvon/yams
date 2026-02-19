#pragma once

#include <optional>
#include <string>
#include <string_view>

namespace yams::daemon {

// Stable classification for daemon IPC (AF_UNIX) failures.
//
// Note: We currently carry this classification by embedding a prefix in Error.message so we can
// propagate it through existing Error/Result APIs without widening core types.
enum class IpcFailureKind {
    SocketMissing,
    PathNotSocket,
    Refused,
    Timeout,
    ResetOrBrokenPipe,
    Eof,
    Cancelled,
    Other
};

inline constexpr std::string_view kIpcFailurePrefix = "[ipc:";

inline constexpr std::string_view to_string(IpcFailureKind k) {
    switch (k) {
        case IpcFailureKind::SocketMissing:
            return "socket_missing";
        case IpcFailureKind::PathNotSocket:
            return "path_not_socket";
        case IpcFailureKind::Refused:
            return "refused";
        case IpcFailureKind::Timeout:
            return "timeout";
        case IpcFailureKind::ResetOrBrokenPipe:
            return "reset_or_broken_pipe";
        case IpcFailureKind::Eof:
            return "eof";
        case IpcFailureKind::Cancelled:
            return "cancelled";
        case IpcFailureKind::Other:
            return "other";
    }
    return "other";
}

inline std::string formatIpcFailure(IpcFailureKind kind, std::string_view detail) {
    std::string out;
    out.reserve(kIpcFailurePrefix.size() + 32 + 2 + detail.size());
    out.append(kIpcFailurePrefix);
    out.append(to_string(kind));
    out.push_back(']');
    out.push_back(' ');
    out.append(detail);
    return out;
}

inline std::optional<IpcFailureKind> parseIpcFailureKind(std::string_view message) {
    if (!message.starts_with(kIpcFailurePrefix)) {
        return std::nullopt;
    }
    auto close = message.find(']');
    if (close == std::string_view::npos) {
        return std::nullopt;
    }
    // message looks like: [ipc:<kind>] ...
    auto kindStart = kIpcFailurePrefix.size();
    if (close <= kindStart) {
        return std::nullopt;
    }
    auto kind = message.substr(kindStart, close - kindStart);
    if (kind == "socket_missing")
        return IpcFailureKind::SocketMissing;
    if (kind == "path_not_socket")
        return IpcFailureKind::PathNotSocket;
    if (kind == "refused")
        return IpcFailureKind::Refused;
    if (kind == "timeout")
        return IpcFailureKind::Timeout;
    if (kind == "reset_or_broken_pipe")
        return IpcFailureKind::ResetOrBrokenPipe;
    if (kind == "eof")
        return IpcFailureKind::Eof;
    if (kind == "cancelled")
        return IpcFailureKind::Cancelled;
    if (kind == "other")
        return IpcFailureKind::Other;
    return std::nullopt;
}

inline bool isTransient(IpcFailureKind kind) {
    switch (kind) {
        case IpcFailureKind::Timeout:
        case IpcFailureKind::ResetOrBrokenPipe:
        case IpcFailureKind::Eof:
        case IpcFailureKind::Cancelled:
            return true;
        case IpcFailureKind::SocketMissing:
        case IpcFailureKind::PathNotSocket:
        case IpcFailureKind::Refused:
        case IpcFailureKind::Other:
            return false;
    }
    return false;
}

} // namespace yams::daemon
