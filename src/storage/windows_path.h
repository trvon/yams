#pragma once

#include <string>
#include <string_view>
#include <yams/core/types.h>

namespace yams::storage::detail {

// Input must already be absolute, lexically normalized, and use native separators.
// Keep this spelling step independent of the host's filesystem grammar for unit tests.
inline Result<std::wstring> extendedWindowsPath(std::wstring_view path) {
    if (path.find(L'/') != std::wstring_view::npos) {
        return Error{ErrorCode::InvalidPath, "Windows storage path needs native separators"};
    }
    if (path.starts_with(LR"(\\?\)")) {
        return std::wstring(path);
    }
    const bool driveLetter = !path.empty() && ((path[0] >= L'A' && path[0] <= L'Z') ||
                                               (path[0] >= L'a' && path[0] <= L'z'));
    if (path.size() >= 3 && driveLetter && path[1] == L':' && path[2] == L'\\') {
        return std::wstring(LR"(\\?\)") + std::wstring(path);
    }
    if (path.starts_with(LR"(\\)") && !path.starts_with(LR"(\\.\)")) {
        const auto shareSeparator = path.find(L'\\', 2);
        if (shareSeparator != std::wstring_view::npos && shareSeparator > 2 &&
            shareSeparator + 1 < path.size() && path[shareSeparator + 1] != L'\\') {
            return std::wstring(LR"(\\?\UNC\)") + std::wstring(path.substr(2));
        }
    }
    return Error{ErrorCode::InvalidPath, "Windows storage path must be absolute"};
}

} // namespace yams::storage::detail
