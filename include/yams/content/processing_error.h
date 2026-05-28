#pragma once

#include <filesystem>
#include <string>
#include <string_view>
#include <utility>

#include <yams/core/format.h>

namespace yams::content {

template <typename... Args>
[[nodiscard]] std::string formatProcessingError(std::string_view kind, std::string_view operation,
                                                const std::filesystem::path& path,
                                                YAMS_FORMAT_NAMESPACE::format_string<Args...> fmt,
                                                Args&&... args) {
    auto details = yams::format(fmt, std::forward<Args>(args)...);
    return yams::format("{} processing failed: {} for '{}' - {}", kind, operation, path.string(),
                        details);
}

} // namespace yams::content
