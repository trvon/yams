#ifndef YAMS_CLI_HOT_COLD_UTILS_H
#define YAMS_CLI_HOT_COLD_UTILS_H

#include <spdlog/spdlog.h>
#include <optional>
#include <string>

namespace yams::cli {

enum class HotColdMode { HotOnly, ColdOnly, Auto };

inline std::optional<HotColdMode> getEnvMode(const std::string& envVar) {
    const char* val = std::getenv(envVar.c_str());
    if (!val)
        return std::nullopt;

    std::string mode = val;
    // Normalize to lowercase
    for (char& c : mode) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }

    if (mode == "hot_only" || mode == "hot") {
        return HotColdMode::HotOnly;
    } else if (mode == "cold_only" || mode == "cold") {
        return HotColdMode::ColdOnly;
    } else if (mode == "auto") {
        return HotColdMode::Auto;
    }
    // Invalid - log warning
    spdlog::warn("Invalid {} value '{}', defaulting to 'auto'", envVar, val);
    return std::nullopt;
}

inline std::string modeToString(HotColdMode mode) {
    switch (mode) {
        case HotColdMode::HotOnly:
            return "hot_only";
        case HotColdMode::ColdOnly:
            return "cold_only";
        case HotColdMode::Auto:
            return "auto";
        default:
            return "unknown";
    }
}

inline HotColdMode getListMode() {
    if (auto mode = getEnvMode("YAMS_LIST_MODE"))
        return *mode;
    return HotColdMode::Auto;
}

inline HotColdMode getGrepMode() {
    if (auto mode = getEnvMode("YAMS_GREP_MODE"))
        return *mode;
    return HotColdMode::Auto;
}

inline HotColdMode getRetrievalMode() {
    if (auto mode = getEnvMode("YAMS_RETRIEVAL_MODE"))
        return *mode;
    return HotColdMode::Auto;
}

// Helper to get boolean "force hot" from mode (for backward compatibility)
inline bool isForceHot(HotColdMode mode) {
    return mode == HotColdMode::HotOnly;
}

} // namespace yams::cli

#endif // YAMS_CLI_HOT_COLD_UTILS_H