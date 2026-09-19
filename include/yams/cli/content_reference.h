#pragma once

#include <nlohmann/json.hpp>
#include <algorithm>
#include <string>
#include <string_view>

namespace yams::cli {

// Machine-readable identity is independent of human hash-display preferences.
// References are local to the selected corpus; a hash alone grants no access.
inline void addContentReference(nlohmann::json& item, std::string_view hash) {
    item["hash"] = hash.empty() ? nlohmann::json(nullptr) : nlohmann::json(hash);
    item["hydration"] = nullptr;
    const bool fullHash =
        hash.size() == 64 && std::all_of(hash.begin(), hash.end(), [](char c) {
            return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
        });
    if (fullHash) {
        item["hydration"] = {
            {"method", "cat"}, {"hash", hash}, {"raw", true}, {"scope", "current_corpus"}};
    }
}

} // namespace yams::cli
