// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <cstddef>
#include <string>
#include <string_view>
#include <unordered_set>

namespace yams::search {

/**
 * @brief Thread-compatible flyweight string interner.
 *
 * Deduplicates strings by allocating each unique string exactly once into an
 * internal pool. Subsequent intern calls return a non-allocating string_view
 * pointing to the stable internal storage. The returned string_views remain valid
 * for the lifetime of the StringInterner instance.
 */
class StringInterner {
public:
    StringInterner() = default;
    ~StringInterner() = default;

    StringInterner(const StringInterner&) = delete;
    StringInterner& operator=(const StringInterner&) = delete;

    StringInterner(StringInterner&&) noexcept = default;
    StringInterner& operator=(StringInterner&&) noexcept = default;

    /**
     * @brief Intern a string, returning a stable string_view.
     * @param str The string view to intern.
     * @return A string_view backed by internal storage, or an empty view if str is empty.
     */
    std::string_view intern(std::string_view str) {
        if (str.empty()) {
            return {};
        }
        auto it = pool_.find(str);
        if (it != pool_.end()) {
            return *it;
        }
        auto [insertedIt, _] = pool_.emplace(str);
        return *insertedIt;
    }

    /**
     * @brief Number of distinct strings stored in the interner.
     */
    [[nodiscard]] std::size_t size() const noexcept { return pool_.size(); }

    /**
     * @brief Whether the interner has zero entries.
     */
    [[nodiscard]] bool empty() const noexcept { return pool_.empty(); }

    /**
     * @brief Total payload bytes stored across all interned strings.
     */
    [[nodiscard]] std::size_t payloadBytes() const noexcept {
        std::size_t total = 0;
        for (const auto& s : pool_) {
            total += s.size();
        }
        return total;
    }

    /**
     * @brief Clear all interned strings.
     * Invalidates all string_views previously returned by intern().
     */
    void clear() noexcept { pool_.clear(); }

private:
    struct StringHash {
        using is_transparent = void;
        std::size_t operator()(std::string_view sv) const noexcept {
            return std::hash<std::string_view>{}(sv);
        }
    };

    struct StringEqual {
        using is_transparent = void;
        bool operator()(std::string_view a, std::string_view b) const noexcept { return a == b; }
    };

    std::unordered_set<std::string, StringHash, StringEqual> pool_;
};

} // namespace yams::search
