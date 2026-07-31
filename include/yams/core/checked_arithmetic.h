// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <concepts>
#include <limits>
#include <type_traits>
#include <utility>

namespace yams::core {

/**
 * @brief Checks whether an integer addition would overflow.
 *
 * @return true when the result is not representable in T. On overflow, @p result is unchanged.
 */
template <typename T>
requires(std::integral<T> && !std::same_as<std::remove_cv_t<T>, bool>)
[[nodiscard]] constexpr bool addOverflow(T lhs, T rhs, T& result) noexcept {
    if constexpr (std::is_unsigned_v<T>) {
        if (rhs > std::numeric_limits<T>::max() - lhs) {
            return true;
        }
    } else {
        if ((rhs > T{0} && lhs > std::numeric_limits<T>::max() - rhs) ||
            (rhs < T{0} && lhs < std::numeric_limits<T>::min() - rhs)) {
            return true;
        }
    }

    result = static_cast<T>(lhs + rhs);
    return false;
}

/**
 * @brief Checks whether an integer value cannot be represented by another integer type.
 *
 * @return true when @p value is not representable in To. On overflow, @p result is unchanged.
 */
template <typename To, typename From>
requires(std::integral<To> && !std::same_as<std::remove_cv_t<To>, bool> && std::integral<From> &&
         !std::same_as<std::remove_cv_t<From>, bool>)
[[nodiscard]] constexpr bool narrowOverflow(From value, To& result) noexcept {
    if (!std::in_range<To>(value)) {
        return true;
    }

    result = static_cast<To>(value);
    return false;
}

/**
 * @brief Checks whether an integer subtraction would overflow.
 *
 * @return true when the result is not representable in T. On overflow, @p result is unchanged.
 */
template <typename T>
requires(std::integral<T> && !std::same_as<std::remove_cv_t<T>, bool>)
[[nodiscard]] constexpr bool subOverflow(T lhs, T rhs, T& result) noexcept {
    if constexpr (std::is_unsigned_v<T>) {
        if (lhs < rhs) {
            return true;
        }
    } else {
        if ((rhs > T{0} && lhs < std::numeric_limits<T>::min() + rhs) ||
            (rhs < T{0} && lhs > std::numeric_limits<T>::max() + rhs)) {
            return true;
        }
    }

    result = static_cast<T>(lhs - rhs);
    return false;
}

/**
 * @brief Checks whether an integer multiplication would overflow.
 *
 * @return true when the result is not representable in T. On overflow, @p result is unchanged.
 */
template <typename T>
requires(std::integral<T> && !std::same_as<std::remove_cv_t<T>, bool>)
[[nodiscard]] constexpr bool mulOverflow(T lhs, T rhs, T& result) noexcept {
    if constexpr (std::is_unsigned_v<T>) {
        if (lhs != T{0} && rhs > std::numeric_limits<T>::max() / lhs) {
            return true;
        }
    } else {
        if (lhs == T{0} || rhs == T{0}) {
            result = T{0};
            return false;
        }

        constexpr T minValue = std::numeric_limits<T>::min();
        constexpr T maxValue = std::numeric_limits<T>::max();
        if (lhs == T{-1}) {
            if (rhs == minValue) {
                return true;
            }
        } else if (rhs == T{-1}) {
            if (lhs == minValue) {
                return true;
            }
        } else if ((lhs > T{0} && rhs > T{0} && lhs > maxValue / rhs) ||
                   (lhs > T{0} && rhs < T{0} && rhs < minValue / lhs) ||
                   (lhs < T{0} && rhs > T{0} && lhs < minValue / rhs) ||
                   (lhs < T{0} && rhs < T{0} && lhs < maxValue / rhs)) {
            return true;
        }
    }

    result = static_cast<T>(lhs * rhs);
    return false;
}

} // namespace yams::core
