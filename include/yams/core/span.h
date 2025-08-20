#pragma once

// Compatibility header for std::span
// Project requires C++20; use std::span unconditionally

#include <span>

namespace yams {

template <typename T, std::size_t Extent = std::dynamic_extent> using span = std::span<T, Extent>;

constexpr std::size_t dynamic_extent = std::dynamic_extent;

template <typename T> constexpr auto as_bytes(span<T> s) noexcept {
    return std::as_bytes(s);
}

template <typename T> constexpr auto as_writable_bytes(span<T> s) noexcept {
    return std::as_writable_bytes(s);
}

} // namespace yams
