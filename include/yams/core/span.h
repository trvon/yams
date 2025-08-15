#pragma once

// Compatibility header for std::span
// Uses std::span in C++20 mode, boost::span in C++17 mode

#if __cplusplus >= 202002L && __has_include(<span>)
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
#else
// C++17 fallback using boost::span
#include <cstddef>
#include <type_traits>
#include <boost/core/span.hpp>

namespace yams {
template <typename T, std::size_t Extent = boost::dynamic_extent>
using span = boost::span<T, Extent>;

constexpr std::size_t dynamic_extent = boost::dynamic_extent;

// boost::span doesn't have as_bytes, so we implement it
template <typename T> constexpr auto as_bytes(span<T> s) noexcept {
    return span<const std::byte>{reinterpret_cast<const std::byte*>(s.data()), s.size_bytes()};
}

template <typename T> constexpr auto as_writable_bytes(span<T> s) noexcept {
    static_assert(!std::is_const_v<T>, "Cannot get writable bytes of const span");
    return span<std::byte>{reinterpret_cast<std::byte*>(s.data()), s.size_bytes()};
}
} // namespace yams
#endif