#pragma once

#include <concepts>
#include <span>
#include <ranges>
#include <string>
#include <filesystem>
#include <optional>
#include <vector>
#include <future>

namespace yams {

// Core concepts used throughout the project

// Concept for data that can be converted to byte spans
template<typename T>
concept ByteSpanConvertible = requires(T t) {
    { std::as_bytes(std::span{t}) } -> std::convertible_to<std::span<const std::byte>>;
};

// Concept for hashable data types
template<typename T>
concept HashableData = ByteSpanConvertible<T> || std::is_same_v<T, std::filesystem::path>;

// Concept for hash function implementations
template<typename T>
concept HashFunction = requires(T t, std::span<const std::byte> data) {
    { t.init() } -> std::same_as<void>;
    { t.update(data) } -> std::same_as<void>;
    { t.finalize() } -> std::convertible_to<std::string>;
};

// Concept for byte ranges
template<typename R>
concept ByteRange = std::ranges::input_range<R> && 
                   std::same_as<std::ranges::range_value_t<R>, std::byte>;

// Concept for storable data
template<typename T>
concept StorableData = requires(T t) {
    { std::span{t} } -> std::convertible_to<std::span<const std::byte>>;
};

// Concept for chunk providers
template<typename T>
concept ChunkProvider = requires(T t, const std::string& hash) {
    { t.getChunk(hash) } -> std::convertible_to<std::optional<std::vector<std::byte>>>;
};

// Concept for progress callbacks
template<typename T>
concept ProgressCallback = requires(T t, uint64_t current, uint64_t total) {
    { t(current, total) } -> std::same_as<void>;
};

// Concept for async operations
template<typename T>
concept AsyncOperation = requires(T t) {
    typename T::value_type;
    { t.get() } -> std::same_as<typename T::value_type>;
    { t.wait() } -> std::same_as<void>;
};

} // namespace yams