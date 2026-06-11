#pragma once

#include <yams/core/types.h>

#include <chrono>
#include <concepts>
#include <cstring>
#include <filesystem>
#include <map>
#include <optional>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <variant>
#include <vector>

namespace yams::daemon {

// Forward declarations for serialization
class BinarySerializer;
class BinaryDeserializer;

// Concepts to constrain serialize/deserialize templates
template <typename T>
concept IsSerializer = requires(T& t, const std::string& str, uint32_t val) {
    t << str;
    t << val;
};

template <typename T>
concept IsDeserializer = requires(T& t) {
    { t.readString() } -> std::same_as<Result<std::string>>;
    { t.template read<uint32_t>() } -> std::same_as<Result<uint32_t>>;
};

namespace ipc_detail {

template <typename Deserializer, typename T>
requires IsDeserializer<Deserializer>
Result<void> readField(Deserializer& deser, T& field) {
    if constexpr (std::is_same_v<std::remove_cvref_t<T>, std::string>) {
        auto value = deser.readString();
        if (!value)
            return value.error();
        field = std::move(value.value());
    } else if constexpr (std::is_same_v<std::remove_cvref_t<T>, std::vector<std::string>>) {
        auto value = deser.readStringVector();
        if (!value)
            return value.error();
        field = std::move(value.value());
    } else if constexpr (std::is_same_v<std::remove_cvref_t<T>,
                                        std::map<std::string, std::string>>) {
        auto value = deser.readStringMap();
        if (!value)
            return value.error();
        field = std::move(value.value());
    } else {
        auto value = deser.template read<std::remove_cvref_t<T>>();
        if (!value)
            return value.error();
        field = value.value();
    }
    return {};
}

template <typename Duration, typename Deserializer>
requires IsDeserializer<Deserializer>
Result<void> readDurationField(Deserializer& deser, Duration& field) {
    auto value = deser.template readDuration<Duration>();
    if (!value)
        return value.error();
    field = value.value();
    return {};
}

template <typename Deserializer, typename T, typename... Rest>
requires IsDeserializer<Deserializer>
Result<void> readFields(Deserializer& deser, T& field, Rest&... rest) {
    auto result = readField(deser, field);
    if (!result)
        return result.error();
    if constexpr (sizeof...(Rest) > 0) {
        return readFields(deser, rest...);
    }
    return {};
}

} // namespace ipc_detail

} // namespace yams::daemon
