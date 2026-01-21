#pragma once

#include <iostream>
#include <string>

#include <spdlog/spdlog.h>
#include <yams/core/types.h>

namespace yams::cli {

template <typename ResultType, typename Fn>
inline auto withResult(const std::string& operationLabel, Fn&& fn) -> ResultType {
    auto result = fn();
    if (!result) {
        spdlog::error("{} failed: {}", operationLabel, result.error().message);
        std::exit(1);
    }
    return result.value();
}

template <typename Fn> inline bool withResultBool(const std::string& operationLabel, Fn&& fn) {
    auto result = fn();
    if (!result) {
        spdlog::error("{} failed: {}", operationLabel, result.error().message);
        return false;
    }
    return result.value();
}

inline void exitOnError(const Result<void>& result, const std::string& operationLabel) {
    if (!result) {
        spdlog::error("{} failed: {}", operationLabel, result.error().message);
        std::exit(1);
    }
}

template <typename T>
inline void exitOnError(const Result<T>& result, const std::string& operationLabel) {
    if (!result) {
        spdlog::error("{} failed: {}", operationLabel, result.error().message);
        std::exit(1);
    }
}

inline void exitOnError(const yams::Error& err, const std::string& operationLabel) {
    spdlog::error("{} failed: {}", operationLabel, err.message);
    std::exit(1);
}

template <typename T> inline T& exitOnNull(T* ptr, const std::string& operationLabel) {
    if (!ptr) {
        spdlog::error("{} failed: null pointer", operationLabel);
        std::exit(1);
    }
    return *ptr;
}

template <typename T>
inline std::shared_ptr<T>& exitOnNull(const std::shared_ptr<T>& ptr,
                                      const std::string& operationLabel) {
    if (!ptr) {
        spdlog::error("{} failed: null pointer", operationLabel);
        std::exit(1);
    }
    return ptr;
}

inline void returnOnError(const Result<void>& result, const std::string& operationLabel) {
    if (!result) {
        std::cout << operationLabel << " failed: " << result.error().message << "\n";
    }
}

template <typename T>
inline void returnOnError(const Result<T>& result, const std::string& operationLabel) {
    if (!result) {
        std::cout << operationLabel << " failed: " << result.error().message << "\n";
    }
}

template <typename PrintFn, typename ErrorMsgFn>
inline yams::Error returnOnErrorWithStatus(const Result<void>& result, PrintFn printStatus,
                                           ErrorMsgFn errorMsgFn) {
    if (!result) {
        printStatus();
        return Error{ErrorCode::InternalError, errorMsgFn(result.error().message)};
    }
    return Error{};
}

} // namespace yams::cli
