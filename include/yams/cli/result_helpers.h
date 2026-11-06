#pragma once

#include <filesystem>
#include <iostream>
#include <string>

#include <CLI/CLI.hpp>
#include <spdlog/spdlog.h>
#include <yams/cli/error_hints.h>
#include <yams/config/config_helpers.h>
#include <yams/core/types.h>

namespace yams::cli {

// Failures throw CLI::RuntimeError rather than calling std::exit: YamsCLI::run()
// turns it into the exit code, and in-process callers keep running.

/// Exit code for a failed command: 2 for invalid usage or arguments, 1 otherwise.
inline int cliExitCode(ErrorCode code) {
    return code == ErrorCode::InvalidArgument ? 2 : 1;
}

/// Print `error` in the shared CLI format and end the command with its exit code.
[[noreturn]] inline void failCommand(const yams::Error& error, std::string_view context = {}) {
    const std::string message =
        context.empty() ? error.message : std::string(context) + ": " + error.message;
    std::cerr << formatErrorWithHint(error.code, message) << '\n';
    throw CLI::RuntimeError(cliExitCode(error.code));
}

template <typename ResultType, typename Fn>
inline auto withResult(const std::string& operationLabel, Fn&& fn) -> ResultType {
    auto result = fn();
    if (!result) {
        spdlog::error("{} failed: {}", operationLabel, result.error().message);
        throw CLI::RuntimeError(1);
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
    if (!result)
        failCommand(result.error(), operationLabel);
}

template <typename T>
inline void exitOnError(const Result<T>& result, const std::string& operationLabel) {
    if (!result)
        failCommand(result.error(), operationLabel);
}

inline void exitOnError(const yams::Error& err, const std::string& operationLabel) {
    failCommand(err, operationLabel);
}

template <typename T> inline T& exitOnNull(T* ptr, const std::string& operationLabel) {
    if (!ptr) {
        spdlog::error("{} failed: null pointer", operationLabel);
        throw CLI::RuntimeError(1);
    }
    return *ptr;
}

template <typename T>
inline std::shared_ptr<T>& exitOnNull(const std::shared_ptr<T>& ptr,
                                      const std::string& operationLabel) {
    if (!ptr) {
        spdlog::error("{} failed: null pointer", operationLabel);
        throw CLI::RuntimeError(1);
    }
    return ptr;
}

inline Result<void> writeConfigValueResult(const std::filesystem::path& configPath,
                                           const std::string& key, const std::string& value) {
    if (yams::config::write_config_value(configPath, key, value)) {
        return Result<void>();
    }
    return Error{ErrorCode::WriteError, "Failed to write config key: " + key};
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
