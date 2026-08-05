// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include <string_view>

namespace yams::test {

inline bool isBenchmarkListArgument(std::string_view argument) noexcept {
    const std::string_view googleList{"--benchmark_list_tests"};
    if (argument == googleList || argument == "--list-tests") {
        return true;
    }
    const std::string_view googleListValue{"--benchmark_list_tests="};
    if (argument.size() < googleListValue.size() ||
        argument.substr(0, googleListValue.size()) != googleListValue) {
        return false;
    }
    const auto value = argument.substr(googleListValue.size());
    return value == "1" || value == "true" || value == "yes" || value == "on";
}

inline char* googleBenchmarkListArgumentStorage() noexcept {
    static char argument[] = "--benchmark_list_tests";
    return argument;
}

inline bool normalizeBenchmarkListArguments(int argc, char** argv) noexcept {
    bool listOnly = false;
    for (int index = 1; index < argc; ++index) {
        const std::string_view argument = argv[index] ? argv[index] : "";
        if (!isBenchmarkListArgument(argument)) {
            continue;
        }
        listOnly = true;
        if (argument == "--list-tests") {
            argv[index] = googleBenchmarkListArgumentStorage();
        }
    }
    return listOnly;
}

} // namespace yams::test
