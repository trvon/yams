#pragma once

#include <CLI/CLI.hpp>
#include <yams/cli/output_controls.h>

namespace yams::cli {
inline void registerContentSliceOptions(CLI::App& command, ContentSliceOptions& options) {
    auto* lines =
        command
            .add_option("--lines", options.lines, "Inclusive numbered line range A:B (text output)")
            ->check(CLI::Validator(
                [](const std::string& value) {
                    try {
                        ContentSliceOptions check;
                        check.lines = value;
                        (void)sliceContent({}, check);
                        return std::string{};
                    } catch (const std::invalid_argument& e) {
                        return std::string(e.what());
                    }
                },
                "A:B"));
    auto* around = command.add_option("--around", options.around,
                                      "Show the first literal pattern match with numbered context");
    lines->excludes(around);
    around->excludes(lines);
    command
        .add_option("--context", options.context,
                    "Lines on each side of --around's first match (default: 2)")
        ->check(CLI::NonNegativeNumber);
}
} // namespace yams::cli
