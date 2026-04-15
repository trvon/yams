#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

namespace yams::cli {

class YamsCLI;

struct TuneOptions {
    std::size_t queries = 10;
    std::size_t k = 5;
    std::uint64_t seed = 0;
    bool json = false;
    bool nonInteractive = false;
};

// Runs the interactive relevance-feedback tuning session.
//
// Fail-soft: a missing corpus, non-TTY stdin, search failure, persistence
// failure, or mid-session quit all log a friendly message and return without
// raising. The parent command must never be aborted by this helper.
void runInteractiveTune(YamsCLI* cli, const TuneOptions& opts);

// Prints the current persisted tuner state (observations, reward EWMA, etc.).
// Reads `tuner_state.json` from the data dir; missing file is reported as
// "no tuner state yet" rather than an error.
void runTuneStatus(YamsCLI* cli, bool json);

// Deletes tuner state files after confirmation. `policy` selects which state
// to drop: "rules" (`tuner_state.json`), "contextual" (future), or "all".
// Returns exit-style status (0 = success or user abort, nonzero on I/O error).
int runTuneReset(YamsCLI* cli, const std::string& policy, bool assumeYes);

// Placeholder for the R6 offline replay harness. Prints a friendly
// "not yet implemented" message and returns. Keeps the CLI shape stable so
// downstream tooling (shell completions, docs) doesn't churn when R6 lands.
void runTuneReplayStub();

} // namespace yams::cli
