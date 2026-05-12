#pragma once

#include <chrono>
#include <ostream>
#include <string>

namespace yams::cli::doctor {

/// Replaces the duplicate `renderEmbeddingProgress` + `finishEmbeddingProgress`
/// lambdas in runRepair. Holds references to captured variables.
class EmbeddingProgressRenderer {
public:
    EmbeddingProgressRenderer(std::ostream& os, bool tty,
                              std::chrono::steady_clock::time_point& lastPrint,
                              const std::chrono::steady_clock::time_point& progressStarted)
        : os_(os), tty_(tty), lastPrint_(lastPrint), progressStarted_(progressStarted) {}

    void render(size_t current, size_t total, const std::string& details);
    void finish();

private:
    std::ostream& os_;
    bool tty_;
    std::chrono::steady_clock::time_point& lastPrint_;
    const std::chrono::steady_clock::time_point& progressStarted_;
};

} // namespace yams::cli::doctor
