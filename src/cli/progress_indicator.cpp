#include <iomanip>
#include <sstream>
#include <yams/cli/progress_indicator.h>

namespace yams::cli {

// Define static members
constexpr const char* ProgressIndicator::SPINNER_CHARS[];
constexpr int ProgressIndicator::SPINNER_COUNT;

namespace {
#ifndef _WIN32
#include <unistd.h>
#endif
#include <cstdlib>

bool detect_tty() {
#ifndef _WIN32
    return isatty(STDOUT_FILENO);
#else
    return true;
#endif
}

bool detect_unicode() {
    const char* lc = std::getenv("LC_ALL");
    if (!lc || !*lc)
        lc = std::getenv("LC_CTYPE");
    if (!lc || !*lc)
        lc = std::getenv("LANG");
    if (!lc)
        return false;
    std::string_view v{lc};
    return v.find("UTF-8") != std::string_view::npos || v.find("utf8") != std::string_view::npos ||
           v.find("utf-8") != std::string_view::npos;
}
} // namespace

ProgressIndicator::ProgressIndicator(Style style, bool autoStart) : style_(style) {
    // Nothing else
    if (autoStart) {
        start("");
    }
}

ProgressIndicator::~ProgressIndicator() {
    if (active_) {
        stop();
    }
}

void ProgressIndicator::start(const std::string& message) {
    if (active_)
        return;

    message_ = message;
    active_ = true;
    current_ = 0;
    spinnerIndex_ = 0;
    lastUpdate_ = std::chrono::steady_clock::now();

    render();
}

void ProgressIndicator::update(size_t current, size_t total) {
    if (!active_)
        return;

    current_ = current;
    total_ = total;

    auto now = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - lastUpdate_).count();

    if (elapsed >= updateIntervalMs_) {
        spinnerIndex_ = (spinnerIndex_ + 1) % SPINNER_COUNT;
        lastUpdate_ = now;
        render();
    }
}

void ProgressIndicator::tick() {
    update(current_ + 1, total_);
}

void ProgressIndicator::stop() {
    if (!active_)
        return;

    // Clear the line
    std::cout << "\r\033[K" << std::flush;
    active_ = false;
}

void ProgressIndicator::render() {
    if (!active_)
        return;

    std::ostringstream oss;
    oss << "\r";

    const bool isTty = detect_tty();
    const bool unicodeOk = detect_unicode();
    switch (style_) {
        case Style::Spinner:
            if (!isTty) {
                // Non-TTY: simple dots progression
                oss << message_;
                int dots = (spinnerIndex_ % 4) + 1;
                for (int i = 0; i < dots; ++i)
                    oss << ".";
            } else if (!unicodeOk) {
                // ASCII spinner fallback
                static const char* ascii[] = {"-", "\\", "|", "/"};
                static constexpr int ac = 4;
                oss << ascii[spinnerIndex_ % ac] << " " << message_;
            } else {
                oss << SPINNER_CHARS[spinnerIndex_] << " " << message_;
            }
            if (showCount_ && current_ > 0) {
                oss << " (" << current_;
                if (total_ > 0) {
                    oss << "/" << total_;
                }
                oss << ")";
            }
            break;

        case Style::Percentage:
            if (total_ > 0) {
                int percent = static_cast<int>((current_ * 100) / total_);
                oss << "[" << std::setw(3) << percent << "%] " << message_;
                if (showCount_) {
                    oss << " (" << current_ << "/" << total_ << ")";
                }
            } else {
                // Fall back to spinner for indeterminate progress
                oss << SPINNER_CHARS[spinnerIndex_] << " " << message_;
                if (showCount_ && current_ > 0) {
                    oss << " (" << current_ << ")";
                }
            }
            break;

        case Style::Bar:
            if (total_ > 0) {
                const int barWidth = 20;
                int filled = static_cast<int>((current_ * barWidth) / total_);
                oss << "[";
                for (int i = 0; i < barWidth; ++i) {
                    if (!unicodeOk) {
                        oss << (i < filled ? "#" : "-");
                    } else {
                        oss << (i < filled ? "█" : "░");
                    }
                }
                oss << "] ";

                // Add percentage
                int percent = static_cast<int>((current_ * 100) / total_);
                oss << std::setw(3) << percent << "% " << message_;
            } else {
                // Fall back to spinner for indeterminate progress
                oss << SPINNER_CHARS[spinnerIndex_] << " " << message_;
                if (showCount_ && current_ > 0) {
                    oss << " (" << current_ << ")";
                }
            }
            break;

        case Style::Dots:
            oss << message_;
            for (int i = 0; i < ((spinnerIndex_ % 4) + 1); ++i) {
                oss << ".";
            }
            if (showCount_ && current_ > 0) {
                oss << " (" << current_;
                if (total_ > 0) {
                    oss << "/" << total_;
                }
                oss << ")";
            }
            break;
    }

    std::cout << oss.str() << std::flush;
}

// no extra methods

} // namespace yams::cli