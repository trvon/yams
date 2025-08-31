#pragma once

#include <atomic>
#include <chrono>
#include <iostream>
#include <string>
#include <string_view>

namespace yams::cli {

/**
 * @brief A reusable progress indicator for CLI commands
 *
 * Provides various styles of progress indication including spinners,
 * percentage bars, and progress bars for long-running operations.
 */
class ProgressIndicator {
public:
    enum class Style {
        Spinner,    // ⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏
        Dots,       // . .. ...
        Percentage, // [45%]
        Bar         // [████████░░░░░░░░]
    };

    /**
     * @brief Construct a progress indicator
     * @param style The visual style to use
     * @param autoStart Whether to start immediately
     */
    explicit ProgressIndicator(Style style = Style::Spinner, bool autoStart = false);

    /**
     * @brief Destructor - automatically stops if still active
     */
    ~ProgressIndicator();

    // Disable copy and move (std::atomic cannot be moved)
    ProgressIndicator(const ProgressIndicator&) = delete;
    ProgressIndicator& operator=(const ProgressIndicator&) = delete;
    ProgressIndicator(ProgressIndicator&&) = delete;
    ProgressIndicator& operator=(ProgressIndicator&&) = delete;

    /**
     * @brief Start showing progress with a message
     * @param message The message to display
     */
    void start(const std::string& message);

    /**
     * @brief Update progress
     * @param current Current progress value
     * @param total Total expected value (0 for indeterminate)
     */
    void update(size_t current, size_t total = 0);

    /**
     * @brief Increment progress by one
     */
    void tick();

    /**
     * @brief Stop and clear the indicator
     */
    void stop();

    /**
     * @brief Set whether to show numeric counts
     * @param show True to show counts
     */
    void setShowCount(bool show) { showCount_ = show; }

    /**
     * @brief Check if the indicator is active
     * @return True if currently showing progress
     */
    bool isActive() const { return active_; }

    /**
     * @brief Set update interval in milliseconds
     * @param ms Milliseconds between updates
     */
    void setUpdateInterval(int ms) { updateIntervalMs_ = ms; }

    // Update the displayed message while active
    void setMessage(const std::string& message) {
        message_ = message;
        render();
    }

private:
    void render();

    Style style_;
    std::string message_;
    std::atomic<bool> active_{false};
    size_t current_ = 0;
    size_t total_ = 0;
    int spinnerIndex_ = 0;
    bool showCount_ = true;
    int updateIntervalMs_ = 100;

    std::chrono::steady_clock::time_point lastUpdate_;

    // Spinner characters
    static constexpr const char* SPINNER_CHARS[] = {"⠋", "⠙", "⠹", "⠸", "⠼",
                                                    "⠴", "⠦", "⠧", "⠇", "⠏"};
    static constexpr int SPINNER_COUNT = 10;
};

} // namespace yams::cli