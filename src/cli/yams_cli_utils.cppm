// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

/**
 * @file yams_cli_utils.cppm
 * @brief C++20 module interface for YAMS CLI utilities
 *
 * This module exports common CLI utilities that have minimal dependencies:
 * - TimeParser: Parse and format time strings
 * - ProgressIndicator: Visual progress feedback
 * - Prompt utilities: Interactive user prompting
 * - Session store: Session state management
 *
 * Part of PBI-082: C++20 Modules Migration
 */

module;

// Global module fragment - include non-modularized dependencies
// These headers are needed but cannot be imported as modules

// Standard library headers
#include <atomic>
#include <chrono>
#include <filesystem>
#include <functional>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

// YAMS core types (until yams.core is fully modularized)
#include <yams/core/types.h>

export module yams.cli.utils;

// ============================================================================
// TimeParser - Time string parsing utilities
// ============================================================================

export namespace yams::cli {

/**
 * @brief Utility for parsing time strings in various formats
 */
class TimeParser {
public:
    /**
     * @brief Parse a time string into a time_point
     *
     * Supported formats:
     * - ISO 8601: "2024-01-01T00:00:00Z", "2024-01-01"
     * - Relative: "7d" (7 days ago), "1w" (1 week), "1m" (1 month), "1y" (1 year)
     * - Relative hours: "24h" (24 hours ago), "1h" (1 hour ago)
     * - Unix timestamp: "1704067200"
     * - Natural: "yesterday", "today", "last-week", "last-month"
     *
     * @param timeStr String to parse
     * @return Parsed time point or error
     */
    static Result<std::chrono::system_clock::time_point> parse(const std::string& timeStr);

    /**
     * @brief Parse a relative time string (e.g., "7d", "1w")
     */
    static std::optional<std::chrono::system_clock::time_point>
    parseRelative(const std::string& relativeStr);

    /**
     * @brief Parse an ISO 8601 date/time string
     */
    static std::optional<std::chrono::system_clock::time_point>
    parseISO8601(const std::string& isoStr);

    /**
     * @brief Parse a Unix timestamp
     */
    static std::optional<std::chrono::system_clock::time_point>
    parseUnixTimestamp(const std::string& timestampStr);

    /**
     * @brief Parse natural language time (e.g., "yesterday", "last-week")
     */
    static std::optional<std::chrono::system_clock::time_point>
    parseNatural(const std::string& naturalStr);

    /**
     * @brief Format a time_point as ISO 8601 string
     */
    static std::string formatISO8601(const std::chrono::system_clock::time_point& tp);

    /**
     * @brief Format a time_point as a human-readable relative string
     */
    static std::string formatRelative(const std::chrono::system_clock::time_point& tp);

    /**
     * @brief Get the start of day for a given time point
     */
    static std::chrono::system_clock::time_point
    startOfDay(const std::chrono::system_clock::time_point& tp);

    /**
     * @brief Get the end of day for a given time point
     */
    static std::chrono::system_clock::time_point
    endOfDay(const std::chrono::system_clock::time_point& tp);
};

} // namespace yams::cli

// ============================================================================
// ProgressIndicator - Visual progress feedback
// ============================================================================

export namespace yams::cli {

/**
 * @brief A reusable progress indicator for CLI commands
 */
class ProgressIndicator {
public:
    enum class Style {
        Spinner,    // ⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏
        Dots,       // . .. ...
        Percentage, // [45%]
        Bar         // [████████░░░░░░░░]
    };

    explicit ProgressIndicator(Style style = Style::Spinner, bool autoStart = false);
    ~ProgressIndicator();

    // Non-copyable, non-movable (contains std::atomic)
    ProgressIndicator(const ProgressIndicator&) = delete;
    ProgressIndicator& operator=(const ProgressIndicator&) = delete;
    ProgressIndicator(ProgressIndicator&&) = delete;
    ProgressIndicator& operator=(ProgressIndicator&&) = delete;

    void start(const std::string& message);
    void update(size_t current, size_t total = 0);
    void tick();
    void stop();

    void setShowCount(bool show) { showCount_ = show; }
    bool isActive() const { return active_; }
    void setUpdateInterval(int ms) { updateIntervalMs_ = ms; }
    void setMessage(const std::string& message);

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

    static constexpr const char* SPINNER_CHARS[] = {"⠋", "⠙", "⠹", "⠸", "⠼",
                                                    "⠴", "⠦", "⠧", "⠇", "⠏"};
    static constexpr int SPINNER_COUNT = 10;
};

} // namespace yams::cli

// ============================================================================
// Prompt Utilities - Interactive user prompting
// ============================================================================

export namespace yams::cli {

// Yes/No prompt options
struct YesNoOptions {
    bool defaultYes{true};
    bool allowEmpty{true};
    std::string yesChars{"yY"};
    std::string noChars{"nN"};
    bool retryOnInvalid{false};
};

// Generic input options
struct InputOptions {
    std::string defaultValue{};
    bool allowEmpty{true};
    bool trimWhitespace{true};
    bool retryOnInvalid{true};
    std::function<bool(const std::string&)> validator{};
};

// Choice/menu item
struct ChoiceItem {
    std::string value;
    std::string label;
    std::string description;
};

// Choice options
struct ChoiceOptions {
    size_t defaultIndex{0};
    bool allowEmpty{true};
    bool retryOnInvalid{true};
    bool showNumericHint{true};
};

/**
 * @brief Prompt user for yes/no confirmation
 */
bool prompt_yes_no(const std::string& prompt, const YesNoOptions& opts = {});

/**
 * @brief Prompt user for text input
 */
std::string prompt_input(const std::string& prompt, const InputOptions& opts = {});

/**
 * @brief Prompt user to choose from a list of options
 */
size_t prompt_choice(const std::string& header, const std::vector<ChoiceItem>& items,
                     const ChoiceOptions& opts = {});

} // namespace yams::cli

// ============================================================================
// Session Store - Session state management
// ============================================================================

export namespace yams::cli::session_store {

/**
 * @brief Get the sessions directory path
 */
std::filesystem::path sessions_dir();

/**
 * @brief Get the session index file path
 */
std::filesystem::path index_path();

/**
 * @brief Get the current session name
 */
std::optional<std::string> current_session();

/**
 * @brief Load active include patterns for scoping
 */
std::vector<std::string>
active_include_patterns(const std::optional<std::string>& name = std::nullopt);

} // namespace yams::cli::session_store
