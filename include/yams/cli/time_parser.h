#pragma once

#include <yams/core/types.h>
#include <chrono>
#include <string>
#include <optional>

namespace yams::cli {

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
     * @param relativeStr Relative time string
     * @return Time point relative to now, or nullopt if invalid format
     */
    static std::optional<std::chrono::system_clock::time_point> parseRelative(const std::string& relativeStr);
    
    /**
     * @brief Parse an ISO 8601 date/time string
     * @param isoStr ISO 8601 formatted string
     * @return Parsed time point or nullopt if invalid format
     */
    static std::optional<std::chrono::system_clock::time_point> parseISO8601(const std::string& isoStr);
    
    /**
     * @brief Parse a Unix timestamp
     * @param timestampStr Unix timestamp as string
     * @return Parsed time point or nullopt if invalid
     */
    static std::optional<std::chrono::system_clock::time_point> parseUnixTimestamp(const std::string& timestampStr);
    
    /**
     * @brief Parse natural language time (e.g., "yesterday", "last-week")
     * @param naturalStr Natural language time string
     * @return Parsed time point or nullopt if not recognized
     */
    static std::optional<std::chrono::system_clock::time_point> parseNatural(const std::string& naturalStr);
    
    /**
     * @brief Format a time_point as ISO 8601 string
     * @param tp Time point to format
     * @return ISO 8601 formatted string
     */
    static std::string formatISO8601(const std::chrono::system_clock::time_point& tp);
    
    /**
     * @brief Format a time_point as a human-readable relative string
     * @param tp Time point to format
     * @return Human-readable string like "2 hours ago", "yesterday", etc.
     */
    static std::string formatRelative(const std::chrono::system_clock::time_point& tp);
    
    /**
     * @brief Get the start of day for a given time point
     * @param tp Time point
     * @return Time point at 00:00:00 of the same day
     */
    static std::chrono::system_clock::time_point startOfDay(const std::chrono::system_clock::time_point& tp);
    
    /**
     * @brief Get the end of day for a given time point
     * @param tp Time point
     * @return Time point at 23:59:59 of the same day
     */
    static std::chrono::system_clock::time_point endOfDay(const std::chrono::system_clock::time_point& tp);
};

} // namespace yams::cli