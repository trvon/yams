#include <spdlog/spdlog.h>
#include <algorithm>
#include <cctype>
#include <ctime>
#include <iomanip>
#include <regex>
#include <sstream>
#include <yams/cli/time_parser.h>

namespace yams::cli {

Result<std::chrono::system_clock::time_point> TimeParser::parse(const std::string& timeStr) {
    if (timeStr.empty()) {
        return Error{ErrorCode::InvalidArgument, "Empty time string"};
    }

    // Try parsing as relative time first (most common)
    if (auto tp = parseRelative(timeStr)) {
        return tp.value();
    }

    // Try parsing as natural language
    if (auto tp = parseNatural(timeStr)) {
        return tp.value();
    }

    // Try parsing as ISO 8601
    if (auto tp = parseISO8601(timeStr)) {
        return tp.value();
    }

    // Try parsing as Unix timestamp
    if (auto tp = parseUnixTimestamp(timeStr)) {
        return tp.value();
    }

    return Error{ErrorCode::InvalidArgument,
                 "Invalid time format. Supported formats: ISO 8601 (2024-01-01), "
                 "relative (7d, 1w, 1m), Unix timestamp, or natural (yesterday, last-week)"};
}

std::optional<std::chrono::system_clock::time_point>
TimeParser::parseRelative(const std::string& relativeStr) {
    static const std::regex relativeRegex(R"(^(\d+)([hdwmy])$)", std::regex_constants::icase);

    std::smatch match;
    if (!std::regex_match(relativeStr, match, relativeRegex)) {
        return std::nullopt;
    }

    int value = std::stoi(match[1].str());
    char unit = std::tolower(match[2].str()[0]);

    auto now = std::chrono::system_clock::now();

    switch (unit) {
        case 'h': // hours
            return now - std::chrono::hours(value);
        case 'd': // days
            return now - std::chrono::hours(value * 24);
        case 'w': // weeks
            return now - std::chrono::hours(value * 24 * 7);
        case 'm': // months (approximate as 30 days)
            return now - std::chrono::hours(value * 24 * 30);
        case 'y': // years (approximate as 365 days)
            return now - std::chrono::hours(value * 24 * 365);
        default:
            return std::nullopt;
    }
}

std::optional<std::chrono::system_clock::time_point>
TimeParser::parseISO8601(const std::string& isoStr) {
    // Support both full ISO 8601 and date-only format
    std::tm tm = {};
    std::istringstream ss(isoStr);

    // Try full ISO 8601 format first (YYYY-MM-DDTHH:MM:SS)
    ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S");
    if (!ss.fail()) {
        // Handle timezone if present
        std::string tz;
        ss >> tz;

        auto tp = std::chrono::system_clock::from_time_t(std::mktime(&tm));

        // Adjust for timezone if specified
        if (tz == "Z" || tz.empty()) {
            // UTC or no timezone specified
            return tp;
        } else if (tz.size() >= 3 && (tz[0] == '+' || tz[0] == '-')) {
            // Parse timezone offset like +00:00 or -05:00
            int sign = (tz[0] == '+') ? 1 : -1;
            int hours = 0, minutes = 0;

            if (tz.size() >= 3) {
                hours = std::stoi(tz.substr(1, 2));
            }
            if (tz.size() >= 6 && tz[3] == ':') {
                minutes = std::stoi(tz.substr(4, 2));
            }

            auto offset = std::chrono::hours(hours) + std::chrono::minutes(minutes);
            return tp - (sign * offset);
        }
        return tp;
    }

    // Try date-only format (YYYY-MM-DD)
    ss.clear();
    ss.str(isoStr);
    ss >> std::get_time(&tm, "%Y-%m-%d");
    if (!ss.fail()) {
        // Set time to start of day
        tm.tm_hour = 0;
        tm.tm_min = 0;
        tm.tm_sec = 0;
        return std::chrono::system_clock::from_time_t(std::mktime(&tm));
    }

    return std::nullopt;
}

std::optional<std::chrono::system_clock::time_point>
TimeParser::parseUnixTimestamp(const std::string& timestampStr) {
    try {
        // Check if string contains only digits
        if (!std::all_of(timestampStr.begin(), timestampStr.end(), ::isdigit)) {
            return std::nullopt;
        }

        long long timestamp = std::stoll(timestampStr);

        // Determine if it's seconds or milliseconds based on magnitude
        // Unix timestamps in seconds are typically 10 digits (until year 2286)
        // Milliseconds are typically 13 digits
        if (timestampStr.length() > 11) {
            // Likely milliseconds
            return std::chrono::system_clock::from_time_t(0) + std::chrono::milliseconds(timestamp);
        } else {
            // Likely seconds
            return std::chrono::system_clock::from_time_t(timestamp);
        }
    } catch (...) {
        return std::nullopt;
    }
}

std::optional<std::chrono::system_clock::time_point>
TimeParser::parseNatural(const std::string& naturalStr) {
    // Convert to lowercase for case-insensitive comparison
    std::string lower = naturalStr;
    std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);

    auto now = std::chrono::system_clock::now();
    auto today = startOfDay(now);

    if (lower == "now") {
        return now;
    } else if (lower == "today") {
        return today;
    } else if (lower == "yesterday") {
        return today - std::chrono::hours(24);
    } else if (lower == "tomorrow") {
        return today + std::chrono::hours(24);
    } else if (lower == "last-week" || lower == "last_week" || lower == "lastweek") {
        return now - std::chrono::hours(24 * 7);
    } else if (lower == "last-month" || lower == "last_month" || lower == "lastmonth") {
        return now - std::chrono::hours(24 * 30);
    } else if (lower == "last-year" || lower == "last_year" || lower == "lastyear") {
        return now - std::chrono::hours(24 * 365);
    }

    return std::nullopt;
}

std::string TimeParser::formatISO8601(const std::chrono::system_clock::time_point& tp) {
    auto time_t = std::chrono::system_clock::to_time_t(tp);
    std::tm tm = *std::gmtime(&time_t);

    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y-%m-%dT%H:%M:%SZ");
    return oss.str();
}

std::string TimeParser::formatRelative(const std::chrono::system_clock::time_point& tp) {
    auto now = std::chrono::system_clock::now();
    auto diff = now - tp;

    if (diff < std::chrono::seconds(0)) {
        // Future time
        diff = -diff;

        if (diff < std::chrono::minutes(1)) {
            return "in a few seconds";
        } else if (diff < std::chrono::hours(1)) {
            auto minutes = std::chrono::duration_cast<std::chrono::minutes>(diff).count();
            return "in " + std::to_string(minutes) + " minute" + (minutes == 1 ? "" : "s");
        } else if (diff < std::chrono::hours(24)) {
            auto hours = std::chrono::duration_cast<std::chrono::hours>(diff).count();
            return "in " + std::to_string(hours) + " hour" + (hours == 1 ? "" : "s");
        } else {
            auto days = std::chrono::duration_cast<std::chrono::hours>(diff).count() / 24;
            return "in " + std::to_string(days) + " day" + (days == 1 ? "" : "s");
        }
    } else {
        // Past time
        if (diff < std::chrono::seconds(1)) {
            return "just now";
        } else if (diff < std::chrono::minutes(1)) {
            auto seconds = std::chrono::duration_cast<std::chrono::seconds>(diff).count();
            return std::to_string(seconds) + " second" + (seconds == 1 ? "" : "s") + " ago";
        } else if (diff < std::chrono::hours(1)) {
            auto minutes = std::chrono::duration_cast<std::chrono::minutes>(diff).count();
            return std::to_string(minutes) + " minute" + (minutes == 1 ? "" : "s") + " ago";
        } else if (diff < std::chrono::hours(24)) {
            auto hours = std::chrono::duration_cast<std::chrono::hours>(diff).count();
            if (hours == 1)
                return "1 hour ago";
            return std::to_string(hours) + " hours ago";
        } else if (diff < std::chrono::hours(48)) {
            return "yesterday";
        } else if (diff < std::chrono::hours(24 * 7)) {
            auto days = std::chrono::duration_cast<std::chrono::hours>(diff).count() / 24;
            return std::to_string(days) + " days ago";
        } else if (diff < std::chrono::hours(24 * 30)) {
            auto weeks = std::chrono::duration_cast<std::chrono::hours>(diff).count() / (24 * 7);
            return std::to_string(weeks) + " week" + (weeks == 1 ? "" : "s") + " ago";
        } else if (diff < std::chrono::hours(24 * 365)) {
            auto months = std::chrono::duration_cast<std::chrono::hours>(diff).count() / (24 * 30);
            return std::to_string(months) + " month" + (months == 1 ? "" : "s") + " ago";
        } else {
            auto years = std::chrono::duration_cast<std::chrono::hours>(diff).count() / (24 * 365);
            return std::to_string(years) + " year" + (years == 1 ? "" : "s") + " ago";
        }
    }
}

std::chrono::system_clock::time_point
TimeParser::startOfDay(const std::chrono::system_clock::time_point& tp) {
    auto time_t = std::chrono::system_clock::to_time_t(tp);
    std::tm tm = *std::localtime(&time_t);
    tm.tm_hour = 0;
    tm.tm_min = 0;
    tm.tm_sec = 0;
    return std::chrono::system_clock::from_time_t(std::mktime(&tm));
}

std::chrono::system_clock::time_point
TimeParser::endOfDay(const std::chrono::system_clock::time_point& tp) {
    auto time_t = std::chrono::system_clock::to_time_t(tp);
    std::tm tm = *std::localtime(&time_t);
    tm.tm_hour = 23;
    tm.tm_min = 59;
    tm.tm_sec = 59;
    return std::chrono::system_clock::from_time_t(std::mktime(&tm));
}

} // namespace yams::cli