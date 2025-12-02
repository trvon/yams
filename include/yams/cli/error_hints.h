#pragma once
#include <string>
#include <string_view>
#include <yams/core/types.h>

namespace yams::cli {

/**
 * Centralized error hint system for CLI.
 * Provides actionable hints based on error codes and message patterns.
 */
struct ErrorHint {
    std::string hint;     // Short actionable suggestion
    std::string command;  // Suggested command to run (if any)
    std::string docLink;  // Documentation link (if any)
};

/**
 * Get an actionable hint for a given error.
 * 
 * @param code The ErrorCode enum value
 * @param message The error message (used for pattern matching)
 * @param command The command that was executing (for context)
 * @return An ErrorHint with actionable suggestions
 */
inline ErrorHint getErrorHint(ErrorCode code, std::string_view message,
                              std::string_view command = "") {
    ErrorHint hint;

    // Pattern-based hints (check message content first for specificity)
    if (message.find("FTS5") != std::string_view::npos ||
        message.find("tokenize") != std::string_view::npos ||
        message.find("fts_") != std::string_view::npos) {
        hint.hint = "Full-text search index may be corrupted";
        hint.command = "yams doctor --fix";
        return hint;
    }

    if (message.find("embedding") != std::string_view::npos ||
        message.find("vector") != std::string_view::npos ||
        message.find("dimension mismatch") != std::string_view::npos) {
        hint.hint = "Embedding model or vector index issue detected";
        hint.command = "yams doctor --check-embeddings";
        return hint;
    }

    if (message.find("daemon") != std::string_view::npos ||
        message.find("socket") != std::string_view::npos ||
        message.find("connection refused") != std::string_view::npos) {
        hint.hint = "Daemon may not be running";
        hint.command = "yams daemon start";
        return hint;
    }

    if (message.find("constraint failed") != std::string_view::npos ||
        message.find("UNIQUE constraint") != std::string_view::npos) {
        hint.hint = "Database integrity issue detected";
        hint.command = "yams doctor --fix";
        return hint;
    }

    if (message.find("no such table") != std::string_view::npos ||
        message.find("table not found") != std::string_view::npos) {
        hint.hint = "Database schema may need migration";
        hint.command = "yams migrate";
        return hint;
    }

    if (message.find("locked") != std::string_view::npos ||
        message.find("SQLITE_BUSY") != std::string_view::npos ||
        message.find("database is locked") != std::string_view::npos) {
        hint.hint = "Database is locked by another process";
        hint.command = "yams daemon stop && yams daemon start";
        return hint;
    }

    if (message.find("permission") != std::string_view::npos ||
        message.find("Permission denied") != std::string_view::npos ||
        message.find("EACCES") != std::string_view::npos) {
        hint.hint = "Check file/directory permissions";
        hint.command = "";
        return hint;
    }

    if (message.find("model") != std::string_view::npos &&
        (message.find("not found") != std::string_view::npos ||
         message.find("load") != std::string_view::npos)) {
        hint.hint = "Embedding model may need to be downloaded";
        hint.command = "yams init --download-model";
        return hint;
    }

    if (message.find("not initialized") != std::string_view::npos ||
        message.find("NotInitialized") != std::string_view::npos) {
        hint.hint = "YAMS data directory not initialized";
        hint.command = "yams init";
        return hint;
    }

    // Error code-based hints (fallback)
    switch (code) {
        case ErrorCode::NotInitialized:
            hint.hint = "YAMS not initialized";
            hint.command = "yams init";
            break;

        case ErrorCode::FileNotFound:
            hint.hint = "Verify the file path exists and is accessible";
            break;

        case ErrorCode::PermissionDenied:
            hint.hint = "Check file/directory permissions";
            break;

        case ErrorCode::DatabaseError:
            hint.hint = "Run diagnostics to check database health";
            hint.command = "yams doctor";
            break;

        case ErrorCode::NetworkError:
            hint.hint = "Check network connectivity and try again";
            break;

        case ErrorCode::Timeout:
            hint.hint = "Operation timed out; the daemon may be overloaded";
            hint.command = "yams daemon status";
            break;

        case ErrorCode::ResourceExhausted:
            hint.hint = "System resources exhausted; try reducing batch size";
            break;

        case ErrorCode::InvalidArgument:
            hint.hint = "Check command syntax";
            hint.command = command.empty() ? "yams --help" : std::string("yams ") + std::string(command) + " --help";
            break;

        case ErrorCode::CorruptedData:
        case ErrorCode::DataCorruption:
            hint.hint = "Data corruption detected; run repair";
            hint.command = "yams doctor --fix";
            break;

        case ErrorCode::StorageFull:
            hint.hint = "Disk space is low; free up space or change data directory";
            break;

        default:
            // No specific hint available
            break;
    }

    return hint;
}

/**
 * Format an error message with an actionable hint.
 * 
 * @param code The ErrorCode enum value
 * @param message The error message
 * @param command The command that was executing (for context)
 * @return Formatted error message with hint
 */
inline std::string formatErrorWithHint(ErrorCode code, std::string_view message,
                                       std::string_view command = "") {
    auto hint = getErrorHint(code, message, command);

    std::string result(message);

    if (!hint.hint.empty()) {
        result += "\n  💡 Hint: " + hint.hint;
        if (!hint.command.empty()) {
            result += "\n  📋 Try: " + hint.command;
        }
        if (!hint.docLink.empty()) {
            result += "\n  📖 Docs: " + hint.docLink;
        }
    }

    return result;
}

/**
 * Check if an error should suggest running 'yams init'.
 */
inline bool shouldSuggestInit(ErrorCode code, std::string_view message) {
    if (code == ErrorCode::NotInitialized) {
        return true;
    }
    if (message.find("not initialized") != std::string_view::npos ||
        message.find("no such table") != std::string_view::npos ||
        message.find("database not found") != std::string_view::npos) {
        return true;
    }
    return false;
}

/**
 * Check if an error should suggest running 'yams doctor'.
 */
inline bool shouldSuggestDoctor(ErrorCode code, std::string_view message) {
    if (code == ErrorCode::DatabaseError || code == ErrorCode::CorruptedData ||
        code == ErrorCode::DataCorruption) {
        return true;
    }
    if (message.find("FTS5") != std::string_view::npos ||
        message.find("constraint") != std::string_view::npos ||
        message.find("integrity") != std::string_view::npos ||
        message.find("embedding") != std::string_view::npos) {
        return true;
    }
    return false;
}

/**
 * Check if an error is related to daemon connectivity.
 */
inline bool isDaemonConnectionError(ErrorCode code, std::string_view message) {
    if (code == ErrorCode::NetworkError) {
        return true;
    }
    if (message.find("daemon") != std::string_view::npos ||
        message.find("socket") != std::string_view::npos ||
        message.find("connection refused") != std::string_view::npos ||
        message.find("ECONNREFUSED") != std::string_view::npos) {
        return true;
    }
    return false;
}

} // namespace yams::cli
