// CLI Error Hints tests
// Catch2 migration from GTest (yams-3s4 / yams-cli)
//
// Tests for error_hints.h - Pattern matching and hint generation for CLI errors.

#include <catch2/catch_test_macros.hpp>

#include <yams/cli/error_hints.h>
#include <yams/core/types.h>

using namespace yams;
using namespace yams::cli;

// ============================================================================
// getErrorHint() - Pattern-based matching tests
// ============================================================================

TEST_CASE("ErrorHints - FTS5 error pattern detected", "[cli][error_hints][catch2]") {
    auto hint = getErrorHint(ErrorCode::DatabaseError, "FTS5 tokenizer failed");
    CHECK(hint.hint == "Full-text search index may be corrupted");
    CHECK(hint.command == "yams doctor --fix");
}

TEST_CASE("ErrorHints - Tokenize error pattern detected", "[cli][error_hints][catch2]") {
    auto hint = getErrorHint(ErrorCode::DatabaseError, "failed to tokenize query");
    CHECK(hint.hint == "Full-text search index may be corrupted");
    CHECK(hint.command == "yams doctor --fix");
}

TEST_CASE("ErrorHints - FTS table error pattern detected", "[cli][error_hints][catch2]") {
    auto hint = getErrorHint(ErrorCode::DatabaseError, "no such table: fts_content");
    CHECK(hint.hint == "Full-text search index may be corrupted");
    CHECK(hint.command == "yams doctor --fix");
}

TEST_CASE("ErrorHints - Embedding error pattern detected", "[cli][error_hints][catch2]") {
    auto hint = getErrorHint(ErrorCode::InternalError, "embedding generation failed");
    CHECK(hint.hint == "Embedding model or vector index issue detected");
    CHECK(hint.command == "yams doctor --check-embeddings");
}

TEST_CASE("ErrorHints - Vector error pattern detected", "[cli][error_hints][catch2]") {
    auto hint = getErrorHint(ErrorCode::InternalError, "vector index corrupted");
    CHECK(hint.hint == "Embedding model or vector index issue detected");
    CHECK(hint.command == "yams doctor --check-embeddings");
}

TEST_CASE("ErrorHints - Dimension mismatch pattern detected", "[cli][error_hints][catch2]") {
    auto hint =
        getErrorHint(ErrorCode::InvalidArgument, "dimension mismatch: expected 384, got 768");
    CHECK(hint.hint == "Embedding model or vector index issue detected");
    CHECK(hint.command == "yams doctor --check-embeddings");
}

TEST_CASE("ErrorHints - Daemon error pattern detected", "[cli][error_hints][catch2]") {
    auto hint = getErrorHint(ErrorCode::NetworkError, "daemon not responding");
    CHECK(hint.hint == "Daemon connection issue detected");
    CHECK(hint.command == "yams daemon start");
}

TEST_CASE("ErrorHints - Socket error pattern detected", "[cli][error_hints][catch2]") {
    auto hint = getErrorHint(ErrorCode::NetworkError, "socket connection failed");
    CHECK(hint.hint == "Daemon connection issue detected");
    CHECK(hint.command == "yams daemon start");
}

TEST_CASE("ErrorHints - Connection refused pattern detected", "[cli][error_hints][catch2]") {
    auto hint = getErrorHint(ErrorCode::NetworkError, "connection refused to /tmp/yams.sock");
    CHECK(hint.hint == "Daemon connection issue detected");
    CHECK(hint.command == "yams daemon start");
}

TEST_CASE("ErrorHints - Unique constraint pattern detected", "[cli][error_hints][catch2]") {
    auto hint = getErrorHint(ErrorCode::DatabaseError, "UNIQUE constraint failed: documents.hash");
    CHECK(hint.hint == "Database integrity issue detected");
    CHECK(hint.command == "yams doctor --fix");
}

TEST_CASE("ErrorHints - No such table pattern detected", "[cli][error_hints][catch2]") {
    auto hint = getErrorHint(ErrorCode::DatabaseError, "no such table: documents");
    CHECK(hint.hint == "Database schema may need migration");
    CHECK(hint.command == "yams migrate");
}

TEST_CASE("ErrorHints - Database locked pattern detected", "[cli][error_hints][catch2]") {
    auto hint = getErrorHint(ErrorCode::DatabaseError, "database is locked");
    CHECK(hint.hint == "Database is locked by another process");
    CHECK(hint.command == "yams daemon stop && yams daemon start");
}

TEST_CASE("ErrorHints - SQLite busy pattern detected", "[cli][error_hints][catch2]") {
    auto hint = getErrorHint(ErrorCode::DatabaseError, "SQLITE_BUSY: database locked");
    CHECK(hint.hint == "Database is locked by another process");
    CHECK(hint.command == "yams daemon stop && yams daemon start");
}

TEST_CASE("ErrorHints - Permission denied pattern detected", "[cli][error_hints][catch2]") {
    auto hint = getErrorHint(ErrorCode::IOError, "Permission denied: /var/yams/data");
    CHECK(hint.hint == "Check file/directory permissions");
    CHECK(hint.command.empty());
}

TEST_CASE("ErrorHints - Model not found pattern detected", "[cli][error_hints][catch2]") {
    auto hint = getErrorHint(ErrorCode::FileNotFound, "model not found: all-MiniLM-L6-v2");
    CHECK(hint.hint == "Embedding model may need to be downloaded");
    CHECK(hint.command == "yams init --download-model");
}

TEST_CASE("ErrorHints - Not initialized pattern detected", "[cli][error_hints][catch2]") {
    auto hint = getErrorHint(ErrorCode::InvalidState, "storage not initialized");
    CHECK(hint.hint == "YAMS data directory not initialized");
    CHECK(hint.command == "yams init");
}

// ============================================================================
// getErrorHint() - Error code fallback tests
// ============================================================================

TEST_CASE("ErrorHints - NotInitialized code fallback", "[cli][error_hints][catch2]") {
    auto hint = getErrorHint(ErrorCode::NotInitialized, "generic error");
    CHECK(hint.hint == "YAMS not initialized");
    CHECK(hint.command == "yams init");
}

TEST_CASE("ErrorHints - FileNotFound code fallback", "[cli][error_hints][catch2]") {
    auto hint = getErrorHint(ErrorCode::FileNotFound, "generic error");
    CHECK(hint.hint == "Verify the file path exists and is accessible");
    CHECK(hint.command.empty());
}

TEST_CASE("ErrorHints - PermissionDenied code fallback", "[cli][error_hints][catch2]") {
    auto hint = getErrorHint(ErrorCode::PermissionDenied, "generic error");
    CHECK(hint.hint == "Check file/directory permissions");
    CHECK(hint.command.empty());
}

TEST_CASE("ErrorHints - DatabaseError code fallback", "[cli][error_hints][catch2]") {
    auto hint = getErrorHint(ErrorCode::DatabaseError, "generic error");
    CHECK(hint.hint == "Run diagnostics to check database health");
    CHECK(hint.command == "yams doctor");
}

TEST_CASE("ErrorHints - NetworkError code fallback", "[cli][error_hints][catch2]") {
    auto hint = getErrorHint(ErrorCode::NetworkError, "generic error");
    CHECK(hint.hint == "Check network connectivity and try again");
    CHECK(hint.command.empty());
}

TEST_CASE("ErrorHints - Timeout code fallback", "[cli][error_hints][catch2]") {
    auto hint = getErrorHint(ErrorCode::Timeout, "operation timed out");
    CHECK(hint.hint == std::string(kDaemonLoadMessage));
    CHECK(hint.command.empty());
}

TEST_CASE("ErrorHints - InvalidArgument code with command", "[cli][error_hints][catch2]") {
    auto hint = getErrorHint(ErrorCode::InvalidArgument, "invalid option", "add");
    CHECK(hint.hint == "Check command syntax");
    CHECK(hint.command == "yams add --help");
}

TEST_CASE("ErrorHints - InvalidArgument code without command", "[cli][error_hints][catch2]") {
    auto hint = getErrorHint(ErrorCode::InvalidArgument, "invalid option", "");
    CHECK(hint.hint == "Check command syntax");
    CHECK(hint.command == "yams --help");
}

TEST_CASE("ErrorHints - CorruptedData code fallback", "[cli][error_hints][catch2]") {
    auto hint = getErrorHint(ErrorCode::CorruptedData, "data checksum mismatch");
    CHECK(hint.hint == "Data corruption detected; run repair");
    CHECK(hint.command == "yams doctor --fix");
}

TEST_CASE("ErrorHints - Unknown code returns empty hint", "[cli][error_hints][catch2]") {
    auto hint = getErrorHint(ErrorCode::Unknown, "something weird happened");
    CHECK(hint.hint.empty());
    CHECK(hint.command.empty());
}

// ============================================================================
// formatErrorWithHint() tests
// ============================================================================

TEST_CASE("ErrorHints - Format error includes message", "[cli][error_hints][catch2]") {
    auto formatted = formatErrorWithHint(ErrorCode::NotInitialized, "storage missing");
    CHECK(formatted.find("storage missing") != std::string::npos);
}

TEST_CASE("ErrorHints - Format error includes hint emoji", "[cli][error_hints][catch2]") {
    auto formatted = formatErrorWithHint(ErrorCode::NotInitialized, "storage missing");
    CHECK(formatted.find("💡 Hint:") != std::string::npos);
}

TEST_CASE("ErrorHints - Format error includes command", "[cli][error_hints][catch2]") {
    auto formatted = formatErrorWithHint(ErrorCode::NotInitialized, "storage missing");
    CHECK(formatted.find("📋 Try:") != std::string::npos);
    CHECK(formatted.find("yams init") != std::string::npos);
}

TEST_CASE("ErrorHints - Format error no hint for unknown", "[cli][error_hints][catch2]") {
    auto formatted = formatErrorWithHint(ErrorCode::Unknown, "mystery error");
    CHECK(formatted == "mystery error");
}

TEST_CASE("ErrorHints - Format error passes command", "[cli][error_hints][catch2]") {
    auto formatted = formatErrorWithHint(ErrorCode::InvalidArgument, "bad input", "search");
    CHECK(formatted.find("yams search --help") != std::string::npos);
}

TEST_CASE("ErrorHints - Format error preserves IPC message", "[cli][error_hints][catch2]") {
    auto formatted =
        formatErrorWithHint(ErrorCode::NetworkError, "[ipc:refused] Connection refused");
    CHECK(formatted.find("[ipc:refused]") != std::string::npos);
    CHECK(formatted.find("yams daemon start") != std::string::npos);
}

// ============================================================================
// shouldSuggestInit() tests
// ============================================================================

TEST_CASE("ErrorHints - Should suggest init for NotInitialized code",
          "[cli][error_hints][catch2]") {
    CHECK(shouldSuggestInit(ErrorCode::NotInitialized, "any message"));
}

TEST_CASE("ErrorHints - Should suggest init for not initialized message",
          "[cli][error_hints][catch2]") {
    CHECK(shouldSuggestInit(ErrorCode::Unknown, "storage not initialized"));
}

TEST_CASE("ErrorHints - Should suggest init for no such table message",
          "[cli][error_hints][catch2]") {
    CHECK(shouldSuggestInit(ErrorCode::DatabaseError, "no such table: documents"));
}

TEST_CASE("ErrorHints - Should suggest init for database not found message",
          "[cli][error_hints][catch2]") {
    CHECK(shouldSuggestInit(ErrorCode::FileNotFound, "database not found"));
}

TEST_CASE("ErrorHints - Should not suggest init for unrelated error",
          "[cli][error_hints][catch2]") {
    CHECK_FALSE(shouldSuggestInit(ErrorCode::NetworkError, "connection refused"));
}

// ============================================================================
// shouldSuggestDoctor() tests
// ============================================================================

TEST_CASE("ErrorHints - Should suggest doctor for DatabaseError code",
          "[cli][error_hints][catch2]") {
    CHECK(shouldSuggestDoctor(ErrorCode::DatabaseError, "any message"));
}

TEST_CASE("ErrorHints - Should suggest doctor for CorruptedData code",
          "[cli][error_hints][catch2]") {
    CHECK(shouldSuggestDoctor(ErrorCode::CorruptedData, "any message"));
}

TEST_CASE("ErrorHints - Should suggest doctor for FTS5 message", "[cli][error_hints][catch2]") {
    CHECK(shouldSuggestDoctor(ErrorCode::Unknown, "FTS5 error occurred"));
}

TEST_CASE("ErrorHints - Should suggest doctor for constraint message",
          "[cli][error_hints][catch2]") {
    CHECK(shouldSuggestDoctor(ErrorCode::Unknown, "constraint failed"));
}

TEST_CASE("ErrorHints - Should suggest doctor for integrity message",
          "[cli][error_hints][catch2]") {
    CHECK(shouldSuggestDoctor(ErrorCode::Unknown, "integrity check failed"));
}

TEST_CASE("ErrorHints - Should suggest doctor for embedding message",
          "[cli][error_hints][catch2]") {
    CHECK(shouldSuggestDoctor(ErrorCode::Unknown, "embedding dimension mismatch"));
}

TEST_CASE("ErrorHints - Should not suggest doctor for unrelated error",
          "[cli][error_hints][catch2]") {
    CHECK_FALSE(shouldSuggestDoctor(ErrorCode::NetworkError, "connection refused"));
}

// ============================================================================
// isDaemonConnectionError() tests
// ============================================================================

TEST_CASE("ErrorHints - Is daemon connection error for NetworkError code",
          "[cli][error_hints][catch2]") {
    CHECK_FALSE(isDaemonConnectionError(ErrorCode::NetworkError, "any message"));
}

TEST_CASE("ErrorHints - Is daemon connection error for daemon message",
          "[cli][error_hints][catch2]") {
    CHECK(isDaemonConnectionError(ErrorCode::Unknown, "daemon not responding"));
}

TEST_CASE("ErrorHints - Is daemon connection error for socket message",
          "[cli][error_hints][catch2]") {
    CHECK(isDaemonConnectionError(ErrorCode::Unknown, "socket error"));
}

TEST_CASE("ErrorHints - Is daemon connection error for connection refused message",
          "[cli][error_hints][catch2]") {
    CHECK(isDaemonConnectionError(ErrorCode::Unknown, "connection refused"));
}

TEST_CASE("ErrorHints - Is daemon connection error for ECONNREFUSED message",
          "[cli][error_hints][catch2]") {
    CHECK(isDaemonConnectionError(ErrorCode::Unknown, "ECONNREFUSED"));
}

TEST_CASE("ErrorHints - Is not daemon connection error for unrelated error",
          "[cli][error_hints][catch2]") {
    CHECK_FALSE(isDaemonConnectionError(ErrorCode::DatabaseError, "constraint failed"));
}

// ============================================================================
// Pattern priority tests - ensure message patterns take precedence over codes
// ============================================================================

TEST_CASE("ErrorHints - Message pattern takes precedence over code", "[cli][error_hints][catch2]") {
    auto hint = getErrorHint(ErrorCode::DatabaseError, "FTS5 tokenizer error");
    CHECK(hint.hint == "Full-text search index may be corrupted");
    CHECK(hint.command == "yams doctor --fix");
}

TEST_CASE("ErrorHints - Daemon message with database code gives daemon hint",
          "[cli][error_hints][catch2]") {
    auto hint = getErrorHint(ErrorCode::DatabaseError, "daemon connection refused");
    CHECK(hint.hint == "Daemon connection issue detected");
    CHECK(hint.command == "yams daemon start");
}

TEST_CASE("ErrorHints - IPC socket missing kind gives daemon start hint",
          "[cli][error_hints][catch2]") {
    auto hint = getErrorHint(ErrorCode::NetworkError, "[ipc:socket_missing] /tmp/yams.sock");
    CHECK(hint.hint == "Daemon IPC socket not found; start the daemon or check socket path");
    CHECK(hint.command == "yams daemon start");
}

TEST_CASE("ErrorHints - IPC timeout kind gives load hint", "[cli][error_hints][catch2]") {
    auto hint = getErrorHint(ErrorCode::Timeout, "[ipc:timeout] connect timed out");
    CHECK(hint.hint == std::string(kDaemonLoadMessage));
}
