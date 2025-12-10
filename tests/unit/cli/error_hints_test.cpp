/**
 * Tests for error_hints.h - Pattern matching and hint generation for CLI errors.
 *
 * These tests verify that:
 * 1. Error codes map to correct hints
 * 2. Message pattern matching works correctly
 * 3. Formatted output includes hints and commands
 * 4. Helper predicates (shouldSuggestInit, shouldSuggestDoctor, etc.) work correctly
 */

#include <gtest/gtest.h>
#include <yams/cli/error_hints.h>
#include <yams/core/types.h>

using namespace yams;
using namespace yams::cli;

// ============================================================================
// getErrorHint() - Pattern-based matching tests
// ============================================================================

TEST(ErrorHintsTest, FTS5ErrorPatternDetected) {
    auto hint = getErrorHint(ErrorCode::DatabaseError, "FTS5 tokenizer failed");
    EXPECT_EQ(hint.hint, "Full-text search index may be corrupted");
    EXPECT_EQ(hint.command, "yams doctor --fix");
}

TEST(ErrorHintsTest, TokenizeErrorPatternDetected) {
    auto hint = getErrorHint(ErrorCode::DatabaseError, "failed to tokenize query");
    EXPECT_EQ(hint.hint, "Full-text search index may be corrupted");
    EXPECT_EQ(hint.command, "yams doctor --fix");
}

TEST(ErrorHintsTest, FtsTableErrorPatternDetected) {
    auto hint = getErrorHint(ErrorCode::DatabaseError, "no such table: fts_content");
    EXPECT_EQ(hint.hint, "Full-text search index may be corrupted");
    EXPECT_EQ(hint.command, "yams doctor --fix");
}

TEST(ErrorHintsTest, EmbeddingErrorPatternDetected) {
    auto hint = getErrorHint(ErrorCode::InternalError, "embedding generation failed");
    EXPECT_EQ(hint.hint, "Embedding model or vector index issue detected");
    EXPECT_EQ(hint.command, "yams doctor --check-embeddings");
}

TEST(ErrorHintsTest, VectorErrorPatternDetected) {
    auto hint = getErrorHint(ErrorCode::InternalError, "vector index corrupted");
    EXPECT_EQ(hint.hint, "Embedding model or vector index issue detected");
    EXPECT_EQ(hint.command, "yams doctor --check-embeddings");
}

TEST(ErrorHintsTest, DimensionMismatchPatternDetected) {
    auto hint = getErrorHint(ErrorCode::InvalidArgument, "dimension mismatch: expected 384, got 768");
    EXPECT_EQ(hint.hint, "Embedding model or vector index issue detected");
    EXPECT_EQ(hint.command, "yams doctor --check-embeddings");
}

TEST(ErrorHintsTest, DaemonErrorPatternDetected) {
    auto hint = getErrorHint(ErrorCode::NetworkError, "daemon not responding");
    EXPECT_EQ(hint.hint, "Daemon may not be running");
    EXPECT_EQ(hint.command, "yams daemon start");
}

TEST(ErrorHintsTest, SocketErrorPatternDetected) {
    auto hint = getErrorHint(ErrorCode::NetworkError, "socket connection failed");
    EXPECT_EQ(hint.hint, "Daemon may not be running");
    EXPECT_EQ(hint.command, "yams daemon start");
}

TEST(ErrorHintsTest, ConnectionRefusedPatternDetected) {
    auto hint = getErrorHint(ErrorCode::NetworkError, "connection refused to /tmp/yams.sock");
    EXPECT_EQ(hint.hint, "Daemon may not be running");
    EXPECT_EQ(hint.command, "yams daemon start");
}

TEST(ErrorHintsTest, UniqueConstraintPatternDetected) {
    auto hint = getErrorHint(ErrorCode::DatabaseError, "UNIQUE constraint failed: documents.hash");
    EXPECT_EQ(hint.hint, "Database integrity issue detected");
    EXPECT_EQ(hint.command, "yams doctor --fix");
}

TEST(ErrorHintsTest, NoSuchTablePatternDetected) {
    auto hint = getErrorHint(ErrorCode::DatabaseError, "no such table: documents");
    EXPECT_EQ(hint.hint, "Database schema may need migration");
    EXPECT_EQ(hint.command, "yams migrate");
}

TEST(ErrorHintsTest, DatabaseLockedPatternDetected) {
    auto hint = getErrorHint(ErrorCode::DatabaseError, "database is locked");
    EXPECT_EQ(hint.hint, "Database is locked by another process");
    EXPECT_EQ(hint.command, "yams daemon stop && yams daemon start");
}

TEST(ErrorHintsTest, SqliteBusyPatternDetected) {
    auto hint = getErrorHint(ErrorCode::DatabaseError, "SQLITE_BUSY: database locked");
    EXPECT_EQ(hint.hint, "Database is locked by another process");
    EXPECT_EQ(hint.command, "yams daemon stop && yams daemon start");
}

TEST(ErrorHintsTest, PermissionDeniedPatternDetected) {
    auto hint = getErrorHint(ErrorCode::IOError, "Permission denied: /var/yams/data");
    EXPECT_EQ(hint.hint, "Check file/directory permissions");
    EXPECT_TRUE(hint.command.empty());
}

TEST(ErrorHintsTest, ModelNotFoundPatternDetected) {
    auto hint = getErrorHint(ErrorCode::FileNotFound, "model not found: all-MiniLM-L6-v2");
    EXPECT_EQ(hint.hint, "Embedding model may need to be downloaded");
    EXPECT_EQ(hint.command, "yams init --download-model");
}

TEST(ErrorHintsTest, NotInitializedPatternDetected) {
    auto hint = getErrorHint(ErrorCode::InvalidState, "storage not initialized");
    EXPECT_EQ(hint.hint, "YAMS data directory not initialized");
    EXPECT_EQ(hint.command, "yams init");
}

// ============================================================================
// getErrorHint() - Error code fallback tests
// ============================================================================

TEST(ErrorHintsTest, NotInitializedCodeFallback) {
    // Use a message that doesn't match any pattern
    auto hint = getErrorHint(ErrorCode::NotInitialized, "generic error");
    EXPECT_EQ(hint.hint, "YAMS not initialized");
    EXPECT_EQ(hint.command, "yams init");
}

TEST(ErrorHintsTest, FileNotFoundCodeFallback) {
    auto hint = getErrorHint(ErrorCode::FileNotFound, "generic error");
    EXPECT_EQ(hint.hint, "Verify the file path exists and is accessible");
    EXPECT_TRUE(hint.command.empty());
}

TEST(ErrorHintsTest, PermissionDeniedCodeFallback) {
    auto hint = getErrorHint(ErrorCode::PermissionDenied, "generic error");
    EXPECT_EQ(hint.hint, "Check file/directory permissions");
    EXPECT_TRUE(hint.command.empty());
}

TEST(ErrorHintsTest, DatabaseErrorCodeFallback) {
    auto hint = getErrorHint(ErrorCode::DatabaseError, "generic error");
    EXPECT_EQ(hint.hint, "Run diagnostics to check database health");
    EXPECT_EQ(hint.command, "yams doctor");
}

TEST(ErrorHintsTest, NetworkErrorCodeFallback) {
    auto hint = getErrorHint(ErrorCode::NetworkError, "generic error");
    EXPECT_EQ(hint.hint, "Check network connectivity and try again");
    EXPECT_TRUE(hint.command.empty());
}

TEST(ErrorHintsTest, TimeoutCodeFallback) {
    auto hint = getErrorHint(ErrorCode::Timeout, "operation timed out");
    EXPECT_EQ(hint.hint, "Operation timed out; the daemon may be overloaded");
    EXPECT_EQ(hint.command, "yams daemon status");
}

TEST(ErrorHintsTest, InvalidArgumentCodeWithCommand) {
    auto hint = getErrorHint(ErrorCode::InvalidArgument, "invalid option", "add");
    EXPECT_EQ(hint.hint, "Check command syntax");
    EXPECT_EQ(hint.command, "yams add --help");
}

TEST(ErrorHintsTest, InvalidArgumentCodeWithoutCommand) {
    auto hint = getErrorHint(ErrorCode::InvalidArgument, "invalid option", "");
    EXPECT_EQ(hint.hint, "Check command syntax");
    EXPECT_EQ(hint.command, "yams --help");
}

TEST(ErrorHintsTest, CorruptedDataCodeFallback) {
    auto hint = getErrorHint(ErrorCode::CorruptedData, "data checksum mismatch");
    EXPECT_EQ(hint.hint, "Data corruption detected; run repair");
    EXPECT_EQ(hint.command, "yams doctor --fix");
}

TEST(ErrorHintsTest, UnknownCodeReturnsEmptyHint) {
    auto hint = getErrorHint(ErrorCode::Unknown, "something weird happened");
    EXPECT_TRUE(hint.hint.empty());
    EXPECT_TRUE(hint.command.empty());
}

// ============================================================================
// formatErrorWithHint() tests
// ============================================================================

TEST(ErrorHintsTest, FormatErrorWithHintIncludesMessage) {
    auto formatted = formatErrorWithHint(ErrorCode::NotInitialized, "storage missing");
    EXPECT_NE(formatted.find("storage missing"), std::string::npos);
}

TEST(ErrorHintsTest, FormatErrorWithHintIncludesHintEmoji) {
    auto formatted = formatErrorWithHint(ErrorCode::NotInitialized, "storage missing");
    EXPECT_NE(formatted.find("💡 Hint:"), std::string::npos);
}

TEST(ErrorHintsTest, FormatErrorWithHintIncludesCommand) {
    auto formatted = formatErrorWithHint(ErrorCode::NotInitialized, "storage missing");
    EXPECT_NE(formatted.find("📋 Try:"), std::string::npos);
    EXPECT_NE(formatted.find("yams init"), std::string::npos);
}

TEST(ErrorHintsTest, FormatErrorWithHintNoHintForUnknown) {
    auto formatted = formatErrorWithHint(ErrorCode::Unknown, "mystery error");
    // Should just be the message without hint decorations
    EXPECT_EQ(formatted, "mystery error");
}

TEST(ErrorHintsTest, FormatErrorWithHintPassesCommand) {
    auto formatted = formatErrorWithHint(ErrorCode::InvalidArgument, "bad input", "search");
    EXPECT_NE(formatted.find("yams search --help"), std::string::npos);
}

// ============================================================================
// shouldSuggestInit() tests
// ============================================================================

TEST(ErrorHintsTest, ShouldSuggestInitForNotInitializedCode) {
    EXPECT_TRUE(shouldSuggestInit(ErrorCode::NotInitialized, "any message"));
}

TEST(ErrorHintsTest, ShouldSuggestInitForNotInitializedMessage) {
    EXPECT_TRUE(shouldSuggestInit(ErrorCode::Unknown, "storage not initialized"));
}

TEST(ErrorHintsTest, ShouldSuggestInitForNoSuchTableMessage) {
    EXPECT_TRUE(shouldSuggestInit(ErrorCode::DatabaseError, "no such table: documents"));
}

TEST(ErrorHintsTest, ShouldSuggestInitForDatabaseNotFoundMessage) {
    EXPECT_TRUE(shouldSuggestInit(ErrorCode::FileNotFound, "database not found"));
}

TEST(ErrorHintsTest, ShouldNotSuggestInitForUnrelatedError) {
    EXPECT_FALSE(shouldSuggestInit(ErrorCode::NetworkError, "connection refused"));
}

// ============================================================================
// shouldSuggestDoctor() tests
// ============================================================================

TEST(ErrorHintsTest, ShouldSuggestDoctorForDatabaseErrorCode) {
    EXPECT_TRUE(shouldSuggestDoctor(ErrorCode::DatabaseError, "any message"));
}

TEST(ErrorHintsTest, ShouldSuggestDoctorForCorruptedDataCode) {
    EXPECT_TRUE(shouldSuggestDoctor(ErrorCode::CorruptedData, "any message"));
}

TEST(ErrorHintsTest, ShouldSuggestDoctorForFTS5Message) {
    EXPECT_TRUE(shouldSuggestDoctor(ErrorCode::Unknown, "FTS5 error occurred"));
}

TEST(ErrorHintsTest, ShouldSuggestDoctorForConstraintMessage) {
    EXPECT_TRUE(shouldSuggestDoctor(ErrorCode::Unknown, "constraint failed"));
}

TEST(ErrorHintsTest, ShouldSuggestDoctorForIntegrityMessage) {
    EXPECT_TRUE(shouldSuggestDoctor(ErrorCode::Unknown, "integrity check failed"));
}

TEST(ErrorHintsTest, ShouldSuggestDoctorForEmbeddingMessage) {
    EXPECT_TRUE(shouldSuggestDoctor(ErrorCode::Unknown, "embedding dimension mismatch"));
}

TEST(ErrorHintsTest, ShouldNotSuggestDoctorForUnrelatedError) {
    EXPECT_FALSE(shouldSuggestDoctor(ErrorCode::NetworkError, "connection refused"));
}

// ============================================================================
// isDaemonConnectionError() tests
// ============================================================================

TEST(ErrorHintsTest, IsDaemonConnectionErrorForNetworkErrorCode) {
    EXPECT_TRUE(isDaemonConnectionError(ErrorCode::NetworkError, "any message"));
}

TEST(ErrorHintsTest, IsDaemonConnectionErrorForDaemonMessage) {
    EXPECT_TRUE(isDaemonConnectionError(ErrorCode::Unknown, "daemon not responding"));
}

TEST(ErrorHintsTest, IsDaemonConnectionErrorForSocketMessage) {
    EXPECT_TRUE(isDaemonConnectionError(ErrorCode::Unknown, "socket error"));
}

TEST(ErrorHintsTest, IsDaemonConnectionErrorForConnectionRefusedMessage) {
    EXPECT_TRUE(isDaemonConnectionError(ErrorCode::Unknown, "connection refused"));
}

TEST(ErrorHintsTest, IsDaemonConnectionErrorForECONNREFUSEDMessage) {
    EXPECT_TRUE(isDaemonConnectionError(ErrorCode::Unknown, "ECONNREFUSED"));
}

TEST(ErrorHintsTest, IsNotDaemonConnectionErrorForUnrelatedError) {
    EXPECT_FALSE(isDaemonConnectionError(ErrorCode::DatabaseError, "constraint failed"));
}

// ============================================================================
// Pattern priority tests - ensure message patterns take precedence over codes
// ============================================================================

TEST(ErrorHintsTest, MessagePatternTakesPrecedenceOverCode) {
    // Use DatabaseError code but with FTS5 message - should get FTS5-specific hint
    auto hint = getErrorHint(ErrorCode::DatabaseError, "FTS5 tokenizer error");
    EXPECT_EQ(hint.hint, "Full-text search index may be corrupted");
    EXPECT_EQ(hint.command, "yams doctor --fix");
}

TEST(ErrorHintsTest, DaemonMessageWithDatabaseCodeGivesDaemonHint) {
    // Daemon message should match even with DatabaseError code
    auto hint = getErrorHint(ErrorCode::DatabaseError, "daemon connection refused");
    EXPECT_EQ(hint.hint, "Daemon may not be running");
    EXPECT_EQ(hint.command, "yams daemon start");
}
