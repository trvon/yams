#include <catch2/catch_test_macros.hpp>

#include <yams/storage/sqlite_retry.h>

#include <sqlite3.h>

#include <chrono>
#include <string_view>

TEST_CASE("SQLite retry policy classifies transient lock failures", "[storage][sqlite][retry]") {
    using namespace yams::storage::sqlite_retry;

    CHECK(isBusyOrLocked(SQLITE_BUSY));
    CHECK(isBusyOrLocked(SQLITE_LOCKED));
    CHECK_FALSE(isBusyOrLocked(SQLITE_CONSTRAINT));
    CHECK_FALSE(isBusyOrLocked(SQLITE_OK));

    CHECK(isBusyOrLockedMessage("database is locked"));
    CHECK(isBusyOrLockedMessage("SQLITE_BUSY: writer busy"));
    CHECK_FALSE(isBusyOrLockedMessage("constraint failed"));
}

TEST_CASE("SQLite retry policy bounds exponential retry attempts", "[storage][sqlite][retry]") {
    using namespace yams::storage::sqlite_retry;

    const BusyRetryPolicy policy{3, std::chrono::milliseconds{7}};
    CHECK(canRetry(SQLITE_BUSY, 0, policy));
    CHECK(canRetry(SQLITE_LOCKED, 1, policy));
    CHECK_FALSE(canRetry(SQLITE_BUSY, 2, policy));
    CHECK_FALSE(canRetry(SQLITE_CONSTRAINT, 0, policy));

#if YAMS_LIBSQL_BACKEND
    CHECK((metadataStatementPolicy().maxRetries == 2));
    CHECK((vectorWritePolicy().maxRetries == 3));
#else
    CHECK((metadataStatementPolicy().maxRetries == 3));
    CHECK((vectorWritePolicy().maxRetries == 5));
#endif
}

TEST_CASE("SQLite retry policy tunes document update metadata bursts", "[storage][sqlite][retry]") {
    using namespace yams::storage::sqlite_retry;

    const auto readPolicy = metadataRepositoryQueryRetryPolicy("read", "client_search");
    CHECK((readPolicy.maxRetries == 3));
    CHECK((readPolicy.baseDelayMs == 25));
    CHECK((readPolicy.maxDelayMs == 500));

    const auto defaultWritePolicy =
        metadataRepositoryQueryRetryPolicy("write", "bench_set_metadata_batch");
    CHECK((defaultWritePolicy.maxRetries == 5));
    CHECK((defaultWritePolicy.baseDelayMs == 25));
    CHECK((defaultWritePolicy.maxDelayMs == 500));

    const auto documentUpdatePolicy =
        metadataRepositoryQueryRetryPolicy("write", "app_document_update_metadata");
    CHECK((documentUpdatePolicy.maxRetries == 7));
    CHECK((documentUpdatePolicy.baseDelayMs == 10));
    CHECK((documentUpdatePolicy.maxDelayMs == 200));

    const auto downloadMetadataPolicy =
        metadataRepositoryQueryRetryPolicy("write", "app_download_metadata_burst");
    CHECK((downloadMetadataPolicy.maxRetries == 1));
    CHECK((downloadMetadataPolicy.baseDelayMs == 10));
    CHECK((downloadMetadataPolicy.maxDelayMs == 10));
}
