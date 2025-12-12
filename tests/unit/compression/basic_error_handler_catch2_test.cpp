// Catch2 tests for Basic Error Handler
// Migrated from GTest: basic_error_handler_test.cpp

#include <catch2/catch_test_macros.hpp>

#include <yams/compression/error_handler.h>

using namespace yams;
using namespace yams::compression;

TEST_CASE("BasicErrorHandler - Construction", "[compression][error_handler][basic][catch2]") {
    ErrorHandlingConfig config;
    config.maxRetryAttempts = 5;
    config.enableIntegrityValidation = true;

    CompressionErrorHandler handler(config);

    CHECK(handler.config().maxRetryAttempts == 5);
    CHECK(handler.config().enableIntegrityValidation == true);
}

TEST_CASE("BasicErrorHandler - DegradedMode", "[compression][error_handler][basic][catch2]") {
    ErrorHandlingConfig config;
    CompressionErrorHandler handler(config);

    CHECK_FALSE(handler.isInDegradedMode());

    handler.setDegradedMode(true);
    CHECK(handler.isInDegradedMode());

    handler.setDegradedMode(false);
    CHECK_FALSE(handler.isInDegradedMode());
}
