// Catch2 tests for Basic Integrity Validator
// Migrated from GTest: basic_integrity_validator_test.cpp

#include <catch2/catch_test_macros.hpp>

#include <yams/compression/integrity_validator.h>

using namespace yams;
using namespace yams::compression;

TEST_CASE("BasicIntegrityValidator - Construction",
          "[compression][integrity][basic][catch2]") {
    ValidationConfig config;
    config.defaultValidationType = ValidationType::Checksum;
    config.enableAsyncValidation = false;

    IntegrityValidator validator(config);

    CHECK(validator.config().defaultValidationType == ValidationType::Checksum);
    CHECK_FALSE(validator.config().enableAsyncValidation);
}
