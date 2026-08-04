// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_test_macros.hpp>

#include <yams/config/config_helpers.h>

#include "../../common/test_helpers_catch2.h"

#include <chrono>
#include <optional>
#include <string>

namespace {

using yams::test::ScopedEnvVar;

struct VectorAliasEnvironment {
    ScopedEnvVar singular{"YAMS_DISABLE_VECTOR", std::nullopt};
    ScopedEnvVar plural{"YAMS_DISABLE_VECTORS", std::nullopt};
    ScopedEnvVar database{"YAMS_DISABLE_VECTOR_DB", std::nullopt};
};

TEST_CASE("typed environment readers distinguish unset valid and invalid values",
          "[config][environment][typed]") {
    SECTION("boolean values are strict and case insensitive") {
        ScopedEnvVar value{"YAMS_TEST_TYPED_BOOL", std::string{" TrUe "}};
        const auto parsed = yams::config::read_env_bool("YAMS_TEST_TYPED_BOOL");
        CHECK(parsed.present);
        REQUIRE(parsed.value.has_value());
        CHECK(*parsed.value);
        CHECK_FALSE(parsed.invalid());
    }

    SECTION("a boolean typo preserves either typed default") {
        ScopedEnvVar value{"YAMS_TEST_TYPED_BOOL", std::string{"treu"}};
        const auto parsed = yams::config::read_env_bool("YAMS_TEST_TYPED_BOOL");
        CHECK(parsed.present);
        CHECK_FALSE(parsed.value.has_value());
        CHECK(parsed.invalid());
        CHECK(parsed.valueOr(true));
        CHECK_FALSE(parsed.valueOr(false));
    }

    SECTION("unset and empty values both preserve the typed default") {
        ScopedEnvVar value{"YAMS_TEST_TYPED_BOOL", std::nullopt};
        const auto unset = yams::config::read_env_bool("YAMS_TEST_TYPED_BOOL");
        CHECK_FALSE(unset.present);
        CHECK_FALSE(unset.invalid());

        ScopedEnvVar empty{"YAMS_TEST_TYPED_BOOL", std::string{}};
        const auto emptyValue = yams::config::read_env_bool("YAMS_TEST_TYPED_BOOL");
        CHECK_FALSE(emptyValue.present);
        CHECK_FALSE(emptyValue.invalid());
    }

    SECTION("integer size and duration reject partial or out-of-domain input") {
        ScopedEnvVar signedValue{"YAMS_TEST_TYPED_INT", std::string{"-12"}};
        const auto parsedInt = yams::config::read_env_int("YAMS_TEST_TYPED_INT");
        REQUIRE(parsedInt.value.has_value());
        CHECK((*parsedInt.value == -12));

        ScopedEnvVar size{"YAMS_TEST_TYPED_SIZE", std::string{"4096"}};
        const auto parsedSize = yams::config::read_env_size("YAMS_TEST_TYPED_SIZE");
        REQUIRE(parsedSize.value.has_value());
        CHECK((*parsedSize.value == 4096));

        ScopedEnvVar partialSize{"YAMS_TEST_TYPED_SIZE", std::string{"4k"}};
        CHECK(yams::config::read_env_size("YAMS_TEST_TYPED_SIZE").invalid());

        ScopedEnvVar duration{"YAMS_TEST_TYPED_DURATION", std::string{"250"}};
        const auto parsedDuration = yams::config::read_env_milliseconds("YAMS_TEST_TYPED_DURATION");
        REQUIRE(parsedDuration.value.has_value());
        CHECK((*parsedDuration.value == std::chrono::milliseconds{250}));

        ScopedEnvVar negativeDuration{"YAMS_TEST_TYPED_DURATION", std::string{"-1"}};
        CHECK(yams::config::read_env_milliseconds("YAMS_TEST_TYPED_DURATION").invalid());
    }

    SECTION("shared environment mutations use the same boundary as readers") {
        ScopedEnvVar restore{"YAMS_TEST_SHARED_ENV", std::nullopt};
        REQUIRE(yams::config::set_environment("YAMS_TEST_SHARED_ENV", "value"));
        CHECK((yams::config::getenv_optional("YAMS_TEST_SHARED_ENV") == "value"));
        REQUIRE(yams::config::set_environment("YAMS_TEST_SHARED_ENV", nullptr));
        CHECK_FALSE(yams::config::getenv_optional("YAMS_TEST_SHARED_ENV").has_value());
    }
}

TEST_CASE("vector environment policy resolves all compatibility aliases consistently",
          "[config][environment][vector]") {
    VectorAliasEnvironment environment;

    SECTION("typed defaults survive when no alias is set") {
        CHECK(yams::config::resolve_vector_environment(true).enabled);
        CHECK_FALSE(yams::config::resolve_vector_environment(false).enabled);
    }

    SECTION("each true alias disables vectors") {
        {
            ScopedEnvVar value{"YAMS_DISABLE_VECTOR", std::string{"on"}};
            CHECK_FALSE(yams::config::resolve_vector_environment(true).enabled);
        }
        {
            ScopedEnvVar value{"YAMS_DISABLE_VECTORS", std::string{"1"}};
            CHECK_FALSE(yams::config::resolve_vector_environment(true).enabled);
        }
        {
            ScopedEnvVar value{"YAMS_DISABLE_VECTOR_DB", std::string{"TRUE"}};
            CHECK_FALSE(yams::config::resolve_vector_environment(true).enabled);
        }
    }

    SECTION("false aliases do not override a disabled typed default") {
        ScopedEnvVar value{"YAMS_DISABLE_VECTORS", std::string{"false"}};
        CHECK(yams::config::resolve_vector_environment(true).enabled);
        CHECK_FALSE(yams::config::resolve_vector_environment(false).enabled);
    }

    SECTION("a typo warns through diagnostics and preserves the typed default") {
        ScopedEnvVar value{"YAMS_DISABLE_VECTORS", std::string{"treu"}};
        const auto enabled = yams::config::resolve_vector_environment(true);
        CHECK(enabled.enabled);
        CHECK_FALSE(enabled.diagnostics.empty());

        const auto disabled = yams::config::resolve_vector_environment(false);
        CHECK_FALSE(disabled.enabled);
        CHECK_FALSE(disabled.diagnostics.empty());
    }

    SECTION("a valid disable wins conservatively over a false compatibility alias") {
        ScopedEnvVar canonical{"YAMS_DISABLE_VECTORS", std::string{"false"}};
        ScopedEnvVar compatibility{"YAMS_DISABLE_VECTOR_DB", std::string{"true"}};
        const auto policy = yams::config::resolve_vector_environment(true);
        CHECK_FALSE(policy.enabled);
        CHECK_FALSE(policy.diagnostics.empty());
        REQUIRE((policy.disableSources.size() == 1));
        CHECK((policy.disableSources.front() == "YAMS_DISABLE_VECTOR_DB"));
    }
}

} // namespace
