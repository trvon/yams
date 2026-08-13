// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_test_macros.hpp>

#include <yams/config/config_helpers.h>

#include "../../common/test_helpers_catch2.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <optional>
#include <random>
#include <string>
#include <thread>
#include <vector>

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

    SECTION("owned restoration rejects a newer ABA writer") {
        ScopedEnvVar restore{"YAMS_TEST_OWNED_ENV", std::string{"before"}};
        const auto generation =
            yams::config::set_environment_owned("YAMS_TEST_OWNED_ENV", "leased");
        REQUIRE(generation.has_value());
        REQUIRE(yams::config::set_environment("YAMS_TEST_OWNED_ENV", "leased"));

        CHECK((yams::config::restore_environment_if_owned("YAMS_TEST_OWNED_ENV", *generation) ==
               yams::config::EnvironmentRestoreResult::OwnershipLost));
        CHECK((yams::config::getenv_optional("YAMS_TEST_OWNED_ENV") == "leased"));
    }

    SECTION("owned restoration restores the prior value while the lease is current") {
        ScopedEnvVar restore{"YAMS_TEST_OWNED_ENV", std::string{"before"}};
        const auto generation =
            yams::config::set_environment_owned("YAMS_TEST_OWNED_ENV", "leased");
        REQUIRE(generation.has_value());

        CHECK((yams::config::restore_environment_if_owned("YAMS_TEST_OWNED_ENV", *generation) ==
               yams::config::EnvironmentRestoreResult::Restored));
        CHECK((yams::config::getenv_optional("YAMS_TEST_OWNED_ENV") == "before"));
    }

    SECTION("nested restoration resumes the outer environment lease") {
        ScopedEnvVar restore{"YAMS_TEST_OWNED_ENV", std::string{"before"}};
        const auto outer = yams::config::set_environment_owned("YAMS_TEST_OWNED_ENV", "outer");
        REQUIRE(outer.has_value());
        const auto inner = yams::config::set_environment_owned("YAMS_TEST_OWNED_ENV", "inner");
        REQUIRE(inner.has_value());

        REQUIRE((yams::config::restore_environment_if_owned("YAMS_TEST_OWNED_ENV", *inner) ==
                 yams::config::EnvironmentRestoreResult::Restored));
        CHECK((yams::config::getenv_optional("YAMS_TEST_OWNED_ENV") == "outer"));

        CHECK((yams::config::restore_environment_if_owned("YAMS_TEST_OWNED_ENV", *outer) ==
               yams::config::EnvironmentRestoreResult::Restored));
        CHECK((yams::config::getenv_optional("YAMS_TEST_OWNED_ENV") == "before"));
    }

    SECTION("non-LIFO release rebases the newer lease onto the original value") {
        ScopedEnvVar restore{"YAMS_TEST_OWNED_ENV", std::string{"before"}};
        const auto outer = yams::config::set_environment_owned("YAMS_TEST_OWNED_ENV", "outer");
        REQUIRE(outer.has_value());
        const auto inner = yams::config::set_environment_owned("YAMS_TEST_OWNED_ENV", "inner");
        REQUIRE(inner.has_value());

        REQUIRE((yams::config::restore_environment_if_owned("YAMS_TEST_OWNED_ENV", *outer) ==
                 yams::config::EnvironmentRestoreResult::Released));
        CHECK((yams::config::getenv_optional("YAMS_TEST_OWNED_ENV") == "inner"));

        CHECK((yams::config::restore_environment_if_owned("YAMS_TEST_OWNED_ENV", *inner) ==
               yams::config::EnvironmentRestoreResult::Restored));
        CHECK((yams::config::getenv_optional("YAMS_TEST_OWNED_ENV") == "before"));
    }

    SECTION("same-value nested leases still restore by ownership token") {
        ScopedEnvVar restore{"YAMS_TEST_OWNED_ENV", std::string{"before"}};
        const auto outer = yams::config::set_environment_owned("YAMS_TEST_OWNED_ENV", "same");
        REQUIRE(outer.has_value());
        const auto inner = yams::config::set_environment_owned("YAMS_TEST_OWNED_ENV", "same");
        REQUIRE(inner.has_value());

        REQUIRE((yams::config::restore_environment_if_owned("YAMS_TEST_OWNED_ENV", *inner) ==
                 yams::config::EnvironmentRestoreResult::Restored));
        REQUIRE((yams::config::restore_environment_if_owned("YAMS_TEST_OWNED_ENV", *outer) ==
                 yams::config::EnvironmentRestoreResult::Restored));
        CHECK((yams::config::getenv_optional("YAMS_TEST_OWNED_ENV") == "before"));
    }

    SECTION("ownership tokens reject cross-key confusion") {
        ScopedEnvVar restoreA{"YAMS_TEST_OWNED_ENV_A", std::string{"before-a"}};
        ScopedEnvVar restoreB{"YAMS_TEST_OWNED_ENV_B", std::string{"before-b"}};
        const auto leaseA = yams::config::set_environment_owned("YAMS_TEST_OWNED_ENV_A", "a");
        const auto leaseB = yams::config::set_environment_owned("YAMS_TEST_OWNED_ENV_B", "b");
        REQUIRE(leaseA.has_value());
        REQUIRE(leaseB.has_value());

        CHECK((yams::config::restore_environment_if_owned("YAMS_TEST_OWNED_ENV_A", *leaseB) ==
               yams::config::EnvironmentRestoreResult::OwnershipLost));
        CHECK((yams::config::getenv_optional("YAMS_TEST_OWNED_ENV_A") == "a"));
        REQUIRE((yams::config::restore_environment_if_owned("YAMS_TEST_OWNED_ENV_B", *leaseB) ==
                 yams::config::EnvironmentRestoreResult::Restored));
        REQUIRE((yams::config::restore_environment_if_owned("YAMS_TEST_OWNED_ENV_A", *leaseA) ==
                 yams::config::EnvironmentRestoreResult::Restored));
    }

    SECTION("restore errors invalidate a current lease chain") {
        ScopedEnvVar restore{"YAMS_TEST_OWNED_ENV", std::string{"before"}};
        const auto outer = yams::config::set_environment_owned("YAMS_TEST_OWNED_ENV", "outer");
        const auto inner = yams::config::set_environment_owned("YAMS_TEST_OWNED_ENV", "inner");
        REQUIRE(outer.has_value());
        REQUIRE(inner.has_value());

        yams::config::testing_fail_owned_environment_restore_once();
        CHECK((yams::config::restore_environment_if_owned("YAMS_TEST_OWNED_ENV", *inner) ==
               yams::config::EnvironmentRestoreResult::Error));
        CHECK((yams::config::restore_environment_if_owned("YAMS_TEST_OWNED_ENV", *outer) ==
               yams::config::EnvironmentRestoreResult::OwnershipLost));
        CHECK((yams::config::getenv_optional("YAMS_TEST_OWNED_ENV") == "inner"));
    }

    SECTION("restore errors invalidate a non-LIFO lease chain") {
        ScopedEnvVar restore{"YAMS_TEST_OWNED_ENV", std::string{"before"}};
        const auto outer = yams::config::set_environment_owned("YAMS_TEST_OWNED_ENV", "outer");
        const auto inner = yams::config::set_environment_owned("YAMS_TEST_OWNED_ENV", "inner");
        REQUIRE(outer.has_value());
        REQUIRE(inner.has_value());

        yams::config::testing_fail_owned_environment_restore_once();
        CHECK((yams::config::restore_environment_if_owned("YAMS_TEST_OWNED_ENV", *outer) ==
               yams::config::EnvironmentRestoreResult::Error));
        CHECK((yams::config::restore_environment_if_owned("YAMS_TEST_OWNED_ENV", *inner) ==
               yams::config::EnvironmentRestoreResult::OwnershipLost));
        CHECK((yams::config::getenv_optional("YAMS_TEST_OWNED_ENV") == "inner"));
    }

#ifdef _WIN32
    SECTION("Windows lease ownership canonicalizes case variants and restores unset state") {
        ScopedEnvVar restore{"YAMS_TEST_CASE_ENV", std::nullopt};

        const auto outer = yams::config::set_environment_owned("YAMS_TEST_CASE_ENV", "outer");
        REQUIRE(outer.has_value());
        const auto inner = yams::config::set_environment_owned("yams_test_case_env", "inner");
        REQUIRE(inner.has_value());
        REQUIRE((yams::config::restore_environment_if_owned("YAMS_TEST_CASE_ENV", *outer) ==
                 yams::config::EnvironmentRestoreResult::Released));
        REQUIRE((yams::config::restore_environment_if_owned("yams_test_case_env", *inner) ==
                 yams::config::EnvironmentRestoreResult::Restored));
        CHECK_FALSE(yams::config::getenv_optional("YAMS_TEST_CASE_ENV").has_value());

        const auto lifoOuter = yams::config::set_environment_owned("YAMS_TEST_CASE_ENV", "outer");
        REQUIRE(lifoOuter.has_value());
        const auto lifoInner = yams::config::set_environment_owned("yams_test_case_env", "inner");
        REQUIRE(lifoInner.has_value());
        REQUIRE((yams::config::restore_environment_if_owned("yams_test_case_env", *lifoInner) ==
                 yams::config::EnvironmentRestoreResult::Restored));
        CHECK((yams::config::getenv_optional("YAMS_TEST_CASE_ENV") == "outer"));
        REQUIRE((yams::config::restore_environment_if_owned("YAMS_TEST_CASE_ENV", *lifoOuter) ==
                 yams::config::EnvironmentRestoreResult::Restored));
        CHECK_FALSE(yams::config::getenv_optional("YAMS_TEST_CASE_ENV").has_value());
    }
#endif
}

TEST_CASE("ScopedEnvVar restores prior values and preserves unset state",
          "[config][environment][scoped-env]") {
    const std::string key = "YAMS_TEST_SCOPED_ENV_RESTORE";
    ScopedEnvVar hostRestore{key};

    REQUIRE(yams::config::set_environment(key.c_str(), "before"));
    {
        ScopedEnvVar guard{key, std::string{"during"}};
        CHECK((yams::config::getenv_optional(key) == "during"));
    }
    CHECK((yams::config::getenv_optional(key) == "before"));

    REQUIRE(yams::config::set_environment(key.c_str(), nullptr));
    {
        ScopedEnvVar guard{key, std::string{"during"}};
        CHECK((yams::config::getenv_optional(key) == "during"));
    }
    CHECK_FALSE(yams::config::getenv_optional(key).has_value());

#ifndef _WIN32
    REQUIRE(yams::config::set_environment(key.c_str(), ""));
    REQUIRE(yams::config::getenv_optional(key).has_value());
    {
        ScopedEnvVar guard{key, std::string{"during"}};
        CHECK((yams::config::getenv_optional(key) == "during"));
    }
    const auto restoredEmpty = yams::config::getenv_optional(key);
    REQUIRE(restoredEmpty.has_value());
    CHECK(restoredEmpty->empty());
#endif
}

TEST_CASE("ScopedEnvVar move operations transfer sole restoration ownership",
          "[config][environment][scoped-env][move]") {
    const std::string firstKey = "YAMS_TEST_SCOPED_ENV_MOVE_FIRST";
    const std::string secondKey = "YAMS_TEST_SCOPED_ENV_MOVE_SECOND";
    ScopedEnvVar restoreFirst{firstKey};
    ScopedEnvVar restoreSecond{secondKey};
    REQUIRE(yams::config::set_environment(firstKey.c_str(), "base-first"));
    REQUIRE(yams::config::set_environment(secondKey.c_str(), "base-second"));

    SECTION("move construction leaves the source inactive") {
        std::optional<ScopedEnvVar> source;
        source.emplace(firstKey, std::string{"override-first"});
        {
            ScopedEnvVar destination{std::move(source.value())};
            source.reset();
            CHECK((yams::config::getenv_optional(firstKey) == "override-first"));
        }
        CHECK((yams::config::getenv_optional(firstKey) == "base-first"));
    }

    SECTION("move assignment restores the destination before adopting the source") {
        std::optional<ScopedEnvVar> first;
        std::optional<ScopedEnvVar> second;
        first.emplace(firstKey, std::string{"override-first"});
        second.emplace(secondKey, std::string{"override-second"});

        *first = std::move(*second);
        CHECK((yams::config::getenv_optional(firstKey) == "base-first"));
        CHECK((yams::config::getenv_optional(secondKey) == "override-second"));

        second.reset();
        CHECK((yams::config::getenv_optional(secondKey) == "override-second"));
        first.reset();
        CHECK((yams::config::getenv_optional(secondKey) == "base-second"));
    }
}

TEST_CASE("ScopedEnvVar restoration is independent of deterministic scenario order",
          "[config][environment][scoped-env][order]") {
    const std::string key = "YAMS_TEST_SCOPED_ENV_ORDER";
    ScopedEnvVar hostRestore{key};
    std::array<std::optional<std::string>, 4> baselines{std::nullopt, std::string{},
                                                        std::string{"alpha"}, std::string{"beta"}};

    for (const auto seed : {66U, 6601U, 6602U}) {
        std::mt19937 random{seed};
        std::shuffle(baselines.begin(), baselines.end(), random);
        for (const auto& baseline : baselines) {
            REQUIRE(
                yams::config::set_environment(key.c_str(), baseline ? baseline->c_str() : nullptr));
            {
                ScopedEnvVar guard{key, std::string{"replacement"}};
                guard.unset();
                guard.set("replacement-again");
            }
            CHECK((yams::config::getenv_optional(key) == baseline));
        }
    }
}

TEST_CASE("environment boundary round-trips empty and unset values",
          "[config][environment][scoped-env][empty]") {
    const std::string key = "YAMS_TEST_SCOPED_ENV_EMPTY";
    ScopedEnvVar hostRestore{key, std::nullopt};

    REQUIRE(yams::config::set_environment(key.c_str(), nullptr));
    CHECK_FALSE(yams::config::getenv_optional(key).has_value());

    REQUIRE(yams::config::set_environment(key.c_str(), ""));
    const auto emptyValue = yams::config::getenv_optional(key);
    REQUIRE(emptyValue.has_value());
    CHECK(emptyValue->empty());

    REQUIRE(yams::config::set_environment(key.c_str(), nullptr));
    CHECK_FALSE(yams::config::getenv_optional(key).has_value());

    REQUIRE(yams::config::set_environment(key.c_str(), "alpha"));
    CHECK(yams::config::getenv_optional(key) == std::string{"alpha"});

    // A ScopedEnvVar guard restores an explicitly empty value instead of collapsing it to unset.
    REQUIRE(yams::config::set_environment(key.c_str(), ""));
    {
        ScopedEnvVar guard{key, std::string{"replacement"}};
        CHECK(yams::config::getenv_optional(key) == std::string{"replacement"});
    }
    const auto restoredEmpty = yams::config::getenv_optional(key);
    REQUIRE(restoredEmpty.has_value());
    CHECK(restoredEmpty->empty());
}

TEST_CASE("config environment boundary serializes concurrent readers and writers",
          "[config][environment][concurrent]") {
    const std::string key = "YAMS_TEST_SCOPED_ENV_CONCURRENT";
    const std::string first(512, 'a');
    const std::string second(768, 'b');
    ScopedEnvVar hostRestore{key, std::nullopt};
    constexpr int kReaderCount = 4;
    std::atomic<bool> start{false};
    std::atomic<bool> done{false};
    std::atomic<bool> valid{true};
    std::array<std::atomic<std::size_t>, kReaderCount> reads{};

    std::thread writer([&] {
        while (!start.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        for (int iteration = 0; iteration < 2000; ++iteration) {
            const char* value = (iteration % 4 == 0)   ? first.c_str()
                                : (iteration % 4 == 1) ? second.c_str()
                                : (iteration % 4 == 2) ? ""
                                                       : nullptr;
            if (!yams::config::set_environment(key.c_str(), value)) {
                valid.store(false, std::memory_order_relaxed);
                break;
            }
            if (iteration == 0) {
                while (std::ranges::any_of(reads, [](const auto& count) {
                    return count.load(std::memory_order_acquire) == 0;
                })) {
                    std::this_thread::yield();
                }
            }
        }
        done.store(true, std::memory_order_release);
    });

    std::vector<std::thread> readers;
    readers.reserve(kReaderCount);
    for (int index = 0; index < kReaderCount; ++index) {
        readers.emplace_back([&, index] {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            while (!done.load(std::memory_order_acquire)) {
                const auto value = yams::config::getenv_optional(key);
                reads[index].fetch_add(1, std::memory_order_release);
                if (value && !value->empty() && *value != first && *value != second) {
                    valid.store(false, std::memory_order_relaxed);
                    return;
                }
            }
        });
    }

    start.store(true, std::memory_order_release);
    writer.join();
    for (auto& reader : readers) {
        reader.join();
    }
    CHECK(valid.load(std::memory_order_relaxed));
    for (const auto& count : reads) {
        CHECK((count.load(std::memory_order_relaxed) > 0));
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
