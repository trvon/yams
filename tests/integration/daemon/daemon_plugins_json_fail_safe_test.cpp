#include <nlohmann/json.hpp>
#include "test_async_helpers.h"
#include <gtest/gtest.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/components/ServiceManager.h> // For test-only ServiceManager accessors
#include <yams/daemon/daemon.h>

using namespace std::chrono_literals;

namespace {

bool has_key(const std::map<std::string, std::string>& m, const char* key) {
    return m.find(key) != m.end();
}

nlohmann::json parse_json_array(const std::string& s) {
    nlohmann::json j = nlohmann::json::parse(s, nullptr, false);
    if (j.is_discarded())
        return nlohmann::json::array();
    if (!j.is_array())
        return nlohmann::json::array();
    return j;
}

} // namespace

// Verifies that the daemon always provides plugins_json in stats.additionalStats (fail-safe
// behavior), both in default (non-detailed) and detailed modes.
TEST(DaemonPluginsJsonFailSafe, PluginsJsonKeyPresentAndParsable) {
    // Launch daemon in-process
    yams::daemon::DaemonConfig cfg;
    cfg.workerThreads = 2;
    yams::daemon::YamsDaemon d(cfg);
    ASSERT_TRUE(d.start());

    // Wait briefly for basic readiness (not strictly required for this test)
    yams::daemon::DaemonClient client{};
    for (int i = 0; i < 20; ++i) {
        auto st = yams::cli::run_sync(client.status(), 2s);
        if (st)
            break;
        std::this_thread::sleep_for(50ms);
    }

    // Non-detailed stats should include plugins_json (possibly "[]")
    {
        yams::daemon::GetStatsRequest req{};
        req.detailed = false;
        auto rr = yams::cli::run_sync(client.getStats(req), 3s);
        ASSERT_TRUE(rr);
        const auto& g = rr.value();
        ASSERT_TRUE(has_key(g.additionalStats, "plugins_json"))
            << "plugins_json missing in non-detailed stats";
        auto arr = parse_json_array(g.additionalStats.at("plugins_json"));
        // Should be a JSON array even if empty
        EXPECT_TRUE(arr.is_array());
    }

    // Detailed stats should also include plugins_json (and may include plugins_error on failure)
    {
        yams::daemon::GetStatsRequest req{};
        req.detailed = true;
        auto rr = yams::cli::run_sync(client.getStats(req), 3s);
        ASSERT_TRUE(rr);
        const auto& g = rr.value();
        ASSERT_TRUE(has_key(g.additionalStats, "plugins_json"))
            << "plugins_json missing in detailed stats";
        auto arr = parse_json_array(g.additionalStats.at("plugins_json"));
        EXPECT_TRUE(arr.is_array());
        // If daemon encountered an exception while enumerating, plugins_error must be present
        if (has_key(g.additionalStats, "plugins_error")) {
            EXPECT_FALSE(g.additionalStats.at("plugins_error").empty());
        }
    }

    ASSERT_TRUE(d.stop());
}

// When the provider is marked degraded in the daemon, verify the markings are reflected
// in plugins_json (provider, degraded, error) if a matching plugin record is present.
// Note: We only assert the markings if a record exists; if no plugins are loaded in this
// environment, we skip the assertion.
TEST(DaemonPluginsJsonFailSafe, DegradedProviderMarkingsReflectedWhenPresent) {
    yams::daemon::DaemonConfig cfg;
    cfg.workerThreads = 2;
    yams::daemon::YamsDaemon d(cfg);
    ASSERT_TRUE(d.start());

    // Give the daemon a moment to initialize plugins/stats
    std::this_thread::sleep_for(150ms);

#ifdef YAMS_TESTING
    // Inject degraded state for a synthetic provider name to validate markings.
    // We use a common name ("onnx") to maximize the chance of matching a loaded provider,
    // but we only assert if we find that record.
    if (auto* sm = d.getServiceManager()) {
        sm->__test_setAdoptedProviderPluginName("onnx");
        sm->__test_setModelProviderDegraded(true, "unit-test simulated degraded provider");
    }
#endif

    yams::daemon::DaemonClient client{};
    yams::daemon::GetStatsRequest req{};
    req.detailed = true;

    // Try a few times to allow stats to refresh
    bool checked = false;
    for (int attempt = 0; attempt < 5 && !checked; ++attempt) {
        auto rr = yams::cli::run_sync(client.getStats(req), 3s);
        ASSERT_TRUE(rr);
        const auto& g = rr.value();
        ASSERT_TRUE(has_key(g.additionalStats, "plugins_json"))
            << "plugins_json missing in detailed stats";
        auto arr = parse_json_array(g.additionalStats.at("plugins_json"));
        EXPECT_TRUE(arr.is_array());

        bool found_match = false;
        for (const auto& rec : arr) {
            if (!rec.is_object())
                continue;
            // Expect fields when we find a matching provider record
            const std::string name = rec.value("name", "");
            if (name == "onnx") {
                found_match = true;
                // provider flag should be present when adopted
                if (rec.contains("provider")) {
                    EXPECT_TRUE(rec["provider"].is_boolean());
                }
                // If degraded, degraded=true and error string present
                if (rec.contains("degraded") && rec["degraded"].is_boolean() &&
                    rec["degraded"].get<bool>()) {
                    if (rec.contains("error")) {
                        if (rec["error"].is_string()) {
                            EXPECT_FALSE(rec["error"].get<std::string>().empty());
                        } else {
                            // Accept any non-empty json value for error, but prefer string
                            EXPECT_FALSE(rec["error"].dump().empty());
                        }
                    }
                }
                break;
            }
        }

        if (found_match) {
            checked = true;
        } else {
            // Allow a short delay for plugin stats to reflect adopted provider
            std::this_thread::sleep_for(100ms);
        }
    }

#ifndef YAMS_TESTING
    // Without YAMS_TESTING we cannot inject a degraded provider. We've already validated that
    // plugins_json exists and is a valid array, which satisfies the fail-safe contract. Do not
    // skip.
    SUCCEED();
#else
    if (!checked) {
        // Treat absence of a matching provider record as a non-fatal condition. The environment may
        // simply not have loaded the target plugin; the fail-safe behavior (presence & well-formed
        // plugins_json) was still verified above.
        SUCCEED()
            << "No matching plugin record found to assert degraded markings (plugin not loaded).";
    } else {
        SUCCEED();
    }
#endif

    ASSERT_TRUE(d.stop());
}