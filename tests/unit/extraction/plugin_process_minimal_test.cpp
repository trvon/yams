#include <chrono>
#include <filesystem>
#include <thread>
#include <catch2/catch_test_macros.hpp>
#include <yams/extraction/plugin_process.hpp>

using namespace yams::extraction;

TEST_CASE("Minimal terminate test", "[extraction][plugin][minimal]") {
    const char* python = std::getenv("PYTHON");
    if (!python) {
        python = "python3";
    }

    std::filesystem::path mock_plugin =
        std::filesystem::current_path() / "tests" / "fixtures" / "mock_plugin.py";

    SECTION("Just spawn and explicit terminate") {
        PluginProcessConfig config{.executable = python, .args = {mock_plugin.string()}};
        PluginProcess process{std::move(config)};

        INFO("Process spawned, pid=" << process.pid());
        REQUIRE(process.is_alive());

        INFO("About to call terminate()");
        process.terminate();

        INFO("Terminate returned");
        REQUIRE(process.state() == ProcessState::Terminated);
    }
}

TEST_CASE("Minimal RAII test", "[extraction][plugin][minimal]") {
    const char* python = std::getenv("PYTHON");
    if (!python) {
        python = "python3";
    }

    std::filesystem::path mock_plugin =
        std::filesystem::current_path() / "tests" / "fixtures" / "mock_plugin.py";

    SECTION("Just spawn and let destructor clean up") {
        {
            PluginProcessConfig config{.executable = python, .args = {mock_plugin.string()}};
            PluginProcess process{std::move(config)};

            INFO("Process spawned, pid=" << process.pid());
            REQUIRE(process.is_alive());

            INFO("About to leave scope");
        } // Destructor runs here

        INFO("Destructor completed");
    }
}

TEST_CASE("Minimal double terminate test", "[extraction][plugin][minimal]") {
    const char* python = std::getenv("PYTHON");
    if (!python) {
        python = "python3";
    }

    std::filesystem::path mock_plugin =
        std::filesystem::current_path() / "tests" / "fixtures" / "mock_plugin.py";

    SECTION("Explicit terminate then destructor") {
        {
            PluginProcessConfig config{.executable = python, .args = {mock_plugin.string()}};
            PluginProcess process{std::move(config)};

            INFO("Process spawned, pid=" << process.pid());
            REQUIRE(process.is_alive());

            INFO("Calling explicit terminate()");
            process.terminate();
            INFO("Explicit terminate returned");

            REQUIRE(process.state() == ProcessState::Terminated);
            INFO("About to leave scope - destructor will run");
        } // Destructor runs here - should be a no-op since already terminated

        INFO("Destructor completed");
    }
}
