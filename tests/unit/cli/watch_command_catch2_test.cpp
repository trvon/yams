// `yams watch` turns watching on for a project's session and nothing else.
#include "embedded_cli_fixture.h"

#include <filesystem>
#include <fstream>
#include <string>

#include <yams/app/services/factory.hpp>
#include <yams/app/services/session_service.hpp>

using yams::test::EmbeddedCorpus;

TEST_CASE("watch leaves the current session alone unless asked", "[cli][watch]") {
    EmbeddedCorpus corpus("yams_watch_cmd_");
    yams::test::ScopedEnvVar stateHome{"XDG_STATE_HOME", (corpus.root / "state").string()};
    yams::test::ScopedEnvVar noCurrent{"YAMS_SESSION_CURRENT", std::nullopt};
    const auto project = corpus.root / "project";
    std::filesystem::create_directories(project);

    auto sessions = yams::app::services::makeSessionService(nullptr);
    REQUIRE(sessions);
    sessions->init("mine", "");
    REQUIRE(sessions->current() == std::optional<std::string>{"mine"});

    // Making the project's session current changed how every later command was scoped.
    const auto enabled = corpus.run({"watch", "--root", project.string(), "--session", "proj",
                                     "--daemon-ready-timeout-ms", "0"});
    INFO(enabled.out << enabled.err);
    REQUIRE(enabled.rc == 0);
    CHECK(sessions->watchEnabled(std::string("proj")));
    CHECK(sessions->listPathSelectors("proj") == std::vector<std::string>{project.string()});
    CHECK(sessions->current() == std::optional<std::string>{"mine"});

    SECTION("with no current session, none is created and the session is not closed") {
        sessions->clearCurrent();
        const auto again = corpus.run({"watch", "--root", project.string(), "--session", "other",
                                       "--daemon-ready-timeout-ms", "0"});
        INFO(again.out << again.err);
        REQUIRE(again.rc == 0);
        CHECK_FALSE(sessions->current().has_value());
        CHECK(sessions->watchEnabled(std::string("other")));
        // Clearing "current" must not go through close(), which stamps the session closed.
        std::ifstream file(corpus.root / "state" / "yams" / "sessions" / "other.json");
        const auto stored = nlohmann::json::parse(file);
        CHECK_FALSE(stored.contains("lastClosedTime"));
    }
    SECTION("--use makes it current") {
        const auto used = corpus.run({"watch", "--root", project.string(), "--session", "proj",
                                      "--use", "--daemon-ready-timeout-ms", "0"});
        INFO(used.out << used.err);
        REQUIRE(used.rc == 0);
        CHECK(sessions->current() == std::optional<std::string>{"proj"});
    }
    SECTION("stopping keeps the current session too") {
        const auto stopped = corpus.run({"watch", "stop", "--root", project.string(), "--session",
                                         "proj", "--daemon-ready-timeout-ms", "0"});
        INFO(stopped.out << stopped.err);
        REQUIRE(stopped.rc == 0);
        CHECK_FALSE(sessions->watchEnabled(std::string("proj")));
        CHECK(sessions->current() == std::optional<std::string>{"mine"});
    }
}
