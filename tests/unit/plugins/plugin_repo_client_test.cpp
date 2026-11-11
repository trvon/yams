/**
 * @file plugin_repo_client_test.cpp
 * @brief Unit tests for the plugin repository client (Catch2)
 *
 * Tests the PluginRepoClient functionality including:
 * - Listing plugins from remote repository
 * - Getting plugin metadata
 * - Version listing
 * - Error handling and network failures
 * - URL encoding and special characters
 */

#include <catch2/catch_test_macros.hpp>

#include <yams/plugins/plugin_repo_client.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#ifndef _WIN32
#include <poll.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#endif

namespace yams::plugins::test {

/**
 * @brief Fixture for PluginRepoClient tests
 */
struct PluginRepoClientFixture {
    PluginRepoClientFixture() {
        // Default config with test URL
        config.repoUrl = "https://test-repo.example.com";
        config.timeout = std::chrono::milliseconds{5000};
        config.verifyTls = true;
    }

    PluginRepoConfig config;
};

// =============================================================================
// Configuration Tests
// =============================================================================

TEST_CASE("PluginRepoConfig defaults", "[plugins][repo-client][config]") {
    PluginRepoConfig defaultConfig;

    SECTION("default repository URL is correct") {
        CHECK(defaultConfig.repoUrl == DEFAULT_PLUGIN_REPO_URL);
    }

    SECTION("default repository URL uses HTTPS") {
        CHECK(defaultConfig.repoUrl.rfind("https://", 0) == 0);
    }

    SECTION("default timeout is 30 seconds") {
        CHECK(defaultConfig.timeout == std::chrono::milliseconds{30000});
    }

    SECTION("TLS verification is enabled by default") {
        CHECK(defaultConfig.verifyTls);
    }
}

TEST_CASE("RemotePluginInfo default values", "[plugins][repo-client][types]") {
    RemotePluginInfo info;

    CHECK(info.name.empty());
    CHECK(info.version.empty());
    CHECK(info.description.empty());
    CHECK(info.interfaces.empty());
    CHECK(info.author.empty());
    CHECK(info.license.empty());
    CHECK(info.downloadUrl.empty());
    CHECK(info.checksum.empty());
    CHECK(info.sizeBytes == 0u);
    CHECK(info.abiVersion == 1);
    CHECK(info.downloads == 0u);
}

TEST_CASE("RemotePluginSummary default values", "[plugins][repo-client][types]") {
    RemotePluginSummary summary;

    CHECK(summary.name.empty());
    CHECK(summary.latestVersion.empty());
    CHECK(summary.description.empty());
    CHECK(summary.interfaces.empty());
    CHECK(summary.downloads == 0u);
}

TEST_CASE_METHOD(PluginRepoClientFixture, "Client creation", "[plugins][repo-client]") {
    SECTION("creates valid client from config") {
        auto client = makePluginRepoClient(config);
        REQUIRE(client != nullptr);
    }
}

TEST_CASE_METHOD(PluginRepoClientFixture, "Custom config options",
                 "[plugins][repo-client][config]") {
    SECTION("custom timeout is preserved") {
        PluginRepoConfig customConfig;
        customConfig.timeout = std::chrono::milliseconds{60000};
        CHECK(customConfig.timeout.count() == 60000);
    }

    SECTION("TLS verification can be disabled") {
        PluginRepoConfig customConfig;
        customConfig.verifyTls = false;
        CHECK_FALSE(customConfig.verifyTls);
    }

    SECTION("proxy can be configured") {
        PluginRepoConfig customConfig;
        customConfig.proxy = "http://proxy.example.com:8080";
        REQUIRE(customConfig.proxy.has_value());
        CHECK(*customConfig.proxy == "http://proxy.example.com:8080");
    }

    SECTION("user agent can be configured") {
        PluginRepoConfig customConfig;
        customConfig.userAgent = "yams-test/1.0";
        CHECK(customConfig.userAgent == "yams-test/1.0");
    }
}

// =============================================================================
// Note: Network tests require a mock HTTP server or integration tests
// These tests are disabled as they require network access
// =============================================================================

TEST_CASE_METHOD(PluginRepoClientFixture, "List plugins (disabled - requires network)",
                 "[.][plugins][repo-client][network]") {
    auto client = makePluginRepoClient(config);
    auto result = client->list();
    CHECK(result.has_value());
}

TEST_CASE_METHOD(PluginRepoClientFixture, "Get plugin metadata (disabled - requires network)",
                 "[.][plugins][repo-client][network]") {
    auto client = makePluginRepoClient(config);
    auto result = client->get("test-plugin");
    CHECK(result.has_value());
}

TEST_CASE_METHOD(PluginRepoClientFixture, "List versions (disabled - requires network)",
                 "[.][plugins][repo-client][network]") {
    auto client = makePluginRepoClient(config);
    auto result = client->versions("test-plugin");
    CHECK(result.has_value());
}

TEST_CASE_METHOD(PluginRepoClientFixture, "Exists check (disabled - requires network)",
                 "[.][plugins][repo-client][network]") {
    auto client = makePluginRepoClient(config);
    auto result = client->exists("test-plugin");
    CHECK(result.has_value());
}

#ifndef _WIN32
namespace {

// A one-thread HTTP listener on 127.0.0.1 that answers every connection with `response` and
// counts the connections it accepted.
class LoopbackHttpServer {
public:
    LoopbackHttpServer() {
        fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        REQUIRE(fd_ >= 0);
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;
        REQUIRE(::bind(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0);
        socklen_t len = sizeof(addr);
        REQUIRE(::getsockname(fd_, reinterpret_cast<sockaddr*>(&addr), &len) == 0);
        port_ = ntohs(addr.sin_port);
        REQUIRE(::listen(fd_, 4) == 0);
    }

    ~LoopbackHttpServer() {
        stop_ = true;
        if (thread_.joinable()) {
            thread_.join();
        }
        ::close(fd_);
    }

    void serve(std::string response) {
        thread_ = std::thread([this, response = std::move(response)] {
            while (!stop_) {
                pollfd pfd{fd_, POLLIN, 0};
                if (::poll(&pfd, 1, 20) <= 0) {
                    continue;
                }
                const int client = ::accept(fd_, nullptr, nullptr);
                if (client < 0) {
                    continue;
                }
                ++connections_;
                char buffer[2048];
                (void)::recv(client, buffer, sizeof(buffer), 0);
                (void)::send(client, response.data(), response.size(), MSG_NOSIGNAL);
                ::close(client);
            }
        });
    }

    std::string url() const { return "http://127.0.0.1:" + std::to_string(port_); }
    int connections() const { return connections_.load(); }

private:
    int fd_{-1};
    std::uint16_t port_{0};
    std::atomic<bool> stop_{false};
    std::atomic<int> connections_{0};
    std::thread thread_;
};

} // namespace

TEST_CASE("Repo client does not follow redirects when TLS verification is off",
          "[plugins][repo-client][security]") {
    LoopbackHttpServer target;
    target.serve("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\n"
                 "Connection: close\r\n\r\n[]");
    LoopbackHttpServer redirector;
    redirector.serve("HTTP/1.1 302 Found\r\nLocation: " + target.url() +
                     "/elsewhere\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");

    PluginRepoConfig config;
    config.repoUrl = redirector.url();
    config.timeout = std::chrono::milliseconds{5000};

    SECTION("verification off: the redirect target is never contacted") {
        config.verifyTls = false;
        auto client = makePluginRepoClient(config);
        auto listed = client->list();
        CHECK_FALSE(listed.has_value());
        CHECK(redirector.connections() == 1);
        CHECK(target.connections() == 0);
    }

    SECTION("verification on: the redirect is followed (shows the listener would be reached)") {
        config.verifyTls = true;
        auto client = makePluginRepoClient(config);
        (void)client->list();
        CHECK(redirector.connections() == 1);
        CHECK(target.connections() == 1);
    }
}
#endif // !_WIN32

} // namespace yams::plugins::test
