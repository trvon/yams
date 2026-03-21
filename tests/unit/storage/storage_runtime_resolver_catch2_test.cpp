#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>

#include <yams/storage/storage_runtime_resolver.h>

namespace {

class TempDir {
public:
    TempDir() {
        path_ = std::filesystem::temp_directory_path() /
                ("yams_storage_runtime_test_" +
                 std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        std::filesystem::create_directories(path_);
    }

    ~TempDir() {
        std::error_code ec;
        std::filesystem::remove_all(path_, ec);
    }

    const std::filesystem::path& path() const { return path_; }

private:
    std::filesystem::path path_;
};

} // namespace

TEST_CASE("Storage runtime resolver defaults to local", "[storage][runtime][resolver][catch2]") {
    TempDir td;
    const auto configPath = td.path() / "config.toml";
    {
        std::ofstream out(configPath);
        out << "[storage]\n";
        out << "engine = \"local\"\n";
    }

    auto decision =
        yams::storage::resolveStorageBootstrapDecision(configPath, td.path() / "primary-data");
    REQUIRE(decision.has_value());
    CHECK(decision.value().configuredEngine == "local");
    CHECK(decision.value().activeEngine == "local");
    CHECK_FALSE(decision.value().fallbackTriggered);
    CHECK_FALSE(decision.value().storageEngineOverride);
}

TEST_CASE("Storage runtime resolver fails strict S3 when URL missing",
          "[storage][runtime][resolver][catch2]") {
    TempDir td;
    const auto configPath = td.path() / "config.toml";
    {
        std::ofstream out(configPath);
        out << "[storage]\n";
        out << "engine = \"s3\"\n";
        out << "[storage.s3]\n";
        out << "fallback_policy = \"strict\"\n";
    }

    auto decision =
        yams::storage::resolveStorageBootstrapDecision(configPath, td.path() / "primary-data");
    REQUIRE_FALSE(decision.has_value());
    CHECK(decision.error().message.find("storage.s3.url") != std::string::npos);
}

TEST_CASE("Storage runtime resolver falls back to configured local data dir",
          "[storage][runtime][resolver][catch2]") {
    TempDir td;
    const auto configPath = td.path() / "config.toml";
    const auto fallbackDir = td.path() / "fallback-data";
    {
        std::ofstream out(configPath);
        out << "[storage]\n";
        out << "engine = \"s3\"\n";
        out << "[storage.s3]\n";
        out << "fallback_policy = \"fallback_local_if_configured\"\n";
        out << "fallback_local_data_dir = \"" << fallbackDir.string() << "\"\n";
    }

    auto decision =
        yams::storage::resolveStorageBootstrapDecision(configPath, td.path() / "primary-data");
    REQUIRE(decision.has_value());
    CHECK(decision.value().configuredEngine == "s3");
    CHECK(decision.value().activeEngine == "local");
    CHECK(decision.value().fallbackTriggered);
    CHECK(decision.value().activeDataDir == fallbackDir);
    CHECK(decision.value().fallbackReason.find("storage.s3.url") != std::string::npos);
}

TEST_CASE("Storage runtime resolver requires fallback local dir when fallback policy enabled",
          "[storage][runtime][resolver][catch2]") {
    TempDir td;
    const auto configPath = td.path() / "config.toml";
    {
        std::ofstream out(configPath);
        out << "[storage]\n";
        out << "engine = \"s3\"\n";
        out << "[storage.s3]\n";
        out << "fallback_policy = \"fallback_local_if_configured\"\n";
    }

    auto decision =
        yams::storage::resolveStorageBootstrapDecision(configPath, td.path() / "primary-data");
    REQUIRE_FALSE(decision.has_value());
    CHECK(decision.error().message.find("fallback_local_data_dir") != std::string::npos);
}

TEST_CASE("Storage runtime resolver supports flattened storage.s3 keys",
          "[storage][runtime][resolver][catch2]") {
    TempDir td;
    const auto configPath = td.path() / "config.toml";
    const auto fallbackDir = td.path() / "flat-fallback";
    {
        std::ofstream out(configPath);
        out << "[storage]\n";
        out << "engine = \"s3\"\n";
        out << "s3.fallback_policy = \"fallback_local_if_configured\"\n";
        out << "s3.fallback_local_data_dir = \"" << fallbackDir.string() << "\"\n";
    }

    auto decision =
        yams::storage::resolveStorageBootstrapDecision(configPath, td.path() / "primary-data");
    REQUIRE(decision.has_value());
    CHECK(decision.value().activeEngine == "local");
    CHECK(decision.value().fallbackTriggered);
    CHECK(decision.value().activeDataDir == fallbackDir);
}

TEST_CASE("Storage runtime resolver detects Cloudflare token-shaped access key in direct mode",
          "[storage][runtime][resolver][r2][catch2]") {
    TempDir td;
    const auto configPath = td.path() / "config.toml";
    {
        std::ofstream out(configPath);
        out << "[storage]\n";
        out << "engine = \"s3\"\n";
        out << "[storage.s3]\n";
        out << "url = \"s3://bucket/prefix\"\n";
        out << "endpoint = \"00000000000000000000000000000000.r2.cloudflarestorage.com\"\n";
        out << "access_key = \"cf-test-token-not-a-real-r2-access-key-0001\"\n";
    }

    auto decision =
        yams::storage::resolveStorageBootstrapDecision(configPath, td.path() / "primary-data");
    REQUIRE_FALSE(decision.has_value());
    CHECK(decision.error().message.find("bearer token") != std::string::npos);
}

TEST_CASE("Storage runtime resolver validates temp credential mode requires R2 endpoint",
          "[storage][runtime][resolver][r2][catch2]") {
    TempDir td;
    const auto configPath = td.path() / "config.toml";

#if defined(_WIN32)
    _putenv_s("YAMS_R2_API_TOKEN", "");
    _putenv_s("CLOUDFLARE_API_TOKEN", "");
#else
    setenv("YAMS_R2_API_TOKEN", "", 1);
    setenv("CLOUDFLARE_API_TOKEN", "", 1);
#endif

    {
        std::ofstream out(configPath);
        out << "[storage]\n";
        out << "engine = \"s3\"\n";
        out << "[storage.s3]\n";
        out << "url = \"s3://yams-s3-smoke-personal/prefix\"\n";
        out << "endpoint = \"s3.us-east-1.amazonaws.com\"\n";
        out << "[storage.s3.r2]\n";
        out << "auth_mode = \"temp_credentials\"\n";
    }

    auto decision =
        yams::storage::resolveStorageBootstrapDecision(configPath, td.path() / "primary-data");
    REQUIRE_FALSE(decision.has_value());
    CHECK(decision.error().message.find("R2 endpoint") != std::string::npos);
}

TEST_CASE("Storage runtime resolver auto-resolves parent access key id in temp mode",
          "[storage][runtime][resolver][r2][catch2]") {
    TempDir td;
    const auto configPath = td.path() / "config.toml";
    {
        std::ofstream out(configPath);
        out << "[storage]\n";
        out << "engine = \"s3\"\n";
        out << "[storage.s3]\n";
        out << "url = \"s3://yams-s3-smoke-personal/prefix\"\n";
        out << "endpoint = \"00000000000000000000000000000000.r2.cloudflarestorage.com\"\n";
        out << "[storage.s3.r2]\n";
        out << "auth_mode = \"temp_credentials\"\n";
        out << "api_token = \"dummy-token-value-not-used-by-preflight\"\n";
    }

    auto decision =
        yams::storage::resolveStorageBootstrapDecision(configPath, td.path() / "primary-data");
    REQUIRE_FALSE(decision.has_value());
    CHECK(decision.error().message.find("resolve Cloudflare token id") != std::string::npos);
}

TEST_CASE("Storage runtime resolver rejects mismatched explicit account id and endpoint",
          "[storage][runtime][resolver][r2][catch2]") {
    TempDir td;
    const auto configPath = td.path() / "config.toml";
    {
        std::ofstream out(configPath);
        out << "[storage]\n";
        out << "engine = \"s3\"\n";
        out << "[storage.s3]\n";
        out << "url = \"s3://yams-s3-smoke-personal/prefix\"\n";
        out << "endpoint = \"00000000000000000000000000000000.r2.cloudflarestorage.com\"\n";
        out << "[storage.s3.r2]\n";
        out << "auth_mode = \"temp_credentials\"\n";
        out << "account_id = \"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"\n";
        out << "api_token = \"dummy-token-value-not-used-by-preflight\"\n";
    }

    auto decision =
        yams::storage::resolveStorageBootstrapDecision(configPath, td.path() / "primary-data");
    REQUIRE_FALSE(decision.has_value());
    CHECK(decision.error().message.find("does not match") != std::string::npos);
}

TEST_CASE("Storage runtime resolver helper identifies Cloudflare endpoint and token pattern",
          "[storage][runtime][resolver][r2][catch2]") {
    CHECK(yams::storage::isCloudflareR2EndpointHost(
        "00000000000000000000000000000000.r2.cloudflarestorage.com"));
    CHECK(yams::storage::extractCloudflareR2AccountId(
              "https://00000000000000000000000000000000.r2.cloudflarestorage.com/path") ==
          "00000000000000000000000000000000");
    CHECK(yams::storage::looksLikeCloudflareApiBearerToken(
        "cf-test-token-not-a-real-r2-access-key-0001"));
    CHECK_FALSE(yams::storage::looksLikeCloudflareApiBearerToken("AKIAIOSFODNN7EXAMPLE"));
}

TEST_CASE("Storage runtime resolver keychain helper is platform-aware",
          "[storage][runtime][resolver][r2][keychain][catch2]") {
#if defined(__APPLE__)
    // Use a deterministic account id that should not exist in normal environments.
    auto token =
        yams::storage::loadCloudflareApiTokenFromKeychain("ffffffffffffffffffffffffffffffff");
    REQUIRE_FALSE(token.has_value());
    CHECK((token.error().code == yams::ErrorCode::NotFound ||
           token.error().code == yams::ErrorCode::PermissionDenied ||
           token.error().code == yams::ErrorCode::InvalidData));
#else
    auto token =
        yams::storage::loadCloudflareApiTokenFromKeychain("00000000000000000000000000000000");
    REQUIRE_FALSE(token.has_value());
    CHECK(token.error().code == yams::ErrorCode::NotSupported);

    auto store = yams::storage::storeCloudflareApiTokenInKeychain(
        "377dc8ebb0de866fdec6be62f070405d", "dummy-token");
    REQUIRE_FALSE(store.has_value());
    CHECK(store.error().code == yams::ErrorCode::NotSupported);
#endif
}
