// pi-lens-ignore: fatal error
#include <catch2/catch_test_macros.hpp>

#include <curl/curl.h>

#include <array>
#include <cstdlib>
#include <cstring>
#include <string>
#include <string_view>

#include <yams/crypto/hasher.h>
#include <yams/storage/s3_signer.h>
#include <yams/storage/storage_backend.h>

#include "../../common/test_helpers_catch2.h"

using namespace yams::storage;

namespace {

std::string sha256Hex(std::string_view data) {
    yams::crypto::SHA256Hasher hasher;
    hasher.init();
    hasher.update(
        std::span<const std::byte>(reinterpret_cast<const std::byte*>(data.data()), data.size()));
    return hasher.finalize();
}

std::string unhex(const std::string& hex) {
    std::string out;
    out.reserve(hex.size() / 2);
    for (std::size_t i = 0; i + 1 < hex.size(); i += 2) {
        auto nibble = [](char c) -> unsigned char {
            if (c >= '0' && c <= '9')
                return static_cast<unsigned char>(c - '0');
            if (c >= 'a' && c <= 'f')
                return static_cast<unsigned char>(c - 'a' + 10);
            return static_cast<unsigned char>(c - 'A' + 10);
        };
        out.push_back(static_cast<char>((nibble(hex[i]) << 4) | nibble(hex[i + 1])));
    }
    return out;
}

// Independent HMAC-SHA256 over the hex-string SHA256Hasher, so the S3 signer test does not
// reuse the production signing implementation.
std::string hmacSha256Hex(std::string_view key, std::string_view data) {
    constexpr std::size_t kBlock = 64;
    std::array<unsigned char, kBlock> kblock{};
    if (key.size() <= kBlock) {
        std::memcpy(kblock.data(), key.data(), key.size());
    } else {
        std::string keyHash = unhex(sha256Hex(key));
        std::memcpy(kblock.data(), keyHash.data(), keyHash.size());
    }

    std::string inner(kBlock + data.size(), '\0');
    for (std::size_t i = 0; i < kBlock; ++i) {
        inner[i] = static_cast<char>(kblock[i] ^ 0x36);
    }
    std::memcpy(inner.data() + kBlock, data.data(), data.size());
    std::string innerHash = unhex(sha256Hex(inner));

    std::string outer(kBlock + innerHash.size(), '\0');
    for (std::size_t i = 0; i < kBlock; ++i) {
        outer[i] = static_cast<char>(kblock[i] ^ 0x5c);
    }
    std::memcpy(outer.data() + kBlock, innerHash.data(), innerHash.size());
    return sha256Hex(outer);
}

struct OracleUrl {
    std::string host;
    std::string path; // begins with '/', includes query when present
};

OracleUrl splitOracleUrl(const std::string& url) {
    OracleUrl out;
    auto scheme = url.find("://");
    if (scheme == std::string::npos) {
        return out;
    }
    auto rest = url.substr(scheme + 3);
    auto slash = rest.find('/');
    if (slash == std::string::npos) {
        out.host = rest;
        out.path = "/";
    } else {
        out.host = rest.substr(0, slash);
        out.path = rest.substr(slash);
    }
    return out;
}

// Replicates the SigV4 string-to-sign with canonicalURI = the wire path exactly as the caller
// supplied it (already RFC3986-encoded). This is the MinIO/AWS-SDK behavior the signer must match.
std::string expectedSignature(const std::string& method, const std::string& url,
                              std::string_view payloadHex, const std::string& accessKey,
                              const std::string& secretKey, const std::string& region,
                              const std::string& amzDate) {
    const std::string ymd = amzDate.substr(0, 8);
    const auto parsed = splitOracleUrl(url);

    std::vector<std::pair<std::string, std::string>> hdrs{
        {"host", parsed.host},
        {"x-amz-content-sha256", std::string(payloadHex)},
        {"x-amz-date", amzDate},
    };
    std::sort(hdrs.begin(), hdrs.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });

    std::string canonicalHeaders;
    std::string signedHeaders;
    for (std::size_t i = 0; i < hdrs.size(); ++i) {
        canonicalHeaders += hdrs[i].first + ":" + hdrs[i].second + "\n";
        signedHeaders += hdrs[i].first;
        if (i + 1 < hdrs.size()) {
            signedHeaders += ";";
        }
    }
    // No query string in the exercised URLs: the canonical query line stays empty.
    const std::string canonicalRequest = method + "\n" + parsed.path + "\n\n" + canonicalHeaders +
                                         "\n" + signedHeaders + "\n" + std::string(payloadHex);
    const std::string canonicalRequestHash = sha256Hex(canonicalRequest);
    const std::string stringToSign = "AWS4-HMAC-SHA256\n" + amzDate + "\n" + ymd + "/" + region +
                                     "/s3/aws4_request\n" + canonicalRequestHash;
    const std::string kSecret = "AWS4" + secretKey;
    const std::string kDate = unhex(hmacSha256Hex(kSecret, ymd));
    const std::string kRegion = unhex(hmacSha256Hex(kDate, region));
    const std::string kService = unhex(hmacSha256Hex(kRegion, "s3"));
    const std::string kSigning = unhex(hmacSha256Hex(kService, "aws4_request"));
    return hmacSha256Hex(kSigning, stringToSign);
}

std::string authorizationSignature(const curl_slist* headers) {
    for (const auto* h = headers; h != nullptr; h = h->next) {
        std::string line(h->data ? h->data : "");
        if (line.rfind("Authorization:", 0) != 0) {
            continue;
        }
        auto sig = line.find("Signature=");
        if (sig == std::string::npos) {
            return {};
        }
        return line.substr(sig + 10);
    }
    return {};
}

} // namespace

TEST_CASE("S3Signer includes optional headers in signature and list",
          "[storage][s3][signer][catch2]") {
    BackendConfig cfg;
    cfg.region = "us-east-1";
    cfg.credentials["access_key"] = "TESTACCESSKEY";
    cfg.credentials["secret_key"] = "TESTSECRETKEY";

    CURL* curl = curl_easy_init();
    REQUIRE(curl != nullptr);

    std::string url = "https://s3.amazonaws.com/test-bucket/test-object?uploads=";
    std::string payloadStr = "<CompleteMultipartUpload/>";
    auto payload = std::span<const std::byte>(reinterpret_cast<const std::byte*>(payloadStr.data()),
                                              payloadStr.size());
    std::vector<std::pair<std::string, std::string>> extra{
        {"content-type", "application/xml"},
        {"x-amz-server-side-encryption", "aws:kms"},
        {"x-amz-storage-class", "STANDARD"},
    };

    auto res = S3Signer::signRequest(curl, cfg, "POST", url, payload, extra);
    REQUIRE(res.has_value());

    // Walk header list and collect
    bool sawAuth = false, sawCT = false, sawSSE = false, sawSC = false;
    for (auto* h = res.value(); h != nullptr; h = h->next) {
        std::string line(h->data ? h->data : "");
        if (line.rfind("Authorization:", 0) == 0) {
            sawAuth = true;
            // Signed headers should include content-type
            REQUIRE(line.find("SignedHeaders=") != std::string::npos);
            REQUIRE(line.find("content-type") != std::string::npos);
        }
        if (line.rfind("content-type:", 0) == 0 || line.rfind("Content-Type:", 0) == 0)
            sawCT = true;
        if (line.rfind("x-amz-server-side-encryption:", 0) == 0)
            sawSSE = true;
        if (line.rfind("x-amz-storage-class:", 0) == 0)
            sawSC = true;
    }

    CHECK(sawAuth);
    CHECK(sawCT);
    CHECK(sawSSE);
    CHECK(sawSC);

    curl_slist_free_all(res.value());
    curl_easy_cleanup(curl);
}

TEST_CASE("S3Signer trims credential and region whitespace", "[storage][s3][signer][catch2]") {
    BackendConfig cfg;
    cfg.region = "  us-east-1\n";
    cfg.credentials["access_key"] = "  TESTACCESSKEY\t";
    cfg.credentials["secret_key"] = "\nTESTSECRETKEY  ";

    CURL* curl = curl_easy_init();
    REQUIRE(curl != nullptr);

    std::string url = "https://s3.amazonaws.com/test-bucket/test-object";
    std::string payloadStr = "payload";
    auto payload = std::span<const std::byte>(reinterpret_cast<const std::byte*>(payloadStr.data()),
                                              payloadStr.size());

    auto res = S3Signer::signRequest(curl, cfg, "PUT", url, payload);
    REQUIRE(res.has_value());

    bool sawAuth = false;
    for (auto* h = res.value(); h != nullptr; h = h->next) {
        std::string line(h->data ? h->data : "");
        if (line.rfind("Authorization:", 0) == 0) {
            sawAuth = true;
            CHECK(line.find("Credential=TESTACCESSKEY/") != std::string::npos);
            CHECK(line.find("\n") == std::string::npos);
            CHECK(line.find("\t") == std::string::npos);
        }
    }

    CHECK(sawAuth);

    curl_slist_free_all(res.value());
    curl_easy_cleanup(curl);
}

TEST_CASE("S3Signer canonical request includes header separator newline",
          "[storage][s3][signer][regression][catch2]") {
    yams::test::ScopedEnvVar fixedDate{"YAMS_S3_SIGNER_FIXED_AMZ_DATE",
                                       std::string{"20250306T120000Z"}};

    BackendConfig cfg;
    cfg.region = "us-east-1";
    cfg.credentials["access_key"] = "TESTACCESSKEY";
    cfg.credentials["secret_key"] = "TESTSECRETKEY";

    CURL* curl = curl_easy_init();
    REQUIRE(curl != nullptr);

    std::string url = "https://bucket.example.com/test.txt";
    std::string payloadStr = "hello";
    auto payload = std::span<const std::byte>(reinterpret_cast<const std::byte*>(payloadStr.data()),
                                              payloadStr.size());

    auto res = S3Signer::signRequest(curl, cfg, "PUT", url, payload);
    REQUIRE(res.has_value());

    std::string authLine;
    std::string dateLine;
    for (auto* h = res.value(); h != nullptr; h = h->next) {
        std::string line(h->data ? h->data : "");
        if (line.rfind("Authorization:", 0) == 0) {
            authLine = line;
        }
        if (line.rfind("x-amz-date:", 0) == 0) {
            dateLine = line;
        }
    }

    CHECK(dateLine == "x-amz-date: 20250306T120000Z");
    CHECK(authLine.find("Credential=TESTACCESSKEY/20250306/us-east-1/s3/aws4_request") !=
          std::string::npos);
    CHECK(authLine.find(
              "Signature=8611799d0306920216eb826af8f119522e26cb25ec5b6d3f1ea63958a45b8991") !=
          std::string::npos);

    curl_slist_free_all(res.value());
    curl_easy_cleanup(curl);
}

TEST_CASE("S3Signer signs the wire path without double-encoding",
          "[storage][s3][signer][regression][catch2]") {
    yams::test::ScopedEnvVar fixedDate{"YAMS_S3_SIGNER_FIXED_AMZ_DATE",
                                       std::string{"20250306T120000Z"}};

    BackendConfig cfg;
    cfg.region = "us-east-1";
    cfg.credentials["access_key"] = "TESTACCESSKEY";
    cfg.credentials["secret_key"] = "TESTSECRETKEY";

    const std::string payloadStr = "hello";
    const std::string payloadHex = sha256Hex(payloadStr);
    const std::string amzDate = "20250306T120000Z";

    // Sanity-check the independent oracle against the known-good simple-path signature.
    {
        const std::string simpleUrl = "https://bucket.example.com/test.txt";
        CURL* curl = curl_easy_init();
        REQUIRE(curl != nullptr);
        auto simple = S3Signer::signRequest(
            curl, cfg, "PUT", simpleUrl,
            std::span<const std::byte>(reinterpret_cast<const std::byte*>(payloadStr.data()),
                                       payloadStr.size()));
        REQUIRE(simple.has_value());
        const std::string knownGood =
            "8611799d0306920216eb826af8f119522e26cb25ec5b6d3f1ea63958a45b8991";
        CHECK(authorizationSignature(simple.value()) == knownGood);
        CHECK(authorizationSignature(simple.value()) ==
              expectedSignature("PUT", simpleUrl, payloadHex, "TESTACCESSKEY", "TESTSECRETKEY",
                                "us-east-1", amzDate));
        curl_slist_free_all(simple.value());
        curl_easy_cleanup(curl);
    }

    // The wire path already contains percent-encoded bytes (e.g. a memory-sync user key that
    // escaped '/' as %2F, then the backend encoded '%' as %25). The canonical URI must match the
    // path as sent; re-encoding it produces a signature MinIO rejects with 403.
    const std::string encodedUrl = "https://s3.example.com/bucket/path%2Fto/obj%20name";
    CURL* curl = curl_easy_init();
    REQUIRE(curl != nullptr);
    auto res = S3Signer::signRequest(
        curl, cfg, "PUT", encodedUrl,
        std::span<const std::byte>(reinterpret_cast<const std::byte*>(payloadStr.data()),
                                   payloadStr.size()));
    REQUIRE(res.has_value());

    CHECK(authorizationSignature(res.value()) == expectedSignature("PUT", encodedUrl, payloadHex,
                                                                   "TESTACCESSKEY", "TESTSECRETKEY",
                                                                   "us-east-1", amzDate));

    curl_slist_free_all(res.value());
    curl_easy_cleanup(curl);
}

TEST_CASE("S3Signer rejects Cloudflare bearer token shape for R2 access key",
          "[storage][s3][signer][r2][catch2]") {
    BackendConfig cfg;
    cfg.region = "auto";
    cfg.credentials["access_key"] = "cf-test-token-not-a-real-r2-access-key-0001";
    cfg.credentials["secret_key"] = "dummy-secret-that-would-otherwisetrytosign";

    CURL* curl = curl_easy_init();
    REQUIRE(curl != nullptr);

    std::string url =
        "https://00000000000000000000000000000000.r2.cloudflarestorage.com/bucket/key";
    auto res = S3Signer::signRequest(curl, cfg, "GET", url, {});
    REQUIRE_FALSE(res.has_value());
    CHECK(res.error().message.find("bearer token") != std::string::npos);

    curl_easy_cleanup(curl);
}
