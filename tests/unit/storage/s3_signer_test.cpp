#include <gtest/gtest.h>

#include <curl/curl.h>

#include <yams/storage/s3_signer.h>
#include <yams/storage/storage_backend.h>

using namespace yams::storage;

TEST(S3SignerTest, IncludesOptionalHeadersInSignatureAndList) {
    BackendConfig cfg;
    cfg.region = "us-east-1";
    cfg.credentials["access_key"] = "TESTACCESSKEY";
    cfg.credentials["secret_key"] = "TESTSECRETKEY";

    CURL* curl = curl_easy_init();
    ASSERT_NE(curl, nullptr);

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
    ASSERT_TRUE(res.has_value());

    // Walk header list and collect
    bool sawAuth = false, sawCT = false, sawSSE = false, sawSC = false;
    for (auto* h = res.value(); h != nullptr; h = h->next) {
        std::string line(h->data ? h->data : "");
        if (line.rfind("Authorization:", 0) == 0) {
            sawAuth = true;
            // Signed headers should include content-type
            ASSERT_NE(line.find("SignedHeaders="), std::string::npos);
            ASSERT_NE(line.find("content-type"), std::string::npos);
        }
        if (line.rfind("content-type:", 0) == 0 || line.rfind("Content-Type:", 0) == 0)
            sawCT = true;
        if (line.rfind("x-amz-server-side-encryption:", 0) == 0)
            sawSSE = true;
        if (line.rfind("x-amz-storage-class:", 0) == 0)
            sawSC = true;
    }

    EXPECT_TRUE(sawAuth);
    EXPECT_TRUE(sawCT);
    EXPECT_TRUE(sawSSE);
    EXPECT_TRUE(sawSC);

    curl_slist_free_all(res.value());
    curl_easy_cleanup(curl);
}
