// Runtime smoke test for the S3 object storage plugin.
// Skips when env is not configured.

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <random>
#include <string>
#include <vector>

#include <yams/storage/storage_backend.h>
#include <yams/storage/storage_backend_extended.h>

using namespace yams::storage;

int main() {
    const char* liveOptIn = std::getenv("YAMS_RUN_LIVE_S3_SMOKE");
    const char* bucket = std::getenv("S3_TEST_BUCKET");
    const char* regionEnv = std::getenv("AWS_REGION");
    const char* ak = std::getenv("AWS_ACCESS_KEY_ID");
    const char* sk = std::getenv("AWS_SECRET_ACCESS_KEY");
    const char* st = std::getenv("AWS_SESSION_TOKEN");
    const char* endpoint = std::getenv("S3_TEST_ENDPOINT"); // optional (for R2/minio)
    const char* pathStyle = std::getenv("S3_TEST_USE_PATH_STYLE");

    if (!liveOptIn || std::string(liveOptIn) != "1") {
        std::cout << "S3 plugin smoke: skipping; set YAMS_RUN_LIVE_S3_SMOKE=1 to enable live "
                     "networked object-storage validation\n";
        return 0;
    }

    if (!bucket || (!ak || !sk)) {
        std::cout << "S3 plugin smoke: env not set; skipping (need S3_TEST_BUCKET, "
                     "AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)\n";
        return 0; // skip silently
    }

    BackendConfig cfg;
    cfg.type = "s3";
    cfg.url = std::string("s3://") + bucket + "/yams-smoke/";
    cfg.region = regionEnv ? regionEnv : "us-east-1";
    cfg.usePathStyle = (pathStyle && std::string(pathStyle) == "1");
    cfg.credentials["access_key"] = ak;
    cfg.credentials["secret_key"] = sk;
    if (st)
        cfg.credentials["session_token"] = st;
    if (endpoint)
        cfg.credentials["endpoint"] = endpoint;
    cfg.requestTimeout = 15;

    auto backend = StorageBackendFactory::create(cfg);
    if (!backend) {
        std::cerr << "Failed to create S3 backend (plugin missing?)\n";
        return 1;
    }

    // Write a small object
    std::string key = "hello-" + std::to_string(std::random_device{}()) + ".txt";
    std::string content = "hello from yams s3 plugin\n";
    std::vector<std::byte> bytes(content.size());
    std::transform(content.begin(), content.end(), bytes.begin(),
                   [](char c) { return std::byte{static_cast<unsigned char>(c)}; });

    if (auto r = backend->store(key, bytes); !r) {
        std::cerr << "PUT failed: " << r.error().message << "\n";
        return 2;
    }

    auto fetched = backend->retrieve(key);
    if (!fetched) {
        std::cerr << "GET failed: " << fetched.error().message << "\n";
        return 6;
    }
    if (fetched.value().size() != bytes.size() ||
        !std::equal(fetched.value().begin(), fetched.value().end(), bytes.begin())) {
        std::cerr << "GET payload mismatch after PUT\n";
        return 7;
    }

    auto exists = backend->exists(key);
    if (!exists) {
        std::cerr << "HEAD failed: " << exists.error().message << "\n";
        return 3;
    }
    if (!exists.value()) {
        std::cerr << "Object not found after PUT\n";
        return 4;
    }

    auto listed = backend->list("hello-");
    if (!listed) {
        std::cerr << "LIST failed: " << listed.error().message << "\n";
        return 8;
    }
    if (std::find(listed.value().begin(), listed.value().end(), key) == listed.value().end()) {
        std::cerr << "LIST missing uploaded key\n";
        return 9;
    }

    if (auto del = backend->remove(key); !del) {
        std::cerr << "DELETE failed: " << del.error().message << "\n";
        return 5;
    }

    std::cout << "S3 plugin smoke: success\n";
    return 0;
}
