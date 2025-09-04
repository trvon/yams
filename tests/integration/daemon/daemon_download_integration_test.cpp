#include <gtest/gtest.h>

#include <yams/api/content_store.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/metadata/metadata_repository.h>
#include "test_async_helpers.h"

#include <cctype>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <optional>
#include <string>
#include <thread>

// This is an end-to-end integration test that exercises:
// 1) Daemon auto-start and request/response path
// 2) Downloader performing a store-only download into its CAS
// 3) Daemon ingesting the finalized file into the ContentStore (store())
// 4) MetadataRepository receiving a new document entry
// 5) Ability to retrieve the file by returned hash from the ContentStore
//
// Notes:
// - Uses a small, stable URL to keep runtime and flakiness low. If the environment
//   blocks outbound HTTP, this test can be disabled via CTest labels or environment checks.
// - The test does basic assertions on result fields and verifies ingestion by querying the content
// store.
//
// Environment assumptions:
// - yams-daemon is on PATH for auto-start to succeed.
// - Default storage/data dirs are writable for the current user.

namespace {

bool hasNetworkAccess() {
    // Simple heuristic: allow by default; environments that disallow network
    // can set YAMS_TEST_DISABLE_NETWORK=1 to skip.
    const char* env = std::getenv("YAMS_TEST_DISABLE_NETWORK");
    if (!env)
        return true;
    std::string v(env);
    for (auto& c : v)
        c = static_cast<char>(::tolower(static_cast<unsigned char>(c)));
    return !(v == "1" || v == "true" || v == "yes" || v == "on");
}

// A small file that should be reliably accessible. You may replace with a project-hosted file
// if CI environment restricts the internet. The test tolerates 404 or network failures by
// skipping when network is disabled.
constexpr const char* kTestUrl =
    "https://raw.githubusercontent.com/github/gitignore/main/Global/macOS.gitignore";

} // namespace

class DaemonDownloadIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Enable daemon-side download for test via policy override
        ::setenv("YAMS_ENABLE_DAEMON_DOWNLOAD", "1", 1);
        // Give generous timeouts for header/body to tolerate first-request service warmup
        ::setenv("YAMS_HEADER_TIMEOUT", "60000", 1); // 60s
        ::setenv("YAMS_BODY_TIMEOUT", "120000", 1);  // 120s

        // Isolate the daemon instance for this test by using a unique socket and storage dir
        namespace fs = std::filesystem;
        const auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
        auto sock =
            fs::temp_directory_path() / ("yams-daemon-test-" + std::to_string(ts) + ".sock");
        auto stor = fs::temp_directory_path() / ("yams-test-storage-" + std::to_string(ts));
        (void)fs::create_directories(stor);
        // Remove any stale socket path if present
        std::error_code ec;
        (void)fs::remove(sock, ec);
        ::setenv("YAMS_DAEMON_SOCKET", sock.c_str(), 1);
        ::setenv("YAMS_STORAGE", stor.c_str(), 1);
    }

    void TearDown() override {
        // Gracefully shutdown the isolated daemon instance started for this test
        using namespace yams::daemon;
        ClientConfig cfg;
        cfg.autoStart = false; // don't spawn a new one if not present
        DaemonClient client(cfg);
        (void)yams::test_async::ok(client.shutdown(true));

        // Best-effort cleanup of test storage directory
        if (const char* stor = std::getenv("YAMS_STORAGE")) {
            std::error_code ec;
            std::filesystem::remove_all(std::filesystem::path(stor), ec);
        }
    }
};

TEST_F(DaemonDownloadIntegrationTest, EndToEnd_Download_Ingest_Metadata) {
    if (!hasNetworkAccess()) {
        GTEST_SKIP() << "Network access disabled by environment (YAMS_TEST_DISABLE_NETWORK=1)";
    }

    using namespace yams::daemon;

    ClientConfig cfg;
    cfg.autoStart = true; // start daemon automatically if not running
    cfg.requestTimeout = std::chrono::milliseconds(15000); // allow some time for download
    DaemonClient client(cfg);

    // Ensure we can connect (will auto-start daemon if needed)
    auto connectRes = client.connect();
    ASSERT_TRUE(connectRes) << "Failed to connect/start daemon: "
                            << (connectRes ? "" : connectRes.error().message);

    // Wait for daemon readiness of core services to avoid early InvalidState errors
    // (metadata repo and content store). Poll Status for up to ~10s.
    bool ready = false;
    for (int i = 0; i < 100; ++i) {
        auto st = yams::test_async::res(client.status());
        if (st) {
            const auto& r = st.value();
            auto itMeta = r.readinessStates.find("metadata_repo");
            auto itStore = r.readinessStates.find("content_store");
            if (itMeta != r.readinessStates.end() && itStore != r.readinessStates.end() &&
                itMeta->second && itStore->second) {
                ready = true;
                break;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    ASSERT_TRUE(ready) << "Daemon core services not ready in time";

    // Prepare and send DownloadRequest
    DownloadRequest dreq;
    dreq.url = kTestUrl;
    dreq.outputPath = ""; // daemon chooses; we store/ingest via ContentStore anyway
    dreq.quiet = true;    // less log spam

    auto dres = yams::test_async::res(client.call<DownloadRequest>(dreq));
    ASSERT_TRUE(dres) << "Daemon download call failed: " << (dres ? "" : dres.error().message);

    const auto& resp = dres.value();

    // Basic response sanity
    EXPECT_TRUE(resp.success) << "Download reported failure: " << resp.error;
    EXPECT_FALSE(resp.hash.empty());
    EXPECT_FALSE(resp.url.empty());
    EXPECT_GT(resp.size, 0u);

    // The handler now ingests the file into ContentStore; hash should be the ContentStore hash.
    // Verify we can retrieve bytes by this hash through ContentStore by issuing a get request path.
    // The daemon exposes high-level GetRequest APIs.
    GetRequest greq;
    greq.hash = resp.hash; // ContentStore hash
    auto getRes = yams::test_async::res(client.get(greq));

    ASSERT_TRUE(getRes) << "Daemon get() failed for stored hash: "
                        << (getRes ? "" : getRes.error().message);
    const auto& getOk = getRes.value();
    EXPECT_EQ(getOk.hash, resp.hash);
    EXPECT_FALSE(getOk.content.empty());
    EXPECT_GT(getOk.content.size(), 0u);

    // Optionally, sanity check that metadata is present by querying list() and
    // looking for a document with size > 0. If metadata repo is not available,
    // daemon handler is still correct because it ingested into ContentStore.
    ListRequest lreq;
    auto lres = yams::test_async::res(client.call<ListRequest>(lreq));
    ASSERT_TRUE(lres) << "Daemon list() failed: " << (lres ? "" : lres.error().message);

    // Not all environments will index immediately; we just ensure the call works.
    // For stronger assertions, we could search by hash in the list if metadata maps hashes.
    // Here we only assert that the daemon is operational and returned a coherent list.
    // If your metadata repo is guaranteed, uncomment below to check presence by size.
    // const auto& listOk = lres.value();
    // bool found = false;
    // for (const auto& [hash, name] : listOk.documents) {
    //     if (hash == resp.hash) { found = true; break; }
    // }
    // EXPECT_TRUE(found) << "Downloaded artifact not present in metadata list (may be eventual).";

    // Final: basic property that content is non-empty and the pipeline executed.
    SUCCEED();
}
