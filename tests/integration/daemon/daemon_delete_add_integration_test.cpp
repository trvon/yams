#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <random>
#include <string>
#include <vector>
#include <gtest/gtest.h>

#include "test_async_helpers.h"
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/daemon.h>
#include <yams/daemon/ipc/ipc_protocol.h>

namespace fs = std::filesystem;
using namespace std::chrono_literals;
using yams::daemon::DaemonClient;
using yams::daemon::DaemonConfig;
using yams::daemon::YamsDaemon;

class DaemonAddDeleteIntegrationTest : public ::testing::Test {
protected:
    fs::path testRoot_;
    fs::path storageDir_;
    fs::path xdgRuntimeDir_;
    fs::path sourceDir_;
    fs::path yamsBinary_;
    DaemonConfig daemonConfig_{};
    std::unique_ptr<YamsDaemon> daemon_;
    yams::daemon::ClientConfig clientConfig_{};

    void SetUp() override {
        // Create isolated test directories
        auto unique = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        testRoot_ = fs::temp_directory_path() / ("yams_daemon_delete_add_it_" + unique);
        storageDir_ = testRoot_ / "storage";
        xdgRuntimeDir_ = testRoot_ / "xdg-runtime";
        sourceDir_ = testRoot_ / "source";
        fs::create_directories(storageDir_);
        fs::create_directories(xdgRuntimeDir_);
        fs::create_directories(sourceDir_);

        // Discover CLI binary (prefer build tree; fallback to PATH). If not present, skip tests.
        yamsBinary_ = fs::current_path() / "tools" / "yams-cli" / "yams";
        if (!fs::exists(yamsBinary_)) {
            yamsBinary_ = fs::current_path() / "yams";
        }
        if (!fs::exists(yamsBinary_)) {
            if (const char* pathEnv = std::getenv("PATH")) {
                std::string path(pathEnv);
#ifdef _WIN32
                const char delim = ';';
#else
                const char delim = ':';
#endif
                size_t start = 0;
                while (start <= path.size()) {
                    size_t end = path.find(delim, start);
                    std::string dir = path.substr(
                        start, end == std::string::npos ? std::string::npos : end - start);
                    if (!dir.empty()) {
                        fs::path candidate = fs::path(dir) / "yams";
                        if (fs::exists(candidate) && fs::is_regular_file(candidate)) {
                            yamsBinary_ = candidate;
                            break;
                        }
                    }
                    if (end == std::string::npos)
                        break;
                    start = end + 1;
                }
            }
        }
        if (!fs::exists(yamsBinary_)) {
            GTEST_SKIP() << "YAMS CLI binary not found in build tree or PATH; skipping tests.";
        }

        // Configure daemon to use XDG_RUNTIME_DIR socket path so CLI will find it automatically
        // DaemonClient::resolveSocketPath will use XDG_RUNTIME_DIR/yams-daemon.sock (set
        // per-command)
        daemonConfig_.socketPath = xdgRuntimeDir_ / "yams-daemon.sock";
        daemonConfig_.pidFile = testRoot_ / "daemon.pid";
        daemonConfig_.logFile = testRoot_ / "daemon.log";
        daemonConfig_.dataDir = storageDir_;

        // Client connects to same socket and does not autostart (we manage lifecycle here)
        clientConfig_.socketPath = daemonConfig_.socketPath;
        clientConfig_.autoStart = false;

        // Start daemon
        daemon_ = std::make_unique<YamsDaemon>(daemonConfig_);
        auto started = daemon_->start();
        ASSERT_TRUE(started) << "Failed to start daemon: " << started.error().message;
        std::this_thread::sleep_for(200ms);
    }

    void TearDown() override {
        if (daemon_) {
            daemon_->stop();
        }
        std::error_code ec;
        fs::remove_all(testRoot_, ec);
    }

    static std::string runCommandWithEnv(const fs::path& bin, const fs::path& storageDir,
                                         const fs::path& xdgRuntimeDir, const std::string& args,
                                         bool expectSuccess = true) {
        // If CLI is not available, return empty output; test setup should have already handled
        // skips.
        if (!fs::exists(bin)) {
            return {};
        }
        // Ensure XDG_RUNTIME_DIR and storage point to our isolated test dirs
        std::string cmd = "XDG_RUNTIME_DIR=\"" + xdgRuntimeDir.string() + "\" " +
                          "YAMS_STORAGE=\"" + storageDir.string() + "\" " + bin.string() +
                          " --storage " + storageDir.string() + " " + args + " 2>&1";

        FILE* pipe = popen(cmd.c_str(), "r");
        EXPECT_NE(pipe, nullptr) << "Failed to run command: " << cmd;

        std::string output;
        char buffer[256];
        while (pipe && fgets(buffer, sizeof(buffer), pipe) != nullptr) {
            output += buffer;
        }

        int exitCode = pclose(pipe);
        if (expectSuccess) {
            EXPECT_EQ(exitCode, 0) << "Command failed: " << cmd << "\nOutput:\n" << output;
        }
        return output;
    }

    static void writeFile(const fs::path& p, std::string_view content) {
        fs::create_directories(p.parent_path());
        std::ofstream f(p);
        f << content;
        f.close();
    }

    static bool containsName(const yams::daemon::ListResponse& lr, const std::string& name) {
        for (const auto& e : lr.items) {
            if (e.name == name)
                return true;
        }
        return false;
    }

    static bool containsPath(const yams::daemon::ListResponse& lr, const std::string& path) {
        for (const auto& e : lr.items) {
            if (e.path == path)
                return true;
        }
        return false;
    }

    yams::daemon::ListResponse listAllViaDaemon(size_t limit = 1000) {
        DaemonClient client(clientConfig_);
        auto conn = client.connect();
        if (!conn) {
            yams::daemon::ListResponse empty{};
            return empty;
        }
        yams::daemon::ListRequest req;
        req.limit = limit;
        req.recent = false;
        auto lr = yams::test_async::res(client.call<yams::daemon::ListRequest>(req));
        if (!lr) {
            yams::daemon::ListResponse empty{};
            return empty;
        }
        return lr.value();
    }
};

// Add: relative path normalization with daemon (CLI should convert to absolute)
// Then verify via daemon list.
TEST_F(DaemonAddDeleteIntegrationTest, AddRelativePathNormalizedWithDaemon) {
    // Create file with relative path
    auto rel = sourceDir_ / "rel_add.txt";
    writeFile(rel, "relative add content");

    // Use CLI with daemon running; CLI should convert relative path to absolute for the daemon
    std::string args = "add " + (sourceDir_.filename() / fs::path("rel_add.txt")).string();
    // Using relative path "source/rel_add.txt" from testRoot_ working dir
    // Provide path relative to current process working dir: use "source/rel_add.txt"
    auto relFromCwd = (sourceDir_.parent_path().filename() / fs::path("source/rel_add.txt"))
                          .string(); // not reliable across cwd
    // Simpler: build relative to testRoot_ directly
    std::string relArg = (fs::path("source") / "rel_add.txt").string();
    auto out = runCommandWithEnv(yamsBinary_, storageDir_, xdgRuntimeDir_, "add " + relArg, true);
    (void)out;

    // Verify via daemon list
    auto lr = listAllViaDaemon();
    EXPECT_TRUE(containsName(lr, "rel_add.txt")) << "Daemon did not register added file by name";
}

// Delete by hash via daemon (daemon-only parity)
TEST_F(DaemonAddDeleteIntegrationTest, DeleteByHash_DaemonOnly) {
    // Prepare a file to add
    auto f = sourceDir_ / "hash_delete.txt";
    writeFile(f, "delete by hash");

    // Add directly via daemon client to get the hash
    DaemonClient client(clientConfig_);
    ASSERT_TRUE(client.connect());
    yams::daemon::AddDocumentRequest addReq;
    addReq.path = f.string();
    addReq.recursive = false;
    auto addRes = yams::test_async::res(client.call<yams::daemon::AddDocumentRequest>(addReq));
    ASSERT_TRUE(addRes) << "Add via daemon failed: " << addRes.error().message;

    std::string hash = addRes.value().hash;
    ASSERT_FALSE(hash.empty());

    // Delete via CLI with daemon running
    auto out = runCommandWithEnv(yamsBinary_, storageDir_, xdgRuntimeDir_,
                                 "delete " + hash + " --force", true);
    (void)out;

    // Verify absent via daemon list
    auto lr = listAllViaDaemon();
    EXPECT_FALSE(containsName(lr, "hash_delete.txt"));
}

// Orphan cleanup: simulate missing content and expect metadata cleanup via daemon delete
TEST_F(DaemonAddDeleteIntegrationTest, OrphanCleanupWithDaemon) {
    // Ensure daemon is running
    DaemonClient client(clientConfig_);
    ASSERT_TRUE(client.connect());

    // Add a file
    auto f = sourceDir_ / "orphan.txt";
    writeFile(f, "orphan content");
    yams::daemon::AddDocumentRequest addReq;
    addReq.path = f.string();
    auto addRes = yams::test_async::res(client.call<yams::daemon::AddDocumentRequest>(addReq));
    ASSERT_TRUE(addRes);
    std::string hash = addRes.value().hash;

    // Simulate content store missing by renaming objects directory
    fs::path objectsDir = storageDir_ / "objects";
    fs::path objectsBak = storageDir_ / "objects.bak";
    std::error_code ec;
    if (fs::exists(objectsDir)) {
        fs::rename(objectsDir, objectsBak, ec);
    }

    // Delete by hash via CLI (daemon mode) => should clean orphaned metadata
    auto out = runCommandWithEnv(yamsBinary_, storageDir_, xdgRuntimeDir_,
                                 "delete " + hash + " --force", true);
    (void)out;

    // Restore objects dir if it was moved
    if (fs::exists(objectsBak)) {
        fs::rename(objectsBak, objectsDir, ec);
    }

    // Verify the metadata is gone (not listed)
    auto lr = listAllViaDaemon();
    EXPECT_FALSE(containsName(lr, "orphan.txt"));
}

// Delete by name, pattern, directory recursive including hidden files
TEST_F(DaemonAddDeleteIntegrationTest, DeleteByNamePatternDirectoryIncludingHidden) {
    // Ensure daemon running
    DaemonClient client(clientConfig_);
    ASSERT_TRUE(client.connect());

    // Create files
    auto f1 = sourceDir_ / "unique_name.txt";
    writeFile(f1, "unique name");

    auto p1 = sourceDir_ / "temp_1.log";
    auto p2 = sourceDir_ / "temp_2.log";
    auto p3 = sourceDir_ / "temp_3.log";
    writeFile(p1, "p1");
    writeFile(p2, "p2");
    writeFile(p3, "p3");

    auto dir = sourceDir_ / "subdir";
    auto hidden = dir / ".hidden.txt";
    auto dfile = dir / "d1.txt";
    fs::create_directories(dir);
    writeFile(hidden, "hidden");
    writeFile(dfile, "d1");

    // Add recursively directory and individual files via daemon
    {
        yams::daemon::AddDocumentRequest addReq;
        addReq.path = f1.string();
        auto r = yams::test_async::res(client.call<yams::daemon::AddDocumentRequest>(addReq));
        ASSERT_TRUE(r);
    }
    {
        yams::daemon::AddDocumentRequest addReq;
        addReq.path = p1.string();
        auto r = yams::test_async::res(client.call<yams::daemon::AddDocumentRequest>(addReq));
        ASSERT_TRUE(r);
        addReq.path = p2.string();
        r = yams::test_async::res(client.call<yams::daemon::AddDocumentRequest>(addReq));
        ASSERT_TRUE(r);
        addReq.path = p3.string();
        r = yams::test_async::res(client.call<yams::daemon::AddDocumentRequest>(addReq));
        ASSERT_TRUE(r);
    }
    {
        yams::daemon::AddDocumentRequest addReq;
        addReq.path = dir.string();
        addReq.recursive = true;
        auto r = yams::test_async::res(client.call<yams::daemon::AddDocumentRequest>(addReq));
        ASSERT_TRUE(r);
    }

    // 1) Delete by name
    auto out1 = runCommandWithEnv(yamsBinary_, storageDir_, xdgRuntimeDir_,
                                  "delete --name \"unique_name.txt\" --force", true);
    (void)out1;
    auto lr1 = listAllViaDaemon();
    EXPECT_FALSE(containsName(lr1, "unique_name.txt"));

    // 2) Delete by pattern temp_*.log
    auto out2 = runCommandWithEnv(yamsBinary_, storageDir_, xdgRuntimeDir_,
                                  "delete --pattern \"temp_*.log\" --force", true);
    (void)out2;
    auto lr2 = listAllViaDaemon();
    EXPECT_FALSE(containsName(lr2, "temp_1.log"));
    EXPECT_FALSE(containsName(lr2, "temp_2.log"));
    EXPECT_FALSE(containsName(lr2, "temp_3.log"));

    // 3) Delete by directory (recursive) including hidden files
    auto out3 = runCommandWithEnv(
        yamsBinary_, storageDir_, xdgRuntimeDir_,
        std::string("delete --directory \"") + dir.string() + "\" --recursive --force", true);
    (void)out3;
    auto lr3 = listAllViaDaemon();
    EXPECT_FALSE(containsName(lr3, "d1.txt"));
    // Hidden file should also be removed given current resolver default includeHidden = true
    EXPECT_FALSE(containsName(lr3, ".hidden.txt"));
}

// Add by directory to verify normalization with daemon (absolute directory path)
TEST_F(DaemonAddDeleteIntegrationTest, AddDirectoryNormalizationWithDaemon) {
    // Create directory with files
    auto d = sourceDir_ / "adddir";
    fs::create_directories(d);
    writeFile(d / "a.txt", "A");
    writeFile(d / "b.txt", "B");

    // Add directory via CLI with recursive flag and relative directory
    auto out = runCommandWithEnv(yamsBinary_, storageDir_, xdgRuntimeDir_,
                                 "add -r " + (fs::path("source") / "adddir").string(), true);
    (void)out;

    // Verify both files appear via daemon list
    auto lr = listAllViaDaemon();
    EXPECT_TRUE(containsName(lr, "a.txt"));
    EXPECT_TRUE(containsName(lr, "b.txt"));
}
