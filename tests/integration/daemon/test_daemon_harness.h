// Lightweight RAII harness to start/stop a YamsDaemon for integration tests
#pragma once

#include <spdlog/spdlog.h>
#include <filesystem>
#include <random>
#include <thread>
#include "test_async_helpers.h"
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/daemon.h>

namespace yams::test {

class DaemonHarness {
public:
    DaemonHarness() {
        namespace fs = std::filesystem;
        // Ensure mock provider and disable ABI plugins for stability
        // Create temp working dir
        auto id = random_id();
        root_ = fs::temp_directory_path() / (std::string("yams_it_") + id);
        fs::create_directories(root_);
        data_ = root_ / "data";
        fs::create_directories(data_);
        // Use short AF_UNIX path under /tmp to avoid sandbox and length issues
        sock_ = std::filesystem::path("/tmp") / ("daemon_" + id + ".sock");
        pid_ = root_ / ("daemon_" + id + ".pid");
        log_ = root_ / ("daemon_" + id + ".log");

        yams::daemon::DaemonConfig cfg;
        cfg.dataDir = data_;
        cfg.socketPath = sock_;
        cfg.pidFile = pid_;
        cfg.logFile = log_;
        cfg.enableModelProvider = true;
        cfg.autoLoadPlugins = false; // stable under mock
        cfg.useMockModelProvider = true;
        daemon_ = std::make_unique<yams::daemon::YamsDaemon>(cfg);
    }

    ~DaemonHarness() {
        // Explicitly stop daemon before cleanup to avoid crashes
        stop();
        cleanup();
    }

    bool start(std::chrono::milliseconds timeout = std::chrono::seconds(10)) {
        if (!daemon_) {
            return false;
        }
        auto s = daemon_->start();
        if (!s) {
            return false;
        }

        // Poll for socket server to be available
        auto client = yams::daemon::DaemonClient(
            yams::daemon::ClientConfig{.socketPath = sock_,
                                       .connectTimeout = std::chrono::milliseconds(500),
                                       .autoStart = false});

        auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));

            // Try to connect - socket server readiness is enough
            auto connectResult =
                yams::cli::run_sync(client.connect(), std::chrono::milliseconds(300));
            if (connectResult) {
                // Connected successfully, daemon is ready
                return true;
            }
        }

        return false;
    }

    void stop() {
        if (daemon_) {
            (void)daemon_->stop();
        }
    }

    const std::filesystem::path& socketPath() const { return sock_; }
    const std::filesystem::path& dataDir() const { return data_; }
    yams::daemon::YamsDaemon* daemon() const { return daemon_.get(); }

private:
    static std::string random_id() {
        static const char* cs = "abcdefghijklmnopqrstuvwxyz0123456789";
        thread_local std::mt19937_64 rng{std::random_device{}()};
        std::uniform_int_distribution<size_t> dist(0, 35);
        std::string out;
        out.reserve(8);
        for (int i = 0; i < 8; ++i)
            out.push_back(cs[dist(rng)]);
        return out;
    }
    void cleanup() {
        namespace fs = std::filesystem;
        std::error_code ec;
        if (!root_.empty())
            fs::remove_all(root_, ec);
    }

    std::unique_ptr<yams::daemon::YamsDaemon> daemon_;
    std::filesystem::path root_, data_, sock_, pid_, log_;
};

} // namespace yams::test
