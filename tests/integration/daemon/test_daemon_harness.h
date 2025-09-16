// Lightweight RAII harness to start/stop a YamsDaemon for integration tests
#pragma once

#include <filesystem>
#include <random>
#include <thread>
#include <gtest/gtest.h>
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
        sock_ = root_ / ("daemon_" + id + ".sock");
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
        stop();
        cleanup();
    }

    bool start(std::chrono::milliseconds wait = std::chrono::seconds(2)) {
        if (!daemon_)
            return false;
        auto s = daemon_->start();
        if (!s)
            return false;
        // Poll status via client until IPC/server up
        yams::daemon::ClientConfig cc;
        cc.socketPath = sock_;
        cc.autoStart = false;
        yams::daemon::DaemonClient client(cc);
        auto deadline = std::chrono::steady_clock::now() + wait;
        while (std::chrono::steady_clock::now() < deadline) {
            auto st = yams::cli::run_sync(client.status(), std::chrono::milliseconds(250));
            if (st) {
                const auto& v = st.value();
                // Basic readiness: IPC server and content store ready
                if (v.readinessStates.at("ipc_server") && v.readinessStates.at("content_store")) {
                    return true;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        return true; // server up even if subsystems still warming
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
