#include <yams/daemon/daemon_lifecycle.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <mutex>
#include <string>
#include <thread>

#include <spdlog/spdlog.h>

#include <yams/daemon/daemon.h>

namespace yams::daemon {

namespace {

bool isSupervisorManagedForegroundDaemon() {
    if (const char* managed = std::getenv("YAMS_DAEMON_FOREGROUND")) {
        return std::string_view(managed) == "1" || std::string_view(managed) == "true" ||
               std::string_view(managed) == "TRUE";
    }
    return false;
}

} // namespace

LifecycleSnapshot DaemonLifecycleAdapter::getLifecycleSnapshot() const {
    if (!daemon_) {
        return LifecycleSnapshot{};
    }
    return daemon_->getLifecycle().snapshot();
}

void DaemonLifecycleAdapter::setSubsystemDegraded(const std::string& subsystem, bool degraded,
                                                  const std::string& reason) {
    if (!daemon_) {
        return;
    }
    daemon_->setSubsystemDegraded(subsystem, degraded, reason);
}

void DaemonLifecycleAdapter::onDocumentRemoved(const std::string& hash) {
    if (!daemon_) {
        return;
    }
    daemon_->onDocumentRemoved(hash);
}

void DaemonLifecycleAdapter::requestShutdown(bool graceful, bool inTestMode) {
    if (!daemon_) {
        return;
    }
    daemon_->spawnShutdownThread([d = daemon_, graceful, inTestMode]() {
        std::atomic<bool> shutdownComplete{false};
        std::mutex watchdogMutex;
        std::condition_variable watchdogCv;
        std::thread watchdog;
        int timeoutMs = 15000;
        auto finalizeWatchdog = [&]() {
            shutdownComplete.store(true, std::memory_order_release);
            {
                std::lock_guard<std::mutex> lock(watchdogMutex);
                watchdogCv.notify_all();
            }
            if (watchdog.joinable()) {
                watchdog.join();
            }
        };
        try {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));

            if (const char* env = std::getenv("YAMS_SHUTDOWN_FORCE_EXIT_MS")) {
                try {
                    int parsed = std::stoi(env);
                    if (parsed <= 0) {
                        timeoutMs = 0;
                    } else {
                        timeoutMs = std::max(parsed, 1000);
                    }
                } catch (...) {
                }
            }

            if (!inTestMode) {
                if (timeoutMs > 0) {
                    watchdog =
                        std::thread([timeoutMs, &shutdownComplete, &watchdogMutex, &watchdogCv]() {
                            auto deadline = std::chrono::steady_clock::now() +
                                            std::chrono::milliseconds(timeoutMs);
                            std::unique_lock<std::mutex> lock(watchdogMutex);
                            watchdogCv.wait_until(lock, deadline, [&]() {
                                return shutdownComplete.load(std::memory_order_acquire);
                            });
                            if (shutdownComplete.load(std::memory_order_acquire)) {
                                return;
                            }
                            try {
                                spdlog::error("Shutdown exceeded {}ms; forcing process exit",
                                              timeoutMs);
                            } catch (...) {
                            }
#if !defined(_WIN32)
                            raise(SIGKILL);
#endif
                            std::_Exit(1);
                        });
                }
            }

            // NOLINTBEGIN(bugprone-empty-catch): logger may be unavailable during shutdown
            try {
                spdlog::info("Initiating daemon shutdown request...");
            } catch (...) {
            }

            d->requestStop();

            bool shutdownSucceeded = false;
            if (d->runLoopStarted_.load(std::memory_order_acquire)) {
                const auto waitTimeout =
                    std::chrono::milliseconds(timeoutMs > 0 ? timeoutMs : 15000);
                shutdownSucceeded = d->waitForStopCompletion(waitTimeout);
                if (!shutdownSucceeded) {
                    try {
                        spdlog::error("Timed out waiting for daemon stop completion after shutdown "
                                      "request");
                    } catch (...) {
                    }
                }
            } else {
                auto result = d->stop();
                if (!result) {
                    const bool alreadyStopped =
                        result.error().code == ErrorCode::InvalidState && !d->isRunning();
                    shutdownSucceeded = alreadyStopped;
                    if (!alreadyStopped) {
                        try {
                            spdlog::error("Daemon shutdown encountered error: {}",
                                          result.error().message);
                        } catch (...) {
                        }
                    }
                } else {
                    shutdownSucceeded = true;
                }
            }

            finalizeWatchdog();

            if (!shutdownSucceeded) {
                return;
            }
        } catch (const std::exception& e) {
            finalizeWatchdog();
            try {
                spdlog::error("Exception during daemon shutdown: {}", e.what());
            } catch (...) {
            }
        } catch (...) {
            finalizeWatchdog();
            try {
                spdlog::error("Unknown exception during daemon shutdown");
            } catch (...) {
            }
        }

        if (!inTestMode && !isSupervisorManagedForegroundDaemon()) {
            try {
                spdlog::info("Daemon shutdown complete, exiting process");
            } catch (...) {
            }
            std::fflush(nullptr);
            std::_Exit(0);
        } else if (!inTestMode) {
            try {
                spdlog::info(
                    "Daemon shutdown complete (foreground-managed mode, not forcing exit)");
            } catch (...) {
            }
        } else {
            try {
                spdlog::info("Daemon shutdown complete (test mode, not exiting)");
            } catch (...) {
            }
        }
        // NOLINTEND(bugprone-empty-catch)

        (void)graceful;
    });
}

} // namespace yams::daemon
