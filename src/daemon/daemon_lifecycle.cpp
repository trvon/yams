#include <yams/daemon/daemon_lifecycle.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <string>
#include <thread>

#include <spdlog/spdlog.h>

#include <yams/daemon/daemon.h>

namespace yams::daemon {

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
        std::thread watchdog;
        auto finalizeWatchdog = [&]() {
            shutdownComplete.store(true, std::memory_order_release);
            if (watchdog.joinable()) {
                watchdog.join();
            }
        };
        try {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));

            if (!inTestMode) {
                int timeoutMs = 15000;
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
                if (timeoutMs > 0) {
                    watchdog = std::thread([timeoutMs, &shutdownComplete]() {
                        auto deadline =
                            std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
                        while (std::chrono::steady_clock::now() < deadline) {
                            if (shutdownComplete.load(std::memory_order_acquire)) {
                                return;
                            }
                            std::this_thread::sleep_for(std::chrono::milliseconds(100));
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
                spdlog::info("Initiating daemon shutdown sequence...");
            } catch (...) {
            }
            auto result = d->stop();
            finalizeWatchdog();
            if (!result) {
                try {
                    spdlog::error("Daemon shutdown encountered error: {}", result.error().message);
                } catch (...) {
                }
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

        d->requestStop();

        if (!inTestMode) {
            try {
                spdlog::info("Daemon shutdown complete, exiting process");
            } catch (...) {
            }
            std::exit(0);
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
