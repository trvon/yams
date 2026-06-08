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
#include <string_view>
#include <thread>

#include <spdlog/spdlog.h>

#include <yams/daemon/daemon.h>
#include <yams/daemon/shutdown_budget.h>

namespace yams::daemon {

namespace {

std::string getenvCopy(std::string_view name) {
    static std::mutex envMutex;
    std::lock_guard<std::mutex> lock(envMutex);
    const std::string key(name);
    const char* env = std::getenv(key.c_str()); // NOLINT(concurrency-mt-unsafe)
    if (!env || !*env) {
        return {};
    }
    return std::string(env);
}

bool isSupervisorManagedForegroundDaemon() {
    const std::string managed = getenvCopy("YAMS_DAEMON_FOREGROUND");
    return managed == "1" || managed == "true" || managed == "TRUE";
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
        int timeoutMs = 120000; // 2 minutes — accommodates slow WorkCoordinator shutdown
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

            if (const std::string env = getenvCopy("YAMS_SHUTDOWN_FORCE_EXIT_MS"); !env.empty()) {
                try {
                    int parsed = std::stoi(env);
                    if (parsed <= 0) {
                        timeoutMs = 0;
                    } else {
                        timeoutMs = std::max(parsed, 1000);
                        if (graceful) {
                            const auto requested = std::chrono::milliseconds(timeoutMs);
                            const auto clamped =
                                shutdown_budget::clampGracefulShutdownWaitTimeout(requested);
                            if (clamped != requested) {
                                spdlog::info("Clamping graceful shutdown timeout from {}ms to "
                                             "{}ms to cover ServiceManager join budgets",
                                             requested.count(), clamped.count());
                            }
                            timeoutMs = static_cast<int>(clamped.count());
                        }
                    }
                } catch (const std::exception& e) {
                    spdlog::debug("Ignoring invalid YAMS_SHUTDOWN_FORCE_EXIT_MS '{}': {}", env,
                                  e.what());
                } catch (...) {
                    spdlog::debug("Ignoring invalid YAMS_SHUTDOWN_FORCE_EXIT_MS '{}': unknown "
                                  "error",
                                  env);
                }
            }

            if (!inTestMode) {
                if (timeoutMs > 0) {
                    watchdog = std::thread([timeoutMs, &shutdownComplete, &watchdogMutex,
                                            &watchdogCv]() {
                        auto deadline =
                            std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
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
                            std::fprintf(stderr, "Shutdown exceeded %dms; forcing process exit\n",
                                         timeoutMs);
                            std::fflush(stderr);
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
                // Intentional best-effort path; keep the primary operation unaffected.
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
                        // Intentional best-effort path; keep the primary operation unaffected.
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
                            // Intentional best-effort path; keep the primary operation unaffected.
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
                // Intentional best-effort path; keep the primary operation unaffected.
            }
        } catch (...) {
            finalizeWatchdog();
            try {
                spdlog::error("Unknown exception during daemon shutdown");
            } catch (...) {
                // Intentional best-effort path; keep the primary operation unaffected.
            }
        }

        if (!inTestMode && !isSupervisorManagedForegroundDaemon()) {
            try {
                spdlog::info("Daemon shutdown complete, exiting process");
            } catch (...) {
                // Intentional best-effort path; keep the primary operation unaffected.
            }
            std::fflush(nullptr);
            std::_Exit(0);
        } else if (!inTestMode) {
            try {
                spdlog::info(
                    "Daemon shutdown complete (foreground-managed mode, not forcing exit)");
            } catch (...) {
                // Intentional best-effort path; keep the primary operation unaffected.
            }
        } else {
            try {
                spdlog::info("Daemon shutdown complete (test mode, not exiting)");
            } catch (...) {
                // Intentional best-effort path; keep the primary operation unaffected.
            }
        }
        // NOLINTEND(bugprone-empty-catch)

        (void)graceful;
    });
}

} // namespace yams::daemon
