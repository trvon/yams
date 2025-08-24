#pragma once

#include <filesystem>
#include "IComponent.h"
#include <yams/core/types.h>

namespace yams::daemon {

class YamsDaemon; // Forward declaration

/**
 * @class LifecycleComponent
 * @brief Manages the daemon's OS-level lifecycle, including PID file and signal handling.
 */
class LifecycleComponent : public IComponent {
public:
    LifecycleComponent(YamsDaemon* daemon, std::filesystem::path pidFile);
    ~LifecycleComponent() override;

    // IComponent interface
    const char* getName() const override { return "LifecycleManager"; }
    Result<void> initialize() override;
    void shutdown() override;

private:
    void setupSignalHandlers();
    void cleanupSignalHandlers();
    static void signalHandler(int signal);
    void handleSignal(int signal);

    Result<void> createPidFile();
    Result<void> removePidFile() const;
    bool isAnotherInstanceRunning() const;

    YamsDaemon* daemon_; // Non-owning pointer to the main daemon class to signal shutdown
    std::filesystem::path pidFile_;
    int pidFileFd_ = -1; // File descriptor for the locked PID file

    static LifecycleComponent* instance_; // Singleton instance for the static signal handler
};

} // namespace yams::daemon
