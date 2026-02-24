#pragma once

#include <string>

#include <yams/daemon/components/DaemonLifecycleFsm.h>

namespace yams::daemon {

class YamsDaemon;

class IDaemonLifecycle {
public:
    virtual ~IDaemonLifecycle() = default;

    virtual LifecycleSnapshot getLifecycleSnapshot() const = 0;
    virtual void setSubsystemDegraded(const std::string& subsystem, bool degraded,
                                      const std::string& reason) = 0;
    virtual void onDocumentRemoved(const std::string& hash) = 0;
    virtual void requestShutdown(bool graceful, bool inTestMode) = 0;
};

class DaemonLifecycleAdapter final : public IDaemonLifecycle {
public:
    explicit DaemonLifecycleAdapter(YamsDaemon* daemon) : daemon_(daemon) {}

    LifecycleSnapshot getLifecycleSnapshot() const override;
    void setSubsystemDegraded(const std::string& subsystem, bool degraded,
                              const std::string& reason) override;
    void onDocumentRemoved(const std::string& hash) override;
    void requestShutdown(bool graceful, bool inTestMode) override;

private:
    YamsDaemon* daemon_{nullptr};
};

} // namespace yams::daemon
