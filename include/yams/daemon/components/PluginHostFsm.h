#pragma once

#include <algorithm>
#include <mutex>
#include <string>
#include <vector>

#include <tinyfsm.hpp>

namespace yams::daemon {

enum class PluginHostState { NotInitialized, ScanningDirectories, LoadingPlugins, Ready, Failed };

struct PluginHostSnapshot {
    PluginHostState state{PluginHostState::NotInitialized};
    std::string lastError;
    std::size_t loadedCount{0};
    std::vector<std::string> loadedPlugins;
};

struct PluginScanStartedEvent {
    std::size_t directoryCount{0};
};
struct PluginLoadedEvent {
    std::string name;
};
struct AllPluginsLoadedEvent {
    std::size_t count{0};
};
struct PluginLoadFailedEvent {
    std::string error;
};

namespace detail {

struct PHNotInitialized;
struct PHScanningDirectories;
struct PHLoadingPlugins;
struct PHReady;
struct PHFailed;

struct PluginHostMachine : tinyfsm::MooreMachine<PluginHostMachine> {
    inline static PluginHostSnapshot snap{};

    virtual void react(const PluginScanStartedEvent&);
    virtual void react(const PluginLoadedEvent&);
    virtual void react(const AllPluginsLoadedEvent&);
    virtual void react(const PluginLoadFailedEvent&);
};

struct PHNotInitialized : PluginHostMachine {
    void entry() { snap.state = PluginHostState::NotInitialized; }
};

struct PHScanningDirectories : PluginHostMachine {
    void entry() { snap.state = PluginHostState::ScanningDirectories; }
};

struct PHLoadingPlugins : PluginHostMachine {
    void entry() { snap.state = PluginHostState::LoadingPlugins; }
};

struct PHReady : PluginHostMachine {
    void entry() { snap.state = PluginHostState::Ready; }
};

struct PHFailed : PluginHostMachine {
    void entry() { snap.state = PluginHostState::Failed; }
};

inline void PluginHostMachine::react(const PluginScanStartedEvent&) {
    transit<PHScanningDirectories>();
}

inline void PluginHostMachine::react(const PluginLoadedEvent& ev) {
    ++snap.loadedCount;
    if (std::find(snap.loadedPlugins.begin(), snap.loadedPlugins.end(), ev.name) ==
        snap.loadedPlugins.end()) {
        snap.loadedPlugins.push_back(ev.name);
    }
    transit<PHLoadingPlugins>();
}

inline void PluginHostMachine::react(const AllPluginsLoadedEvent& ev) {
    snap.loadedCount = ev.count;
    transit<PHReady>();
}

inline void PluginHostMachine::react(const PluginLoadFailedEvent& ev) {
    snap.lastError = ev.error;
    transit<PHFailed>();
}

} // namespace detail
} // namespace yams::daemon

namespace tinyfsm {
template <> inline void Fsm<yams::daemon::detail::PluginHostMachine>::set_initial_state() {
    current_state_ptr = &_state_instance<yams::daemon::detail::PHNotInitialized>::value;
}
} // namespace tinyfsm

namespace yams::daemon {

class PluginHostFsm {
public:
    PluginHostFsm() {
        std::lock_guard<std::mutex> lock(mutex_);
        detail::PluginHostMachine::snap = {};
        detail::PluginHostMachine::start();
    }

    PluginHostSnapshot snapshot() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return detail::PluginHostMachine::snap;
    }

    template <typename E> void dispatch(const E& ev) {
        std::lock_guard<std::mutex> lock(mutex_);
        detail::PluginHostMachine::dispatch(ev);
    }

private:
    mutable std::mutex mutex_;
};

} // namespace yams::daemon
