#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <map>
#include <mutex>
#include <string>
#include <unordered_map>

namespace yams::daemon {

class DaemonFSM {
public:
    enum class State {
        Starting,
        IPCListening,
        CoreReady,
        QueryReady,
        Ready,
        Degraded,
        ShuttingDown,
        Stopped
    };
    enum class Event {
        IpcListening,
        ContentStoreReady,
        DatabaseReady,
        MetadataRepoReady,
        SearchEngineReady,
        ModelProviderReady,
        VectorIndexReady,
        PluginsReady,
        ShutdownRequested,
        Stopped
    };
    enum class Subsystem {
        IpcServer,
        ContentStore,
        Database,
        MetadataRepo,
        SearchEngine,
        ModelProvider,
        VectorIndex,
        Plugins
    };

    DaemonFSM();

    void on(Event e);
    bool isAtLeast(State s) const;
    bool waitFor(State s, std::chrono::milliseconds timeout);

    void observed(Subsystem s, bool ready = true);
    void progress(Subsystem s, int pct);

    State state() const { return current_.load(); }
    std::string overallStatus() const;

    std::map<std::string, bool> readinessMap() const;
    std::map<std::string, int> progressMap() const;

private:
    static const char* keyOf(Subsystem s);
    void recompute();

    std::atomic<State> current_{State::Starting};
    std::unordered_map<Subsystem, bool> ready_;
    std::unordered_map<Subsystem, int> progress_;
    mutable std::mutex mu_;
    std::condition_variable_any cv_;
};

} // namespace yams::daemon
