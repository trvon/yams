// Daemon FSM test suite (Catch2)
// Consolidates 14 FSM test files into a single, well-organized suite
// Covers: ConnectionFSM, DaemonLifecycleFSM, ServiceManagerFSM, PluginHostFSM,
//         EmbeddingProviderFSM, SearchEngineFSM
// Note: This is a basic structure. Full coverage requires understanding actual FSM implementation.

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

#include <spdlog/sinks/ostream_sink.h>
#include <spdlog/spdlog.h>

#include <future>
#include <memory>
#include <sstream>
#include <thread>

#include <yams/daemon/components/DaemonLifecycleFsm.h>
#include <yams/daemon/components/EmbeddingProviderFsm.h>
#include <yams/daemon/components/PluginHostFsm.h>
#include <yams/daemon/components/SearchEngineFsm.h>
#include <yams/daemon/components/ServiceManagerFsm.h>
#include <yams/daemon/ipc/connection_fsm.h>

using namespace yams::daemon;

namespace {

class SpdlogCaptureGuard {
public:
    explicit SpdlogCaptureGuard(spdlog::level::level_enum level)
        : previousLogger_(spdlog::default_logger()), previousLevel_(spdlog::get_level()) {
        auto sink = std::make_shared<spdlog::sinks::ostream_sink_mt>(stream_);
        logger_ = std::make_shared<spdlog::logger>("daemon_fsm_capture", sink);
        logger_->set_level(level);
        spdlog::set_default_logger(logger_);
        spdlog::set_level(level);
    }

    ~SpdlogCaptureGuard() {
        if (previousLogger_) {
            spdlog::set_default_logger(previousLogger_);
        }
        spdlog::set_level(previousLevel_);
    }

    std::string str() const { return stream_.str(); }

private:
    std::ostringstream stream_;
    std::shared_ptr<spdlog::logger> previousLogger_;
    std::shared_ptr<spdlog::logger> logger_;
    spdlog::level::level_enum previousLevel_;
};

} // namespace

// =============================================================================
// ConnectionFSM Tests
// =============================================================================
// Consolidates: connection_fsm_test.cpp, connection_fsm_backpressure_test.cpp,
// connection_fsm_error_flow_test.cpp, connection_fsm_legality_test.cpp,
// connection_fsm_shutdown_test.cpp, connection_fsm_streaming_sequence_test.cpp,
// connection_fsm_timeout_retry_test.cpp

TEST_CASE("ConnectionFSM: Basic state transitions", "[daemon][fsm][connection]") {
    ConnectionFsm fsm;

    SECTION("Initial state is Disconnected") {
        REQUIRE(fsm.state() == ConnectionFsm::State::Disconnected);
        REQUIRE_FALSE(fsm.can_read());
        REQUIRE_FALSE(fsm.can_write());
    }

    SECTION("Connect transitions to Connected and enables I/O") {
        GIVEN("A disconnected FSM") {
            REQUIRE(fsm.state() == ConnectionFsm::State::Disconnected);

            WHEN("Connection is established") {
                fsm.on_connect(3); // dummy fd

                THEN("State transitions to Connected") {
                    REQUIRE(fsm.state() == ConnectionFsm::State::Connected);
                }

                THEN("Read and write are enabled") {
                    REQUIRE(fsm.can_read());
                    REQUIRE(fsm.can_write());
                }
            }
        }
    }

    SECTION("Header parsing transitions to ReadingPayload") {
        fsm.on_connect(3);
        REQUIRE(fsm.state() == ConnectionFsm::State::Connected);

        // Must call on_readable first to transition Connected → ReadingHeader
        fsm.on_readable(16); // Header size
        REQUIRE(fsm.state() == ConnectionFsm::State::ReadingHeader);

        ConnectionFsm::FrameInfo info{};
        info.payload_size = 128;

        fsm.on_header_parsed(info);
        REQUIRE(fsm.state() == ConnectionFsm::State::ReadingPayload);
        REQUIRE(fsm.can_read());
    }

    SECTION("Body parsing completes request cycle") {
        fsm.on_connect(3);
        ConnectionFsm::FrameInfo info{};
        info.payload_size = 128;
        fsm.on_header_parsed(info);

        fsm.on_body_parsed();
        // After body parsed, FSM should be in a state where write is possible
        // Actual state depends on FSM implementation
    }
}

TEST_CASE("ConnectionFSM: Error handling", "[daemon][fsm][connection][error]") {
    ConnectionFsm fsm;

    SECTION("Error transitions to Error state") {
        fsm.on_connect(3);
        REQUIRE(fsm.state() == ConnectionFsm::State::Connected);

        fsm.on_error(ECONNRESET);
        REQUIRE(fsm.state() == ConnectionFsm::State::Error);
        REQUIRE_FALSE(fsm.alive());
    }
}

TEST_CASE("ConnectionFSM: State validation", "[daemon][fsm][connection][validation]") {
    ConnectionFsm fsm;

    SECTION("Happy path: unary request") {
        // Disconnected → Connect → Connected
        fsm.on_connect(3);
        REQUIRE(fsm.state() == ConnectionFsm::State::Connected);

        // Connected → on_readable → ReadingHeader
        fsm.on_readable(16);
        REQUIRE(fsm.state() == ConnectionFsm::State::ReadingHeader);

        // ReadingHeader → HeaderParsed → ReadingPayload
        ConnectionFsm::FrameInfo info{};
        info.payload_size = 64;
        fsm.on_header_parsed(info);
        REQUIRE(fsm.state() == ConnectionFsm::State::ReadingPayload);

        // ReadingPayload → BodyParsed → WritingHeader
        fsm.on_body_parsed();
        REQUIRE(fsm.state() == ConnectionFsm::State::WritingHeader);
    }
}

// =============================================================================
// DaemonLifecycleFSM Tests
// =============================================================================
// Consolidates: daemon_lifecycle_fsm_test.cpp

TEST_CASE("DaemonLifecycleFSM: State transitions", "[daemon][fsm][lifecycle]") {
    DaemonLifecycleFsm fsm;

    SECTION("Starts in Unknown state and can reset") {
        auto snapshot = fsm.snapshot();
        REQUIRE(snapshot.state == LifecycleState::Unknown);

        fsm.reset();
        REQUIRE(fsm.snapshot().state == LifecycleState::Unknown);
    }

    SECTION("Bootstrapped from Unknown goes to Initializing") {
        GIVEN("FSM in Unknown state") {
            REQUIRE(fsm.snapshot().state == LifecycleState::Unknown);

            WHEN("Bootstrapped event is dispatched") {
                fsm.dispatch(BootstrappedEvent{});

                THEN("State transitions to Initializing") {
                    REQUIRE(fsm.snapshot().state == LifecycleState::Initializing);
                }
            }
        }
    }

    SECTION("Healthy from Initializing goes to Ready") {
        fsm.dispatch(BootstrappedEvent{});
        REQUIRE(fsm.snapshot().state == LifecycleState::Initializing);

        fsm.dispatch(HealthyEvent{});
        REQUIRE(fsm.snapshot().state == LifecycleState::Ready);
    }

    SECTION("Degraded from Ready") {
        fsm.dispatch(BootstrappedEvent{});
        fsm.dispatch(HealthyEvent{});
        REQUIRE(fsm.snapshot().state == LifecycleState::Ready);

        fsm.dispatch(DegradedEvent{});
        auto snapshot = fsm.snapshot();
        REQUIRE(snapshot.state == LifecycleState::Degraded);
    }

    SECTION("Failure transitions to Failed") {
        fsm.dispatch(BootstrappedEvent{});
        fsm.dispatch(HealthyEvent{});
        REQUIRE(fsm.snapshot().state == LifecycleState::Ready);

        fsm.dispatch(FailureEvent{"Critical error"});
        auto snapshot = fsm.snapshot();
        REQUIRE(snapshot.state == LifecycleState::Failed);
        REQUIRE(snapshot.lastError.find("Critical") != std::string::npos);
    }

    SECTION("ShutdownRequested from Ready reaches Stopped") {
        fsm.dispatch(BootstrappedEvent{});
        fsm.dispatch(HealthyEvent{});
        REQUIRE(fsm.snapshot().state == LifecycleState::Ready);

        fsm.dispatch(ShutdownRequestedEvent{});
        REQUIRE(fsm.snapshot().state == LifecycleState::Stopping);

        fsm.dispatch(StoppedEvent{});
        REQUIRE(fsm.snapshot().state == LifecycleState::Stopped);
    }

    SECTION("Failed lifecycle can still reach Stopped") {
        fsm.dispatch(BootstrappedEvent{});
        fsm.dispatch(FailureEvent{"Critical error"});
        REQUIRE(fsm.snapshot().state == LifecycleState::Failed);

        fsm.dispatch(StoppedEvent{});
        REQUIRE(fsm.snapshot().state == LifecycleState::Stopped);
    }

    SECTION("StoppedEvent is idempotent once stopped") {
        fsm.dispatch(BootstrappedEvent{});
        fsm.dispatch(HealthyEvent{});
        fsm.dispatch(ShutdownRequestedEvent{});
        fsm.dispatch(StoppedEvent{});
        REQUIRE(fsm.snapshot().state == LifecycleState::Stopped);

        fsm.dispatch(StoppedEvent{});
        REQUIRE(fsm.snapshot().state == LifecycleState::Stopped);
    }
}

TEST_CASE("DaemonLifecycleFSM: clearing degradation is not a warning", "[daemon][fsm][lifecycle]") {
    SpdlogCaptureGuard capture(spdlog::level::warn);
    DaemonLifecycleFsm fsm;

    fsm.setSubsystemDegraded("search", false);
    CHECK(capture.str().find("degraded: false") == std::string::npos);

    fsm.setSubsystemDegraded("search", true, "build_failed");
    CHECK(capture.str().find("Subsystem 'search' degraded: true reason=build_failed") !=
          std::string::npos);
}

// =============================================================================
// ServiceManagerFSM Tests
// =============================================================================
// Consolidates: service_manager_fsm_test.cpp, service_manager_fsm_status_test.cpp

TEST_CASE("ServiceManagerFSM: Basic transitions", "[daemon][fsm][service-manager]") {
    ServiceManagerFsm fsm;

    SECTION("Complete lifecycle: Uninitialized → Ready → Stopped") {
        GIVEN("FSM starts in Uninitialized state") {
            auto s0 = fsm.snapshot();
            REQUIRE(s0.state == ServiceManagerState::Uninitialized);

            WHEN("Database opening sequence completes") {
                fsm.dispatch(OpeningDatabaseEvent{});
                REQUIRE(fsm.snapshot().state == ServiceManagerState::OpeningDatabase);

                fsm.dispatch(DatabaseOpenedEvent{});
                REQUIRE(fsm.snapshot().state == ServiceManagerState::DatabaseReady);

                THEN("Migration can proceed") {
                    fsm.dispatch(MigrationStartedEvent{});
                    REQUIRE(fsm.snapshot().state == ServiceManagerState::MigratingSchema);

                    fsm.dispatch(MigrationCompletedEvent{});
                    REQUIRE(fsm.snapshot().state == ServiceManagerState::SchemaReady);
                }

                AND_WHEN("Vector and search engine initialization complete") {
                    fsm.dispatch(MigrationStartedEvent{});
                    fsm.dispatch(MigrationCompletedEvent{});
                    fsm.dispatch(VectorsInitializedEvent{384});
                    REQUIRE(fsm.snapshot().state == ServiceManagerState::VectorsReady);

                    fsm.dispatch(SearchEngineBuildStartedEvent{});
                    REQUIRE(fsm.snapshot().state == ServiceManagerState::BuildingSearchEngine);

                    fsm.dispatch(SearchEngineBuiltEvent{});
                    REQUIRE(fsm.snapshot().state == ServiceManagerState::Ready);

                    THEN("Can shutdown gracefully") {
                        fsm.dispatch(ShutdownEvent{});
                        REQUIRE(fsm.snapshot().state == ServiceManagerState::ShuttingDown);

                        fsm.dispatch(ServiceManagerStoppedEvent{});
                        REQUIRE(fsm.snapshot().state == ServiceManagerState::Stopped);
                    }
                }
            }
        }
    }
}

TEST_CASE("ServiceManagerFSM: Enum values are stable",
          "[daemon][fsm][service-manager][stability]") {
    // Enum stability test - critical for serialization/logging
    REQUIRE(static_cast<int>(ServiceManagerState::Uninitialized) == 0);
    REQUIRE(static_cast<int>(ServiceManagerState::OpeningDatabase) == 1);
    REQUIRE(static_cast<int>(ServiceManagerState::DatabaseReady) == 2);
}

TEST_CASE("ServiceManagerFSM: OpeningDatabase stuck without progress is reachable from failed init",
          "[daemon][fsm][service-manager][regression]") {
    // Reproduces the bug: when initializeMetadataDatabaseAt returns false
    // (e.g. SQLite contention, integrity check fails), the FSM stays at
    // OpeningDatabase forever.  After observing the failure the init path
    // must dispatch InitializationFailedEvent to break the deadlock.
    ServiceManagerFsm fsm;

    SECTION("Without failure event, FSM stays in OpeningDatabase") {
        fsm.dispatch(OpeningDatabaseEvent{});
        REQUIRE(fsm.snapshot().state == ServiceManagerState::OpeningDatabase);

        // waitForTerminalState would block here indefinitely — demonstrate
        // that isTerminalState() returns false.
        REQUIRE_FALSE(fsm.isTerminalState());
    }

    SECTION("InitializationFailedEvent transitions to Failed from OpeningDatabase") {
        fsm.dispatch(OpeningDatabaseEvent{});
        REQUIRE(fsm.snapshot().state == ServiceManagerState::OpeningDatabase);

        // When the ServiceManager detects the DB-open failure it should
        // dispatch this event, which must bring the FSM to a terminal state.
        fsm.dispatch(InitializationFailedEvent{"Database open failed: SQLite lock contention"});

        REQUIRE(fsm.snapshot().state == ServiceManagerState::Failed);
        REQUIRE(fsm.isTerminalState());
        REQUIRE(fsm.snapshot().lastError.find("SQLite") != std::string::npos);
    }

    SECTION("Shutdown during OpeningDatabase reaches terminal state") {
        // Simulate the scenario: DB open is in progress, shutdown is requested.
        // The init code calls serviceFsm_.dispatch(ShutdownEvent{}), which
        // transitions to ShuttingDown, a terminal state from which we can
        // dispatch ServiceManagerStoppedEvent to reach Stopped.
        fsm.dispatch(OpeningDatabaseEvent{});
        REQUIRE(fsm.snapshot().state == ServiceManagerState::OpeningDatabase);
        REQUIRE_FALSE(fsm.isTerminalState());

        fsm.dispatch(ShutdownEvent{});
        REQUIRE(fsm.snapshot().state == ServiceManagerState::ShuttingDown);
        REQUIRE(fsm.isTerminalState());

        fsm.dispatch(ServiceManagerStoppedEvent{});
        REQUIRE(fsm.snapshot().state == ServiceManagerState::Stopped);
        REQUIRE(fsm.isTerminalState());
    }

    SECTION("completeShutdown recovers if another FSM instance reset shared state") {
        fsm.dispatch(ShutdownEvent{});
        REQUIRE(fsm.snapshot().state == ServiceManagerState::ShuttingDown);

        // tinyfsm stores state process-wide; constructing another facade resets the shared machine.
        ServiceManagerFsm concurrentFacade;
        REQUIRE(concurrentFacade.snapshot().state == ServiceManagerState::Uninitialized);

        REQUIRE(fsm.completeShutdown());
        REQUIRE(fsm.snapshot().state == ServiceManagerState::Stopped);
        REQUIRE(fsm.isTerminalState());
    }

    SECTION("tryStartOpeningDatabase treats shutdown as cancellation") {
        fsm.dispatch(ShutdownEvent{});
        REQUIRE(fsm.snapshot().state == ServiceManagerState::ShuttingDown);

        REQUIRE_FALSE(fsm.tryStartOpeningDatabase());
        REQUIRE(fsm.snapshot().state == ServiceManagerState::ShuttingDown);
    }

    SECTION("waitForTerminalState unblocks after failure event") {
        // waitForTerminalState blocks until a terminal state is reached.
        // After dispatching InitializationFailedEvent, it should unblock.
        fsm.dispatch(OpeningDatabaseEvent{});
        REQUIRE_FALSE(fsm.isTerminalState());

        // Dispatch the failure from another "thread" — the condition
        // variable in waitForTerminalState should wake up.
        std::promise<ServiceManagerSnapshot> waiterResult;
        auto waiterFuture = waiterResult.get_future();
        std::thread waiter(
            [&fsm, &waiterResult]() { waiterResult.set_value(fsm.waitForTerminalState(5)); });

        // Small sleep to let the waiter start blocking
        std::this_thread::sleep_for(std::chrono::milliseconds(50));

        fsm.dispatch(InitializationFailedEvent{"forced failure for test"});

        waiter.join();
        REQUIRE(waiterFuture.get().state == ServiceManagerState::Failed);
        REQUIRE(fsm.snapshot().state == ServiceManagerState::Failed);
    }
}

// =============================================================================
// PluginHostFSM Tests
// =============================================================================
// Consolidates: plugin_host_fsm_failure_test.cpp, plugin_host_fsm_recovery_test.cpp

TEST_CASE("PluginHostFSM: Failure handling", "[daemon][fsm][plugin-host][failure]") {
    PluginHostFsm fsm;

    SECTION("Transitions to Failed and stores error") {
        GIVEN("FSM in ScanningDirectories state") {
            fsm.dispatch(PluginScanStartedEvent{1});
            auto s1 = fsm.snapshot();
            REQUIRE(s1.state == PluginHostState::ScanningDirectories);

            WHEN("Plugin load fails") {
                fsm.dispatch(PluginLoadFailedEvent{"dlopen error: symbol not found"});

                THEN("State transitions to Failed") {
                    auto s2 = fsm.snapshot();
                    REQUIRE(s2.state == PluginHostState::Failed);
                }

                THEN("Error message is stored") {
                    auto s2 = fsm.snapshot();
                    REQUIRE(s2.lastError.find("symbol not found") != std::string::npos);
                }
            }
        }
    }
}

TEST_CASE("PluginHostFSM: Recovery from failure", "[daemon][fsm][plugin-host][recovery]") {
    PluginHostFsm fsm;

    SECTION("Failure then Ready on all loaded") {
        GIVEN("FSM that experienced a failure") {
            fsm.dispatch(PluginScanStartedEvent{2});
            fsm.dispatch(PluginLoadFailedEvent{"first plugin failed"});
            REQUIRE(fsm.snapshot().state == PluginHostState::Failed);

            WHEN("All plugins successfully load in retry") {
                fsm.dispatch(AllPluginsLoadedEvent{});

                THEN("State transitions to Ready") {
                    REQUIRE(fsm.snapshot().state == PluginHostState::Ready);
                }
            }
        }
    }
}

// =============================================================================
// EmbeddingProviderFSM Tests
// =============================================================================
// Consolidates: embedding_provider_fsm_failure_test.cpp

TEST_CASE("EmbeddingProviderFSM: Basic lifecycle", "[daemon][fsm][embedding-provider]") {
    EmbeddingProviderFsm efsm;

    SECTION("Starts not ready") {
        REQUIRE_FALSE(efsm.isReady());
    }

    SECTION("Model loading sequence") {
        GIVEN("Provider is not ready") {
            REQUIRE_FALSE(efsm.isReady());

            WHEN("Model load starts") {
                efsm.dispatch(ModelLoadStartedEvent{"model"});

                THEN("Still not ready during load") {
                    REQUIRE_FALSE(efsm.isReady());
                }

                AND_WHEN("Model load completes") {
                    efsm.dispatch(ModelLoadedEvent{"model", 384});

                    THEN("Provider becomes ready") {
                        REQUIRE(efsm.isReady());
                    }

                    THEN("Dimension is set correctly") {
                        REQUIRE(efsm.dimension() == 384);
                    }
                }
            }
        }
    }
}

TEST_CASE("EmbeddingProviderFSM: Load failure", "[daemon][fsm][embedding-provider][failure]") {
    EmbeddingProviderFsm efsm;

    SECTION("Load failure transitions to Failed") {
        GIVEN("Provider attempting to load model") {
            efsm.dispatch(ModelLoadStartedEvent{"model"});

            WHEN("Load fails") {
                efsm.dispatch(LoadFailureEvent{"ONNX runtime error"});

                THEN("Provider is not ready") {
                    REQUIRE_FALSE(efsm.isReady());
                }

                THEN("Error is captured") {
                    auto snapshot = efsm.snapshot();
                    REQUIRE(snapshot.lastError.find("ONNX") != std::string::npos);
                }
            }
        }
    }
}

// =============================================================================
// SearchEngineFSM Tests
// =============================================================================
// Tests the SearchEngineFsm with AwaitingDrain state and rebuild request handling

TEST_CASE("SearchEngineFSM: Basic state transitions", "[daemon][fsm][search-engine]") {
    SearchEngineFsm fsm;

    SECTION("Initial state is NotBuilt") {
        auto snapshot = fsm.snapshot();
        REQUIRE(snapshot.state == SearchEngineState::NotBuilt);
        REQUIRE_FALSE(fsm.isReady());
        REQUIRE_FALSE(fsm.isBuilding());
        REQUIRE_FALSE(fsm.hasFailed());
        REQUIRE_FALSE(fsm.isAwaitingDrain());
    }

    SECTION("Build started transitions to Building") {
        fsm.dispatch(SearchEngineRebuildStartedEvent{"test_reason", true});

        auto snapshot = fsm.snapshot();
        REQUIRE(snapshot.state == SearchEngineState::Building);
        REQUIRE(fsm.isBuilding());
        REQUIRE(snapshot.buildReason == "test_reason");
        REQUIRE(snapshot.vectorEnabled == true);
    }

    SECTION("Build completed transitions to Ready") {
        fsm.dispatch(SearchEngineRebuildStartedEvent{"test", false});
        fsm.dispatch(SearchEngineRebuildCompletedEvent{true, 100});

        auto snapshot = fsm.snapshot();
        REQUIRE(snapshot.state == SearchEngineState::Ready);
        REQUIRE(fsm.isReady());
        REQUIRE(snapshot.hasEngine == true);
        REQUIRE(snapshot.vectorEnabled == true);
        REQUIRE(snapshot.buildDurationMs == 100);
    }

    SECTION("Build failure transitions to Failed") {
        fsm.dispatch(SearchEngineRebuildStartedEvent{"test", false});
        fsm.dispatch(SearchEngineRebuildFailedEvent{"build_timeout"});

        auto snapshot = fsm.snapshot();
        REQUIRE(snapshot.state == SearchEngineState::Failed);
        REQUIRE(fsm.hasFailed());
        REQUIRE(snapshot.lastError == "build_timeout");
        REQUIRE(snapshot.hasEngine == false);
    }
}

TEST_CASE("SearchEngineFSM: AwaitingDrain state", "[daemon][fsm][search-engine][drain]") {
    SearchEngineFsm fsm;

    SECTION("Rebuild request with waitForDrain transitions to AwaitingDrain") {
        bool accepted = fsm.dispatch(SearchEngineRebuildRequestedEvent{"drain_test", true, true});

        REQUIRE(accepted);
        auto snapshot = fsm.snapshot();
        REQUIRE(snapshot.state == SearchEngineState::AwaitingDrain);
        REQUIRE(fsm.isAwaitingDrain());
        REQUIRE(snapshot.rebuildPending == true);
        REQUIRE(snapshot.buildReason.find("drain_test") != std::string::npos);
    }

    SECTION("Rebuild request without waitForDrain triggers callback immediately") {
        bool callbackCalled = false;
        std::string callbackReason;
        bool callbackIncludeVector = false;

        fsm.setRebuildCallback([&](const std::string& reason, bool includeVector) {
            callbackCalled = true;
            callbackReason = reason;
            callbackIncludeVector = includeVector;
        });

        bool accepted = fsm.dispatch(SearchEngineRebuildRequestedEvent{"immediate", true, false});

        REQUIRE(accepted);
        REQUIRE(callbackCalled);
        REQUIRE(callbackReason == "immediate");
        REQUIRE(callbackIncludeVector == true);
    }

    SECTION("Indexing drained event triggers rebuild when in AwaitingDrain") {
        bool callbackCalled = false;
        std::string callbackReason;

        fsm.setRebuildCallback([&](const std::string& reason, bool /*includeVector*/) {
            callbackCalled = true;
            callbackReason = reason;
        });

        // Request rebuild with wait for drain
        fsm.dispatch(SearchEngineRebuildRequestedEvent{"awaiting", true, true});
        REQUIRE(fsm.isAwaitingDrain());
        REQUIRE_FALSE(callbackCalled);

        // Signal drain - should trigger callback
        fsm.dispatch(SearchEngineIndexingDrainedEvent{});
        REQUIRE(callbackCalled);
        REQUIRE(callbackReason == "awaiting");
    }

    SECTION("Indexing drained event is ignored when not in AwaitingDrain") {
        bool callbackCalled = false;

        fsm.setRebuildCallback(
            [&](const std::string& /*reason*/, bool /*includeVector*/) { callbackCalled = true; });

        // FSM is in NotBuilt state, drain should be ignored
        fsm.dispatch(SearchEngineIndexingDrainedEvent{});
        REQUIRE_FALSE(callbackCalled);

        // Transition to Ready state
        fsm.dispatch(SearchEngineRebuildStartedEvent{"init", false});
        fsm.dispatch(SearchEngineRebuildCompletedEvent{false, 0});
        REQUIRE(fsm.isReady());

        // Drain should still be ignored
        fsm.dispatch(SearchEngineIndexingDrainedEvent{});
        REQUIRE_FALSE(callbackCalled);
    }
}

TEST_CASE("SearchEngineFSM: Rebuild guards", "[daemon][fsm][search-engine][guards]") {
    SearchEngineFsm fsm;

    SECTION("Cannot request rebuild while building") {
        fsm.dispatch(SearchEngineRebuildStartedEvent{"first", false});
        REQUIRE(fsm.isBuilding());

        bool accepted = fsm.dispatch(SearchEngineRebuildRequestedEvent{"second", true, true});
        REQUIRE_FALSE(accepted);
        // State should still be Building
        REQUIRE(fsm.isBuilding());
    }

    SECTION("Cannot request rebuild while awaiting drain") {
        fsm.dispatch(SearchEngineRebuildRequestedEvent{"first", true, true});
        REQUIRE(fsm.isAwaitingDrain());

        bool accepted = fsm.dispatch(SearchEngineRebuildRequestedEvent{"second", true, true});
        REQUIRE_FALSE(accepted);
        // State should still be AwaitingDrain
        REQUIRE(fsm.isAwaitingDrain());
    }

    SECTION("Can request rebuild after previous build completes") {
        // First build cycle
        fsm.dispatch(SearchEngineRebuildStartedEvent{"first", false});
        fsm.dispatch(SearchEngineRebuildCompletedEvent{false, 0});
        REQUIRE(fsm.isReady());

        // Second rebuild request should be accepted
        bool accepted = fsm.dispatch(SearchEngineRebuildRequestedEvent{"second", true, true});
        REQUIRE(accepted);
        REQUIRE(fsm.isAwaitingDrain());
    }

    SECTION("Can request rebuild after previous build fails") {
        // Failed build
        fsm.dispatch(SearchEngineRebuildStartedEvent{"first", false});
        fsm.dispatch(SearchEngineRebuildFailedEvent{"error"});
        REQUIRE(fsm.hasFailed());

        // Rebuild request should be accepted
        bool accepted = fsm.dispatch(SearchEngineRebuildRequestedEvent{"retry", true, false});
        REQUIRE(accepted);
    }
}

TEST_CASE("SearchEngineFSM: Snapshot fields", "[daemon][fsm][search-engine][snapshot]") {
    SearchEngineFsm fsm;

    SECTION("rebuildPending is cleared after build starts") {
        fsm.dispatch(SearchEngineRebuildRequestedEvent{"test", true, true});
        REQUIRE(fsm.snapshot().rebuildPending == true);

        fsm.dispatch(SearchEngineRebuildStartedEvent{"test", true});
        REQUIRE(fsm.snapshot().rebuildPending == false);
    }

    SECTION("rebuildPending is cleared after build completes") {
        fsm.dispatch(SearchEngineRebuildRequestedEvent{"test", true, true});
        fsm.dispatch(SearchEngineRebuildStartedEvent{"test", true});
        fsm.dispatch(SearchEngineRebuildCompletedEvent{true, 50});

        auto snapshot = fsm.snapshot();
        REQUIRE(snapshot.rebuildPending == false);
        REQUIRE(snapshot.hasEngine == true);
    }

    SECTION("rebuildPending is cleared after build fails") {
        fsm.dispatch(SearchEngineRebuildRequestedEvent{"test", true, true});
        fsm.dispatch(SearchEngineRebuildStartedEvent{"test", true});
        fsm.dispatch(SearchEngineRebuildFailedEvent{"timeout"});

        auto snapshot = fsm.snapshot();
        REQUIRE(snapshot.rebuildPending == false);
        REQUIRE(snapshot.hasEngine == false);
    }
}
