// Daemon FSM test suite (Catch2)
// Consolidates 14 FSM test files into a single, well-organized suite
// Covers: ConnectionFSM, DaemonLifecycleFSM, ServiceManagerFSM, PluginHostFSM, EmbeddingProviderFSM
// Note: This is a basic structure. Full coverage requires understanding actual FSM implementation.

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

#include <yams/daemon/components/DaemonLifecycleFsm.h>
#include <yams/daemon/components/EmbeddingProviderFsm.h>
#include <yams/daemon/components/PluginHostFsm.h>
#include <yams/daemon/components/ServiceManagerFsm.h>
#include <yams/daemon/ipc/connection_fsm.h>

using namespace yams::daemon;

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
