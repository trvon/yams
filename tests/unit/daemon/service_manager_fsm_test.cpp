#include <gtest/gtest.h>
#include <yams/daemon/components/EmbeddingProviderFsm.h>
#include <yams/daemon/components/ServiceManagerFsm.h>

using namespace yams::daemon;

TEST(ServiceManagerFsm, BasicTransitions) {
    ServiceManagerFsm fsm;
    auto s0 = fsm.snapshot();
    EXPECT_EQ(s0.state, ServiceManagerState::Uninitialized);

    fsm.dispatch(OpeningDatabaseEvent{});
    EXPECT_EQ(fsm.snapshot().state, ServiceManagerState::OpeningDatabase);

    fsm.dispatch(DatabaseOpenedEvent{});
    EXPECT_EQ(fsm.snapshot().state, ServiceManagerState::DatabaseReady);

    fsm.dispatch(MigrationStartedEvent{});
    EXPECT_EQ(fsm.snapshot().state, ServiceManagerState::MigratingSchema);

    fsm.dispatch(MigrationCompletedEvent{});
    EXPECT_EQ(fsm.snapshot().state, ServiceManagerState::SchemaReady);

    fsm.dispatch(VectorsInitializedEvent{384});
    EXPECT_EQ(fsm.snapshot().state, ServiceManagerState::VectorsReady);

    fsm.dispatch(SearchEngineBuildStartedEvent{});
    EXPECT_EQ(fsm.snapshot().state, ServiceManagerState::BuildingSearchEngine);

    fsm.dispatch(SearchEngineBuiltEvent{});
    EXPECT_EQ(fsm.snapshot().state, ServiceManagerState::Ready);

    fsm.dispatch(ShutdownEvent{});
    EXPECT_EQ(fsm.snapshot().state, ServiceManagerState::ShuttingDown);

    fsm.dispatch(ServiceManagerStoppedEvent{});
    EXPECT_EQ(fsm.snapshot().state, ServiceManagerState::Stopped);
}

TEST(EmbeddingProviderFsm, Basic) {
    EmbeddingProviderFsm efsm;
    EXPECT_FALSE(efsm.isReady());
    efsm.dispatch(ModelLoadStartedEvent{"model"});
    EXPECT_FALSE(efsm.isReady());
    efsm.dispatch(ModelLoadedEvent{"model", 384});
    EXPECT_TRUE(efsm.isReady());
    EXPECT_EQ(efsm.dimension(), static_cast<std::size_t>(384));
}
