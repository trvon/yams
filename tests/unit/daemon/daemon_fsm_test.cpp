#include <chrono>
#include <gtest/gtest.h>
#include <yams/daemon/components/DaemonFSM.h>

using namespace yams::daemon;

TEST(DaemonFSM, BasicTransitions) {
    DaemonFSM fsm;
    EXPECT_FALSE(fsm.isAtLeast(DaemonFSM::State::IPCListening));
    fsm.on(DaemonFSM::Event::IpcListening);
    EXPECT_TRUE(fsm.isAtLeast(DaemonFSM::State::IPCListening));
    fsm.on(DaemonFSM::Event::ContentStoreReady);
    fsm.on(DaemonFSM::Event::DatabaseReady);
    fsm.on(DaemonFSM::Event::MetadataRepoReady);
    EXPECT_TRUE(fsm.isAtLeast(DaemonFSM::State::CoreReady));
    fsm.on(DaemonFSM::Event::VectorIndexReady);
    EXPECT_TRUE(fsm.isAtLeast(DaemonFSM::State::QueryReady));
}
