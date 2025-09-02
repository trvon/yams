#include <gtest/gtest.h>
#include <yams/daemon/components/DaemonLifecycleFsm.h>

using namespace yams::daemon;

static LifecycleState stateOf(const DaemonLifecycleFsm& fsm) {
    return fsm.snapshot().state;
}

TEST(DaemonLifecycleFsmTest, StartsUnknownAndResets) {
    DaemonLifecycleFsm fsm;
    EXPECT_EQ(stateOf(fsm), LifecycleState::Unknown);
    fsm.reset();
    EXPECT_EQ(stateOf(fsm), LifecycleState::Unknown);
}

TEST(DaemonLifecycleFsmTest, BootstrappedFromUnknownGoesInitializing) {
    DaemonLifecycleFsm fsm;
    fsm.dispatch(BootstrappedEvent{});
    EXPECT_EQ(stateOf(fsm), LifecycleState::Initializing);
}

TEST(DaemonLifecycleFsmTest, HealthyFromInitializingGoesReady) {
    DaemonLifecycleFsm fsm;
    fsm.dispatch(BootstrappedEvent{});
    ASSERT_EQ(stateOf(fsm), LifecycleState::Initializing);
    fsm.dispatch(HealthyEvent{});
    EXPECT_EQ(stateOf(fsm), LifecycleState::Ready);
}

TEST(DaemonLifecycleFsmTest, DegradedFromReady) {
    DaemonLifecycleFsm fsm;
    fsm.dispatch(BootstrappedEvent{});
    fsm.dispatch(HealthyEvent{});
    ASSERT_EQ(stateOf(fsm), LifecycleState::Ready);
    fsm.dispatch(DegradedEvent{});
    EXPECT_EQ(stateOf(fsm), LifecycleState::Degraded);
}

TEST(DaemonLifecycleFsmTest, FailureFromVariousGoesFailed) {
    DaemonLifecycleFsm fsm;
    // From Unknown
    fsm.dispatch(FailureEvent{.error = "oops"});
    EXPECT_EQ(stateOf(fsm), LifecycleState::Failed);
    // From Failed -> Failure ignored
    fsm.dispatch(FailureEvent{.error = "again"});
    EXPECT_EQ(stateOf(fsm), LifecycleState::Failed);
}

TEST(DaemonLifecycleFsmTest, ShutdownRequestedTransitionsToStopping) {
    DaemonLifecycleFsm fsm;
    // From Unknown
    fsm.dispatch(ShutdownRequestedEvent{});
    EXPECT_EQ(stateOf(fsm), LifecycleState::Stopping);
}

TEST(DaemonLifecycleFsmTest, StoppedOnlyFromStoppingOrFailed) {
    DaemonLifecycleFsm fsm;
    // From Stopping -> Stopped
    fsm.dispatch(ShutdownRequestedEvent{});
    ASSERT_EQ(stateOf(fsm), LifecycleState::Stopping);
    fsm.dispatch(StoppedEvent{});
    EXPECT_EQ(stateOf(fsm), LifecycleState::Stopped);

    // From Failed -> Stopped
    fsm.reset();
    fsm.dispatch(FailureEvent{.error = "boom"});
    ASSERT_EQ(stateOf(fsm), LifecycleState::Failed);
    fsm.dispatch(StoppedEvent{});
    EXPECT_EQ(stateOf(fsm), LifecycleState::Stopped);
}

TEST(DaemonLifecycleFsmTest, DegradedAndRecoveryToReady) {
    DaemonLifecycleFsm fsm;
    fsm.dispatch(BootstrappedEvent{});
    fsm.dispatch(DegradedEvent{}); // from Initializing -> Degraded allowed
    ASSERT_EQ(stateOf(fsm), LifecycleState::Degraded);
    fsm.dispatch(HealthyEvent{});
    EXPECT_EQ(stateOf(fsm), LifecycleState::Ready);
}

TEST(DaemonLifecycleFsmTest, TickIsNoOp) {
    DaemonLifecycleFsm fsm;
    fsm.dispatch(BootstrappedEvent{});
    ASSERT_EQ(stateOf(fsm), LifecycleState::Initializing);
    fsm.tick();
    EXPECT_EQ(stateOf(fsm), LifecycleState::Initializing);
}
