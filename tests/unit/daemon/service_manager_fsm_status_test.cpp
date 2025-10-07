#include <gtest/gtest.h>
#include <yams/daemon/components/EmbeddingProviderFsm.h>
#include <yams/daemon/components/PluginHostFsm.h>
#include <yams/daemon/components/ServiceManagerFsm.h>

using namespace yams::daemon;

TEST(ServiceManagerFsmStatus, EnumValuesAreStableOrder) {
    // Ensure enum underlying ordering is defined for status export (no runtime logic).
    EXPECT_LT(static_cast<int>(ServiceManagerState::Uninitialized),
              static_cast<int>(ServiceManagerState::Ready));
    EXPECT_LT(static_cast<int>(EmbeddingProviderState::Unavailable),
              static_cast<int>(EmbeddingProviderState::ModelReady));
    EXPECT_LT(static_cast<int>(PluginHostState::NotInitialized),
              static_cast<int>(PluginHostState::Ready));
}
