#pragma once

#include <yams/daemon/components/AtomicSnapshotRegistry.h>
#include <yams/daemon/components/DaemonMetrics.h>

namespace yams::daemon {

// Zero-copy publication of the latest MetricsSnapshot for multi-reader consumers.
using MetricsSnapshotRegistry = AtomicSnapshotRegistry<MetricsSnapshot>;

} // namespace yams::daemon
