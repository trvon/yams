// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <yams/daemon/components/GraphComponent.h>
#include <yams/metadata/metadata_repository.h>

namespace yams::metadata {

void MetadataRepository::setGraphComponent(
    std::shared_ptr<yams::daemon::GraphComponent> graphComponent) {
    if (!graphComponent) {
        treeDiffAppliedCallback_ = {};
        return;
    }
    treeDiffAppliedCallback_ = [graphComponent = std::move(graphComponent)](
                                   int64_t diffId,
                                   const std::vector<TreeChangeRecord>& changes) -> Result<void> {
        return graphComponent->onTreeDiffApplied(diffId, changes);
    };
}

} // namespace yams::metadata
