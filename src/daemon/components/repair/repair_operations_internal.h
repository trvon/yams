// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

// Factories for the individual repair operations, one per ops/*.cpp; the registry owns them.
#pragma once

#include <yams/daemon/components/repair/repair_operation.h>

#include <memory>

namespace yams::daemon::repair {

std::unique_ptr<IRepairOperation> makeStuckDocumentsOperation();
std::unique_ptr<IRepairOperation> makeOrphanedMetadataOperation();
std::unique_ptr<IRepairOperation> makeMimeTypesOperation();
std::unique_ptr<IRepairOperation> makeDownloadsOperation();
std::unique_ptr<IRepairOperation> makePathTreeOperation();
std::unique_ptr<IRepairOperation> makeSemanticDedupeOperation();
std::unique_ptr<IRepairOperation> makeOrphanedChunksOperation();
std::unique_ptr<IRepairOperation> makeBlockReferencesOperation();
std::unique_ptr<IRepairOperation> makeKnowledgeGraphOperation();
std::unique_ptr<IRepairOperation> makeFts5IndexOperation();
std::unique_ptr<IRepairOperation> makeMissingEmbeddingsOperation();
std::unique_ptr<IRepairOperation> makeTopologyArtifactsOperation();
std::unique_ptr<IRepairOperation> makeOptimizeDatabaseOperation();

} // namespace yams::daemon::repair
