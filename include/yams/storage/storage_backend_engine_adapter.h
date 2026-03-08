#pragma once

#include <memory>

#include <yams/storage/storage_backend.h>
#include <yams/storage/storage_engine.h>

namespace yams::storage {

// Wrap an IStorageBackend so it can be consumed as an IStorageEngine.
// Returns nullptr when backend is null.
std::shared_ptr<IStorageEngine>
createStorageEngineFromBackend(std::unique_ptr<IStorageBackend> backend);

} // namespace yams::storage
