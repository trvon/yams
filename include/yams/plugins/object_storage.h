#pragma once

#include <yams/plugins/abi.h>
#include <yams/storage/storage_backend.h>

extern "C" {
using ObjectStorageCreateFn = yams::storage::IStorageBackend* (*)();
using ObjectStorageDestroyFn = void (*)(yams::storage::IStorageBackend*);

YAMS_PLUGIN_API yams::storage::IStorageBackend* yams_plugin_create_object_storage();
YAMS_PLUGIN_API void yams_plugin_destroy_object_storage(yams::storage::IStorageBackend* backend);
}
