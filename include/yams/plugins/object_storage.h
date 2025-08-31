#pragma once

#include <yams/storage/storage_backend.h>

extern "C" {
using ObjectStorageCreateFn = yams::storage::IStorageBackend* (*)();
using ObjectStorageDestroyFn = void (*)(yams::storage::IStorageBackend*);

yams::storage::IStorageBackend* yams_plugin_create_object_storage();
void yams_plugin_destroy_object_storage(yams::storage::IStorageBackend* backend);
}
