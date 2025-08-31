#include <yams/plugins/object_storage.h>
#include <yams/storage/storage_backend.h>

using namespace yams::storage;

extern "C" yams::storage::IStorageBackend* yams_plugin_create_object_storage() {
    return new URLBackend();
}

extern "C" void yams_plugin_destroy_object_storage(yams::storage::IStorageBackend* backend) {
    delete backend;
}
