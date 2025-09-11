#pragma once

#include <memory>
#include <string>
#include <variant>

#include <yams/plugins/object_storage_iface.hpp>
#include <yams/plugins/object_storage_v1.h>

namespace yams::plugins::adapter {

// Wrap a C ABI v1 object_storage into a C++ IObjectStorageBackend
std::shared_ptr<IObjectStorageBackend> wrap_c_abi(yams_object_storage_v1* v1_iface);

// Expose a C ABI v1 struct from a C++ IObjectStorageBackend implementation.
// The returned struct holds function pointers that forward into the C++ object.
// Lifetime: caller owns the returned struct; destroy() will delete the wrapper and release the
// impl.
yams_object_storage_v1* expose_as_c_abi(std::shared_ptr<IObjectStorageBackend> impl);

// Variant that also returns an opaque backend handle to pass into the function table
// when invoking methods directly without calling create(). The caller is responsible
// for eventually calling destroy(handle) via the function table to free resources.
std::pair<yams_object_storage_v1*, void*>
expose_as_c_abi_with_state(std::shared_ptr<IObjectStorageBackend> impl);

// Utility: serialize/deserialize options/metadata to/from JSON for C ABI calls.
std::string to_json(const PutOptions& opts);
std::string to_json(const GetOptions& opts);
std::string to_json(const ObjectMetadata& md);

// Parse minimal fields; unrecognized fields are ignored.
PutOptions parse_put_options(const char* json);
GetOptions parse_get_options(const char* json);

} // namespace yams::plugins::adapter
