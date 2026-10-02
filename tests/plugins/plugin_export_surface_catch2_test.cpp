// Plugin export surface: a plugin's dynamic symbol table is its ABI. Only the
// yams_plugin_* C entry points may be visible; statically linked third-party
// code (OpenSSL, libcurl, zlib, spdlog, ...) and plugin internals must not be,
// otherwise a host that loads the plugin with RTLD_GLOBAL, or another library
// in the process, can bind to the plugin's private copies.

#include <catch2/catch_test_macros.hpp>
#include <yams/compat/dlfcn.h>

#include <string>
#include <vector>

namespace {

struct PluginHandle {
    explicit PluginHandle(const char* path) : handle(dlopen(path, RTLD_LAZY | RTLD_LOCAL)) {}
    ~PluginHandle() {
        if (handle) {
            dlclose(handle);
        }
    }
    PluginHandle(const PluginHandle&) = delete;
    PluginHandle& operator=(const PluginHandle&) = delete;
    void* handle;
};

const std::vector<std::string> kRequiredEntryPoints = {
    "yams_plugin_get_abi_version",
    "yams_plugin_get_name",
    "yams_plugin_get_version",
    "yams_plugin_get_manifest_json",
    "yams_plugin_init",
    "yams_plugin_shutdown",
    "yams_plugin_get_interface",
    "yams_plugin_get_health_json",
};

void requireEntryPoints(void* handle) {
    for (const auto& name : kRequiredEntryPoints) {
        INFO("entry point " << name);
        CHECK(dlsym(handle, name.c_str()) != nullptr);
    }
}

void requireHidden(void* handle, const std::vector<std::string>& names) {
    for (const auto& name : names) {
        INFO("symbol must not be exported: " << name);
        CHECK(dlsym(handle, name.c_str()) == nullptr);
    }
}

} // namespace

#ifdef YAMS_S3_PLUGIN_PATH
TEST_CASE("S3 plugin exports only the plugin C ABI", "[plugins][s3][abi][catch2]") {
    PluginHandle plugin(YAMS_S3_PLUGIN_PATH);
    REQUIRE(plugin.handle != nullptr);
    requireEntryPoints(plugin.handle);
    // Legacy object-storage factory used by ObjectStoragePluginLoader.
    CHECK(dlsym(plugin.handle, "yams_plugin_create_object_storage") != nullptr);
    CHECK(dlsym(plugin.handle, "yams_plugin_destroy_object_storage") != nullptr);
    // Statically linked libcurl, OpenSSL and zlib stay private.
    requireHidden(plugin.handle, {"curl_easy_init", "curl_global_init", "SSL_new", "SSL_CTX_new",
                                  "EVP_sha256", "OPENSSL_init_ssl", "deflate", "inflate"});
}
#endif

#ifdef YAMS_ZYP_PLUGIN_PATH
TEST_CASE("zyp plugin exports only the plugin C ABI", "[plugins][zyp][abi][catch2]") {
    PluginHandle plugin(YAMS_ZYP_PLUGIN_PATH);
    REQUIRE(plugin.handle != nullptr);
    requireEntryPoints(plugin.handle);
    // The zpdf wrapper and PDF metadata parser are implementation details.
    requireHidden(plugin.handle,
                  {"_ZN4yams3zyp10TextBufferD1Ev", "_ZN4yams3zyp8DocumentC1EPv",
                   "_ZN4yams3zyp15extractMetadataESt4spanIKhLm18446744073709551615EE"});
}
#endif
