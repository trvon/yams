#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <yams/compat/dlfcn.h>


#include <gtest/gtest.h>
#include <yams/plugins/abi.h>

// RAII handle for dlopen/dlclose in tests
struct TestPluginHandle {
    void* handle{nullptr};

    TestPluginHandle() = default;
    TestPluginHandle(const TestPluginHandle&) = delete;
    TestPluginHandle& operator=(const TestPluginHandle&) = delete;

    TestPluginHandle(TestPluginHandle&& o) noexcept : handle(o.handle) { o.handle = nullptr; }
    TestPluginHandle& operator=(TestPluginHandle&& o) noexcept {
        if (this != &o) {
            if (handle)
                dlclose(handle);
            handle = o.handle;
            o.handle = nullptr;
        }
        return *this;
    }

    ~TestPluginHandle() {
        if (handle)
            dlclose(handle);
    }

    bool open(const char* so_path, int flags = RTLD_LAZY | RTLD_LOCAL) {
        handle = dlopen(so_path, flags);
        return handle != nullptr;
    }

    template <class T> T sym(const char* name) const {
        return reinterpret_cast<T>(dlsym(handle, name));
    }
};

#ifndef YAMS_TRY_AUTO_DOWNLOAD
#define YAMS_TRY_AUTO_DOWNLOAD(lang) (false)
#endif

// Standardized skip for env/resource dependent plugins (grammars, models, etc.)
// rc: plugin call return code, out_ptr: optional result struct with `error` field, reason: context
#define PLUGIN_MISSING_SKIP(rc, out_ptr, reason)                                                   \
    do {                                                                                           \
        if ((rc) == YAMS_PLUGIN_ERR_NOT_FOUND || ((out_ptr) != nullptr && (out_ptr)->error)) {     \
            std::string _yams_skip_msg = (reason);                                                 \
            if ((out_ptr) && (out_ptr)->error) {                                                   \
                _yams_skip_msg += " — ";                                                           \
                _yams_skip_msg += (out_ptr)->error;                                                \
            }                                                                                      \
            GTEST_SKIP() << "Plugin missing dependency: " << _yams_skip_msg;                       \
        }                                                                                          \
    } while (0)
