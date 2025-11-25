#pragma once

#include <stdint.h>

#if defined(_WIN32) || defined(_WIN64)
#define YAMS_PLUGIN_API __declspec(dllexport)
#elif defined(__GNUC__) || defined(__clang__)
#define YAMS_PLUGIN_API __attribute__((visibility("default")))
#else
#define YAMS_PLUGIN_API
#endif

#ifdef __cplusplus
extern "C" {
#endif

#define YAMS_PLUGIN_ABI_VERSION 1

#define YAMS_PLUGIN_OK 0
#define YAMS_PLUGIN_ERR_INCOMPATIBLE -1
#define YAMS_PLUGIN_ERR_NOT_FOUND -2
#define YAMS_PLUGIN_ERR_INIT_FAILED -3
#define YAMS_PLUGIN_ERR_INVALID -4

YAMS_PLUGIN_API int yams_plugin_get_abi_version(void);
YAMS_PLUGIN_API const char* yams_plugin_get_name(void);
YAMS_PLUGIN_API const char* yams_plugin_get_version(void);
YAMS_PLUGIN_API const char* yams_plugin_get_manifest_json(void);
YAMS_PLUGIN_API int yams_plugin_init(const char* config_json, const void* host_context);
YAMS_PLUGIN_API void yams_plugin_shutdown(void);
YAMS_PLUGIN_API int yams_plugin_get_interface(const char* iface_id, uint32_t version,
                                              void** out_iface);
YAMS_PLUGIN_API int yams_plugin_get_health_json(char** out_json);

#ifdef __cplusplus
}
#endif
