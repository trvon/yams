#pragma once

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define YAMS_PLUGIN_ABI_VERSION 1

#define YAMS_PLUGIN_OK 0
#define YAMS_PLUGIN_ERR_INCOMPATIBLE -1
#define YAMS_PLUGIN_ERR_NOT_FOUND -2
#define YAMS_PLUGIN_ERR_INIT_FAILED -3
#define YAMS_PLUGIN_ERR_INVALID -4

int yams_plugin_get_abi_version(void);
const char* yams_plugin_get_name(void);
const char* yams_plugin_get_version(void);
const char* yams_plugin_get_manifest_json(void);
int yams_plugin_init(const char* config_json, const void* host_context);
void yams_plugin_shutdown(void);
int yams_plugin_get_interface(const char* iface_id, uint32_t version, void** out_iface);
int yams_plugin_get_health_json(char** out_json);

#ifdef __cplusplus
}
#endif
