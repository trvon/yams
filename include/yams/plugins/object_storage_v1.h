#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct yams_object_storage_v1 yams_object_storage_v1;

typedef int (*yams_os_create_fn)(const char* config_json, void** out_backend);
typedef void (*yams_os_destroy_fn)(void* backend);
typedef int (*yams_os_put_fn)(void* backend, const char* key, const void* buf, size_t len,
                              const char* opts_json);
typedef int (*yams_os_get_fn)(void* backend, const char* key, void** out_buf, size_t* out_len,
                              const char* opts_json);
typedef int (*yams_os_head_fn)(void* backend, const char* key, char** out_metadata_json,
                               const char* opts_json);
typedef int (*yams_os_del_fn)(void* backend, const char* key, const char* opts_json);
typedef int (*yams_os_list_fn)(void* backend, const char* prefix, char** out_list_json,
                               const char* opts_json);

typedef struct yams_object_storage_v1 {
    uint32_t size;
    uint32_t version;
    yams_os_create_fn create;
    yams_os_destroy_fn destroy;
    yams_os_put_fn put;
    yams_os_get_fn get;
    yams_os_head_fn head;
    yams_os_del_fn del;
    yams_os_list_fn list;
} yams_object_storage_v1;

#ifdef __cplusplus
}
#endif
