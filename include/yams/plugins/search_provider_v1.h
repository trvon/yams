#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include "host_services_v1.h" // For yams_search_request_t and yams_search_response_t

#ifdef __cplusplus
extern "C" {
#endif

#define YAMS_IFACE_SEARCH_PROVIDER_V1_ID "search_provider_v1"
#define YAMS_IFACE_SEARCH_PROVIDER_V1_VERSION 1

// The search provider interface v-table
typedef struct {
    uint32_t abi_version;

    // Performs a search.
    // handle: The plugin's instance handle.
    // req: The search request.
    // callback: The function to call with the search results.
    // user_data: A user-defined pointer that will be passed to the callback.
    // Returns 0 on success, non-zero on failure.
    int (*search)(void* handle, const yams_search_request_t* req, yams_search_callback_t callback,
                  void* user_data);

} yams_search_provider_v1;

#ifdef __cplusplus
}
#endif
