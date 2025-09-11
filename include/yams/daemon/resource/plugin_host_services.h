#pragma once

#include <yams/plugins/host_services_v1.h>

namespace yams::app::services {
struct AppContext;
}

// These functions are implemented in plugin_host_services.cpp
yams_plugin_host_context_v1*
yams_create_host_context(const yams::app::services::AppContext& app_ctx);
void yams_free_host_context(void* host_ctx);
