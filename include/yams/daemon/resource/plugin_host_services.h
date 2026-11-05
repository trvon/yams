#pragma once

#include <yams/plugins/host_services_v1.h>

#include <functional>

namespace yams::app::services {
struct AppContext;
}

// These functions are implemented by the daemon host-services bridge.
yams_plugin_host_context_v1*
yams_create_host_context(const yams::app::services::AppContext& app_ctx);
void yams_free_host_context(void* host_ctx);

// Sets the function that answers plugins' extraction budget requests through
// the host context's resource_service. The daemon wires it to the
// ResourceGovernor. Without a provider, requests fail and plugins use their own
// defaults. Pass an empty function to clear it.
void yams_set_extraction_budget_provider(std::function<yams_extraction_budget_v1()> provider);
