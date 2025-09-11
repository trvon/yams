// Minimal shim for host services context creation/free to satisfy linkage in tests
// and daemon when the full app-services implementation is not linked.

#include <cstdlib>
#include <cstring>

#include <yams/daemon/resource/plugin_host_services.h>
#include <yams/plugins/host_services_v1.h>

yams_plugin_host_context_v1*
yams_create_host_context(const yams::app::services::AppContext& /*app_ctx*/) {
    auto* ctx =
        static_cast<yams_plugin_host_context_v1*>(std::malloc(sizeof(yams_plugin_host_context_v1)));
    if (!ctx)
        return nullptr;
    std::memset(ctx, 0, sizeof(*ctx));
    ctx->version = YAMS_PLUGIN_HOST_SERVICES_API_VERSION;
    // document/search/grep service pointers left null in shim
    return ctx;
}

void yams_free_host_context(void* host_ctx) {
    if (host_ctx)
        std::free(host_ctx);
}
