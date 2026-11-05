// Minimal shim for host services context creation/free to satisfy linkage in tests
// and daemon when the full app-services implementation is not linked.

#include <cstdlib>
#include <cstring>
#include <mutex>

#include <yams/daemon/resource/plugin_host_services.h>
#include <yams/plugins/host_services_v1.h>

namespace {

std::mutex g_budgetMutex;
std::function<yams_extraction_budget_v1()> g_budgetProvider;

int getExtractionBudget(void* /*handle*/, yams_extraction_budget_v1* out) {
    if (!out)
        return 1;
    std::function<yams_extraction_budget_v1()> provider;
    {
        std::lock_guard lock(g_budgetMutex);
        provider = g_budgetProvider;
    }
    if (!provider)
        return 1;
    try {
        *out = provider();
    } catch (...) {
        return 1;
    }
    if (out->max_threads == 0)
        out->max_threads = 1;
    return 0;
}

yams_resource_service_v1 g_resourceService{nullptr, &getExtractionBudget};

} // namespace

void yams_set_extraction_budget_provider(std::function<yams_extraction_budget_v1()> provider) {
    std::lock_guard lock(g_budgetMutex);
    g_budgetProvider = std::move(provider);
}

yams_plugin_host_context_v1*
yams_create_host_context(const yams::app::services::AppContext& /*app_ctx*/) {
    auto* ctx =
        static_cast<yams_plugin_host_context_v1*>(std::malloc(sizeof(yams_plugin_host_context_v1)));
    if (!ctx)
        return nullptr;
    std::memset(ctx, 0, sizeof(*ctx));
    ctx->version = YAMS_PLUGIN_HOST_SERVICES_API_VERSION;
    // document/search/grep service pointers left null in shim
    ctx->resource_service = &g_resourceService;
    return ctx;
}

void yams_free_host_context(void* host_ctx) {
    if (host_ctx)
        std::free(host_ctx);
}
