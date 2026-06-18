#include <benchmark/benchmark.h>

#include <cstring>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <yams/compat/dlfcn.h>

extern "C" {
#include <yams/plugins/abi.h>
#include <yams/plugins/entity_extractor_v2.h>
}

namespace {

struct GlintPluginAPI {
    void* handle{};
    yams_entity_extractor_v2* api{};

    GlintPluginAPI() = default;
    GlintPluginAPI(const GlintPluginAPI&) = delete;
    GlintPluginAPI& operator=(const GlintPluginAPI&) = delete;
    GlintPluginAPI(GlintPluginAPI&&) noexcept = default;
    GlintPluginAPI& operator=(GlintPluginAPI&&) noexcept = default;

    ~GlintPluginAPI() {
        // Avoid dlclose during benchmark teardown; plugin tests use same pattern.
    }
};

std::optional<GlintPluginAPI> loadGlintPlugin() {
    GlintPluginAPI p;

#ifdef __APPLE__
    const char* candidates[] = {
        "build/debug/plugins/glint/yams_glint.dylib",
        "builddir/plugins/glint/yams_glint.dylib",
        "plugins/glint/yams_glint.dylib",
    };
#elif defined(_WIN32)
    const char* candidates[] = {
        "build/debug/plugins/glint/yams_glint.dll",
        "builddir/plugins/glint/yams_glint.dll",
        "plugins/glint/yams_glint.dll",
    };
#else
    const char* candidates[] = {
        "build/debug/plugins/glint/yams_glint.so",
        "builddir/plugins/glint/yams_glint.so",
        "plugins/glint/yams_glint.so",
    };
#endif

    for (const char* path : candidates) {
        p.handle = dlopen(path, RTLD_LAZY | RTLD_LOCAL);
        if (!p.handle) {
            continue;
        }

        auto getiface = reinterpret_cast<int (*)(const char*, uint32_t, void**)>(
            dlsym(p.handle, "yams_plugin_get_interface"));
        auto init = reinterpret_cast<int (*)(const char*, const void*)>(
            dlsym(p.handle, "yams_plugin_init"));
        if (!getiface || !init) {
            continue;
        }
        if (init(nullptr, nullptr) != 0) {
            continue;
        }

        void* ptr = nullptr;
        int rc =
            getiface(YAMS_IFACE_ENTITY_EXTRACTOR_V2, YAMS_IFACE_ENTITY_EXTRACTOR_V2_VERSION, &ptr);
        if (rc == 0 && ptr) {
            p.api = reinterpret_cast<yams_entity_extractor_v2*>(ptr);
            std::optional<GlintPluginAPI> out;
            out.emplace(std::move(p));
            return out;
        }
    }
    return std::nullopt;
}

GlintPluginAPI& plugin() {
    static auto loaded = loadGlintPlugin();
    if (!loaded || !loaded->api) {
        throw std::runtime_error("glint plugin not available");
    }
    return *loaded;
}

std::string makeText(std::size_t repeats) {
    std::string text;
    text.reserve(repeats * 96);
    for (std::size_t i = 0; i < repeats; ++i) {
        text += "Albert Einstein developed relativity at Princeton while Google hired Ada Lovelace "
                "in 1998. ";
    }
    return text;
}

void BM_GlintSupportsTextPlain(benchmark::State& state) {
    auto& p = plugin();
    for (auto _ : state) {
        benchmark::DoNotOptimize(p.api->supports(p.api->self, "text/plain"));
    }
}

void BM_GlintExtractShortText(benchmark::State& state) {
    auto& p = plugin();
    static const std::string text = makeText(1);
    for (auto _ : state) {
        yams_entity_extraction_result_v2* result = nullptr;
        int rc = p.api->extract(p.api->self, text.data(), text.size(), nullptr, &result);
        benchmark::DoNotOptimize(rc);
        if (result) {
            benchmark::DoNotOptimize(result->entity_count);
            p.api->free_result(p.api->self, result);
        }
    }
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * text.size()));
}

void BM_GlintExtractMediumText(benchmark::State& state) {
    auto& p = plugin();
    static const std::string text = makeText(8);
    for (auto _ : state) {
        yams_entity_extraction_result_v2* result = nullptr;
        int rc = p.api->extract(p.api->self, text.data(), text.size(), nullptr, &result);
        benchmark::DoNotOptimize(rc);
        if (result) {
            benchmark::DoNotOptimize(result->entity_count);
            p.api->free_result(p.api->self, result);
        }
    }
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * text.size()));
}

void BM_GlintExtractLongText(benchmark::State& state) {
    auto& p = plugin();
    static const std::string text = makeText(32);
    for (auto _ : state) {
        yams_entity_extraction_result_v2* result = nullptr;
        int rc = p.api->extract(p.api->self, text.data(), text.size(), nullptr, &result);
        benchmark::DoNotOptimize(rc);
        if (result) {
            benchmark::DoNotOptimize(result->entity_count);
            p.api->free_result(p.api->self, result);
        }
    }
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * text.size()));
}

} // namespace

BENCHMARK(BM_GlintSupportsTextPlain)->Unit(benchmark::kNanosecond);
BENCHMARK(BM_GlintExtractShortText)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_GlintExtractMediumText)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_GlintExtractLongText)->Unit(benchmark::kMicrosecond);

BENCHMARK_MAIN();
