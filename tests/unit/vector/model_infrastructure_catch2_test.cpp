#include <catch2/catch_test_macros.hpp>

#include <yams/vector/batch_metrics.h>
#include <yams/vector/model_cache.h>
#include <yams/vector/model_loader.h>
#include <yams/vector/model_registry.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>

namespace fs = std::filesystem;
using namespace yams::vector;

namespace {

fs::path tempDir() {
    auto root = fs::temp_directory_path() /
                ("yams_vector_infra_" +
                 std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    fs::create_directories(root);
    return root;
}

ModelInfo makeModel(const fs::path& path, std::string id, size_t dim = 384) {
    ModelInfo info;
    info.model_id = std::move(id);
    info.name = "mini";
    info.version = "1.0.0";
    info.path = path.string();
    info.format = "ONNX";
    info.embedding_dimension = dim;
    info.model_size_bytes = fs::file_size(path);
    info.throughput_per_sec = 10.0;
    info.is_available = true;
    return info;
}

} // namespace

TEST_CASE("Batch metrics singleton records successes failures and backoffs", "[vector][infra]") {
    auto& metrics = batchmetrics::get();
    metrics.effectiveTokens.store(0);
    metrics.recentAvgDocs.store(0);
    metrics.successes.store(0);
    metrics.failures.store(0);
    metrics.backoffs.store(0);

    batchmetrics::set_effective_tokens(123);
    batchmetrics::record_success(10);
    batchmetrics::record_success(20);
    batchmetrics::record_failure();
    batchmetrics::record_backoff();

    CHECK((metrics.effectiveTokens.load() == 123U));
    CHECK((metrics.successes.load() == 2U));
    CHECK((metrics.failures.load() == 1U));
    CHECK((metrics.backoffs.load() == 1U));
    CHECK((metrics.recentAvgDocs.load() > 0U));
}

TEST_CASE("Model discovery registry and compatibility cover happy and fail-soft paths",
          "[vector][infra]") {
    auto root = tempDir();
    auto onnx = root / "mini-v1.2.3.onnx";
    std::ofstream(onnx, std::ios::binary) << "not a real onnx but enough for mock loader";
    std::ofstream(root / "mini-v1.2.3.json") << R"({"dimension": 768})";

    CHECK((discovery::getModelFormat(onnx.string()) == "ONNX"));
    CHECK((discovery::getModelFormat((root / "x.unknown").string()) == "Unknown"));
    CHECK((discovery::extractVersion("model-v2.3.4.onnx") == "2.3.4"));
    CHECK((discovery::extractVersion("model.onnx") == "1.0.0"));

    auto missing = discovery::parseModelMetadata((root / "missing.onnx").string());
    CHECK_FALSE(missing.has_value());

    auto parsed = discovery::parseModelMetadata(onnx.string());
    REQUIRE(parsed.has_value());
    CHECK((parsed.value().name == "mini"));
    CHECK((parsed.value().version == "1.2.3"));
    CHECK((parsed.value().embedding_dimension == 768U));

    ModelRegistry registry;
    auto info = parsed.value();
    info.throughput_per_sec = 15.0;
    REQUIRE(registry.registerModel(info).has_value());
    CHECK(registry.hasModel(info.model_id));
    CHECK_FALSE(registry.registerModel(info).has_value());

    auto byDim = registry.getModelsByDimension(768);
    REQUIRE(byDim.has_value());
    CHECK((byDim.value().size() == 1U));

    auto best = registry.selectBestModel(768, {{"min_throughput", "5"}});
    REQUIRE(best.has_value());
    CHECK((best.value() == info.model_id));

    auto noMatch = registry.selectBestModel(768, {{"gpu", "true"}});
    CHECK_FALSE(noMatch.has_value());

    REQUIRE(registry.updateMetrics(info.model_id, 4.0, true).has_value());
    REQUIRE(registry.updateMetrics(info.model_id, 0.0, false).has_value());
    auto stats = registry.getStats();
    CHECK((stats.total_models == 1U));
    CHECK((stats.available_models == 1U));
    CHECK((stats.models_by_dimension[768] == 1U));

    auto sameFamily = info;
    sameFamily.model_id = "mini_2.0.0";
    sameFamily.version = "2.0.0";
    CHECK(ModelCompatibilityChecker::areCompatible(info, sameFamily));
    sameFamily.embedding_dimension = 1024;
    CHECK_FALSE(ModelCompatibilityChecker::areCompatible(info, sameFamily));

    REQUIRE(registry.unregisterModel(info.model_id).has_value());
    CHECK_FALSE(registry.hasModel(info.model_id));

    std::error_code ec;
    fs::remove_all(root, ec);
}

TEST_CASE("ModelLoader validates detects and loads mock ONNX models", "[vector][infra]") {
    auto root = tempDir();
    auto onnx = root / "all-MiniLM-L6-v2.onnx";
    std::ofstream(onnx, std::ios::binary) << "mock";

    ModelLoader loader;
    CHECK(loader.supportsFormat("ONNX"));
    CHECK_FALSE(loader.supportsFormat("TFLite"));

    auto detected = loader.detectFormat(onnx.string());
    REQUIRE(detected.has_value());
    CHECK((detected.value() == "ONNX"));

    auto validation = loader.validateModel(onnx.string());
    REQUIRE(validation.has_value());
    CHECK(validation.value().is_valid);
    CHECK((validation.value().max_sequence_length == 512U));

    LoadOptions options;
    options.enable_optimization = false;
    auto loaded = loader.loadModel(onnx.string(), options);
    REQUIRE(loaded.has_value());
    CHECK((loader.getLoadedModelCount() == 1U));

    auto asyncLoaded = loader.loadModelAsync(onnx.string(), options).get();
    REQUIRE(asyncLoaded.has_value());
    CHECK((loader.getStats().successful_loads >= 2U));

    auto unknown = loader.detectFormat((root / "model.bin").string());
    CHECK_FALSE(unknown.has_value());

    std::error_code ec;
    fs::remove_all(root, ec);
}

TEST_CASE("ModelCache memory manager and warmup utilities cover cache helpers", "[vector][infra]") {
    CacheMemoryManager memory(100);
    CHECK(memory.canAllocate(40));
    REQUIRE(memory.allocate("a", 40).has_value());
    CHECK((memory.getUsedMemory() == 40U));
    CHECK((memory.getAvailableMemory() == 60U));
    CHECK_FALSE(memory.isUnderPressure());
    REQUIRE(memory.allocate("b", 55).has_value());
    CHECK(memory.isUnderPressure());
    CHECK((memory.getMemoryPressure() > 0.9));
    CHECK_FALSE(memory.allocate("c", 10).has_value());
    memory.deallocate("a");
    CHECK((memory.getUsedMemory() == 55U));

    ModelCacheConfig cfg;
    cfg.max_memory_bytes = 1;
    cfg.enable_preloading = false;
    cfg.enable_warmup = false;
    ModelCache cache(cfg);
    CHECK_FALSE(cache.initialize(nullptr).has_value());
    CHECK_FALSE(cache.getModel("missing").has_value());
    CHECK_FALSE(cache.unloadModel("missing").has_value());
    CHECK((cache.getModelCount() == 0U));
    CHECK((cache.getAvailableMemory() == 1U));

    auto samples = warming::generateWarmupSamples(3, 12);
    REQUIRE((samples.size() == 3U));
    CHECK((samples.front().size() == 12U));
    CHECK_FALSE(warming::warmupWithSamples(nullptr, samples).has_value());
    auto dummy = std::make_shared<int>(42);
    CHECK(warming::warmupWithSamples(dummy, samples).has_value());
}
