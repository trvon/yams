#include <yams/cli/doctor/benchmark.h>
#include <yams/cli/ui_helpers.hpp>
#include <yams/cli/yams_cli.h>
#include <yams/config/config_helpers.h>
#include <yams/search/benchmark_history_store.h>
#include <yams/search/internal_benchmark.h>

#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>

#include <fstream>

namespace yams::cli::doctor {

BenchmarkCommand::BenchmarkCommand(YamsCLI* cli, Config config)
    : cli_(cli), config_(std::move(config)) {}

void BenchmarkCommand::execute(std::ostream& os) {
    using namespace yams::cli::ui;
    using namespace yams::search;

    os << "\n" << section_header("Search Quality Benchmark") << "\n\n";

    if (!cli_) {
        os << "  " << status_error("CLI context unavailable") << "\n";
        return;
    }

    auto ensured = cli_->ensureStorageInitialized();
    if (!ensured) {
        os << "  " << status_error("Storage init failed: " + ensured.error().message) << "\n";
        return;
    }

    auto appCtx = cli_->getAppContext();
    if (!appCtx) {
        os << "  " << status_error("AppContext unavailable") << "\n";
        return;
    }
    if (!appCtx->searchEngine) {
        os << "  " << status_error("Search engine unavailable") << "\n";
        return;
    }
    if (!appCtx->metadataRepo) {
        os << "  " << status_error("Metadata repository unavailable") << "\n";
        return;
    }

    InternalBenchmark benchmark(appCtx->searchEngine, appCtx->metadataRepo);
    BenchmarkConfig cfg;
    cfg.queryCount = config_.queryCount;
    cfg.k = 10;
    cfg.includeExecutions = config_.verbose;
    cfg.verbose = config_.verbose;
    cfg.warmupQueries = std::min(size_t(5), config_.queryCount / 10);

    SyntheticQueryGenerator gen(appCtx->metadataRepo);
    auto docCount = gen.getAvailableDocumentCount();
    if (!docCount) {
        os << "  " << status_error("Failed to query corpus: " + docCount.error().message) << "\n";
        return;
    }
    if (docCount.value() == 0) {
        os << "  " << status_error("No documents indexed - nothing to benchmark") << "\n";
        return;
    }

    os << "  " << colorize("Corpus size:", Ansi::CYAN) << " " << format_number(docCount.value())
       << " documents\n";
    os << "  " << colorize("Queries:", Ansi::CYAN) << " " << config_.queryCount << "\n";
    os << "  " << colorize("K value:", Ansi::CYAN) << " " << cfg.k << "\n\n";

    os << status_pending("Generating synthetic queries") << "\n";
    auto results = benchmark.run(cfg);
    if (!results) {
        os << "  " << status_error("Benchmark failed: " + results.error().message) << "\n";
        return;
    }
    const auto& benchResults = results.value();

    if (config_.json) {
        os << benchResults.toJson().dump(2) << "\n";
    } else {
        os << benchResults.summary();
    }

    // Append history
    try {
        BenchmarkHistoryStore history(yams::config::get_data_dir() / "benchmark_history.json");
        BenchmarkHistoryStore::Row row;
        row.results = benchResults;
        row.configHash = std::to_string(cfg.queryCount) + "-" + std::to_string(cfg.k) + "-" +
                         std::to_string(docCount.value());
        auto appended = history.append(row);
        if (!appended)
            spdlog::warn("Failed to persist benchmark history row: {}", appended.error().message);
    } catch (const std::exception& e) {
        spdlog::warn("Benchmark history append raised: {}", e.what());
    }

    // Save baseline
    if (!config_.saveBaseline.empty()) {
        try {
            std::ofstream out(config_.saveBaseline);
            if (!out)
                os << "  " << status_error("Cannot write baseline to: " + config_.saveBaseline)
                   << "\n";
            else {
                out << benchResults.toJson().dump(2);
                os << "\n" << status_ok("Baseline saved to: " + config_.saveBaseline) << "\n";
            }
        } catch (const std::exception& e) {
            os << "  " << status_error("Failed to save baseline: " + std::string(e.what())) << "\n";
        }
    }

    // Compare baseline
    if (!config_.compareBaseline.empty()) {
        try {
            std::ifstream in(config_.compareBaseline);
            if (!in) {
                os << "  " << status_error("Cannot read baseline from: " + config_.compareBaseline)
                   << "\n";
            } else {
                nlohmann::json baselineJson = nlohmann::json::parse(in);
                BenchmarkResults baseline;
                baseline.mrr = baselineJson.value("mrr", 0.0f);
                baseline.recallAtK = baselineJson.value("recall_at_k", 0.0f);
                baseline.latency.meanMs = baselineJson.contains("latency")
                                              ? baselineJson["latency"].value("mean_ms", 0.0)
                                              : 0.0;

                auto comparison =
                    InternalBenchmark::compare(baseline, benchResults, cfg.regressionThreshold);
                os << "\n" << subsection_header("Baseline Comparison") << "\n\n";

                os << "  " << colorize("MRR delta:", Ansi::CYAN) << " ";
                if (comparison.mrrDelta > 0)
                    os << colorize("+" + std::to_string(comparison.mrrDelta), Ansi::GREEN);
                else if (comparison.mrrDelta < 0)
                    os << colorize(std::to_string(comparison.mrrDelta), Ansi::RED);
                else
                    os << "0.0";
                os << "\n";

                os << "  " << colorize("Recall@K delta:", Ansi::CYAN) << " ";
                if (comparison.recallDelta > 0)
                    os << colorize("+" + std::to_string(comparison.recallDelta), Ansi::GREEN);
                else if (comparison.recallDelta < 0)
                    os << colorize(std::to_string(comparison.recallDelta), Ansi::RED);
                else
                    os << "0.0";
                os << "\n";

                os << "  " << colorize("Latency delta:", Ansi::CYAN) << " ";
                if (comparison.latencyDelta < 0)
                    os << colorize(std::to_string(comparison.latencyDelta) + " ms (faster)",
                                   Ansi::GREEN);
                else if (comparison.latencyDelta > 0)
                    os << colorize("+" + std::to_string(comparison.latencyDelta) + " ms (slower)",
                                   Ansi::YELLOW);
                else
                    os << "0.0 ms";
                os << "\n\n";

                if (comparison.isRegression)
                    os << status_error("REGRESSION DETECTED: " + comparison.summary) << "\n";
                else if (comparison.isImprovement)
                    os << status_ok("IMPROVEMENT: " + comparison.summary) << "\n";
                else
                    os << status_ok("No significant change") << "\n";
            }
        } catch (const nlohmann::json::parse_error& e) {
            os << "  " << status_error("Invalid baseline JSON: " + std::string(e.what())) << "\n";
        } catch (const std::exception& e) {
            os << "  " << status_error("Failed to compare baseline: " + std::string(e.what()))
               << "\n";
        }
    }
}

} // namespace yams::cli::doctor
