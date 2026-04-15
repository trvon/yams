#include <yams/cli/tune_runner.h>

#include <yams/cli/ui_helpers.hpp>
#include <yams/cli/yams_cli.h>
#include <yams/config/config_helpers.h>
#include <yams/search/internal_benchmark.h>
#include <yams/search/relevance_label_store.h>
#include <yams/search/search_engine.h>
#include <yams/search/search_tuner.h>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <system_error>
#include <vector>

#ifdef _WIN32
#include <io.h>
#else
#include <unistd.h>
#endif

namespace yams::cli {

namespace {

bool isStdinTty() {
#ifdef _WIN32
    return _isatty(_fileno(stdin)) != 0;
#else
    return ::isatty(fileno(stdin)) != 0;
#endif
}

std::string iso8601UtcNow() {
    const auto now = std::chrono::system_clock::now();
    const std::time_t t = std::chrono::system_clock::to_time_t(now);
    std::tm tm{};
#ifdef _WIN32
    gmtime_s(&tm, &t);
#else
    gmtime_r(&t, &tm);
#endif
    std::ostringstream os;
    os << std::put_time(&tm, "%Y-%m-%dT%H:%M:%SZ");
    return os.str();
}

std::string truncatePreview(const std::string& s, std::size_t max) {
    if (s.size() <= max) {
        return s;
    }
    return s.substr(0, max) + "...";
}

} // namespace

void runInteractiveTune(YamsCLI* cli, const TuneOptions& opts) {
    using namespace yams::cli::ui;
    using namespace yams::search;

    std::cout << "\n" << section_header("Interactive Relevance Tuner") << "\n\n";

    if (opts.nonInteractive || !isStdinTty()) {
        std::cout
            << "  " << status_ok("Non-interactive mode")
            << "\n  This command presents top-K search results and asks you to label relevance.\n"
            << "  Run from a terminal (TTY) to start labeling. Synthetic queries are generated\n"
            << "  from your own corpus, and labeled sessions are appended to\n"
            << "  " << (yams::config::get_data_dir() / "relevance_labels.jsonl").string() << "\n";
        return;
    }

    if (!cli) {
        std::cout << "  " << status_error("CLI context unavailable") << "\n";
        return;
    }

    try {
        auto ensured = cli->ensureStorageInitialized();
        if (!ensured) {
            std::cout << "  " << status_error("Storage init failed: " + ensured.error().message)
                      << "\n";
            return;
        }

        auto appCtx = cli->getAppContext();
        if (!appCtx || !appCtx->searchEngine || !appCtx->metadataRepo) {
            std::cout << "  " << status_error("Search engine or metadata repo unavailable") << "\n";
            return;
        }

        SyntheticQueryGenerator gen(appCtx->metadataRepo);
        auto docCount = gen.getAvailableDocumentCount();
        if (!docCount || docCount.value() == 0) {
            std::cout << "  " << status_ok("No documents indexed yet — add content before tuning.")
                      << "\n";
            return;
        }

        QueryGeneratorConfig qcfg;
        qcfg.queryCount = opts.queries;
        if (opts.seed != 0) {
            qcfg.seed = opts.seed;
        } else {
            qcfg.seed = static_cast<std::uint64_t>(
                std::chrono::system_clock::now().time_since_epoch().count());
        }

        auto queriesResult = gen.generate(qcfg);
        if (!queriesResult || queriesResult.value().empty()) {
            std::cout << "  "
                      << status_error("Could not generate synthetic queries from the corpus")
                      << "\n";
            return;
        }
        auto queries = std::move(queriesResult.value());

        std::cout << "  " << colorize("Corpus:", Ansi::CYAN) << " "
                  << format_number(docCount.value()) << " documents\n";
        std::cout << "  " << colorize("Queries to label:", Ansi::CYAN) << " " << queries.size()
                  << "   " << colorize("Top-K per query:", Ansi::CYAN) << " " << opts.k << "\n\n";
        std::cout << "  Label each result: [y]es / [n]o / [s]kip / [q]uit session\n\n";

        RelevanceSession session;
        session.timestamp = iso8601UtcNow();
        session.source = "interactive";
        session.k = opts.k;
        session.configHash = std::to_string(queries.size()) + "-" + std::to_string(opts.k) + "-" +
                             std::to_string(docCount.value());

        SearchParams params;
        params.limit = static_cast<int>(opts.k);

        bool userQuit = false;
        for (std::size_t qi = 0; qi < queries.size() && !userQuit; ++qi) {
            const auto& sq = queries[qi];
            std::cout << colorize("Query " + std::to_string(qi + 1) + "/" +
                                      std::to_string(queries.size()) + ":",
                                  Ansi::BOLD)
                      << " " << sq.text << "\n";

            auto searchRes = appCtx->searchEngine->search(sq.text, params);
            if (!searchRes) {
                std::cout << "  " << status_error("Search failed: " + searchRes.error().message)
                          << "\n\n";
                continue;
            }

            const auto& results = searchRes.value();
            LabeledQuery lq;
            lq.queryText = sq.text;

            if (results.empty()) {
                std::cout << "  " << status_ok("(no results)") << "\n\n";
                session.queries.push_back(std::move(lq));
                continue;
            }

            const std::size_t labelCount = std::min<std::size_t>(opts.k, results.size());
            for (std::size_t i = 0; i < labelCount && !userQuit; ++i) {
                const auto& r = results[i];
                const auto& doc = r.document;
                lq.rankedDocHashes.push_back(doc.sha256Hash);

                std::cout << "  [" << (i + 1) << "] " << colorize(doc.filePath, Ansi::CYAN) << "\n";
                if (!r.snippet.empty()) {
                    std::cout << "      " << truncatePreview(r.snippet, 180) << "\n";
                }
                std::cout << "      score=" << std::fixed << std::setprecision(3) << r.score
                          << "   label [y/n/s/q]? " << std::flush;

                std::string input;
                if (!std::getline(std::cin, input)) {
                    userQuit = true;
                    break;
                }
                char c =
                    input.empty()
                        ? 's'
                        : static_cast<char>(std::tolower(static_cast<unsigned char>(input[0])));
                RelevanceLabel label = RelevanceLabel::Unknown;
                switch (c) {
                    case 'y':
                        label = RelevanceLabel::Relevant;
                        break;
                    case 'n':
                        label = RelevanceLabel::NotRelevant;
                        break;
                    case 'q':
                        userQuit = true;
                        label = RelevanceLabel::Unknown;
                        break;
                    default:
                        label = RelevanceLabel::Unknown;
                        break;
                }
                lq.labels.push_back(label);
            }

            double gain = 0.0;
            double maxGain = 0.0;
            for (std::size_t i = 0; i < lq.labels.size(); ++i) {
                const double disc = 1.0 / std::log2(static_cast<double>(i + 2));
                maxGain += disc;
                if (lq.labels[i] == RelevanceLabel::Relevant) {
                    gain += disc;
                }
            }
            lq.reward = (maxGain > 0.0) ? (gain / maxGain) : 0.0;
            std::cout << "  -> reward=" << std::fixed << std::setprecision(3) << lq.reward
                      << "\n\n";
            session.queries.push_back(std::move(lq));
        }

        const auto labelsPath = yams::config::get_data_dir() / "relevance_labels.jsonl";
        RelevanceLabelStore store(labelsPath);
        auto appended = store.append(session);
        if (!appended) {
            std::cout << "  "
                      << status_error("Failed to persist session: " + appended.error().message)
                      << "\n";
        } else {
            std::cout << "  " << status_ok("Session saved to " + labelsPath.string()) << "\n";
        }

        try {
            if (auto tuner = appCtx->searchEngine->getSearchTuner()) {
                tuner->observeRelevanceFeedback(session);
                std::cout << "  " << status_ok("Forwarded to SearchTuner") << "\n";
            }
        } catch (const std::exception& e) {
            spdlog::warn("observeRelevanceFeedback raised: {}", e.what());
        }

        std::cout << "\n  " << colorize("Mean reward:", Ansi::CYAN) << " " << std::fixed
                  << std::setprecision(3) << session.meanReward() << "   "
                  << colorize("Queries labeled:", Ansi::CYAN) << " " << session.queries.size()
                  << "\n";

        if (opts.json) {
            std::cout << "\n" << session.toJson().dump(2) << "\n";
        }
    } catch (const std::exception& e) {
        std::cout << "  " << ui::status_error(std::string("Tune error: ") + e.what()) << "\n";
    }
}

void runTuneStatus(YamsCLI* cli, bool json) {
    using namespace yams::cli::ui;
    (void)cli;

    const auto statePath = yams::config::get_data_dir() / "tuner_state.json";
    const auto labelsPath = yams::config::get_data_dir() / "relevance_labels.jsonl";

    std::error_code ec;
    const bool haveState = std::filesystem::exists(statePath, ec);
    const bool haveLabels = std::filesystem::exists(labelsPath, ec);

    nlohmann::json report;
    report["state_path"] = statePath.string();
    report["labels_path"] = labelsPath.string();
    report["state_present"] = haveState;
    report["labels_present"] = haveLabels;

    if (haveState) {
        try {
            std::ifstream in(statePath);
            nlohmann::json j = nlohmann::json::parse(in, nullptr, true);
            for (auto key : {"observations", "last_adjustment_observation", "last_decision",
                             "ewma_latency_ms", "ewma_relevance_reward", "relevance_sessions",
                             "relevance_queries", "last_relevance_timestamp"}) {
                if (j.contains(key)) {
                    report["tuner"][key] = j[key];
                }
            }
        } catch (const std::exception& e) {
            report["tuner_error"] = std::string("parse failed: ") + e.what();
        }
    }

    if (haveLabels) {
        try {
            yams::search::RelevanceLabelStore store(labelsPath);
            auto recent = store.readRecent(50);
            if (recent) {
                report["label_sessions_recent"] = recent.value().size();
                double total = 0.0;
                for (const auto& s : recent.value()) {
                    total += s.meanReward();
                }
                if (!recent.value().empty()) {
                    report["label_mean_reward_recent"] = total / recent.value().size();
                }
            }
        } catch (const std::exception& e) {
            report["labels_error"] = std::string("read failed: ") + e.what();
        }
    }

    if (json) {
        std::cout << report.dump(2) << "\n";
        return;
    }

    std::cout << "\n" << section_header("Tuner Status") << "\n\n";
    std::cout << "  " << colorize("Backend:", Ansi::CYAN) << " rules (cold-start)\n";
    std::cout << "  " << colorize("State file:", Ansi::CYAN) << " " << statePath.string()
              << (haveState ? "" : "  (not present)") << "\n";
    if (haveState && report.contains("tuner")) {
        const auto& t = report["tuner"];
        if (t.contains("observations"))
            std::cout << "  " << colorize("Observations:", Ansi::CYAN) << " "
                      << t["observations"].get<std::uint64_t>() << "\n";
        if (t.contains("last_decision"))
            std::cout << "  " << colorize("Last decision:", Ansi::CYAN) << " "
                      << t["last_decision"].get<std::string>() << "\n";
        if (t.contains("ewma_relevance_reward"))
            std::cout << "  " << colorize("Reward EWMA:", Ansi::CYAN) << " " << std::fixed
                      << std::setprecision(3) << t["ewma_relevance_reward"].get<double>() << "\n";
        if (t.contains("relevance_sessions"))
            std::cout << "  " << colorize("Sessions labeled:", Ansi::CYAN) << " "
                      << t["relevance_sessions"].get<std::uint64_t>() << "\n";
    }
    if (haveLabels && report.contains("label_mean_reward_recent")) {
        std::cout << "  " << colorize("Recent label mean reward:", Ansi::CYAN) << " " << std::fixed
                  << std::setprecision(3) << report["label_mean_reward_recent"].get<double>()
                  << "  (" << report["label_sessions_recent"].get<std::size_t>() << " sessions)\n";
    }
    if (!haveState && !haveLabels) {
        std::cout << "  " << status_ok("No tuner state yet — run `yams tune` to start labeling.")
                  << "\n";
    }
}

int runTuneReset(YamsCLI* cli, const std::string& policy, bool assumeYes) {
    using namespace yams::cli::ui;
    (void)cli;

    const auto dataDir = yams::config::get_data_dir();
    std::vector<std::filesystem::path> targets;
    const std::string p = policy.empty() ? "rules" : policy;
    if (p == "rules" || p == "all") {
        targets.push_back(dataDir / "tuner_state.json");
    }
    if (p == "contextual" || p == "all") {
        targets.push_back(dataDir / "tuner_contextual_state.json");
    }
    if (targets.empty()) {
        std::cerr << "Unknown policy '" << p << "' (expected rules|contextual|all)\n";
        return 2;
    }

    std::cout << "\n" << section_header("Reset Tuner State") << "\n\n";
    for (const auto& path : targets) {
        std::cout << "  " << colorize("Target:", Ansi::CYAN) << " " << path.string() << "\n";
    }

    if (!assumeYes) {
        std::cout << "\n  Delete the files above? [y/N]: " << std::flush;
        std::string input;
        if (!std::getline(std::cin, input) || input.empty() ||
            (input[0] != 'y' && input[0] != 'Y')) {
            std::cout << "  " << status_ok("Aborted.") << "\n";
            return 0;
        }
    }

    int rc = 0;
    for (const auto& path : targets) {
        std::error_code ec;
        if (!std::filesystem::exists(path, ec)) {
            std::cout << "  " << status_ok("(skipped, not present) ") << path.filename().string()
                      << "\n";
            continue;
        }
        std::filesystem::remove(path, ec);
        if (ec) {
            std::cout << "  "
                      << status_error("Failed to remove " + path.string() + ": " + ec.message())
                      << "\n";
            rc = 1;
        } else {
            std::cout << "  " << status_ok("Removed ") << path.string() << "\n";
        }
    }
    return rc;
}

void runTuneReplayStub() {
    using namespace yams::cli::ui;
    std::cout << "\n" << section_header("Tune Replay") << "\n\n";
    std::cout << "  " << status_ok("R6 offline replay harness not yet implemented.") << "\n";
    std::cout << "  When available, this will replay your labeled sessions against\n"
                 "  rules and contextual policies and report per-policy mean reward.\n";
}

} // namespace yams::cli
