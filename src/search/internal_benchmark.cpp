#include <yams/search/internal_benchmark.h>
#include <yams/search/search_tuner.h>

#include <spdlog/spdlog.h>
#include <algorithm>
#include <cctype>
#include <cmath>
#include <iomanip>
#include <numeric>
#include <sstream>
#include <unordered_set>

namespace yams::search {

// ============================================================================
// LatencyStats
// ============================================================================

LatencyStats LatencyStats::compute(const std::vector<double>& samples) {
    LatencyStats stats;
    if (samples.empty()) {
        return stats;
    }

    stats.sampleCount = samples.size();

    // Copy and sort for percentile calculations
    std::vector<double> sorted = samples;
    std::sort(sorted.begin(), sorted.end());

    stats.minMs = sorted.front();
    stats.maxMs = sorted.back();

    // Mean
    double sum = std::accumulate(sorted.begin(), sorted.end(), 0.0);
    stats.meanMs = sum / static_cast<double>(sorted.size());

    // Median
    size_t mid = sorted.size() / 2;
    if (sorted.size() % 2 == 0) {
        stats.medianMs = (sorted[mid - 1] + sorted[mid]) / 2.0;
    } else {
        stats.medianMs = sorted[mid];
    }

    // Percentiles
    auto percentile = [&sorted](double p) -> double {
        if (sorted.empty())
            return 0.0;
        double idx = p * static_cast<double>(sorted.size() - 1);
        size_t lower = static_cast<size_t>(std::floor(idx));
        size_t upper = static_cast<size_t>(std::ceil(idx));
        if (lower == upper || upper >= sorted.size()) {
            return sorted[std::min(lower, sorted.size() - 1)];
        }
        double frac = idx - static_cast<double>(lower);
        return sorted[lower] * (1.0 - frac) + sorted[upper] * frac;
    };

    stats.p95Ms = percentile(0.95);
    stats.p99Ms = percentile(0.99);

    // Standard deviation
    if (sorted.size() > 1) {
        double sqSum = 0.0;
        for (double v : sorted) {
            double diff = v - stats.meanMs;
            sqSum += diff * diff;
        }
        stats.stddevMs = std::sqrt(sqSum / static_cast<double>(sorted.size() - 1));
    }

    return stats;
}

// ============================================================================
// BenchmarkResults
// ============================================================================

std::string BenchmarkResults::summary() const {
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(4);

    oss << "Benchmark Results (" << queriesRun << " queries)\n";
    oss << "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n";

    // Quality metrics
    oss << "Quality Metrics:\n";
    oss << "  MRR:         " << mrr << " (1.0 = perfect)\n";
    oss << "  Recall@" << std::setw(2) << k << ":   " << recallAtK << " ("
        << static_cast<int>(recallAtK * 100) << "% found in top-" << k << ")\n";

    // Latency metrics
    oss << "\nLatency (ms):\n";
    oss << "  Mean:        " << std::setprecision(2) << latency.meanMs << "\n";
    oss << "  Median:      " << latency.medianMs << "\n";
    oss << "  P95:         " << latency.p95Ms << "\n";
    oss << "  P99:         " << latency.p99Ms << "\n";

    // Execution stats
    oss << "\nExecution:\n";
    oss << "  Succeeded:   " << queriesSucceeded << "/" << queriesRun << "\n";
    oss << "  Failed:      " << queriesFailed << "\n";
    oss << "  Total time:  " << totalTime.count() << " ms\n";

    if (tuningState) {
        oss << "  Tuning:      " << *tuningState << "\n";
    }

    return oss.str();
}

// ============================================================================
// SyntheticQueryGenerator
// ============================================================================

SyntheticQueryGenerator::SyntheticQueryGenerator(
    std::shared_ptr<metadata::MetadataRepository> metadataRepo)
    : metadataRepo_(std::move(metadataRepo)) {}

Result<size_t> SyntheticQueryGenerator::getAvailableDocumentCount() const {
    auto countResult = metadataRepo_->getDocumentCount();
    if (!countResult) {
        return Error{countResult.error()};
    }
    return static_cast<size_t>(countResult.value());
}

Result<std::vector<SyntheticQuery>>
SyntheticQueryGenerator::generate(const QueryGeneratorConfig& config) {
    spdlog::debug("SyntheticQueryGenerator: generating {} queries (seed={})", config.queryCount,
                  config.seed);

    std::mt19937_64 rng(config.seed);

    // Calculate how many of each query type to generate
    size_t knownItemCount =
        static_cast<size_t>(static_cast<float>(config.queryCount) * config.knownItemRatio);
    size_t tfidfCount =
        static_cast<size_t>(static_cast<float>(config.queryCount) * config.tfidfRatio);
    size_t semanticCount = config.queryCount - knownItemCount - tfidfCount;

    // Sample more documents than needed to account for filtering
    size_t sampleSize = std::min(config.queryCount * 3, size_t{1000});
    auto docsResult = sampleDocuments(sampleSize, rng, config);
    if (!docsResult) {
        return Error{docsResult.error()};
    }

    const auto& docs = docsResult.value();
    if (docs.empty()) {
        return Error{ErrorCode::InvalidArgument, "No documents available for query generation"};
    }

    spdlog::debug("SyntheticQueryGenerator: sampled {} documents", docs.size());

    std::vector<SyntheticQuery> queries;
    queries.reserve(config.queryCount);

    // Track which documents we've used to avoid duplicates
    std::unordered_set<std::string> usedHashes;

    // Helper to get content for a document
    auto getContent = [this](int64_t docId) -> std::optional<std::string> {
        auto contentResult = metadataRepo_->getContent(docId);
        if (!contentResult || !contentResult.value().has_value()) {
            return std::nullopt;
        }
        return contentResult.value()->contentText;
    };

    // Generate KNOWN_ITEM queries (exact phrase extraction)
    size_t generated = 0;
    for (const auto& doc : docs) {
        if (generated >= knownItemCount)
            break;
        if (usedHashes.count(doc.sha256Hash))
            continue;

        auto content = getContent(doc.id);
        if (!content || content->length() < config.minDocumentLength)
            continue;
        if (config.excludeBinaryFiles && isBinaryContent(*content))
            continue;

        auto phrase = extractPhrase(*content, config.minPhraseLength, config.maxPhraseLength, rng);
        if (!phrase)
            continue;

        SyntheticQuery query;
        query.text = *phrase;
        query.expectedDocHash = doc.sha256Hash;
        query.expectedFilePath = doc.filePath;
        query.type = QueryType::KNOWN_ITEM;
        query.sourcePhrase = *phrase;

        queries.push_back(std::move(query));
        usedHashes.insert(doc.sha256Hash);
        ++generated;
    }

    spdlog::debug("SyntheticQueryGenerator: generated {} KNOWN_ITEM queries", generated);

    // Generate TFIDF_TERMS queries (top distinctive terms)
    generated = 0;
    for (const auto& doc : docs) {
        if (generated >= tfidfCount)
            break;
        if (usedHashes.count(doc.sha256Hash))
            continue;

        auto content = getContent(doc.id);
        if (!content || content->length() < config.minDocumentLength)
            continue;
        if (config.excludeBinaryFiles && isBinaryContent(*content))
            continue;

        auto terms = extractTopTerms(*content, 3);
        if (terms.size() < 2)
            continue;

        // Combine terms into query
        std::ostringstream oss;
        for (size_t i = 0; i < terms.size(); ++i) {
            if (i > 0)
                oss << " ";
            oss << terms[i];
        }

        SyntheticQuery query;
        query.text = oss.str();
        query.expectedDocHash = doc.sha256Hash;
        query.expectedFilePath = doc.filePath;
        query.type = QueryType::TFIDF_TERMS;
        query.sourcePhrase = oss.str();

        queries.push_back(std::move(query));
        usedHashes.insert(doc.sha256Hash);
        ++generated;
    }

    spdlog::debug("SyntheticQueryGenerator: generated {} TFIDF_TERMS queries", generated);

    // Generate SEMANTIC queries (for now, just use longer phrases - future: paraphrasing)
    generated = 0;
    for (const auto& doc : docs) {
        if (generated >= semanticCount)
            break;
        if (usedHashes.count(doc.sha256Hash))
            continue;

        auto content = getContent(doc.id);
        if (!content || content->length() < config.minDocumentLength)
            continue;
        if (config.excludeBinaryFiles && isBinaryContent(*content))
            continue;

        // Use slightly different phrase extraction for semantic
        auto phrase =
            extractPhrase(*content, config.minPhraseLength + 2, config.maxPhraseLength + 2, rng);
        if (!phrase)
            continue;

        SyntheticQuery query;
        query.text = *phrase;
        query.expectedDocHash = doc.sha256Hash;
        query.expectedFilePath = doc.filePath;
        query.type = QueryType::SEMANTIC;
        query.sourcePhrase = *phrase;

        queries.push_back(std::move(query));
        usedHashes.insert(doc.sha256Hash);
        ++generated;
    }

    spdlog::debug("SyntheticQueryGenerator: generated {} SEMANTIC queries", generated);
    spdlog::info("SyntheticQueryGenerator: total {} queries generated", queries.size());

    // Shuffle to mix query types
    std::shuffle(queries.begin(), queries.end(), rng);

    return queries;
}

Result<std::vector<metadata::DocumentInfo>>
SyntheticQueryGenerator::sampleDocuments(size_t count, std::mt19937_64& rng,
                                         const QueryGeneratorConfig& config) {
    metadata::DocumentQueryOptions opts;
    opts.limit = static_cast<int>(count * 2); // Oversample
    opts.textOnly = config.excludeBinaryFiles;
    opts.orderByIndexedDesc = true; // Prefer recently indexed

    auto docsResult = metadataRepo_->queryDocuments(opts);
    if (!docsResult) {
        return Error{docsResult.error()};
    }

    auto docs = std::move(docsResult.value());

    // Shuffle to randomize selection
    std::shuffle(docs.begin(), docs.end(), rng);

    // Limit to requested count
    if (docs.size() > count) {
        docs.resize(count);
    }

    return docs;
}

std::optional<std::string> SyntheticQueryGenerator::extractPhrase(const std::string& content,
                                                                  size_t minWords, size_t maxWords,
                                                                  std::mt19937_64& rng) {
    // Tokenize into words (simple whitespace split)
    std::vector<std::string> words;
    std::istringstream iss(content);
    std::string word;
    while (iss >> word && words.size() < 5000) { // Limit for performance
        // Basic cleanup: remove punctuation from ends
        while (!word.empty() && std::ispunct(static_cast<unsigned char>(word.back()))) {
            word.pop_back();
        }
        while (!word.empty() && std::ispunct(static_cast<unsigned char>(word.front()))) {
            word.erase(0, 1);
        }
        if (word.length() >= 2) { // Skip single chars
            words.push_back(word);
        }
    }

    if (words.size() < minWords) {
        return std::nullopt;
    }

    // Try a few random positions to find a good phrase
    for (int attempt = 0; attempt < 10; ++attempt) {
        std::uniform_int_distribution<size_t> lenDist(minWords, std::min(maxWords, words.size()));
        size_t phraseLen = lenDist(rng);

        std::uniform_int_distribution<size_t> posDist(0, words.size() - phraseLen);
        size_t startPos = posDist(rng);

        // Build phrase
        std::ostringstream oss;
        bool hasAlpha = false;
        for (size_t i = 0; i < phraseLen; ++i) {
            if (i > 0)
                oss << " ";
            oss << words[startPos + i];
            // Check if any word has alphabetic characters
            for (char c : words[startPos + i]) {
                if (std::isalpha(static_cast<unsigned char>(c))) {
                    hasAlpha = true;
                    break;
                }
            }
        }

        // Only accept phrases with at least some alphabetic content
        if (hasAlpha) {
            return oss.str();
        }
    }

    return std::nullopt;
}

std::vector<std::string> SyntheticQueryGenerator::extractTopTerms(const std::string& content,
                                                                  size_t count) {
    // Simple term frequency extraction
    std::unordered_map<std::string, int> termFreq;

    std::istringstream iss(content);
    std::string word;
    while (iss >> word) {
        // Normalize: lowercase, remove punctuation
        std::string normalized;
        for (char c : word) {
            if (std::isalnum(static_cast<unsigned char>(c))) {
                normalized += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
            }
        }
        if (normalized.length() >= 3 && normalized.length() <= 20) {
            termFreq[normalized]++;
        }
    }

    // Filter out common stop words
    static const std::unordered_set<std::string> stopWords = {
        "the",   "and",   "for",  "are",   "but",   "not",   "you",   "all",   "can",
        "her",   "was",   "one",  "our",   "out",   "has",   "have",  "been",  "this",
        "that",  "with",  "they", "from",  "will",  "would", "there", "their", "what",
        "about", "which", "when", "make",  "like",  "just",  "over",  "such",  "into",
        "than",  "them",  "some", "could", "other", "more"};

    // Sort by frequency and filter
    std::vector<std::pair<std::string, int>> sorted;
    for (const auto& [term, freq] : termFreq) {
        if (freq >= 2 && !stopWords.count(term)) {
            sorted.emplace_back(term, freq);
        }
    }

    std::sort(sorted.begin(), sorted.end(),
              [](const auto& a, const auto& b) { return a.second > b.second; });

    std::vector<std::string> result;
    for (size_t i = 0; i < std::min(count, sorted.size()); ++i) {
        result.push_back(sorted[i].first);
    }

    return result;
}

bool SyntheticQueryGenerator::isBinaryContent(const std::string& content) const {
    // Check first N bytes for null bytes or high proportion of non-printable chars
    size_t checkLen = std::min(content.size(), size_t{512});
    int nonPrintable = 0;

    for (size_t i = 0; i < checkLen; ++i) {
        unsigned char c = static_cast<unsigned char>(content[i]);
        if (c == 0) {
            return true; // Null byte = binary
        }
        if (c < 32 && c != '\n' && c != '\r' && c != '\t') {
            ++nonPrintable;
        }
    }

    // More than 10% non-printable = likely binary
    return nonPrintable > static_cast<int>(checkLen / 10);
}

// ============================================================================
// InternalBenchmark
// ============================================================================

InternalBenchmark::InternalBenchmark(std::shared_ptr<SearchEngine> searchEngine,
                                     std::shared_ptr<metadata::MetadataRepository> metadataRepo)
    : searchEngine_(std::move(searchEngine)), metadataRepo_(std::move(metadataRepo)) {}

Result<BenchmarkResults> InternalBenchmark::run(const BenchmarkConfig& config) {
    // Generate queries
    SyntheticQueryGenerator generator(metadataRepo_);
    auto queriesResult = generator.generate(config.queryConfig);
    if (!queriesResult) {
        return Error{queriesResult.error()};
    }

    // Limit to requested count
    auto queries = std::move(queriesResult.value());
    if (queries.size() > config.queryCount) {
        queries.resize(config.queryCount);
    }

    return runWithQueries(queries, config);
}

Result<BenchmarkResults>
InternalBenchmark::runWithQueries(const std::vector<SyntheticQuery>& queries,
                                  const BenchmarkConfig& config) {
    spdlog::info("InternalBenchmark: running {} queries (k={}, warmup={})", queries.size(),
                 config.k, config.warmupQueries);

    auto startTime = std::chrono::steady_clock::now();

    // Warmup phase (not counted in results)
    for (size_t i = 0; i < std::min(config.warmupQueries, queries.size()); ++i) {
        if (config.verbose) {
            spdlog::debug("Warmup query {}: {}", i + 1, queries[i].text);
        }
        executeQuery(queries[i], config.k);
    }

    // Main benchmark phase
    std::vector<QueryExecution> executions;
    executions.reserve(queries.size());

    for (size_t i = 0; i < queries.size(); ++i) {
        const auto& query = queries[i];

        if (config.verbose) {
            spdlog::info("Query {}/{}: \"{}\"", i + 1, queries.size(), query.text.substr(0, 50));
        }

        auto exec = executeQuery(query, config.k);
        executions.push_back(std::move(exec));
    }

    auto endTime = std::chrono::steady_clock::now();
    auto totalTime = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

    // Aggregate results
    auto results = aggregateResults(executions, config.k, totalTime);

    // Optionally include per-query breakdown
    if (config.includeExecutions) {
        results.executions = std::move(executions);
    }

    // Capture tuning state if available
    auto corpusStats = metadataRepo_->getCorpusStats();
    if (corpusStats) {
        SearchTuner tuner(corpusStats.value());
        results.tuningState = tuningStateToString(tuner.currentState());
        results.tunedParams = tuner.getParams().toJson();
    }

    spdlog::info("InternalBenchmark: completed. MRR={:.4f}, Recall@{}={:.4f}", results.mrr,
                 config.k, results.recallAtK);

    return results;
}

QueryExecution InternalBenchmark::executeQuery(const SyntheticQuery& query, size_t k) {
    QueryExecution exec;
    exec.query = query;

    auto start = std::chrono::high_resolution_clock::now();

    SearchParams params;
    params.limit = static_cast<int>(k);

    auto result = searchEngine_->search(query.text, params);

    auto end = std::chrono::high_resolution_clock::now();
    exec.latencyMs = std::chrono::duration<double, std::milli>(end - start).count();

    if (!result) {
        exec.error = result.error().message;
        return exec;
    }

    // Extract retrieved document hashes
    for (const auto& sr : result.value()) {
        exec.retrievedHashes.push_back(sr.document.sha256Hash);
    }

    // Find position of expected document
    for (size_t i = 0; i < exec.retrievedHashes.size(); ++i) {
        if (exec.retrievedHashes[i] == query.expectedDocHash) {
            exec.reciprocalRank = i + 1; // 1-based
            exec.foundInTopK = true;
            break;
        }
    }

    return exec;
}

BenchmarkResults InternalBenchmark::aggregateResults(const std::vector<QueryExecution>& executions,
                                                     size_t k,
                                                     std::chrono::milliseconds totalTime) {
    BenchmarkResults results;
    results.k = k;
    results.totalTime = totalTime;
    results.queriesRun = executions.size();

    // Timestamp
    auto now = std::chrono::system_clock::now();
    auto time_t = std::chrono::system_clock::to_time_t(now);
    std::ostringstream oss;
    oss << std::put_time(std::gmtime(&time_t), "%Y-%m-%dT%H:%M:%SZ");
    results.timestamp = oss.str();

    if (executions.empty()) {
        return results;
    }

    // Compute metrics
    double sumRR = 0.0;
    size_t foundCount = 0;
    std::vector<double> latencies;
    latencies.reserve(executions.size());

    for (const auto& exec : executions) {
        if (!exec.error.empty()) {
            ++results.queriesFailed;
            continue;
        }

        ++results.queriesSucceeded;
        latencies.push_back(exec.latencyMs);

        if (exec.foundInTopK) {
            ++foundCount;
            sumRR += 1.0 / static_cast<double>(exec.reciprocalRank);
        }
    }

    // MRR (Mean Reciprocal Rank)
    if (results.queriesSucceeded > 0) {
        results.mrr = static_cast<float>(sumRR / static_cast<double>(results.queriesSucceeded));
    }

    // Recall@K
    if (results.queriesSucceeded > 0) {
        results.recallAtK =
            static_cast<float>(foundCount) / static_cast<float>(results.queriesSucceeded);
    }

    // Latency statistics
    results.latency = LatencyStats::compute(latencies);

    return results;
}

BenchmarkComparison InternalBenchmark::compare(const BenchmarkResults& baseline,
                                               const BenchmarkResults& current,
                                               float regressionThreshold) {
    BenchmarkComparison cmp;
    cmp.mrrDelta = current.mrr - baseline.mrr;
    cmp.recallDelta = current.recallAtK - baseline.recallAtK;
    cmp.latencyDelta = current.latency.meanMs - baseline.latency.meanMs;

    // Determine if regression or improvement
    cmp.isRegression = cmp.mrrDelta < -regressionThreshold;
    cmp.isImprovement = cmp.mrrDelta > regressionThreshold;

    // Generate summary
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(4);

    if (cmp.isRegression) {
        oss << "REGRESSION: ";
    } else if (cmp.isImprovement) {
        oss << "IMPROVEMENT: ";
    } else {
        oss << "No significant change: ";
    }

    oss << "MRR " << (cmp.mrrDelta >= 0 ? "+" : "") << cmp.mrrDelta;
    oss << ", Recall " << (cmp.recallDelta >= 0 ? "+" : "") << cmp.recallDelta;
    oss << ", Latency " << (cmp.latencyDelta >= 0 ? "+" : "") << cmp.latencyDelta << "ms";

    cmp.summary = oss.str();

    return cmp;
}

} // namespace yams::search
