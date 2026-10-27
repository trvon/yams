#include <yams/topology/topology_input_extractor.h>
#include <yams/vector/vector_database.h>

#include <spdlog/spdlog.h>
#include <simeon/minhash.hpp>

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdint>
#include <iomanip>
#include <limits>
#include <numeric>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace yams::topology {

namespace {

bool isAsciiDigit(char c) noexcept {
    return c >= '0' && c <= '9';
}

bool isAsciiSpace(char c) noexcept {
    return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v';
}

/// Read exactly `count` decimal digits at `pos`; advances `pos` on success.
bool parseFixedDigits(std::string_view s, std::size_t& pos, std::size_t count, int& out) noexcept {
    if (pos + count > s.size()) {
        return false;
    }
    int value = 0;
    for (std::size_t i = 0; i < count; ++i) {
        const char c = s[pos + i];
        if (!isAsciiDigit(c)) {
            return false;
        }
        value = value * 10 + (c - '0');
    }
    pos += count;
    out = value;
    return true;
}

/// Days since 1970-01-01 for a proleptic Gregorian civil date (H. Hinnant's days_from_civil).
constexpr std::int64_t daysFromCivil(std::int64_t year, unsigned month, unsigned day) noexcept {
    year -= month <= 2;
    const std::int64_t era = (year >= 0 ? year : year - 399) / 400;
    const auto yoe = static_cast<unsigned>(year - era * 400);
    const unsigned doy = (153 * (month + (month > 2 ? -3 : 9)) + 2) / 5 + day - 1;
    const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + static_cast<std::int64_t>(doe) - 719468;
}

constexpr bool isLeapYear(int year) noexcept {
    return (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
}

constexpr int daysInMonth(int year, int month) noexcept {
    constexpr int kDays[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    return month == 2 && isLeapYear(year) ? 29 : kDays[month - 1];
}

/// Optionally signed run of decimal digits spanning the whole view.
std::optional<std::int64_t> parseEpochSeconds(std::string_view s) noexcept {
    std::size_t pos = 0;
    bool negative = false;
    if (s[0] == '-' || s[0] == '+') {
        negative = s[0] == '-';
        pos = 1;
    }
    if (pos >= s.size() || s.size() - pos > 18) {
        return std::nullopt;
    }
    std::int64_t value = 0;
    for (; pos < s.size(); ++pos) {
        if (!isAsciiDigit(s[pos])) {
            return std::nullopt;
        }
        value = value * 10 + (s[pos] - '0');
    }
    return negative ? -value : value;
}

} // namespace

std::optional<std::int64_t> parseTopologyEventTime(std::string_view value) noexcept {
    while (!value.empty() && isAsciiSpace(value.front())) {
        value.remove_prefix(1);
    }
    while (!value.empty() && isAsciiSpace(value.back())) {
        value.remove_suffix(1);
    }
    if (value.empty()) {
        return std::nullopt;
    }
    if (auto epoch = parseEpochSeconds(value)) {
        return epoch;
    }

    std::size_t pos = 0;
    int year = 0;
    int month = 0;
    int day = 0;
    if (!parseFixedDigits(value, pos, 4, year) || pos >= value.size() || value[pos] != '-') {
        return std::nullopt;
    }
    ++pos;
    if (!parseFixedDigits(value, pos, 2, month) || pos >= value.size() || value[pos] != '-') {
        return std::nullopt;
    }
    ++pos;
    if (!parseFixedDigits(value, pos, 2, day)) {
        return std::nullopt;
    }
    if (month < 1 || month > 12 || day < 1 || day > daysInMonth(year, month)) {
        return std::nullopt;
    }
    std::int64_t seconds =
        daysFromCivil(year, static_cast<unsigned>(month), static_cast<unsigned>(day)) * 86400;
    if (pos == value.size()) {
        return seconds;
    }

    if (value[pos] != 'T' && value[pos] != 't' && value[pos] != ' ') {
        return std::nullopt;
    }
    ++pos;
    int hour = 0;
    int minute = 0;
    int second = 0;
    if (!parseFixedDigits(value, pos, 2, hour) || pos >= value.size() || value[pos] != ':') {
        return std::nullopt;
    }
    ++pos;
    if (!parseFixedDigits(value, pos, 2, minute)) {
        return std::nullopt;
    }
    if (pos < value.size() && value[pos] == ':') {
        ++pos;
        if (!parseFixedDigits(value, pos, 2, second)) {
            return std::nullopt;
        }
        if (pos < value.size() && (value[pos] == '.' || value[pos] == ',')) {
            ++pos;
            std::size_t fractionDigits = 0;
            while (pos < value.size() && isAsciiDigit(value[pos])) {
                ++pos;
                ++fractionDigits;
            }
            if (fractionDigits == 0) {
                return std::nullopt;
            }
        }
    }
    if (hour > 23 || minute > 59 || second > 60) {
        return std::nullopt;
    }
    seconds += static_cast<std::int64_t>(hour) * 3600 + minute * 60 + second;
    if (pos == value.size()) {
        return seconds;
    }

    if (value[pos] == 'Z' || value[pos] == 'z') {
        ++pos;
        return pos == value.size() ? std::optional<std::int64_t>{seconds} : std::nullopt;
    }
    if (value[pos] != '+' && value[pos] != '-') {
        return std::nullopt;
    }
    const bool negativeOffset = value[pos] == '-';
    ++pos;
    int offsetHours = 0;
    int offsetMinutes = 0;
    if (!parseFixedDigits(value, pos, 2, offsetHours)) {
        return std::nullopt;
    }
    if (pos < value.size() && value[pos] == ':') {
        ++pos;
    }
    if (pos < value.size() && !parseFixedDigits(value, pos, 2, offsetMinutes)) {
        return std::nullopt;
    }
    if (pos != value.size() || offsetHours > 23 || offsetMinutes > 59) {
        return std::nullopt;
    }
    const std::int64_t offset = static_cast<std::int64_t>(offsetHours) * 3600 + offsetMinutes * 60;
    // `+02:00` means the local clock runs ahead of UTC, so UTC is earlier.
    return negativeOffset ? seconds + offset : seconds - offset;
}

namespace {

/// Lift `event_time` / `session_id` out of the loaded metadata map; createdTime is the event
/// time fallback (zero = unknown). Never fails: a bad `event_time` is logged and skipped.
void resolveTypedInputFields(TopologyDocumentInput& input, const metadata::DocumentInfo& document,
                             TopologyExtractionStats& stats) {
    if (auto it = input.metadata.find(std::string(kTopologyEventTimeMetadataKey));
        it != input.metadata.end()) {
        if (auto parsed = parseTopologyEventTime(it->second)) {
            input.eventTime = parsed;
            ++stats.eventTimesFromMetadata;
        } else {
            ++stats.eventTimesInvalid;
            spdlog::debug("[topology] ignoring unparsable {} '{}' on document {}",
                          kTopologyEventTimeMetadataKey, it->second, document.sha256Hash);
        }
    }
    if (!input.eventTime.has_value()) {
        const auto created =
            static_cast<std::int64_t>(document.createdTime.time_since_epoch().count());
        if (created != 0) {
            input.eventTime = created;
            ++stats.eventTimesFromCreatedTime;
        }
    }
    if (auto it = input.metadata.find(std::string(kTopologySessionIdMetadataKey));
        it != input.metadata.end() && !it->second.empty()) {
        input.sessionId = it->second;
        ++stats.sessionIdsLoaded;
    }
}

constexpr std::string_view kDocNodePrefix = "doc:";
constexpr std::uint64_t kFnv1aOffset = 14695981039346656037ULL;
constexpr std::uint64_t kFnv1aPrime = 1099511628211ULL;

void appendFingerprintBytes(std::uint64_t& fingerprint, std::string_view bytes) {
    for (const auto byte : bytes) {
        fingerprint ^= static_cast<unsigned char>(byte);
        fingerprint *= kFnv1aPrime;
    }
}

bool hasFeatureComposition(const FeatureComposition& config) noexcept {
    return config.enableEntityFusion || config.enableMatryoshkaCoarseView ||
           config.enableMinHashSketch;
}

std::string featureProjectionFingerprint(const FeatureComposition& config,
                                         const std::vector<float>& matryoshkaWeights,
                                         const std::vector<std::string>& entityAxes) {
    std::ostringstream descriptor;
    descriptor << std::setprecision(std::numeric_limits<float>::max_digits10);
    descriptor << "schema=v1"
               << ";entity=" << config.enableEntityFusion << ";entity_k=" << config.entitySignatureK
               << ";entity_alpha=" << config.entityFusionAlpha
               << ";entity_min_confidence=" << config.entityMinConfidence
               << ";matryoshka=" << config.enableMatryoshkaCoarseView
               << ";matryoshka_dim=" << config.matryoshkaTargetDim
               << ";minhash=" << config.enableMinHashSketch
               << ";minhash_dim=" << config.minhashSketchDim
               << ";minhash_alpha=" << config.minhashAlpha;
    for (const auto weight : matryoshkaWeights) {
        descriptor << ";w=" << weight;
    }
    for (const auto& axis : entityAxes) {
        descriptor << ";entity_axis=" << axis.size() << ':' << axis;
    }

    std::uint64_t fingerprint = kFnv1aOffset;
    appendFingerprintBytes(fingerprint, descriptor.str());
    std::ostringstream out;
    out << std::hex << std::setw(16) << std::setfill('0') << fingerprint;
    return out.str();
}

std::string composedCoordinateSpaceIdentity(std::string_view rawIdentity,
                                            const FeatureComposition& config,
                                            std::size_t rawDimension, std::size_t composedDimension,
                                            const std::vector<float>& matryoshkaWeights,
                                            const std::vector<std::string>& entityAxes) {
    if (rawIdentity.empty() || !hasFeatureComposition(config)) {
        return std::string{rawIdentity};
    }
    std::ostringstream out;
    out << std::setprecision(std::numeric_limits<float>::max_digits10);
    out << "topology-coordinate:v1;raw=" << rawIdentity << ";raw_dim=" << rawDimension
        << ";composed_dim=" << composedDimension << ";entity=" << config.enableEntityFusion
        << ";entity_k=" << config.entitySignatureK << ";entity_alpha=" << config.entityFusionAlpha
        << ";entity_min_confidence=" << config.entityMinConfidence
        << ";matryoshka=" << config.enableMatryoshkaCoarseView
        << ";matryoshka_dim=" << config.matryoshkaTargetDim
        << ";minhash=" << config.enableMinHashSketch << ";minhash_dim=" << config.minhashSketchDim
        << ";minhash_alpha=" << config.minhashAlpha << ";projection=fnv1a64:"
        << featureProjectionFingerprint(config, matryoshkaWeights, entityAxes);
    return out.str();
}

// Per Phase V design (and src/search/query_text_utils.cpp's filter list), drop
// entity types that are uninformative for topical clustering. Match against the
// canonicalized type string stored in kg_nodes.type.
bool isFilteredEntityType(const std::string& canonicalLabel) {
    static const std::unordered_set<std::string> filtered{"date", "time", "number", "percentage",
                                                          "ordinal"};
    return filtered.contains(canonicalLabel);
}

void l2NormalizeInPlace(std::vector<float>& v) {
    double sumSq = 0.0;
    for (float x : v) {
        sumSq += static_cast<double>(x) * x;
    }
    if (sumSq <= 0.0) {
        return;
    }
    const auto norm = static_cast<float>(std::sqrt(sumSq));
    for (float& x : v) {
        x /= norm;
    }
}

// V-B: matryoshka coarse view via per-dim variance-weighted truncation.
// Computed once over the corpus embeddings being clustered.
std::vector<float> computeVarianceWeights(const std::vector<std::vector<float>>& embeddings,
                                          std::size_t targetDim) {
    if (embeddings.empty() || targetDim == 0) {
        return {};
    }
    const std::size_t fullDim = embeddings.front().size();
    if (targetDim >= fullDim) {
        return {};
    }
    std::vector<double> mean(fullDim, 0.0);
    std::size_t n = 0;
    for (const auto& e : embeddings) {
        if (e.size() != fullDim) {
            continue;
        }
        for (std::size_t i = 0; i < fullDim; ++i) {
            mean[i] += e[i];
        }
        ++n;
    }
    if (n == 0) {
        return {};
    }
    for (auto& m : mean) {
        m /= static_cast<double>(n);
    }
    std::vector<double> var(fullDim, 0.0);
    for (const auto& e : embeddings) {
        if (e.size() != fullDim) {
            continue;
        }
        for (std::size_t i = 0; i < fullDim; ++i) {
            const double d = static_cast<double>(e[i]) - mean[i];
            var[i] += d * d;
        }
    }
    for (auto& vv : var) {
        vv /= static_cast<double>(n);
    }
    // Pick top-targetDim dimensions by variance, store (dim_index, sqrt(var)) so
    // applyMatryoshkaCoarse can rebuild the projection deterministically.
    // We return a dense fullDim-length weight vector with zeros outside top-K.
    std::vector<std::size_t> idx(fullDim);
    std::iota(idx.begin(), idx.end(), std::size_t{0});
    std::partial_sort(idx.begin(), idx.begin() + static_cast<std::ptrdiff_t>(targetDim), idx.end(),
                      [&](std::size_t a, std::size_t b) { return var[a] > var[b]; });
    std::vector<float> weights(fullDim, 0.0F);
    for (std::size_t k = 0; k < targetDim; ++k) {
        weights[idx[k]] = static_cast<float>(std::sqrt(var[idx[k]]));
    }
    return weights;
}

std::vector<float> applyMatryoshkaCoarse(const std::vector<float>& dense,
                                         const std::vector<float>& weights, std::size_t targetDim) {
    if (weights.size() != dense.size() || targetDim == 0 || targetDim >= dense.size()) {
        return dense;
    }
    // Collect indices of non-zero weights (these are the top-targetDim variance dims).
    std::vector<std::size_t> kept;
    kept.reserve(targetDim);
    for (std::size_t i = 0; i < weights.size(); ++i) {
        if (weights[i] > 0.0F) {
            kept.push_back(i);
        }
    }
    std::vector<float> coarse;
    coarse.reserve(kept.size());
    for (std::size_t i : kept) {
        coarse.push_back(dense[i] * weights[i]);
    }
    l2NormalizeInPlace(coarse);
    return coarse;
}

// V-C: bucket-count sketch over a 256-u32 MinHash signature → fixed-dim float vec.
// Each bucket sums (sig[i] mod some-prime), giving a dim-reduced MinHash-derived feature.
// Empty input → empty output (signals "no MinHash available").
std::vector<float> bucketCountSketch(const std::vector<std::uint32_t>& sig, std::size_t sketchDim) {
    if (sig.empty() || sketchDim == 0) {
        return {};
    }
    std::vector<float> sketch(sketchDim, 0.0F);
    for (std::size_t i = 0; i < sig.size(); ++i) {
        const std::size_t bucket = sig[i] % sketchDim;
        sketch[bucket] += 1.0F;
    }
    l2NormalizeInPlace(sketch);
    return sketch;
}

std::string nodeKeyForDocumentHash(std::string_view documentHash) {
    return std::string{kDocNodePrefix} + std::string{documentHash};
}

// Phase V-A: build corpus-wide histogram of canonical entity types, keep top-K.
// One bulk pass: collect all (docId, nodeId) pairs, then bulk-fetch kg_nodes once,
// then count by canonicalized node.type.
struct EntityTypeIndex {
    std::vector<std::string> topKTypes;                     // index → canonical type string
    std::unordered_map<std::string, std::size_t> typeToIdx; // canonical type → index in topKTypes
    std::unordered_map<std::int64_t, std::string> nodeIdToType; // cache for per-doc lookup
};

EntityTypeIndex buildEntityTypeIndex(metadata::KnowledgeGraphStore& kgStore,
                                     const std::vector<metadata::DocumentInfo>& documents,
                                     std::size_t K, float minConfidence) {
    EntityTypeIndex out;
    if (K == 0 || documents.empty()) {
        return out;
    }
    // Pass 1: collect all unique nodeIds referenced by docs' entities (with conf ≥ τ).
    std::unordered_set<std::int64_t> uniqueNodeIds;
    std::unordered_map<std::int64_t, std::vector<std::int64_t>> docToNodes;
    docToNodes.reserve(documents.size());
    for (const auto& doc : documents) {
        auto entitiesResult = kgStore.getDocEntitiesForDocument(doc.id);
        if (!entitiesResult) {
            continue;
        }
        std::vector<std::int64_t> nodeIds;
        nodeIds.reserve(entitiesResult.value().size());
        for (const auto& ent : entitiesResult.value()) {
            if (!ent.nodeId.has_value()) {
                continue;
            }
            if (ent.confidence.has_value() && ent.confidence.value() < minConfidence) {
                continue;
            }
            nodeIds.push_back(*ent.nodeId);
            uniqueNodeIds.insert(*ent.nodeId);
        }
        if (!nodeIds.empty()) {
            docToNodes.emplace(doc.id, std::move(nodeIds));
        }
    }
    if (uniqueNodeIds.empty()) {
        return out;
    }
    std::vector<std::int64_t> bulkIds(uniqueNodeIds.begin(), uniqueNodeIds.end());
    auto nodesResult = kgStore.getNodesByIds(bulkIds);
    if (!nodesResult) {
        return out;
    }
    out.nodeIdToType.reserve(nodesResult.value().size());
    std::unordered_map<std::string, std::size_t> typeCounts;
    for (const auto& node : nodesResult.value()) {
        if (!node.type.has_value()) {
            continue;
        }
        std::string canonical = *node.type;
        std::transform(canonical.begin(), canonical.end(), canonical.begin(),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        if (canonical.empty() || isFilteredEntityType(canonical)) {
            continue;
        }
        out.nodeIdToType[node.id] = canonical;
    }
    // Pass 2: count types per doc (each type contributes once per doc, not per occurrence).
    for (const auto& [docId, nodeIds] : docToNodes) {
        std::unordered_set<std::string> docTypes;
        for (auto nid : nodeIds) {
            auto it = out.nodeIdToType.find(nid);
            if (it != out.nodeIdToType.end()) {
                docTypes.insert(it->second);
            }
        }
        for (const auto& t : docTypes) {
            ++typeCounts[t];
        }
    }
    // Top-K by frequency (ties broken by lexicographic order for determinism).
    std::vector<std::pair<std::string, std::size_t>> ranked(typeCounts.begin(), typeCounts.end());
    std::sort(ranked.begin(), ranked.end(), [](const auto& a, const auto& b) {
        if (a.second != b.second)
            return a.second > b.second;
        return a.first < b.first;
    });
    const std::size_t kEffective = std::min(K, ranked.size());
    out.topKTypes.reserve(kEffective);
    for (std::size_t i = 0; i < kEffective; ++i) {
        out.typeToIdx[ranked[i].first] = i;
        out.topKTypes.push_back(ranked[i].first);
    }
    return out;
}

// Phase V-A: build per-doc entity-type histogram weighted by GLiNER confidence.
std::vector<float> buildEntityTypeSignature(metadata::KnowledgeGraphStore& kgStore,
                                            std::int64_t docId, const EntityTypeIndex& idx,
                                            float minConfidence,
                                            std::size_t* skippedLowConfidenceOut) {
    if (idx.topKTypes.empty()) {
        return {};
    }
    std::vector<float> sig(idx.topKTypes.size(), 0.0F);
    auto entitiesResult = kgStore.getDocEntitiesForDocument(docId);
    if (!entitiesResult) {
        return sig;
    }
    bool any = false;
    for (const auto& ent : entitiesResult.value()) {
        if (!ent.nodeId.has_value()) {
            continue;
        }
        if (ent.confidence.has_value() && ent.confidence.value() < minConfidence) {
            if (skippedLowConfidenceOut != nullptr) {
                ++(*skippedLowConfidenceOut);
            }
            continue;
        }
        auto typeIt = idx.nodeIdToType.find(*ent.nodeId);
        if (typeIt == idx.nodeIdToType.end()) {
            continue;
        }
        auto bucketIt = idx.typeToIdx.find(typeIt->second);
        if (bucketIt == idx.typeToIdx.end()) {
            continue;
        }
        sig[bucketIt->second] += ent.confidence.value_or(1.0F);
        any = true;
    }
    if (!any) {
        return {};
    }
    l2NormalizeInPlace(sig);
    return sig;
}

// Phase V composer: returns the final per-doc feature vector.
std::vector<float> composeFeatureVector(std::vector<float> dense,
                                        const std::vector<float>& matryoshkaWeights,
                                        const std::vector<float>& entitySignature,
                                        const std::vector<float>& minhashSketch,
                                        const FeatureComposition& cfg) {
    if (dense.empty()) {
        return dense; // nothing to compose against
    }
    // V-B: matryoshka coarse projection (no-op if disabled or target_dim >= full).
    if (cfg.enableMatryoshkaCoarseView && !matryoshkaWeights.empty() &&
        cfg.matryoshkaTargetDim > 0 && cfg.matryoshkaTargetDim < dense.size()) {
        dense = applyMatryoshkaCoarse(dense, matryoshkaWeights, cfg.matryoshkaTargetDim);
    } else {
        // Always L2-normalize the dense view to make weight tuning interpretable; embedding
        // already operates on cosine, so this is a no-op for clustering geometry but
        // makes weighted concat well-defined.
        l2NormalizeInPlace(dense);
    }
    // Determine weights. When all branches off, return dense unchanged (no-op == V0 baseline).
    const bool entityOn = cfg.enableEntityFusion && !entitySignature.empty();
    const bool minhashOn = cfg.enableMinHashSketch && !minhashSketch.empty();
    if (!entityOn && !minhashOn) {
        return dense;
    }
    const float alphaE = entityOn ? cfg.entityFusionAlpha : 0.0F;
    const float alphaM = minhashOn ? cfg.minhashAlpha : 0.0F;
    const float alphaD = std::max(0.0F, 1.0F - alphaE - alphaM);
    std::vector<float> composed;
    composed.reserve(dense.size() + (entityOn ? entitySignature.size() : 0) +
                     (minhashOn ? minhashSketch.size() : 0));
    for (float v : dense) {
        composed.push_back(v * alphaD);
    }
    if (entityOn) {
        for (float v : entitySignature) {
            composed.push_back(v * alphaE);
        }
    }
    if (minhashOn) {
        for (float v : minhashSketch) {
            composed.push_back(v * alphaM);
        }
    }
    return composed;
}

std::optional<std::string> documentHashFromNodeKey(std::string_view nodeKey) {
    if (!nodeKey.starts_with(kDocNodePrefix) || nodeKey.size() <= kDocNodePrefix.size()) {
        return std::nullopt;
    }
    return std::string{nodeKey.substr(kDocNodePrefix.size())};
}

std::vector<float> aggregateEmbedding(const std::vector<vector::VectorRecord>& records) {
    for (const auto& record : records) {
        if (record.level == vector::EmbeddingLevel::DOCUMENT && !record.embedding.empty()) {
            return record.embedding;
        }
    }

    std::vector<float> aggregate;
    std::size_t contributing = 0;
    for (const auto& record : records) {
        if (record.embedding.empty()) {
            continue;
        }
        if (aggregate.empty()) {
            aggregate.assign(record.embedding.begin(), record.embedding.end());
            contributing = 1;
            continue;
        }
        if (aggregate.size() != record.embedding.size()) {
            continue;
        }
        for (std::size_t i = 0; i < aggregate.size(); ++i) {
            aggregate[i] += record.embedding[i];
        }
        ++contributing;
    }

    if (contributing > 1) {
        for (auto& value : aggregate) {
            value /= static_cast<float>(contributing);
        }
    }
    return aggregate;
}

std::vector<TopologyNeighbor> collectNeighborsForDocument(metadata::KnowledgeGraphStore& kgStore,
                                                          std::int64_t nodeId,
                                                          std::size_t maxNeighborsPerDocument,
                                                          TopologyExtractionStats* stats) {
    // semantic_neighbor edges point from a document to its own k nearest neighbours, so the
    // outgoing edges are this document's neighbours and an incoming edge from the same node
    // makes the pair mutual. Fetch them separately (strongest first): a hub's many incoming
    // edges must not crowd its own neighbours out of a shared window.
    constexpr std::string_view kSemanticNeighbor{"semantic_neighbor"};
    auto outgoingResult = kgStore.getEdgesFrom(nodeId, kSemanticNeighbor,
                                               std::max<std::size_t>(maxNeighborsPerDocument, 1));
    if (!outgoingResult) {
        return {};
    }
    // Mutual edges carry the pair's similarity, so they rank near the top of the incoming list.
    auto incomingResult = kgStore.getEdgesTo(nodeId, kSemanticNeighbor,
                                             std::max<std::size_t>(maxNeighborsPerDocument * 4, 1));
    if (!incomingResult) {
        return {};
    }

    if (stats != nullptr) {
        stats->neighborEdgesScanned +=
            outgoingResult.value().size() + incomingResult.value().size();
    }

    std::unordered_map<std::int64_t, float> outgoingScores;
    std::unordered_set<std::int64_t> incomingIds;
    std::vector<std::int64_t> neighborNodeIds;
    neighborNodeIds.reserve(outgoingResult.value().size());

    for (const auto& edge : outgoingResult.value()) {
        auto it = outgoingScores.find(edge.dstNodeId);
        if (it == outgoingScores.end()) {
            outgoingScores.emplace(edge.dstNodeId, edge.weight);
            neighborNodeIds.push_back(edge.dstNodeId);
        } else {
            it->second = std::max(it->second, edge.weight);
        }
    }
    for (const auto& edge : incomingResult.value()) {
        incomingIds.insert(edge.srcNodeId);
    }

    if (neighborNodeIds.empty()) {
        return {};
    }

    auto nodesResult = kgStore.getNodesByIds(neighborNodeIds);
    if (!nodesResult) {
        return {};
    }

    std::vector<TopologyNeighbor> neighbors;
    neighbors.reserve(nodesResult.value().size());
    for (const auto& node : nodesResult.value()) {
        auto documentHash = documentHashFromNodeKey(node.nodeKey);
        if (!documentHash.has_value()) {
            continue;
        }
        const auto scoreIt = outgoingScores.find(node.id);
        if (scoreIt == outgoingScores.end()) {
            continue;
        }
        neighbors.push_back(TopologyNeighbor{.documentHash = *documentHash,
                                             .score = scoreIt->second,
                                             .reciprocal = incomingIds.contains(node.id)});
    }

    std::sort(neighbors.begin(), neighbors.end(),
              [](const TopologyNeighbor& lhs, const TopologyNeighbor& rhs) {
                  if (lhs.score != rhs.score) {
                      return lhs.score > rhs.score;
                  }
                  return lhs.documentHash < rhs.documentHash;
              });
    if (neighbors.size() > maxNeighborsPerDocument) {
        neighbors.resize(maxNeighborsPerDocument);
    }
    if (stats != nullptr) {
        stats->neighborsReturned += neighbors.size();
    }
    return neighbors;
}

} // namespace

TopologyInputExtractor::TopologyInputExtractor(
    std::shared_ptr<metadata::IMetadataRepository> metadataRepo,
    std::shared_ptr<metadata::KnowledgeGraphStore> kgStore,
    std::shared_ptr<vector::VectorDatabase> vectorDb)
    : metadataRepo_(std::move(metadataRepo)), kgStore_(std::move(kgStore)),
      vectorDb_(std::move(vectorDb)) {}

Result<std::vector<TopologyDocumentInput>>
TopologyInputExtractor::extract(const TopologyExtractionConfig& config,
                                TopologyExtractionStats* stats) const {
    if (!metadataRepo_) {
        return Error{ErrorCode::InvalidState, "topology extractor requires metadata repository"};
    }
    if (!kgStore_) {
        return Error{ErrorCode::InvalidState, "topology extractor requires knowledge graph store"};
    }
    if (config.includeEmbeddings && !vectorDb_) {
        return Error{ErrorCode::InvalidState,
                     "topology extractor requires vector database when embeddings are enabled"};
    }

    TopologyExtractionStats localStats;
    localStats.seedDocuments = config.documentHashes.size();
    localStats.documentsRequested = config.documentHashes.empty()
                                        ? static_cast<std::size_t>(std::max(config.limit, 0))
                                        : config.documentHashes.size();

    std::vector<metadata::DocumentInfo> documents;
    if (!config.documentHashes.empty()) {
        auto batchResult = metadataRepo_->batchGetDocumentsByHash(config.documentHashes);
        if (!batchResult) {
            return batchResult.error();
        }
        documents.reserve(batchResult.value().size());
        for (const auto& hash : config.documentHashes) {
            auto it = batchResult.value().find(hash);
            if (it != batchResult.value().end()) {
                documents.push_back(it->second);
            }
        }
    } else {
        metadata::DocumentQueryOptions options;
        options.limit = config.limit;
        auto queryResult = metadataRepo_->queryDocuments(options);
        if (!queryResult) {
            return queryResult.error();
        }
        documents = std::move(queryResult.value());
    }

    localStats.documentsLoaded = documents.size();

    std::unordered_map<std::string, vector::VectorRecord> docLevelVectors;
    if (config.includeEmbeddings && vectorDb_) {
        docLevelVectors = vectorDb_->getDocumentLevelVectorsAll();
    }
    std::optional<std::string> commonEmbeddingSpaceIdentity;
    std::optional<std::size_t> commonRawEmbeddingDimension;
    bool embeddingSpaceIdentityUnavailable = false;

    const auto observeEmbeddingSpaceIdentity = [&](std::string_view identity) {
        if (identity.empty()) {
            embeddingSpaceIdentityUnavailable = true;
            return;
        }
        if (!commonEmbeddingSpaceIdentity.has_value()) {
            commonEmbeddingSpaceIdentity = identity;
        } else if (*commonEmbeddingSpaceIdentity != identity) {
            embeddingSpaceIdentityUnavailable = true;
            localStats.mixedEmbeddingSpaces = true;
        }
    };

    // Phase V — feature composer prep:
    //   1. (V-A) build corpus top-K canonical entity-type histogram once
    //   2. (V-B) collect raw embeddings to compute matryoshka variance weights once
    EntityTypeIndex entityIdx;
    const FeatureComposition& fc = config.featureComposition;
    if (fc.enableEntityFusion && fc.entitySignatureK > 0) {
        entityIdx =
            buildEntityTypeIndex(*kgStore_, documents, fc.entitySignatureK, fc.entityMinConfidence);
        localStats.composedTopKEntityTypeCount = entityIdx.topKTypes.size();
    }
    std::vector<float> matryoshkaWeights;
    if (fc.enableMatryoshkaCoarseView && fc.matryoshkaTargetDim > 0 && config.includeEmbeddings &&
        vectorDb_) {
        std::vector<std::vector<float>> sample;
        sample.reserve(documents.size());
        for (const auto& doc : documents) {
            auto it = docLevelVectors.find(doc.sha256Hash);
            if (it != docLevelVectors.end() && !it->second.embedding.empty()) {
                sample.push_back(it->second.embedding);
                if (sample.size() >= 4096) {
                    break;
                }
            }
        }
        matryoshkaWeights = computeVarianceWeights(sample, fc.matryoshkaTargetDim);
    }

    std::vector<TopologyDocumentInput> extracted;
    extracted.reserve(documents.size());
    for (const auto& document : documents) {
        std::vector<float> embedding;
        std::string embeddingSpaceIdentity;
        if (config.includeEmbeddings && vectorDb_) {
            auto it = docLevelVectors.find(document.sha256Hash);
            if (it != docLevelVectors.end() && !it->second.embedding.empty()) {
                embedding = it->second.embedding;
                embeddingSpaceIdentity = it->second.model_id;
            } else {
                auto vectors = vectorDb_->getVectorsByDocument(document.sha256Hash);
                embedding = aggregateEmbedding(vectors);
                for (const auto& record : vectors) {
                    if (record.embedding.empty()) {
                        continue;
                    }
                    if (embeddingSpaceIdentity.empty()) {
                        embeddingSpaceIdentity = record.model_id;
                    } else if (embeddingSpaceIdentity != record.model_id) {
                        embeddingSpaceIdentity.clear();
                        localStats.mixedEmbeddingSpaces = true;
                        break;
                    }
                }
            }
            if (embedding.empty()) {
                ++localStats.documentsMissingEmbeddings;
                if (config.requireEmbeddings) {
                    continue;
                }
            } else {
                observeEmbeddingSpaceIdentity(embeddingSpaceIdentity);
                if (!commonRawEmbeddingDimension.has_value()) {
                    commonRawEmbeddingDimension = embedding.size();
                } else if (*commonRawEmbeddingDimension != embedding.size()) {
                    embeddingSpaceIdentityUnavailable = true;
                    localStats.mixedEmbeddingSpaces = true;
                }
            }
        }

        auto nodeResult = kgStore_->getNodeByKey(nodeKeyForDocumentHash(document.sha256Hash));
        if (!nodeResult) {
            return nodeResult.error();
        }
        if (!nodeResult.value().has_value()) {
            ++localStats.documentsMissingGraphNodes;
            if (config.requireGraphNode) {
                continue;
            }
        }

        // Phase V — compose final feature vector. When all branches off, this returns
        // the dense embedding unchanged (V0 baseline reproduces pre-Phase-V behaviour).
        std::vector<float> entitySig;
        if (fc.enableEntityFusion && !entityIdx.topKTypes.empty()) {
            entitySig =
                buildEntityTypeSignature(*kgStore_, document.id, entityIdx, fc.entityMinConfidence,
                                         &localStats.composedFilteredLowConfidenceCount);
            if (!entitySig.empty()) {
                ++localStats.composedDocsWithEntitySignature;
            }
        }
        // V-C: compute MinHash on the doc body in-extractor (no schema migration needed).
        // Skipped (and branch becomes no-op) when the body is unavailable.
        std::vector<float> minhashSketch;
        if (fc.enableMinHashSketch && fc.minhashSketchDim > 0) {
            auto contentResult = metadataRepo_->getContent(document.id);
            if (contentResult && contentResult.value().has_value()) {
                const auto& body = contentResult.value().value().contentText;
                if (!body.empty()) {
                    static thread_local simeon::MinHashEncoder kEncoder{simeon::MinHashConfig{}};
                    std::vector<std::uint32_t> sig(kEncoder.k());
                    kEncoder.encode(body, sig.data());
                    minhashSketch = bucketCountSketch(sig, fc.minhashSketchDim);
                    if (!minhashSketch.empty()) {
                        ++localStats.composedDocsWithMinHashSketch;
                    }
                }
            }
        }
        std::vector<float> composed;
        if (!embedding.empty()) {
            composed = composeFeatureVector(std::move(embedding), matryoshkaWeights, entitySig,
                                            minhashSketch, fc);
        }

        TopologyDocumentInput input;
        input.documentHash = document.sha256Hash;
        input.filePath = document.filePath;
        input.embedding = std::move(composed);

        if (config.includeMetadata) {
            auto metadataResult = metadataRepo_->getAllMetadata(document.id);
            if (!metadataResult) {
                return metadataResult.error();
            }
            input.metadata.reserve(metadataResult.value().size() + 3);
            for (const auto& [key, value] : metadataResult.value()) {
                input.metadata[key] = value.asString();
            }
            input.metadata["mime_type"] = document.mimeType;
            input.metadata["file_name"] = document.fileName;
            input.metadata["file_extension"] = document.fileExtension;
            ++localStats.metadataMapsLoaded;
        }
        resolveTypedInputFields(input, document, localStats);

        if (nodeResult.value().has_value()) {
            input.neighbors = collectNeighborsForDocument(
                *kgStore_, nodeResult.value()->id, config.maxNeighborsPerDocument, &localStats);
        }

        extracted.push_back(std::move(input));
    }

    std::sort(extracted.begin(), extracted.end(),
              [](const TopologyDocumentInput& lhs, const TopologyDocumentInput& rhs) {
                  return lhs.documentHash < rhs.documentHash;
              });

    localStats.documentsReturned = extracted.size();
    localStats.regionDocuments = extracted.size();
    if (!extracted.empty()) {
        localStats.composedFeatureDim = extracted.front().embedding.size();
    }
    if (!embeddingSpaceIdentityUnavailable && commonEmbeddingSpaceIdentity.has_value()) {
        localStats.embeddingSpaceIdentity = composedCoordinateSpaceIdentity(
            *commonEmbeddingSpaceIdentity, fc, commonRawEmbeddingDimension.value_or(0),
            localStats.composedFeatureDim, matryoshkaWeights, entityIdx.topKTypes);
    }
    if (stats != nullptr) {
        *stats = localStats;
    }
    return extracted;
}

} // namespace yams::topology
