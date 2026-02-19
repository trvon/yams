#include <yams/daemon/components/embed_preparer.h>

#include <algorithm>
#include <cctype>
#include <unordered_set>
#include <vector>

#include <yams/vector/document_chunker.h>
#include <yams/vector/vector_database.h>

namespace yams::daemon::embed {

namespace {

bool looksLikeHeading(const std::string& text) {
    if (text.empty()) {
        return false;
    }
    std::size_t lineEnd = text.find('\n');
    if (lineEnd == std::string::npos) {
        lineEnd = text.size();
    }
    std::string firstLine = text.substr(0, lineEnd);
    firstLine.erase(firstLine.begin(),
                    std::find_if(firstLine.begin(), firstLine.end(),
                                 [](unsigned char ch) { return !std::isspace(ch); }));
    firstLine.erase(std::find_if(firstLine.rbegin(), firstLine.rend(),
                                 [](unsigned char ch) { return !std::isspace(ch); })
                        .base(),
                    firstLine.end());

    if (firstLine.empty()) {
        return false;
    }
    if (firstLine.rfind("#", 0) == 0 || firstLine.rfind("##", 0) == 0) {
        return true;
    }
    if (firstLine.size() <= 120 && firstLine.back() == ':') {
        return true;
    }

    int alpha = 0;
    int upper = 0;
    for (unsigned char ch : firstLine) {
        if (std::isalpha(ch)) {
            ++alpha;
            if (std::isupper(ch)) {
                ++upper;
            }
        }
    }
    return alpha >= 6 && upper >= (alpha * 8) / 10;
}

struct ScoredChunk {
    double score;
    std::size_t idx;
};

} // namespace

std::optional<InternalEventBus::EmbedPreparedDoc>
prepareEmbedPreparedDoc(const EmbedSourceDoc& src, yams::vector::DocumentChunker& chunker,
                        const ConfigResolver::EmbeddingSelectionPolicy& selectionPolicy) {
    if (src.hash.empty() || src.extractedText.empty()) {
        return std::nullopt;
    }

    InternalEventBus::EmbedPreparedDoc doc;
    doc.hash = src.hash;
    doc.fileName = src.fileName;
    doc.filePath = src.filePath;
    doc.mimeType = src.mimeType;

    auto chunks = chunker.chunkDocument(src.extractedText, src.hash);
    if (chunks.empty()) {
        InternalEventBus::EmbedPreparedChunk c;
        c.chunkId = yams::vector::utils::generateChunkId(src.hash, 0);
        c.content = src.extractedText;
        c.startOffset = 0;
        c.endOffset = src.extractedText.size();
        doc.chunks.push_back(std::move(c));
        return doc;
    }

    const bool fullMode =
        selectionPolicy.mode == ConfigResolver::EmbeddingSelectionPolicy::Mode::Full;
    std::size_t effectiveMaxChunks = fullMode ? 0u : selectionPolicy.maxChunksPerDoc;
    std::size_t effectiveMaxChars = fullMode ? 0u : selectionPolicy.maxCharsPerDoc;

    if (!fullMode &&
        selectionPolicy.mode == ConfigResolver::EmbeddingSelectionPolicy::Mode::Adaptive) {
        const std::size_t localChunkCount = chunks.size();
        if (effectiveMaxChunks > 0) {
            effectiveMaxChunks = std::min<std::size_t>(
                std::max<std::size_t>(effectiveMaxChunks, 4u),
                std::max<std::size_t>(effectiveMaxChunks, 4u + (localChunkCount / 12u)));
        }
        if (effectiveMaxChars > 0) {
            effectiveMaxChars =
                std::max<std::size_t>(effectiveMaxChars, 16000u + localChunkCount * 256u);
        }
    }

    std::vector<ScoredChunk> scored;
    scored.reserve(chunks.size());
    if (selectionPolicy.strategy ==
        ConfigResolver::EmbeddingSelectionPolicy::Strategy::IntroHeadings) {
        // Preserve original order; selection loop below will filter.
        for (std::size_t rank = 0; rank < chunks.size(); ++rank) {
            scored.push_back(ScoredChunk{0.0, rank});
        }
    } else {
        for (std::size_t rank = 0; rank < chunks.size(); ++rank) {
            const auto& c = chunks[rank];
            const auto chunkSize = c.content.size();

            double score = 1.0 / (1.0 + static_cast<double>(rank));
            if (rank == 0) {
                score += selectionPolicy.introBoost;
            }
            if (looksLikeHeading(c.content)) {
                score += selectionPolicy.headingBoost;
            }
            if (chunkSize >= 200 && chunkSize <= 1600) {
                score += 0.25;
            } else if (chunkSize < 80) {
                score -= 0.15;
            }
            scored.push_back(ScoredChunk{score, rank});
        }

        std::stable_sort(
            scored.begin(), scored.end(),
            [](const ScoredChunk& a, const ScoredChunk& b) { return a.score > b.score; });
    }

    std::size_t selectedCount = 0;
    std::size_t selectedChars = 0;
    std::unordered_set<std::size_t> picked;
    picked.reserve(chunks.size());

    for (const auto& item : scored) {
        if (effectiveMaxChunks > 0 && selectedCount >= effectiveMaxChunks) {
            break;
        }
        const auto idx = item.idx;

        if (selectionPolicy.strategy ==
                ConfigResolver::EmbeddingSelectionPolicy::Strategy::IntroHeadings &&
            idx != 0 && !looksLikeHeading(chunks[idx].content)) {
            continue;
        }
        const auto chunkSize = chunks[idx].content.size();
        if (effectiveMaxChars > 0 && selectedChars > 0 &&
            (selectedChars + chunkSize) > effectiveMaxChars) {
            continue;
        }

        InternalEventBus::EmbedPreparedChunk out;
        out.chunkId = chunks[idx].chunk_id.empty()
                          ? yams::vector::utils::generateChunkId(src.hash, idx)
                          : chunks[idx].chunk_id;
        out.content = std::move(chunks[idx].content);
        out.startOffset = chunks[idx].start_offset;
        out.endOffset = chunks[idx].end_offset;
        doc.chunks.push_back(std::move(out));

        picked.insert(idx);
        selectedCount += 1;
        selectedChars += chunkSize;
    }

    if (doc.chunks.empty()) {
        const auto idx = 0u;
        InternalEventBus::EmbedPreparedChunk out;
        out.chunkId = chunks[idx].chunk_id.empty()
                          ? yams::vector::utils::generateChunkId(src.hash, idx)
                          : chunks[idx].chunk_id;
        out.content = std::move(chunks[idx].content);
        out.startOffset = chunks[idx].start_offset;
        out.endOffset = chunks[idx].end_offset;
        doc.chunks.push_back(std::move(out));
    }

    return doc;
}

} // namespace yams::daemon::embed
