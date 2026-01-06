#pragma once

#include <nlohmann/json.hpp>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace yams::storage {

/**
 * @brief Corpus-level statistics for adaptive search tuning.
 *
 * These metrics characterize the indexed corpus to inform the SearchTuner FSM
 * about optimal parameter selection (RRF k, component weights, etc.).
 *
 * Metrics are computed from database queries and cached with a configurable TTL.
 */
struct CorpusStats {
    // --- Size metrics ---
    int64_t docCount{0};           // Total documents indexed
    int64_t totalSizeBytes{0};     // Sum of all document sizes (file_size)
    double avgDocLengthBytes{0.0}; // Average document size in bytes

    // --- Content type ratios ---
    // Based on file extension classification
    double codeRatio{0.0};   // Fraction of code files (.cpp, .py, .js, .rs, .go, etc.)
    double proseRatio{0.0};  // Fraction of prose files (.md, .txt, .rst, .doc, etc.)
    double binaryRatio{0.0}; // Fraction of binary files (images, archives, executables, etc.)

    // --- Feature coverage ---
    int64_t embeddingCount{0};     // Documents with embeddings (has_embedding=1)
    double embeddingCoverage{0.0}; // embeddingCount / docCount

    int64_t tagCount{0};     // Total tag assignments (metadata key='tag' or 'tag:*')
    int64_t docsWithTags{0}; // Documents with at least one tag
    double tagCoverage{0.0}; // docsWithTags / docCount

    int64_t symbolCount{0};    // Total KG entities (from kg_doc_entities)
    double symbolDensity{0.0}; // symbolCount / docCount (entities per document)

    // --- Path structure ---
    double pathDepthAvg{0.0}; // Average path depth (number of '/' segments)
    double pathDepthMax{0.0}; // Maximum path depth

    // --- Language distribution (optional, for future use) ---
    std::unordered_map<std::string, int64_t> extensionCounts; // extension -> count

    // --- Timestamps ---
    int64_t computedAtMs{0}; // When these stats were computed (unix ms)
    int64_t ttlMs{60000};    // Cache TTL (default 60s)

    // --- Helpers ---
    [[nodiscard]] bool isExpired(int64_t nowMs) const noexcept {
        return (nowMs - computedAtMs) > ttlMs;
    }

    [[nodiscard]] bool isEmpty() const noexcept { return docCount == 0; }

    [[nodiscard]] bool isCodeDominant() const noexcept { return codeRatio > 0.7; }
    [[nodiscard]] bool isProseDominant() const noexcept { return proseRatio > 0.7; }
    [[nodiscard]] bool isMixed() const noexcept { return !isCodeDominant() && !isProseDominant(); }

    [[nodiscard]] bool hasKnowledgeGraph() const noexcept { return symbolDensity > 0.1; }
    [[nodiscard]] bool hasPaths() const noexcept { return pathDepthAvg > 1.0; }
    [[nodiscard]] bool hasTags() const noexcept { return tagCoverage > 0.1; }
    [[nodiscard]] bool hasEmbeddings() const noexcept { return embeddingCoverage > 0.5; }

    // Size classification for FSM
    [[nodiscard]] bool isMinimal() const noexcept { return docCount < 100; }
    [[nodiscard]] bool isSmall() const noexcept { return docCount < 1000; }
    [[nodiscard]] bool isLarge() const noexcept { return docCount >= 10000; }

    // Scientific corpus detection (prose without path/tag structure, like benchmarks)
    [[nodiscard]] bool isScientific() const noexcept {
        return isProseDominant() && pathDepthAvg < 1.5 && tagCoverage < 0.1;
    }

    /**
     * @brief Serialize to JSON for daemon status / CLI output.
     */
    [[nodiscard]] nlohmann::json toJson() const {
        nlohmann::json j;
        j["doc_count"] = docCount;
        j["total_size_bytes"] = totalSizeBytes;
        j["avg_doc_length_bytes"] = avgDocLengthBytes;
        j["code_ratio"] = codeRatio;
        j["prose_ratio"] = proseRatio;
        j["binary_ratio"] = binaryRatio;
        j["embedding_count"] = embeddingCount;
        j["embedding_coverage"] = embeddingCoverage;
        j["tag_count"] = tagCount;
        j["docs_with_tags"] = docsWithTags;
        j["tag_coverage"] = tagCoverage;
        j["symbol_count"] = symbolCount;
        j["symbol_density"] = symbolDensity;
        j["path_depth_avg"] = pathDepthAvg;
        j["path_depth_max"] = pathDepthMax;
        j["computed_at_ms"] = computedAtMs;

        // Extension breakdown (top 10)
        nlohmann::json extJson = nlohmann::json::object();
        std::vector<std::pair<std::string, int64_t>> sorted(extensionCounts.begin(),
                                                            extensionCounts.end());
        std::sort(sorted.begin(), sorted.end(),
                  [](const auto& a, const auto& b) { return a.second > b.second; });
        int count = 0;
        for (const auto& [ext, cnt] : sorted) {
            if (++count > 10)
                break;
            extJson[ext] = cnt;
        }
        j["top_extensions"] = extJson;

        // Classification summary
        j["classification"] = nlohmann::json::object();
        j["classification"]["is_code_dominant"] = isCodeDominant();
        j["classification"]["is_prose_dominant"] = isProseDominant();
        j["classification"]["is_mixed"] = isMixed();
        j["classification"]["is_scientific"] = isScientific();
        j["classification"]["is_minimal"] = isMinimal();
        j["classification"]["is_small"] = isSmall();
        j["classification"]["is_large"] = isLarge();
        j["classification"]["has_kg"] = hasKnowledgeGraph();
        j["classification"]["has_paths"] = hasPaths();
        j["classification"]["has_tags"] = hasTags();
        j["classification"]["has_embeddings"] = hasEmbeddings();

        return j;
    }

    /**
     * @brief Create from JSON (for deserialization).
     */
    static CorpusStats fromJson(const nlohmann::json& j) {
        CorpusStats stats;
        if (j.contains("doc_count"))
            stats.docCount = j["doc_count"].get<int64_t>();
        if (j.contains("total_size_bytes"))
            stats.totalSizeBytes = j["total_size_bytes"].get<int64_t>();
        if (j.contains("avg_doc_length_bytes"))
            stats.avgDocLengthBytes = j["avg_doc_length_bytes"].get<double>();
        if (j.contains("code_ratio"))
            stats.codeRatio = j["code_ratio"].get<double>();
        if (j.contains("prose_ratio"))
            stats.proseRatio = j["prose_ratio"].get<double>();
        if (j.contains("binary_ratio"))
            stats.binaryRatio = j["binary_ratio"].get<double>();
        if (j.contains("embedding_count"))
            stats.embeddingCount = j["embedding_count"].get<int64_t>();
        if (j.contains("embedding_coverage"))
            stats.embeddingCoverage = j["embedding_coverage"].get<double>();
        if (j.contains("tag_count"))
            stats.tagCount = j["tag_count"].get<int64_t>();
        if (j.contains("docs_with_tags"))
            stats.docsWithTags = j["docs_with_tags"].get<int64_t>();
        if (j.contains("tag_coverage"))
            stats.tagCoverage = j["tag_coverage"].get<double>();
        if (j.contains("symbol_count"))
            stats.symbolCount = j["symbol_count"].get<int64_t>();
        if (j.contains("symbol_density"))
            stats.symbolDensity = j["symbol_density"].get<double>();
        if (j.contains("path_depth_avg"))
            stats.pathDepthAvg = j["path_depth_avg"].get<double>();
        if (j.contains("path_depth_max"))
            stats.pathDepthMax = j["path_depth_max"].get<double>();
        if (j.contains("computed_at_ms"))
            stats.computedAtMs = j["computed_at_ms"].get<int64_t>();
        if (j.contains("top_extensions") && j["top_extensions"].is_object()) {
            for (auto& [key, val] : j["top_extensions"].items()) {
                stats.extensionCounts[key] = val.get<int64_t>();
            }
        }
        return stats;
    }
};

// File extension classification sets (used by getCorpusStats implementation)
namespace detail {

// Code file extensions
inline const std::unordered_set<std::string> kCodeExtensions = {
    // C/C++
    ".c",
    ".cc",
    ".cpp",
    ".cxx",
    ".h",
    ".hpp",
    ".hxx",
    ".hh",
    // Rust
    ".rs",
    // Go
    ".go",
    // Python
    ".py",
    ".pyw",
    ".pyi",
    // JavaScript/TypeScript
    ".js",
    ".jsx",
    ".ts",
    ".tsx",
    ".mjs",
    ".cjs",
    // Java/Kotlin/Scala
    ".java",
    ".kt",
    ".kts",
    ".scala",
    // C#/F#
    ".cs",
    ".fs",
    ".fsx",
    // Ruby
    ".rb",
    ".rake",
    // PHP
    ".php",
    // Swift
    ".swift",
    // Shell
    ".sh",
    ".bash",
    ".zsh",
    ".fish",
    // SQL
    ".sql",
    // Zig/Nim/V
    ".zig",
    ".nim",
    ".v",
    // Lua
    ".lua",
    // Perl
    ".pl",
    ".pm",
    // Assembly
    ".asm",
    ".s",
    // Web
    ".html",
    ".htm",
    ".css",
    ".scss",
    ".sass",
    ".less",
    // Config as code
    ".json",
    ".yaml",
    ".yml",
    ".toml",
    ".ini",
    ".xml",
    // Build files
    ".cmake",
    ".meson",
    ".gradle",
};

// Prose/documentation file extensions
inline const std::unordered_set<std::string> kProseExtensions = {
    ".md",   ".markdown", ".txt", ".text", ".rst", ".asciidoc", ".adoc", ".doc",
    ".docx", ".odt",      ".rtf", ".pdf",  ".tex", ".latex",    ".org",  ".wiki",
};

// Binary file extensions (non-text)
inline const std::unordered_set<std::string> kBinaryExtensions = {
    // Images
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".bmp",
    ".ico",
    ".svg",
    ".webp",
    ".tiff",
    // Audio/Video
    ".mp3",
    ".mp4",
    ".wav",
    ".avi",
    ".mkv",
    ".mov",
    ".flac",
    ".ogg",
    // Archives
    ".zip",
    ".tar",
    ".gz",
    ".bz2",
    ".xz",
    ".7z",
    ".rar",
    // Executables
    ".exe",
    ".dll",
    ".so",
    ".dylib",
    ".a",
    ".o",
    ".obj",
    // Databases
    ".db",
    ".sqlite",
    ".sqlite3",
    // Fonts
    ".ttf",
    ".otf",
    ".woff",
    ".woff2",
    // Other binary
    ".bin",
    ".dat",
    ".iso",
    ".img",
};

} // namespace detail

} // namespace yams::storage
