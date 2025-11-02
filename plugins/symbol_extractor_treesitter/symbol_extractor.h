#pragma once

#include <format>

extern "C" {
#include <tree_sitter/api.h>
}

#include <memory>
#include <optional>
#include <source_location>
#include <span>
#include <string>
#include <string_view>
#include <vector>
#include <tl/expected.hpp>

namespace yams::plugins::treesitter {

// Forward declarations
struct yams_symbol_v1;
struct yams_symbol_relation_v1;
struct yams_symbol_extraction_result_v1;

/**
 * @brief Enhanced symbol information with C++20 features
 */
struct SymbolInfo {
    std::string name;
    std::string qualified_name;
    std::string kind; // "function", "class", "struct", "method", etc.
    std::string file_path;

    uint32_t start_line = 0;   // 1-based
    uint32_t end_line = 0;     // 1-based
    uint32_t start_offset = 0; // Byte offset
    uint32_t end_offset = 0;   // Byte offset

    std::optional<std::string> return_type;
    std::vector<std::string> parameters;
    std::optional<std::string> documentation;

    [[nodiscard]] bool is_valid() const noexcept {
        return !name.empty() && start_line > 0 && end_line >= start_line &&
               start_offset < end_offset;
    }

    [[nodiscard]] std::string to_string() const {
        return std::format("{} [{}] at L{}:{}-L{}:{}", name, kind, start_line, start_offset,
                           end_line, end_offset);
    }
};

/**
 * @brief Symbol relationship information
 */
struct SymbolRelation {
    std::string src_symbol; // Caller/source
    std::string dst_symbol; // Callee/destination
    std::string kind;       // "calls", "inherits", "includes", etc.
    double weight = 1.0;    // Relationship strength 0.0-1.0

    [[nodiscard]] bool is_valid() const noexcept {
        return !src_symbol.empty() && !dst_symbol.empty() && !kind.empty();
    }

    [[nodiscard]] std::string to_string() const {
        return std::format("{} --[{} {}]--> {}", src_symbol, kind, weight, dst_symbol);
    }
};

/**
 * @brief Comprehensive extraction results with statistics
 */
struct ExtractionResult {
    std::vector<SymbolInfo> symbols;
    std::vector<SymbolRelation> relations;

    struct Stats {
        size_t total_symbols = 0;
        size_t functions = 0;
        size_t classes = 0;
        size_t structs = 0;
        size_t variables = 0;
        size_t relations = 0;
        size_t calls = 0;
        size_t inherits = 0;
    } stats;

    std::optional<std::string> error;

    [[nodiscard]] bool success() const noexcept { return !error.has_value(); }
    [[nodiscard]] bool has_symbols() const noexcept { return !symbols.empty(); }
    [[nodiscard]] bool has_relations() const noexcept { return !relations.empty(); }

    void calculate_stats() noexcept {
        stats.total_symbols = symbols.size();
        stats.relations = relations.size();

        for (const auto& sym : symbols) {
            if (sym.kind == "function")
                ++stats.functions;
            else if (sym.kind == "class")
                ++stats.classes;
            else if (sym.kind == "struct")
                ++stats.structs;
            else if (sym.kind == "variable")
                ++stats.variables;
        }

        for (const auto& rel : relations) {
            if (rel.kind == "calls")
                ++stats.calls;
            else if (rel.kind == "inherits")
                ++stats.inherits;
        }
    }

    [[nodiscard]] std::string summary() const {
        return std::format("Extraction: {} symbols ({} functions, {} classes, {} structs), {} "
                           "relations ({} calls)",
                           stats.total_symbols, stats.functions, stats.classes, stats.structs,
                           stats.relations, stats.calls);
    }
};

/**
 * @brief C++20/23 symbol extraction engine with proper error handling
 */
class SymbolExtractor {
public:
    using Result = tl::expected<ExtractionResult, std::string>;

    explicit SymbolExtractor(TSLanguage* language);
    ~SymbolExtractor() = default;

    SymbolExtractor(const SymbolExtractor&) = delete;
    SymbolExtractor& operator=(const SymbolExtractor&) = delete;
    SymbolExtractor(SymbolExtractor&&) = default;
    SymbolExtractor& operator=(SymbolExtractor&&) = default;

    /**
     * @brief Extract symbols and relations from source code
     * @param content Source code content
     * @param language Language name for query selection
     * @param file_path Source file path for error reporting
     * @param enable_call_graph Extract call relationships (slower)
     * @return Extraction results or error message
     */
    Result extract(std::string_view content, std::string_view language, std::string_view file_path,
                   bool enable_call_graph = true,
                   std::source_location loc = std::source_location::current());

private:
    struct ExtractionContext {
        std::string_view content;
        std::string_view language;
        std::string_view file_path;
        TSTree* tree{nullptr};
        TSNode root{};
        bool enable_call_graph{true};

        [[nodiscard]] std::string extractNodeText(TSNode node) const {
            if (ts_node_is_null(node))
                return "";

            uint32_t start_byte = ts_node_start_byte(node);
            uint32_t end_byte = ts_node_end_byte(node);

            if (start_byte >= end_byte || end_byte > content.length())
                return "";

            return std::string(content.substr(start_byte, end_byte - start_byte));
        }
    };

    Result extractFunctions(const ExtractionContext& ctx);
    Result extractClasses(const ExtractionContext& ctx);
    Result extractStructs(const ExtractionContext& ctx);
    Result extractCallRelations(const ExtractionContext& ctx,
                                const std::vector<SymbolInfo>& symbols);

    bool executeQuery(const ExtractionContext& ctx, std::string_view query_text,
                      std::string_view symbol_kind, std::vector<SymbolInfo>& output);

    TSLanguage* language_;
    mutable std::unique_ptr<TSParser, decltype(&ts_parser_delete)> parser_{nullptr,
                                                                           ts_parser_delete};
};

} // namespace yams::plugins::treesitter