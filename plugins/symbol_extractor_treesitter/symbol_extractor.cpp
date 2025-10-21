#include "symbol_extractor.h"
#include "grammar_loader.h"

#include <algorithm>
#include <expected>
#include <format>
#include <ranges>
#include <set>
#include <unordered_map>
#include <vector>

#include <cstring>
#include <functional>

extern "C" {
#include <tree_sitter/api.h>
}

namespace yams::plugins::treesitter {

// SymbolExtractor Implementation
SymbolExtractor::SymbolExtractor(TSLanguage* language)
    : language_(language), parser_(ts_parser_new(), ts_parser_delete) {
    if (language_) {
        ts_parser_set_language(parser_.get(), language_);
    }
}

SymbolExtractor::Result SymbolExtractor::extract(std::string_view content,
                                                 std::string_view language,
                                                 std::string_view file_path, bool enable_call_graph,
                                                 std::source_location loc) {
    if (!language_) {
        return std::unexpected(std::format("No tree-sitter language loaded for {} (at {}:{})",
                                           language, loc.file_name(), loc.line()));
    }

    if (content.empty()) {
        return std::unexpected("Empty content provided");
    }

    // Defensive: verify parser language set successfully
    if (ts_parser_language(parser_.get()) == nullptr) {
        return std::unexpected("Parser has no language set");
    }

    ExtractionResult result;
    ExtractionContext ctx{.content = content,
                          .language = language,
                          .file_path = file_path,
                          .enable_call_graph = enable_call_graph};

    // Parse the content (guard against null pointers)
    if (!parser_.get() || content.data() == nullptr) {
        return std::unexpected("Invalid parser or content");
    }
    // Use ts_parser_set_language already set in ctor. Extra guard around parse.
    ctx.tree =
        ts_parser_parse_string(parser_.get(), nullptr, content.data(), (uint32_t)content.length());
    if (!ctx.tree) {
        return std::unexpected("Failed to parse content");
    }
    ctx.root = ts_tree_root_node(ctx.tree);

    try {
        // Extract symbols by type
        auto func_result = extractFunctions(ctx);
        if (!func_result)
            return func_result;

        auto class_result = extractClasses(ctx);
        if (!class_result)
            return class_result;

        auto struct_result = extractStructs(ctx);
        if (!struct_result)
            return struct_result;

        // Combine all symbols
        result.symbols.reserve(func_result->symbols.size() + class_result->symbols.size() +
                               struct_result->symbols.size());
        std::ranges::move(func_result->symbols, std::back_inserter(result.symbols));
        std::ranges::move(class_result->symbols, std::back_inserter(result.symbols));
        std::ranges::move(struct_result->symbols, std::back_inserter(result.symbols));

        // Extract call relationships if enabled
        if (enable_call_graph) {
            auto call_result = extractCallRelations(ctx, result.symbols);
            if (call_result) {
                std::ranges::move(call_result->relations, std::back_inserter(result.relations));
            }
        }

        result.calculate_stats();

    } catch (const std::exception& e) {
        if (ctx.tree)
            ts_tree_delete(ctx.tree);
        return std::unexpected(std::format("Extraction exception: {}", e.what()));
    }

    if (ctx.tree)
        ts_tree_delete(ctx.tree);
    return result;
}

SymbolExtractor::Result SymbolExtractor::extractFunctions(const ExtractionContext& ctx) {
    ExtractionResult result;

    std::string_view language = ctx.language;
    std::vector<std::string_view> queries;

    // Language-specific queries for function extraction
    if (language == "cpp" || language == "c++" || language == "c") {
        queries = {
            // Function definitions: int foo() { ... }
            "(function_definition declarator: (function_declarator declarator: (identifier) "
            "@name))",
            // Method definitions: class::method() { ... }
            "(function_definition declarator: (function_declarator declarator: "
            "(qualified_identifier name: (identifier) @name)))",
            // Simple function declarations: int foo();
            "(function_declaration declarator: (identifier) @name)",
        };
    } else if (language == "python") {
        queries = {
            "(function_definition name: (identifier) @name)",
        };
    } else if (language == "rust") {
        queries = {
            "(function_item name: (identifier) @name)",
        };
    } else if (language == "go") {
        queries = {
            "(function_declaration name: (identifier) @name)",
            "(method_declaration name: (field_identifier) @name)",
        };
    } else if (language == "javascript" || language == "js") {
        queries = {
            "(function_declaration name: (identifier) @name)",
            "(method_definition name: (property_identifier) @name)",
        };
    }

    // Execute queries
    for (auto query : queries) {
        if (executeQuery(ctx, query, "function", result.symbols)) {
            // Query succeeded, don't try recursive traversal
            return result;
        }
    }

    // Fallback: recursive traversal
    std::function<void(TSNode)> traverse;
    traverse = [&ctx, &result, &traverse, this](TSNode node) {
        if (ts_node_is_null(node))
            return;

        const char* node_type = ts_node_type(node);

        // Language-specific function detection
        bool is_function = false;
        std::string_view lang = ctx.language;

        if ((lang == "cpp" || lang == "c++" || lang == "c") &&
            (std::strstr(node_type, "function_definition") ||
             std::strstr(node_type, "function_declaration"))) {
            is_function = true;
        } else if (lang == "python" && std::strcmp(node_type, "function_definition") == 0) {
            is_function = true;
        } else if (lang == "rust" && std::strcmp(node_type, "function_item") == 0) {
            is_function = true;
        } else if (lang == "go" && (std::strcmp(node_type, "function_declaration") == 0 ||
                                    std::strcmp(node_type, "method_declaration") == 0)) {
            is_function = true;
        } else if ((lang == "javascript" || lang == "js") &&
                   (std::strcmp(node_type, "function_declaration") == 0 ||
                    std::strcmp(node_type, "method_definition") == 0)) {
            is_function = true;
        }

        if (is_function) {
            // Find function identifier
            std::function<TSNode(TSNode)> find_identifier;
            find_identifier = [&find_identifier](TSNode n) -> TSNode {
                if (ts_node_is_null(n))
                    return TSNode{};

                const char* type = ts_node_type(n);
                if (std::strcmp(type, "identifier") == 0 ||
                    std::strcmp(type, "field_identifier") == 0 ||
                    std::strcmp(type, "property_identifier") == 0) {
                    return n;
                }

                // Check children
                uint32_t child_count = ts_node_child_count(n);
                for (uint32_t i = 0; i < child_count; ++i) {
                    TSNode child = ts_node_child(n, i);
                    TSNode found = find_identifier(child);
                    if (!ts_node_is_null(found))
                        return found;
                }
                return TSNode{};
            };

            TSNode name_node = find_identifier(node);
            if (!ts_node_is_null(name_node)) {
                SymbolInfo sym;
                sym.name = ctx.extractNodeText(name_node);
                sym.qualified_name = sym.name; // Simple case
                sym.kind = "function";
                sym.file_path = std::string(ctx.file_path);

                TSPoint start = ts_node_start_point(node);
                TSPoint end = ts_node_end_point(node);
                sym.start_line = start.row + 1;
                sym.end_line = end.row + 1;
                sym.start_offset = ts_node_start_byte(node);
                sym.end_offset = ts_node_end_byte(node);

                // Try to extract return type
                uint32_t child_count = ts_node_child_count(node);
                if (child_count > 0) {
                    TSNode first_child = ts_node_child(node, 0);
                    if (std::strcmp(ts_node_type(first_child), "primitive_type") == 0 ||
                        std::strcmp(ts_node_type(first_child), "type_identifier") == 0) {
                        sym.return_type = ctx.extractNodeText(first_child);
                    }
                }

                if (sym.is_valid()) {
                    result.symbols.push_back(std::move(sym));
                }
            }
        }

        // Recurse to children
        uint32_t child_count = ts_node_child_count(node);
        for (uint32_t i = 0; i < child_count; ++i) {
            traverse(ts_node_child(node, i));
        }
    };

    traverse(ctx.root);
    return result;
}

SymbolExtractor::Result SymbolExtractor::extractClasses(const ExtractionContext& ctx) {
    ExtractionResult result;

    // Class extraction is primarily for C++/Java style languages
    std::string_view language = ctx.language;
    std::vector<std::string_view> queries;

    if (language == "cpp" || language == "c++") {
        queries = {
            "(class_specifier name: (type_identifier) @name)",
            "(struct_specifier name: (type_identifier) @name)",
        };
    } else if (language == "python") {
        queries = {
            "(class_definition name: (identifier) @name)",
        };
    } else if (language == "java") {
        queries = {
            "(class_declaration name: (identifier) @name)",
            "(interface_declaration name: (identifier) @name)",
            "(enum_declaration name: (identifier) @name)",
        };
    } else if (language == "javascript" || language == "js" || language == "js") {
        queries = {
            "(class_declaration name: (identifier) @name)",
        };
    }

    // Execute queries
    for (auto query : queries) {
        std::string kind = "class";
        if (std::strstr(query.data(), "struct"))
            kind = "struct";
        else if (std::strstr(query.data(), "interface"))
            kind = "interface";
        else if (std::strstr(query.data(), "enum"))
            kind = "enum";

        if (executeQuery(ctx, query, kind, result.symbols)) {
            // Query succeeded for this type
        }
    }

    return result;
}

SymbolExtractor::Result SymbolExtractor::extractStructs(const ExtractionContext& ctx) {
    // For C, structs are extracted in the class extraction phase
    // This provides a separate hook for languages where structs are distinct
    return ExtractionResult{};
}

bool SymbolExtractor::executeQuery(const ExtractionContext& ctx, std::string_view query_text,
                                   std::string_view symbol_kind, std::vector<SymbolInfo>& output) {
    TSQueryError error_type = TSQueryErrorNone;
    uint32_t error_offset = 0;

    TSLanguage* lang = language_;
    TSQuery* query =
        ts_query_new(lang, query_text.data(), query_text.size(), &error_offset, &error_type);
    if (!query)
        return false;

    auto query_deleter = [](TSQuery* q) {
        if (q)
            ts_query_delete(q);
    };
    std::unique_ptr<TSQuery, decltype(query_deleter)> query_guard(query, query_deleter);

    TSQueryCursor* cursor = ts_query_cursor_new();
    auto cursor_deleter = [](TSQueryCursor* c) {
        if (c)
            ts_query_cursor_delete(c);
    };
    std::unique_ptr<TSQueryCursor, decltype(cursor_deleter)> cursor_guard(cursor, cursor_deleter);

    ts_query_cursor_exec(cursor, query, ctx.root);

    bool found_any = false;
    TSQueryMatch match;
    while (ts_query_cursor_next_match(cursor, &match)) {
        for (uint32_t i = 0; i < match.capture_count; ++i) {
            TSQueryCapture capture = match.captures[i];
            uint32_t name_len = 0;
            const char* capture_name =
                ts_query_capture_name_for_id(query, capture.index, &name_len);

            if (capture_name && std::string_view(capture_name) == "name") {
                std::string name_text = ctx.extractNodeText(capture.node);
                if (!name_text.empty()) {
                    SymbolInfo sym;
                    sym.name = std::move(name_text);
                    sym.qualified_name = sym.name; // Simple case
                    sym.kind = std::string(symbol_kind);
                    sym.file_path = std::string(ctx.file_path);

                    TSPoint start = ts_node_start_point(capture.node);
                    TSPoint end = ts_node_end_point(capture.node);
                    sym.start_line = start.row + 1;
                    sym.end_line = end.row + 1;
                    sym.start_offset = ts_node_start_byte(capture.node);
                    sym.end_offset = ts_node_end_byte(capture.node);

                    // Find parent node for full symbol bounds
                    TSNode parent_node = ts_node_parent(capture.node);
                    while (!ts_node_is_null(parent_node)) {
                        const char* node_type = ts_node_type(parent_node);
                        if (std::strstr(node_type, symbol_kind.data())) {
                            // Use parent bounds for better span
                            TSPoint parent_start = ts_node_start_point(parent_node);
                            TSPoint parent_end = ts_node_end_point(parent_node);
                            sym.start_line = parent_start.row + 1;
                            sym.end_line = parent_end.row + 1;
                            sym.start_offset = ts_node_start_byte(parent_node);
                            sym.end_offset = ts_node_end_byte(parent_node);
                            break;
                        }
                        parent_node = ts_node_parent(parent_node);
                    }

                    if (sym.is_valid()) {
                        output.push_back(std::move(sym));
                        found_any = true;
                    }
                }
            }
        }
    }

    return found_any;
}

SymbolExtractor::Result
SymbolExtractor::extractCallRelations(const ExtractionContext& ctx,
                                      const std::vector<SymbolInfo>& symbols) {
    ExtractionResult result;

    // Build symbol lookup by name and qualified name
    std::unordered_map<std::string, const SymbolInfo*> symbol_map;
    for (const auto& sym : symbols) {
        symbol_map[sym.name] = &sym;
        if (sym.qualified_name != sym.name) {
            symbol_map[sym.qualified_name] = &sym;
        }
    }

    // Query patterns for call expressions
    std::vector<std::string_view> call_patterns;
    std::string_view lang = ctx.language;

    if (lang == "cpp" || lang == "c++" || lang == "c") {
        call_patterns = {
            "(call_expression function: (identifier) @callee)",
            "(call_expression function: (field_expression field: (field_identifier) @callee))",
            "(call_expression function: (qualified_identifier name: (identifier) @callee))",
        };
    } else if (lang == "python") {
        call_patterns = {
            "(call_expression function: (identifier) @callee)",
            "(call_expression function: (attribute attribute: (identifier) @callee))",
        };
    } else if (lang == "rust") {
        call_patterns = {
            "(call_expression function: (identifier) @callee)",
        };
    } else if (lang == "go") {
        call_patterns = {
            "(call_expression function: (identifier) @callee)",
        };
    }

    if (call_patterns.empty()) {
        return result; // Call graph not supported for this language
    }

    // Find all call sites
    std::unordered_map<std::string /*caller*/, std::set<std::string /*callee*/>> call_sites;

    // First pass: find all functions that could be callers
    std::vector<TSNode> function_nodes;
    std::function<void(TSNode)> collect_functions;
    collect_functions = [&](TSNode node) {
        if (ts_node_is_null(node))
            return;

        const char* type = ts_node_type(node);
        if (std::strstr(type, "function")) {
            function_nodes.push_back(node);
        }

        uint32_t children = ts_node_child_count(node);
        for (uint32_t i = 0; i < children; ++i) {
            collect_functions(ts_node_child(node, i));
        }
    };

    collect_functions(ctx.root);

    // For each function, find calls within it
    for (TSNode func_node : function_nodes) {
        // Find function name
        std::string caller_name;
        std::function<void(TSNode)> find_func_name;
        find_func_name = [&](TSNode node) {
            if (ts_node_is_null(node))
                return;

            const char* type = ts_node_type(node);
            if (std::strcmp(type, "identifier") == 0 ||
                std::strcmp(type, "field_identifier") == 0 ||
                std::strcmp(type, "property_identifier") == 0) {
                caller_name = ctx.extractNodeText(node);
                return;
            }

            // Continue searching
            uint32_t children = ts_node_child_count(node);
            for (uint32_t i = 0; i < children; ++i) {
                find_func_name(ts_node_child(node, i));
            }
        };
        find_func_name(func_node);

        if (caller_name.empty() || !symbol_map.count(caller_name)) {
            continue; // Not a known function we extracted
        }

        // Find calls within this function
        for (auto pattern : call_patterns) {
            uint32_t error_offset = 0;
            TSQueryError error_type = TSQueryErrorNone;
            TSQuery* query =
                ts_query_new(language_, pattern.data(), pattern.size(), &error_offset, &error_type);
            if (!query)
                continue;

            auto query_deleter = [](TSQuery* q) {
                if (q)
                    ts_query_delete(q);
            };
            std::unique_ptr<TSQuery, decltype(query_deleter)> query_guard(query, query_deleter);

            TSQueryCursor* cursor = ts_query_cursor_new();
            auto cursor_deleter = [](TSQueryCursor* c) {
                if (c)
                    ts_query_cursor_delete(c);
            };
            std::unique_ptr<TSQueryCursor, decltype(cursor_deleter)> cursor_guard(cursor,
                                                                                  cursor_deleter);

            ts_query_cursor_exec(cursor, query, func_node);

            TSQueryMatch match;
            while (ts_query_cursor_next_match(cursor, &match)) {
                for (uint32_t i = 0; i < match.capture_count; ++i) {
                    TSQueryCapture capture = match.captures[i];
                    uint32_t name_len = 0;
                    const char* capture_name =
                        ts_query_capture_name_for_id(query, capture.index, &name_len);
                    if (capture_name && std::string_view(capture_name) == "callee") {
                        std::string callee_name = ctx.extractNodeText(capture.node);
                        if (!callee_name.empty() && callee_name != caller_name &&
                            symbol_map.count(callee_name)) {
                            call_sites[caller_name].insert(callee_name);
                        }
                    }
                }
            }
        }
    }

    // Convert to relations
    for (const auto& [caller, callees] : call_sites) {
        for (const auto& callee : callees) {
            SymbolRelation rel;
            rel.src_symbol = caller;
            rel.dst_symbol = callee;
            rel.kind = "calls";
            rel.weight = 1.0;
            result.relations.push_back(std::move(rel));
        }
    }

    return result;
}

} // namespace yams::plugins::treesitter