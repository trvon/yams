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
#include <spdlog/spdlog.h>

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
        return tl::unexpected(std::format("No tree-sitter language loaded for {} (at {}:{})",
                                          language, loc.file_name(), loc.line()));
    }

    if (content.empty()) {
        return tl::unexpected("Empty content provided");
    }

    // Defensive: verify parser language set successfully
    if (ts_parser_language(parser_.get()) == nullptr) {
        return tl::unexpected("Parser has no language set");
    }

    ExtractionResult result;
    ExtractionContext ctx{.content = content,
                          .language = language,
                          .file_path = file_path,
                          .enable_call_graph = enable_call_graph};

    // Parse the content (guard against null pointers)
    if (!parser_.get() || content.data() == nullptr) {
        return tl::unexpected("Invalid parser or content");
    }
    // Use ts_parser_set_language already set in ctor. Extra guard around parse.
    ctx.tree =
        ts_parser_parse_string(parser_.get(), nullptr, content.data(), (uint32_t)content.length());
    if (!ctx.tree) {
        return tl::unexpected("Failed to parse content");
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

        // Extract include/import relationships
        auto include_result = extractIncludes(ctx);
        if (include_result) {
            std::ranges::move(include_result->relations, std::back_inserter(result.relations));
        }

        // Extract inheritance relationships
        auto inherit_result = extractInheritance(ctx, result.symbols);
        if (inherit_result) {
            std::ranges::move(inherit_result->relations, std::back_inserter(result.relations));
        }

        result.calculate_stats();

    } catch (const std::exception& e) {
        if (ctx.tree)
            ts_tree_delete(ctx.tree);
        return tl::unexpected(std::format("Extraction exception: {}", e.what()));
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
            // Method definitions with qualified names: class::method() { ... }
            // This also captures constructors like MyClass::MyClass()
            "(function_definition declarator: (function_declarator declarator: "
            "(qualified_identifier name: (identifier) @name)))",
            // Method definitions with field_identifier: void method() { ... }
            "(function_definition declarator: (function_declarator declarator: (field_identifier) "
            "@name))",
            // Method declarations inside class bodies: void method();
            // Also captures constructor declarations: MyClass();
            "(field_declaration declarator: (function_declarator declarator: (field_identifier) "
            "@name))",
            // Constructor declarations inside class (alternative pattern)
            "(field_declaration declarator: (function_declarator declarator: (identifier) @name))",
            // Function declarations in headers: int foo();
            "(declaration declarator: (function_declarator declarator: (identifier) @name))",
            // Destructor definitions: ~Foo() { ... }
            "(function_definition declarator: (function_declarator declarator: (destructor_name) "
            "@name))",
            // Destructor declarations inside class
            "(field_declaration declarator: (function_declarator declarator: (destructor_name) "
            "@name))",
            // Operator overload definitions: operator==, operator+, etc.
            "(function_definition declarator: (function_declarator declarator: (operator_name) "
            "@name))",
            // Operator overload declarations
            "(field_declaration declarator: (function_declarator declarator: (operator_name) "
            "@name))",
            // Template function definitions: template<typename T> void foo() { ... }
            "(template_declaration (function_definition declarator: (function_declarator "
            "declarator: (identifier) @name)))",
            // Template method definitions
            "(template_declaration (function_definition declarator: (function_declarator "
            "declarator: (qualified_identifier name: (identifier) @name))))",
            // Template method declarations in class
            "(template_declaration (field_declaration declarator: (function_declarator declarator: "
            "(field_identifier) @name)))",
        };
    } else if (language == "python") {
        queries = {
            // Function definitions
            "(function_definition name: (identifier) @name)",
            // Async function definitions (same AST node type)
            // Decorated functions
            "(decorated_definition (function_definition name: (identifier) @name))",
            // Class methods (also function_definition)
            // Lambda expressions are unnamed, skip
        };
    } else if (language == "rust") {
        queries = {
            // Function items (standalone functions)
            "(function_item name: (identifier) @name)",
            // Associated functions (in impl blocks)
            "(function_signature_item name: (identifier) @name)",
            // Methods in impl blocks
            "(impl_item (function_item name: (identifier) @name))",
            // Trait methods
            "(trait_item (function_signature_item name: (identifier) @name))",
        };
    } else if (language == "go") {
        queries = {
            // Function declarations
            "(function_declaration name: (identifier) @name)",
            // Method declarations with receiver
            "(method_declaration name: (field_identifier) @name)",
        };
    } else if (language == "javascript" || language == "js" || language == "typescript" ||
               language == "ts") {
        queries = {
            // Function declarations: function foo() {}
            "(function_declaration name: (identifier) @name)",
            // Method definitions in classes: class { method() {} }
            "(method_definition name: (property_identifier) @name)",
            // Arrow functions assigned to variables: const foo = () => {}
            "(variable_declarator name: (identifier) @name value: (arrow_function))",
            // Function expressions: const foo = function() {}
            "(variable_declarator name: (identifier) @name value: (function))",
            // Generator functions: function* foo() {}
            "(generator_function_declaration name: (identifier) @name)",
            // Async functions: async function foo() {}
            "(function_declaration name: (identifier) @name)",
        };
    } else if (language == "java") {
        queries = {
            // Method declarations
            "(method_declaration name: (identifier) @name)",
            // Constructor declarations
            "(constructor_declaration name: (identifier) @name)",
        };
    } else if (language == "csharp" || language == "cs") {
        queries = {
            // Method declarations
            "(method_declaration name: (identifier) @name)",
            // Constructor declarations
            "(constructor_declaration name: (identifier) @name)",
            // Destructor declarations
            "(destructor_declaration name: (identifier) @name)",
            // Property declarations
            "(property_declaration name: (identifier) @name)",
        };
    } else if (language == "php") {
        queries = {
            // Function definitions
            "(function_definition name: (name) @name)",
            // Method declarations
            "(method_declaration name: (name) @name)",
        };
    } else if (language == "kotlin" || language == "kt") {
        queries = {
            // Function declarations: fun foo() {}
            "(function_declaration (simple_identifier) @name)",
            // Secondary/companion object functions
            "(property_declaration (variable_declaration (simple_identifier) @name))",
        };
    } else if (language == "perl" || language == "pl") {
        queries = {
            // Subroutine definitions
            "(subroutine_declaration_statement name: (identifier) @name)",
        };
    } else if (language == "r") {
        queries = {
            // Function definitions with <-
            "(binary_operator lhs: (identifier) @name operator: \"<-\")",
            // Function definitions with =
            "(binary_operator lhs: (identifier) @name operator: \"=\")",
        };
    } else if (language == "sql") {
        queries = {
            // CREATE FUNCTION
            "(create_function_statement name: (identifier) @name)",
            // CREATE PROCEDURE
            "(create_procedure_statement name: (identifier) @name)",
        };
    } else if (language == "sol" || language == "solidity") {
        queries = {
            // Regular functions
            "(function_definition name: (identifier) @name)",
            // Constructors (may have optional name)
            "(constructor_definition)",
            // Fallback/receive functions
            "(fallback_receive_definition)",
            // Modifiers
            "(modifier_definition name: (identifier) @name)",
        };
    } else if (language == "dart" || language == "flutter") {
        queries = {
            // Function declarations: void foo() {}
            "(function_signature name: (identifier) @name)",
            // Method declarations in classes
            "(method_signature name: (identifier) @name)",
            // Getter declarations: get foo => ...
            "(getter_signature name: (identifier) @name)",
            // Setter declarations: set foo(x) => ...
            "(setter_signature name: (identifier) @name)",
            // Function expressions assigned to variables: final foo = () {};
            "(initialized_variable_definition name: (identifier) @name)",
        };
    }

    // Execute queries - try all patterns to get comprehensive coverage
    bool any_query_succeeded = false;
    for (auto query : queries) {
        if (executeQuery(ctx, query, "function", result.symbols)) {
            any_query_succeeded = true;
            // Continue trying other queries to capture all symbol types
        }
    }

    // If any query succeeded, return accumulated results
    if (any_query_succeeded) {
        return result;
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

    if (language == "cpp" || language == "c++" || language == "c") {
        queries = {
            // Class specifiers
            "(class_specifier name: (type_identifier) @name)",
            // Struct specifiers
            "(struct_specifier name: (type_identifier) @name)",
            // Union specifiers
            "(union_specifier name: (type_identifier) @name)",
            // Enum specifiers
            "(enum_specifier name: (type_identifier) @name)",
            // Template class declarations
            "(template_declaration (class_specifier name: (type_identifier) @name))",
            // Template struct declarations
            "(template_declaration (struct_specifier name: (type_identifier) @name))",
        };
    } else if (language == "python") {
        queries = {
            // Class definitions
            "(class_definition name: (identifier) @name)",
            // Decorated class definitions
            "(decorated_definition (class_definition name: (identifier) @name))",
        };
    } else if (language == "rust") {
        queries = {
            // Struct items
            "(struct_item name: (type_identifier) @name)",
            // Enum items
            "(enum_item name: (type_identifier) @name)",
            // Trait items
            "(trait_item name: (type_identifier) @name)",
        };
    } else if (language == "go") {
        queries = {
            // Type declarations (structs, interfaces)
            "(type_declaration (type_spec name: (type_identifier) @name))",
        };
    } else if (language == "java") {
        queries = {
            // Class declarations
            "(class_declaration name: (identifier) @name)",
            // Interface declarations
            "(interface_declaration name: (identifier) @name)",
            // Enum declarations
            "(enum_declaration name: (identifier) @name)",
            // Annotation type declarations
            "(annotation_type_declaration name: (identifier) @name)",
        };
    } else if (language == "javascript" || language == "js" || language == "typescript" ||
               language == "ts") {
        queries = {
            // Class declarations
            "(class_declaration name: (identifier) @name)",
            // Class expressions
            "(class name: (identifier) @name)",
        };
    } else if (language == "typescript" || language == "ts") {
        queries = {
            // Interface declarations
            "(interface_declaration name: (type_identifier) @name)",
            // Type alias declarations
            "(type_alias_declaration name: (type_identifier) @name)",
            // Enum declarations
            "(enum_declaration name: (identifier) @name)",
        };
    } else if (language == "csharp" || language == "cs") {
        queries = {
            // Class declarations
            "(class_declaration name: (identifier) @name)",
            // Struct declarations
            "(struct_declaration name: (identifier) @name)",
            // Interface declarations
            "(interface_declaration name: (identifier) @name)",
            // Enum declarations
            "(enum_declaration name: (identifier) @name)",
            // Record declarations
            "(record_declaration name: (identifier) @name)",
        };
    } else if (language == "php") {
        queries = {
            // Class declarations
            "(class_declaration name: (name) @name)",
            // Interface declarations
            "(interface_declaration name: (name) @name)",
            // Trait declarations
            "(trait_declaration name: (name) @name)",
            // Enum declarations (PHP 8.1+)
            "(enum_declaration name: (name) @name)",
        };
    } else if (language == "kotlin" || language == "kt") {
        queries = {
            // Class declarations
            "(class_declaration (type_identifier) @name)",
            // Object declarations
            "(object_declaration (type_identifier) @name)",
            // Interface declarations
            "(interface_declaration (type_identifier) @name)",
        };
    } else if (language == "sol" || language == "solidity") {
        queries = {
            // Contract declarations
            "(contract_declaration name: (identifier) @name)",
            // Interface declarations
            "(interface_declaration name: (identifier) @name)",
            // Library declarations
            "(library_declaration name: (identifier) @name)",
            // Struct declarations
            "(struct_declaration name: (identifier) @name)",
            // Enum declarations
            "(enum_declaration name: (identifier) @name)",
            // Event declarations
            "(event_definition name: (identifier) @name)",
        };
    } else if (language == "dart" || language == "flutter") {
        queries = {
            // Class declarations: class Foo {}
            "(class_definition name: (identifier) @name)",
            // Mixin declarations: mixin Bar {}
            "(mixin_declaration name: (identifier) @name)",
            // Extension declarations: extension FooExt on Foo {}
            "(extension_declaration name: (identifier) @name)",
            // Enum declarations: enum Color { red, green }
            "(enum_declaration name: (identifier) @name)",
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
                    // Look for the actual definition/declaration node, not intermediate nodes
                    TSNode parent_node = ts_node_parent(capture.node);
                    TSNode best_match = {};
                    while (!ts_node_is_null(parent_node)) {
                        const char* node_type = ts_node_type(parent_node);
                        // For functions, we want function_definition, not function_declarator
                        // For classes/structs, we want class_specifier/struct_specifier
                        bool is_definition =
                            (symbol_kind == "function" &&
                             (std::strcmp(node_type, "function_definition") == 0 ||
                              std::strcmp(node_type, "template_declaration") == 0)) ||
                            (symbol_kind == "class" &&
                             (std::strcmp(node_type, "class_specifier") == 0 ||
                              std::strcmp(node_type, "template_declaration") == 0)) ||
                            (symbol_kind == "struct" &&
                             std::strcmp(node_type, "struct_specifier") == 0);

                        if (is_definition) {
                            best_match = parent_node;
                            // Keep going up in case there's a template wrapper
                            if (std::strcmp(node_type, "template_declaration") != 0) {
                                // Check if there's a template_declaration above
                                TSNode grandparent = ts_node_parent(parent_node);
                                if (!ts_node_is_null(grandparent) &&
                                    std::strcmp(ts_node_type(grandparent), "template_declaration") ==
                                        0) {
                                    best_match = grandparent;
                                }
                            }
                            break;
                        }
                        parent_node = ts_node_parent(parent_node);
                    }

                    if (!ts_node_is_null(best_match)) {
                        TSPoint parent_start = ts_node_start_point(best_match);
                        TSPoint parent_end = ts_node_end_point(best_match);
                        sym.start_line = parent_start.row + 1;
                        sym.end_line = parent_end.row + 1;
                        sym.start_offset = ts_node_start_byte(best_match);
                        sym.end_offset = ts_node_end_byte(best_match);
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

    spdlog::info("[CallExtraction] Starting for language={}, symbol_count={}", ctx.language,
                 symbols.size());

    // Collect function symbols with valid byte ranges
    // We use the already-extracted symbols instead of re-querying
    std::vector<const SymbolInfo*> function_symbols;
    for (const auto& sym : symbols) {
        if (sym.kind == "function" && sym.start_offset < sym.end_offset) {
            function_symbols.push_back(&sym);
        }
    }
    spdlog::info("[CallExtraction] Found {} function symbols to analyze", function_symbols.size());

    if (function_symbols.empty()) {
        return result;
    }

    // Query patterns for call expressions - language specific
    std::vector<std::string_view> call_patterns;
    std::string_view lang = ctx.language;

    if (lang == "cpp" || lang == "c++" || lang == "c") {
        call_patterns = {
            // Direct function calls: foo()
            "(call_expression function: (identifier) @callee) @call",
            // Method calls: obj.method()
            "(call_expression function: (field_expression field: (field_identifier) @callee)) @call",
            // Qualified calls: Class::method()
            "(call_expression function: (qualified_identifier name: (identifier) @callee)) @call",
        };
    } else if (lang == "python") {
        call_patterns = {
            "(call function: (identifier) @callee) @call",
            "(call function: (attribute attribute: (identifier) @callee)) @call",
        };
    } else if (lang == "rust") {
        call_patterns = {
            "(call_expression function: (identifier) @callee) @call",
            "(call_expression function: (field_expression field: (field_identifier) @callee)) @call",
        };
    } else if (lang == "go") {
        call_patterns = {
            "(call_expression function: (identifier) @callee) @call",
            "(call_expression function: (selector_expression field: (field_identifier) @callee)) "
            "@call",
        };
    } else if (lang == "javascript" || lang == "js" || lang == "typescript" || lang == "ts") {
        call_patterns = {
            "(call_expression function: (identifier) @callee) @call",
            "(call_expression function: (member_expression property: (property_identifier) "
            "@callee)) @call",
        };
    } else if (lang == "java") {
        call_patterns = {
            "(method_invocation name: (identifier) @callee) @call",
        };
    } else if (lang == "sol" || lang == "solidity") {
        call_patterns = {
            "(call_expression function: (identifier) @callee) @call",
            "(call_expression function: (member_expression property: (identifier) @callee)) @call",
        };
    }

    if (call_patterns.empty()) {
        spdlog::info("[CallExtraction] No call patterns for language={}", ctx.language);
        return result;
    }

    // Find ALL call expressions in the entire file first
    // We'll match them to containing functions by byte range
    struct CallSite {
        std::string callee_name;
        uint32_t byte_offset;
    };
    std::vector<CallSite> all_calls;

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

        // Execute on the entire tree root
        ts_query_cursor_exec(cursor, query, ctx.root);

        TSQueryMatch match;
        while (ts_query_cursor_next_match(cursor, &match)) {
            std::string callee_name;
            uint32_t call_byte = 0;

            for (uint32_t i = 0; i < match.capture_count; ++i) {
                TSQueryCapture capture = match.captures[i];
                uint32_t name_len = 0;
                const char* capture_name =
                    ts_query_capture_name_for_id(query, capture.index, &name_len);
                if (capture_name) {
                    std::string_view cap_name(capture_name, name_len);
                    if (cap_name == "callee") {
                        callee_name = ctx.extractNodeText(capture.node);
                    } else if (cap_name == "call") {
                        call_byte = ts_node_start_byte(capture.node);
                    }
                }
            }

            if (!callee_name.empty()) {
                all_calls.push_back({std::move(callee_name), call_byte});
            }
        }
    }

    spdlog::info("[CallExtraction] Found {} total call expressions in file", all_calls.size());

    // Now match calls to their containing functions by byte range
    std::unordered_map<std::string /*caller*/, std::set<std::string /*callee*/>> call_sites;

    for (const auto& call : all_calls) {
        // Find which function contains this call
        for (const auto* func : function_symbols) {
            if (call.byte_offset >= func->start_offset && call.byte_offset < func->end_offset) {
                // This call is inside this function
                // Skip self-recursive calls
                if (call.callee_name != func->name) {
                    call_sites[func->name].insert(call.callee_name);
                }
                break; // A call can only be in one function
            }
        }
    }

    spdlog::info("[CallExtraction] Matched calls to {} unique caller functions", call_sites.size());

    // Log some sample calls for debugging
    size_t logged = 0;
    for (const auto& [caller, callees] : call_sites) {
        if (logged++ < 5) {
            spdlog::info("[CallExtraction] {} calls: {}", caller,
                         [&]() {
                             std::string s;
                             size_t count = 0;
                             for (const auto& c : callees) {
                                 if (count++ > 0)
                                     s += ", ";
                                 s += c;
                                 if (count >= 5) {
                                     s += "...";
                                     break;
                                 }
                             }
                             return s;
                         }());
        }
    }

    // Convert to relations - include ALL calls, not just to known symbols
    // This allows cross-file relationships to be resolved later
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

    spdlog::info("[CallExtraction] Extracted {} call relations", result.relations.size());
    return result;
}

SymbolExtractor::Result SymbolExtractor::extractIncludes(const ExtractionContext& ctx) {
    ExtractionResult result;

    std::string_view lang = ctx.language;
    std::vector<std::pair<std::string_view, std::string_view>> patterns; // {query, capture_name}

    if (lang == "cpp" || lang == "c++" || lang == "c") {
        // C/C++ #include directives
        patterns = {
            // #include "header.h" or #include <header>
            {"(preproc_include path: (string_literal) @path)", "path"},
            {"(preproc_include path: (system_lib_string) @path)", "path"},
        };
    } else if (lang == "python") {
        // Python import statements
        patterns = {
            // import foo
            {"(import_statement name: (dotted_name) @module)", "module"},
            // from foo import bar
            {"(import_from_statement module_name: (dotted_name) @module)", "module"},
            // from . import bar (relative)
            {"(import_from_statement module_name: (relative_import) @module)", "module"},
        };
    } else if (lang == "javascript" || lang == "js" || lang == "typescript" || lang == "ts") {
        // ES6 import statements
        patterns = {
            // import foo from "module"
            {"(import_statement source: (string) @source)", "source"},
            // import("module") dynamic imports
            {"(call_expression function: (import) arguments: (arguments (string) @source))",
             "source"},
            // require("module")
            {"(call_expression function: (identifier) @fn arguments: (arguments (string) @source))",
             "source"},
        };
    } else if (lang == "rust") {
        // Rust use statements
        patterns = {
            // use std::collections::HashMap;
            {"(use_declaration argument: (scoped_identifier) @path)", "path"},
            {"(use_declaration argument: (identifier) @path)", "path"},
            {"(use_declaration argument: (use_wildcard) @path)", "path"},
            // extern crate foo;
            {"(extern_crate_declaration name: (identifier) @path)", "path"},
        };
    } else if (lang == "go") {
        // Go import statements
        patterns = {
            // import "fmt"
            {"(import_declaration (import_spec path: (interpreted_string_literal) @path))", "path"},
            // import ( "fmt" "os" )
            {"(import_spec path: (interpreted_string_literal) @path)", "path"},
        };
    } else if (lang == "java") {
        // Java import statements
        patterns = {
            // import java.util.HashMap;
            {"(import_declaration (scoped_identifier) @path)", "path"},
            // import static java.util.Collections.sort;
            {"(import_declaration (scoped_identifier) @path)", "path"},
        };
    } else if (lang == "csharp" || lang == "cs") {
        // C# using statements
        patterns = {
            // using System.Collections.Generic;
            {"(using_directive (identifier) @path)", "path"},
            {"(using_directive (qualified_name) @path)", "path"},
        };
    } else if (lang == "php") {
        // PHP use statements
        patterns = {
            // use Namespace\ClassName;
            {"(namespace_use_clause (qualified_name) @path)", "path"},
            // require/include
            {"(include_expression (string) @path)", "path"},
            {"(require_expression (string) @path)", "path"},
        };
    } else if (lang == "kotlin" || lang == "kt") {
        // Kotlin import statements
        patterns = {
            // import kotlin.collections.HashMap
            {"(import_header (identifier) @path)", "path"},
        };
    } else if (lang == "sol" || lang == "solidity") {
        // Solidity import statements
        patterns = {
            // import "./Contract.sol";
            {"(import_directive source: (string) @path)", "path"},
        };
    } else if (lang == "dart" || lang == "flutter") {
        // Dart import statements
        patterns = {
            // import 'package:foo/foo.dart';
            {"(import_or_export (string_literal) @path)", "path"},
        };
    }

    if (patterns.empty()) {
        return result;
    }

    std::string file_path_str = std::string(ctx.file_path);
    size_t extracted_count = 0;

    for (const auto& [pattern, capture_name] : patterns) {
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

        ts_query_cursor_exec(cursor, query, ctx.root);

        TSQueryMatch match;
        while (ts_query_cursor_next_match(cursor, &match)) {
            for (uint32_t i = 0; i < match.capture_count; ++i) {
                TSQueryCapture capture = match.captures[i];
                uint32_t name_len = 0;
                const char* captured_name =
                    ts_query_capture_name_for_id(query, capture.index, &name_len);

                if (captured_name && std::string_view(captured_name, name_len) == capture_name) {
                    std::string include_path = ctx.extractNodeText(capture.node);

                    // Clean up quotes from string literals
                    if (!include_path.empty() &&
                        (include_path.front() == '"' || include_path.front() == '\'' ||
                         include_path.front() == '<')) {
                        include_path = include_path.substr(1);
                    }
                    if (!include_path.empty() &&
                        (include_path.back() == '"' || include_path.back() == '\'' ||
                         include_path.back() == '>')) {
                        include_path.pop_back();
                    }

                    if (!include_path.empty()) {
                        SymbolRelation rel;
                        rel.src_symbol = file_path_str;
                        rel.dst_symbol = include_path;
                        rel.kind = "includes";
                        rel.weight = 1.0;
                        result.relations.push_back(std::move(rel));
                        ++extracted_count;
                    }
                }
            }
        }
    }

    if (extracted_count > 0) {
        spdlog::info("[IncludeExtraction] Extracted {} include/import relations from {}",
                     extracted_count, ctx.file_path);
    }

    return result;
}

SymbolExtractor::Result
SymbolExtractor::extractInheritance(const ExtractionContext& ctx,
                                    const std::vector<SymbolInfo>& symbols) {
    ExtractionResult result;

    std::string_view lang = ctx.language;

    // Build a map of class/struct symbols for matching
    std::unordered_map<std::string, const SymbolInfo*> class_map;
    for (const auto& sym : symbols) {
        if (sym.kind == "class" || sym.kind == "struct" || sym.kind == "interface") {
            class_map[sym.name] = &sym;
        }
    }

    if (class_map.empty()) {
        return result;
    }

    // Query patterns for inheritance - language specific
    // The strategy: find class definitions with base classes and extract the relationship
    if (lang == "cpp" || lang == "c++" || lang == "c") {
        // C++ inheritance: class Derived : public Base { ... }
        // We need to traverse the AST to find base_class_clause nodes

        std::function<void(TSNode)> traverse;
        traverse = [&](TSNode node) {
            if (ts_node_is_null(node))
                return;

            const char* node_type = ts_node_type(node);

            // Look for class_specifier or struct_specifier
            if (std::strcmp(node_type, "class_specifier") == 0 ||
                std::strcmp(node_type, "struct_specifier") == 0) {
                // Find the class name
                std::string class_name;
                TSNode name_node = ts_node_child_by_field_name(node, "name", 4);
                if (!ts_node_is_null(name_node)) {
                    class_name = ctx.extractNodeText(name_node);
                }

                // Look for base_class_clause child
                uint32_t child_count = ts_node_child_count(node);
                for (uint32_t i = 0; i < child_count; ++i) {
                    TSNode child = ts_node_child(node, i);
                    if (std::strcmp(ts_node_type(child), "base_class_clause") == 0) {
                        // Find all base_class_clause children (base types)
                        uint32_t base_count = ts_node_child_count(child);
                        for (uint32_t j = 0; j < base_count; ++j) {
                            TSNode base_spec = ts_node_child(child, j);
                            const char* base_type = ts_node_type(base_spec);

                            // Look for type_identifier in the base specifier
                            std::function<std::string(TSNode)> find_base_type;
                            find_base_type = [&ctx, &find_base_type](TSNode n) -> std::string {
                                if (ts_node_is_null(n))
                                    return "";

                                const char* type = ts_node_type(n);
                                if (std::strcmp(type, "type_identifier") == 0 ||
                                    std::strcmp(type, "qualified_identifier") == 0) {
                                    return ctx.extractNodeText(n);
                                }

                                uint32_t cc = ts_node_child_count(n);
                                for (uint32_t k = 0; k < cc; ++k) {
                                    std::string found = find_base_type(ts_node_child(n, k));
                                    if (!found.empty())
                                        return found;
                                }
                                return "";
                            };

                            std::string base_name = find_base_type(base_spec);
                            if (!base_name.empty() && !class_name.empty() &&
                                base_name != class_name) {
                                SymbolRelation rel;
                                rel.src_symbol = class_name;
                                rel.dst_symbol = base_name;
                                rel.kind = "inherits";
                                rel.weight = 1.0;
                                result.relations.push_back(std::move(rel));
                            }
                        }
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
    } else if (lang == "python") {
        // Python: class Foo(Bar, Baz): ...
        std::function<void(TSNode)> traverse;
        traverse = [&](TSNode node) {
            if (ts_node_is_null(node))
                return;

            if (std::strcmp(ts_node_type(node), "class_definition") == 0) {
                // Get class name
                std::string class_name;
                TSNode name_node = ts_node_child_by_field_name(node, "name", 4);
                if (!ts_node_is_null(name_node)) {
                    class_name = ctx.extractNodeText(name_node);
                }

                // Get superclasses from argument_list
                TSNode superclasses = ts_node_child_by_field_name(node, "superclasses", 12);
                if (!ts_node_is_null(superclasses)) {
                    uint32_t count = ts_node_child_count(superclasses);
                    for (uint32_t i = 0; i < count; ++i) {
                        TSNode child = ts_node_child(superclasses, i);
                        const char* type = ts_node_type(child);
                        // Look for identifier or attribute
                        if (std::strcmp(type, "identifier") == 0 ||
                            std::strcmp(type, "attribute") == 0) {
                            std::string base_name = ctx.extractNodeText(child);
                            if (!base_name.empty() && !class_name.empty() &&
                                base_name != class_name) {
                                SymbolRelation rel;
                                rel.src_symbol = class_name;
                                rel.dst_symbol = base_name;
                                rel.kind = "inherits";
                                rel.weight = 1.0;
                                result.relations.push_back(std::move(rel));
                            }
                        }
                    }
                }
            }

            uint32_t child_count = ts_node_child_count(node);
            for (uint32_t i = 0; i < child_count; ++i) {
                traverse(ts_node_child(node, i));
            }
        };

        traverse(ctx.root);
    } else if (lang == "java") {
        // Java: class Foo extends Bar implements Baz
        std::function<void(TSNode)> traverse;
        traverse = [&](TSNode node) {
            if (ts_node_is_null(node))
                return;

            if (std::strcmp(ts_node_type(node), "class_declaration") == 0) {
                std::string class_name;
                TSNode name_node = ts_node_child_by_field_name(node, "name", 4);
                if (!ts_node_is_null(name_node)) {
                    class_name = ctx.extractNodeText(name_node);
                }

                // Find superclass
                TSNode superclass = ts_node_child_by_field_name(node, "superclass", 10);
                if (!ts_node_is_null(superclass)) {
                    std::string base_name = ctx.extractNodeText(superclass);
                    if (!base_name.empty() && !class_name.empty()) {
                        SymbolRelation rel;
                        rel.src_symbol = class_name;
                        rel.dst_symbol = base_name;
                        rel.kind = "inherits";
                        rel.weight = 1.0;
                        result.relations.push_back(std::move(rel));
                    }
                }

                // Find interfaces
                TSNode interfaces = ts_node_child_by_field_name(node, "interfaces", 10);
                if (!ts_node_is_null(interfaces)) {
                    uint32_t count = ts_node_child_count(interfaces);
                    for (uint32_t i = 0; i < count; ++i) {
                        TSNode child = ts_node_child(interfaces, i);
                        if (std::strcmp(ts_node_type(child), "type_identifier") == 0 ||
                            std::strcmp(ts_node_type(child), "generic_type") == 0) {
                            std::string iface_name = ctx.extractNodeText(child);
                            if (!iface_name.empty() && !class_name.empty()) {
                                SymbolRelation rel;
                                rel.src_symbol = class_name;
                                rel.dst_symbol = iface_name;
                                rel.kind = "implements";
                                rel.weight = 1.0;
                                result.relations.push_back(std::move(rel));
                            }
                        }
                    }
                }
            }

            uint32_t child_count = ts_node_child_count(node);
            for (uint32_t i = 0; i < child_count; ++i) {
                traverse(ts_node_child(node, i));
            }
        };

        traverse(ctx.root);
    } else if (lang == "javascript" || lang == "js" || lang == "typescript" || lang == "ts") {
        // JS/TS: class Foo extends Bar { ... }
        std::function<void(TSNode)> traverse;
        traverse = [&](TSNode node) {
            if (ts_node_is_null(node))
                return;

            if (std::strcmp(ts_node_type(node), "class_declaration") == 0 ||
                std::strcmp(ts_node_type(node), "class") == 0) {
                std::string class_name;
                TSNode name_node = ts_node_child_by_field_name(node, "name", 4);
                if (!ts_node_is_null(name_node)) {
                    class_name = ctx.extractNodeText(name_node);
                }

                // Find class_heritage for 'extends'
                uint32_t child_count = ts_node_child_count(node);
                for (uint32_t i = 0; i < child_count; ++i) {
                    TSNode child = ts_node_child(node, i);
                    if (std::strcmp(ts_node_type(child), "class_heritage") == 0) {
                        // Look for extends_clause
                        uint32_t hcount = ts_node_child_count(child);
                        for (uint32_t j = 0; j < hcount; ++j) {
                            TSNode hchild = ts_node_child(child, j);
                            if (std::strcmp(ts_node_type(hchild), "extends_clause") == 0) {
                                // Get the base class identifier
                                uint32_t ecount = ts_node_child_count(hchild);
                                for (uint32_t k = 0; k < ecount; ++k) {
                                    TSNode echild = ts_node_child(hchild, k);
                                    if (std::strcmp(ts_node_type(echild), "identifier") == 0) {
                                        std::string base_name = ctx.extractNodeText(echild);
                                        if (!base_name.empty() && !class_name.empty()) {
                                            SymbolRelation rel;
                                            rel.src_symbol = class_name;
                                            rel.dst_symbol = base_name;
                                            rel.kind = "inherits";
                                            rel.weight = 1.0;
                                            result.relations.push_back(std::move(rel));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            uint32_t child_count = ts_node_child_count(node);
            for (uint32_t i = 0; i < child_count; ++i) {
                traverse(ts_node_child(node, i));
            }
        };

        traverse(ctx.root);
    } else if (lang == "rust") {
        // Rust: impl Trait for Struct
        // Rust doesn't have traditional inheritance, but we can extract trait implementations
        std::function<void(TSNode)> traverse;
        traverse = [&](TSNode node) {
            if (ts_node_is_null(node))
                return;

            if (std::strcmp(ts_node_type(node), "impl_item") == 0) {
                // Look for "impl Trait for Type" pattern
                std::string trait_name;
                std::string type_name;

                TSNode trait_node = ts_node_child_by_field_name(node, "trait", 5);
                TSNode type_node = ts_node_child_by_field_name(node, "type", 4);

                if (!ts_node_is_null(trait_node)) {
                    trait_name = ctx.extractNodeText(trait_node);
                }
                if (!ts_node_is_null(type_node)) {
                    type_name = ctx.extractNodeText(type_node);
                }

                if (!trait_name.empty() && !type_name.empty()) {
                    SymbolRelation rel;
                    rel.src_symbol = type_name;
                    rel.dst_symbol = trait_name;
                    rel.kind = "implements";
                    rel.weight = 1.0;
                    result.relations.push_back(std::move(rel));
                }
            }

            uint32_t child_count = ts_node_child_count(node);
            for (uint32_t i = 0; i < child_count; ++i) {
                traverse(ts_node_child(node, i));
            }
        };

        traverse(ctx.root);
    } else if (lang == "go") {
        // Go uses implicit interface implementation, can't easily extract from AST
        // Struct embedding could be extracted but is less common
    } else if (lang == "sol" || lang == "solidity") {
        // Solidity: contract Foo is Bar, Baz { ... }
        std::function<void(TSNode)> traverse;
        traverse = [&](TSNode node) {
            if (ts_node_is_null(node))
                return;

            if (std::strcmp(ts_node_type(node), "contract_declaration") == 0) {
                std::string class_name;
                TSNode name_node = ts_node_child_by_field_name(node, "name", 4);
                if (!ts_node_is_null(name_node)) {
                    class_name = ctx.extractNodeText(name_node);
                }

                // Find inheritance_specifier
                uint32_t child_count = ts_node_child_count(node);
                for (uint32_t i = 0; i < child_count; ++i) {
                    TSNode child = ts_node_child(node, i);
                    if (std::strcmp(ts_node_type(child), "class_heritage") == 0 ||
                        std::strcmp(ts_node_type(child), "inheritance_specifier") == 0) {
                        uint32_t hcount = ts_node_child_count(child);
                        for (uint32_t j = 0; j < hcount; ++j) {
                            TSNode hchild = ts_node_child(child, j);
                            if (std::strcmp(ts_node_type(hchild), "identifier") == 0 ||
                                std::strcmp(ts_node_type(hchild), "user_defined_type") == 0) {
                                std::string base_name = ctx.extractNodeText(hchild);
                                if (!base_name.empty() && !class_name.empty()) {
                                    SymbolRelation rel;
                                    rel.src_symbol = class_name;
                                    rel.dst_symbol = base_name;
                                    rel.kind = "inherits";
                                    rel.weight = 1.0;
                                    result.relations.push_back(std::move(rel));
                                }
                            }
                        }
                    }
                }
            }

            uint32_t child_count = ts_node_child_count(node);
            for (uint32_t i = 0; i < child_count; ++i) {
                traverse(ts_node_child(node, i));
            }
        };

        traverse(ctx.root);
    }

    if (!result.relations.empty()) {
        spdlog::info("[InheritanceExtraction] Extracted {} inheritance relations from {}",
                     result.relations.size(), ctx.file_path);
    }

    return result;
}

} // namespace yams::plugins::treesitter