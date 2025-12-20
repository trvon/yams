#include "symbol_extractor.h"
#include "grammar_loader.h"

#include <algorithm>
#include <array>
#include <expected>
#include <format>
#include <ranges>
#include <set>
#include <span>
#include <unordered_map>
#include <vector>

#include <spdlog/spdlog.h>
#include <cstring>
#include <functional>

extern "C" {
#include <tree_sitter/api.h>
}

namespace yams::plugins::treesitter {

// ============================================================================
// Language Configuration - Constexpr Node Type Tables
// ============================================================================

// Maximum sizes for constexpr arrays
inline constexpr size_t MaxNodeTypes = 8;
inline constexpr size_t MaxQueries = 16;

// Fixed-size array wrapper for constexpr compatibility
template <size_t N> struct ConstList {
    std::array<std::string_view, N> items{};
    size_t count = 0;

    constexpr bool contains(std::string_view item) const noexcept {
        for (size_t i = 0; i < count; ++i) {
            if (items[i] == item)
                return true;
        }
        return false;
    }

    constexpr bool matches(const char* cstr) const noexcept {
        if (!cstr)
            return false;
        return contains(std::string_view(cstr));
    }

    constexpr std::string_view operator[](size_t i) const noexcept {
        return i < count ? items[i] : std::string_view{};
    }

    constexpr auto begin() const noexcept { return items.begin(); }
    constexpr auto end() const noexcept { return items.begin() + count; }
};

// Type aliases for clarity
using NodeTypeList = ConstList<MaxNodeTypes>;
using QueryList = ConstList<MaxQueries>;

// Language configuration with all extraction settings
struct LanguageConfig {
    std::string_view name;
    std::array<std::string_view, 4> aliases;
    size_t alias_count = 0;

    // Node types for AST traversal fallback
    NodeTypeList class_types;
    NodeTypeList field_types;
    NodeTypeList function_types;
    NodeTypeList import_types;
    NodeTypeList identifier_types;

    // Tree-sitter query patterns
    QueryList function_queries;
    QueryList class_queries;
    QueryList import_queries;
    QueryList call_queries;
};

// Helper to create lists at compile time
template <size_t N, typename... Args> constexpr auto makeList(Args... args) -> ConstList<N> {
    ConstList<N> result;
    result.count = sizeof...(args);
    size_t i = 0;
    ((result.items[i++] = args), ...);
    return result;
}

// Convenience wrappers
template <typename... Args> constexpr auto makeNodeTypes(Args... args) {
    return makeList<MaxNodeTypes>(args...);
}

template <typename... Args> constexpr auto makeQueries(Args... args) {
    return makeList<MaxQueries>(args...);
}

// ============================================================================
// Per-Language Configurations
// ============================================================================

inline constexpr LanguageConfig lang_cpp = {
    .name = "cpp",
    .aliases = {"c++", "cxx", "cc"},
    .alias_count = 3,
    .class_types =
        makeNodeTypes("class_specifier", "struct_specifier", "union_specifier", "enum_specifier"),
    .field_types = makeNodeTypes("field_declaration"),
    .function_types =
        makeNodeTypes("function_definition", "function_declarator", "template_declaration"),
    .import_types = makeNodeTypes("preproc_include", "preproc_import"),
    .identifier_types = makeNodeTypes("identifier", "type_identifier", "field_identifier"),
    .function_queries = makeQueries(
        "(function_definition declarator: (function_declarator declarator: (identifier) @name))",
        "(function_definition declarator: (function_declarator declarator: (qualified_identifier "
        "name: (identifier) @name)))",
        "(function_definition declarator: (function_declarator declarator: (field_identifier) "
        "@name))",
        "(field_declaration declarator: (function_declarator declarator: (field_identifier) "
        "@name))",
        "(field_declaration declarator: (function_declarator declarator: (identifier) @name))",
        "(declaration declarator: (function_declarator declarator: (identifier) @name))",
        "(function_definition declarator: (function_declarator declarator: (destructor_name) "
        "@name))",
        "(template_declaration (function_definition declarator: (function_declarator declarator: "
        "(identifier) @name)))"),
    .class_queries =
        makeQueries("(class_specifier name: (type_identifier) @name)",
                    "(struct_specifier name: (type_identifier) @name)",
                    "(union_specifier name: (type_identifier) @name)",
                    "(enum_specifier name: (type_identifier) @name)",
                    "(template_declaration (class_specifier name: (type_identifier) @name))",
                    "(template_declaration (struct_specifier name: (type_identifier) @name))"),
    .import_queries = makeQueries("(preproc_include path: (string_literal) @path)",
                                  "(preproc_include path: (system_lib_string) @path)"),
    .call_queries = makeQueries(
        "(call_expression function: (identifier) @callee) @call",
        "(call_expression function: (field_expression field: (field_identifier) @callee)) @call",
        "(call_expression function: (qualified_identifier name: (identifier) @callee)) @call"),
};

inline constexpr LanguageConfig lang_c = {
    .name = "c",
    .aliases = {},
    .alias_count = 0,
    .class_types = makeNodeTypes("struct_specifier", "union_specifier", "enum_specifier"),
    .field_types = makeNodeTypes("field_declaration"),
    .function_types = makeNodeTypes("function_definition", "function_declarator"),
    .import_types = makeNodeTypes("preproc_include"),
    .identifier_types = makeNodeTypes("identifier", "type_identifier", "field_identifier"),
    .function_queries = makeQueries(
        "(function_definition declarator: (function_declarator declarator: (identifier) @name))",
        "(declaration declarator: (function_declarator declarator: (identifier) @name))"),
    .class_queries = makeQueries("(struct_specifier name: (type_identifier) @name)",
                                 "(union_specifier name: (type_identifier) @name)",
                                 "(enum_specifier name: (type_identifier) @name)"),
    .import_queries = makeQueries("(preproc_include path: (string_literal) @path)",
                                  "(preproc_include path: (system_lib_string) @path)"),
    .call_queries = makeQueries(
        "(call_expression function: (identifier) @callee) @call",
        "(call_expression function: (field_expression field: (field_identifier) @callee)) @call"),
};

inline constexpr LanguageConfig lang_python = {
    .name = "python",
    .aliases = {"py"},
    .alias_count = 1,
    .class_types = makeNodeTypes("class_definition"),
    .field_types = makeNodeTypes("attribute", "assignment"),
    .function_types = makeNodeTypes("function_definition", "decorated_definition"),
    .import_types = makeNodeTypes("import_statement", "import_from_statement"),
    .identifier_types = makeNodeTypes("identifier"),
    .function_queries =
        makeQueries("(function_definition name: (identifier) @name)",
                    "(decorated_definition (function_definition name: (identifier) @name))"),
    .class_queries =
        makeQueries("(class_definition name: (identifier) @name)",
                    "(decorated_definition (class_definition name: (identifier) @name))"),
    .import_queries = makeQueries("(import_statement name: (dotted_name) @module)",
                                  "(import_from_statement module_name: (dotted_name) @module)",
                                  "(import_from_statement module_name: (relative_import) @module)"),
    .call_queries =
        makeQueries("(call function: (identifier) @callee) @call",
                    "(call function: (attribute attribute: (identifier) @callee)) @call"),
};

inline constexpr LanguageConfig lang_rust = {
    .name = "rust",
    .aliases = {"rs"},
    .alias_count = 1,
    .class_types = makeNodeTypes("struct_item", "enum_item", "trait_item", "impl_item"),
    .field_types = makeNodeTypes("field_declaration", "field_pattern"),
    .function_types = makeNodeTypes("function_item", "function_signature_item"),
    .import_types = makeNodeTypes("use_declaration"),
    .identifier_types = makeNodeTypes("identifier", "type_identifier", "field_identifier"),
    .function_queries =
        makeQueries("(function_item name: (identifier) @name)",
                    "(function_signature_item name: (identifier) @name)",
                    "(impl_item (function_item name: (identifier) @name))",
                    "(trait_item (function_signature_item name: (identifier) @name))"),
    .class_queries = makeQueries("(struct_item name: (type_identifier) @name)",
                                 "(enum_item name: (type_identifier) @name)",
                                 "(trait_item name: (type_identifier) @name)"),
    .import_queries = makeQueries("(use_declaration argument: (scoped_identifier) @path)",
                                  "(use_declaration argument: (identifier) @path)",
                                  "(extern_crate_declaration name: (identifier) @path)"),
    .call_queries = makeQueries(
        "(call_expression function: (identifier) @callee) @call",
        "(call_expression function: (field_expression field: (field_identifier) @callee)) @call"),
};

inline constexpr LanguageConfig lang_go = {
    .name = "go",
    .aliases = {"golang"},
    .alias_count = 1,
    .class_types = makeNodeTypes("type_spec", "type_declaration"),
    .field_types = makeNodeTypes("field_declaration"),
    .function_types = makeNodeTypes("function_declaration", "method_declaration"),
    .import_types = makeNodeTypes("import_declaration", "import_spec"),
    .identifier_types = makeNodeTypes("identifier", "type_identifier", "field_identifier"),
    .function_queries = makeQueries("(function_declaration name: (identifier) @name)",
                                    "(method_declaration name: (field_identifier) @name)"),
    .class_queries = makeQueries("(type_declaration (type_spec name: (type_identifier) @name))"),
    .import_queries =
        makeQueries("(import_declaration (import_spec path: (interpreted_string_literal) @path))",
                    "(import_spec path: (interpreted_string_literal) @path)"),
    .call_queries = makeQueries("(call_expression function: (identifier) @callee) @call",
                                "(call_expression function: (selector_expression field: "
                                "(field_identifier) @callee)) @call"),
};

inline constexpr LanguageConfig lang_java = {
    .name = "java",
    .aliases = {},
    .alias_count = 0,
    .class_types = makeNodeTypes("class_declaration", "interface_declaration", "enum_declaration",
                                 "annotation_type_declaration"),
    .field_types = makeNodeTypes("field_declaration"),
    .function_types = makeNodeTypes("method_declaration", "constructor_declaration"),
    .import_types = makeNodeTypes("import_declaration"),
    .identifier_types = makeNodeTypes("identifier"),
    .function_queries = makeQueries("(method_declaration name: (identifier) @name)",
                                    "(constructor_declaration name: (identifier) @name)"),
    .class_queries = makeQueries("(class_declaration name: (identifier) @name)",
                                 "(interface_declaration name: (identifier) @name)",
                                 "(enum_declaration name: (identifier) @name)",
                                 "(annotation_type_declaration name: (identifier) @name)"),
    .import_queries = makeQueries("(import_declaration (scoped_identifier) @path)"),
    .call_queries = makeQueries("(method_invocation name: (identifier) @callee) @call"),
};

inline constexpr LanguageConfig lang_javascript = {
    .name = "javascript",
    .aliases = {"js"},
    .alias_count = 1,
    .class_types = makeNodeTypes("class_declaration", "class", "class_static_block"),
    .field_types =
        makeNodeTypes("public_field_definition", "field_definition", "property_signature"),
    .function_types =
        makeNodeTypes("function_declaration", "function_expression", "arrow_function",
                      "method_definition", "generator_function", "generator_function_declaration"),
    .import_types = makeNodeTypes("import_statement", "import_specifier", "namespace_import",
                                  "export_statement", "export_specifier"),
    .identifier_types = makeNodeTypes("identifier", "property_identifier"),
    .function_queries =
        makeQueries("(function_declaration name: (identifier) @name)",
                    "(function_expression name: (identifier) @name)",
                    "(method_definition name: (property_identifier) @name)",
                    "(variable_declarator name: (identifier) @name value: (arrow_function))",
                    "(variable_declarator name: (identifier) @name value: (function_expression))",
                    "(generator_function_declaration name: (identifier) @name)",
                    "(generator_function name: (identifier) @name)"),
    .class_queries = makeQueries("(class_declaration name: (identifier) @name)",
                                 "(class name: (identifier) @name)"),
    .import_queries =
        makeQueries("(import_statement source: (string) @source)",
                    "(export_statement source: (string) @source)",
                    "(call_expression function: (import) arguments: (arguments (string) @source))"),
    .call_queries = makeQueries("(call_expression function: (identifier) @callee) @call",
                                "(call_expression function: (member_expression property: "
                                "(property_identifier) @callee)) @call"),
};

inline constexpr LanguageConfig lang_typescript = {
    .name = "typescript",
    .aliases = {"ts"},
    .alias_count = 1,
    .class_types = makeNodeTypes("class_declaration", "class", "interface_declaration",
                                 "type_alias_declaration", "abstract_class_declaration"),
    .field_types =
        makeNodeTypes("public_field_definition", "field_definition", "property_signature"),
    .function_types = makeNodeTypes("function_declaration", "function_expression", "arrow_function",
                                    "method_definition", "method_signature",
                                    "abstract_method_signature", "generator_function"),
    .import_types = makeNodeTypes("import_statement", "import_specifier", "namespace_import",
                                  "export_statement", "import_alias"),
    .identifier_types = makeNodeTypes("identifier", "property_identifier", "type_identifier"),
    .function_queries =
        makeQueries("(function_declaration name: (identifier) @name)",
                    "(function_expression name: (identifier) @name)",
                    "(method_definition name: (property_identifier) @name)",
                    "(variable_declarator name: (identifier) @name value: (arrow_function))",
                    "(variable_declarator name: (identifier) @name value: (function_expression))",
                    "(method_signature name: (property_identifier) @name)",
                    "(abstract_method_signature name: (property_identifier) @name)"),
    .class_queries = makeQueries("(class_declaration name: (identifier) @name)",
                                 "(class name: (identifier) @name)",
                                 "(abstract_class_declaration name: (identifier) @name)",
                                 "(interface_declaration name: (type_identifier) @name)",
                                 "(type_alias_declaration name: (type_identifier) @name)",
                                 "(enum_declaration name: (identifier) @name)"),
    .import_queries =
        makeQueries("(import_statement source: (string) @source)",
                    "(export_statement source: (string) @source)",
                    "(call_expression function: (import) arguments: (arguments (string) @source))"),
    .call_queries = makeQueries("(call_expression function: (identifier) @callee) @call",
                                "(call_expression function: (member_expression property: "
                                "(property_identifier) @callee)) @call"),
};

inline constexpr LanguageConfig lang_csharp = {
    .name = "csharp",
    .aliases = {"c#", "cs"},
    .alias_count = 2,
    .class_types = makeNodeTypes("class_declaration", "struct_declaration", "interface_declaration",
                                 "enum_declaration"),
    .field_types = makeNodeTypes("field_declaration", "property_declaration"),
    .function_types =
        makeNodeTypes("method_declaration", "constructor_declaration", "local_function_statement"),
    .import_types = makeNodeTypes("using_directive"),
    .identifier_types = makeNodeTypes("identifier"),
    .function_queries = makeQueries("(method_declaration name: (identifier) @name)",
                                    "(constructor_declaration name: (identifier) @name)",
                                    "(destructor_declaration name: (identifier) @name)",
                                    "(property_declaration name: (identifier) @name)"),
    .class_queries = makeQueries("(class_declaration name: (identifier) @name)",
                                 "(struct_declaration name: (identifier) @name)",
                                 "(interface_declaration name: (identifier) @name)",
                                 "(enum_declaration name: (identifier) @name)",
                                 "(record_declaration name: (identifier) @name)"),
    .import_queries = makeQueries("(using_directive (identifier) @path)",
                                  "(using_directive (qualified_name) @path)"),
    .call_queries = makeQueries("(invocation_expression function: (identifier) @callee) @call",
                                "(invocation_expression function: (member_access_expression name: "
                                "(identifier) @callee)) @call"),
};

inline constexpr LanguageConfig lang_php = {
    .name = "php",
    .aliases = {},
    .alias_count = 0,
    .class_types = makeNodeTypes("class_declaration", "interface_declaration", "trait_declaration"),
    .field_types = makeNodeTypes("property_declaration", "property_element"),
    .function_types = makeNodeTypes("function_definition", "method_declaration"),
    .import_types = makeNodeTypes("namespace_use_declaration", "use_declaration"),
    .identifier_types = makeNodeTypes("name", "variable_name"),
    .function_queries = makeQueries("(function_definition name: (name) @name)",
                                    "(method_declaration name: (name) @name)"),
    .class_queries = makeQueries(
        "(class_declaration name: (name) @name)", "(interface_declaration name: (name) @name)",
        "(trait_declaration name: (name) @name)", "(enum_declaration name: (name) @name)"),
    .import_queries =
        makeQueries("(namespace_use_clause (qualified_name) @path)",
                    "(include_expression (string) @path)", "(require_expression (string) @path)"),
    .call_queries = makeQueries("(function_call_expression function: (name) @callee) @call",
                                "(member_call_expression name: (name) @callee) @call"),
};

inline constexpr LanguageConfig lang_kotlin = {
    .name = "kotlin",
    .aliases = {"kt"},
    .alias_count = 1,
    .class_types =
        makeNodeTypes("class_declaration", "object_declaration", "interface_declaration"),
    .field_types = makeNodeTypes("property_declaration"),
    .function_types = makeNodeTypes("function_declaration"),
    .import_types = makeNodeTypes("import_header", "import_list"),
    .identifier_types = makeNodeTypes("simple_identifier"),
    .function_queries = makeQueries("(function_declaration (simple_identifier) @name)"),
    .class_queries = makeQueries("(class_declaration (type_identifier) @name)",
                                 "(object_declaration (type_identifier) @name)",
                                 "(interface_declaration (type_identifier) @name)"),
    .import_queries = makeQueries("(import_header (identifier) @path)"),
    .call_queries =
        makeQueries("(call_expression (simple_identifier) @callee) @call",
                    "(call_expression (navigation_expression (simple_identifier) @callee)) @call"),
};

inline constexpr LanguageConfig lang_perl = {
    .name = "perl",
    .aliases = {"pl"},
    .alias_count = 1,
    .class_types = makeNodeTypes("package_statement"),
    .field_types = makeNodeTypes(),
    .function_types = makeNodeTypes("subroutine_declaration_statement", "function_definition"),
    .import_types = makeNodeTypes("use_statement", "require_statement"),
    .identifier_types = makeNodeTypes("identifier", "scalar_variable"),
    .function_queries = makeQueries("(subroutine_declaration_statement name: (identifier) @name)"),
    .class_queries = makeQueries("(package_statement (package_name) @name)"),
    .import_queries = makeQueries("(use_statement (package_name) @path)"),
    .call_queries = makeQueries("(function_call_expression (identifier) @callee) @call"),
};

inline constexpr LanguageConfig lang_r = {
    .name = "r",
    .aliases = {},
    .alias_count = 0,
    .class_types = makeNodeTypes(),
    .field_types = makeNodeTypes(),
    .function_types = makeNodeTypes("function_definition"),
    .import_types = makeNodeTypes("call"),
    .identifier_types = makeNodeTypes("identifier"),
    .function_queries = makeQueries("(binary_operator lhs: (identifier) @name operator: \"<-\")",
                                    "(binary_operator lhs: (identifier) @name operator: \"=\")"),
    .class_queries = makeQueries(),
    .import_queries = makeQueries(),
    .call_queries = makeQueries("(call function: (identifier) @callee) @call"),
};

inline constexpr LanguageConfig lang_sql = {
    .name = "sql",
    .aliases = {},
    .alias_count = 0,
    .class_types = makeNodeTypes("create_table_statement"),
    .field_types = makeNodeTypes("column_definition"),
    .function_types = makeNodeTypes("create_function_statement", "create_procedure_statement"),
    .import_types = makeNodeTypes(),
    .identifier_types = makeNodeTypes("identifier", "object_reference"),
    .function_queries = makeQueries("(create_function_statement name: (identifier) @name)",
                                    "(create_procedure_statement name: (identifier) @name)"),
    .class_queries = makeQueries("(create_table_statement name: (identifier) @name)"),
    .import_queries = makeQueries(),
    .call_queries = makeQueries(),
};

inline constexpr LanguageConfig lang_solidity = {
    .name = "solidity",
    .aliases = {"sol"},
    .alias_count = 1,
    .class_types = makeNodeTypes("contract_declaration", "interface_declaration",
                                 "library_declaration", "struct_declaration"),
    .field_types = makeNodeTypes("state_variable_declaration"),
    .function_types =
        makeNodeTypes("function_definition", "constructor_definition", "modifier_definition"),
    .import_types = makeNodeTypes("import_directive"),
    .identifier_types = makeNodeTypes("identifier"),
    .function_queries = makeQueries("(function_definition name: (identifier) @name)",
                                    "(modifier_definition name: (identifier) @name)"),
    .class_queries = makeQueries("(contract_declaration name: (identifier) @name)",
                                 "(interface_declaration name: (identifier) @name)",
                                 "(library_declaration name: (identifier) @name)",
                                 "(struct_declaration name: (identifier) @name)",
                                 "(enum_declaration name: (identifier) @name)",
                                 "(event_definition name: (identifier) @name)"),
    .import_queries = makeQueries("(import_directive source: (string) @path)"),
    .call_queries = makeQueries(
        "(call_expression function: (identifier) @callee) @call",
        "(call_expression function: (member_expression property: (identifier) @callee)) @call"),
};

inline constexpr LanguageConfig lang_dart = {
    .name = "dart",
    .aliases = {"flutter"},
    .alias_count = 1,
    .class_types = makeNodeTypes("class_definition", "mixin_declaration", "enum_declaration"),
    .field_types = makeNodeTypes("declaration", "initialized_variable_definition"),
    .function_types = makeNodeTypes("function_signature", "method_signature", "function_body"),
    .import_types = makeNodeTypes("import_or_export"),
    .identifier_types = makeNodeTypes("identifier"),
    .function_queries = makeQueries("(function_signature name: (identifier) @name)",
                                    "(method_signature name: (identifier) @name)",
                                    "(getter_signature name: (identifier) @name)",
                                    "(setter_signature name: (identifier) @name)"),
    .class_queries = makeQueries("(class_definition name: (identifier) @name)",
                                 "(mixin_declaration name: (identifier) @name)",
                                 "(extension_declaration name: (identifier) @name)",
                                 "(enum_declaration name: (identifier) @name)"),
    .import_queries = makeQueries("(import_or_export (string_literal) @path)"),
    .call_queries =
        makeQueries("(function_expression_invocation function: (identifier) @callee) @call",
                    "(cascade_selector (identifier) @callee) @call"),
};

// P4 (Programming Protocol-independent Packet Processors) - network data plane language
inline constexpr LanguageConfig lang_p4 = {
    .name = "p4",
    .aliases = {"p4_16", "p4lang"},
    .alias_count = 2,
    .class_types = makeNodeTypes("headerTypeDeclaration", "structTypeDeclaration",
                                 "controlDeclaration", "parserDeclaration"),
    .field_types = makeNodeTypes("structField", "parameter"),
    .function_types = makeNodeTypes("actionDeclaration", "functionDeclaration", "externDeclaration",
                                    "methodPrototype"),
    .import_types = makeNodeTypes("includeStatement"),
    .identifier_types = makeNodeTypes("IDENTIFIER", "nonTypeName", "typeName"),
    .function_queries =
        makeQueries("(actionDeclaration name: (IDENTIFIER) @name)",
                    "(functionDeclaration (functionPrototype name: (IDENTIFIER) @name))",
                    "(externDeclaration name: (IDENTIFIER) @name)",
                    "(methodPrototype name: (IDENTIFIER) @name)"),
    .class_queries = makeQueries("(headerTypeDeclaration name: (IDENTIFIER) @name)",
                                 "(structTypeDeclaration name: (IDENTIFIER) @name)",
                                 "(controlDeclaration name: (IDENTIFIER) @name)",
                                 "(parserDeclaration name: (IDENTIFIER) @name)",
                                 "(tableDeclaration name: (IDENTIFIER) @name)",
                                 "(typedefDeclaration name: (IDENTIFIER) @name)"),
    .import_queries = makeQueries("(includeStatement (STRING_LITERAL) @path)"),
    .call_queries = makeQueries("(functionCall function: (IDENTIFIER) @callee) @call",
                                "(methodCallExpression method: (IDENTIFIER) @callee) @call"),
};

// Master configuration table
inline constexpr std::array<const LanguageConfig*, 17> language_configs = {
    &lang_cpp,        &lang_c,          &lang_python,   &lang_rust, &lang_go,     &lang_java,
    &lang_javascript, &lang_typescript, &lang_csharp,   &lang_php,  &lang_kotlin, &lang_perl,
    &lang_r,          &lang_sql,        &lang_solidity, &lang_dart, &lang_p4,
};

// Lookup function - returns nullptr if language not found
constexpr const LanguageConfig* getLanguageConfig(std::string_view language) noexcept {
    for (const auto* config : language_configs) {
        if (config->name == language)
            return config;
        for (size_t i = 0; i < config->alias_count; ++i) {
            if (config->aliases[i] == language)
                return config;
        }
    }
    return nullptr;
}

// ============================================================================
// Helper: Extract symbols by node type (fallback when queries don't work)
// ============================================================================

SymbolExtractor::Result SymbolExtractor::extractSymbolsByNodeType(const ExtractionContext& ctx,
                                                                  std::string_view symbol_kind) {
    ExtractionResult result;
    const auto* config = getLanguageConfig(ctx.language);
    if (!config)
        return result;

    const NodeTypeList* type_list = nullptr;
    if (symbol_kind == "function")
        type_list = &config->function_types;
    else if (symbol_kind == "class")
        type_list = &config->class_types;
    else if (symbol_kind == "field")
        type_list = &config->field_types;

    if (!type_list || type_list->count == 0)
        return result;

    // Find identifier in node
    auto find_identifier = [&config](TSNode n) -> TSNode {
        std::function<TSNode(TSNode)> search = [&](TSNode node) -> TSNode {
            if (ts_node_is_null(node))
                return TSNode{};

            const char* type = ts_node_type(node);
            if (config->identifier_types.matches(type))
                return node;

            uint32_t child_count = ts_node_child_count(node);
            for (uint32_t i = 0; i < child_count; ++i) {
                TSNode found = search(ts_node_child(node, i));
                if (!ts_node_is_null(found))
                    return found;
            }
            return TSNode{};
        };
        return search(n);
    };

    // Traverse AST
    std::function<void(TSNode)> traverse = [&](TSNode node) {
        if (ts_node_is_null(node))
            return;

        const char* node_type = ts_node_type(node);
        if (type_list->matches(node_type)) {
            TSNode name_node = find_identifier(node);
            if (!ts_node_is_null(name_node)) {
                SymbolInfo sym;
                sym.name = ctx.extractNodeText(name_node);
                sym.qualified_name = sym.name;
                sym.kind = std::string(symbol_kind);
                sym.file_path = std::string(ctx.file_path);

                TSPoint start = ts_node_start_point(node);
                TSPoint end = ts_node_end_point(node);
                sym.start_line = start.row + 1;
                sym.end_line = end.row + 1;
                sym.start_offset = ts_node_start_byte(node);
                sym.end_offset = ts_node_end_byte(node);

                if (sym.is_valid()) {
                    result.symbols.push_back(std::move(sym));
                }
            }
        }

        uint32_t child_count = ts_node_child_count(node);
        for (uint32_t i = 0; i < child_count; ++i) {
            traverse(ts_node_child(node, i));
        }
    };

    traverse(ctx.root);
    return result;
}

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

        // Extract fields/member variables
        auto field_result = extractFields(ctx);
        if (!field_result)
            return field_result;

        // Combine all symbols
        result.symbols.reserve(func_result->symbols.size() + class_result->symbols.size() +
                               struct_result->symbols.size() + field_result->symbols.size());
        std::ranges::move(func_result->symbols, std::back_inserter(result.symbols));
        std::ranges::move(class_result->symbols, std::back_inserter(result.symbols));
        std::ranges::move(struct_result->symbols, std::back_inserter(result.symbols));
        std::ranges::move(field_result->symbols, std::back_inserter(result.symbols));

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

        // Extract member containment relationships (class contains methods/fields)
        auto member_result = extractMemberRelations(ctx, result.symbols);
        if (member_result) {
            std::ranges::move(member_result->relations, std::back_inserter(result.relations));
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

    const auto* config = getLanguageConfig(ctx.language);
    if (!config || config->function_queries.count == 0) {
        // Fallback for unknown language: use node type traversal
        return extractSymbolsByNodeType(ctx, "function");
    }

    // Execute queries from config
    bool any_query_succeeded = false;
    for (size_t i = 0; i < config->function_queries.count; ++i) {
        if (executeQuery(ctx, config->function_queries[i], "function", result.symbols)) {
            any_query_succeeded = true;
        }
    }

    if (any_query_succeeded) {
        return result;
    }

    // Fallback: node type traversal
    return extractSymbolsByNodeType(ctx, "function");
}

SymbolExtractor::Result SymbolExtractor::extractClasses(const ExtractionContext& ctx) {
    ExtractionResult result;

    const auto* config = getLanguageConfig(ctx.language);
    if (!config || config->class_queries.count == 0) {
        return extractSymbolsByNodeType(ctx, "class");
    }

    // Execute queries from config, inferring kind from query pattern
    for (size_t i = 0; i < config->class_queries.count; ++i) {
        std::string_view query = config->class_queries[i];
        std::string kind = "class";
        if (query.find("struct") != std::string_view::npos)
            kind = "struct";
        else if (query.find("interface") != std::string_view::npos)
            kind = "interface";
        else if (query.find("enum") != std::string_view::npos)
            kind = "enum";
        else if (query.find("trait") != std::string_view::npos)
            kind = "trait";

        executeQuery(ctx, query, kind, result.symbols);
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
                                    std::strcmp(ts_node_type(grandparent),
                                                "template_declaration") == 0) {
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

    // Get call patterns from config
    const auto* config = getLanguageConfig(ctx.language);
    if (!config || config->call_queries.count == 0) {
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

    for (size_t i = 0; i < config->call_queries.count; ++i) {
        std::string_view pattern = config->call_queries[i];
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
            spdlog::info("[CallExtraction] {} calls: {}", caller, [&]() {
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

    const auto* config = getLanguageConfig(ctx.language);
    if (!config || config->import_queries.count == 0) {
        return result;
    }

    std::string file_path_str = std::string(ctx.file_path);
    size_t extracted_count = 0;

    for (size_t i = 0; i < config->import_queries.count; ++i) {
        std::string_view pattern = config->import_queries[i];
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
            // Take the first capture (queries are structured with path/module first)
            if (match.capture_count > 0) {
                TSQueryCapture capture = match.captures[0];
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

SymbolExtractor::Result SymbolExtractor::extractFields(const ExtractionContext& ctx) {
    // Use the generic node type traversal with field types
    return extractSymbolsByNodeType(ctx, "field");
}

SymbolExtractor::Result
SymbolExtractor::extractMemberRelations(const ExtractionContext& ctx,
                                        const std::vector<SymbolInfo>& symbols) {
    ExtractionResult result;

    // Build map of classes/structs by name with their byte ranges
    struct ClassInfo {
        std::string name;
        uint32_t start_offset;
        uint32_t end_offset;
    };
    std::vector<ClassInfo> classes;

    for (const auto& sym : symbols) {
        if (sym.kind == "class" || sym.kind == "struct" || sym.kind == "trait" ||
            sym.kind == "interface") {
            classes.push_back({sym.name, sym.start_offset, sym.end_offset});
        }
    }

    if (classes.empty()) {
        return result;
    }

    // For each function/method/field, check if it's within a class boundary
    for (const auto& sym : symbols) {
        if (sym.kind == "function" || sym.kind == "method" || sym.kind == "field") {
            // Find containing class by byte range
            for (const auto& cls : classes) {
                if (sym.start_offset >= cls.start_offset && sym.end_offset <= cls.end_offset) {
                    // This symbol is contained within this class
                    if (sym.name != cls.name) { // Don't create self-contains
                        SymbolRelation rel;
                        rel.src_symbol = cls.name;
                        rel.dst_symbol = sym.name;
                        rel.kind = "contains";
                        rel.weight = 1.0;
                        result.relations.push_back(std::move(rel));
                    }
                    break; // A symbol can only be in one class
                }
            }
        }
    }

    if (!result.relations.empty()) {
        spdlog::info("[MemberRelations] Extracted {} contains relations from {}",
                     result.relations.size(), ctx.file_path);
    }

    return result;
}

} // namespace yams::plugins::treesitter