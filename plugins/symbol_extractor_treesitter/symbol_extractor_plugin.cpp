//
// Enhanced Tree-sitter Symbol Extractor Plugin with Auto-grammar Loading
//
// This version fixes the critical issues identified in our review:
// 1. Fixes name extraction from tree-sitter captures (was hardcoded to "symbol")
// 2. Improves grammar loading from standard YAMS paths
// 3. Maintains compatibility with existing build system
// 4. Follows tree-sitter best practices
//

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <utility>

#include <dlfcn.h>
#include <filesystem>
#include <map>
#include <vector>

#include <string_view>
#include "grammar_loader.h"
#include "symbol_extractor.h"
#include <tree_sitter/api.h>

#include <yams/plugins/abi.h>
#include <yams/plugins/symbol_extractor_v1.h>

// Enhanced symbol name extraction utilities
namespace {

static char* dup_cstr(const std::string& s) {
    if (s.empty())
        return nullptr;
    char* p = (char*)malloc(s.size() + 1);
    if (p)
        std::memcpy(p, s.c_str(), s.size() + 1);
    return p;
}

// Extract actual text from content using byte range (KEY FIX: not hardcoded!)
static std::string extract_node_text(const std::string& content, TSNode node) {
    if (ts_node_is_null(node))
        return "";

    uint32_t start_byte = ts_node_start_byte(node);
    uint32_t end_byte = ts_node_end_byte(node);

    if (start_byte >= end_byte || end_byte > content.length())
        return "";

    return content.substr(start_byte, end_byte - start_byte);
}

struct GrammarLib {
    void* handle{nullptr};
    using LangFn = const TSLanguage* (*)();
    LangFn fn{nullptr};
    ~GrammarLib() {
        if (handle)
            dlclose(handle);
    }
};

struct GrammarSpec {
    const char* key;        // language key used by caller (e.g., "cpp", "python")
    const char* env_var;    // override env var name
    const char* symbol;     // factory symbol inside grammar lib
    const char* default_so; // common .so name
};

static const GrammarSpec kSpecs[] = {
    {"c", "YAMS_TS_C_LIB", "tree_sitter_c", "libtree-sitter-c.so"},
    {"cpp", "YAMS_TS_CPP_LIB", "tree_sitter_cpp", "libtree-sitter-cpp.so"},
    {"c++", "YAMS_TS_CPP_LIB", "tree_sitter_cpp", "libtree-sitter-cpp.so"},
    {"python", "YAMS_TS_PY_LIB", "tree_sitter_python", "libtree-sitter-python.so"},
    {"go", "YAMS_TS_GO_LIB", "tree_sitter_go", "libtree-sitter-go.so"},
    {"rust", "YAMS_TS_RUST_LIB", "tree_sitter_rust", "libtree-sitter-rust.so"},
    {"js", "YAMS_TS_JS_LIB", "tree_sitter_javascript", "libtree-sitter-javascript.so"},
    {"javascript", "YAMS_TS_JS_LIB", "tree_sitter_javascript", "libtree-sitter-javascript.so"},
    {"ts", "YAMS_TS_TS_LIB", "tree_sitter_typescript", "libtree-sitter-typescript.so"},
    {"typescript", "YAMS_TS_TS_LIB", "tree_sitter_typescript", "libtree-sitter-typescript.so"},
    {"java", "YAMS_TS_JAVA_LIB", "tree_sitter_java", "libtree-sitter-java.so"},
};

// STANDARD GRAMMAR PATHS - matching CLI implementation
[[maybe_unused]] static std::vector<std::string> getStandardGrammarPaths() {
    std::vector<std::string> paths;

    // XDG standard locations (PRIMARY from CLI implementation)
    const char* xdg_data_home = std::getenv("XDG_DATA_HOME");
    if (xdg_data_home && *xdg_data_home) {
        paths.emplace_back(std::string(xdg_data_home) + "/yams/grammars");
    }

    // Default user path (FALLBACK from CLI implementation)
    const char* home = std::getenv("HOME");
    if (home) {
        paths.emplace_back(std::string(home) + "/.local/share/yams/grammars");
    }

    // System-wide locations
    paths.emplace_back("/usr/local/share/yams/grammars");
    paths.emplace_back("/usr/share/yams/grammars");

    return paths;
}

struct Ctx {
    TSParser* parser{nullptr};
    std::map<std::string, std::string> grammar_paths; // From plugin config
    std::vector<std::string> supported_langs;         // From plugin config
    Ctx() {
        parser = ts_parser_new();
        // Default supported set
        for (auto& s : kSpecs)
            supported_langs.emplace_back(s.key);
    }
    ~Ctx() {
        if (parser)
            ts_parser_delete(parser);
    }
} g_ctx;

static const GrammarSpec* find_spec(const std::string& lang) {
    for (auto& s : kSpecs)
        if (lang == s.key)
            return &s;
    return nullptr;
}

// CORE FIX: Find identifier in tree using tree-sitter traversal
static TSNode find_identifier_node(TSNode parent) {
    if (ts_node_is_null(parent))
        return TSNode{};

    const char* node_type = ts_node_type(parent);
    if (std::strcmp(node_type, "identifier") == 0 ||
        std::strcmp(node_type, "field_identifier") == 0 ||
        std::strcmp(node_type, "type_identifier") == 0) {
        return parent;
    }

    // Recurse through children
    uint32_t child_count = ts_node_child_count(parent);
    for (uint32_t i = 0; i < child_count; ++i) {
        TSNode found = find_identifier_node(ts_node_child(parent, i));
        if (!ts_node_is_null(found))
            return found;
    }
    return TSNode{};
}

// KEY FIX: Enhanced symbol extraction with actual name extraction
static void collect_symbols_by_grammar(const std::string& lang, const std::string& content,
                                       TSNode root, const char* file_path,
                                       std::vector<yams_symbol_v1>& outSyms) {
    if (ts_node_is_null(root))
        return;

    const char* node_type = ts_node_type(root);
    bool is_function = false;

    // Language-specific function detection
    if ((lang == "cpp" || lang == "c++" || lang == "c") && std::strstr(node_type, "function")) {
        is_function = true;
    } else if (lang == "python" && std::strcmp(node_type, "function_definition") == 0) {
        is_function = true;
    } else if (lang == "rust" && std::strcmp(node_type, "function_item") == 0) {
        is_function = true;
    } else if (lang == "go" && (std::strcmp(node_type, "function_declaration") == 0 ||
                                std::strcmp(node_type, "method_declaration") == 0)) {
        is_function = true;
    } else if ((lang == "js" || lang == "javascript") && std::strstr(node_type, "function")) {
        is_function = true;
    }

    if (is_function) {
        TSNode id_node = find_identifier_node(root);
        if (!ts_node_is_null(id_node)) {
            // KEY FIX: Extract ACTUAL name from content, not "symbol"!
            std::string name_text = extract_node_text(content, id_node);
            if (!name_text.empty()) {
                yams_symbol_v1 sym{};
                sym.name = dup_cstr(name_text);
                sym.qualified_name = dup_cstr(name_text); // Simple namespace
                sym.kind = dup_cstr("function");
                sym.file_path = dup_cstr(file_path ? file_path : "");

                TSPoint sp = ts_node_start_point(root);
                TSPoint ep = ts_node_end_point(root);
                sym.start_line = sp.row + 1;
                sym.end_line = ep.row + 1;
                sym.start_offset = ts_node_start_byte(root);
                sym.end_offset = ts_node_end_byte(root);

                // Try to extract return type
                uint32_t child_count = ts_node_child_count(root);
                if (child_count > 0) {
                    TSNode first_child = ts_node_child(root, 0);
                    const char* child_type = ts_node_type(first_child);
                    if (std::strcmp(child_type, "primitive_type") == 0 ||
                        std::strcmp(child_type, "type_identifier") == 0) {
                        std::string return_type = extract_node_text(content, first_child);
                        if (!return_type.empty()) {
                            sym.return_type = dup_cstr(return_type);
                        }
                    }
                }

                outSyms.push_back(sym);
            }
        }
    }

    // Recurse to children
    uint32_t child_count = ts_node_child_count(root);
    for (uint32_t i = 0; i < child_count; ++i) {
        collect_symbols_by_grammar(lang, content, ts_node_child(root, i), file_path, outSyms);
    }
}

// Enhanced query-based extraction with proper capture handling

} // namespace

extern "C" {

int yams_plugin_get_abi_version(void) {
    return YAMS_PLUGIN_ABI_VERSION;
}
const char* yams_plugin_get_name(void) {
    return "symbol_extractor_treesitter";
}
const char* yams_plugin_get_version(void) {
    return "1.0.0";
}

// Thin wrappers that delegate to class-based implementation
static bool supports_language_abi(void* /*self*/, const char* language) {
    if (!language)
        return false;
    std::string L(language);
    std::transform(L.begin(), L.end(), L.begin(),
                   [](unsigned char c) { return (char)std::tolower(c); });
    return L == "c" || L == "cpp" || L == "c++" || L == "python" || L == "rust" || L == "go" ||
           L == "javascript" || L == "js" || L == "typescript" || L == "ts" || L == "java";
}

// Cache for languages where grammar download has already been attempted (success or failure)
// This prevents spawning download processes for every file of the same language type
static std::mutex s_download_attempted_mutex;
static std::set<std::string> s_download_attempted;

static int extract_symbols_abi(void* /*self*/, const char* content, size_t content_len,
                               const char* file_path, const char* language,
                               yams_symbol_extraction_result_v1** out) {
    if (!content || content_len == 0 || !out || !language)
        return YAMS_PLUGIN_ERR_INVALID;

    // Load grammar using GrammarLoader
    using yams::plugins::treesitter::GrammarLoader;
    GrammarLoader loader;
    auto lg = loader.loadGrammar(language);
    std::string dl_err;
    if (!lg) {
        // Check if auto-download is enabled (default: disabled to prevent process spawning)
        const char* auto_dl = std::getenv("YAMS_AUTO_DOWNLOAD_GRAMMARS");
        bool should_auto_download = (auto_dl && std::string(auto_dl) == "1");

        // Check if we've already attempted download for this language
        bool already_attempted = false;
        {
            std::lock_guard<std::mutex> lock(s_download_attempted_mutex);
            already_attempted = s_download_attempted.count(language) > 0;
        }

        if (should_auto_download && !already_attempted) {
            // Mark as attempted BEFORE downloading to prevent concurrent attempts
            {
                std::lock_guard<std::mutex> lock(s_download_attempted_mutex);
                s_download_attempted.insert(language);
            }

            std::fprintf(stderr,
                         "[yams] auto-installing tree-sitter grammar for '%s' into datadir...\n",
                         language);
            auto dl = yams::plugins::treesitter::GrammarDownloader::downloadGrammar(language);
            if (dl.has_value()) {
                lg = loader.loadGrammar(language);
            } else if (dl.error().size()) {
                dl_err = dl.error();
            }
        } else if (!should_auto_download) {
            dl_err =
                "Grammar not found. Set YAMS_AUTO_DOWNLOAD_GRAMMARS=1 to enable auto-download.";
        } else {
            dl_err = "Grammar download already attempted for this language.";
        }
    }
    if (!lg) {
        auto* r = (yams_symbol_extraction_result_v1*)std::calloc(
            1, sizeof(yams_symbol_extraction_result_v1));
        if (!r)
            return YAMS_PLUGIN_ERR_INVALID;
        r->error = dup_cstr(dl_err.empty() ? lg.error().message : dl_err);
        *out = r;
        return YAMS_PLUGIN_ERR_NOT_FOUND;
    }
    auto [handle, tslang] = *lg;

    // Use SymbolExtractor
    using yams::plugins::treesitter::SymbolExtractor;
    SymbolExtractor extractor(tslang);
    auto res = extractor.extract(std::string_view(content, content_len), language,
                                 file_path ? file_path : "", /*enable_call_graph*/ true);

    // Close library handle owned by loader
    (void)handle; // In loader impl, handle lifetime may be managed; safe noop here

    auto* r =
        (yams_symbol_extraction_result_v1*)std::calloc(1, sizeof(yams_symbol_extraction_result_v1));
    if (!r)
        return YAMS_PLUGIN_ERR_INVALID;

    if (!res) {
        r->error = dup_cstr(res.error());
        *out = r;
        return YAMS_PLUGIN_ERR_INVALID;
    }

    const auto& ok = *res;
    if (!ok.symbols.empty()) {
        r->symbol_count = ok.symbols.size();
        r->symbols = (yams_symbol_v1*)std::calloc(ok.symbols.size(), sizeof(yams_symbol_v1));
        for (size_t i = 0; i < ok.symbols.size(); ++i) {
            const auto& s = ok.symbols[i];
            yams_symbol_v1& dst = r->symbols[i];
            dst.name = dup_cstr(s.name);
            dst.qualified_name = dup_cstr(s.qualified_name);
            dst.kind = dup_cstr(s.kind);
            dst.file_path = dup_cstr(s.file_path);
            dst.start_line = s.start_line;
            dst.end_line = s.end_line;
            dst.start_offset = s.start_offset;
            dst.end_offset = s.end_offset;
            dst.return_type = s.return_type ? dup_cstr(*s.return_type) : nullptr;
            dst.parameters = nullptr;
            dst.parameter_count = 0;
            dst.documentation = s.documentation ? dup_cstr(*s.documentation) : nullptr;
        }
    }

    if (!ok.relations.empty()) {
        r->relation_count = ok.relations.size();
        r->relations = (yams_symbol_relation_v1*)std::calloc(ok.relations.size(),
                                                             sizeof(yams_symbol_relation_v1));
        for (size_t i = 0; i < ok.relations.size(); ++i) {
            const auto& rel = ok.relations[i];
            yams_symbol_relation_v1& dst = r->relations[i];
            dst.src_symbol = dup_cstr(rel.src_symbol);
            dst.dst_symbol = dup_cstr(rel.dst_symbol);
            dst.kind = dup_cstr(rel.kind);
            dst.weight = rel.weight;
        }
    }

    *out = r;
    return YAMS_PLUGIN_OK;
}

static void free_result_abi(void* /*self*/, yams_symbol_extraction_result_v1* res) {
    if (!res)
        return;
    if (res->symbols) {
        for (size_t i = 0; i < res->symbol_count; ++i) {
            free(res->symbols[i].name);
            free(res->symbols[i].qualified_name);
            free(res->symbols[i].kind);
            free(res->symbols[i].file_path);
            if (res->symbols[i].parameters) {
                for (size_t j = 0; j < res->symbols[i].parameter_count; ++j)
                    free(res->symbols[i].parameters[j]);
                free(res->symbols[i].parameters);
            }
            free(res->symbols[i].return_type);
            free(res->symbols[i].documentation);
        }
        free(res->symbols);
    }
    if (res->relations) {
        for (size_t i = 0; i < res->relation_count; ++i) {
            free(res->relations[i].src_symbol);
            free(res->relations[i].dst_symbol);
            free(res->relations[i].kind);
        }
        free(res->relations);
    }
    free(res->error);
    free(res);
}

static int get_capabilities_json_abi(void* /*self*/, char** out_json) {
    if (!out_json)
        return YAMS_PLUGIN_ERR_INVALID;
    std::string json = R"({
  "languages": ["cpp", "c++", "c", "python", "go", "rust", "javascript", "typescript", "java"],
  "features": ["symbols", "relations", "auto_grammar_loading"],
  "version": "1.0.0"
})";
    *out_json = dup_cstr(json);
    return *out_json ? YAMS_PLUGIN_OK : YAMS_PLUGIN_ERR_INVALID;
}

static void free_string_abi(void* /*self*/, char* s) {
    free(s);
}

int yams_plugin_get_interface(const char* iface_id, uint32_t version, void** out_iface) {
    if (!iface_id || !out_iface)
        return YAMS_PLUGIN_ERR_INVALID;
    *out_iface = nullptr;
    if (std::strcmp(iface_id, YAMS_IFACE_SYMBOL_EXTRACTOR_V1) == 0 &&
        version == YAMS_IFACE_SYMBOL_EXTRACTOR_V1_VERSION) {
        static yams_symbol_extractor_v1 g_iface = {
            .abi_version = YAMS_IFACE_SYMBOL_EXTRACTOR_V1_VERSION,
            .self = nullptr,
            .supports_language = supports_language_abi,
            .extract_symbols = extract_symbols_abi,
            .free_result = free_result_abi,
            .get_capabilities_json = get_capabilities_json_abi,
            .free_string = free_string_abi,
        };
        *out_iface = &g_iface;
        return YAMS_PLUGIN_OK;
    }
    return YAMS_PLUGIN_ERR_NOT_FOUND;
}

int yams_plugin_get_health_json(char** out_json) {
    (void)out_json;
    return YAMS_PLUGIN_ERR_NOT_FOUND;
}

} // extern "C"
