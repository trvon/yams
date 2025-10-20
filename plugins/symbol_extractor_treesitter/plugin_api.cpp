#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <string>
#include <string_view>

#include <tree_sitter/api.h>

#include <yams/plugins/abi.h>
#include <yams/plugins/symbol_extractor_v1.h>

#include "grammar_loader.h"
#include "symbol_extractor.h"

namespace {
static char* dup_cstr(const std::string& s) {
    if (s.empty())
        return nullptr;
    char* p = (char*)std::malloc(s.size() + 1);
    if (p)
        std::memcpy(p, s.c_str(), s.size() + 1);
    return p;
}

static bool supports_language_abi(void* /*self*/, const char* language) {
    if (!language)
        return false;
    std::string L(language);
    std::transform(L.begin(), L.end(), L.begin(),
                   [](unsigned char c) { return (char)std::tolower(c); });
    return L == "c" || L == "cpp" || L == "c++" || L == "python" || L == "rust" || L == "go" ||
           L == "javascript" || L == "js" || L == "typescript" || L == "ts" || L == "java";
}

static int extract_symbols_abi(void* /*self*/, const char* content, size_t content_len,
                               const char* file_path, const char* language,
                               yams_symbol_extraction_result_v1** out) {
    if (!content || content_len == 0 || !out || !language)
        return YAMS_PLUGIN_ERR_INVALID;

    using yams::plugins::treesitter::GrammarLoader;
    GrammarLoader loader;
    auto lg = loader.loadGrammar(language);
    if (!lg) {
        auto* r = (yams_symbol_extraction_result_v1*)std::calloc(
            1, sizeof(yams_symbol_extraction_result_v1));
        if (!r)
            return YAMS_PLUGIN_ERR_INVALID;
        r->error = dup_cstr(lg.error().message);
        *out = r;
        return YAMS_PLUGIN_ERR_NOT_FOUND;
    }
    auto [handle, tslang] = *lg;

    using yams::plugins::treesitter::SymbolExtractor;
    SymbolExtractor extractor(tslang);
    auto res = extractor.extract(std::string_view(content, content_len), language,
                                 file_path ? file_path : "", true);

    (void)handle; // managed by loader

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
