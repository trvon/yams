#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <dlfcn.h>
#include <string>
#include <string_view>
#include <unordered_set>

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

    std::string lang(language);
    std::transform(lang.begin(), lang.end(), lang.begin(), ::tolower);

    static const std::unordered_set<std::string> supported_languages = {
        "c",   "cpp",        "c++", "python", "rust",   "go", "javascript",
        "js",  "typescript", "ts",  "java",   "csharp", "c#", "cs",
        "php", "kotlin",     "kt",  "perl",   "pl",     "r",  "sql"};

    return supported_languages.count(lang) > 0;
}

static int extract_symbols_abi(void* /*self*/, const char* content, size_t content_len,
                               const char* file_path, const char* language,
                               yams_symbol_extraction_result_v1** out) {
    if (!out)
        return YAMS_PLUGIN_ERR_INVALID;
    if (!content || content_len == 0 || !language) {
        auto* r = (yams_symbol_extraction_result_v1*)std::calloc(
            1, sizeof(yams_symbol_extraction_result_v1));
        if (r) {
            r->error = dup_cstr("Empty or invalid input");
            *out = r;
        }
        return YAMS_PLUGIN_ERR_INVALID;
    }

    std::fprintf(stderr, "[yams] extract_symbols_abi entry (lang=%s)\n", language);
    using yams::plugins::treesitter::GrammarLoader;
    std::fprintf(stderr, "[yams] constructing GrammarLoader...\n");
    GrammarLoader loader;
    std::fprintf(stderr, "[yams] GrammarLoader constructed\n");
    auto lg = loader.loadGrammar(language);
    std::fprintf(stderr, "[yams] loadGrammar returned (ok=%d)\n", lg.has_value());
    std::string dl_err;
    if (!lg) {
        // Check if auto-download is enabled (default: true for backwards compatibility)
        const char* auto_dl = std::getenv("YAMS_AUTO_DOWNLOAD_GRAMMARS");
        bool should_auto_download = (!auto_dl || std::string(auto_dl) != "0");

        if (should_auto_download) {
            std::fprintf(stderr,
                         "[yams] auto-installing tree-sitter grammar for '%s' into datadir...\n",
                         language);
            auto dl = yams::plugins::treesitter::GrammarDownloader::downloadGrammar(language);
            if (dl.has_value()) {
                lg = loader.loadGrammar(language);
            } else if (dl.error().size()) {
                dl_err = dl.error();
            }
        } else {
            std::fprintf(stderr,
                         "[yams] auto-download disabled, skipping grammar install for '%s'\n",
                         language);
            dl_err = "Grammar auto-download is disabled (YAMS_AUTO_DOWNLOAD_GRAMMARS=0)";
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
    std::fprintf(stderr, "[yams] grammar loaded: handle=%p lang=%p for '%s'\n", handle,
                 (void*)tslang, language);
    // Validate language version compatibility (support versions 13-15)
    uint32_t ver = ts_language_version(tslang);
    std::fprintf(stderr, "[yams] ts_language_version=%u\n", ver);
    if (ver == 0 || ver < TREE_SITTER_MIN_COMPATIBLE_LANGUAGE_VERSION ||
        ver > TREE_SITTER_LANGUAGE_VERSION) {
        auto* r = (yams_symbol_extraction_result_v1*)std::calloc(
            1, sizeof(yams_symbol_extraction_result_v1));
        if (!r)
            return YAMS_PLUGIN_ERR_INVALID;
        r->error = dup_cstr("Incompatible tree-sitter language version");
        *out = r;
        dlclose(handle);
        return YAMS_PLUGIN_ERR_INVALID;
    }

    using yams::plugins::treesitter::SymbolExtractor;
    std::fprintf(stderr, "[yams] starting extraction (len=%zu, path=%s, lang=%s)\n", content_len,
                 file_path ? file_path : "", language);
    SymbolExtractor extractor(tslang);
    auto res = extractor.extract(std::string_view(content, content_len), language,
                                 file_path ? file_path : "", true);

    (void)handle; // managed by loader
    std::fprintf(stderr, "[yams] extraction finished (ok=%d)\n", res.has_value());

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
      "languages": ["cpp", "c++", "c", "python", "go", "rust", "javascript", "typescript", "java", "csharp", "php", "kotlin", "perl", "r", "sql"],
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

int yams_plugin_init(const char* config_json, const void* host_context) {
    // Plugin is stateless and uses runtime grammar loading
    (void)config_json;
    (void)host_context;
    return YAMS_PLUGIN_OK;
}

void yams_plugin_shutdown(void) {
    // No cleanup needed - all grammars are loaded/unloaded per-request
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
