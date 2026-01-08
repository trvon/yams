#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <yams/plugins/abi.h>
#include <yams/plugins/entity_extractor_v2.h>

#include "gliner_session.h"

namespace {

// Global plugin state
struct GlintPluginContext {
    std::mutex mutex;
    std::unique_ptr<yams::glint::GlinerSession> session;
    yams::glint::GlinerConfig config;
    bool initialized = false;
    std::string last_error;

    // Default entity labels for NL text
    std::vector<std::string> default_labels = {"person", "organization", "location",   "date",
                                               "event",  "product",      "technology", "concept"};
};

static GlintPluginContext g_ctx;

char* dup_cstr(const std::string& s) {
    if (s.empty())
        return nullptr;
    char* p = static_cast<char*>(std::malloc(s.size() + 1));
    if (p)
        std::memcpy(p, s.c_str(), s.size() + 1);
    return p;
}

// Initialize session lazily
bool ensure_session() {
    std::lock_guard<std::mutex> lock(g_ctx.mutex);

    if (g_ctx.session && g_ctx.session->is_ready()) {
        return true;
    }

    // Create session with config
    g_ctx.config.entity_labels = g_ctx.default_labels;
    g_ctx.session = std::make_unique<yams::glint::GlinerSession>(g_ctx.config);

    if (!g_ctx.session->initialize()) {
        g_ctx.last_error = std::string(g_ctx.session->last_error());
        spdlog::error("[Glint] Failed to initialize session: {}", g_ctx.last_error);
        return false;
    }

    g_ctx.initialized = true;
    spdlog::info("[Glint] Session initialized successfully");
    return true;
}

bool supports_abi(void*, const char* content_type) {
    if (!content_type)
        return false;
    std::string ct(content_type);
    return ct == "text/plain" || ct == "text/markdown" || ct == "application/json";
}

int extract_abi(void*, const char* content, size_t content_len,
                const yams_entity_extraction_options_v2* options,
                yams_entity_extraction_result_v2** out) {
    if (!out)
        return YAMS_PLUGIN_ERR_INVALID;

    auto* r = static_cast<yams_entity_extraction_result_v2*>(
        std::calloc(1, sizeof(yams_entity_extraction_result_v2)));
    if (!r)
        return YAMS_PLUGIN_ERR_INVALID;

    r->entities = nullptr;
    r->entity_count = 0;
    r->relations = nullptr;
    r->relation_count = 0;
    r->error = nullptr;

    // Check for empty input
    if (!content || content_len == 0) {
        *out = r;
        return YAMS_PLUGIN_OK;
    }

    // Ensure session is initialized
    if (!ensure_session()) {
        r->error = dup_cstr(g_ctx.last_error);
        *out = r;
        return YAMS_PLUGIN_OK; // Return OK but with error message
    }

    // Parse custom entity labels from options if provided
    std::vector<std::string> labels;
    if (options && options->entity_types && options->entity_type_count > 0) {
        labels.reserve(options->entity_type_count);
        for (size_t i = 0; i < options->entity_type_count; ++i) {
            if (options->entity_types[i]) {
                labels.emplace_back(options->entity_types[i]);
            }
        }
    }

    // Extract entities
    std::string_view text(content, content_len);
    std::vector<yams::glint::EntitySpan> spans;

    {
        std::lock_guard<std::mutex> lock(g_ctx.mutex);
        if (labels.empty()) {
            spans = g_ctx.session->extract(text);
        } else {
            spans = g_ctx.session->extract(text, labels);
        }
    }

    // Convert to ABI format
    if (!spans.empty()) {
        r->entities =
            static_cast<yams_entity_v2*>(std::calloc(spans.size(), sizeof(yams_entity_v2)));
        if (!r->entities) {
            std::free(r);
            return YAMS_PLUGIN_ERR_INVALID;
        }

        r->entity_count = spans.size();

        for (size_t i = 0; i < spans.size(); ++i) {
            const auto& span = spans[i];
            r->entities[i].text = dup_cstr(span.text);
            r->entities[i].type = dup_cstr(span.label);
            r->entities[i].start_offset = span.start_char;
            r->entities[i].end_offset = span.end_char;
            r->entities[i].confidence = span.score;
            r->entities[i].qualified_name = nullptr;
            r->entities[i].scope = nullptr;

            // Add properties JSON with word indices
            nlohmann::json props;
            props["start_word"] = span.start_word;
            props["end_word"] = span.end_word;
            r->entities[i].properties_json = dup_cstr(props.dump());
        }
    }

    *out = r;
    return YAMS_PLUGIN_OK;
}

void free_result_abi(void*, yams_entity_extraction_result_v2* res) {
    if (!res)
        return;
    if (res->entities) {
        for (size_t i = 0; i < res->entity_count; ++i) {
            std::free(res->entities[i].text);
            std::free(res->entities[i].type);
            std::free(res->entities[i].qualified_name);
            std::free(res->entities[i].scope);
            std::free(res->entities[i].properties_json);
        }
        std::free(res->entities);
    }
    if (res->relations) {
        for (size_t i = 0; i < res->relation_count; ++i) {
            std::free(res->relations[i].src_entity);
            std::free(res->relations[i].dst_entity);
            std::free(res->relations[i].relation_type);
        }
        std::free(res->relations);
    }
    std::free(res->error);
    std::free(res);
}

int get_capabilities_json_abi(void*, char** out_json) {
    if (!out_json)
        return YAMS_PLUGIN_ERR_INVALID;

    nlohmann::json caps;
    caps["content_types"] = {"text/plain", "text/markdown", "application/json"};
    caps["entity_types"] = g_ctx.default_labels;
    caps["model"] = "gliner";
    caps["version"] = "0.1.0";
    caps["supports_custom_labels"] = true;
    caps["max_width"] = g_ctx.config.max_width;
    caps["threshold"] = g_ctx.config.threshold;

    *out_json = dup_cstr(caps.dump(2));
    return *out_json ? YAMS_PLUGIN_OK : YAMS_PLUGIN_ERR_INVALID;
}

void free_string_abi(void*, char* s) {
    std::free(s);
}

} // namespace

extern "C" {

YAMS_PLUGIN_API int yams_plugin_get_abi_version(void) {
    return YAMS_PLUGIN_ABI_VERSION;
}

YAMS_PLUGIN_API const char* yams_plugin_get_name(void) {
    return "glint";
}

YAMS_PLUGIN_API const char* yams_plugin_get_version(void) {
    return "0.1.0";
}

YAMS_PLUGIN_API const char* yams_plugin_get_manifest_json(void) {
    return R"({
  "name": "glint",
  "version": "0.1.0",
  "interfaces": [
    {"id": "entity_extractor_v2", "version": 1}
  ],
  "description": "Glint - GLiNER-based named entity extraction for natural language text",
  "requires": ["onnx"]
})";
}

YAMS_PLUGIN_API int yams_plugin_init(const char* config_json, const void* host_context) {
    (void)host_context;

    // Parse configuration
    if (config_json && *config_json) {
        try {
            auto cfg = nlohmann::json::parse(config_json, nullptr, false);
            if (!cfg.is_discarded()) {
                if (cfg.contains("model_path") && cfg["model_path"].is_string()) {
                    g_ctx.config.model_path = cfg["model_path"].get<std::string>();
                }
                if (cfg.contains("tokenizer_path") && cfg["tokenizer_path"].is_string()) {
                    g_ctx.config.tokenizer_path = cfg["tokenizer_path"].get<std::string>();
                }
                if (cfg.contains("threshold") && cfg["threshold"].is_number()) {
                    g_ctx.config.threshold = cfg["threshold"].get<float>();
                }
                if (cfg.contains("max_width") && cfg["max_width"].is_number()) {
                    g_ctx.config.max_width = cfg["max_width"].get<size_t>();
                }
                if (cfg.contains("entity_labels") && cfg["entity_labels"].is_array()) {
                    g_ctx.default_labels.clear();
                    for (const auto& label : cfg["entity_labels"]) {
                        if (label.is_string()) {
                            g_ctx.default_labels.push_back(label.get<std::string>());
                        }
                    }
                }
            }
        } catch (const std::exception& e) {
            spdlog::warn("[Glint] Failed to parse config: {}", e.what());
        }
    }

    spdlog::info("[Glint] Plugin initialized");
    return YAMS_PLUGIN_OK;
}

YAMS_PLUGIN_API void yams_plugin_shutdown(void) {
    std::lock_guard<std::mutex> lock(g_ctx.mutex);
    g_ctx.session.reset();
    g_ctx.initialized = false;
    spdlog::info("[Glint] Plugin shutdown");
}

YAMS_PLUGIN_API int yams_plugin_get_interface(const char* iface_id, uint32_t version,
                                              void** out_iface) {
    if (!iface_id || !out_iface)
        return YAMS_PLUGIN_ERR_INVALID;
    *out_iface = nullptr;

    if (std::strcmp(iface_id, YAMS_IFACE_ENTITY_EXTRACTOR_V2) == 0 &&
        version == YAMS_IFACE_ENTITY_EXTRACTOR_V2_VERSION) {
        static yams_entity_extractor_v2 g_iface = {
            .abi_version = YAMS_IFACE_ENTITY_EXTRACTOR_V2_VERSION,
            .self = nullptr,
            .supports = supports_abi,
            .extract = extract_abi,
            .free_result = free_result_abi,
            .get_capabilities_json = get_capabilities_json_abi,
            .free_string = free_string_abi,
        };
        *out_iface = &g_iface;
        return YAMS_PLUGIN_OK;
    }
    return YAMS_PLUGIN_ERR_NOT_FOUND;
}

YAMS_PLUGIN_API int yams_plugin_get_health_json(char** out_json) {
    if (!out_json)
        return YAMS_PLUGIN_ERR_INVALID;

    nlohmann::json health;
    health["status"] = g_ctx.initialized ? "ready" : "not_initialized";
    health["model_path"] = g_ctx.config.model_path;
    health["threshold"] = g_ctx.config.threshold;

    if (g_ctx.session) {
        health["session_ready"] = g_ctx.session->is_ready();
        if (!g_ctx.session->last_error().empty()) {
            health["last_error"] = std::string(g_ctx.session->last_error());
        }
    }

    *out_json = dup_cstr(health.dump(2));
    return *out_json ? YAMS_PLUGIN_OK : YAMS_PLUGIN_ERR_INVALID;
}

} // extern "C"
