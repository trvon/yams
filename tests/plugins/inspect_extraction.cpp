#include <cstring>
#include <dlfcn.h>
#include <fstream>
#include <iostream>
#include <optional>
#include <string>
#include <vector>

#include <yams/plugins/abi.h>
#include <yams/plugins/symbol_extractor_v1.h>

struct PluginHandle {
    void* handle = nullptr;
    yams_symbol_extractor_v1* api = nullptr;
};

std::optional<PluginHandle> loadPlugin() {
    PluginHandle p;
    const char* path = "builddir/plugins/symbol_extractor_treesitter/yams_symbol_extractor.so";

    p.handle = dlopen(path, RTLD_LAZY | RTLD_LOCAL);
    if (!p.handle) {
        std::cerr << "Failed to load plugin: " << dlerror() << std::endl;
        return std::nullopt;
    }

    auto get_iface = reinterpret_cast<int (*)(const char*, uint32_t, void**)>(
        dlsym(p.handle, "yams_plugin_get_interface"));

    void* iface_ptr = nullptr;
    int rc = get_iface(YAMS_IFACE_SYMBOL_EXTRACTOR_V1, YAMS_IFACE_SYMBOL_EXTRACTOR_V1_VERSION,
                       &iface_ptr);
    if (rc != YAMS_PLUGIN_OK || !iface_ptr) {
        return std::nullopt;
    }

    p.api = static_cast<yams_symbol_extractor_v1*>(iface_ptr);

    auto init_fn =
        reinterpret_cast<int (*)(const char*, const void*)>(dlsym(p.handle, "yams_plugin_init"));
    if (init_fn) {
        init_fn(nullptr, nullptr);
    }

    return p;
}

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <file.cpp>" << std::endl;
        return 1;
    }

    auto plugin = loadPlugin();
    if (!plugin) {
        std::cerr << "Failed to load plugin" << std::endl;
        return 1;
    }

    std::ifstream file(argv[1]);
    std::string content((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());

    yams_symbol_extraction_result_v1* result = nullptr;
    int rc = plugin->api->extract_symbols(plugin->api->self, content.c_str(), content.size(),
                                          argv[1], "cpp", &result);

    if (rc != YAMS_PLUGIN_OK || !result) {
        std::cerr << "Extraction failed" << std::endl;
        return 1;
    }

    std::cout << "Extracted " << result->symbol_count << " symbols:\n\n";

    for (size_t i = 0; i < result->symbol_count; ++i) {
        std::cout << "Symbol #" << i + 1 << ":\n";
        std::cout << "  Name: " << (result->symbols[i].name ? result->symbols[i].name : "NULL")
                  << "\n";
        std::cout << "  Kind: " << (result->symbols[i].kind ? result->symbols[i].kind : "NULL")
                  << "\n";
        std::cout << "  Line: " << result->symbols[i].start_line << "\n";
        std::cout << "  Qualified: "
                  << (result->symbols[i].qualified_name ? result->symbols[i].qualified_name
                                                        : "NULL")
                  << "\n";
        std::cout << "\n";
    }

    if (result->relation_count > 0) {
        std::cout << "\nExtracted " << result->relation_count << " relations:\n\n";
        for (size_t i = 0; i < result->relation_count; ++i) {
            std::cout << "  " << result->relations[i].src_symbol << " -> "
                      << result->relations[i].dst_symbol << " (" << result->relations[i].kind
                      << ")\n";
        }
    }

    plugin->api->free_result(plugin->api->self, result);
    return 0;
}
