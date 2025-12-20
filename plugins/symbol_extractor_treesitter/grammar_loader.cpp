#include "grammar_loader.h"

#include <cstdio>
#include <expected>
#include <format>
#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>

#include <yams/compat/dlfcn.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <thread>

extern "C" {
#include <tree_sitter/api.h>
}

namespace yams::plugins::treesitter {

// GrammarLoader Implementation
GrammarLoader::GrammarLoader() {
    // Initialize default search paths
    auto paths = getGrammarSearchPaths();
    cached_search_paths_ = std::move(paths);
    search_paths_cached_ = true;
}

void GrammarLoader::addGrammarPath(std::string_view language, std::string_view path) {
    grammar_paths_[std::string(language)] = std::string(path);
}

const GrammarLoader::GrammarSpec* GrammarLoader::findSpec(std::string_view language) const {
    std::string lang_lower(language);
    std::transform(lang_lower.begin(), lang_lower.end(), lang_lower.begin(),
                   [](unsigned char c) { return std::tolower(c); });

    for (const auto& spec : kSpecs) {
        if (lang_lower == spec.key) {
            return &spec;
        }
    }
    return nullptr;
}

std::vector<std::filesystem::path> GrammarLoader::getGrammarSearchPaths() const {
    if (search_paths_cached_) {
        return cached_search_paths_;
    }

    std::vector<std::filesystem::path> paths;

    // Add config override paths first (highest priority)
    for (const auto& [lang, path] : grammar_paths_) {
        paths.emplace_back(path);
    }

    // Environment variable overrides
    for (const auto& spec : kSpecs) {
        if (const char* env_path = std::getenv(spec.env_var.data())) {
            if (*env_path) {
                paths.emplace_back(env_path);
            }
        }
    }

    // Config-specified data_dir (highest priority)
    std::filesystem::path cfgPath;
#ifdef _WIN32
    // Windows: LOCALAPPDATA/yams/config.toml or APPDATA/yams/config.toml
    if (const char* localappdata = std::getenv("LOCALAPPDATA")) {
        cfgPath = std::filesystem::path(localappdata) / "yams" / "config.toml";
    } else if (const char* appdata = std::getenv("APPDATA")) {
        cfgPath = std::filesystem::path(appdata) / "yams" / "config.toml";
    }
#else
    // Unix: ~/.config/yams/config.toml
    auto cfgHome = std::getenv("XDG_CONFIG_HOME");
    if (cfgHome && *cfgHome) {
        cfgPath = std::filesystem::path(cfgHome) / "yams" / "config.toml";
    } else if (const char* home = std::getenv("HOME")) {
        cfgPath = std::filesystem::path(home) / ".config" / "yams" / "config.toml";
    }
#endif
    if (!cfgPath.empty() && std::filesystem::exists(cfgPath)) {
        // Naive parse: look for a line with 'data_dir = "..."' (TOML)
        std::ifstream in(cfgPath);
        std::string line;
        while (std::getline(in, line)) {
            auto pos = line.find("data_dir");
            if (pos == std::string::npos)
                continue;
            auto q1 = line.find('"', pos);
            if (q1 == std::string::npos)
                continue;
            auto q2 = line.find('"', q1 + 1);
            if (q2 == std::string::npos || q2 <= q1 + 1)
                continue;
            auto val = line.substr(q1 + 1, q2 - (q1 + 1));
            std::filesystem::path dataDir = val;
            // Expand ~ (Unix) or %USERPROFILE% style paths
            if (!val.empty() && val[0] == '~') {
#ifdef _WIN32
                if (const char* userprofile = std::getenv("USERPROFILE")) {
                    std::string rest = val.size() > 2 ? val.substr(2) : std::string();
                    dataDir = std::filesystem::path(userprofile) / rest;
                }
#else
                if (const char* home = std::getenv("HOME")) {
                    std::string rest = val.size() > 2 ? val.substr(2) : std::string();
                    dataDir = std::filesystem::path(home) / rest;
                }
#endif
            }
            paths.emplace_back(dataDir / "grammars");
            break;
        }
    }

#ifdef _WIN32
    // Windows paths: LOCALAPPDATA/yams/grammars (primary)
    if (const char* localappdata = std::getenv("LOCALAPPDATA")) {
        if (*localappdata) {
            paths.emplace_back(std::filesystem::path(localappdata) / "yams" / "grammars");
        }
    }
    // Fallback to APPDATA
    if (const char* appdata = std::getenv("APPDATA")) {
        if (*appdata) {
            paths.emplace_back(std::filesystem::path(appdata) / "yams" / "grammars");
        }
    }
    // User profile fallback
    if (const char* userprofile = std::getenv("USERPROFILE")) {
        if (*userprofile) {
            paths.emplace_back(std::filesystem::path(userprofile) / ".yams" / "grammars");
        }
    }
    // Program installation paths
    if (const char* localappdata = std::getenv("LOCALAPPDATA")) {
        if (*localappdata) {
            paths.emplace_back(std::filesystem::path(localappdata) / "Programs" / "yams" / "lib" /
                               "yams" / "grammars");
        }
    }
#else
    // XDG standard locations (Unix/Linux/macOS)
    if (const char* xdg_data_home = std::getenv("XDG_DATA_HOME")) {
        if (*xdg_data_home) {
            paths.emplace_back(std::filesystem::path(xdg_data_home) / "yams" / "grammars");
        }
    }

    // Default user path
    if (const char* home = std::getenv("HOME")) {
        paths.emplace_back(std::filesystem::path(home) / ".local" / "share" / "yams" / "grammars");
    }

    // System-wide locations
    paths.emplace_back("/usr/local/share/yams/grammars");
    paths.emplace_back("/usr/share/yams/grammars");
#endif

    return paths;
}

std::vector<std::string> GrammarLoader::getLibraryCandidates(std::string_view language) const {
    const auto* spec = findSpec(language);
    if (!spec) {
        return {};
    }

    std::vector<std::string> candidates;
    auto search_paths = getGrammarSearchPaths();

    // Extract the base name from spec (e.g., "tree-sitter-cpp" from "libtree-sitter-cpp.so")
    std::string spec_name(spec->default_so);

    // Remove extension
    auto dot_pos = spec_name.rfind('.');
    if (dot_pos != std::string::npos) {
        spec_name = spec_name.substr(0, dot_pos);
    }

    // Remove "lib" prefix if present to get the core name
    std::string core_name = spec_name;
    if (core_name.rfind("lib", 0) == 0) {
        core_name = core_name.substr(3);
    }

    // Generate all library name variants for this platform
    std::vector<std::string> lib_names;

#ifdef _WIN32
    // Windows: typically no "lib" prefix, .dll extension
    // Primary: tree-sitter-cpp.dll
    lib_names.push_back(core_name + ".dll");
    // Also try with lib prefix: libtree-sitter-cpp.dll
    lib_names.push_back("lib" + core_name + ".dll");
    // Underscore variants
    std::string underscore_name = core_name;
    std::replace(underscore_name.begin(), underscore_name.end(), '-', '_');
    lib_names.push_back(underscore_name + ".dll");
    lib_names.push_back("lib" + underscore_name + ".dll");
#elif defined(__APPLE__)
    // macOS: typically "lib" prefix, .dylib extension
    // Primary: libtree-sitter-cpp.dylib
    lib_names.push_back("lib" + core_name + ".dylib");
    // Also try without lib prefix
    lib_names.push_back(core_name + ".dylib");
    // Try .so extension (some builds use this)
    lib_names.push_back("lib" + core_name + ".so");
    lib_names.push_back(core_name + ".so");
    // Underscore variants
    std::string underscore_name = core_name;
    std::replace(underscore_name.begin(), underscore_name.end(), '-', '_');
    lib_names.push_back("lib" + underscore_name + ".dylib");
    lib_names.push_back(underscore_name + ".dylib");
#else
    // Linux: typically "lib" prefix, .so extension
    // Primary: libtree-sitter-cpp.so
    lib_names.push_back("lib" + core_name + ".so");
    // Also try without lib prefix
    lib_names.push_back(core_name + ".so");
    // Underscore variants
    std::string underscore_name = core_name;
    std::replace(underscore_name.begin(), underscore_name.end(), '-', '_');
    lib_names.push_back("lib" + underscore_name + ".so");
    lib_names.push_back(underscore_name + ".so");
#endif

    // For each search path, add all library name variants
    for (const auto& base_path : search_paths) {
        if (!std::filesystem::exists(base_path))
            continue;

        for (const auto& lib_name : lib_names) {
            candidates.push_back((base_path / lib_name).string());
        }
    }

    return candidates;
}

tl::expected<GrammarLoader::GrammarHandle, GrammarLoadError>
GrammarLoader::loadGrammar(std::string_view language) {
    const auto* spec = findSpec(language);
    if (!spec) {
        return tl::unexpected(GrammarLoadError{
            GrammarLoadError::NOT_FOUND, std::format("Language '{}' not supported", language)});
    }

    auto candidates = getLibraryCandidates(language);
    if (candidates.empty()) {
        return tl::unexpected(
            GrammarLoadError{GrammarLoadError::NOT_FOUND,
                             std::format("No library candidates for language '{}'", language)});
    }

    // Try each candidate
    std::vector<std::string> tried_paths;
    for (const auto& candidate : candidates) {
        tried_paths.push_back(candidate);

        std::fprintf(stderr, "[yams] trying grammar candidate: %s\n", candidate.c_str());
        void* handle = dlopen(candidate.c_str(), RTLD_LAZY | RTLD_LOCAL);
        if (!handle) {
            std::fprintf(stderr, "[yams] dlopen failed: %s\n", dlerror());
            continue; // Try next candidate
        }

        // Try to get the language factory function
        auto* factory_fn = reinterpret_cast<TSLanguage* (*)()>(dlsym(handle, spec->symbol.data()));
        if (factory_fn) {
            // Success! Create language and return
            TSLanguage* lang = factory_fn();
            if (lang) {
                return std::make_pair(handle, lang);
            }
        }

        // Factory function failed, close handle and continue
        dlclose(handle);
    }

    // All candidates failed
    std::string tried_join;
    for (size_t i = 0; i < tried_paths.size(); ++i) {
        tried_join += tried_paths[i];
        if (i + 1 < tried_paths.size())
            tried_join += ", ";
    }
    return tl::unexpected(GrammarLoadError{
        GrammarLoadError::LOAD_FAILED,
        std::format("Failed to load grammar for '{}'. Tried: {}", language, tried_join)});
}

bool GrammarLoader::grammarExists(std::string_view language) const {
    const auto* spec = findSpec(language);
    if (!spec)
        return false;

    auto candidates = getLibraryCandidates(language);
    for (const auto& candidate : candidates) {
        if (std::filesystem::exists(candidate)) {
            // Quick check if it's loadable
            void* handle = dlopen(candidate.c_str(), RTLD_LAZY | RTLD_LOCAL);
            if (handle) {
                auto* factory_fn =
                    reinterpret_cast<TSLanguage* (*)()>(dlsym(handle, spec->symbol.data()));
                bool ok = (factory_fn != nullptr);
                dlclose(handle);
                return ok;
            }
        }
    }
    return false;
}

// GrammarDownloader Implementation
tl::expected<std::filesystem::path, std::string>
GrammarDownloader::downloadGrammar(std::string_view language) {
    if (!canAutoDownload()) {
        return tl::unexpected(std::string{"Auto-download tools not available (git, gcc required)"});
    }

    // Find grammar repository
    auto* repo_info =
        std::find_if(std::begin(kGrammarRepos), std::end(kGrammarRepos),
                     [language](const auto& repo) { return repo.language == language; });

    if (repo_info == std::end(kGrammarRepos)) {
        return tl::unexpected(std::format("Language '{}' auto-download not supported", language));
    }

    auto grammar_path =
        GrammarLoader{}.getGrammarSearchPaths()[0]; // Use primary user path (XDG or ~/.local/share)
    std::filesystem::create_directories(grammar_path);

    // Create temp directory for build
    auto temp_dir =
        std::filesystem::temp_directory_path() / std::format("yams-grammar-build-{}", language);
    std::filesystem::create_directories(temp_dir);

    // Cleanup guard
    auto cleanup = [&temp_dir]() {
        try {
            std::filesystem::remove_all(temp_dir);
        } catch (...) {
        }
    };

    try {
        // Clone repository
        std::string clone_cmd =
            std::format("cd {} && git clone --depth 1 https://github.com/{} tree-sitter-{}",
                        temp_dir.string(), repo_info->repo, language);

        std::cout << "🔄 Cloning grammar repository..." << std::endl;
        if (std::system(clone_cmd.c_str()) != 0) {
            cleanup();
            return tl::unexpected(std::string{"Failed to clone grammar repository"});
        }

        // Build grammar (portable)
        auto build_dir = temp_dir / std::format("tree-sitter-{}", language);

        // Special handling for TypeScript which has subdirectories
        if (language == "typescript") {
            build_dir = build_dir / "typescript";
        }

        auto have = [](const char* tool) {
#ifdef _WIN32
            std::string cmd = std::string("where ") + tool + " > NUL 2>&1";
#else
            std::string cmd = std::string("which ") + tool + " > /dev/null 2>&1";
#endif
            return std::system(cmd.c_str()) == 0;
        };
        std::string cxx =
            have("g++") ? "g++" : (have("c++") ? "c++" : (have("clang++") ? "clang++" : ""));
        std::string cc = have("gcc") ? "gcc" : (have("cc") ? "cc" : (have("clang") ? "clang" : ""));

        auto parser_c = build_dir / "src" / "parser.c";
        auto scanner_c = build_dir / "src" / "scanner.c";
        auto scanner_cc = build_dir / "src" / "scanner.cc";

        bool use_cxx = std::filesystem::exists(scanner_cc);
        std::string compiler = use_cxx ? (cxx.empty() ? cc : cxx) : (cc.empty() ? cxx : cc);
        if (compiler.empty()) {
            cleanup();
            return tl::unexpected(std::string{"No suitable compiler found"});
        }

#ifdef __APPLE__
        std::string lib_name = std::format("libtree-sitter-{}.dylib", language);
        std::string flags = "-dynamiclib";
#elif defined(_WIN32)
        std::string lib_name = std::format("tree-sitter-{}.dll", language);
        std::string flags = "-shared";
#else
        std::string lib_name = std::format("libtree-sitter-{}.so", language);
        std::string flags = "-shared -fPIC";
#endif

        // Find tree-sitter include directory
        std::vector<std::string> include_search;
#ifdef _WIN32
        // Windows: check vcpkg, conan, and common install locations
        if (const char* vcpkg_root = std::getenv("VCPKG_ROOT")) {
            include_search.push_back(std::string(vcpkg_root) + "/installed/x64-windows/include");
        }
        if (const char* localappdata = std::getenv("LOCALAPPDATA")) {
            include_search.push_back(std::string(localappdata) + "/yams/include");
        }
#else
        include_search = {"/usr/local/include", "/usr/include", "/opt/homebrew/include"};
#endif

        // Add conan tree-sitter paths (works on all platforms)
#ifdef _WIN32
        const char* home = std::getenv("USERPROFILE");
#else
        const char* home = std::getenv("HOME");
#endif
        if (home) {
            std::string conan_base = std::string(home) + "/.conan2/p/b";
            if (std::filesystem::exists(conan_base)) {
                try {
                    for (const auto& entry : std::filesystem::directory_iterator(conan_base)) {
                        if (entry.is_directory()) {
                            std::string dirname = entry.path().filename().string();
                            if (dirname.find("tree-") == 0) { // starts_with for C++20
                                include_search.push_back(entry.path().string() + "/p/include");
                            }
                        }
                    }
                } catch (...) {
                }
            }
        }

        std::string ts_include;
        for (const auto& path : include_search) {
            auto ts_header = std::filesystem::path(path) / "tree_sitter" / "api.h";
            if (std::filesystem::exists(ts_header)) {
                ts_include = " -I" + path;
                std::fprintf(stderr, "[yams] Found tree-sitter headers at: %s\n", path.c_str());
                break;
            }
        }

        if (ts_include.empty()) {
            std::fprintf(stderr, "[yams] Warning: tree-sitter headers not found, build may fail\n");
        }

        std::ostringstream cmd;
        cmd << "cd " << build_dir.string() << " && " << compiler << ' ' << flags << " -I."
            << ts_include << " " << parser_c.string() << ' ';
        if (std::filesystem::exists(scanner_c)) {
            cmd << scanner_c.string() << ' ';
        } else if (std::filesystem::exists(scanner_cc)) {
            cmd << scanner_cc.string() << ' ';
        }
        cmd << "-o " << lib_name;

        std::fprintf(stderr, "[yams] Build command: %s\n", cmd.str().c_str());

        std::cout << "🛠️  Building grammar..." << std::endl;
        if (std::system(cmd.str().c_str()) != 0) {
            cleanup();
            return tl::unexpected(std::string{"Failed to build grammar"});
        }

        // Find built library
#ifdef __APPLE__
        auto built_lib = build_dir / std::format("libtree-sitter-{}.dylib", language);
#elif defined(_WIN32)
        auto built_lib = build_dir / std::format("tree-sitter-{}.dll", language);
#else
        auto built_lib = build_dir / std::format("libtree-sitter-{}.so", language);
#endif

        if (!std::filesystem::exists(built_lib)) {
            cleanup();
            return tl::unexpected(std::string{"Built library not found"});
        }

        // Copy to final location
        auto final_path = grammar_path / lib_name;
        std::filesystem::copy_file(built_lib, final_path,
                                   std::filesystem::copy_options::overwrite_existing);

        cleanup();

        std::cout << "✅ Grammar downloaded successfully to: " << final_path << std::endl;
        return final_path;

    } catch (const std::exception& e) {
        cleanup();
        return tl::unexpected(std::string{"Download failed: "} + e.what());
    }
}

bool GrammarDownloader::canAutoDownload() {
    // Check for required tools: git and at least one compiler
    auto have = [](const char* tool) {
#ifdef _WIN32
        std::string cmd = std::string("where ") + tool + " > NUL 2>&1";
#else
        std::string cmd = std::string("which ") + tool + " > /dev/null 2>&1";
#endif
        return std::system(cmd.c_str()) == 0;
    };
    bool has_git = have("git");
    bool has_cc =
        have("g++") || have("gcc") || have("clang++") || have("clang") || have("c++") || have("cc");
    return has_git && has_cc;
}

} // namespace yams::plugins::treesitter