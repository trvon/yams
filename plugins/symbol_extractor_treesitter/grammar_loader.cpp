#include "grammar_loader.h"
#include <expected>
#include <format>
#include <iostream>
#include <memory>

#include <dlfcn.h>

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

    // XDG standard locations
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

    return paths;
}

std::vector<std::string> GrammarLoader::getLibraryCandidates(std::string_view language) const {
    const auto* spec = findSpec(language);
    if (!spec) {
        return {};
    }

    std::vector<std::string> candidates;

    // For each search path, try different library name variants
    auto search_paths = getGrammarSearchPaths();

    for (const auto& base_path : search_paths) {
        if (!std::filesystem::exists(base_path))
            continue;

        // Standard library name
        auto standard_path = base_path / spec->default_so;
        candidates.push_back(standard_path.string());

        // Alternative naming conventions
        std::string lib_name(spec->default_so);

        // Underscore variant
        if (lib_name.find("tree-sitter-") != std::string::npos) {
            std::string alt_name = lib_name;
            std::replace(alt_name.begin(), alt_name.end(), '-', '_');
            candidates.push_back((base_path / alt_name).string());
        }

        // Without "lib" prefix
        if (lib_name.starts_with("lib")) {
            std::string no_prefix = lib_name.substr(3);
            candidates.push_back((base_path / no_prefix).string());
        }

        // Platform-specific extensions
#ifdef __APPLE__
        candidates.push_back((base_path / (lib_name + ".dylib")).string());
#elif defined(_WIN32)
        candidates.push_back((base_path / (lib_name + ".dll")).string());
#endif
    }

    return candidates;
}

std::expected<GrammarLoader::GrammarHandle, GrammarLoadError>
GrammarLoader::loadGrammar(std::string_view language) {
    const auto* spec = findSpec(language);
    if (!spec) {
        return std::unexpected(GrammarLoadError{
            GrammarLoadError::NOT_FOUND, std::format("Language '{}' not supported", language)});
    }

    auto candidates = getLibraryCandidates(language);
    if (candidates.empty()) {
        return std::unexpected(
            GrammarLoadError{GrammarLoadError::NOT_FOUND,
                             std::format("No library candidates for language '{}'", language)});
    }

    // Try each candidate
    std::vector<std::string> tried_paths;
    for (const auto& candidate : candidates) {
        tried_paths.push_back(candidate);

        void* handle = dlopen(candidate.c_str(), RTLD_LAZY | RTLD_LOCAL);
        if (!handle) {
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
    return std::unexpected(GrammarLoadError{
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
std::expected<std::filesystem::path, std::string>
GrammarDownloader::downloadGrammar(std::string_view language) {
    if (!canAutoDownload()) {
        return std::unexpected(
            std::string{"Auto-download tools not available (git, gcc required)"});
    }

    // Find grammar repository
    auto* repo_info =
        std::find_if(std::begin(kGrammarRepos), std::end(kGrammarRepos),
                     [language](const auto& repo) { return repo.language == language; });

    if (repo_info == std::end(kGrammarRepos)) {
        return std::unexpected(std::format("Language '{}' auto-download not supported", language));
    }

    auto grammar_path = GrammarLoader{}.getGrammarSearchPaths()[0]; // Use primary user path
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
            return std::unexpected(std::string{"Failed to clone grammar repository"});
        }

        // Build grammar
        auto build_dir = temp_dir / std::format("tree-sitter-{}", language);
        std::string build_cmd =
            std::format("cd {} && ", build_dir.string()) +
            std::vformat(repo_info->build_command, std::make_format_args(language));

        std::cout << "🛠️  Building grammar..." << std::endl;
        if (std::system(build_cmd.c_str()) != 0) {
            cleanup();
            return std::unexpected(std::string{"Failed to build grammar"});
        }

        // Find built library
        auto lib_name = std::format("libtree-sitter-{}.so", language);
        auto built_lib = build_dir / lib_name;

        if (!std::filesystem::exists(built_lib)) {
            cleanup();
            return std::unexpected(std::string{"Built library not found"});
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
        return std::unexpected(std::string{"Download failed: "} + e.what());
    }
}

bool GrammarDownloader::canAutoDownload() {
    // Check for required tools
    return (std::system("which git > /dev/null 2>&1") == 0) &&
           (std::system("which gcc > /dev/null 2>&1") == 0);
}

} // namespace yams::plugins::treesitter