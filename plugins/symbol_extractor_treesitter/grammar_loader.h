#pragma once

#include <expected>
#include <unordered_map>

#include <filesystem>
#include <span>
#include <string>
#include <string_view>
#include <vector>

// Forward declaration
struct TSLanguage;

namespace yams::plugins::treesitter {

struct GrammarLoadError {
    enum Type { NOT_FOUND, LOAD_FAILED, INVALID_SYMBOL };
    Type type;
    std::string message;
};

/**
 * @brief Manages Tree-sitter grammar discovery and loading
 *
 * Follows XDG Base Directory Specification for grammar paths:
 * - XDG_DATA_HOME/yams/grammars (primary)
 * - ~/.local/share/yams/grammars (fallback)
 * - /usr/local/share/yams/grammars
 * - /usr/share/yams/grammars
 *
 * Plus environment variable overrides and config paths.
 */
class GrammarLoader {
public:
    GrammarLoader();

    /**
     * @brief Add custom grammar path override from plugin config
     */
    void addGrammarPath(std::string_view language, std::string_view path);

    /**
     * @brief Load grammar library for a language
     * @return Language factory function or error
     */
    using GrammarHandle = std::pair<void*, TSLanguage*>;
    std::expected<GrammarHandle, GrammarLoadError> loadGrammar(std::string_view language);

    /**
     * @brief Get all search paths that will be checked
     */
    std::vector<std::filesystem::path> getGrammarSearchPaths() const;

    /**
     * @brief Check if grammar is available (without loading)
     */
    bool grammarExists(std::string_view language) const;

private:
    struct GrammarSpec {
        std::string_view key;
        std::string_view env_var;
        std::string_view symbol;
        std::string_view default_so;
    };

    static constexpr GrammarSpec kSpecs[] = {
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

    const GrammarSpec* findSpec(std::string_view language) const;
    std::vector<std::string> getLibraryCandidates(std::string_view language) const;

    std::unordered_map<std::string, std::string> grammar_paths_;
    mutable std::vector<std::filesystem::path> cached_search_paths_;
    mutable bool search_paths_cached_ = false;
};

/**
 * @brief Automatically download and build missing grammars
 */
class GrammarDownloader {
public:
    /**
     * @brief Download and build grammar for a language
     * @return Path to built library or error
     */
    static std::expected<std::filesystem::path, std::string>
    downloadGrammar(std::string_view language);

    /**
     * @brief Check if download tools are available (git, gcc)
     */
    static bool canAutoDownload();

private:
    static constexpr struct GrammarRepo {
        std::string_view language;
        std::string_view repo;
        std::string_view build_command;
    } kGrammarRepos[] = {
        {"cpp", "tree-sitter/tree-sitter-cpp",
         "gcc -shared -fPIC -o libtree-sitter-{lang}.so src/parser.c src/scanner.c -I."},
        {"python", "tree-sitter/tree-sitter-python",
         "gcc -shared -fPIC -o libtree-sitter-{lang}.so src/parser.c src/scanner.c -I."},
        {"rust", "tree-sitter/tree-sitter-rust",
         "gcc -shared -fPIC -o libtree-sitter-{lang}.so src/parser.c -I."},
        {"go", "tree-sitter/tree-sitter-go",
         "gcc -shared -fPIC -o libtree-sitter-{lang}.so src/parser.c -I."},
        {"javascript", "tree-sitter/tree-sitter-javascript",
         "gcc -shared -fPIC -o libtree-sitter-{lang}.so src/parser.c -I."},
        {"typescript", "tree-sitter/tree-sitter-typescript",
         "gcc -shared -fPIC -o libtree-sitter-{lang}.so src/parser.c -I."},
        {"java", "tree-sitter/tree-sitter-java",
         "gcc -shared -fPIC -o libtree-sitter-{lang}.so src/parser.c -I."},
        {"c", "tree-sitter/tree-sitter-c",
         "gcc -shared -fPIC -o libtree-sitter-{lang}.so src/parser.c -I."},
    };
};

} // namespace yams::plugins::treesitter