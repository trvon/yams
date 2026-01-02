#pragma once

#include <unordered_map>
#include <tl/expected.hpp>

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
    tl::expected<GrammarHandle, GrammarLoadError> loadGrammar(std::string_view language);

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
        {"csharp", "YAMS_TS_CSHARP_LIB", "tree_sitter_c_sharp", "libtree-sitter-c-sharp.so"},
        {"c#", "YAMS_TS_CSHARP_LIB", "tree_sitter_c_sharp", "libtree-sitter-c-sharp.so"},
        {"cs", "YAMS_TS_CSHARP_LIB", "tree_sitter_c_sharp", "libtree-sitter-c-sharp.so"},
        {"php", "YAMS_TS_PHP_LIB", "tree_sitter_php", "libtree-sitter-php.so"},
        {"kotlin", "YAMS_TS_KOTLIN_LIB", "tree_sitter_kotlin", "libtree-sitter-kotlin.so"},
        {"kt", "YAMS_TS_KOTLIN_LIB", "tree_sitter_kotlin", "libtree-sitter-kotlin.so"},
        {"perl", "YAMS_TS_PERL_LIB", "tree_sitter_perl", "libtree-sitter-perl.so"},
        {"pl", "YAMS_TS_PERL_LIB", "tree_sitter_perl", "libtree-sitter-perl.so"},
        {"r", "YAMS_TS_R_LIB", "tree_sitter_r", "libtree-sitter-r.so"},
        {"sql", "YAMS_TS_SQL_LIB", "tree_sitter_sql", "libtree-sitter-sql.so"},
        {"sol", "YAMS_TS_SOL_LIB", "tree_sitter_solidity", "libtree-sitter-solidity.so"},
        {"solidity", "YAMS_TS_SOL_LIB", "tree_sitter_solidity", "libtree-sitter-solidity.so"},
        {"dart", "YAMS_TS_DART_LIB", "tree_sitter_dart", "libtree-sitter-dart.so"},
        {"flutter", "YAMS_TS_DART_LIB", "tree_sitter_dart", "libtree-sitter-dart.so"},
        {"p4", "YAMS_TS_P4_LIB", "tree_sitter_p4", "libtree-sitter-p4.so"},
        {"p4_16", "YAMS_TS_P4_LIB", "tree_sitter_p4", "libtree-sitter-p4.so"},
        {"p4lang", "YAMS_TS_P4_LIB", "tree_sitter_p4", "libtree-sitter-p4.so"},
        {"zig", "YAMS_TS_ZIG_LIB", "tree_sitter_zig", "libtree-sitter-zig.so"},
        {"swift", "YAMS_TS_SWIFT_LIB", "tree_sitter_swift", "libtree-sitter-swift.so"},
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
    static tl::expected<std::filesystem::path, std::string>
    downloadGrammar(std::string_view language);

    /**
     * @brief Check if download tools are available (git, gcc)
     */
    static bool canAutoDownload();

private:
    static constexpr struct GrammarRepo {
        std::string_view language;
        std::string_view repo;
    } kGrammarRepos[] = {
        {"c", "tree-sitter/tree-sitter-c"},
        {"cpp", "tree-sitter/tree-sitter-cpp"},
        {"python", "tree-sitter/tree-sitter-python"},
        {"rust", "tree-sitter/tree-sitter-rust"},
        {"go", "tree-sitter/tree-sitter-go"},
        {"javascript", "tree-sitter/tree-sitter-javascript"},
        {"typescript", "tree-sitter/tree-sitter-typescript"},
        {"java", "tree-sitter/tree-sitter-java"},
        {"csharp", "tree-sitter/tree-sitter-c-sharp"},
        {"php", "tree-sitter/tree-sitter-php"},
        {"kotlin", "fwcd/tree-sitter-kotlin"},
        {"perl", "tree-sitter-perl/tree-sitter-perl"},
        {"r", "r-lib/tree-sitter-r"},
        {"sql", "DerekStride/tree-sitter-sql"},
        {"solidity", "JoranHonig/tree-sitter-solidity"},
        {"dart", "UserNobody14/tree-sitter-dart"},
        {"p4", "prona-p4-learning-platform/tree-sitter-p4"},
        {"zig", "maxxnino/tree-sitter-zig"},
        {"swift", "alex-pinkus/tree-sitter-swift"},
    };
};

} // namespace yams::plugins::treesitter