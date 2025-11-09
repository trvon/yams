#pragma once

#include <string>
#include <vector>
#include <yams/search/query_ast.h>

namespace yams::search {

/**
 * @brief Extracts literal terms from search QueryAST for pre-filtering
 *
 * Analyzes the query AST to extract the most selective literal terms that can
 * be used for fast pre-filtering before full FTS5 search execution.
 *
 * Example:
 *   Query: "author:john AND SpecificClassName"
 *   Extracts: ["SpecificClassName"] (most selective)
 */
class QueryLiteralExtractor {
public:
    /**
     * @brief Result of literal extraction from query AST
     */
    struct QueryLiterals {
        std::vector<std::string> terms;    ///< All extracted literal terms
        std::string mostSelective;         ///< Longest/most selective term
        float estimatedSelectivity = 1.0f; ///< Estimated match rate [0,1]
        bool hasPureTerms = false;         ///< True if query has simple terms (not fuzzy/wildcard)

        bool empty() const { return terms.empty(); }
        size_t count() const { return terms.size(); }
    };

    /**
     * @brief Extract literals from query AST
     * @param queryAst Root of the query AST
     * @return Extracted literals with selectivity estimation
     */
    static QueryLiterals extract(const QueryNode* queryAst);

private:
    /**
     * @brief Recursively traverse AST and collect terms
     */
    static void collectTerms(const QueryNode* node, std::vector<std::string>& terms,
                             bool& hasPureTerms);

    /**
     * @brief Select most selective term from candidates
     * Prefers: longer terms, uppercase terms, terms with special chars
     */
    static std::string selectMostSelective(const std::vector<std::string>& terms);

    /**
     * @brief Estimate selectivity of a term (0 = very selective, 1 = matches everything)
     */
    static float estimateSelectivity(const std::string& term);

    /**
     * @brief Score a term for selectivity (higher = more selective)
     */
    static int scoreTermSelectivity(const std::string& term);
};

} // namespace yams::search
