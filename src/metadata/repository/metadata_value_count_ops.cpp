#include "metadata_value_count_ops.hpp"

#include "document_query_filters.hpp"

namespace yams::metadata::repository {

Result<std::unordered_map<std::string, std::vector<MetadataValueCount>>>
runMetadataValueCountQuery(Database& db, const std::vector<std::string>& keys,
                           const DocumentQueryOptions& options, bool needsDocumentJoin,
                           bool joinFtsForContains, bool hasPathIndexing, std::string* sqlOut) {
    std::string sql;
    if (needsDocumentJoin) {
        sql = "SELECT m.key, m.value, COUNT(*) FROM metadata m "
              "JOIN documents d ON d.id = m.document_id";
        if (joinFtsForContains) {
            sql += " JOIN documents_path_fts ON d.id = documents_path_fts.rowid";
        }
    } else {
        sql = "SELECT m.key, m.value, COUNT(*) FROM metadata m";
    }

    std::vector<std::string> conditions;
    std::vector<BindParam> params;

    std::string inList;
    inList.reserve(keys.size() * 2);
    inList += '(';
    for (size_t i = 0; i < keys.size(); ++i) {
        if (i > 0)
            inList += ',';
        inList += '?';
    }
    inList += ')';
    conditions.emplace_back("m.key IN " + inList);
    for (const auto& key : keys) {
        addTextParam(params, key);
    }
    conditions.emplace_back("m.value != ''");

    if (needsDocumentJoin) {
        appendMetadataValueCountDocumentFilters(options, joinFtsForContains, hasPathIndexing,
                                                conditions, params);
    }

    if (!conditions.empty()) {
        sql += " WHERE ";
        for (size_t i = 0; i < conditions.size(); ++i) {
            if (i > 0)
                sql += " AND ";
            sql += conditions[i];
        }
    }

    sql += " GROUP BY m.key, m.value";
    sql += " ORDER BY COUNT(*) DESC, m.value ASC";

    if (sqlOut) {
        *sqlOut = sql;
    }

    auto stmtResult = db.prepare(sql);
    if (!stmtResult) {
        return stmtResult.error();
    }

    Statement stmt = std::move(stmtResult).value();
    int index = 1;
    for (const auto& param : params) {
        Result<void> bindResult;
        if (param.type == BindParam::Type::Text) {
            bindResult = stmt.bind(index, param.text);
        } else {
            bindResult = stmt.bind(index, param.integer);
        }
        if (!bindResult) {
            return bindResult.error();
        }
        ++index;
    }

    std::unordered_map<std::string, std::vector<MetadataValueCount>> result;
    while (true) {
        auto stepResult = stmt.step();
        if (!stepResult) {
            return stepResult.error();
        }
        if (!stepResult.value()) {
            break;
        }

        const std::string key = stmt.getString(0);
        MetadataValueCount row;
        row.value = stmt.getString(1);
        row.count = stmt.getInt64(2);
        result[key].push_back(std::move(row));
    }

    return result;
}

} // namespace yams::metadata::repository
