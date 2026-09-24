// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

// FTS5 user-query sanitizer: search text from MCP clients and the CLI. Oracle: every sanitized
// query must execute as a MATCH expression on an FTS5 table shaped like documents_fts, and
// sanitizing the output again must still produce a valid expression.

#include <yams/metadata/database.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

namespace yams::metadata {
std::string sanitizeFts5UserQuery(std::string query, bool allowPrefixWildcard);
} // namespace yams::metadata

namespace {

void fuzzRequire(bool condition) {
    if (!condition) {
        __builtin_trap();
    }
}

yams::metadata::Database& ftsDatabase() {
    static std::unique_ptr<yams::metadata::Database> db = [] {
        auto created = std::make_unique<yams::metadata::Database>();
        fuzzRequire(created->open(":memory:", yams::metadata::ConnectionMode::Memory).has_value());
        fuzzRequire(created
                        ->execute("CREATE VIRTUAL TABLE documents_fts USING fts5(content, title, "
                                  "content_type, tokenize='unicode61 tokenchars ''_-''');"
                                  "INSERT INTO documents_fts VALUES('hello world IL-6 cells', "
                                  "'report', 'text/plain');")
                        .has_value());
        return created;
    }();
    return *db;
}

bool executesAsMatch(const std::string& query) {
    auto stmt =
        ftsDatabase().prepare("SELECT rowid FROM documents_fts WHERE documents_fts MATCH ?");
    if (!stmt) {
        return false;
    }
    if (!stmt.value().bind(1, query)) {
        return false;
    }
    return stmt.value().step().has_value();
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
    if (data == nullptr || size < 1 || size > std::size_t{64} * 1024) {
        return 0;
    }
    const bool allowPrefix = (data[0] & 1U) != 0;
    const std::string input(reinterpret_cast<const char*>(data + 1), size - 1);
    // SQLite binds text up to the first NUL; the sanitizer contract is about query syntax.
    if (input.find('\0') != std::string::npos) {
        return 0;
    }

    const std::string sanitized = yams::metadata::sanitizeFts5UserQuery(input, allowPrefix);
    fuzzRequire(!sanitized.empty());
    fuzzRequire(executesAsMatch(sanitized));

    const std::string again = yams::metadata::sanitizeFts5UserQuery(sanitized, allowPrefix);
    fuzzRequire(executesAsMatch(again));
    return 0;
}
