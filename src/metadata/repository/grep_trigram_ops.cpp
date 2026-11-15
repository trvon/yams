// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors
//
// Grep trigram index (migration v45): per-document trigram sets of the bytes grep scans.

#include <spdlog/spdlog.h>

#include <yams/common/utf8_utils.h>
#include <yams/metadata/metadata_repository.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <optional>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "result_helpers.hpp"
#include "transaction_helpers.hpp"

namespace yams::metadata {

using repository::beginTransactionWithRetry;
using repository::commitOrRollback;
using repository::rollbackIgnoringErrors;
using repository::scope_exit;

namespace {

// Length in bytes of the UTF-8 sequence starting with `lead`; 1 for anything malformed, which
// the caller has already replaced with valid text.
std::size_t utf8SequenceLength(unsigned char lead) {
    if (lead < 0x80)
        return 1;
    if ((lead >> 5) == 0x6)
        return 2;
    if ((lead >> 4) == 0xE)
        return 3;
    if ((lead >> 3) == 0x1E)
        return 4;
    return 1;
}

// The FTS5 trigram tokenizer works on characters, so split the literal the same way.
void appendCharacterTrigrams(std::string_view text, std::set<std::string>& out) {
    std::vector<std::string_view> chars;
    for (std::size_t i = 0; i < text.size();) {
        const auto len =
            std::min(utf8SequenceLength(static_cast<unsigned char>(text[i])), text.size() - i);
        chars.push_back(text.substr(i, len));
        i += len;
    }
    for (std::size_t i = 0; i + 3 <= chars.size(); ++i) {
        std::string gram;
        gram.reserve(12);
        gram.append(chars[i]).append(chars[i + 1]).append(chars[i + 2]);
        out.insert(std::move(gram));
    }
}

std::string quoteFts5Term(std::string_view term) {
    std::string quoted = "\"";
    for (const char c : term) {
        if (c == '"')
            quoted += '"';
        quoted += c;
    }
    quoted += '"';
    return quoted;
}

} // namespace

bool MetadataRepository::grepTrigramIndexAvailable() {
    const int known = grepTrigramAvailable_.load(std::memory_order_acquire);
    if (known >= 0)
        return known == 1;
    auto present = executeReadQuery<bool>([&](Database& db) -> Result<bool> {
        YAMS_TRY_UNWRAP(stmt, db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND "
                                         "name = 'grep_trigram_fts'"));
        YAMS_TRY_UNWRAP(row, stmt.step());
        return row;
    });
    const bool available = present && present.value();
    grepTrigramAvailable_.store(available ? 1 : 0, std::memory_order_release);
    return available;
}

Result<void> MetadataRepository::indexGrepTrigrams(int64_t documentId, std::string_view sha256Hash,
                                                   std::string_view bytes) {
    if (!grepTrigramIndexAvailable())
        return Error{ErrorCode::NotSupported, "grep trigram index unavailable"};
    // The tokenizer needs UTF-8. Invalid bytes become U+FFFD; a valid literal cannot span one,
    // so no trigram a literal could need is lost.
    std::string storage;
    const auto text = common::ensureValidUtf8(bytes, storage);
    const auto now = std::chrono::duration_cast<std::chrono::seconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();
    return executeQuery<void>([&](Database& db) -> Result<void> {
        YAMS_TRY(beginTransactionWithRetry(db));
        auto rollback = scope_exit([&] { rollbackIgnoringErrors(db); });
        YAMS_TRY_UNWRAP(del, db.prepare("DELETE FROM grep_trigram_fts WHERE rowid = ?"));
        YAMS_TRY(del.bind(1, documentId));
        YAMS_TRY(del.execute());
        YAMS_TRY_UNWRAP(ins, db.prepare("INSERT INTO grep_trigram_fts(rowid, body) VALUES (?, ?)"));
        YAMS_TRY(ins.bind(1, documentId));
        YAMS_TRY(ins.bind(2, text));
        YAMS_TRY(ins.execute());
        YAMS_TRY_UNWRAP(cov,
                        db.prepare("INSERT OR REPLACE INTO grep_trigram_coverage "
                                   "(document_id, sha256_hash, indexed_time) VALUES (?, ?, ?)"));
        YAMS_TRY(cov.bind(1, documentId));
        YAMS_TRY(cov.bind(2, sha256Hash));
        YAMS_TRY(cov.bind(3, static_cast<int64_t>(now)));
        YAMS_TRY(cov.execute());
        YAMS_TRY(commitOrRollback(db));
        rollback.dismiss();
        return Result<void>();
    });
}

Result<void> MetadataRepository::removeGrepTrigrams(int64_t documentId) {
    if (!grepTrigramIndexAvailable())
        return Result<void>();
    return executeQuery<void>([&](Database& db) -> Result<void> {
        YAMS_TRY(beginTransactionWithRetry(db));
        auto rollback = scope_exit([&] { rollbackIgnoringErrors(db); });
        YAMS_TRY_UNWRAP(del, db.prepare("DELETE FROM grep_trigram_fts WHERE rowid = ?"));
        YAMS_TRY(del.bind(1, documentId));
        YAMS_TRY(del.execute());
        YAMS_TRY_UNWRAP(cov, db.prepare("DELETE FROM grep_trigram_coverage WHERE document_id = ?"));
        YAMS_TRY(cov.bind(1, documentId));
        YAMS_TRY(cov.execute());
        YAMS_TRY(commitOrRollback(db));
        rollback.dismiss();
        return Result<void>();
    });
}

Result<std::unordered_map<int64_t, std::string>> MetadataRepository::grepTrigramCoverage() {
    if (!grepTrigramIndexAvailable())
        return std::unordered_map<int64_t, std::string>{};
    return executeReadQuery<std::unordered_map<int64_t, std::string>>(
        [&](Database& db) -> Result<std::unordered_map<int64_t, std::string>> {
            YAMS_TRY_UNWRAP(stmt, db.prepare("SELECT document_id, sha256_hash FROM "
                                             "grep_trigram_coverage"));
            std::unordered_map<int64_t, std::string> coverage;
            while (true) {
                YAMS_TRY_UNWRAP(row, stmt.step());
                if (!row)
                    break;
                coverage.emplace(stmt.getInt64(0), stmt.getString(1));
            }
            return coverage;
        });
}

Result<std::optional<std::vector<int64_t>>>
MetadataRepository::grepTrigramMatches(std::span<const std::string> literals) {
    std::set<std::string> grams;
    for (const auto& literal : literals) {
        std::string storage;
        appendCharacterTrigrams(common::ensureValidUtf8(literal, storage), grams);
    }
    if (grams.empty() || !grepTrigramIndexAvailable())
        return std::optional<std::vector<int64_t>>{};
    std::string expression;
    for (const auto& gram : grams) {
        if (!expression.empty())
            expression += " AND ";
        expression += quoteFts5Term(gram);
    }
    return executeReadQuery<std::optional<std::vector<int64_t>>>(
        [&](Database& db) -> Result<std::optional<std::vector<int64_t>>> {
            YAMS_TRY_UNWRAP(stmt, db.prepare("SELECT rowid FROM grep_trigram_fts WHERE "
                                             "grep_trigram_fts MATCH ?"));
            YAMS_TRY(stmt.bind(1, expression));
            std::vector<int64_t> ids;
            while (true) {
                YAMS_TRY_UNWRAP(row, stmt.step());
                if (!row)
                    break;
                ids.push_back(stmt.getInt64(0));
            }
            return std::optional<std::vector<int64_t>>{std::move(ids)};
        });
}

} // namespace yams::metadata
