#pragma once

#include <cstring>
#include <string>
#include <utility>
#include <vector>

#include <sqlite3.h>

#include <simeon/pq.hpp>

#include "simeon_pq_store_codec.h"

namespace yams::vector::detail {

inline bool hasPersistedSimeonPqMeta(sqlite3* db, std::size_t dim) {
    sqlite3_stmt* stmt = nullptr;
    const char* sql = "SELECT EXISTS(SELECT 1 FROM simeon_pq_meta WHERE dim = ?)";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
        return false;
    }
    sqlite3_bind_int64(stmt, 1, static_cast<sqlite3_int64>(dim));
    bool exists = false;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        exists = sqlite3_column_int(stmt, 0) != 0;
    }
    sqlite3_finalize(stmt);
    return exists;
}

inline bool loadPersistedSimeonPqMeta(sqlite3* db, std::size_t dim, std::uint32_t& m,
                                      std::uint32_t& k, std::uint64_t& seed,
                                      std::size_t& rerankFactor, bool& trained,
                                      std::size_t& vectorCount, std::vector<float>& codebooks) {
    sqlite3_stmt* stmt = nullptr;
    const char* sql = "SELECT m, k, seed, rerank_factor, trained, vector_count, codebooks_blob "
                      "FROM simeon_pq_meta WHERE dim = ?";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
        return false;
    }
    sqlite3_bind_int64(stmt, 1, static_cast<sqlite3_int64>(dim));
    if (sqlite3_step(stmt) != SQLITE_ROW) {
        sqlite3_finalize(stmt);
        return false;
    }

    m = static_cast<std::uint32_t>(sqlite3_column_int(stmt, 0));
    k = static_cast<std::uint32_t>(sqlite3_column_int(stmt, 1));
    seed = static_cast<std::uint64_t>(sqlite3_column_int64(stmt, 2));
    rerankFactor = static_cast<std::size_t>(sqlite3_column_int64(stmt, 3));
    const bool storedTrained = sqlite3_column_int(stmt, 4) != 0;
    vectorCount = static_cast<std::size_t>(sqlite3_column_int64(stmt, 5));
    const void* blobPtr = sqlite3_column_blob(stmt, 6);
    const int blobSize = sqlite3_column_bytes(stmt, 6);
    std::vector<std::uint8_t> blob;
    if (blobPtr && blobSize > 0) {
        blob.resize(static_cast<std::size_t>(blobSize));
        std::memcpy(blob.data(), blobPtr, blob.size());
    }
    sqlite3_finalize(stmt);

    trained = storedTrained;
    return deserializeSimeonPqCodebooks(blob, static_cast<std::uint32_t>(dim), m, k, trained,
                                        codebooks);
}

inline bool loadPersistedSimeonPqCodes(sqlite3* db, std::size_t dim, std::size_t m,
                                       std::vector<std::size_t>& rowids,
                                       std::vector<std::uint8_t>& codes) {
    sqlite3_stmt* stmt = nullptr;
    const char* sql = "SELECT rowid, code FROM simeon_pq_codes WHERE dim = ? ORDER BY rowid";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
        return false;
    }
    sqlite3_bind_int64(stmt, 1, static_cast<sqlite3_int64>(dim));

    rowids.clear();
    codes.clear();
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const auto rowid = static_cast<std::size_t>(sqlite3_column_int64(stmt, 0));
        const void* blobPtr = sqlite3_column_blob(stmt, 1);
        const int blobSize = sqlite3_column_bytes(stmt, 1);
        if (!blobPtr || blobSize != static_cast<int>(m)) {
            sqlite3_finalize(stmt);
            rowids.clear();
            codes.clear();
            return false;
        }
        rowids.push_back(rowid);
        const auto* bytes = static_cast<const std::uint8_t*>(blobPtr);
        codes.insert(codes.end(), bytes, bytes + blobSize);
    }
    sqlite3_finalize(stmt);
    return !rowids.empty() && codes.size() == rowids.size() * m;
}

inline bool savePersistedSimeonPq(sqlite3* db, std::size_t dim, const simeon::ProductQuantizer& pq,
                                  std::uint64_t seed, std::size_t rerankFactor,
                                  std::span<const std::size_t> rowids,
                                  std::span<const std::uint8_t> codes, std::string& errorMessage) {
    sqlite3_stmt* metaStmt = nullptr;
    const char* metaSql =
        "INSERT OR REPLACE INTO simeon_pq_meta "
        "(dim, format_version, m, k, seed, rerank_factor, trained, vector_count, codebooks_blob, "
        "updated_at) VALUES (?, 1, ?, ?, ?, ?, ?, ?, ?, unixepoch())";
    if (sqlite3_prepare_v2(db, metaSql, -1, &metaStmt, nullptr) != SQLITE_OK) {
        errorMessage = "failed to prepare simeon_pq_meta upsert";
        return false;
    }
    auto codebookBlob =
        serializeSimeonPqCodebooks(pq.dim(), pq.m(), pq.k(), pq.is_trained(), pq.codebooks());
    sqlite3_bind_int64(metaStmt, 1, static_cast<sqlite3_int64>(dim));
    sqlite3_bind_int(metaStmt, 2, static_cast<int>(pq.m()));
    sqlite3_bind_int(metaStmt, 3, static_cast<int>(pq.k()));
    sqlite3_bind_int64(metaStmt, 4, static_cast<sqlite3_int64>(seed));
    sqlite3_bind_int64(metaStmt, 5, static_cast<sqlite3_int64>(rerankFactor));
    sqlite3_bind_int(metaStmt, 6, pq.is_trained() ? 1 : 0);
    sqlite3_bind_int64(metaStmt, 7, static_cast<sqlite3_int64>(rowids.size()));
    sqlite3_bind_blob(metaStmt, 8, codebookBlob.data(), static_cast<int>(codebookBlob.size()),
                      SQLITE_TRANSIENT);
    if (sqlite3_step(metaStmt) != SQLITE_DONE) {
        sqlite3_finalize(metaStmt);
        errorMessage = "failed to persist simeon_pq_meta";
        return false;
    }
    sqlite3_finalize(metaStmt);

    if (sqlite3_exec(db, "BEGIN IMMEDIATE", nullptr, nullptr, nullptr) != SQLITE_OK) {
        errorMessage = "failed to begin simeon_pq_codes transaction";
        return false;
    }
    const std::string deleteSql = "DELETE FROM simeon_pq_codes WHERE dim = " + std::to_string(dim);
    if (sqlite3_exec(db, deleteSql.c_str(), nullptr, nullptr, nullptr) != SQLITE_OK) {
        sqlite3_exec(db, "ROLLBACK", nullptr, nullptr, nullptr);
        errorMessage = "failed to clear simeon_pq_codes";
        return false;
    }

    sqlite3_stmt* codeStmt = nullptr;
    const char* codeSql = "INSERT INTO simeon_pq_codes (dim, rowid, code) VALUES (?, ?, ?)";
    if (sqlite3_prepare_v2(db, codeSql, -1, &codeStmt, nullptr) != SQLITE_OK) {
        sqlite3_exec(db, "ROLLBACK", nullptr, nullptr, nullptr);
        errorMessage = "failed to prepare simeon_pq_codes insert";
        return false;
    }
    const std::size_t m = pq.m();
    for (std::size_t i = 0; i < rowids.size(); ++i) {
        sqlite3_reset(codeStmt);
        sqlite3_bind_int64(codeStmt, 1, static_cast<sqlite3_int64>(dim));
        sqlite3_bind_int64(codeStmt, 2, static_cast<sqlite3_int64>(rowids[i]));
        sqlite3_bind_blob(codeStmt, 3, codes.data() + (i * m), static_cast<int>(m),
                          SQLITE_TRANSIENT);
        if (sqlite3_step(codeStmt) != SQLITE_DONE) {
            sqlite3_finalize(codeStmt);
            sqlite3_exec(db, "ROLLBACK", nullptr, nullptr, nullptr);
            errorMessage = "failed to persist simeon_pq_codes";
            return false;
        }
    }
    sqlite3_finalize(codeStmt);
    if (sqlite3_exec(db, "COMMIT", nullptr, nullptr, nullptr) != SQLITE_OK) {
        sqlite3_exec(db, "ROLLBACK", nullptr, nullptr, nullptr);
        errorMessage = "failed to commit simeon_pq_codes";
        return false;
    }
    return true;
}

} // namespace yams::vector::detail
