#pragma once

#include <cstdint>
#include <cstring>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include <sqlite3.h>

#include <simeon/pq.hpp>

#include "simeon_pq_store_codec.h"

namespace yams::vector::detail {

inline constexpr std::uint32_t kSimeonPqPersistenceFormatVersion = 2;

inline bool loadVectorIndexGeneration(sqlite3* db, std::uint64_t& generation) {
    sqlite3_stmt* stmt = nullptr;
    const char* sql = "SELECT generation FROM vector_index_generation WHERE id = 1";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
        return false;
    }
    if (sqlite3_step(stmt) != SQLITE_ROW) {
        sqlite3_finalize(stmt);
        return false;
    }
    const auto stored = sqlite3_column_int64(stmt, 0);
    sqlite3_finalize(stmt);
    if (stored < 0) {
        return false;
    }
    generation = static_cast<std::uint64_t>(stored);
    return true;
}

inline bool loadPersistedSimeonPqMeta(sqlite3* db, std::size_t dim, std::uint32_t& formatVersion,
                                      std::uint32_t& m, std::uint32_t& k, std::uint64_t& seed,
                                      std::size_t& trainLimit, std::uint64_t& sourceGeneration,
                                      std::uint64_t& currentGeneration, std::size_t& rerankFactor,
                                      bool& trained, std::size_t& vectorCount,
                                      std::vector<float>& codebooks) {
    sqlite3_stmt* stmt = nullptr;
    const char* sql =
        "SELECT format_version, m, k, seed, train_limit, source_generation, rerank_factor, "
        "trained, vector_count, codebooks_blob, "
        "(SELECT generation FROM vector_index_generation WHERE id = 1) "
        "FROM simeon_pq_meta WHERE dim = ?";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
        return false;
    }
    sqlite3_bind_int64(stmt, 1, static_cast<sqlite3_int64>(dim));
    if (sqlite3_step(stmt) != SQLITE_ROW) {
        sqlite3_finalize(stmt);
        return false;
    }

    const auto storedTrainLimit = sqlite3_column_int64(stmt, 4);
    const auto storedSourceGeneration = sqlite3_column_int64(stmt, 5);
    const auto liveGeneration = sqlite3_column_int64(stmt, 10);
    const auto storedVectorCount = sqlite3_column_int64(stmt, 8);
    if (storedTrainLimit < 0 || storedSourceGeneration < 0 || liveGeneration < 0 ||
        storedVectorCount < 0) {
        sqlite3_finalize(stmt);
        return false;
    }
    formatVersion = static_cast<std::uint32_t>(sqlite3_column_int(stmt, 0));
    m = static_cast<std::uint32_t>(sqlite3_column_int(stmt, 1));
    k = static_cast<std::uint32_t>(sqlite3_column_int(stmt, 2));
    seed = static_cast<std::uint64_t>(sqlite3_column_int64(stmt, 3));
    trainLimit = static_cast<std::size_t>(storedTrainLimit);
    sourceGeneration = static_cast<std::uint64_t>(storedSourceGeneration);
    currentGeneration = static_cast<std::uint64_t>(liveGeneration);
    rerankFactor = static_cast<std::size_t>(sqlite3_column_int64(stmt, 6));
    const bool storedTrained = sqlite3_column_int(stmt, 7) != 0;
    vectorCount = static_cast<std::size_t>(storedVectorCount);
    const void* blobPtr = sqlite3_column_blob(stmt, 9);
    const int blobSize = sqlite3_column_bytes(stmt, 9);
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

inline bool loadPersistedSimeonPqCodes(sqlite3* db, std::size_t dim, std::size_t m, std::size_t k,
                                       std::vector<std::size_t>& rowids,
                                       std::vector<std::uint8_t>& codes) {
    if (m == 0 || k == 0 || k > 256) {
        return false;
    }
    sqlite3_stmt* stmt = nullptr;
    const char* sql = "SELECT rowid, code FROM simeon_pq_codes WHERE dim = ? ORDER BY rowid";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
        return false;
    }
    sqlite3_bind_int64(stmt, 1, static_cast<sqlite3_int64>(dim));

    rowids.clear();
    codes.clear();
    int rc = SQLITE_OK;
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        const auto rowid = static_cast<std::size_t>(sqlite3_column_int64(stmt, 0));
        const void* blobPtr = sqlite3_column_blob(stmt, 1);
        const int blobSize = sqlite3_column_bytes(stmt, 1);
        if (!blobPtr || blobSize != static_cast<int>(m)) {
            sqlite3_finalize(stmt);
            rowids.clear();
            codes.clear();
            return false;
        }
        const auto* bytes = static_cast<const std::uint8_t*>(blobPtr);
        for (int i = 0; i < blobSize; ++i) {
            if (static_cast<std::size_t>(bytes[i]) >= k) {
                sqlite3_finalize(stmt);
                rowids.clear();
                codes.clear();
                return false;
            }
        }
        rowids.push_back(rowid);
        codes.insert(codes.end(), bytes, bytes + blobSize);
    }
    sqlite3_finalize(stmt);
    return rc == SQLITE_DONE && !rowids.empty() && codes.size() == rowids.size() * m;
}

inline bool savePersistedSimeonPq(sqlite3* db, std::size_t dim, const simeon::ProductQuantizer& pq,
                                  std::uint64_t seed, std::size_t trainLimit,
                                  std::size_t rerankFactor, std::uint64_t expectedSourceGeneration,
                                  std::span<const std::size_t> rowids,
                                  std::span<const std::uint8_t> codes, std::string& errorMessage) {
    auto codebookBlob =
        serializeSimeonPqCodebooks(pq.dim(), pq.m(), pq.k(), pq.is_trained(), pq.codebooks());
    if (sqlite3_exec(db, "BEGIN IMMEDIATE", nullptr, nullptr, nullptr) != SQLITE_OK) {
        errorMessage = "failed to begin Simeon PQ snapshot transaction";
        return false;
    }
    const auto rollback = [db] { sqlite3_exec(db, "ROLLBACK", nullptr, nullptr, nullptr); };

    std::uint64_t sourceGeneration = 0;
    if (!loadVectorIndexGeneration(db, sourceGeneration)) {
        rollback();
        errorMessage = "failed to read vector mutation generation";
        return false;
    }
    if (sourceGeneration != expectedSourceGeneration) {
        rollback();
        errorMessage = "Simeon PQ snapshot is stale relative to vector mutation generation";
        return false;
    }

    sqlite3_stmt* metaStmt = nullptr;
    const char* metaSql =
        "INSERT OR REPLACE INTO simeon_pq_meta "
        "(dim, format_version, m, k, seed, train_limit, source_generation, rerank_factor, trained, "
        "vector_count, codebooks_blob, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
        "unixepoch())";
    if (sqlite3_prepare_v2(db, metaSql, -1, &metaStmt, nullptr) != SQLITE_OK) {
        rollback();
        errorMessage = "failed to prepare simeon_pq_meta upsert";
        return false;
    }
    sqlite3_bind_int64(metaStmt, 1, static_cast<sqlite3_int64>(dim));
    sqlite3_bind_int(metaStmt, 2, static_cast<int>(kSimeonPqPersistenceFormatVersion));
    sqlite3_bind_int(metaStmt, 3, static_cast<int>(pq.m()));
    sqlite3_bind_int(metaStmt, 4, static_cast<int>(pq.k()));
    sqlite3_bind_int64(metaStmt, 5, static_cast<sqlite3_int64>(seed));
    sqlite3_bind_int64(metaStmt, 6, static_cast<sqlite3_int64>(trainLimit));
    sqlite3_bind_int64(metaStmt, 7, static_cast<sqlite3_int64>(sourceGeneration));
    sqlite3_bind_int64(metaStmt, 8, static_cast<sqlite3_int64>(rerankFactor));
    sqlite3_bind_int(metaStmt, 9, pq.is_trained() ? 1 : 0);
    sqlite3_bind_int64(metaStmt, 10, static_cast<sqlite3_int64>(rowids.size()));
    sqlite3_bind_blob(metaStmt, 11, codebookBlob.data(), static_cast<int>(codebookBlob.size()),
                      SQLITE_TRANSIENT);
    if (sqlite3_step(metaStmt) != SQLITE_DONE) {
        sqlite3_finalize(metaStmt);
        rollback();
        errorMessage = "failed to persist simeon_pq_meta";
        return false;
    }
    sqlite3_finalize(metaStmt);

    const std::string deleteSql = "DELETE FROM simeon_pq_codes WHERE dim = " + std::to_string(dim);
    if (sqlite3_exec(db, deleteSql.c_str(), nullptr, nullptr, nullptr) != SQLITE_OK) {
        rollback();
        errorMessage = "failed to clear simeon_pq_codes";
        return false;
    }

    sqlite3_stmt* codeStmt = nullptr;
    const char* codeSql = "INSERT INTO simeon_pq_codes (dim, rowid, code) VALUES (?, ?, ?)";
    if (sqlite3_prepare_v2(db, codeSql, -1, &codeStmt, nullptr) != SQLITE_OK) {
        rollback();
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
            rollback();
            errorMessage = "failed to persist simeon_pq_codes";
            return false;
        }
    }
    sqlite3_finalize(codeStmt);
    if (sqlite3_exec(db, "COMMIT", nullptr, nullptr, nullptr) != SQLITE_OK) {
        rollback();
        errorMessage = "failed to commit Simeon PQ snapshot";
        return false;
    }
    return true;
}

} // namespace yams::vector::detail
