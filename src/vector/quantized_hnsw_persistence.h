#pragma once

#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include <sqlite3.h>
#include <spdlog/spdlog.h>

#include <sqlite-vec-cpp/index/hnsw_persistence.hpp>

#include "quantized_hnsw_store_codec.h"

namespace yams::vector::detail {

inline bool hasPersistedQuantizedHnswNodes(sqlite3* db, const std::string& tablePrefix) {
    const std::string sql =
        "SELECT EXISTS(SELECT 1 FROM \"" + tablePrefix + "_hnsw_nodes\" LIMIT 1)";
    sqlite3_stmt* stmt = nullptr;
    if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK) {
        return false;
    }
    bool exists = false;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        exists = sqlite3_column_int(stmt, 0) != 0;
    }
    sqlite3_finalize(stmt);
    return exists;
}

template <typename QuantizedSearchT>
bool loadPersistedQuantizedStoreBlob(sqlite3* db, size_t dim,
                                     sqlite_vec_cpp::index::QuantizationType expectedType,
                                     size_t expectedCount, QuantizedSearchT& search) {
    sqlite3_stmt* stmt = nullptr;
    const char* sql =
        "SELECT format_version, quantization_type, rerank_factor, vector_count, store_blob "
        "FROM quantized_hnsw_meta WHERE dim = ?";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
        return false;
    }
    sqlite3_bind_int64(stmt, 1, static_cast<sqlite3_int64>(dim));
    if (sqlite3_step(stmt) != SQLITE_ROW) {
        sqlite3_finalize(stmt);
        return false;
    }

    const int formatVersion = sqlite3_column_int(stmt, 0);
    const auto storedType =
        static_cast<sqlite_vec_cpp::index::QuantizationType>(sqlite3_column_int(stmt, 1));
    const auto storedCount = static_cast<size_t>(sqlite3_column_int64(stmt, 3));
    const void* blobPtr = sqlite3_column_blob(stmt, 4);
    const int blobSize = sqlite3_column_bytes(stmt, 4);
    std::vector<uint8_t> blob;
    if (blobPtr && blobSize > 0) {
        blob.resize(static_cast<size_t>(blobSize));
        std::memcpy(blob.data(), blobPtr, blob.size());
    }
    sqlite3_finalize(stmt);

    if (formatVersion != 1 || storedType != expectedType || storedCount != expectedCount ||
        blob.empty()) {
        return false;
    }

    const auto generation = search.base_index().mutation_generation();
    switch (expectedType) {
        case sqlite_vec_cpp::index::QuantizationType::LVQ8: {
            sqlite_vec_cpp::quantization::LVQ8Store store;
            if (!deserializeLvq8StoreBlob(blob, store) || store.count != expectedCount) {
                return false;
            }
            search.import_quantization(std::move(store), generation);
            return true;
        }
        case sqlite_vec_cpp::index::QuantizationType::LVQ4: {
            sqlite_vec_cpp::quantization::LVQ4Store store;
            if (!deserializeLvq4StoreBlob(blob, store) || store.count != expectedCount) {
                return false;
            }
            search.import_quantization(std::move(store), generation);
            return true;
        }
        case sqlite_vec_cpp::index::QuantizationType::RaBitQ: {
            sqlite_vec_cpp::quantization::RaBitQStore store;
            if (!deserializeRaBitQStoreBlob(blob, store) || store.count != expectedCount) {
                return false;
            }
            search.import_quantization(std::move(store), generation);
            return true;
        }
        case sqlite_vec_cpp::index::QuantizationType::None:
            break;
    }
    return false;
}

template <typename QuantizedIndexT, typename QuantizedSearchT, typename MetricT>
bool loadPersistedQuantizedHnswDim(sqlite3* db, size_t dim, const std::string& tablePrefix,
                                   typename QuantizedSearchT::Config searchConfig,
                                   std::unique_ptr<QuantizedIndexT>& outBase,
                                   std::unique_ptr<QuantizedSearchT>& outSearch) {
    if (!hasPersistedQuantizedHnswNodes(db, tablePrefix)) {
        return false;
    }

    try {
        char* err = nullptr;
        auto loaded_hnsw = sqlite_vec_cpp::index::load_hnsw_index<float, MetricT>(
            db, "main", tablePrefix.c_str(), &err);
        if (err) {
            spdlog::warn("[QHNSW] Failed to load persisted base index for dim={}: {}", dim, err);
            sqlite3_free(err);
            return false;
        }

        auto base = std::make_unique<QuantizedIndexT>(std::move(loaded_hnsw));
        auto search = std::make_unique<QuantizedSearchT>(*base, searchConfig);
        if (!loadPersistedQuantizedStoreBlob(db, dim, searchConfig.quantization, base->size(),
                                             *search)) {
            return false;
        }

        outBase = std::move(base);
        outSearch = std::move(search);
        return true;
    } catch (const std::exception& e) {
        spdlog::warn("[QHNSW] Exception loading persisted quantized index for dim={}: {}", dim,
                     e.what());
        return false;
    }
}

template <typename QuantizedIndexT, typename MetricT>
bool savePersistedQuantizedBase(sqlite3* db, const std::string& tablePrefix, size_t dim,
                                const QuantizedIndexT& index, std::string& errorMessage) {
    char* err = nullptr;
    int rc = sqlite_vec_cpp::index::save_hnsw_index<float, MetricT>(db, "main", tablePrefix.c_str(),
                                                                    index, &err);
    if (rc != SQLITE_OK) {
        errorMessage = err ? err : "unknown";
        if (err) {
            sqlite3_free(err);
        }
        return false;
    }
    if (err) {
        sqlite3_free(err);
    }
    (void)dim;
    return true;
}

inline bool saveQuantizedHnswMeta(sqlite3* db, size_t dim,
                                  sqlite_vec_cpp::index::QuantizationType mode, size_t rerankFactor,
                                  size_t vectorCount, const std::vector<uint8_t>& storeBlob,
                                  std::string& errorMessage) {
    sqlite3_stmt* stmt = nullptr;
    const char* sql =
        "INSERT OR REPLACE INTO quantized_hnsw_meta "
        "(dim, format_version, quantization_type, rerank_factor, vector_count, store_blob, "
        "updated_at) VALUES (?, 1, ?, ?, ?, ?, unixepoch())";
    int rc = sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        errorMessage = "Failed to prepare quantized_hnsw_meta upsert";
        return false;
    }
    sqlite3_bind_int64(stmt, 1, static_cast<sqlite3_int64>(dim));
    sqlite3_bind_int(stmt, 2, static_cast<int>(mode));
    sqlite3_bind_int64(stmt, 3, static_cast<sqlite3_int64>(rerankFactor));
    sqlite3_bind_int64(stmt, 4, static_cast<sqlite3_int64>(vectorCount));
    sqlite3_bind_blob(stmt, 5, storeBlob.data(), static_cast<int>(storeBlob.size()),
                      SQLITE_TRANSIENT);
    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    if (rc != SQLITE_DONE) {
        errorMessage = "Failed to persist quantized_hnsw_meta for dim " + std::to_string(dim);
        return false;
    }
    return true;
}

inline bool hasReusablePersistedQuantizedHnsw(sqlite3* db, size_t dim,
                                              sqlite_vec_cpp::index::QuantizationType type) {
    sqlite3_stmt* stmt = nullptr;
    const char* sql =
        "SELECT EXISTS(SELECT 1 FROM quantized_hnsw_meta WHERE dim = ? AND quantization_type = ?)";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
        return false;
    }
    sqlite3_bind_int64(stmt, 1, static_cast<sqlite3_int64>(dim));
    sqlite3_bind_int(stmt, 2, static_cast<int>(type));
    bool exists = false;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        exists = sqlite3_column_int(stmt, 0) != 0;
    }
    sqlite3_finalize(stmt);
    return exists;
}

} // namespace yams::vector::detail
