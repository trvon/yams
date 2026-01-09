#include <yams/cli/vector_db_util.h>
#include <yams/cli/yams_cli.h>
#include <yams/config/config_helpers.h>

#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <nlohmann/json.hpp>
#include <sqlite3.h>

// Forward declare sqlite3_vec_init for vec0 module initialization
extern "C" int sqlite3_vec_init(sqlite3* db, char** pzErrMsg, const sqlite3_api_routines* pApi);

namespace yams::cli::vecutil {

namespace fs = std::filesystem;

// ============================================================================
// Dimension Resolution
// ============================================================================

std::optional<size_t> getDimensionFromDb(const fs::path& dbPath) {
    if (!fs::exists(dbPath)) {
        return std::nullopt;
    }

    sqlite3* db = nullptr;
    if (sqlite3_open(dbPath.string().c_str(), &db) != SQLITE_OK) {
        if (db) {
            sqlite3_close(db);
        }
        return std::nullopt;
    }

    std::optional<size_t> result;
    sqlite3_stmt* stmt = nullptr;

    // Try new schema with embedding_dim column first
    const char* sql1 = "SELECT DISTINCT embedding_dim FROM vectors LIMIT 1";
    if (sqlite3_prepare_v2(db, sql1, -1, &stmt, nullptr) == SQLITE_OK) {
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            result = static_cast<size_t>(sqlite3_column_int(stmt, 0));
        }
        sqlite3_finalize(stmt);
    }

    // Fallback: try doc_embeddings (legacy vec0 schema)
    if (!result) {
        const char* sql2 = "SELECT sql FROM sqlite_master WHERE name='doc_embeddings' LIMIT 1";
        if (sqlite3_prepare_v2(db, sql2, -1, &stmt, nullptr) == SQLITE_OK) {
            if (sqlite3_step(stmt) == SQLITE_ROW) {
                const unsigned char* txt = sqlite3_column_text(stmt, 0);
                if (txt) {
                    std::string ddl(reinterpret_cast<const char*>(txt));
                    // Parse float[N] from DDL
                    auto pos = ddl.find("float[");
                    if (pos != std::string::npos) {
                        auto end = ddl.find(']', pos);
                        if (end != std::string::npos && end > pos + 6) {
                            std::string num = ddl.substr(pos + 6, end - (pos + 6));
                            try {
                                result = static_cast<size_t>(std::stoul(num));
                            } catch (...) {
                                // Parse error, leave as nullopt
                            }
                        }
                    }
                }
            }
            sqlite3_finalize(stmt);
        }
    }

    sqlite3_close(db);
    return result;
}

std::optional<size_t> getModelDimensionHeuristic(const std::string& modelName) {
    // MiniLM variants are 384-dim
    if (modelName.find("MiniLM") != std::string::npos ||
        modelName.find("minilm") != std::string::npos) {
        return 384;
    }

    // Nomic, mpnet, bge are typically 768-dim
    if (modelName.find("nomic") != std::string::npos ||
        modelName.find("mpnet") != std::string::npos ||
        modelName.find("bge") != std::string::npos) {
        return 768;
    }

    // e5-large is 1024-dim
    if (modelName.find("e5-large") != std::string::npos) {
        return 1024;
    }

    return std::nullopt;
}

DimensionResolution resolveEmbeddingDimension(YamsCLI* cli, const fs::path& dataPath) {
    DimensionResolution result;

    // Priority 1: Existing DB schema
    fs::path dbPath = dataPath / "vectors.db";
    if (auto dbDim = getDimensionFromDb(dbPath)) {
        result.dimension = *dbDim;
        result.source = "db";
        return result;
    }

    // Priority 2: Config file
    fs::path configPath = yams::config::get_config_path();
    if (fs::exists(configPath)) {
        auto dimConfig = yams::config::read_dimension_config(configPath);
        if (dimConfig.embeddings) {
            result.dimension = *dimConfig.embeddings;
            result.source = "config";
            return result;
        }
        if (dimConfig.vectorDb) {
            result.dimension = *dimConfig.vectorDb;
            result.source = "config";
            return result;
        }
    }

    // Priority 3: Environment variable
    if (const char* envDim = std::getenv("YAMS_EMBED_DIM")) {
        try {
            result.dimension = static_cast<size_t>(std::stoul(envDim));
            result.source = "env";
            return result;
        } catch (...) {
            // Parse error, continue to next priority
        }
    }

    // Priority 4: Embedding generator (if CLI available)
    if (cli) {
        try {
            auto emb = cli->getEmbeddingGenerator();
            if (emb) {
                size_t genDim = emb->getEmbeddingDimension();
                if (genDim > 0) {
                    result.dimension = genDim;
                    result.source = "generator";
                    return result;
                }
            }
        } catch (...) {
            // Generator not available, continue
        }
    }

    // Priority 5: Model name heuristic
    std::string modelName;
    if (const char* pref = std::getenv("YAMS_PREFERRED_MODEL")) {
        modelName = pref;
    }

    if (modelName.empty()) {
        // Check for installed models
        fs::path modelsDir = dataPath / "models";
        std::error_code ec;
        if (fs::exists(modelsDir, ec) && fs::is_directory(modelsDir, ec)) {
            for (const auto& entry : fs::directory_iterator(modelsDir, ec)) {
                if (!entry.is_directory()) {
                    continue;
                }
                if (fs::exists(entry.path() / "model.onnx", ec)) {
                    modelName = entry.path().filename().string();
                    break;
                }
            }
        }
    }

    if (!modelName.empty()) {
        if (auto modelDim = getModelDimensionHeuristic(modelName)) {
            result.dimension = *modelDim;
            result.source = "model";
            return result;
        }
    }

    // Priority 6: Fallback heuristic (384 is most common for MiniLM)
    result.dimension = 384;
    result.source = "heuristic";
    return result;
}

// ============================================================================
// Vec0 Module and Schema Validation
// ============================================================================

bool isVec0Available(sqlite3* db) {
    if (!db) {
        return false;
    }

    const char* sql = "SELECT 1 FROM pragma_module_list WHERE name='vec0'";
    sqlite3_stmt* stmt = nullptr;
    bool available = false;

    if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) == SQLITE_OK) {
        available = (sqlite3_step(stmt) == SQLITE_ROW);
        sqlite3_finalize(stmt);
    }

    return available;
}

Result<void> initVecModule(sqlite3* db) {
    if (!db) {
        return Error{ErrorCode::InvalidArgument, "Null database handle"};
    }

    char* errMsg = nullptr;
    int rc = sqlite3_vec_init(db, &errMsg, nullptr);
    if (rc != SQLITE_OK) {
        std::string msg = errMsg ? errMsg : "Unknown error";
        if (errMsg) {
            sqlite3_free(errMsg);
        }
        return Error{ErrorCode::DatabaseError, "Failed to initialize vec0: " + msg};
    }

    return Result<void>();
}

SchemaValidation validateVecSchema(const fs::path& dbPath) {
    SchemaValidation result;

    if (!fs::exists(dbPath)) {
        result.error = "Database file does not exist";
        return result;
    }
    result.exists = true;

    sqlite3* db = nullptr;
    int rc = sqlite3_open(dbPath.string().c_str(), &db);
    if (rc != SQLITE_OK) {
        result.error = "Failed to open database: " + std::string(sqlite3_errmsg(db));
        if (db) {
            sqlite3_close(db);
        }
        return result;
    }

    // Initialize vec0 module
    char* errMsg = nullptr;
    rc = sqlite3_vec_init(db, &errMsg, nullptr);
    if (rc != SQLITE_OK) {
        result.error = "Failed to initialize vec0: " + std::string(errMsg ? errMsg : "unknown");
        if (errMsg) {
            sqlite3_free(errMsg);
        }
        sqlite3_close(db);
        return result;
    }

    // Check if vec0 module is available
    result.vec0Available = isVec0Available(db);
    if (!result.vec0Available) {
        result.error = "vec0 module not available after initialization";
        sqlite3_close(db);
        return result;
    }

    // Check doc_embeddings table schema
    const char* sql = "SELECT sql FROM sqlite_master WHERE name='doc_embeddings' AND type='table'";
    sqlite3_stmt* stmt = nullptr;

    if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) == SQLITE_OK) {
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            const unsigned char* txt = sqlite3_column_text(stmt, 0);
            if (txt) {
                result.ddl = reinterpret_cast<const char*>(txt);
                result.schemaValid = true;
                result.usesVec0 = (result.ddl.find("USING vec0") != std::string::npos);

                // Extract dimension
                auto pos = result.ddl.find("float[");
                if (pos != std::string::npos) {
                    auto end = result.ddl.find(']', pos);
                    if (end != std::string::npos && end > pos + 6) {
                        std::string num = result.ddl.substr(pos + 6, end - (pos + 6));
                        try {
                            result.dimension = static_cast<size_t>(std::stoul(num));
                        } catch (...) {
                            // Parse error
                        }
                    }
                }
            }
        }
        sqlite3_finalize(stmt);
    }

    sqlite3_close(db);
    return result;
}

// ============================================================================
// Database File Operations
// ============================================================================

Result<void> ensureDbFile(const fs::path& dbPath) {
    try {
        if (!fs::exists(dbPath)) {
            // Create parent directories if needed
            fs::create_directories(dbPath.parent_path());

            // Create empty file
            std::ofstream file(dbPath);
            file.flush();
            file.close();
        }

        if (!fs::exists(dbPath)) {
            return Error{ErrorCode::IOError, "Failed to create database file"};
        }

        return Result<void>();
    } catch (const std::exception& e) {
        return Error{ErrorCode::IOError, e.what()};
    }
}

void writeVectorSentinel(const fs::path& dataDir, size_t dim) {
    try {
        nlohmann::json sentinel;
        sentinel["embedding_dim"] = dim;
        sentinel["schema"] = "vec0";
        sentinel["schema_version"] = 1;
        sentinel["updated"] = static_cast<int64_t>(std::time(nullptr));

        fs::path sentinelPath = dataDir / "vectors_sentinel.json";
        std::ofstream out(sentinelPath);
        out << sentinel.dump(2);
    } catch (...) {
        // Best effort, ignore errors
    }
}

} // namespace yams::cli::vecutil
