#include <yams/cli/doctor/repairs/vector_fix.h>
#include <yams/cli/doctor/doctor_context.h>
#include <yams/cli/ui_helpers.hpp>
#include <yams/cli/vector_db_util.h>
#include <yams/cli/yams_cli.h>
#include <yams/config/config_helpers.h>

#include <nlohmann/json.hpp>

#include <sqlite3.h>
#include <cstdlib>
#include <filesystem>

namespace yams::cli::doctor {

VectorFixRepair::VectorFixRepair(YamsCLI* cli, bool jsonOutput)
    : cli_(cli), jsonOutput_(jsonOutput) {}

VectorFixRepair::Result VectorFixRepair::execute(const DoctorContext& ctx) {
    namespace fs = std::filesystem;
    Result r;

    if (!cli_) {
        r.error = "CLI context unavailable";
        return r;
    }

    auto vecDbPath = cli_->getDataPath() / "vectors.db";
    auto configPath = ctx.configPath();

    // Step 1: Read DB dimension
    std::optional<size_t> dbDim;
    if (fs::exists(vecDbPath)) {
        sqlite3* db = nullptr;
        if (sqlite3_open(vecDbPath.string().c_str(), &db) == SQLITE_OK && db) {
            const char* sql1 = "SELECT DISTINCT embedding_dim FROM vectors LIMIT 1";
            sqlite3_stmt* stmt = nullptr;
            if (sqlite3_prepare_v2(db, sql1, -1, &stmt, nullptr) == SQLITE_OK) {
                if (sqlite3_step(stmt) == SQLITE_ROW) {
                    dbDim = static_cast<size_t>(sqlite3_column_int(stmt, 0));
                }
                sqlite3_finalize(stmt);
            }
            if (!dbDim) {
                const char* sql2 =
                    "SELECT sql FROM sqlite_master WHERE name='doc_embeddings' LIMIT 1";
                if (sqlite3_prepare_v2(db, sql2, -1, &stmt, nullptr) == SQLITE_OK) {
                    if (sqlite3_step(stmt) == SQLITE_ROW) {
                        const unsigned char* txt = sqlite3_column_text(stmt, 0);
                        if (txt) {
                            std::string ddl(reinterpret_cast<const char*>(txt));
                            auto pos = ddl.find("float[");
                            if (pos != std::string::npos) {
                                auto end = ddl.find(']', pos);
                                if (end != std::string::npos && end > pos + 6) {
                                    std::string num = ddl.substr(pos + 6, end - (pos + 6));
                                    try {
                                        dbDim = static_cast<size_t>(std::stoul(num));
                                    } catch (...) {
                                    }
                                }
                            }
                        }
                    }
                    sqlite3_finalize(stmt);
                }
            }
            sqlite3_close(db);
        }
    }

    if (!dbDim) {
        r.error = "No vectors.db found or no vectors stored yet";
        return r;
    }
    r.dbDim = dbDim;

    // Step 2: Find current model dimension
    std::optional<size_t> modelDim;
    std::string modelName;
    fs::path modelsPath = cli_->getDataPath() / "models";

    if (const char* pref = std::getenv("YAMS_PREFERRED_MODEL"))
        modelName = pref;

    if (modelName.empty() && !configPath.empty()) {
        auto config = ctx.parseToml();
        auto it = config.find("embeddings.preferred_model");
        if (it != config.end() && !it->second.empty())
            modelName = it->second;
    }

    if (modelName.empty()) {
        std::error_code ec;
        if (fs::exists(modelsPath, ec) && fs::is_directory(modelsPath, ec)) {
            for (const auto& entry : fs::directory_iterator(modelsPath, ec)) {
                if (!entry.is_directory())
                    continue;
                if (fs::exists(entry.path() / "model.onnx", ec)) {
                    modelName = entry.path().filename().string();
                    break;
                }
            }
        }
    }

    auto getModelDim = [&](const std::string& name) -> std::optional<size_t> {
        auto dataPath = cli_ ? cli_->getDataPath() : yams::config::get_data_dir();
        if (auto metaDim = vecutil::getModelDimensionFromMetadata(dataPath, name))
            return *metaDim;
        return vecutil::getModelDimensionHeuristic(name);
    };

    if (!modelName.empty()) {
        modelDim = getModelDim(modelName);
        r.modelName = modelName;
        r.modelDim = modelDim;
    }

    // Step 3: Check for mismatch
    if (modelDim && *modelDim == *dbDim) {
        r.mismatch = false;
        return r;
    }
    r.mismatch = true;

    // Step 4: Find a model that matches DB dimension
    std::string matchingModel;
    std::error_code ec;
    if (fs::exists(modelsPath, ec) && fs::is_directory(modelsPath, ec)) {
        for (const auto& entry : fs::directory_iterator(modelsPath, ec)) {
            if (!entry.is_directory())
                continue;
            if (!fs::exists(entry.path() / "model.onnx", ec))
                continue;
            std::string name = entry.path().filename().string();
            auto dim = getModelDim(name);
            if (dim && *dim == *dbDim) {
                matchingModel = name;
                break;
            }
        }
    }

    if (matchingModel.empty()) {
        r.error = "No installed model matches " + std::to_string(*dbDim) + "-dim";
        return r;
    }
    r.matchingModel = matchingModel;

    // Step 5: Fix config
    try {
        ctx.writeConfigDims(*dbDim);
        if (!matchingModel.empty()) {
            yams::config::write_config_value(configPath, std::string("embeddings.preferred_model"),
                                             matchingModel);
        }
        r.fixed = true;
    } catch (const std::exception& e) {
        r.error = std::string("Failed to update config: ") + e.what();
    }

    return r;
}

void VectorFixRepair::render(std::ostream& os, const Result& r, bool jsonOutput) {
    using namespace yams::cli::ui;

    if (!r.error.empty() && !r.dbDim) {
        if (jsonOutput)
            os << nlohmann::json{{"status", "no_vectors"}, {"message", r.error}}.dump() << "\n";
        else
            os << status_warning(r.error) << "\n"
               << "Nothing to fix. Index some documents first with 'yams add'.\n";
        return;
    }

    if (jsonOutput) {
        if (r.fixed) {
            nlohmann::json j;
            j["status"] = "fixed";
            j["db_dim"] = *r.dbDim;
            j["previous_model"] = r.modelName;
            if (r.modelDim)
                j["previous_model_dim"] = *r.modelDim;
            j["new_model"] = r.matchingModel;
            j["new_model_dim"] = *r.dbDim;
            os << j.dump() << "\n";
        } else if (r.mismatch) {
            nlohmann::json j;
            j["status"] = "error";
            j["db_dim"] = *r.dbDim;
            j["model"] = r.modelName;
            if (r.modelDim)
                j["model_dim"] = *r.modelDim;
            j["mismatch"] = true;
            j["fixed"] = false;
            j["error"] = r.error;
            os << j.dump() << "\n";
        } else {
            nlohmann::json j;
            j["status"] = "ok";
            j["db_dim"] = *r.dbDim;
            j["model"] = r.modelName;
            j["model_dim"] = *r.modelDim;
            j["mismatch"] = false;
            os << j.dump() << "\n";
        }
        return;
    }

    if (r.dbDim)
        os << "DB stored dimension: " << *r.dbDim << "\n";
    if (r.modelDim)
        os << "Current model: " << r.modelName << " (" << *r.modelDim << "-dim)\n";

    if (!r.mismatch) {
        os << "\n" << status_ok("No dimension mismatch. DB and model are aligned.") << "\n";
        return;
    }

    if (!r.error.empty() && !r.fixed) {
        os << "\n" << status_error(r.error) << "\n\n";
        os << "Options:\n";
        os << "  1. Download a model that matches " << *r.dbDim << "-dim\n";
        os << "  2. Recreate vectors with the default Simeon dimension (1024)\n";
        return;
    }

    if (r.fixed) {
        os << "\n" << status_ok("Found matching model: " + r.matchingModel) << "\n";
        os << "\n" << status_ok("Config updated") << "\n";
        os << "  embeddings.preferred_model = \"" << r.matchingModel << "\"\n";
        os << "  embeddings.embedding_dim = " << *r.dbDim << "\n";
        os << "  vector_database.embedding_dim = " << *r.dbDim << "\n";
        os << "  vector_index.dimension = " << *r.dbDim << "\n\n";
        os << "Restart daemon to apply: yams daemon restart\n";
    }
}

} // namespace yams::cli::doctor
