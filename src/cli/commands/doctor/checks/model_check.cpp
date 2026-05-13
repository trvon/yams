#include <yams/cli/doctor/checks/model_check.h>
#include <yams/cli/doctor/doctor_context.h>
#include <yams/cli/ui_helpers.hpp>
#include <yams/cli/vector_db_util.h>
#include <yams/cli/yams_cli.h>

#include <cstdlib>
#include <filesystem>

namespace yams::cli::doctor {

ModelCheck::Result ModelCheck::execute(const DoctorContext& ctx) {
    namespace fs = std::filesystem;
    Result r;

    const char* home = std::getenv("HOME");
    if (!home)
        return r;

    fs::path oldBase = fs::path(home) / ".yams" / "models";
    fs::path newBase = ctx.dataDir() / "models";

    std::error_code ec;

    // Check old location
    size_t oldCount = 0;
    if (fs::exists(oldBase, ec) && fs::is_directory(oldBase, ec)) {
        for (const auto& entry : fs::directory_iterator(oldBase, ec)) {
            if (entry.is_directory() && fs::exists(entry.path() / "model.onnx", ec)) {
                oldCount++;
            }
        }
    }
    r.oldLocationCount = oldCount;
    if (oldCount > 0) {
        r.oldLocationPath = oldBase.string();
    }
    r.newLocationPath = newBase.string();

    if (!fs::exists(newBase, ec) || !fs::is_directory(newBase, ec))
        return r;

    for (const auto& entry : fs::directory_iterator(newBase, ec)) {
        if (!entry.is_directory())
            continue;
        const auto& dir = entry.path();
        auto name = dir.filename().string();

        const bool hasOnnx = fs::exists(dir / "model.onnx", ec);
        if (!hasOnnx)
            continue;

        ModelInfo info;
        info.name = name;

        if (auto cfgDim = vecutil::getModelDimensionFromMetadata(ctx.dataDir(), name))
            info.dim = static_cast<int>(*cfgDim);
        else if (auto heurDim = vecutil::getModelDimensionHeuristic(name))
            info.dim = static_cast<int>(*heurDim);

        const bool hasConfig = fs::exists(dir / "config.json", ec) ||
                               fs::exists(dir / "sentence_bert_config.json", ec);
        const bool hasTokenizer = fs::exists(dir / "tokenizer.json", ec);

        info.missingConfig = !hasConfig;
        info.missingTokenizer = !hasTokenizer;

        r.models.push_back(std::move(info));
    }

    return r;
}

void ModelCheck::render(std::ostream& os, const Result& r) {
    using namespace yams::cli::ui;

    if (r.oldLocationCount > 0) {
        os << "\n";
        os << "  "
           << status_warning("Found " + std::to_string(r.oldLocationCount) +
                             " model(s) in OLD location: " + r.oldLocationPath)
           << "\n";
        os << "  " << status_warning("Models should be in unified storage: " + r.newLocationPath)
           << "\n";
        os << "\nMigration command:\n";
        os << "  mkdir -p " << r.newLocationPath << "\n";
        os << "  mv " << r.oldLocationPath << "/* " << r.newLocationPath << "/\n";
        os << "  yams daemon restart\n\n";
    }

    if (!r.models.empty()) {
        os << "\nModels (installed)\n-------------------\n";
        std::vector<std::string> warnings;
        for (const auto& m : r.models) {
            if (m.dim > 0)
                os << "- " << m.name << ": dim=" << m.dim << " (heuristic)\n";
            else
                os << "- " << m.name << ": dim=unknown\n";

            if (m.missingConfig)
                warnings.push_back(
                    "  - " + m.name +
                    ": missing config.json — run 'yams model download --apply-config " + m.name +
                    " --force'");
            if (m.name.find("nomic") != std::string::npos && m.missingTokenizer)
                warnings.push_back(
                    "  - " + m.name +
                    ": missing tokenizer.json — run 'yams model download --apply-config " + m.name +
                    " --force'");
        }

        if (!warnings.empty()) {
            os << "\nSome models are missing companion files required for robust inference.\n";
            for (const auto& w : warnings)
                os << w << "\n";
            os << "\nTip: You can also supply --hf <org/name> when downloading if the name "
                  "isn't recognized.\n";
        }
    }
}

} // namespace yams::cli::doctor
