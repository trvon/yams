#pragma once

#include <filesystem>
#include <fstream>
#include <string>

#include <yams/app/services/services.hpp>

namespace yams::test::capability {

inline bool indexing_available(const yams::app::services::AppContext& ctx) {
    using namespace yams::app::services;
    try {
        auto idx = makeIndexingService(ctx);
        if (!idx)
            return false;
        auto tmp = std::filesystem::temp_directory_path() /
                   (std::string("yams_idx_probe_") + std::to_string(::getpid()));
        std::error_code ec;
        std::filesystem::create_directories(tmp, ec);
        std::ofstream(tmp / "p.txt") << "probe";
        AddDirectoryRequest req;
        req.directoryPath = tmp.string();
        req.includePatterns = {"*.txt"};
        req.recursive = true;
        req.deferExtraction = true;
        req.verify = false;
        auto r = idx->addDirectory(req);
        std::filesystem::remove_all(tmp, ec);
        if (!r)
            return false;
        const auto& resp = r.value();
        return resp.filesProcessed >= 1;
    } catch (...) {
        return false;
    }
}

} // namespace yams::test::capability
