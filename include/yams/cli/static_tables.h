#pragma once

#include <yams/core/types.h>

#include <cstddef>
#include <filesystem>
#include <string>
#include <string_view>
#include <vector>

namespace yams::cli {

// Static-embedding tables (Simeon SMSTAT01) for the dense leg and the deep rerank, installed to
// <data-dir>/models/static/<name>.smstat. An explicit choice records the selected
// path; file presence alone does not activate the model.
struct StaticTableInfo {
    std::string name;
    std::string url;
    std::string sha256;
    std::size_t size_mb;
    std::string description;
};

inline constexpr std::string_view kDefaultStaticTable = "yams-static-bge-c4-256";

const std::vector<StaticTableInfo>& staticTableCatalog();
const StaticTableInfo* findStaticTable(std::string_view name);

inline std::filesystem::path staticTablesDir(const std::filesystem::path& dataDir) {
    return dataDir / "models" / "static";
}

// Records an explicit selection after validating the table. Stages a checked,
// format-preserving config edit beside the destination and publishes by rename;
// an error leaves the previous config and working model intact.
Result<void> selectStaticTable(const std::filesystem::path& table,
                               const std::filesystem::path& configPath);

// Downloads `url` to <outDir>/<name>.smstat (outDir empty: staticTablesDir(dataDir)), verifies
// `sha256` when given, and checks that the file loads as a table. An existing file is kept
// unless `force`. The downloader stages under `dataDir`. Returns the table path.
Result<std::filesystem::path> downloadStaticTable(const std::string& name, const std::string& url,
                                                  const std::string& sha256,
                                                  const std::filesystem::path& dataDir,
                                                  const std::filesystem::path& outDir, bool force);

} // namespace yams::cli
