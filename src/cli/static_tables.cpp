#include <yams/cli/static_tables.h>

#include <yams/common/fs_utils.h>
#include <yams/downloader/downloader.hpp>
#include <yams/vector/simeon_embedding_backend.h>

#include <algorithm>
#include <cctype>
#include <iostream>
#include <system_error>

namespace yams::cli {

namespace fs = std::filesystem;

const std::vector<StaticTableInfo>& staticTableCatalog() {
    static const std::vector<StaticTableInfo> tables = {
        {"yams-static-bge-c4-256",
         "https://github.com/trvon/simeon/releases/download/static-tables-v1/"
         "yams-static-bge-c4-256.smstat",
         "cd7ade87d3bb360eca94792561bafb9b8ffd43eeb0739826e02642a848120d74", 17,
         "WordPiece static embeddings, 256 dims (tokenlearn: bge-base-en-v1.5 teacher, MIT; "
         "1M C4 passages, ODC-BY)"},
    };
    return tables;
}

const StaticTableInfo* findStaticTable(std::string_view name) {
    const auto& tables = staticTableCatalog();
    auto it = std::find_if(tables.begin(), tables.end(),
                           [&](const StaticTableInfo& t) { return t.name == name; });
    return it == tables.end() ? nullptr : &*it;
}

Result<fs::path> downloadStaticTable(const std::string& name, const std::string& url,
                                     const std::string& sha256, const fs::path& dataDir,
                                     const fs::path& outDir, bool force) {
    if (url.empty()) {
        return Error{ErrorCode::InvalidArgument, "no URL for static table: " + name};
    }
    if (name.empty() || name.find_first_of("/\\") != std::string::npos || name == "." ||
        name == ".." || name.find("..") != std::string::npos) {
        return Error{ErrorCode::InvalidArgument, "invalid static table name: " + name};
    }
    const fs::path dir = outDir.empty() ? staticTablesDir(dataDir) : outDir;
    yams::common::ensureDirectories(dir);
    const fs::path out = dir / (name + ".smstat");
    std::error_code ec;
    if (fs::exists(out, ec) && !force) {
        return out;
    }
    if (sha256.empty()) {
        std::cout << "Warning: no sha256 given; the download is not verified (pass --sha256)\n";
    }

    yams::downloader::StorageConfig storage{};
    storage.objectsDir = dataDir / "storage" / "objects";
    storage.stagingDir = dataDir / "staging" / "downloader";
    yams::downloader::DownloaderConfig dcfg{};
    auto manager = yams::downloader::makeDownloadManager(storage, dcfg);
    yams::downloader::DownloadRequest req{};
    req.url = url;
    req.storeOnly = true;
    req.exportPath = out;
    req.overwrite = yams::downloader::OverwritePolicy::Always;
    if (!sha256.empty()) {
        std::string hex = sha256;
        std::transform(hex.begin(), hex.end(), hex.begin(),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        req.checksum = yams::downloader::Checksum{yams::downloader::HashAlgo::Sha256, hex};
    }
    std::cout << "Downloading static table " << name << "\n  from " << url << "\n";
    auto res = manager->download(req, {}, [] { return false; }, {});
    if (!res.ok() || !res.value().success) {
        const std::string msg = res.ok() ? (res.value().error ? res.value().error->message
                                                              : std::string("download failed"))
                                         : res.error().message;
        return Error{ErrorCode::NetworkError, "static table " + name + ": " + msg};
    }
    if (!yams::vector::loadSharedStaticEmbeddingTable(out)) {
        fs::remove(out, ec);
        return Error{ErrorCode::CorruptedData,
                     "downloaded file is not a valid SMSTAT01 table: " + out.string()};
    }
    return out;
}

} // namespace yams::cli
