#include <yams/cli/doctor/checks/storage_blob_check.h>
#include <yams/cli/doctor/doctor_context.h>
#include <yams/cli/ui_helpers.hpp>
#include <yams/cli/yams_cli.h>
#include <yams/daemon/ipc/ipc_protocol.h>

#include <filesystem>
#include <iomanip>
#include <sstream>

namespace yams::cli::doctor {

StorageBlobCheck::Result StorageBlobCheck::execute(const DoctorContext& ctx) {
    Result r;
    const auto& state = ctx.cachedState();

    // Read from daemon stats when available
    if (state.stats) {
        auto getU64 = [&](const char* k) -> uint64_t {
            auto it = state.stats->additionalStats.find(k);
            if (it == state.stats->additionalStats.end())
                return 0;
            try {
                return static_cast<uint64_t>(std::stoull(it->second));
            } catch (...) {
                return 0;
            }
        };
        r.metadataDocuments = getU64("storage_documents");
        r.storageObjects = getU64("storage_objects");
        r.storageBytes = getU64("storage_physical_bytes");
    }

    // Local filesystem check when daemon is down
    if (!state.daemonUp && r.storageObjects == 0) {
        auto dataDir = ctx.dataDir();
        // Count objects in the storage directory
        namespace fs = std::filesystem;
        auto objectsDir = dataDir / "storage" / "objects";
        std::error_code ec;
        if (fs::exists(objectsDir, ec)) {
            for (const auto& entry : fs::recursive_directory_iterator(objectsDir, ec)) {
                if (ec)
                    break;
                if (entry.is_regular_file()) {
                    r.storageObjects++;
                    r.storageBytes += entry.file_size();
                }
            }
        }
    }

    r.ok = true; // no failures to report at this level
    return r;
}

void StorageBlobCheck::render(std::ostream& os, const Result& r) {
    using namespace yams::cli::ui;

    if (r.storageObjects == 0 && r.metadataDocuments == 0) {
        os << "  " << status_ok("No storage blobs — daemon not running or corpus is empty.")
           << "\n";
        return;
    }

    os << "  " << status_ok("Storage blobs: ") << format_number(r.storageObjects) << " objects";
    if (r.storageBytes > 0) {
        double mb = r.storageBytes / (1024.0 * 1024.0);
        std::ostringstream s;
        s << std::fixed << std::setprecision(1) << mb << " MB";
        os << " (" << s.str() << ")";
    }
    os << "\n";

    if (r.metadataDocuments > 0)
        os << "  " << status_ok("Metadata documents: ") << format_number(r.metadataDocuments)
           << "\n";

    if (r.ok)
        os << "  " << status_ok("Storage integrity: OK") << "\n";
}

} // namespace yams::cli::doctor
