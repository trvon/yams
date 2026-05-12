#include <yams/cli/doctor/checks/db_integrity.h>
#include <yams/cli/doctor/doctor_context.h>
#include <yams/cli/ui_helpers.hpp>

#include <filesystem>
#include <iomanip>
#include <sstream>

namespace yams::cli::doctor {

DbIntegrityCheck::DbIntegrityCheck() = default;

DbIntegrityCheck::Result DbIntegrityCheck::execute(const DoctorContext& ctx) {
    Result r;

    auto dataDir = ctx.dataDir();
    const auto& state = ctx.cachedState();

    if (state.status && (state.status->metadataWalBytes > 0 || state.status->vectorWalBytes > 0)) {
        r.metadataWalBytes = state.status->metadataWalBytes;
        r.vectorWalBytes = state.status->vectorWalBytes;
    } else {
        r.metadataWalBytes = DoctorContext::walFileSize(dataDir, "yams.db-wal");
        r.vectorWalBytes = DoctorContext::walFileSize(dataDir, "vectors.db-wal");
    }

    constexpr uint64_t kWarnBytes = 128ULL * 1024ULL * 1024ULL;
    if (r.metadataWalBytes >= kWarnBytes || r.vectorWalBytes >= kWarnBytes) {
        r.ok = false;
    }

    // ── Corrupt artifacts ──
    std::error_code ec;
    namespace fs = std::filesystem;
    if (fs::exists(dataDir, ec)) {
        for (const auto& entry : fs::directory_iterator(dataDir, ec)) {
            if (ec)
                break;
            auto name = entry.path().filename().string();
            if (name.find(".corrupt-") != std::string::npos) {
                std::ostringstream oss;
                oss << name;
                if (entry.is_regular_file()) {
                    auto fsize = entry.file_size();
                    if (fsize > 0)
                        oss << " (" << (fsize / (1024ULL * 1024ULL)) << " MB)";
                }
                r.corruptArtifacts.push_back(oss.str());
            }
        }
    }

    if (!r.corruptArtifacts.empty()) {
        r.ok = false;
    }

    return r;
}

void DbIntegrityCheck::render(std::ostream& os, const Result& r) {
    using namespace yams::cli::ui;

    constexpr uint64_t kWarnBytes = 128ULL * 1024ULL * 1024ULL;

    auto walStatus = [](uint64_t bytes, uint64_t warnBytes) -> std::string {
        if (bytes == 0)
            return status_ok("0 B");
        if (bytes >= warnBytes)
            return status_error(std::to_string(bytes / (1024ULL * 1024ULL)) + " MB");
        return status_ok(std::to_string(bytes / (1024ULL * 1024ULL)) + " MB");
    };

    os << "  yams.db-wal:   " << walStatus(r.metadataWalBytes, kWarnBytes);
    if (r.metadataWalBytes > 0 && r.metadataWalBytes < 1024ULL * 1024ULL)
        os << " (" << r.metadataWalBytes << " B)";
    os << "\n";
    os << "  vectors.db-wal: " << walStatus(r.vectorWalBytes, kWarnBytes);
    if (r.vectorWalBytes > 0 && r.vectorWalBytes < 1024ULL * 1024ULL)
        os << " (" << r.vectorWalBytes << " B)";
    os << "\n";

    for (const auto& artifact : r.corruptArtifacts) {
        os << "\n  " << status_warning(artifact) << "\n";
    }

    if (r.ok) {
        os << "\n  " << status_ok("No DB integrity issues detected.") << "\n";
    }
}

} // namespace yams::cli::doctor
