#include <yams/cli/doctor/checks/storage_blob_check.h>
#include <yams/cli/doctor/doctor_context.h>
#include <yams/cli/ui_helpers.hpp>
#include <yams/cli/yams_cli.h>
#include <yams/metadata/database.h>

#include <filesystem>
#include <iomanip>
#include <set>
#include <sstream>

namespace yams::cli::doctor {

StorageBlobCheck::Result StorageBlobCheck::execute(const DoctorContext& ctx) {
    Result r;
    namespace fs = std::filesystem;
    auto dataDir = ctx.dataDir();
    auto objectsDir = dataDir / "storage" / "objects";
    std::error_code ec;

    // ── Count storage objects from filesystem ──
    std::set<std::string> objectHashes;
    if (fs::exists(objectsDir, ec)) {
        for (const auto& entry : fs::recursive_directory_iterator(objectsDir, ec)) {
            if (ec)
                break;
            if (entry.is_regular_file()) {
                r.storageObjects++;
                std::error_code sizeEc;
                auto sz = entry.file_size(sizeEc);
                if (!sizeEc)
                    r.storageBytes += sz;
                auto hashName = entry.path().filename().string();
                objectHashes.insert(hashName);
            }
        }
    }

    // ── Count metadata documents and check blob existence ──
    bool dbAvailable = false;
    if (ctx.cli())
        try {
            auto db = ctx.cli()->getDatabase();
            dbAvailable = db && db->isOpen();
            if (dbAvailable) {
                // Count total documents
                auto stCountR = db->prepare("SELECT COUNT(*) FROM documents");
                if (stCountR) {
                    auto stCount = std::move(stCountR).value();
                    auto step = stCount.step();
                    if (step && step.value())
                        r.metadataDocuments = static_cast<uint64_t>(stCount.getInt64(0));
                }

                // Find documents whose sha256_hash has no corresponding CAS object
                auto stDocsR = db->prepare("SELECT id, file_path, sha256_hash FROM documents WHERE "
                                           "sha256_hash IS NOT NULL "
                                           "AND extraction_status != 'orphaned' ORDER BY id");
                if (stDocsR) {
                    auto stDocs = std::move(stDocsR).value();
                    while (true) {
                        auto step = stDocs.step();
                        if (!step || !step.value())
                            break;

                        std::string hash = stDocs.getString(2);
                        if (hash.empty())
                            continue;

                        if (objectHashes.find(hash) == objectHashes.end()) {
                            r.missingBlobs++;
                            if (r.missingBlobDetails.size() < 50) {
                                r.missingBlobDetails.push_back({hash, stDocs.getString(1), hash});
                            }
                        }
                    }
                }
            }
        } catch (...) {
        }

    // ── Detect orphaned storage objects (no corresponding metadata doc) ──
    // Only meaningful when DB is available; all objects are treated as
    // potentially referenced when DB is missing.
    if (!objectHashes.empty() && dbAvailable) {
        try {
            auto db = ctx.cli()->getDatabase();
            if (db && db->isOpen()) {
                auto stHashR =
                    db->prepare("SELECT sha256_hash FROM documents WHERE sha256_hash IS NOT NULL");
                if (stHashR) {
                    std::set<std::string> docHashes;
                    auto stHash = std::move(stHashR).value();
                    while (true) {
                        auto step = stHash.step();
                        if (!step || !step.value())
                            break;
                        std::string h = stHash.getString(0);
                        if (!h.empty())
                            docHashes.insert(h);
                    }
                    for (const auto& oh : objectHashes) {
                        if (docHashes.find(oh) == docHashes.end()) {
                            r.orphanedBlobs++;
                            if (r.orphanedBlobDetails.size() < 50) {
                                r.orphanedBlobDetails.push_back({oh, 0});
                            }
                        }
                    }
                }
            }
        } catch (...) {
        }
    }

    r.ok = (r.missingBlobs == 0 && r.orphanedBlobs == 0);
    return r;
}

void StorageBlobCheck::render(std::ostream& os, const Result& r) {
    using namespace yams::cli::ui;

    if (r.storageObjects == 0 && r.metadataDocuments == 0) {
        os << "  " << status_ok("No storage blobs — corpus is empty.") << "\n";
        return;
    }

    os << "  " << status_ok("Storage objects: ") << format_number(r.storageObjects) << " objects";
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

    if (r.missingBlobs > 0) {
        os << "  " << status_error("Missing blobs (metadata ref without CAS object): ")
           << format_number(r.missingBlobs) << "\n";
        size_t show = std::min(r.missingBlobDetails.size(), size_t(5));
        for (size_t i = 0; i < show; ++i) {
            const auto& d = r.missingBlobDetails[i];
            os << "    " << bullet(d.filePath)
               << "  hash: " << colorize(d.sha256Hash.substr(0, 16) + "...", Ansi::DIM) << "\n";
        }
    }

    if (r.orphanedBlobs > 0) {
        os << "  " << status_warning("Orphaned objects (CAS without metadata ref): ")
           << format_number(r.orphanedBlobs) << "\n";
    }

    if (r.ok)
        os << "  " << status_ok("Storage blob integrity: OK") << "\n";
    else
        os << "  " << status_error("Storage blob integrity: ISSUES DETECTED") << "\n";
}

} // namespace yams::cli::doctor
