// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include "repair_operation_support.h"

#include <yams/detection/file_type_detector.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cctype>
#include <exception>

namespace yams::daemon::repair {

std::string normalizedRepairExtension(const metadata::DocumentInfo& doc) {
    std::string extension = doc.fileExtension;
    if (extension.empty()) {
        auto pos = doc.fileName.rfind('.');
        if (pos != std::string::npos)
            extension = doc.fileName.substr(pos);
    }
    if (extension.empty())
        return {};
    if (extension.front() != '.')
        extension.insert(extension.begin(), '.');
    std::transform(extension.begin(), extension.end(), extension.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return extension;
}

bool shouldRedetectMime(const metadata::DocumentInfo& doc) {
    if (doc.mimeType.empty() || doc.mimeType == "application/octet-stream")
        return true;

    if (doc.mimeType != "text/plain")
        return false;

    const auto extension = normalizedRepairExtension(doc);
    if (extension.empty())
        return true;

    const auto hintedMime = detection::FileTypeDetector::getMimeTypeFromExtension(extension);
    return !hintedMime.empty() && hintedMime != "text/plain";
}

std::string bestEffortMimeForDocument(const metadata::DocumentInfo& doc,
                                      const std::shared_ptr<api::IContentStore>& store) {
    try {
        (void)detection::FileTypeDetector::initializeWithMagicNumbers();
        auto& detector = detection::FileTypeDetector::instance();
        const auto extension = normalizedRepairExtension(doc);
        const auto hintedMime =
            extension.empty() ? std::string{}
                              : detection::FileTypeDetector::getMimeTypeFromExtension(extension);

        if (store) {
            // MIME repair only needs the leading magic bytes. Avoid materializing full objects
            // here: `repair --all` can scan large corpora and full reads can stall streaming
            // progress or exhaust daemon memory, surfacing to the CLI as an IPC EOF if the daemon
            // is killed.
            constexpr std::size_t kMimeSniffBytes = 8192;
            auto bytesResult = store->retrieveBytesPrefix(doc.sha256Hash, kMimeSniffBytes);
            if (bytesResult && !bytesResult.value().empty()) {
                const auto& bytes = bytesResult.value();
                auto detected = detector.detectFromBuffer(
                    std::span<const std::byte>(bytes.data(), bytes.size()));
                if (detected) {
                    std::string mime = detected.value().mimeType;
                    if (!hintedMime.empty() && hintedMime != "application/octet-stream") {
                        const auto detectedType = detector.getFileTypeCategory(mime);
                        const auto hintedType = detector.getFileTypeCategory(hintedMime);
                        if (mime.empty() || mime == "application/octet-stream" ||
                            (mime == "text/plain" && hintedMime != "text/plain") ||
                            (detectedType == "executable" && hintedType == "executable" &&
                             mime != hintedMime)) {
                            mime = hintedMime;
                        }
                    }
                    if (!mime.empty())
                        return mime;
                }
            }
        }

        if (!doc.filePath.empty() && std::filesystem::exists(doc.filePath)) {
            if (auto detected = detector.detectFromFile(doc.filePath)) {
                const auto& mime = detected.value().mimeType;
                if (!mime.empty())
                    return mime;
            }
        }

        if (!extension.empty()) {
            const auto mime = hintedMime;
            if (!mime.empty())
                return mime;
        }
    } catch (const std::exception& e) {
        spdlog::debug("RepairService: MIME sniff failed for {}: {}", doc.sha256Hash, e.what());
    } catch (...) {
        spdlog::debug("RepairService: MIME sniff failed for {}: unknown exception", doc.sha256Hash);
    }

    return "application/octet-stream";
}

} // namespace yams::daemon::repair
