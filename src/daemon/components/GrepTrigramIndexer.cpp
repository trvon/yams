// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#include <yams/daemon/components/GrepTrigramIndexer.h>

#include <spdlog/spdlog.h>

#include <string_view>
#include <utility>

namespace yams::daemon {

GrepTrigramIndexer::GrepTrigramIndexer(std::shared_ptr<metadata::IMetadataRepository> repository,
                                       Options options)
    : repository_(std::move(repository)), options_(std::move(options)) {}

bool GrepTrigramIndexer::available() const {
    return repository_ && options_.fetchBytes && repository_->grepTrigramIndexAvailable();
}

GrepTrigramIndexer::PassResult GrepTrigramIndexer::runPass() {
    PassResult result;
    if (!available()) {
        return result;
    }
    // Enough to fill a batch past the skip list, plus one to tell whether more are waiting.
    auto pending = repository_->grepTrigramPending(options_.batchSize + skipped_.size() + 1,
                                                   options_.maxDocumentBytes);
    if (!pending) {
        spdlog::debug("[grep-trigram] pending query failed: {}", pending.error().message);
        return result;
    }
    std::size_t attempted = 0;
    for (const auto& doc : pending.value()) {
        if (skipped_.contains(doc.documentId)) {
            continue;
        }
        if (attempted == options_.batchSize) {
            result.morePending = true;
            break;
        }
        if ((options_.stopRequested && options_.stopRequested()) ||
            (options_.admit && !options_.admit())) {
            result.interrupted = true;
            return result;
        }
        ++attempted;
        auto bytes = options_.fetchBytes(doc.sha256Hash);
        if (!bytes) {
            spdlog::debug("[grep-trigram] cannot read {}: {}", doc.sha256Hash,
                          bytes.error().message);
            skipped_.insert(doc.documentId);
            ++result.failed;
            continue;
        }
        const std::string_view text(reinterpret_cast<const char*>(bytes.value().data()),
                                    bytes.value().size());
        if (auto indexed = repository_->indexGrepTrigrams(doc.documentId, doc.sha256Hash, text);
            !indexed) {
            spdlog::debug("[grep-trigram] cannot index {}: {}", doc.sha256Hash,
                          indexed.error().message);
            skipped_.insert(doc.documentId);
            ++result.failed;
            continue;
        }
        ++result.indexed;
    }
    return result;
}

} // namespace yams::daemon
