// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <string>
#include <vector>

#include <yams/metadata/metadata_repository.h>

#include "crud_ops.hpp"

namespace yams::metadata {

// Search history operations (refactored with YAMS_TRY - ADR-0004 Phase 2)
Result<int64_t> MetadataRepository::insertSearchHistory(const SearchHistoryEntry& entry) {
    return executeQuery<int64_t>([&](Database& db) -> Result<int64_t> {
        repository::CrudOps<SearchHistoryEntry> ops;
        return ops.insert(db, entry);
    });
}

Result<std::vector<SearchHistoryEntry>> MetadataRepository::getRecentSearches(int limit) {
    return executeReadQuery<std::vector<SearchHistoryEntry>>(
        [&](Database& db) -> Result<std::vector<SearchHistoryEntry>> {
            repository::CrudOps<SearchHistoryEntry> ops;
            return ops.getAllOrdered(db, "query_time DESC", limit);
        });
}

Result<int64_t> MetadataRepository::insertFeedbackEvent(const FeedbackEvent& event) {
    // Best-effort: feedback events are telemetry. If the DB is locked by concurrent
    // writes, drop the event rather than blocking the worker thread for 15+ seconds
    // in busy_timeout + retry loops, which causes system-wide starvation.
    return executeBestEffortWrite<int64_t>([&](Database& db) -> Result<int64_t> {
        repository::CrudOps<FeedbackEvent> ops;
        return ops.insert(db, event);
    });
}

Result<std::vector<FeedbackEvent>>
MetadataRepository::getFeedbackEventsByTrace(const std::string& traceId, int limit) {
    return executeReadQuery<std::vector<FeedbackEvent>>(
        [&](Database& db) -> Result<std::vector<FeedbackEvent>> {
            repository::CrudOps<FeedbackEvent> ops;
            return ops.query(db, "trace_id = ? ORDER BY created_at DESC", traceId, limit);
        });
}

Result<std::vector<FeedbackEvent>> MetadataRepository::getRecentFeedbackEvents(int limit) {
    return executeReadQuery<std::vector<FeedbackEvent>>(
        [&](Database& db) -> Result<std::vector<FeedbackEvent>> {
            repository::CrudOps<FeedbackEvent> ops;
            return ops.getAllOrdered(db, "created_at DESC", limit);
        });
}

// Saved queries operations (refactored with YAMS_TRY - ADR-0004 Phase 2)
Result<int64_t> MetadataRepository::insertSavedQuery(const SavedQuery& query) {
    return executeQuery<int64_t>([&](Database& db) -> Result<int64_t> {
        repository::CrudOps<SavedQuery> ops;
        return ops.insert(db, query);
    });
}

Result<std::optional<SavedQuery>> MetadataRepository::getSavedQuery(int64_t id) {
    return executeReadQuery<std::optional<SavedQuery>>(
        [&](Database& db) -> Result<std::optional<SavedQuery>> {
            repository::CrudOps<SavedQuery> ops;
            return ops.getById(db, id);
        });
}

Result<std::vector<SavedQuery>> MetadataRepository::getAllSavedQueries() {
    return executeReadQuery<std::vector<SavedQuery>>(
        [&](Database& db) -> Result<std::vector<SavedQuery>> {
            repository::CrudOps<SavedQuery> ops;
            return ops.getAllOrdered(db, "use_count DESC, last_used DESC");
        });
}

Result<void> MetadataRepository::updateSavedQuery(const SavedQuery& query) {
    return executeQuery<void>([&](Database& db) -> Result<void> {
        repository::CrudOps<SavedQuery> ops;
        return ops.update(db, query);
    });
}

Result<void> MetadataRepository::deleteSavedQuery(int64_t id) {
    return executeQuery<void>([&](Database& db) -> Result<void> {
        repository::CrudOps<SavedQuery> ops;
        return ops.deleteById(db, id);
    });
}

// Full-text search operations
} // namespace yams::metadata
