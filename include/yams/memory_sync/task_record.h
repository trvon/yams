// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include <algorithm>
#include <array>
#include <limits>
#include <string>
#include <string_view>
#include <unordered_set>

#include <nlohmann/json.hpp>
#include <yams/core/types.h>
#include <yams/memory_sync/version_vector.h>

namespace yams::memory_sync {

inline constexpr std::string_view kTaskRecordPrefix = "task-record/";
inline constexpr std::size_t kMaxTaskRecordBytes = 1024 * 1024;

// A task identity survives source revisions. The publication key's final segment
// is SHA-256 of the exact JSON bytes, not a mutable task-head alias. Supersession
// references are revision hashes within the same task; they need not be locally
// hydrated yet. They do not imply that either record is trusted or executable.
inline Result<void> validateTaskRecordPublication(std::string_view key, std::string_view value,
                                                  std::string_view contentHash) {
    const auto invalid = [](std::string message) -> Result<void> {
        return Error{ErrorCode::InvalidArgument, std::move(message)};
    };
    if (value.size() > kMaxTaskRecordBytes) {
        return invalid("task record exceeds 1 MiB");
    }
    std::unordered_set<std::string> fieldNames;
    bool duplicateField = false;
    const auto record = nlohmann::json::parse(
        value,
        [&](int, nlohmann::json::parse_event_t event, nlohmann::json& parsed) {
            if (event == nlohmann::json::parse_event_t::key) {
                duplicateField |= !fieldNames.insert(parsed.get<std::string>()).second;
            }
            return true;
        },
        false);
    if (!record.is_object() || duplicateField) {
        return invalid("task record must be a JSON object without duplicate fields");
    }
    static constexpr std::array<std::string_view, 11> fields = {"schema",
                                                                "task_id",
                                                                "kind",
                                                                "owner",
                                                                "source_uri",
                                                                "source_revision",
                                                                "recorded_at_ms",
                                                                "source_timestamp_ms",
                                                                "supersedes",
                                                                "ownership_semantics",
                                                                "body"};
    if (record.size() != fields.size() ||
        !std::all_of(fields.begin(), fields.end(),
                     [&](auto field) { return record.contains(field); })) {
        return invalid("task record fields do not match yams.task-record/v1");
    }
    const auto text = [&](std::string_view field, std::size_t limit) {
        const auto& item = record.at(field);
        if (!item.is_string()) {
            return false;
        }
        const auto& str = item.get_ref<const std::string&>();
        return !str.empty() && str.size() <= limit &&
               std::none_of(str.begin(), str.end(),
                            [](unsigned char c) { return c < 32 || c == 127; });
    };
    if (record.at("schema") != "yams.task-record/v1" ||
        record.at("ownership_semantics") != "record_only") {
        return invalid(
            "task claims are descriptive records, not acquired leases or fencing tokens");
    }
    if (!text("task_id", 36) || !text("owner", 128) || !text("source_uri", 4096) ||
        !text("source_revision", 256) || !record.at("body").is_string()) {
        return invalid(
            "task record requires task identity, owner, source provenance and text body");
    }
    const auto& taskId = record.at("task_id").get_ref<const std::string&>();
    if (taskId.size() != 36) {
        return invalid("task_id must be a canonical lowercase UUID");
    }
    for (std::size_t i = 0; i < taskId.size(); ++i) {
        const char c = taskId[i];
        const bool separator = i == 8 || i == 13 || i == 18 || i == 23;
        if (separator ? c != '-' : !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
            return invalid("task_id must be a canonical lowercase UUID");
        }
    }
    const auto& kind = record.at("kind");
    if (kind != "claim" && kind != "note" && kind != "decision" && kind != "checkpoint" &&
        kind != "completion") {
        return invalid("unsupported task record kind");
    }
    for (const auto field : {"recorded_at_ms", "source_timestamp_ms"}) {
        const auto& stamp = record.at(field);
        const bool valid =
            stamp.is_number_unsigned()
                ? stamp.get<std::uint64_t>() > 0 &&
                      stamp.get<std::uint64_t>() <= std::numeric_limits<std::int64_t>::max()
                : stamp.is_number_integer() && stamp.get<std::int64_t>() > 0;
        if (!valid) {
            return invalid("task timestamps must be positive signed-64-bit Unix milliseconds");
        }
    }
    const auto canonicalHash = [](std::string_view hash) {
        return hash.size() == 64 && std::all_of(hash.begin(), hash.end(), [](char c) {
                   return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
               });
    };
    if (!canonicalHash(contentHash) ||
        key != std::string(kTaskRecordPrefix) + taskId + "/" + std::string(contentHash)) {
        return invalid("task record key must bind task_id and SHA-256 of the exact JSON bytes");
    }
    const auto& links = record.at("supersedes");
    if (!links.is_array() || links.size() > 64) {
        return invalid("supersedes must contain at most 64 revision hashes");
    }
    std::unordered_set<std::string> seen;
    for (const auto& link : links) {
        if (!link.is_string()) {
            return invalid("supersession reference must be a SHA-256 hash");
        }
        const auto& hash = link.get_ref<const std::string&>();
        if (!canonicalHash(hash) || hash == contentHash || !seen.insert(hash).second) {
            return invalid(
                "supersession references must be distinct non-self lowercase SHA-256 hashes");
        }
    }
    return {};
}

} // namespace yams::memory_sync
