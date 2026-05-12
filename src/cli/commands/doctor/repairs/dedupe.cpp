#include <yams/cli/doctor/repairs/dedupe.h>

#include <yams/cli/doctor/rendering/render.h>
#include <yams/cli/ui_helpers.hpp>
#include <yams/cli/yams_cli.h>
#include <yams/config/config_helpers.h>
#include <yams/vector/vector_database.h>

#include <nlohmann/json.hpp>

#include <algorithm>
#include <chrono>
#include <cctype>
#include <filesystem>
#include <iomanip>
#include <numeric>
#include <optional>
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace yams::cli::doctor {

namespace {

struct SemanticDedupeMatch {
    size_t lhs{0};
    size_t rhs{0};
    double cosine{0.0};
    double titleOverlap{0.0};
    double pathOverlap{0.0};
    double score{0.0};
};

struct SemanticDedupeGroupPlan {
    std::vector<size_t> members;
    size_t canonicalIndex{0};
};

struct SemanticDedupeAnalysis {
    struct Row {
        metadata::DocumentInfo doc;
        std::string normalizedTitle;
        std::string normalizedPath;
    };

    std::vector<Row> docs;
    std::vector<SemanticDedupeMatch> accepted;
    std::vector<SemanticDedupeGroupPlan> groups;
};

float cosineSimilarity(const std::vector<float>& a, const std::vector<float>& b) {
    if (a.empty() || b.empty() || a.size() != b.size()) {
        return 0.0f;
    }
    return static_cast<float>(vector::VectorDatabase::computeCosineSimilarity(a, b));
}

std::string normalizeTextForTokens(std::string value) {
    for (char& c : value) {
        if (!std::isalnum(static_cast<unsigned char>(c))) {
            c = ' ';
        } else {
            c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        }
    }

    std::ostringstream out;
    std::istringstream in(value);
    std::string token;
    bool first = true;
    while (in >> token) {
        if (!first) {
            out << ' ';
        }
        out << token;
        first = false;
    }
    return out.str();
}

std::unordered_set<std::string> tokenSet(std::string_view text) {
    std::unordered_set<std::string> tokens;
    std::istringstream in{std::string(text)};
    std::string token;
    while (in >> token) {
        tokens.insert(token);
    }
    return tokens;
}

double jaccardOverlap(std::string_view lhs, std::string_view rhs) {
    const auto lt = tokenSet(lhs);
    const auto rt = tokenSet(rhs);
    if (lt.empty() || rt.empty()) {
        return 0.0;
    }

    size_t intersection = 0;
    for (const auto& token : lt) {
        if (rt.contains(token)) {
            ++intersection;
        }
    }
    const size_t uni = lt.size() + rt.size() - intersection;
    if (uni == 0) {
        return 0.0;
    }
    return static_cast<double>(intersection) / static_cast<double>(uni);
}

std::optional<SemanticDedupeAnalysis>
analyzeSemanticDuplicates(metadata::MetadataRepository& metadataRepo,
                          vector::VectorDatabase& vectorDb, const DedupeCommand::Config& cfg) {
    auto docsResult = metadataRepo.queryDocuments(metadata::DocumentQueryOptions{});
    if (!docsResult) {
        return std::nullopt;
    }

    SemanticDedupeAnalysis analysis;
    analysis.docs.reserve(docsResult.value().size());
    for (const auto& doc : docsResult.value()) {
        if (doc.sha256Hash.empty() || !vectorDb.hasEmbedding(doc.sha256Hash)) {
            continue;
        }
        analysis.docs.push_back(SemanticDedupeAnalysis::Row{
            doc, normalizeTextForTokens(doc.fileName), normalizeTextForTokens(doc.filePath)});
    }

    if (analysis.docs.empty()) {
        return analysis;
    }

    std::vector<int> parent(analysis.docs.size());
    std::iota(parent.begin(), parent.end(), 0);
    auto findRoot = [&](int idx) {
        int root = idx;
        while (parent[root] != root) {
            root = parent[root];
        }
        while (parent[idx] != idx) {
            int next = parent[idx];
            parent[idx] = root;
            idx = next;
        }
        return root;
    };
    auto unite = [&](int lhs, int rhs) {
        lhs = findRoot(lhs);
        rhs = findRoot(rhs);
        if (lhs != rhs) {
            parent[rhs] = lhs;
        }
    };

    for (size_t i = 0; i < analysis.docs.size(); ++i) {
        vector::VectorSearchParams params;
        params.k = 6;
        params.similarity_threshold = static_cast<float>(cfg.semanticThreshold - 0.08);
        auto neighbors = vectorDb.searchSimilarToDocument(analysis.docs[i].doc.sha256Hash, params);
        auto baseVectors = vectorDb.getVectorsByDocument(analysis.docs[i].doc.sha256Hash);
        if (baseVectors.empty()) {
            continue;
        }

        for (const auto& neighbor : neighbors) {
            if (neighbor.document_hash.empty() ||
                neighbor.document_hash == analysis.docs[i].doc.sha256Hash) {
                continue;
            }

            auto it =
                std::find_if(analysis.docs.begin(), analysis.docs.end(), [&](const auto& row) {
                    return row.doc.sha256Hash == neighbor.document_hash;
                });
            if (it == analysis.docs.end()) {
                continue;
            }

            const size_t j = static_cast<size_t>(std::distance(analysis.docs.begin(), it));
            if (j <= i) {
                continue;
            }

            auto neighborVectors = vectorDb.getVectorsByDocument(it->doc.sha256Hash);
            if (neighborVectors.empty()) {
                continue;
            }

            const double cosine =
                cosineSimilarity(baseVectors.front().embedding, neighborVectors.front().embedding);
            const double titleOverlap =
                jaccardOverlap(analysis.docs[i].normalizedTitle, it->normalizedTitle);
            const double pathOverlap =
                jaccardOverlap(analysis.docs[i].normalizedPath, it->normalizedPath);
            const double score = cosine * 0.8 + titleOverlap * 0.15 + pathOverlap * 0.05;

            if (cosine < cfg.semanticThreshold) {
                continue;
            }
            if (titleOverlap == 0.0 && pathOverlap == 0.0 && cosine < 0.975) {
                continue;
            }

            analysis.accepted.push_back(
                SemanticDedupeMatch{i, j, cosine, titleOverlap, pathOverlap, score});
            unite(static_cast<int>(i), static_cast<int>(j));
        }
    }

    std::unordered_map<int, std::vector<size_t>> groupedMembers;
    for (size_t i = 0; i < analysis.docs.size(); ++i) {
        groupedMembers[findRoot(static_cast<int>(i))].push_back(i);
    }

    for (auto& [root, members] : groupedMembers) {
        (void)root;
        if (members.size() < 2) {
            continue;
        }

        auto cmpNewest = [&](size_t lhs, size_t rhs) {
            return analysis.docs[lhs].doc.modifiedTime > analysis.docs[rhs].doc.modifiedTime;
        };
        auto cmpOldest = [&](size_t lhs, size_t rhs) {
            return analysis.docs[lhs].doc.modifiedTime < analysis.docs[rhs].doc.modifiedTime;
        };
        auto cmpLargest = [&](size_t lhs, size_t rhs) {
            return analysis.docs[lhs].doc.fileSize > analysis.docs[rhs].doc.fileSize;
        };
        if (cfg.strategy == "keep-oldest") {
            std::sort(members.begin(), members.end(), cmpOldest);
        } else if (cfg.strategy == "keep-largest") {
            std::sort(members.begin(), members.end(), cmpLargest);
        } else {
            std::sort(members.begin(), members.end(), cmpNewest);
        }

        analysis.groups.push_back(SemanticDedupeGroupPlan{members, members.front()});
    }

    return analysis;
}

Result<void> persistSemanticDuplicateAnalysis(metadata::MetadataRepository& metadataRepo,
                                              const SemanticDedupeAnalysis& analysis,
                                              const DedupeCommand::Config& cfg) {
    const auto now =
        std::chrono::time_point_cast<std::chrono::seconds>(std::chrono::system_clock::now());

    for (const auto& groupPlan : analysis.groups) {
        nlohmann::json evidence;
        evidence["strategy"] = cfg.strategy;
        evidence["threshold"] = cfg.semanticThreshold;
        evidence["documents"] = nlohmann::json::array();

        std::vector<std::string> hashes;
        hashes.reserve(groupPlan.members.size());
        double maxPairScore = 0.0;
        for (size_t memberIndex : groupPlan.members) {
            const auto& doc = analysis.docs[memberIndex].doc;
            hashes.push_back(doc.sha256Hash);
            evidence["documents"].push_back(
                {{"id", doc.id}, {"hash", doc.sha256Hash}, {"path", doc.filePath}});
        }
        std::sort(hashes.begin(), hashes.end());

        for (const auto& match : analysis.accepted) {
            const bool lhsInGroup = std::find(groupPlan.members.begin(), groupPlan.members.end(),
                                              match.lhs) != groupPlan.members.end();
            const bool rhsInGroup = std::find(groupPlan.members.begin(), groupPlan.members.end(),
                                              match.rhs) != groupPlan.members.end();
            if (lhsInGroup && rhsInGroup) {
                maxPairScore = std::max(maxPairScore, match.score);
            }
        }

        std::ostringstream keyBuilder;
        keyBuilder << "semantic:" << cfg.strategy << ':' << std::fixed << std::setprecision(3)
                   << cfg.semanticThreshold << ':';
        for (size_t i = 0; i < hashes.size(); ++i) {
            if (i > 0) {
                keyBuilder << ',';
            }
            keyBuilder << hashes[i];
        }

        metadata::SemanticDuplicateGroup group;
        group.groupKey = keyBuilder.str();
        group.algorithmVersion = "semantic-dedupe-v1";
        group.status = "suggested";
        group.reviewState = "pending";
        group.canonicalDocumentId = analysis.docs[groupPlan.canonicalIndex].doc.id;
        group.memberCount = static_cast<int64_t>(groupPlan.members.size());
        group.maxPairScore = maxPairScore;
        group.threshold = cfg.semanticThreshold;
        group.evidenceJson = evidence.dump();
        group.createdAt = now;
        if (auto existing = metadataRepo.getSemanticDuplicateGroupByKey(group.groupKey);
            existing && existing.value().has_value()) {
            group.createdAt = existing.value()->createdAt;
        }
        group.updatedAt = now;
        group.lastComputedAt = now;

        auto groupId = metadataRepo.upsertSemanticDuplicateGroup(group);
        if (!groupId) {
            return groupId.error();
        }

        std::vector<metadata::SemanticDuplicateGroupMember> members;
        members.reserve(groupPlan.members.size());
        for (size_t memberIndex : groupPlan.members) {
            metadata::SemanticDuplicateGroupMember member;
            member.groupId = groupId.value();
            member.documentId = analysis.docs[memberIndex].doc.id;
            member.role = (memberIndex == groupPlan.canonicalIndex) ? "canonical" : "duplicate";
            member.decision = (memberIndex == groupPlan.canonicalIndex) ? "keep" : "unknown";
            member.reason = (memberIndex == groupPlan.canonicalIndex) ? cfg.strategy : "";
            member.createdAt = now;
            member.updatedAt = now;

            if (memberIndex != groupPlan.canonicalIndex) {
                for (const auto& match : analysis.accepted) {
                    const bool directPair =
                        (match.lhs == groupPlan.canonicalIndex && match.rhs == memberIndex) ||
                        (match.lhs == memberIndex && match.rhs == groupPlan.canonicalIndex);
                    if (directPair) {
                        member.similarityToCanonical = match.cosine;
                        member.titleOverlap = match.titleOverlap;
                        member.pathOverlap = match.pathOverlap;
                        member.pairScore = match.score;
                        break;
                    }
                }
            }

            members.push_back(std::move(member));
        }

        auto memberResult =
            metadataRepo.replaceSemanticDuplicateGroupMembers(groupId.value(), members);
        if (!memberResult) {
            return memberResult.error();
        }
    }

    return Result<void>();
}

} // namespace

void DedupeCommand::execute(std::ostream& os, YamsCLI* cli, const Config& cfg) {
    (void)os;

    try {
        DoctorRender::printHeader(std::cout, "Document Dedupe");
        if (!cli) {
            std::cout << "  " << ui::status_error("CLI context unavailable") << "\n";
            return;
        }

        if (cfg.mode == "semantic") {
            auto ensured = cli->ensureStorageInitialized();
            if (!ensured) {
                std::cout << "  "
                          << ui::status_error("Storage init failed: " + ensured.error().message)
                          << "\n";
                return;
            }

            auto metadataRepo = cli->getMetadataRepository();
            auto vectorDb = cli->getVectorDatabase();
            if (!metadataRepo || !vectorDb) {
                std::cout << "  " << ui::status_error("Metadata or vector database unavailable")
                          << "\n";
                return;
            }

            auto analysis = analyzeSemanticDuplicates(*metadataRepo, *vectorDb, cfg);
            if (!analysis.has_value()) {
                std::cout << "  " << ui::status_error("Semantic dedupe query failed") << "\n";
                return;
            }
            if (analysis->docs.empty()) {
                std::cout << "  "
                          << ui::status_ok("No embedded documents available for semantic dedupe")
                          << "\n";
                return;
            }
            if (analysis->groups.empty()) {
                std::cout << "  " << ui::status_ok("No semantic duplicate groups") << "\n";
                return;
            }

            size_t candDel = 0;
            for (const auto& group : analysis->groups) {
                candDel += group.members.size() - 1;
                if (cfg.verbose) {
                    std::cout << "Group: semantic keep="
                              << analysis->docs[group.canonicalIndex].doc.id
                              << " total=" << group.members.size() << "\n";
                    for (size_t rank = 0; rank < group.members.size(); ++rank) {
                        const auto& doc = analysis->docs[group.members[rank]].doc;
                        std::cout << (rank == 0 ? "  KEEP" : "  DEL ") << " id=" << doc.id
                                  << " hash=" << doc.sha256Hash.substr(0, 8)
                                  << " size=" << doc.fileSize << " path=" << doc.filePath << "\n";
                    }
                }
            }

            auto persistResult = persistSemanticDuplicateAnalysis(*metadataRepo, *analysis, cfg);
            if (!persistResult) {
                std::cout << "  "
                          << ui::status_error("Persist failed: " + persistResult.error().message)
                          << "\n";
                return;
            }

            DoctorRender::printStatusLine(std::cout, "Embedded documents",
                                          std::to_string(analysis->docs.size()));
            DoctorRender::printStatusLine(std::cout, "Semantic duplicate groups",
                                          std::to_string(analysis->groups.size()));
            DoctorRender::printStatusLine(std::cout, "Deletion candidates",
                                          std::to_string(candDel));
            DoctorRender::printStatusLine(std::cout, "Semantic threshold",
                                          std::to_string(cfg.semanticThreshold));
            std::cout << "  " << ui::status_ok("Saved semantic duplicate suggestions to metadata")
                      << "\n";
            if (candDel > 0) {
                std::cout << "  "
                          << ui::status_info(
                                 "Use 'yams repair --dedupe' to remove semantic duplicates.")
                          << "\n";
            }

            if (cfg.verbose && !analysis->accepted.empty()) {
                auto accepted = analysis->accepted;
                std::sort(accepted.begin(), accepted.end(),
                          [](const auto& lhs, const auto& rhs) { return lhs.score > rhs.score; });
                size_t shown = 0;
                for (const auto& match : accepted) {
                    const auto& lhs = analysis->docs[match.lhs].doc;
                    const auto& rhs = analysis->docs[match.rhs].doc;
                    std::cout << "Pair: " << lhs.id << " <-> " << rhs.id << " cosine=" << std::fixed
                              << std::setprecision(3) << match.cosine
                              << " title_overlap=" << match.titleOverlap
                              << " path_overlap=" << match.pathOverlap << " score=" << match.score
                              << "\n";
                    if (++shown >= 10) {
                        break;
                    }
                }
            }
            return;
        }

        namespace fs = std::filesystem;
        fs::path dataDir = cli->getDataPath();

        std::string dbName = "yams.db";
        try {
            auto parsed = yams::config::parse_simple_toml(yams::config::get_config_path());
            auto it = parsed.find("knowledge_graph.db_path");
            if (it != parsed.end() && !it->second.empty()) {
                dbName = it->second;
            }
        } catch (...) {
        }

        fs::path dbPath = fs::path(dbName).is_absolute() ? fs::path(dbName) : dataDir / dbName;
        if (!fs::exists(dbPath)) {
            std::cout << "  "
                      << ui::status_error(
                             "metadata database not found (knowledge_graph.db_path): " +
                             dbPath.string())
                      << "\n";
            return;
        }
        yams::metadata::Database db;
        auto ro = db.open(dbPath.string(), yams::metadata::ConnectionMode::ReadWrite);
        if (!ro) {
            std::cout << "  " << ui::status_error(std::string("Open failed: ") + ro.error().message)
                      << "\n";
            return;
        }
        auto ps = db.prepare(
            "SELECT id,file_path,file_name,file_size,sha256_hash,modified_time,indexed_time FROM "
            "documents");
        if (!ps) {
            std::cout << "  " << ui::status_error("Prepare failed: " + ps.error().message) << "\n";
            return;
        }
        yams::metadata::Statement stmt = std::move(ps).value();
        struct Row {
            int64_t id;
            std::string path;
            std::string name;
            int64_t size;
            std::string hash;
            int64_t mtime;
            int64_t itime;
        };
        std::vector<Row> rows;
        while (true) {
            auto step = stmt.step();
            if (!step) {
                std::cout << "  " << ui::status_error("Step error: " + step.error().message)
                          << "\n";
                return;
            }
            if (!step.value())
                break;
            rows.push_back({stmt.getInt64(0), stmt.getString(1), stmt.getString(2),
                            stmt.getInt64(3), stmt.getString(4), stmt.getInt64(5),
                            stmt.getInt64(6)});
        }
        if (rows.empty()) {
            std::cout << "  " << ui::status_ok("No documents") << "\n";
            return;
        }
        struct G {
            Row r;
        };
        std::unordered_map<std::string, std::vector<G>> groups;
        auto keyFn = [&](const Row& r) -> std::string {
            if (cfg.mode == "name")
                return r.name;
            if (cfg.mode == "hash")
                return r.hash;
            return r.path;
        };
        for (auto& r : rows)
            groups[keyFn(r)].push_back({r});
        size_t dupGroups = 0, candDel = 0, skipped = 0;
        std::vector<int64_t> toDelete;
        for (auto& [k, vec] : groups) {
            if (vec.size() < 2)
                continue;
            bool hashesDiffer = false;
            std::string h0 = vec.front().r.hash;
            for (auto& gi : vec)
                if (gi.r.hash != h0) {
                    hashesDiffer = true;
                    break;
                }
            if (hashesDiffer && !cfg.force) {
                skipped++;
                continue;
            }
            dupGroups++;
            auto newest = [](const G& a, const G& b) { return a.r.mtime > b.r.mtime; };
            auto oldest = [](const G& a, const G& b) { return a.r.mtime < b.r.mtime; };
            auto largest = [](const G& a, const G& b) { return a.r.size > b.r.size; };
            if (cfg.strategy == "keep-oldest")
                std::sort(vec.begin(), vec.end(), oldest);
            else if (cfg.strategy == "keep-largest")
                std::sort(vec.begin(), vec.end(), largest);
            else
                std::sort(vec.begin(), vec.end(), newest);
            for (size_t i = 1; i < vec.size(); ++i)
                toDelete.push_back(vec[i].r.id);
            candDel += vec.size() - 1;
            if (cfg.verbose) {
                std::cout << "Group: " << k << " keep=" << vec[0].r.id << " total=" << vec.size()
                          << (hashesDiffer ? " (hash-diff)" : "") << "\n";
                for (size_t i = 0; i < vec.size(); ++i) {
                    const auto& rr = vec[i].r;
                    std::cout << (i == 0 ? "  KEEP" : "  DEL ") << " id=" << rr.id
                              << " hash=" << rr.hash.substr(0, 8) << " size=" << rr.size
                              << " mtime=" << rr.mtime << " itime=" << rr.itime << "\n";
                }
            }
        }
        if (!dupGroups) {
            std::cout << "  " << ui::status_ok("No duplicate groups (mode=" + cfg.mode + ")")
                      << "\n";
            return;
        }
        DoctorRender::printStatusLine(std::cout, "Total documents", std::to_string(rows.size()));
        DoctorRender::printStatusLine(std::cout, "Duplicate groups", std::to_string(dupGroups));
        DoctorRender::printStatusLine(std::cout, "Deletion candidates", std::to_string(candDel));
        if (skipped) {
            DoctorRender::printStatusLine(std::cout, "Skipped (hash mismatch, use --force)",
                                          std::to_string(skipped));
        }
        if (!cfg.apply) {
            std::cout << "  " << ui::status_warning("Dry-run. Use --apply to delete.") << "\n";
            return;
        }
        if (toDelete.empty()) {
            std::cout << "  " << ui::status_ok("Nothing to delete") << "\n";
            return;
        }
        auto br = db.execute("BEGIN TRANSACTION");
        if (!br) {
            std::cout << "  " << ui::status_error("BEGIN failed: " + br.error().message) << "\n";
            return;
        }
        bool err = false;
        for (auto id : toDelete) {
            auto psd = db.prepare("DELETE FROM documents WHERE id=?");
            if (!psd) {
                std::cout << "  " << ui::status_error("Prepare delete failed") << "\n";
                err = true;
                break;
            }
            auto st2 = std::move(psd).value();
            auto b = st2.bind(1, id);
            if (!b) {
                err = true;
                break;
            }
            auto ex = st2.execute();
            if (!ex) {
                err = true;
                break;
            }
        }
        auto er = db.execute(err ? "ROLLBACK" : "COMMIT");
        if (!er)
            std::cout << "  " << ui::status_error("Txn end failed: " + er.error().message) << "\n";
        if (err) {
            std::cout << "  " << ui::status_error("Aborted due to errors") << "\n";
            return;
        }
        std::cout << "  "
                  << ui::status_ok("Deleted " + std::to_string(toDelete.size()) + " duplicate rows")
                  << "\n";
    } catch (const std::exception& e) {
        std::cout << "  " << ui::status_error(std::string("Exception: ") + e.what()) << "\n";
    }
}

} // namespace yams::cli::doctor
