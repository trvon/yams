#include <yams/cli/tui/browse_services.hpp>

#include <yams/api/content_store.h>
#include <yams/app/services/services.hpp>
#include <yams/cli/tui/tui_services.hpp>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/query_helpers.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#if defined(__unix__) || defined(__APPLE__)
#include <unistd.h>
#endif

namespace yams::cli::tui {

namespace {
inline char toPrintable(unsigned char c) {
    if (std::isprint(c))
        return static_cast<char>(c);
    return '.';
}
} // namespace

BrowseServices::BrowseServices(TUIServices* services) noexcept : backend_(services) {}

void BrowseServices::attach(TUIServices* services) noexcept {
    backend_ = services;
}

// ------------- Documents listing -------------

std::vector<DocEntry> BrowseServices::loadAllDocuments(BrowseState& state) {
    std::vector<DocEntry> all;
    std::string status;

    if (!backend_) {
        state.setStatus("Internal error: TUI services unavailable", BrowseState::Status::Error);
        return all;
    }

    auto ensure = backend_->ensureReady();
    if (!ensure) {
        state.setStatus(ensure.error().message, BrowseState::Status::Error);
        return all;
    }

    state.hasMoreDocuments = false;

    auto documentSvc = backend_->documentService();
    if (documentSvc) {
        constexpr int kInitialLimit = 1000;
        app::services::ListDocumentsRequest req;
        req.limit = kInitialLimit;
        req.sortBy = "indexed";
        req.sortOrder = "desc";

        auto listResult = documentSvc->list(req);
        if (listResult) {
            const auto& list = listResult.value();
            all.reserve(list.documents.size());

            auto toTimePoint = [](std::int64_t epoch) {
                using namespace std::chrono;
                if (epoch <= 0) {
                    return sys_seconds{floor<seconds>(system_clock::now())};
                }
                return sys_seconds{seconds{epoch}};
            };

            for (const auto& entry : list.documents) {
                DocEntry doc;
                doc.hash = entry.hash;
                doc.name = entry.fileName.empty() ? entry.name : entry.fileName;
                doc.size = static_cast<size_t>(entry.size);
                doc.type = entry.fileType.empty() ? entry.mimeType : entry.fileType;
                doc.createdAt = toTimePoint(entry.indexed != 0 ? entry.indexed : entry.created);
                doc.id = -1; // Document service does not currently expose id
                doc.path = entry.path;
                all.push_back(std::move(doc));
            }

            state.hasMoreDocuments = list.hasMore;
            if (list.count > 0) {
                status = std::to_string(list.count) + " documents loaded";
                if (list.hasMore) {
                    status += " (more available)";
                }
            }
        } else {
            status = "Document list failed: " + listResult.error().message;
            spdlog::warn("TUI Services: document service list failed: {}",
                         listResult.error().message);
        }
    }

    if (all.empty()) {
        auto metadataRepo = backend_->metadataRepo();
        auto store = backend_->contentStore();

        if (metadataRepo) {
            auto startTime = std::chrono::steady_clock::now();
            auto countResult = metadataRepo->getDocumentCount();
            const int64_t count = (countResult ? countResult.value() : 0);

            if (count > 0) {
                constexpr int INITIAL_LOAD_LIMIT = 1000;
                auto docsRes = metadata::queryDocumentsByPattern(*metadataRepo, "%");
                if (docsRes.has_value()) {
                    int loaded = 0;
                    for (const auto& di : docsRes.value()) {
                        if (loaded % 100 == 0) {
                            auto elapsed = std::chrono::steady_clock::now() - startTime;
                            if (elapsed > std::chrono::seconds(10)) {
                                status = std::to_string(loaded) +
                                         " documents loaded (timeout - more available)";
                                break;
                            }
                        }

                        DocEntry e;
                        e.hash = di.sha256Hash;
                        e.name = di.fileName;
                        e.size = static_cast<size_t>(di.fileSize);
                        e.type = di.mimeType;
                        e.createdAt = di.createdTime;
                        e.id = di.id;
                        e.path = di.filePath;
                        all.push_back(std::move(e));

                        ++loaded;
                        if (loaded >= INITIAL_LOAD_LIMIT) {
                            status = std::to_string(loaded) +
                                     " documents loaded (limited for performance)";
                            break;
                        }
                    }

                    if (status.empty()) {
                        status = std::to_string(all.size()) + " documents loaded";
                    }
                } else {
                    status = "Failed to query documents";
                    spdlog::warn("TUI Services: metadata query failed - {}",
                                 docsRes.error().message);
                }
            } else {
                status = "0 documents in metadata DB";
            }
        } else if (store) {
            auto stats = store->getStats();
            size_t limit = static_cast<size_t>(std::min<uint64_t>(stats.totalObjects, 50));
            for (size_t i = 0; i < limit; ++i) {
                DocEntry e;
                e.hash = std::string(64, static_cast<char>('a' + (i % 26)));
                e.name = "object_" + std::to_string(i + 1);
                e.size = 1024 * (i + 1);
                e.type = "unknown";
                e.createdAt = std::chrono::system_clock::now() - std::chrono::hours(i);
                e.id = static_cast<int64_t>(i);
                e.path = "";
                all.push_back(std::move(e));
            }
            status = std::to_string(all.size()) + " objects (no metadata DB)";
        } else {
            status = "Storage not initialized";
        }
    }

    constexpr size_t MAX_TUI_DOCS = 2000;
    if (all.size() > MAX_TUI_DOCS) {
        all.resize(MAX_TUI_DOCS);
        status += " (capped to " + std::to_string(MAX_TUI_DOCS) + " for TUI)";
    }

    if (!status.empty()) {
        state.setStatus(status);
    }

    return all;
}

std::vector<DocEntry> BrowseServices::fuzzySearch(std::string_view query, float min_similarity,
                                                  int limit) {
    std::vector<DocEntry> out;
    if (!backend_ || query.empty())
        return out;

    auto ensure = backend_->ensureReady();
    if (!ensure) {
        spdlog::warn("TUI Services: search unavailable: {}", ensure.error().message);
        return out;
    }

    app::services::SearchRequest request;
    request.query = std::string(query);
    request.limit = static_cast<std::size_t>(std::max(1, limit));
    request.fuzzy = true;
    request.similarity = min_similarity;
    request.type = "hybrid";

    auto response = backend_->search(request);
    if (!response) {
        spdlog::warn("TUI Services: search failed: {}", response.error().message);
        return out;
    }

    const auto& payload = response.value();
    out.reserve(payload.results.size());
    auto toTimePoint = [](std::int64_t epoch) {
        using namespace std::chrono;
        if (epoch <= 0) {
            return sys_seconds{floor<seconds>(system_clock::now())};
        }
        return sys_seconds{seconds{epoch}};
    };

    for (const auto& item : payload.results) {
        DocEntry doc;
        doc.hash = item.hash;
        doc.name = item.fileName.empty() ? item.title : item.fileName;
        doc.size = static_cast<size_t>(item.size);
        doc.type = item.fileType.empty() ? item.mimeType : item.fileType;
        doc.createdAt = toTimePoint(item.indexed != 0 ? item.indexed : item.created);
        doc.id = item.id;
        doc.path = item.path;
        out.push_back(std::move(doc));
    }

    return out;
}

std::vector<DocEntry> BrowseServices::filterBasic(const std::vector<DocEntry>& all,
                                                  std::string_view needle) {
    if (needle.empty())
        return all;

    std::vector<DocEntry> filtered;
    filtered.reserve(all.size());

    // Case-insensitive substring search without extra allocations for names
    auto ci_contains = [](std::string_view hay, std::string_view ndl) -> bool {
        if (ndl.empty())
            return true;
        auto it = std::search(hay.begin(), hay.end(), ndl.begin(), ndl.end(), [](char a, char b) {
            return std::tolower(static_cast<unsigned char>(a)) ==
                   std::tolower(static_cast<unsigned char>(b));
        });
        return it != hay.end();
    };

    for (const auto& d : all) {
        const bool name_match = ci_contains(std::string_view(d.name), needle);
        // Hash match is case-sensitive; avoid temporary string by using the sized find()
        const bool hash_match = d.hash.find(needle.data(), 0, needle.size()) != std::string::npos;

        if (name_match || hash_match) {
            filtered.push_back(d);
        }
    }
    return filtered;
}

// ------------- Content loading -------------

std::optional<std::string> BrowseServices::loadTextContent(const DocEntry& doc) {
    if (!backend_)
        return std::nullopt;

    auto ensure = backend_->ensureReady();
    if (!ensure)
        return std::nullopt;

    const auto documentSvc = backend_->documentService();
    const auto metadataRepo = backend_->metadataRepo();

    auto resolveHash = [&]() -> std::string {
        if (!doc.hash.empty()) {
            return doc.hash;
        }
        if (metadataRepo && doc.id >= 0) {
            auto info = metadataRepo->getDocument(doc.id);
            if (info && info.value().has_value()) {
                return info.value()->sha256Hash;
            }
        }
        return {};
    };

    if (documentSvc) {
        app::services::CatDocumentRequest req;
        req.hash = resolveHash();
        if (req.hash.empty() && doc.id >= 0) {
            req.name = std::to_string(doc.id);
        }
        if (!req.hash.empty() || !req.name.empty()) {
            auto cat = documentSvc->cat(req);
            if (cat && !cat.value().content.empty()) {
                return cat.value().content;
            }
        }
    }

    if (metadataRepo) {
        auto tryLoad = [&](int64_t id) -> std::optional<std::string> {
            auto contentRes = metadataRepo->getContent(id);
            if (!contentRes || !contentRes.value().has_value()) {
                return std::nullopt;
            }
            return contentRes.value()->contentText;
        };

        if (doc.id >= 0) {
            if (auto content = tryLoad(doc.id)) {
                return content;
            }
        }

        auto hash = resolveHash();
        if (!hash.empty()) {
            auto info = metadataRepo->getDocumentByHash(hash);
            if (info && info.value().has_value()) {
                if (auto content = tryLoad(info.value()->id)) {
                    return content;
                }
            }
        }
    }

    return std::nullopt;
}

std::vector<std::byte> BrowseServices::loadRawBytes(std::string_view hash, size_t cap_bytes) {
    std::vector<std::byte> out;
    if (!backend_ || hash.empty())
        return out;

    auto ensure = backend_->ensureReady();
    if (!ensure)
        return out;

    auto store = backend_->contentStore();
    if (!store)
        return out;

    auto res = store->retrieveBytes(std::string(hash));
    if (!res.has_value())
        return out;

    auto& bytes = res.value();
    if (bytes.empty())
        return out;

    if (cap_bytes > 0 && bytes.size() > cap_bytes) {
        out.assign(bytes.begin(), bytes.begin() + static_cast<std::ptrdiff_t>(cap_bytes));
    } else {
        out = std::move(bytes);
    }
    return out;
}

// ------------- Content utilities -------------

bool BrowseServices::looksBinary(std::string_view sample) const {
    if (sample.empty())
        return false;
    size_t check = std::min<size_t>(sample.size(), 1024);
    size_t nonprint = 0;
    for (size_t i = 0; i < check; ++i) {
        unsigned char c = static_cast<unsigned char>(sample[i]);
        if (c == '\n' || c == '\r' || c == '\t')
            continue;
        if (!std::isprint(c))
            nonprint++;
    }
    return nonprint > check / 10;
}

std::vector<std::string> BrowseServices::splitLines(const std::string& content,
                                                    size_t max_lines) const {
    std::vector<std::string> lines;
    lines.reserve(std::min<size_t>(max_lines, 1024));

    std::istringstream iss(content);
    std::string line;
    while (std::getline(iss, line)) {
        if (!line.empty() && line.back() == '\r')
            line.pop_back();
        lines.push_back(std::move(line));
        if (lines.size() >= max_lines)
            break;
    }
    return lines;
}

std::vector<std::string> BrowseServices::toHexDump(const std::vector<std::byte>& bytes,
                                                   size_t bytes_per_line, size_t max_lines) const {
    std::vector<std::string> lines;
    if (bytes_per_line == 0)
        return lines;

    const size_t total = bytes.size();
    const size_t line_count = std::min(max_lines, (total + bytes_per_line - 1) / bytes_per_line);
    lines.reserve(line_count);

    for (size_t line_idx = 0; line_idx < line_count; ++line_idx) {
        size_t offset = line_idx * bytes_per_line;
        if (offset >= total)
            break;

        size_t n = std::min(bytes_per_line, total - offset);
        std::ostringstream oss;

        // Offset
        oss << std::hex;
        oss.width(8);
        oss.fill('0');
        oss << offset;
        oss << "  ";

        // Hex bytes
        for (size_t i = 0; i < bytes_per_line; ++i) {
            if (i < n) {
                unsigned int v =
                    static_cast<unsigned int>(std::to_integer<unsigned char>(bytes[offset + i]));
                if (v < 0x10)
                    oss << '0';
                oss << std::uppercase << v << std::nouppercase;
            } else {
                oss << "  ";
            }
            oss << ' ';
        }

        // ASCII gutter
        oss << " |";
        for (size_t i = 0; i < n; ++i) {
            auto c = static_cast<unsigned char>(std::to_integer<unsigned char>(bytes[offset + i]));
            oss << toPrintable(c);
        }
        oss << '|';

        lines.push_back(oss.str());
    }

    return lines;
}

std::vector<std::string> BrowseServices::makePreviewLines(const DocEntry& doc, PreviewMode mode,
                                                          size_t max_bytes, size_t max_lines) {
    switch (mode) {
        case PreviewMode::Text:
        case PreviewMode::Auto: {
            // Prefer metadata text
            if (auto text = loadTextContent(doc)) {
                auto lines = splitLines(*text, max_lines);
                if (!lines.empty())
                    return lines;
            }
            // Fallback to raw bytes
            auto bytes = loadRawBytes(doc.hash, max_bytes);
            if (bytes.empty()) {
                return {"No preview available."};
            }
            // Convert to string to run binary heuristic and split
            std::string sample;
            sample.resize(bytes.size());
            for (size_t i = 0; i < bytes.size(); ++i) {
                sample[i] = static_cast<char>(std::to_integer<unsigned char>(bytes[i]));
            }
            if (mode == PreviewMode::Text) {
                if (looksBinary(sample)) {
                    return {"Binary content. Preview unavailable."};
                }
                return splitLines(sample, max_lines);
            }
            // Auto mode: try to display if it doesn't look binary; otherwise message
            if (!looksBinary(sample)) {
                return splitLines(sample, max_lines);
            }
            return {"Binary content. Preview unavailable."};
        }
        case PreviewMode::Hex: {
            auto bytes = loadRawBytes(doc.hash, max_bytes);
            if (bytes.empty()) {
                return {"No preview available."};
            }
            return toHexDump(bytes, 16, max_lines);
        }
    }
    return {"No preview available."};
}

// ------------- External pager -------------

bool BrowseServices::openInPager(BrowseState& state, const std::string& name,
                                 const std::optional<std::string>& text,
                                 const std::vector<std::byte>& raw_bytes) {
#if defined(__unix__) || defined(__APPLE__)
    // Determine pager
    const char* env_pager = std::getenv("PAGER");
    std::string pager = (env_pager && *env_pager) ? std::string(env_pager) : std::string("less -R");

    // Create temp file
    char tmpl[] = "/tmp/yams-pager-XXXXXX";
    int fd = mkstemp(tmpl);
    if (fd == -1) {
        state.setStatus("Failed to create temporary file", BrowseState::Status::Error);
        return false;
    }
    std::string tmp_path = tmpl;

    // Write content
    {
        std::ofstream ofs(tmp_path, std::ios::binary);
        if (!ofs) {
            state.setStatus("Failed to open temporary file for writing",
                            BrowseState::Status::Error);
            ::close(fd);
            std::remove(tmp_path.c_str());
            return false;
        }
        if (text.has_value()) {
            ofs.write(text->data(), static_cast<std::streamsize>(text->size()));
        } else if (!raw_bytes.empty()) {
            for (const auto& b : raw_bytes) {
                char c = static_cast<char>(std::to_integer<unsigned char>(b));
                ofs.write(&c, 1);
            }
        } else {
            ofs << name << "\n";
            ofs << "(no content available)\n";
        }
        ofs.flush();
        ::close(fd);
    }

    // Execute pager
    std::string cmd = pager + " " + tmp_path;
    int rc = std::system(cmd.c_str());

    // Cleanup
    std::remove(tmp_path.c_str());

    if (rc == -1) {
        state.setStatus("Failed to execute pager", BrowseState::Status::Error);
        return false;
    }
    return true;
#else
    (void)name;
    (void)text;
    (void)raw_bytes;
    state.setStatus("External pager not supported on this platform", BrowseState::Status::Error);
    return false;
#endif
}

bool BrowseServices::openInPagerWithSuspend(BrowseState& state, const std::string& name,
                                            const std::optional<std::string>& text,
                                            const std::vector<std::byte>& raw_bytes,
                                            const SuspendRunner& suspend) {
    if (suspend) {
        bool ok = true;
        suspend([&]() {
            if (!openInPager(state, name, text, raw_bytes)) {
                ok = false;
            }
        });
        return ok;
    }
    return openInPager(state, name, text, raw_bytes);
}

} // namespace yams::cli::tui
