// Private, bounded reconciliation workspace and versioned committed view.
// Disposable across process restarts: recovery always revalidates the operation store.
#pragma once

#include <sqlite3.h>
#include <algorithm>
#include <atomic>
#include <filesystem>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <stdexcept>
#include "sweep_file.h"
#include <yams/memory_sync/memory_sync.h>

namespace yams::memory_sync {

class MemorySyncLoop::SweepStore : public MemorySyncLoop::AdapterIndex,
                                   public std::enable_shared_from_this<SweepStore> {
public:
    using Verifier = std::function<bool(const MemoryIndexRecord&)>;

private:
    struct SqlError : std::runtime_error {
        int code;
        SqlError(int rc, const char* message) : std::runtime_error(message), code(rc) {}
    };
    using ScratchFile = detail::SweepFile;
    class Statement {
        sqlite3_stmt* stmt_{nullptr};

    public:
        Statement(sqlite3* db, const char* sql) {
            const auto rc = sqlite3_prepare_v2(db, sql, -1, &stmt_, nullptr);
            if (rc != SQLITE_OK) {
                sqlite3_finalize(stmt_);
                throw SqlError(rc, sqlite3_errmsg(db));
            }
        }
        ~Statement() { sqlite3_finalize(stmt_); }
        void bind(int index, std::string_view value) {
            const auto rc = sqlite3_bind_text64(stmt_, index, value.data(), value.size(),
                                                SQLITE_TRANSIENT, SQLITE_UTF8);
            if (rc != SQLITE_OK)
                throw SqlError(rc, "cannot bind sweep value");
        }
        void integer(int index, std::int64_t value) {
            const auto rc = sqlite3_bind_int64(stmt_, index, value);
            if (rc != SQLITE_OK)
                throw SqlError(rc, "cannot bind sweep integer");
        }
        bool step() {
            const auto rc = sqlite3_step(stmt_);
            if (rc == SQLITE_ROW)
                return true;
            if (rc == SQLITE_DONE)
                return false;
            throw SqlError(rc, sqlite3_errmsg(sqlite3_db_handle(stmt_)));
        }
        std::int64_t number(int column) const { return sqlite3_column_int64(stmt_, column); }
        std::string text(int column) const {
            const auto* value = sqlite3_column_text(stmt_, column);
            const auto size = sqlite3_column_bytes(stmt_, column);
            return value ? std::string(reinterpret_cast<const char*>(value), size) : std::string{};
        }
    };
    ScratchFile file_;
    std::unique_ptr<sqlite3, decltype(&sqlite3_close_v2)> db_{nullptr, sqlite3_close_v2};
    mutable std::recursive_mutex mutex_;
    Verifier verifier_;
    std::function<Result<void>()> adapterWorkCheck_;
    std::size_t pageLimit_{512};
    std::size_t rowBytes_{0};
    std::size_t count_{0};
    std::int64_t generation_{0};
    // Unique per store for the process lifetime, so an identity never matches a later store
    // that reuses this one's address.
    const std::uint64_t instance_{nextInstance()};
    bool building_{true};

    static std::uint64_t nextInstance() noexcept {
        static std::atomic<std::uint64_t> next{1};
        return next.fetch_add(1, std::memory_order_relaxed);
    }
    mutable bool healthy_{true};

    void sql(const char* query, std::initializer_list<std::string_view> values = {}) {
        Statement statement{db_.get(), query};
        int index = 1;
        for (const auto value : values)
            statement.bind(index++, value);
        while (statement.step()) {
        }
    }
    static Error failure(const std::exception& error) {
        if (const auto* file = dynamic_cast<const detail::SweepFileError*>(&error)) {
            return Error{file->code, "memory sync sweep workspace: " + std::string(error.what())};
        }
        const auto* sql = dynamic_cast<const SqlError*>(&error);
        return Error{sql && (sql->code & 0xff) == SQLITE_FULL ? ErrorCode::ResourceExhausted
                                                              : ErrorCode::IOError,
                     "memory sync sweep workspace: " + std::string(error.what())};
    }
    SweepStore(const std::filesystem::path& directory, std::size_t maxBytes, Verifier verifier,
               std::size_t pageLimit, std::size_t maxRowBytes,
               const std::function<Result<void>()>& cooperate)
        : file_(directory, cooperate), verifier_(std::move(verifier)), adapterWorkCheck_(cooperate),
          pageLimit_(pageLimit) {
        sqlite3* raw = nullptr;
        const auto rc = sqlite3_open_v2(file_.path.string().c_str(), &raw,
                                        SQLITE_OPEN_READWRITE | SQLITE_OPEN_NOMUTEX, nullptr);
        db_.reset(raw);
        if (rc != SQLITE_OK)
            throw SqlError(rc, raw ? sqlite3_errmsg(raw) : "cannot open workspace");
        sqlite3_limit(
            db_.get(), SQLITE_LIMIT_LENGTH,
            static_cast<int>(std::min<std::size_t>(maxRowBytes, std::numeric_limits<int>::max())));
        rowBytes_ = static_cast<std::size_t>(sqlite3_limit(db_.get(), SQLITE_LIMIT_LENGTH, -1));
        sql("PRAGMA page_size=4096");
        sql("PRAGMA cache_size=-1024");
        sql("PRAGMA temp_store=FILE");
        sql("PRAGMA journal_mode=OFF");
        sql("PRAGMA synchronous=OFF");
        // Bound index/encoding amplification as well as resident cache. All ordering uses
        // an explicit index, so no unbounded ORDER BY spill lands in the OS tmpfs.
        const auto pages =
            std::max<std::size_t>(64, std::min<std::size_t>(maxBytes / 4096, 0x0fffffff) * 8);
        const auto pageCountPragma = "PRAGMA max_page_count=" + std::to_string(pages);
        sql(pageCountPragma.c_str());
        sql("CREATE TABLE operations(op TEXT PRIMARY KEY, hash TEXT, logical TEXT) WITHOUT ROWID");
        sql("CREATE TABLE vectors(vv TEXT PRIMARY KEY, hash TEXT, op TEXT) WITHOUT ROWID");
        sql("CREATE TABLE forks(op TEXT PRIMARY KEY) WITHOUT ROWID");
        sql("CREATE TABLE candidates(key TEXT, logical TEXT, hash TEXT, envelope TEXT, "
            "writer TEXT, counter TEXT, op TEXT)");
        sql("CREATE INDEX candidate_order ON candidates(writer,counter,hash)");
        sql("CREATE TABLE winners(logical TEXT, generation INTEGER, envelope TEXT, origin TEXT, "
            "PRIMARY KEY(logical,generation DESC)) WITHOUT ROWID");
        sql("CREATE TABLE banned(origin TEXT PRIMARY KEY,generation INTEGER) WITHOUT ROWID");
        sql("CREATE TABLE adapter_index(key TEXT PRIMARY KEY,value TEXT) WITHOUT ROWID");
        sql("BEGIN");
    }

public:
    static Result<std::shared_ptr<SweepStore>>
    create(const std::filesystem::path& directory, std::size_t maxBytes, Verifier verifier = {},
           std::size_t pageLimit = 512, std::size_t maxRowBytes = std::size_t{1024} * 1024,
           const std::function<Result<void>()>& cooperate = {}) {
        try {
            return std::shared_ptr<SweepStore>(new SweepStore(
                directory, maxBytes, std::move(verifier), pageLimit, maxRowBytes, cooperate));
        } catch (const std::exception& error) {
            return failure(error);
        }
    }
    Result<void> put(std::string_view key, std::string_view value) override {
        // Probes/waits never hold the workspace's SQLite mutex. Deferral preserves rows.
        if (adapterWorkCheck_) {
            if (auto ready = adapterWorkCheck_(); !ready) {
                return ready.error();
            }
        }
        std::lock_guard lock(mutex_);
        if (!healthy_) {
            return Error{ErrorCode::IOError, "adapter workspace is invalid"};
        }
        if (key.size() > rowBytes_ || value.size() > rowBytes_ - key.size()) {
            return Error{ErrorCode::ResourceExhausted, "adapter index row exceeds its byte budget"};
        }
        try {
            sql("INSERT INTO adapter_index VALUES(?1,?2) ON CONFLICT(key) DO UPDATE "
                "SET value=excluded.value",
                {key, value});
            return {};
        } catch (const std::exception& error) {
            const auto* sql = dynamic_cast<const SqlError*>(&error);
            if (sql && (sql->code & 0xff) == SQLITE_TOOBIG) {
                // SQLite also charges the record header. A rejected statement is not a
                // failed transaction; retain previously validated bookkeeping rows.
                return Error{ErrorCode::ResourceExhausted,
                             "adapter index encoded row exceeds its byte budget"};
            }
            healthy_ = false;
            return failure(error);
        }
    }
    Result<std::optional<std::string>> lookup(std::string_view key) const override {
        if (adapterWorkCheck_) {
            if (auto ready = adapterWorkCheck_(); !ready) {
                return ready.error();
            }
        }
        std::lock_guard lock(mutex_);
        if (!healthy_) {
            return Error{ErrorCode::IOError, "adapter workspace is invalid"};
        }
        if (key.size() > rowBytes_) {
            return Error{ErrorCode::ResourceExhausted, "adapter index key exceeds its byte budget"};
        }
        try {
            Statement statement{db_.get(), "SELECT value FROM adapter_index WHERE key=?1"};
            statement.bind(1, key);
            if (!statement.step()) {
                return std::optional<std::string>{};
            }
            return std::optional<std::string>{statement.text(0)};
        } catch (const std::exception& error) {
            healthy_ = false;
            return failure(error);
        }
    }
    Result<void> observe(const MemoryIndexRecord& record, std::string_view hash,
                         std::string_view logical, bool validIdentity) {
        std::lock_guard lock(mutex_);
        try {
            sql("INSERT OR IGNORE INTO forks SELECT ?1 WHERE EXISTS "
                "(SELECT 1 FROM operations WHERE op=?1 AND (hash<>?2 OR logical<>?3))",
                {record.operationId, hash, logical});
            sql("INSERT OR IGNORE INTO operations VALUES(?1,?2,?3)",
                {record.operationId, hash, logical});
            if (validIdentity) {
                const auto vector = nlohmann::json(record.vv).dump();
                sql("INSERT OR IGNORE INTO forks SELECT op FROM vectors WHERE vv=?1 AND hash<>?2",
                    {vector, hash});
                sql("INSERT OR IGNORE INTO forks SELECT ?3 WHERE EXISTS "
                    "(SELECT 1 FROM vectors WHERE vv=?1 AND hash<>?2)",
                    {vector, hash, record.operationId});
                sql("INSERT OR IGNORE INTO vectors VALUES(?1,?2,?3)",
                    {vector, hash, record.operationId});
            }
            return {};
        } catch (const std::exception& error) {
            return failure(error);
        }
    }
    Result<void> append(const ScanCandidate& candidate) {
        std::lock_guard lock(mutex_);
        try {
            const auto encoded = serialize(candidate.record);
            const std::string_view envelope{reinterpret_cast<const char*>(encoded.data()),
                                            encoded.size()};
            auto counter = std::to_string(candidate.record.vv.get(candidate.record.origin));
            counter.insert(0, 20 - counter.size(), '0'); // lexical order of the full uint64 domain
            sql("INSERT INTO candidates VALUES(?1,?2,?3,?4,?5,?6,?7)",
                {candidate.indexKey, candidate.logicalKey, candidate.recordHash, envelope,
                 candidate.record.origin, counter, candidate.record.operationId});
            return {};
        } catch (const std::exception& error) {
            return failure(error);
        }
    }
    Result<void>
    visit(const std::function<Result<void>(const ScanCandidate&, bool)>& visitor) const {
        std::lock_guard lock(mutex_);
        try {
            Statement rows{db_.get(),
                           "SELECT c.key,c.logical,c.hash,c.envelope,f.op FROM candidates AS c "
                           "INDEXED BY candidate_order LEFT JOIN forks AS f ON f.op=c.op "
                           "ORDER BY c.writer,c.counter,c.hash"};
            while (rows.step()) {
                const auto encoded = rows.text(3);
                auto record = deserialize(std::span<const std::byte>{
                    reinterpret_cast<const std::byte*>(encoded.data()), encoded.size()});
                if (!record)
                    return record.error();
                ScanCandidate candidate{rows.text(0), rows.text(1), rows.text(2),
                                        std::move(record.value())};
                if (auto result = visitor(candidate, !rows.text(4).empty()); !result)
                    return result.error();
            }
            return {};
        } catch (const std::exception& error) {
            return failure(error);
        }
    }

private:
    Result<MemoryIndexRecord> decode(std::string_view encoded, std::string_view key) const {
        auto record = deserialize(std::span<const std::byte>{
            reinterpret_cast<const std::byte*>(encoded.data()), encoded.size()});
        if (!record)
            return record.error();
        if (record.value().logicalKey != key || (verifier_ && !verifier_(record.value()))) {
            return Error{ErrorCode::Unauthorized,
                         "committed winner failed identity/authentication"};
        }
        return record;
    }
    Result<MemoryIndexRecord> findAt(std::string_view key, std::int64_t generation) const {
        std::lock_guard lock(mutex_);
        if (!healthy_)
            return Error{ErrorCode::IOError, "committed winner workspace is invalid"};
        try {
            Statement row{db_.get(), "SELECT envelope,origin FROM winners WHERE logical=?1 "
                                     "AND generation<=?2 ORDER BY generation DESC LIMIT 1"};
            row.bind(1, key);
            row.integer(2, generation);
            if (!row.step() || row.text(0).empty()) {
                return Error{ErrorCode::NotFound, "no committed memory winner"};
            }
            const auto origin = row.text(1);
            Statement banned{db_.get(), "SELECT 1 FROM banned WHERE origin=?1 AND generation<=?2"};
            banned.bind(1, origin);
            banned.integer(2, generation);
            if (banned.step())
                return Error{ErrorCode::NotFound, "writer is quarantined"};
            return decode(row.text(0), key);
        } catch (const std::exception& error) {
            healthy_ = false;
            return failure(error);
        }
    }
    Result<WinnerPage> pageAt(std::string_view prefix, std::string_view after, std::size_t limit,
                              std::int64_t generation) const {
        std::lock_guard lock(mutex_);
        if (!healthy_)
            return Error{ErrorCode::IOError, "committed winner workspace is invalid"};
        if (limit == 0)
            return WinnerPage{};
        try {
            Statement rows{db_.get(),
                           "SELECT w.logical,w.envelope FROM winners AS w WHERE w.logical>=?1 "
                           "AND w.logical>?2 AND w.generation<=?3 AND w.envelope IS NOT NULL "
                           "AND w.generation=(SELECT n.generation FROM winners AS n "
                           "WHERE n.logical=w.logical AND n.generation<=?3 "
                           "ORDER BY n.generation DESC LIMIT 1) "
                           "AND NOT EXISTS(SELECT 1 FROM banned AS b WHERE b.origin=w.origin "
                           "AND b.generation<=?3) ORDER BY w.logical LIMIT ?4"};
            rows.bind(1, prefix);
            rows.bind(2, after);
            rows.integer(3, generation);
            rows.integer(4, static_cast<std::int64_t>(std::min(limit, pageLimit_)));
            WinnerPage page;
            while (rows.step()) {
                const auto key = rows.text(0);
                if (!key.starts_with(prefix))
                    break;
                auto record = decode(rows.text(1), key);
                if (!record)
                    return record.error();
                page.emplace_back(key, std::move(record.value()));
            }
            return page;
        } catch (const std::exception& error) {
            healthy_ = false;
            return failure(error);
        }
    }
    class View final : public WinnerView {
        std::shared_ptr<const SweepStore> store_;
        std::int64_t generation_;
        std::size_t count_;
        bool named_;

    public:
        View(std::shared_ptr<const SweepStore> store, std::int64_t generation, std::size_t count,
             bool named)
            : store_(std::move(store)), generation_(generation), count_(count), named_(named) {}
        std::optional<Identity> identity() const noexcept override {
            if (!named_) {
                return std::nullopt;
            }
            return Identity{.instance = store_->instance_, .generation = generation_};
        }
        std::size_t size() const noexcept override { return count_; }
        Result<MemoryIndexRecord> find(std::string_view key) const override {
            return store_->findAt(key, generation_);
        }
        Result<WinnerPage> page(std::string_view prefix, std::string_view after,
                                std::size_t limit) const override {
            return store_->pageAt(prefix, after, limit, generation_);
        }
    };

public:
    [[nodiscard]] std::shared_ptr<const WinnerView> view() const {
        std::lock_guard lock(mutex_);
        // Rows added while building do not advance the generation, so a view taken then
        // cannot name its contents.
        return std::make_shared<View>(shared_from_this(), generation_, count_,
                                      !building_ && healthy_);
    }
    [[nodiscard]] std::size_t size() const noexcept { return count_; }
    [[nodiscard]] bool needsCompaction() const noexcept {
        return generation_ > 512 && static_cast<std::uint64_t>(generation_) / 2 > count_;
    }
    Result<MemoryIndexRecord> find(std::string_view key) const { return findAt(key, generation_); }
    Result<WinnerPage> page(std::string_view prefix, std::string_view after,
                            std::size_t limit) const {
        return pageAt(prefix, after, limit, generation_);
    }
    Result<void> select(const MemoryIndexRecord& record) {
        std::lock_guard lock(mutex_);
        if (!healthy_)
            return Error{ErrorCode::IOError, "committed winner workspace is invalid"};
        auto previous = findAt(record.logicalKey, generation_);
        if (!previous && previous.error().code != ErrorCode::NotFound)
            return previous.error();
        if (previous) {
            const auto decision = resolveLww(record, previous.value());
            if (decision == LwwDecision::Fork) {
                return Error{ErrorCode::HashMismatch, "committed winner operation fork"};
            }
            if (decision != LwwDecision::First)
                return {};
        }
        try {
            Statement banned{db_.get(), "SELECT 1 FROM banned WHERE origin=?1"};
            banned.bind(1, record.origin);
            if (banned.step()) {
                return Error{ErrorCode::Unauthorized, "cannot retain a quarantined writer"};
            }
            if (!building_) {
                if (generation_ == std::numeric_limits<std::int64_t>::max()) {
                    return Error{ErrorCode::ResourceExhausted, "winner generation exhausted"};
                }
                ++generation_;
            }
            const auto encoded = serialize(record);
            Statement row{db_.get(), "INSERT OR REPLACE INTO winners VALUES(?1,?2,?3,?4)"};
            row.bind(1, record.logicalKey);
            row.integer(2, generation_);
            row.bind(
                3, std::string_view{reinterpret_cast<const char*>(encoded.data()), encoded.size()});
            row.bind(4, record.origin);
            row.step();
            if (!previous)
                ++count_;
            return {};
        } catch (const std::exception& error) {
            healthy_ = false;
            return failure(error);
        }
    }
    Result<void> discard(std::string_view key) {
        std::lock_guard lock(mutex_);
        auto previous = findAt(key, generation_);
        if (!previous) {
            return previous.error().code == ErrorCode::NotFound ? Result<void>{}
                                                                : Result<void>{previous.error()};
        }
        try {
            if (building_) {
                sql("DELETE FROM winners WHERE logical=?1", {key});
            } else {
                if (generation_ == std::numeric_limits<std::int64_t>::max()) {
                    return Error{ErrorCode::ResourceExhausted, "winner generation exhausted"};
                }
                Statement row{db_.get(), "INSERT INTO winners VALUES(?1,?2,NULL,'')"};
                row.bind(1, key);
                row.integer(2, ++generation_);
                row.step();
            }
            --count_;
            return {};
        } catch (const std::exception& error) {
            healthy_ = false;
            return failure(error);
        }
    }
    Result<void> ban(std::string_view writer) {
        std::lock_guard lock(mutex_);
        try {
            Statement present{db_.get(), "SELECT 1 FROM banned WHERE origin=?1"};
            present.bind(1, writer);
            if (present.step())
                return {};
            if (generation_ == std::numeric_limits<std::int64_t>::max()) {
                return Error{ErrorCode::ResourceExhausted, "winner generation exhausted"};
            }
            Statement row{db_.get(), "INSERT INTO banned VALUES(?1,?2)"};
            row.bind(1, writer);
            row.integer(2, ++generation_);
            row.step();
            Statement count{db_.get(),
                            "SELECT COUNT(*) FROM winners AS w WHERE w.envelope IS NOT NULL "
                            "AND w.generation=(SELECT n.generation FROM winners AS n "
                            "WHERE n.logical=w.logical ORDER BY n.generation DESC LIMIT 1) "
                            "AND NOT EXISTS(SELECT 1 FROM banned AS b WHERE b.origin=w.origin)"};
            if (count.step())
                count_ = static_cast<std::size_t>(count.number(0));
            return {};
        } catch (const std::exception& error) {
            healthy_ = false;
            return failure(error);
        }
    }
    Result<void> seal() {
        std::lock_guard lock(mutex_);
        try {
            sql("DROP TABLE candidates");
            sql("DROP TABLE operations");
            sql("DROP TABLE vectors");
            sql("DROP TABLE forks");
            sql("COMMIT");
            building_ = false;
            return {};
        } catch (const std::exception& error) {
            healthy_ = false;
            return failure(error);
        }
    }
};
} // namespace yams::memory_sync
