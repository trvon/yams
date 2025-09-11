// Integration: ensure FTS5 indexing and querying work end-to-end against daemon DB
#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>

#include "test_async_helpers.h"
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/daemon.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/metadata/database.h>

using namespace std::chrono_literals;
namespace fs = std::filesystem;

TEST(Fts5IT, IngestAndSearch) {
    if (std::getenv("GTEST_DISCOVERY_MODE")) {
        GTEST_SKIP() << "Skipping during test discovery";
    }

    // Temp daemon layout
    fs::path tmp = fs::temp_directory_path() / ("yams_fts5_it_" + std::to_string(::getpid()));
    fs::create_directories(tmp);
    auto cleanup = [&]() {
        std::error_code ec;
        fs::remove_all(tmp, ec);
    };

    fs::path sock = tmp / "d.sock";
    fs::path pid = tmp / "d.pid";
    fs::path log = tmp / "d.log";
    fs::path data = tmp / "data";
    fs::create_directories(data);

    yams::daemon::DaemonConfig cfg;
    cfg.dataDir = data;
    cfg.socketPath = sock;
    cfg.pidFile = pid;
    cfg.logFile = log;
    yams::daemon::YamsDaemon daemon(cfg);
    auto started = daemon.start();
    ASSERT_TRUE(started) << "Failed to start daemon: " << started.error().message;
    auto guard = std::unique_ptr<void, void (*)(void*)>{
        &daemon, +[](void* d) { (void)static_cast<yams::daemon::YamsDaemon*>(d)->stop(); }};

    yams::daemon::ClientConfig cc;
    cc.socketPath = sock;
    cc.requestTimeout = 10s;
    cc.autoStart = false;
    yams::daemon::DaemonClient client(cc);

    // Wait for database file to appear instead of overall readiness
    bool db_ready = false;
    for (int i = 0; i < 100; ++i) {
        if (fs::exists(data / "yams.db")) {
            db_ready = true;
            break;
        }
        std::this_thread::sleep_for(100ms);
    }
    ASSERT_TRUE(db_ready) << "Database was not created in time";

    // Check FTS5 availability on daemon DB
    yams::metadata::Database db;
    ASSERT_TRUE(db.open((data / "yams.db").string(), yams::metadata::ConnectionMode::ReadWrite));
    auto fts = db.hasFTS5();
    ASSERT_TRUE(fts.has_value()) << "Failed to query FTS5: " << fts.error().message;
    if (!fts.value()) {
        GTEST_SKIP() << "FTS5 not available in this SQLite build";
    }

    // Create and ingest a plain text document via daemon
    std::string content = "alpha bravo charlie delta echo";
    yams::daemon::AddDocumentRequest add;
    add.path = "";
    add.content = content;
    add.name = "fts5_doc.txt";
    add.mimeType = "text/plain";
    add.noEmbeddings = true; // focus on FTS5 path
    auto addRes = yams::cli::run_sync(client.streamingAddDocument(add), 10s);
    ASSERT_TRUE(addRes) << "AddDocument failed: " << addRes.error().message;
    ASSERT_FALSE(addRes.value().hash.empty());

    // Wait for migrations and the inserted document to be visible
    int64_t rowid = -1;
    for (int i = 0; i < 50; ++i) {
        auto tExists = db.tableExists("documents");
        if (!tExists || !tExists.value()) {
            std::this_thread::sleep_for(100ms);
            continue;
        }
        auto stmt =
            db.prepare("SELECT id FROM documents WHERE file_name = ? ORDER BY id DESC LIMIT 1");
        if (!stmt) {
            std::this_thread::sleep_for(100ms);
            continue;
        }
        if (!stmt.value().bind(1, std::string("fts5_doc.txt"))) {
            std::this_thread::sleep_for(100ms);
            continue;
        }
        auto stepped = stmt.value().step();
        if (stepped && stepped.value()) {
            rowid = stmt.value().getInt64(0);
            break;
        }
        std::this_thread::sleep_for(100ms);
    }
    ASSERT_GT(rowid, 0) << "document row not visible after add";

    // Upsert into FTS5 (delete then insert) using same content
    ASSERT_TRUE(db.execute("DELETE FROM documents_fts WHERE rowid = " + std::to_string(rowid)));
    auto ins = db.prepare(
        "INSERT INTO documents_fts (rowid, content, title, content_type) VALUES (?, ?, ?, ?)");
    ASSERT_TRUE(ins);
    ASSERT_TRUE(ins.value().bindAll(rowid, content, std::string("fts5_doc.txt"),
                                    std::string("text/plain")));
    ASSERT_TRUE(ins.value().execute());

    // Wait for content_store and metadata_repo readiness (best-effort)
    for (int i = 0; i < 20; ++i) {
        auto st = yams::cli::run_sync(client.status(), 500ms);
        if (st && st.value().readinessStates.count("content_store") &&
            st.value().readinessStates.at("content_store") &&
            st.value().readinessStates.count("metadata_repo") &&
            st.value().readinessStates.at("metadata_repo"))
            break;
        std::this_thread::sleep_for(200ms);
    }

    // Query via daemon using search; expect hit on "bravo"
    yams::daemon::SearchRequest sreq;
    sreq.query = "bravo";
    sreq.limit = 5;
    sreq.fuzzy = false;
    auto sres = yams::cli::run_sync(client.search(sreq), 10s);
    ASSERT_TRUE(sres) << "Search failed: " << sres.error().message;
    EXPECT_GE(sres.value().totalCount, 1);

    cleanup();
}
