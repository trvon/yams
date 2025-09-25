#include <yams/api/mobile_bindings.h>

#include <yams/api/content_store_builder.h>
#include <yams/app/services/factory.hpp>
#include <yams/app/services/services.hpp>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/migration.h>

#include <nlohmann/json.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/asio/use_future.hpp>

#include <algorithm>
#include <cstddef>
#include <filesystem>
#include <future>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

#ifndef _WIN32
#include <unistd.h>
#endif

namespace {

using yams::ErrorCode;
using yams::Result;
using yams::app::services::AppContext;
using yams::app::services::GrepRequest;
using yams::app::services::GrepResponse;
using yams::app::services::StoreDocumentRequest;
using yams::app::services::StoreDocumentResponse;
using yams::metadata::ConnectionMode;
using yams::metadata::ConnectionPool;
using yams::metadata::ConnectionPoolConfig;
using yams::metadata::Database;
using yams::metadata::MetadataRepository;
using yams::metadata::MigrationManager;
using yams::metadata::YamsMetadataMigrations;
using SearchResponseResult = Result<yams::app::services::SearchResponse>;

constexpr yams_mobile_version_info kVersion{0u, 1u, 0u};

template <typename T> std::shared_ptr<T> to_shared(std::unique_ptr<T>&& ptr) {
    return std::shared_ptr<T>(ptr.release());
}

thread_local std::string g_last_error;
thread_local std::string g_temp_string;

void set_last_error(const std::string& message) {
    g_last_error = message;
}

std::filesystem::path make_temp_directory() {
    auto base = std::filesystem::temp_directory_path();
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<std::uint64_t> dist;
    auto pid = static_cast<std::uint64_t>(::getpid());
    std::filesystem::path candidate;
    for (int i = 0; i < 8; ++i) {
        candidate = base / ("yams_mobile_" + std::to_string(pid) + "_" + std::to_string(dist(gen)));
        std::error_code ec;
        if (std::filesystem::create_directories(candidate, ec))
            return candidate;
    }
    std::filesystem::create_directories(base / "yams_mobile_fallback");
    return base / "yams_mobile_fallback";
}

yams_mobile_status map_error_code(ErrorCode code) {
    switch (code) {
        case ErrorCode::Success:
            return YAMS_MOBILE_STATUS_OK;
        case ErrorCode::InvalidArgument:
            return YAMS_MOBILE_STATUS_INVALID_ARGUMENT;
        case ErrorCode::NotInitialized:
            return YAMS_MOBILE_STATUS_NOT_INITIALIZED;
        case ErrorCode::Timeout:
            return YAMS_MOBILE_STATUS_TIMEOUT;
        case ErrorCode::NotFound:
            return YAMS_MOBILE_STATUS_NOT_FOUND;
        case ErrorCode::ResourceBusy:
        case ErrorCode::OperationInProgress:
        case ErrorCode::OperationCancelled:
            return YAMS_MOBILE_STATUS_UNAVAILABLE;
        default:
            return YAMS_MOBILE_STATUS_INTERNAL_ERROR;
    }
}

struct MobileContextConfig {
    std::filesystem::path working_directory;
    std::filesystem::path cache_directory;
    std::filesystem::path storage_directory;
    uint32_t max_worker_threads = 0;
    uint32_t flags = 0;
};

struct MobileState {
    MobileContextConfig config;
    std::unique_ptr<boost::asio::thread_pool> worker_pool;
    std::unique_ptr<Database> database;
    std::unique_ptr<ConnectionPool> connection_pool;
    std::shared_ptr<MetadataRepository> metadata_repo;
    std::shared_ptr<yams::api::IContentStore> content_store;
    AppContext app_context;
    std::shared_ptr<yams::app::services::IDocumentService> document_service;
    std::shared_ptr<yams::app::services::ISearchService> search_service;
    std::shared_ptr<yams::app::services::IGrepService> grep_service;
};

} // namespace

struct yams_mobile_context_t {
    MobileState state;
};

struct yams_mobile_grep_result_t {
    yams::app::services::GrepResponse response;
};

struct yams_mobile_search_result_t {
    yams::app::services::SearchResponse response;
};

struct yams_mobile_metadata_result_t {
    std::string json;
};

struct yams_mobile_vector_status_result_t {
    std::string json;
};

yams_mobile_version_info yams_mobile_get_version(void) {
    return kVersion;
}

yams_mobile_status yams_mobile_context_create(const yams_mobile_context_config* config,
                                              yams_mobile_context_t** out_context) {
    if (out_context == nullptr) {
        set_last_error("out_context pointer is null");
        return YAMS_MOBILE_STATUS_INVALID_ARGUMENT;
    }
    *out_context = nullptr;
    if (config == nullptr) {
        set_last_error("config pointer is null");
        return YAMS_MOBILE_STATUS_INVALID_ARGUMENT;
    }

    try {
        auto ctx = std::make_unique<yams_mobile_context_t>();

        // Paths
        ctx->state.config.working_directory =
            config->working_directory && *config->working_directory
                ? std::filesystem::path(config->working_directory)
                : make_temp_directory();
        ctx->state.config.cache_directory = config->cache_directory && *config->cache_directory
                                                ? std::filesystem::path(config->cache_directory)
                                                : (ctx->state.config.working_directory / "cache");
        ctx->state.config.storage_directory = ctx->state.config.working_directory / "storage";
        ctx->state.config.max_worker_threads = config->max_worker_threads;
        ctx->state.config.flags = config->flags;

        std::error_code ec;
        std::filesystem::create_directories(ctx->state.config.working_directory, ec);
        std::filesystem::create_directories(ctx->state.config.cache_directory, ec);
        std::filesystem::create_directories(ctx->state.config.storage_directory, ec);

        // Thread pool / executor
        auto threads = ctx->state.config.max_worker_threads;
        if (threads == 0)
            threads = std::max<uint32_t>(1, std::thread::hardware_concurrency());
        ctx->state.worker_pool = std::make_unique<boost::asio::thread_pool>(threads);

        // Metadata database
        auto db = std::make_unique<Database>();
        auto dbPath = ctx->state.config.working_directory / "yams_mobile.db";
        auto open = db->open(dbPath.string(), ConnectionMode::Create);
        if (!open) {
            set_last_error(open.error().message);
            return map_error_code(open.error().code);
        }

        ctx->state.database = std::move(db);
        ctx->state.connection_pool =
            std::make_unique<ConnectionPool>(dbPath.string(), ConnectionPoolConfig{});
        ctx->state.metadata_repo =
            std::make_shared<MetadataRepository>(*ctx->state.connection_pool);

        MigrationManager migrator(*ctx->state.database);
        auto initRes = migrator.initialize();
        if (!initRes) {
            set_last_error(initRes.error().message);
            return map_error_code(initRes.error().code);
        }
        migrator.registerMigrations(YamsMetadataMigrations::getAllMigrations());
        auto migrateRes = migrator.migrate();
        if (!migrateRes) {
            set_last_error(migrateRes.error().message);
            return map_error_code(migrateRes.error().code);
        }

        // Content store
        yams::api::ContentStoreBuilder builder;
        auto storeRes = builder.withStoragePath(ctx->state.config.storage_directory).build();
        if (!storeRes) {
            set_last_error(storeRes.error().message);
            return map_error_code(storeRes.error().code);
        }
        ctx->state.content_store = to_shared(std::move(storeRes.value()));

        // App context wiring
        ctx->state.app_context.store = ctx->state.content_store;
        ctx->state.app_context.metadataRepo = ctx->state.metadata_repo;
        ctx->state.app_context.workerExecutor = ctx->state.worker_pool->get_executor();

        // Services
        ctx->state.document_service =
            yams::app::services::makeDocumentService(ctx->state.app_context);
        ctx->state.search_service = yams::app::services::makeSearchService(ctx->state.app_context);
        ctx->state.grep_service = yams::app::services::makeGrepService(ctx->state.app_context);

        *out_context = ctx.release();
        set_last_error("");
        return YAMS_MOBILE_STATUS_OK;
    } catch (const std::bad_alloc&) {
        set_last_error("failed to allocate mobile context");
        return YAMS_MOBILE_STATUS_INTERNAL_ERROR;
    } catch (const std::exception& ex) {
        set_last_error(ex.what());
        return YAMS_MOBILE_STATUS_UNKNOWN;
    } catch (...) {
        set_last_error("unexpected error creating mobile context");
        return YAMS_MOBILE_STATUS_UNKNOWN;
    }
}

void yams_mobile_context_destroy(yams_mobile_context_t* ctx) {
    if (!ctx)
        return;
    if (ctx->state.worker_pool) {
        ctx->state.worker_pool->join();
    }
    delete ctx;
}

yams_mobile_status yams_mobile_grep_execute(yams_mobile_context_t* ctx,
                                            const yams_mobile_grep_request* request,
                                            yams_mobile_grep_result_t** out_result) {
    if (out_result)
        *out_result = nullptr;
    if (!ctx || !request) {
        set_last_error("invalid arguments");
        return YAMS_MOBILE_STATUS_INVALID_ARGUMENT;
    }
    auto service = ctx->state.grep_service;
    if (!service) {
        set_last_error("grep service not initialized");
        return YAMS_MOBILE_STATUS_NOT_INITIALIZED;
    }

    GrepRequest req;
    req.pattern = request->pattern ? request->pattern : "";
    req.literalText = request->literal != 0;
    req.ignoreCase = request->ignore_case != 0;
    req.word = request->word_boundary != 0;
    req.maxCount = request->max_matches;

    auto result = service->grep(req);
    if (!result) {
        set_last_error(result.error().message);
        return map_error_code(result.error().code);
    }

    if (out_result) {
        auto wrapper = std::make_unique<yams_mobile_grep_result_t>();
        wrapper->response = std::move(result.value());
        *out_result = wrapper.release();
    }
    set_last_error("");
    return YAMS_MOBILE_STATUS_OK;
}

void yams_mobile_grep_result_destroy(yams_mobile_grep_result_t* result) {
    delete result;
}

yams_mobile_status yams_mobile_search_execute(yams_mobile_context_t* ctx,
                                              const yams_mobile_search_request* request,
                                              yams_mobile_search_result_t** out_result) {
    if (out_result)
        *out_result = nullptr;
    if (!ctx || !request) {
        set_last_error("invalid arguments");
        return YAMS_MOBILE_STATUS_INVALID_ARGUMENT;
    }
    auto service = ctx->state.search_service;
    if (!service) {
        set_last_error("search service not initialized");
        return YAMS_MOBILE_STATUS_NOT_INITIALIZED;
    }

    yams::app::services::SearchRequest req;
    req.query = request->query ? request->query : "";
    req.limit = request->limit;
    req.pathsOnly = request->paths_only != 0;
    req.type = request->semantic ? "semantic" : "hybrid";
    if (request->tags && request->tag_count > 0) {
        for (size_t i = 0; i < request->tag_count; ++i) {
            if (request->tags[i])
                req.tags.emplace_back(request->tags[i]);
        }
    }

    try {
        auto promise = std::make_shared<std::promise<SearchResponseResult>>();
        auto future = promise->get_future();
        boost::asio::co_spawn(
            ctx->state.worker_pool->get_executor(),
            [svc = service, req, promise]() mutable -> boost::asio::awaitable<void> {
                try {
                    auto result = co_await svc->search(req);
                    promise->set_value(std::move(result));
                } catch (...) {
                    try {
                        promise->set_exception(std::current_exception());
                    } catch (...) {
                    }
                }
                co_return;
            },
            boost::asio::detached);

        auto result = future.get();
        if (!result) {
            set_last_error(result.error().message);
            return map_error_code(result.error().code);
        }

        if (out_result) {
            auto wrapper = std::make_unique<yams_mobile_search_result_t>();
            wrapper->response = std::move(result.value());
            *out_result = wrapper.release();
        }
        set_last_error("");
        return YAMS_MOBILE_STATUS_OK;
    } catch (const std::exception& ex) {
        set_last_error(ex.what());
        return YAMS_MOBILE_STATUS_INTERNAL_ERROR;
    }
}

void yams_mobile_search_result_destroy(yams_mobile_search_result_t* result) {
    delete result;
}

yams_mobile_status yams_mobile_store_document(yams_mobile_context_t* ctx,
                                              const yams_mobile_document_store_request* request,
                                              yams_mobile_string_view* out_hash) {
    if (out_hash) {
        out_hash->data = nullptr;
        out_hash->length = 0;
    }
    if (!ctx || !request) {
        set_last_error("invalid arguments");
        return YAMS_MOBILE_STATUS_INVALID_ARGUMENT;
    }
    auto service = ctx->state.document_service;
    if (!service) {
        set_last_error("document service not initialized");
        return YAMS_MOBILE_STATUS_NOT_INITIALIZED;
    }

    StoreDocumentRequest req;
    if (request->path)
        req.path = request->path;
    req.deferExtraction = request->sync_now == 0;
    if (request->tags && request->tag_count > 0) {
        for (size_t i = 0; i < request->tag_count; ++i) {
            if (request->tags[i])
                req.tags.emplace_back(request->tags[i]);
        }
    }

    auto result = service->store(req);
    if (!result) {
        set_last_error(result.error().message);
        return map_error_code(result.error().code);
    }

    if (out_hash) {
        g_temp_string = result.value().hash;
        out_hash->data = g_temp_string.c_str();
        out_hash->length = g_temp_string.size();
    }
    set_last_error("");
    return YAMS_MOBILE_STATUS_OK;
}

yams_mobile_status yams_mobile_remove_document(yams_mobile_context_t* ctx,
                                               const char* document_hash) {
    if (!ctx || !document_hash || *document_hash == '\0') {
        set_last_error("invalid arguments");
        return YAMS_MOBILE_STATUS_INVALID_ARGUMENT;
    }
    auto repo = ctx->state.metadata_repo;
    auto store = ctx->state.content_store;
    if (!repo || !store) {
        set_last_error("context not initialized");
        return YAMS_MOBILE_STATUS_NOT_INITIALIZED;
    }

    auto doc = repo->getDocumentByHash(document_hash);
    if (!doc) {
        set_last_error(doc.error().message);
        return map_error_code(doc.error().code);
    }
    if (!doc.value().has_value()) {
        set_last_error("document not found");
        return YAMS_MOBILE_STATUS_NOT_FOUND;
    }

    auto info = *doc.value();
    auto removeContent = store->remove(info.sha256Hash);
    if (!removeContent) {
        set_last_error(removeContent.error().message);
        return map_error_code(removeContent.error().code);
    }

    auto deleteMeta = repo->deleteDocument(info.id);
    if (!deleteMeta) {
        set_last_error(deleteMeta.error().message);
        return map_error_code(deleteMeta.error().code);
    }

    set_last_error("");
    return YAMS_MOBILE_STATUS_OK;
}

yams_mobile_status yams_mobile_get_metadata(yams_mobile_context_t* ctx,
                                            const yams_mobile_metadata_request* request,
                                            yams_mobile_metadata_result_t** out_result) {
    if (out_result)
        *out_result = nullptr;
    if (!ctx) {
        set_last_error("context is null");
        return YAMS_MOBILE_STATUS_INVALID_ARGUMENT;
    }
    auto repo = ctx->state.metadata_repo;
    if (!repo) {
        set_last_error("metadata repository not initialized");
        return YAMS_MOBILE_STATUS_NOT_INITIALIZED;
    }

    std::vector<yams::metadata::DocumentInfo> docs;
    if (request && request->document_hash && *request->document_hash) {
        auto doc = repo->getDocumentByHash(request->document_hash);
        if (!doc) {
            set_last_error(doc.error().message);
            return map_error_code(doc.error().code);
        }
        if (doc.value().has_value())
            docs.push_back(*doc.value());
    } else {
        std::string pattern = "%";
        if (request && request->path && *request->path)
            pattern = request->path;
        auto list = repo->findDocumentsByPath(pattern);
        if (!list) {
            set_last_error(list.error().message);
            return map_error_code(list.error().code);
        }
        docs = std::move(list.value());
    }

    nlohmann::json root = nlohmann::json::array();
    for (const auto& doc : docs) {
        nlohmann::json entry;
        entry["id"] = doc.id;
        entry["path"] = doc.filePath;
        entry["name"] = doc.fileName;
        entry["hash"] = doc.sha256Hash;
        entry["size"] = doc.fileSize;
        entry["mime"] = doc.mimeType;
        entry["contentExtracted"] = doc.contentExtracted;
        entry["extractionStatus"] = static_cast<int>(doc.extractionStatus);

        auto tags = repo->getDocumentTags(doc.id);
        if (tags)
            entry["tags"] = tags.value();

        auto meta = repo->getAllMetadata(doc.id);
        if (meta) {
            nlohmann::json metaJson = nlohmann::json::object();
            for (const auto& [k, v] : meta.value()) {
                metaJson[k] = v.asString();
            }
            entry["metadata"] = std::move(metaJson);
        }

        root.push_back(std::move(entry));
    }

    if (out_result) {
        auto res = std::make_unique<yams_mobile_metadata_result_t>();
        res->json = root.dump();
        *out_result = res.release();
    }
    set_last_error("");
    return YAMS_MOBILE_STATUS_OK;
}

void yams_mobile_metadata_result_destroy(yams_mobile_metadata_result_t* result) {
    delete result;
}

yams_mobile_status yams_mobile_get_vector_status(yams_mobile_context_t* ctx,
                                                 const yams_mobile_vector_status_request* request,
                                                 yams_mobile_vector_status_result_t** out_result) {
    if (out_result)
        *out_result = nullptr;
    if (!ctx) {
        set_last_error("context is null");
        return YAMS_MOBILE_STATUS_INVALID_ARGUMENT;
    }
    auto repo = ctx->state.metadata_repo;
    auto store = ctx->state.content_store;
    if (!repo || !store) {
        set_last_error("context not initialized");
        return YAMS_MOBILE_STATUS_NOT_INITIALIZED;
    }

    std::uint64_t docCount = 0;
    auto countRes = repo->getDocumentCount();
    if (countRes)
        docCount = static_cast<std::uint64_t>(countRes.value());

    auto stats = store->getStats();
    nlohmann::json j;
    j["documents"] = docCount;
    j["storageBytes"] = stats.totalBytes;
    j["dedupBytes"] = stats.deduplicatedBytes;
    j["warmupRequested"] = (request && request->warmup != 0);

    if (out_result) {
        auto res = std::make_unique<yams_mobile_vector_status_result_t>();
        res->json = j.dump();
        *out_result = res.release();
    }

    set_last_error("");
    return YAMS_MOBILE_STATUS_OK;
}

void yams_mobile_vector_status_result_destroy(yams_mobile_vector_status_result_t* result) {
    delete result;
}

yams_mobile_string_view
yams_mobile_metadata_result_json(const yams_mobile_metadata_result_t* result) {
    if (!result)
        return {nullptr, 0};
    return {result->json.c_str(), result->json.size()};
}

yams_mobile_string_view
yams_mobile_vector_status_result_json(const yams_mobile_vector_status_result_t* result) {
    if (!result)
        return {nullptr, 0};
    return {result->json.c_str(), result->json.size()};
}

yams_mobile_string_view
yams_mobile_grep_result_stats_json(const yams_mobile_grep_result_t* result) {
    if (!result)
        return {nullptr, 0};
    nlohmann::json j;
    j["executionTimeMs"] = result->response.executionTimeMs;
    j["totalMatches"] = result->response.totalMatches;
    j["filesSearched"] = result->response.filesSearched;
    j["stats"] = result->response.searchStats;
    g_temp_string = j.dump();
    return {g_temp_string.c_str(), g_temp_string.size()};
}

yams_mobile_string_view
yams_mobile_search_result_stats_json(const yams_mobile_search_result_t* result) {
    if (!result)
        return {nullptr, 0};
    nlohmann::json j;
    j["executionTimeMs"] = result->response.executionTimeMs;
    j["total"] = result->response.total;
    j["type"] = result->response.type;
    j["usedHybrid"] = result->response.usedHybrid;
    j["stats"] = result->response.searchStats;
    g_temp_string = j.dump();
    return {g_temp_string.c_str(), g_temp_string.size()};
}

const char* yams_mobile_last_error_message(void) {
    return g_last_error.c_str();
}
