#include <yams/api/mobile_bindings.h>

#include <yams/api/content_store_builder.h>
#include <yams/app/services/factory.hpp>
#include <yams/app/services/services.hpp>
#include <yams/daemon/client/daemon_client.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/migration.h>
#include <yams/metadata/query_helpers.h>

#include <nlohmann/json.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/asio/use_future.hpp>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstddef>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <future>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#ifndef _WIN32
#include <unistd.h>
#endif

namespace {

using yams::ErrorCode;
using yams::Result;
using yams::app::services::AppContext;
using yams::app::services::DeleteByNameRequest;
using yams::app::services::DeleteByNameResponse;
using yams::app::services::DocumentEntry;
using yams::app::services::DownloadServiceRequest;
using yams::app::services::DownloadServiceResponse;
using yams::app::services::GrepRequest;
using yams::app::services::GrepResponse;
using yams::app::services::ListDocumentsRequest;
using yams::app::services::ListDocumentsResponse;
using yams::app::services::RelatedDocument;
using yams::app::services::RetrievedDocument;
using yams::app::services::RetrieveDocumentRequest;
using yams::app::services::RetrieveDocumentResponse;
using yams::app::services::StoreDocumentRequest;
using yams::app::services::StoreDocumentResponse;
using yams::app::services::UpdateMetadataRequest;
using yams::app::services::UpdateMetadataResponse;
using yams::metadata::ConnectionMode;
using yams::metadata::ConnectionPool;
using yams::metadata::ConnectionPoolConfig;
using yams::metadata::Database;
using yams::metadata::MetadataRepository;
using yams::metadata::MigrationManager;
using yams::metadata::queryDocumentsByPattern;
using yams::metadata::YamsMetadataMigrations;
using SearchResponseResult = Result<yams::app::services::SearchResponse>;

void set_last_error(const std::string& message);

constexpr yams_mobile_version_info kVersion{
    YAMS_MOBILE_API_VERSION_MAJOR, YAMS_MOBILE_API_VERSION_MINOR, YAMS_MOBILE_API_VERSION_PATCH};

constexpr std::uint32_t pack_version(std::uint16_t major, std::uint16_t minor,
                                     std::uint16_t patch) {
    return (static_cast<std::uint32_t>(major) << 16U) | (static_cast<std::uint32_t>(minor) << 8U) |
           static_cast<std::uint32_t>(patch);
}

constexpr std::uint32_t pack_version(const yams_mobile_version_info& info) {
    return pack_version(info.major, info.minor, info.patch);
}

constexpr std::uint32_t kPackedVersion = pack_version(kVersion);

struct VersionTriplet {
    std::uint16_t major;
    std::uint16_t minor;
    std::uint16_t patch;
};

constexpr VersionTriplet unpack_version(std::uint32_t encoded) {
    return VersionTriplet{static_cast<std::uint16_t>((encoded >> 16U) & 0xFFFFU),
                          static_cast<std::uint16_t>((encoded >> 8U) & 0x00FFU),
                          static_cast<std::uint16_t>(encoded & 0x00FFU)};
}

std::string version_to_string(std::uint32_t encoded) {
    auto v = unpack_version(encoded);
    std::ostringstream oss;
    oss << v.major << '.' << v.minor << '.' << v.patch;
    return oss.str();
}

bool is_version_supported(std::uint32_t encoded) {
    if (encoded == 0U)
        return true;
    auto version = unpack_version(encoded);
    if (version.major > kVersion.major)
        return false;
    return true;
}

template <typename T> bool ensure_struct_size(std::uint32_t size, const char* name) {
    if (size == 0U)
        return true;
    if (size < sizeof(T)) {
        std::ostringstream oss;
        oss << name << " struct_size=" << size << " is smaller than expected " << sizeof(T);
        set_last_error(oss.str());
        return false;
    }
    return true;
}

yams_mobile_status validate_config_header(const yams_mobile_context_config* config) {
    if (!ensure_struct_size<yams_mobile_context_config>(config->struct_size,
                                                        "yams_mobile_context_config"))
        return YAMS_MOBILE_STATUS_INVALID_ARGUMENT;
    if (!is_version_supported(config->version)) {
        std::ostringstream oss;
        oss << "unsupported mobile ABI version " << version_to_string(config->version)
            << "; expected <= " << version_to_string(kPackedVersion);
        set_last_error(oss.str());
        return YAMS_MOBILE_STATUS_UNAVAILABLE;
    }
    if (config->reserved != 0U) {
        set_last_error("yams_mobile_context_config.reserved must be zero");
        return YAMS_MOBILE_STATUS_INVALID_ARGUMENT;
    }
    if (config->flags != 0U) {
        set_last_error("yams_mobile_context_config.flags must be zero");
        return YAMS_MOBILE_STATUS_INVALID_ARGUMENT;
    }
    if (config->backend_mode != YAMS_MOBILE_BACKEND_EMBEDDED &&
        config->backend_mode != YAMS_MOBILE_BACKEND_DAEMON) {
        set_last_error("yams_mobile_context_config.backend_mode must be embedded or daemon");
        return YAMS_MOBILE_STATUS_INVALID_ARGUMENT;
    }
    return YAMS_MOBILE_STATUS_OK;
}

yams_mobile_status validate_request_header(const yams_mobile_request_header* header,
                                           const char* api_name) {
    if (!header)
        return YAMS_MOBILE_STATUS_INVALID_ARGUMENT;
    if (!ensure_struct_size<yams_mobile_request_header>(header->struct_size,
                                                        "yams_mobile_request_header"))
        return YAMS_MOBILE_STATUS_INVALID_ARGUMENT;
    if (!is_version_supported(header->version)) {
        std::ostringstream oss;
        oss << api_name << " request targets unsupported ABI version "
            << version_to_string(header->version)
            << " (runtime=" << version_to_string(kPackedVersion) << ")";
        set_last_error(oss.str());
        return YAMS_MOBILE_STATUS_UNAVAILABLE;
    }
    if (header->flags != 0U) {
        std::ostringstream oss;
        oss << api_name << " request uses unsupported flag bits (" << header->flags << ")";
        set_last_error(oss.str());
        return YAMS_MOBILE_STATUS_INVALID_ARGUMENT;
    }
    return YAMS_MOBILE_STATUS_OK;
}

enum class TelemetrySinkType { None, Console, Stderr, File };

struct MobileState;

template <typename T> std::shared_ptr<T> to_shared(std::unique_ptr<T>&& ptr) {
    return std::shared_ptr<T>(ptr.release());
}

thread_local std::string g_last_error;
thread_local std::string g_temp_string;

void set_last_error(const std::string& message) {
    g_last_error = message;
}

std::string to_lower_copy(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return value;
}

std::string iso_timestamp_now() {
    using clock = std::chrono::system_clock;
    auto now = clock::now();
    std::time_t t = clock::to_time_t(now);
#ifdef _WIN32
    std::tm tm{};
    gmtime_s(&tm, &t);
#else
    std::tm tm{};
    gmtime_r(&t, &tm);
#endif
    std::ostringstream oss;
    oss << std::put_time(&tm, "%FT%TZ");
    return oss.str();
}

nlohmann::json stats_map_to_json(const std::unordered_map<std::string, std::string>& stats) {
    nlohmann::json obj = nlohmann::json::object();
    for (const auto& [key, value] : stats) {
        obj[key] = value;
    }
    return obj;
}

void emit_telemetry(MobileState& state, const nlohmann::json& payload);

nlohmann::json document_entry_to_json(const DocumentEntry& entry) {
    nlohmann::json j;
    j["name"] = entry.name;
    j["fileName"] = entry.fileName;
    j["hash"] = entry.hash;
    j["path"] = entry.path;
    j["extension"] = entry.extension;
    j["size"] = entry.size;
    j["mimeType"] = entry.mimeType;
    j["fileType"] = entry.fileType;
    j["created"] = entry.created;
    j["modified"] = entry.modified;
    j["indexed"] = entry.indexed;
    if (!entry.tags.empty())
        j["tags"] = entry.tags;
    if (!entry.metadata.empty())
        j["metadata"] = entry.metadata;
    if (entry.snippet)
        j["snippet"] = *entry.snippet;
    if (entry.changeType)
        j["changeType"] = *entry.changeType;
    if (entry.changeTime)
        j["changeTime"] = *entry.changeTime;
    if (entry.matchReason)
        j["matchReason"] = *entry.matchReason;
    if (entry.relevanceScore != 0.0)
        j["relevanceScore"] = entry.relevanceScore;
    return j;
}

nlohmann::json related_document_to_json(const RelatedDocument& rel) {
    nlohmann::json j;
    j["hash"] = rel.hash;
    j["path"] = rel.path;
    j["name"] = rel.name;
    if (rel.relationship)
        j["relationship"] = *rel.relationship;
    j["distance"] = rel.distance;
    j["relevanceScore"] = rel.relevanceScore;
    return j;
}

nlohmann::json retrieved_document_to_json(const RetrievedDocument& doc, bool includeExtractedText) {
    nlohmann::json j;
    j["hash"] = doc.hash;
    j["path"] = doc.path;
    j["name"] = doc.name;
    j["fileName"] = doc.fileName;
    j["mimeType"] = doc.mimeType;
    j["fileType"] = doc.fileType;
    j["size"] = doc.size;
    j["created"] = doc.created;
    j["modified"] = doc.modified;
    j["indexed"] = doc.indexed;
    if (!doc.metadata.empty())
        j["metadata"] = doc.metadata;
    if (!doc.tags.empty())
        j["tags"] = doc.tags;
    if (includeExtractedText && doc.extractedText)
        j["extractedText"] = *doc.extractedText;
    if (doc.content)
        j["contentLength"] = doc.content->size();
    j["isStreaming"] = doc.isStreaming;
    j["bytesTransferred"] = doc.bytesTransferred;
    return j;
}

nlohmann::json list_response_to_json(const ListDocumentsResponse& resp) {
    nlohmann::json root;
    root["count"] = static_cast<std::uint64_t>(resp.count);
    root["totalFound"] = static_cast<std::uint64_t>(resp.totalFound);
    root["hasMore"] = resp.hasMore;
    root["executionTimeMs"] = resp.executionTimeMs;
    if (!resp.queryInfo.empty())
        root["queryInfo"] = resp.queryInfo;
    if (!resp.appliedFormat.empty())
        root["format"] = resp.appliedFormat;
    if (resp.pattern)
        root["pattern"] = *resp.pattern;
    if (!resp.filteredByTags.empty())
        root["filteredByTags"] = resp.filteredByTags;

    nlohmann::json docs = nlohmann::json::array();
    for (const auto& entry : resp.documents)
        docs.push_back(document_entry_to_json(entry));
    root["documents"] = std::move(docs);

    if (!resp.paths.empty())
        root["paths"] = resp.paths;
    if (!resp.addedDocuments.empty()) {
        nlohmann::json added = nlohmann::json::array();
        for (const auto& entry : resp.addedDocuments)
            added.push_back(document_entry_to_json(entry));
        root["addedDocuments"] = std::move(added);
    }
    if (!resp.modifiedDocuments.empty()) {
        nlohmann::json modified = nlohmann::json::array();
        for (const auto& entry : resp.modifiedDocuments)
            modified.push_back(document_entry_to_json(entry));
        root["modifiedDocuments"] = std::move(modified);
    }
    if (!resp.deletedDocuments.empty()) {
        nlohmann::json deleted = nlohmann::json::array();
        for (const auto& entry : resp.deletedDocuments)
            deleted.push_back(document_entry_to_json(entry));
        root["deletedDocuments"] = std::move(deleted);
    }

    if (!resp.groupedResults.empty()) {
        nlohmann::json grouped = nlohmann::json::object();
        for (const auto& [key, docsVec] : resp.groupedResults) {
            nlohmann::json arr = nlohmann::json::array();
            for (const auto& entry : docsVec)
                arr.push_back(document_entry_to_json(entry));
            grouped[key] = std::move(arr);
        }
        root["groupedResults"] = std::move(grouped);
    }

    if (!resp.queryInfo.empty())
        root["queryInfo"] = resp.queryInfo;

    if (!resp.sortBy.empty())
        root["sortBy"] = resp.sortBy;
    if (!resp.sortOrder.empty())
        root["sortOrder"] = resp.sortOrder;

    return root;
}

nlohmann::json retrieve_response_to_json(const RetrieveDocumentResponse& resp,
                                         bool includeExtractedText) {
    nlohmann::json root;
    root["totalFound"] = static_cast<std::uint64_t>(resp.totalFound);
    root["hasMore"] = resp.hasMore;
    root["graphEnabled"] = resp.graphEnabled;
    root["totalBytes"] = resp.totalBytes;
    if (resp.outputPath)
        root["outputPath"] = *resp.outputPath;

    if (resp.document)
        root["document"] = retrieved_document_to_json(*resp.document, includeExtractedText);

    if (!resp.documents.empty()) {
        nlohmann::json arr = nlohmann::json::array();
        for (const auto& doc : resp.documents)
            arr.push_back(retrieved_document_to_json(doc, includeExtractedText));
        root["documents"] = std::move(arr);
    }

    if (!resp.related.empty()) {
        nlohmann::json relArr = nlohmann::json::array();
        for (const auto& rel : resp.related)
            relArr.push_back(related_document_to_json(rel));
        root["related"] = std::move(relArr);
    }

    return root;
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
    std::filesystem::path daemon_socket_path;
    uint32_t max_worker_threads = 0;
    uint32_t flags = 0;
    uint32_t backend_mode = YAMS_MOBILE_BACKEND_EMBEDDED;
    std::uint32_t api_version = kPackedVersion;
    TelemetrySinkType telemetry_sink = TelemetrySinkType::None;
    std::string telemetry_file_path;
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
    std::shared_ptr<yams::app::services::IDownloadService> download_service;
    std::shared_ptr<yams::daemon::DaemonClient> daemon_client;
    TelemetrySinkType telemetry_sink = TelemetrySinkType::None;
    std::ofstream telemetry_stream;
    std::mutex telemetry_mutex;
};

template <typename Req>
Result<yams::daemon::ResponseOfT<Req>> daemon_call(MobileState& state, Req req) {
    if (!state.daemon_client || !state.worker_pool) {
        return yams::Error{ErrorCode::NotInitialized, "daemon client not initialized"};
    }

    using Resp = yams::daemon::ResponseOfT<Req>;
    auto promise = std::make_shared<std::promise<Result<Resp>>>();
    auto future = promise->get_future();
    auto client = state.daemon_client;

    boost::asio::co_spawn(
        state.worker_pool->get_executor(),
        [client, req = std::move(req), promise]() mutable -> boost::asio::awaitable<void> {
            try {
                auto result = co_await client->call<Req>(req);
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

    return future.get();
}

Result<void> daemon_connect(MobileState& state) {
    if (!state.daemon_client || !state.worker_pool) {
        return yams::Error{ErrorCode::NotInitialized, "daemon client not initialized"};
    }

    auto promise = std::make_shared<std::promise<Result<void>>>();
    auto future = promise->get_future();
    auto client = state.daemon_client;

    boost::asio::co_spawn(
        state.worker_pool->get_executor(),
        [client, promise]() mutable -> boost::asio::awaitable<void> {
            try {
                auto result = co_await client->connect();
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

    return future.get();
}

void emit_telemetry(MobileState& state, const nlohmann::json& payload) {
    if (state.telemetry_sink == TelemetrySinkType::None)
        return;
    std::string line = payload.dump();
    std::lock_guard<std::mutex> lock(state.telemetry_mutex);
    switch (state.telemetry_sink) {
        case TelemetrySinkType::Console:
            std::cout << line << std::endl;
            break;
        case TelemetrySinkType::Stderr:
            std::cerr << line << std::endl;
            break;
        case TelemetrySinkType::File:
            if (state.telemetry_stream.is_open()) {
                state.telemetry_stream << line << std::endl;
                state.telemetry_stream.flush();
            }
            break;
        case TelemetrySinkType::None:
        default:
            break;
    }
}

} // namespace

struct yams_mobile_context_t {
    MobileState state;
};

struct yams_mobile_grep_result_t {
    yams::app::services::GrepResponse response;
    std::string json;
    std::string stats_json;
    bool pre_serialized = false;
};

struct yams_mobile_search_result_t {
    yams::app::services::SearchResponse response;
    std::string json;
    std::string stats_json;
    bool pre_serialized = false;
};

struct yams_mobile_metadata_result_t {
    std::string json;
};

struct yams_mobile_vector_status_result_t {
    std::string json;
};

struct yams_mobile_list_result_t {
    std::string json;
};

struct yams_mobile_document_get_result_t {
    std::string json;
    std::string content;
};

struct yams_mobile_update_result_t {
    std::string json;
};

struct yams_mobile_delete_result_t {
    std::string json;
};

struct yams_mobile_graph_query_result_t {
    std::string json;
};

YAMS_MOBILE_API yams_mobile_version_info yams_mobile_get_version(void) {
    return kVersion;
}

YAMS_MOBILE_API yams_mobile_status yams_mobile_context_create(
    const yams_mobile_context_config* config, yams_mobile_context_t** out_context) {
    if (out_context == nullptr) {
        set_last_error("out_context pointer is null");
        return YAMS_MOBILE_STATUS_INVALID_ARGUMENT;
    }
    *out_context = nullptr;
    if (config == nullptr) {
        set_last_error("config pointer is null");
        return YAMS_MOBILE_STATUS_INVALID_ARGUMENT;
    }

    if (auto status = validate_config_header(config); status != YAMS_MOBILE_STATUS_OK)
        return status;

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
        ctx->state.config.backend_mode = config->backend_mode;
        ctx->state.config.api_version = config->version != 0U ? config->version : kPackedVersion;
        if (config->daemon_socket_path && *config->daemon_socket_path) {
            ctx->state.config.daemon_socket_path =
                std::filesystem::path(config->daemon_socket_path);
        }

        if (config->telemetry_sink && *config->telemetry_sink) {
            std::string sink = config->telemetry_sink;
            auto lower = to_lower_copy(sink);
            if (lower == "console") {
                ctx->state.config.telemetry_sink = TelemetrySinkType::Console;
            } else if (lower == "stderr") {
                ctx->state.config.telemetry_sink = TelemetrySinkType::Stderr;
            } else if (lower == "noop" || lower == "none") {
                ctx->state.config.telemetry_sink = TelemetrySinkType::None;
            } else if (lower.rfind("file:", 0) == 0) {
                std::string path = sink.substr(5);
                if (path.empty()) {
                    set_last_error("telemetry sink file path is empty");
                    return YAMS_MOBILE_STATUS_INVALID_ARGUMENT;
                }
                ctx->state.config.telemetry_sink = TelemetrySinkType::File;
                ctx->state.config.telemetry_file_path = path;
            } else {
                set_last_error("unsupported telemetry sink");
                return YAMS_MOBILE_STATUS_INVALID_ARGUMENT;
            }
        }

        std::error_code ec;
        std::filesystem::create_directories(ctx->state.config.working_directory, ec);
        std::filesystem::create_directories(ctx->state.config.cache_directory, ec);
        if (ctx->state.config.backend_mode == YAMS_MOBILE_BACKEND_EMBEDDED) {
            std::filesystem::create_directories(ctx->state.config.storage_directory, ec);
        }

        // Thread pool / executor
        auto threads = ctx->state.config.max_worker_threads;
        if (threads == 0)
            threads = std::max<uint32_t>(1, std::thread::hardware_concurrency());
        ctx->state.worker_pool = std::make_unique<boost::asio::thread_pool>(threads);

        if (ctx->state.config.backend_mode == YAMS_MOBILE_BACKEND_DAEMON) {
            yams::daemon::ClientConfig clientConfig;
            clientConfig.autoStart = false;
            clientConfig.executor = ctx->state.worker_pool->get_executor();
            if (!ctx->state.config.daemon_socket_path.empty()) {
                clientConfig.socketPath = ctx->state.config.daemon_socket_path;
            }

            ctx->state.daemon_client = std::make_shared<yams::daemon::DaemonClient>(clientConfig);
            auto connectRes = daemon_connect(ctx->state);
            if (!connectRes) {
                set_last_error(connectRes.error().message);
                return map_error_code(connectRes.error().code);
            }
        } else {
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
            ctx->state.search_service =
                yams::app::services::makeSearchService(ctx->state.app_context);
            ctx->state.grep_service = yams::app::services::makeGrepService(ctx->state.app_context);
            ctx->state.download_service =
                yams::app::services::makeDownloadService(ctx->state.app_context);
        }

        ctx->state.telemetry_sink = ctx->state.config.telemetry_sink;
        if (ctx->state.telemetry_sink == TelemetrySinkType::File) {
            ctx->state.telemetry_stream.open(ctx->state.config.telemetry_file_path,
                                             std::ios::app | std::ios::out);
            if (!ctx->state.telemetry_stream.is_open()) {
                set_last_error("failed to open telemetry sink file");
                return YAMS_MOBILE_STATUS_INVALID_ARGUMENT;
            }
        }

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

YAMS_MOBILE_API void yams_mobile_context_destroy(yams_mobile_context_t* ctx) {
    if (!ctx)
        return;
    if (ctx->state.worker_pool) {
        ctx->state.worker_pool->join();
    }
    delete ctx;
}

YAMS_MOBILE_API yams_mobile_status
yams_mobile_grep_execute(yams_mobile_context_t* ctx, const yams_mobile_grep_request* request,
                         yams_mobile_grep_result_t** out_result) {
    if (out_result)
        *out_result = nullptr;
    if (!ctx || !request) {
        set_last_error("invalid arguments");
        return YAMS_MOBILE_STATUS_INVALID_ARGUMENT;
    }
    if (auto status = validate_request_header(&request->header, "grep");
        status != YAMS_MOBILE_STATUS_OK)
        return status;

    // Daemon mode
    if (ctx->state.daemon_client) {
        yams::daemon::GrepRequest dreq;
        dreq.pattern = request->pattern ? request->pattern : "";
        dreq.literalText = request->literal != 0;
        dreq.caseInsensitive = request->ignore_case != 0;
        dreq.wholeWord = request->word_boundary != 0;
        dreq.maxMatches = request->max_matches;

        auto result = daemon_call(ctx->state, std::move(dreq));
        if (!result) {
            set_last_error(result.error().message);
            return map_error_code(result.error().code);
        }

        auto& resp = result.value();
        nlohmann::json root;
        root["totalMatches"] = resp.totalMatches;
        root["filesSearched"] = resp.filesSearched;
        root["regexMatches"] = resp.regexMatches;
        root["semanticMatches"] = resp.semanticMatches;
        root["executionTimeMs"] = resp.executionTimeMs;
        if (!resp.queryInfo.empty())
            root["queryInfo"] = resp.queryInfo;
        if (!resp.searchStats.empty())
            root["stats"] = resp.searchStats;

        // Group matches by file for consistent JSON format
        std::unordered_map<std::string, nlohmann::json> fileMap;
        for (const auto& m : resp.matches) {
            auto& entry = fileMap[m.file];
            if (!entry.contains("file")) {
                entry["file"] = m.file;
                entry["fileName"] = m.file;
                entry["matchCount"] = 0;
                entry["matches"] = nlohmann::json::array();
            }
            nlohmann::json mj;
            mj["lineNumber"] = m.lineNumber;
            mj["line"] = m.line;
            if (!m.contextBefore.empty())
                mj["before"] = m.contextBefore;
            if (!m.contextAfter.empty())
                mj["after"] = m.contextAfter;
            if (!m.matchType.empty())
                mj["matchType"] = m.matchType;
            mj["confidence"] = m.confidence;
            entry["matches"].push_back(std::move(mj));
            entry["matchCount"] = entry["matches"].size();
        }
        nlohmann::json files = nlohmann::json::array();
        for (auto& [_, entry] : fileMap)
            files.push_back(std::move(entry));
        root["files"] = std::move(files);
        if (!resp.filesWith.empty())
            root["filesWith"] = resp.filesWith;
        if (!resp.filesWithout.empty())
            root["filesWithout"] = resp.filesWithout;
        if (!resp.pathsOnly.empty())
            root["pathsOnly"] = resp.pathsOnly;

        nlohmann::json statsJson;
        statsJson["executionTimeMs"] = resp.executionTimeMs;
        statsJson["totalMatches"] = resp.totalMatches;
        statsJson["filesSearched"] = resp.filesSearched;
        if (!resp.searchStats.empty())
            statsJson["stats"] = resp.searchStats;

        if (out_result) {
            auto wrapper = std::make_unique<yams_mobile_grep_result_t>();
            wrapper->json = root.dump();
            wrapper->stats_json = statsJson.dump();
            wrapper->pre_serialized = true;
            *out_result = wrapper.release();
        }

        nlohmann::json telemetry;
        telemetry["event"] = "grep";
        telemetry["timestamp"] = iso_timestamp_now();
        telemetry["executionTimeMs"] = resp.executionTimeMs;
        telemetry["totalMatches"] = resp.totalMatches;
        telemetry["filesSearched"] = resp.filesSearched;
        emit_telemetry(ctx->state, telemetry);

        set_last_error("");
        return YAMS_MOBILE_STATUS_OK;
    }

    // Embedded mode
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

    auto response = std::move(result.value());

    nlohmann::json telemetry;
    telemetry["event"] = "grep";
    telemetry["timestamp"] = iso_timestamp_now();
    telemetry["executionTimeMs"] = response.executionTimeMs;
    telemetry["totalMatches"] = static_cast<std::int64_t>(response.totalMatches);
    telemetry["filesSearched"] = static_cast<std::int64_t>(response.filesSearched);
    telemetry["stats"] = stats_map_to_json(response.searchStats);

    if (out_result) {
        auto wrapper = std::make_unique<yams_mobile_grep_result_t>();
        wrapper->response = std::move(response);
        *out_result = wrapper.release();
    }

    emit_telemetry(ctx->state, telemetry);
    set_last_error("");
    return YAMS_MOBILE_STATUS_OK;
}

YAMS_MOBILE_API void yams_mobile_grep_result_destroy(yams_mobile_grep_result_t* result) {
    delete result;
}

YAMS_MOBILE_API yams_mobile_status
yams_mobile_search_execute(yams_mobile_context_t* ctx, const yams_mobile_search_request* request,
                           yams_mobile_search_result_t** out_result) {
    if (out_result)
        *out_result = nullptr;
    if (!ctx || !request) {
        set_last_error("invalid arguments");
        return YAMS_MOBILE_STATUS_INVALID_ARGUMENT;
    }
    if (auto status = validate_request_header(&request->header, "search");
        status != YAMS_MOBILE_STATUS_OK)
        return status;

    // Daemon mode
    if (ctx->state.daemon_client) {
        yams::daemon::SearchRequest dreq;
        dreq.query = request->query ? request->query : "";
        dreq.limit = request->limit;
        dreq.pathsOnly = request->paths_only != 0;
        dreq.searchType = request->semantic ? "semantic" : "hybrid";
        if (request->tags && request->tag_count > 0) {
            for (size_t i = 0; i < request->tag_count; ++i) {
                if (request->tags[i])
                    dreq.tags.emplace_back(request->tags[i]);
            }
        }

        auto result = daemon_call(ctx->state, std::move(dreq));
        if (!result) {
            set_last_error(result.error().message);
            return map_error_code(result.error().code);
        }

        auto& resp = result.value();
        nlohmann::json root;
        root["total"] = resp.totalCount;
        root["executionTimeMs"] = resp.elapsed.count();
        if (!resp.traceId.empty())
            root["traceId"] = resp.traceId;

        nlohmann::json arr = nlohmann::json::array();
        for (const auto& item : resp.results) {
            nlohmann::json entry;
            entry["id"] = item.id;
            entry["path"] = item.path;
            entry["title"] = item.title;
            if (!item.snippet.empty())
                entry["snippet"] = item.snippet;
            entry["score"] = item.score;
            if (!item.metadata.empty())
                entry["metadata"] = item.metadata;
            arr.push_back(std::move(entry));
        }
        root["results"] = std::move(arr);

        nlohmann::json statsJson;
        statsJson["executionTimeMs"] = resp.elapsed.count();
        statsJson["total"] = resp.totalCount;

        if (out_result) {
            auto wrapper = std::make_unique<yams_mobile_search_result_t>();
            wrapper->json = root.dump();
            wrapper->stats_json = statsJson.dump();
            wrapper->pre_serialized = true;
            *out_result = wrapper.release();
        }

        nlohmann::json telemetry;
        telemetry["event"] = "search";
        telemetry["timestamp"] = iso_timestamp_now();
        telemetry["executionTimeMs"] = resp.elapsed.count();
        telemetry["total"] = resp.totalCount;
        emit_telemetry(ctx->state, telemetry);

        set_last_error("");
        return YAMS_MOBILE_STATUS_OK;
    }

    // Embedded mode
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

        auto response = std::move(result.value());

        nlohmann::json telemetry;
        telemetry["event"] = "search";
        telemetry["timestamp"] = iso_timestamp_now();
        telemetry["executionTimeMs"] = response.executionTimeMs;
        telemetry["total"] = static_cast<std::int64_t>(response.total);
        telemetry["type"] = response.type;
        telemetry["stats"] = stats_map_to_json(response.searchStats);

        if (out_result) {
            auto wrapper = std::make_unique<yams_mobile_search_result_t>();
            wrapper->response = std::move(response);
            *out_result = wrapper.release();
        }

        emit_telemetry(ctx->state, telemetry);
        set_last_error("");
        return YAMS_MOBILE_STATUS_OK;
    } catch (const std::exception& ex) {
        set_last_error(ex.what());
        return YAMS_MOBILE_STATUS_INTERNAL_ERROR;
    }
}

YAMS_MOBILE_API void yams_mobile_search_result_destroy(yams_mobile_search_result_t* result) {
    delete result;
}

YAMS_MOBILE_API yams_mobile_status yams_mobile_store_document(
    yams_mobile_context_t* ctx, const yams_mobile_document_store_request* request,
    yams_mobile_string_view* out_hash) {
    if (out_hash) {
        out_hash->data = nullptr;
        out_hash->length = 0;
    }
    if (!ctx || !request) {
        set_last_error("invalid arguments");
        return YAMS_MOBILE_STATUS_INVALID_ARGUMENT;
    }
    if (auto status = validate_request_header(&request->header, "store_document");
        status != YAMS_MOBILE_STATUS_OK)
        return status;

    // Daemon mode
    if (ctx->state.daemon_client) {
        yams::daemon::AddDocumentRequest dreq;
        if (request->path)
            dreq.path = request->path;
        if (request->tags && request->tag_count > 0) {
            for (size_t i = 0; i < request->tag_count; ++i) {
                if (request->tags[i])
                    dreq.tags.emplace_back(request->tags[i]);
            }
        }

        auto result = daemon_call(ctx->state, std::move(dreq));
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

    // Embedded mode
    auto service = ctx->state.document_service;
    if (!service) {
        set_last_error("document service not initialized");
        return YAMS_MOBILE_STATUS_NOT_INITIALIZED;
    }

    StoreDocumentRequest req;
    if (request->path)
        req.path = request->path;
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

YAMS_MOBILE_API yams_mobile_status yams_mobile_remove_document(yams_mobile_context_t* ctx,
                                                               const char* document_hash) {
    if (!ctx || !document_hash || *document_hash == '\0') {
        set_last_error("invalid arguments");
        return YAMS_MOBILE_STATUS_INVALID_ARGUMENT;
    }

    // Daemon mode
    if (ctx->state.daemon_client) {
        yams::daemon::DeleteRequest dreq;
        dreq.hash = document_hash;

        auto result = daemon_call(ctx->state, std::move(dreq));
        if (!result) {
            set_last_error(result.error().message);
            return map_error_code(result.error().code);
        }

        if (result.value().failureCount > 0) {
            std::string errMsg = "delete failed";
            if (!result.value().results.empty() && !result.value().results[0].error.empty())
                errMsg = result.value().results[0].error;
            set_last_error(errMsg);
            return YAMS_MOBILE_STATUS_INTERNAL_ERROR;
        }

        set_last_error("");
        return YAMS_MOBILE_STATUS_OK;
    }

    // Embedded mode
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

YAMS_MOBILE_API yams_mobile_status yams_mobile_download(yams_mobile_context_t* ctx,
                                                        const yams_mobile_download_request* request,
                                                        yams_mobile_string_view* out_hash) {
    if (out_hash) {
        out_hash->data = nullptr;
        out_hash->length = 0;
    }
    if (!ctx || !request) {
        set_last_error("invalid arguments");
        return YAMS_MOBILE_STATUS_INVALID_ARGUMENT;
    }
    if (auto status = validate_request_header(&request->header, "download");
        status != YAMS_MOBILE_STATUS_OK)
        return status;
    if (!request->url || *request->url == '\0') {
        set_last_error("download url is required");
        return YAMS_MOBILE_STATUS_INVALID_ARGUMENT;
    }
    if (request->metadata_count > 0 && (!request->metadata_keys || !request->metadata_values)) {
        set_last_error("metadata keys and values are required when metadata_count > 0");
        return YAMS_MOBILE_STATUS_INVALID_ARGUMENT;
    }

    // Daemon mode
    if (ctx->state.daemon_client) {
        yams::daemon::DownloadRequest dreq;
        dreq.url = request->url;
        if (request->tags && request->tag_count > 0) {
            dreq.tags.reserve(request->tag_count);
            for (size_t i = 0; i < request->tag_count; ++i) {
                if (request->tags[i])
                    dreq.tags.emplace_back(request->tags[i]);
            }
        }
        for (size_t i = 0; i < request->metadata_count; ++i) {
            const char* key = request->metadata_keys[i];
            const char* value = request->metadata_values[i];
            if (!key || *key == '\0')
                continue;
            dreq.metadata[std::string(key)] = value ? std::string(value) : std::string();
        }

        auto result = daemon_call(ctx->state, std::move(dreq));
        if (!result) {
            set_last_error(result.error().message);
            return map_error_code(result.error().code);
        }

        if (!result.value().success) {
            set_last_error(result.value().error.empty() ? "download failed" : result.value().error);
            return YAMS_MOBILE_STATUS_INTERNAL_ERROR;
        }

        if (out_hash) {
            g_temp_string = result.value().hash;
            out_hash->data = g_temp_string.c_str();
            out_hash->length = g_temp_string.size();
        }

        nlohmann::json telemetry;
        telemetry["event"] = "download";
        telemetry["timestamp"] = iso_timestamp_now();
        telemetry["url"] = result.value().url;
        telemetry["hash"] = result.value().hash;
        telemetry["success"] = result.value().success;
        telemetry["sizeBytes"] = result.value().size;
        emit_telemetry(ctx->state, telemetry);

        set_last_error("");
        return YAMS_MOBILE_STATUS_OK;
    }

    // Embedded mode
    auto service = ctx->state.download_service;
    if (!service) {
        set_last_error("download service not initialized");
        return YAMS_MOBILE_STATUS_NOT_INITIALIZED;
    }

    DownloadServiceRequest req;
    req.url = request->url;
    if (request->timeout_ms > 0) {
        req.timeout = std::chrono::milliseconds(request->timeout_ms);
    }
    req.overwrite = request->overwrite != 0 ? yams::downloader::OverwritePolicy::Always
                                            : yams::downloader::OverwritePolicy::Never;

    if (request->tags && request->tag_count > 0) {
        req.tags.reserve(request->tag_count);
        for (size_t i = 0; i < request->tag_count; ++i) {
            if (request->tags[i])
                req.tags.emplace_back(request->tags[i]);
        }
    }

    for (size_t i = 0; i < request->metadata_count; ++i) {
        const char* key = request->metadata_keys[i];
        const char* value = request->metadata_values[i];
        if (!key || *key == '\0')
            continue;
        req.metadata[std::string(key)] = value ? std::string(value) : std::string();
    }

    auto result = service->download(req);
    if (!result) {
        set_last_error(result.error().message);
        return map_error_code(result.error().code);
    }

    if (out_hash) {
        g_temp_string = result.value().hash;
        out_hash->data = g_temp_string.c_str();
        out_hash->length = g_temp_string.size();
    }

    nlohmann::json telemetry;
    telemetry["event"] = "download";
    telemetry["timestamp"] = iso_timestamp_now();
    telemetry["url"] = result.value().url;
    telemetry["hash"] = result.value().hash;
    telemetry["success"] = result.value().success;
    telemetry["sizeBytes"] = result.value().sizeBytes;
    emit_telemetry(ctx->state, telemetry);

    set_last_error("");
    return YAMS_MOBILE_STATUS_OK;
}

YAMS_MOBILE_API yams_mobile_status
yams_mobile_update_document(yams_mobile_context_t* ctx, const yams_mobile_update_request* request,
                            yams_mobile_update_result_t** out_result) {
    if (out_result)
        *out_result = nullptr;
    if (!ctx || !request) {
        set_last_error("invalid arguments");
        return YAMS_MOBILE_STATUS_INVALID_ARGUMENT;
    }
    if (auto status = validate_request_header(&request->header, "update_document");
        status != YAMS_MOBILE_STATUS_OK)
        return status;
    if ((!request->hash || *request->hash == '\0') && (!request->name || *request->name == '\0')) {
        set_last_error("hash or name is required");
        return YAMS_MOBILE_STATUS_INVALID_ARGUMENT;
    }
    if (request->metadata_count > 0 && (!request->metadata_keys || !request->metadata_values)) {
        set_last_error("metadata keys and values are required when metadata_count > 0");
        return YAMS_MOBILE_STATUS_INVALID_ARGUMENT;
    }

    // Daemon mode
    if (ctx->state.daemon_client) {
        yams::daemon::UpdateDocumentRequest dreq;
        if (request->hash)
            dreq.hash = request->hash;
        if (request->name)
            dreq.name = request->name;
        if (request->add_tags && request->add_tag_count > 0) {
            dreq.addTags.reserve(request->add_tag_count);
            for (size_t i = 0; i < request->add_tag_count; ++i) {
                if (request->add_tags[i])
                    dreq.addTags.emplace_back(request->add_tags[i]);
            }
        }
        if (request->remove_tags && request->remove_tag_count > 0) {
            dreq.removeTags.reserve(request->remove_tag_count);
            for (size_t i = 0; i < request->remove_tag_count; ++i) {
                if (request->remove_tags[i])
                    dreq.removeTags.emplace_back(request->remove_tags[i]);
            }
        }
        for (size_t i = 0; i < request->metadata_count; ++i) {
            const char* key = request->metadata_keys[i];
            const char* value = request->metadata_values[i];
            if (!key || *key == '\0')
                continue;
            dreq.metadata[std::string(key)] = value ? std::string(value) : std::string();
        }

        auto result = daemon_call(ctx->state, std::move(dreq));
        if (!result) {
            set_last_error(result.error().message);
            return map_error_code(result.error().code);
        }

        auto& resp = result.value();
        auto wrapper = std::make_unique<yams_mobile_update_result_t>();
        nlohmann::json j;
        j["success"] = resp.contentUpdated || resp.metadataUpdated || resp.tagsUpdated;
        j["hash"] = resp.hash;
        j["contentUpdated"] = resp.contentUpdated;
        j["metadataUpdated"] = resp.metadataUpdated;
        j["tagsUpdated"] = resp.tagsUpdated;
        wrapper->json = j.dump();
        if (out_result)
            *out_result = wrapper.release();

        set_last_error("");
        return YAMS_MOBILE_STATUS_OK;
    }

    // Embedded mode
    auto service = ctx->state.document_service;
    if (!service) {
        set_last_error("document service not initialized");
        return YAMS_MOBILE_STATUS_NOT_INITIALIZED;
    }

    UpdateMetadataRequest req;
    if (request->hash)
        req.hash = request->hash;
    if (request->name)
        req.name = request->name;

    if (request->add_tags && request->add_tag_count > 0) {
        req.addTags.reserve(request->add_tag_count);
        for (size_t i = 0; i < request->add_tag_count; ++i) {
            if (request->add_tags[i])
                req.addTags.emplace_back(request->add_tags[i]);
        }
    }

    if (request->remove_tags && request->remove_tag_count > 0) {
        req.removeTags.reserve(request->remove_tag_count);
        for (size_t i = 0; i < request->remove_tag_count; ++i) {
            if (request->remove_tags[i])
                req.removeTags.emplace_back(request->remove_tags[i]);
        }
    }

    for (size_t i = 0; i < request->metadata_count; ++i) {
        const char* key = request->metadata_keys[i];
        const char* value = request->metadata_values[i];
        if (!key || *key == '\0')
            continue;
        req.keyValues[std::string(key)] = value ? std::string(value) : std::string();
    }

    auto result = service->updateMetadata(req);
    if (!result) {
        set_last_error(result.error().message);
        return map_error_code(result.error().code);
    }

    auto wrapper = std::make_unique<yams_mobile_update_result_t>();
    nlohmann::json j;
    j["success"] = result.value().success;
    j["hash"] = result.value().hash;
    j["updatesApplied"] = static_cast<std::uint64_t>(result.value().updatesApplied);
    j["contentUpdated"] = result.value().contentUpdated;
    j["tagsAdded"] = static_cast<std::uint64_t>(result.value().tagsAdded);
    j["tagsRemoved"] = static_cast<std::uint64_t>(result.value().tagsRemoved);
    if (result.value().documentId)
        j["documentId"] = *result.value().documentId;
    if (!result.value().backupHash.empty())
        j["backupHash"] = result.value().backupHash;
    wrapper->json = j.dump();

    if (out_result)
        *out_result = wrapper.release();

    set_last_error("");
    return YAMS_MOBILE_STATUS_OK;
}

YAMS_MOBILE_API void yams_mobile_update_result_destroy(yams_mobile_update_result_t* result) {
    delete result;
}

YAMS_MOBILE_API yams_mobile_string_view
yams_mobile_update_result_json(const yams_mobile_update_result_t* result) {
    if (!result)
        return {nullptr, 0};
    return {result->json.c_str(), result->json.size()};
}

YAMS_MOBILE_API yams_mobile_status
yams_mobile_delete_by_name(yams_mobile_context_t* ctx, const yams_mobile_delete_request* request,
                           yams_mobile_delete_result_t** out_result) {
    if (out_result)
        *out_result = nullptr;
    if (!ctx || !request) {
        set_last_error("invalid arguments");
        return YAMS_MOBILE_STATUS_INVALID_ARGUMENT;
    }
    if (auto status = validate_request_header(&request->header, "delete_by_name");
        status != YAMS_MOBILE_STATUS_OK)
        return status;

    // Daemon mode - uses DeleteRequest with name/hash/pattern
    if (ctx->state.daemon_client) {
        yams::daemon::DeleteRequest dreq;
        if (request->hash)
            dreq.hash = request->hash;
        if (request->name)
            dreq.name = request->name;
        if (request->pattern)
            dreq.pattern = request->pattern;
        dreq.dryRun = request->dry_run != 0;

        if (dreq.hash.empty() && dreq.name.empty() && dreq.pattern.empty()) {
            set_last_error("hash, name, or pattern is required");
            return YAMS_MOBILE_STATUS_INVALID_ARGUMENT;
        }

        auto result = daemon_call(ctx->state, std::move(dreq));
        if (!result) {
            set_last_error(result.error().message);
            return map_error_code(result.error().code);
        }

        auto& resp = result.value();
        auto wrapper = std::make_unique<yams_mobile_delete_result_t>();
        nlohmann::json j;
        j["dryRun"] = resp.dryRun;
        j["count"] = resp.successCount;

        nlohmann::json deleted = nlohmann::json::array();
        nlohmann::json errors = nlohmann::json::array();
        for (const auto& entry : resp.results) {
            nlohmann::json item;
            item["name"] = entry.name;
            item["hash"] = entry.hash;
            item["deleted"] = entry.success;
            if (!entry.error.empty())
                item["error"] = entry.error;
            if (entry.success)
                deleted.push_back(std::move(item));
            else
                errors.push_back(std::move(item));
        }
        j["deleted"] = std::move(deleted);
        j["errors"] = std::move(errors);
        wrapper->json = j.dump();
        if (out_result)
            *out_result = wrapper.release();

        set_last_error("");
        return YAMS_MOBILE_STATUS_OK;
    }

    // Embedded mode
    auto service = ctx->state.document_service;
    if (!service) {
        set_last_error("document service not initialized");
        return YAMS_MOBILE_STATUS_NOT_INITIALIZED;
    }

    DeleteByNameRequest req;
    if (request->hash)
        req.hash = request->hash;
    if (request->name)
        req.name = request->name;
    if (request->pattern)
        req.pattern = request->pattern;
    req.dryRun = request->dry_run != 0;

    if (req.hash.empty() && req.name.empty() && req.pattern.empty()) {
        set_last_error("hash, name, or pattern is required");
        return YAMS_MOBILE_STATUS_INVALID_ARGUMENT;
    }

    auto result = service->deleteByName(req);
    if (!result) {
        set_last_error(result.error().message);
        return map_error_code(result.error().code);
    }

    auto wrapper = std::make_unique<yams_mobile_delete_result_t>();
    nlohmann::json j;
    j["dryRun"] = result.value().dryRun;
    j["count"] = static_cast<std::uint64_t>(result.value().count);

    nlohmann::json deleted = nlohmann::json::array();
    for (const auto& entry : result.value().deleted) {
        nlohmann::json item;
        item["name"] = entry.name;
        item["hash"] = entry.hash;
        item["deleted"] = entry.deleted;
        if (entry.error)
            item["error"] = *entry.error;
        deleted.push_back(std::move(item));
    }
    j["deleted"] = std::move(deleted);

    nlohmann::json errors = nlohmann::json::array();
    for (const auto& entry : result.value().errors) {
        nlohmann::json item;
        item["name"] = entry.name;
        item["hash"] = entry.hash;
        item["deleted"] = entry.deleted;
        if (entry.error)
            item["error"] = *entry.error;
        errors.push_back(std::move(item));
    }
    j["errors"] = std::move(errors);

    wrapper->json = j.dump();
    if (out_result)
        *out_result = wrapper.release();

    set_last_error("");
    return YAMS_MOBILE_STATUS_OK;
}

YAMS_MOBILE_API void yams_mobile_delete_result_destroy(yams_mobile_delete_result_t* result) {
    delete result;
}

YAMS_MOBILE_API yams_mobile_string_view
yams_mobile_delete_result_json(const yams_mobile_delete_result_t* result) {
    if (!result)
        return {nullptr, 0};
    return {result->json.c_str(), result->json.size()};
}

YAMS_MOBILE_API yams_mobile_status
yams_mobile_graph_query(yams_mobile_context_t* ctx, const yams_mobile_graph_query_request* request,
                        yams_mobile_graph_query_result_t** out_result) {
    if (out_result)
        *out_result = nullptr;
    if (!ctx || !request) {
        set_last_error("invalid arguments");
        return YAMS_MOBILE_STATUS_INVALID_ARGUMENT;
    }
    if (auto status = validate_request_header(&request->header, "graph_query");
        status != YAMS_MOBILE_STATUS_OK)
        return status;

    if (!ctx->state.daemon_client) {
        set_last_error("graph query currently requires daemon backend mode");
        return YAMS_MOBILE_STATUS_UNAVAILABLE;
    }

    yams::daemon::GraphQueryRequest req;
    if (request->document_hash)
        req.documentHash = request->document_hash;
    if (request->document_name)
        req.documentName = request->document_name;
    if (request->snapshot_id)
        req.snapshotId = request->snapshot_id;
    req.nodeId = request->node_id;
    req.maxDepth = request->max_depth > 0 ? request->max_depth : 1;
    req.maxResults = request->max_results > 0 ? request->max_results : 200;
    req.offset = request->offset;
    req.limit = request->limit > 0 ? request->limit : 100;
    req.reverseTraversal = request->reverse_traversal != 0;
    req.includeEdgeProperties = request->include_edge_properties != 0;
    req.includeNodeProperties = request->include_node_properties != 0;

    if (request->relation_filters && request->relation_filter_count > 0) {
        req.relationFilters.reserve(request->relation_filter_count);
        for (size_t i = 0; i < request->relation_filter_count; ++i) {
            if (request->relation_filters[i])
                req.relationFilters.emplace_back(request->relation_filters[i]);
        }
    }

    auto result = daemon_call(ctx->state, std::move(req));
    if (!result) {
        set_last_error(result.error().message);
        return map_error_code(result.error().code);
    }

    nlohmann::json root;
    root["totalNodesFound"] = result.value().totalNodesFound;
    root["totalEdgesTraversed"] = result.value().totalEdgesTraversed;
    root["truncated"] = result.value().truncated;
    root["maxDepthReached"] = result.value().maxDepthReached;
    root["queryTimeMs"] = result.value().queryTimeMs;
    root["kgAvailable"] = result.value().kgAvailable;
    if (!result.value().warning.empty())
        root["warning"] = result.value().warning;

    nlohmann::json origin;
    origin["nodeId"] = result.value().originNode.nodeId;
    origin["nodeKey"] = result.value().originNode.nodeKey;
    origin["label"] = result.value().originNode.label;
    origin["type"] = result.value().originNode.type;
    origin["documentHash"] = result.value().originNode.documentHash;
    origin["documentPath"] = result.value().originNode.documentPath;
    origin["snapshotId"] = result.value().originNode.snapshotId;
    origin["distance"] = result.value().originNode.distance;
    if (!result.value().originNode.properties.empty())
        origin["properties"] = result.value().originNode.properties;
    root["originNode"] = std::move(origin);

    nlohmann::json nodes = nlohmann::json::array();
    for (const auto& node : result.value().connectedNodes) {
        nlohmann::json n;
        n["nodeId"] = node.nodeId;
        n["nodeKey"] = node.nodeKey;
        n["label"] = node.label;
        n["type"] = node.type;
        n["documentHash"] = node.documentHash;
        n["documentPath"] = node.documentPath;
        n["snapshotId"] = node.snapshotId;
        n["distance"] = node.distance;
        if (!node.properties.empty())
            n["properties"] = node.properties;
        nodes.push_back(std::move(n));
    }
    root["connectedNodes"] = std::move(nodes);

    nlohmann::json edges = nlohmann::json::array();
    for (const auto& edge : result.value().edges) {
        nlohmann::json e;
        e["edgeId"] = edge.edgeId;
        e["srcNodeId"] = edge.srcNodeId;
        e["dstNodeId"] = edge.dstNodeId;
        e["relation"] = edge.relation;
        e["weight"] = edge.weight;
        if (!edge.properties.empty())
            e["properties"] = edge.properties;
        edges.push_back(std::move(e));
    }
    root["edges"] = std::move(edges);

    if (!result.value().nodeTypeCounts.empty()) {
        nlohmann::json counts = nlohmann::json::array();
        for (const auto& [type, count] : result.value().nodeTypeCounts) {
            counts.push_back({{"type", type}, {"count", count}});
        }
        root["nodeTypeCounts"] = std::move(counts);
    }

    if (!result.value().relationTypeCounts.empty()) {
        nlohmann::json counts = nlohmann::json::array();
        for (const auto& [relation, count] : result.value().relationTypeCounts) {
            counts.push_back({{"relation", relation}, {"count", count}});
        }
        root["relationTypeCounts"] = std::move(counts);
    }

    auto wrapper = std::make_unique<yams_mobile_graph_query_result_t>();
    wrapper->json = root.dump();
    if (out_result)
        *out_result = wrapper.release();

    set_last_error("");
    return YAMS_MOBILE_STATUS_OK;
}

YAMS_MOBILE_API void
yams_mobile_graph_query_result_destroy(yams_mobile_graph_query_result_t* result) {
    delete result;
}

YAMS_MOBILE_API yams_mobile_string_view
yams_mobile_graph_query_result_json(const yams_mobile_graph_query_result_t* result) {
    if (!result)
        return {nullptr, 0};
    return {result->json.c_str(), result->json.size()};
}

YAMS_MOBILE_API yams_mobile_status
yams_mobile_get_metadata(yams_mobile_context_t* ctx, const yams_mobile_metadata_request* request,
                         yams_mobile_metadata_result_t** out_result) {
    if (out_result)
        *out_result = nullptr;
    if (!ctx) {
        set_last_error("context is null");
        return YAMS_MOBILE_STATUS_INVALID_ARGUMENT;
    }

    // Daemon mode — use GetRequest with metadataOnly or ListRequest for pattern
    if (ctx->state.daemon_client) {
        if (request) {
            if (auto status = validate_request_header(&request->header, "get_metadata");
                status != YAMS_MOBILE_STATUS_OK)
                return status;
        }

        if (request && request->document_hash && *request->document_hash) {
            // Single document by hash
            yams::daemon::GetRequest dreq;
            dreq.hash = request->document_hash;
            dreq.metadataOnly = true;

            auto result = daemon_call(ctx->state, std::move(dreq));
            if (!result) {
                set_last_error(result.error().message);
                return map_error_code(result.error().code);
            }

            auto& resp = result.value();
            nlohmann::json root = nlohmann::json::array();
            if (!resp.hash.empty()) {
                nlohmann::json entry;
                entry["id"] = 0;
                entry["path"] = resp.path;
                entry["name"] = resp.name;
                entry["hash"] = resp.hash;
                entry["size"] = resp.size;
                entry["mime"] = resp.mimeType;
                if (!resp.metadata.empty())
                    entry["metadata"] = resp.metadata;
                root.push_back(std::move(entry));
            }

            if (out_result) {
                auto res = std::make_unique<yams_mobile_metadata_result_t>();
                res->json = root.dump();
                *out_result = res.release();
            }
            set_last_error("");
            return YAMS_MOBILE_STATUS_OK;
        } else {
            // Pattern-based listing
            yams::daemon::ListRequest dreq;
            dreq.showMetadata = true;
            dreq.showTags = true;
            dreq.limit = 1000;
            if (request && request->path && *request->path)
                dreq.namePattern = request->path;

            auto result = daemon_call(ctx->state, std::move(dreq));
            if (!result) {
                set_last_error(result.error().message);
                return map_error_code(result.error().code);
            }

            auto& resp = result.value();
            nlohmann::json root = nlohmann::json::array();
            for (const auto& item : resp.items) {
                nlohmann::json entry;
                entry["id"] = 0;
                entry["path"] = item.path;
                entry["name"] = item.name;
                entry["hash"] = item.hash;
                entry["size"] = item.size;
                entry["mime"] = item.mimeType;
                if (!item.tags.empty())
                    entry["tags"] = item.tags;
                if (!item.metadata.empty())
                    entry["metadata"] = item.metadata;
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
    }

    // Embedded mode
    auto repo = ctx->state.metadata_repo;
    if (!repo) {
        set_last_error("metadata repository not initialized");
        return YAMS_MOBILE_STATUS_NOT_INITIALIZED;
    }
    if (request) {
        if (auto status = validate_request_header(&request->header, "get_metadata");
            status != YAMS_MOBILE_STATUS_OK)
            return status;
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
        auto list = queryDocumentsByPattern(*repo, pattern);
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

YAMS_MOBILE_API void yams_mobile_metadata_result_destroy(yams_mobile_metadata_result_t* result) {
    delete result;
}

YAMS_MOBILE_API yams_mobile_status yams_mobile_get_vector_status(
    yams_mobile_context_t* ctx, const yams_mobile_vector_status_request* request,
    yams_mobile_vector_status_result_t** out_result) {
    if (out_result)
        *out_result = nullptr;
    if (!ctx) {
        set_last_error("context is null");
        return YAMS_MOBILE_STATUS_INVALID_ARGUMENT;
    }

    // Daemon mode — use GetStatsRequest
    if (ctx->state.daemon_client) {
        if (request) {
            if (auto status = validate_request_header(&request->header, "get_vector_status");
                status != YAMS_MOBILE_STATUS_OK)
                return status;
        }

        yams::daemon::GetStatsRequest dreq;
        auto result = daemon_call(ctx->state, std::move(dreq));
        if (!result) {
            set_last_error(result.error().message);
            return map_error_code(result.error().code);
        }

        auto& resp = result.value();
        nlohmann::json j;
        j["documents"] = resp.totalDocuments;
        j["storageBytes"] = resp.totalSize;
        j["dedupBytes"] = static_cast<std::uint64_t>(
            resp.compressionRatio > 0.0 ? resp.totalSize * resp.compressionRatio : 0);
        j["warmupRequested"] = (request && request->warmup != 0);

        if (out_result) {
            auto res = std::make_unique<yams_mobile_vector_status_result_t>();
            res->json = j.dump();
            *out_result = res.release();
        }
        set_last_error("");
        return YAMS_MOBILE_STATUS_OK;
    }

    // Embedded mode
    auto repo = ctx->state.metadata_repo;
    auto store = ctx->state.content_store;
    if (!repo || !store) {
        set_last_error("context not initialized");
        return YAMS_MOBILE_STATUS_NOT_INITIALIZED;
    }
    if (request) {
        if (auto status = validate_request_header(&request->header, "get_vector_status");
            status != YAMS_MOBILE_STATUS_OK)
            return status;
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

YAMS_MOBILE_API void
yams_mobile_vector_status_result_destroy(yams_mobile_vector_status_result_t* result) {
    delete result;
}

YAMS_MOBILE_API yams_mobile_status
yams_mobile_list_documents(yams_mobile_context_t* ctx, const yams_mobile_list_request* request,
                           yams_mobile_list_result_t** out_result) {
    if (out_result)
        *out_result = nullptr;
    if (!ctx) {
        set_last_error("context is null");
        return YAMS_MOBILE_STATUS_INVALID_ARGUMENT;
    }

    // Daemon mode
    if (ctx->state.daemon_client) {
        if (request) {
            if (auto status = validate_request_header(&request->header, "list_documents");
                status != YAMS_MOBILE_STATUS_OK)
                return status;
        }

        yams::daemon::ListRequest dreq;
        dreq.limit = 100;
        dreq.offset = 0;
        if (request) {
            if (request->pattern && *request->pattern)
                dreq.namePattern = request->pattern;
            if (request->limit > 0)
                dreq.limit = request->limit;
            if (request->offset > 0)
                dreq.offset = static_cast<int>(request->offset);
            dreq.pathsOnly = request->paths_only != 0;
            dreq.matchAllTags = request->match_all_tags != 0;
            if (request->tags && request->tag_count > 0) {
                for (size_t i = 0; i < request->tag_count; ++i) {
                    if (request->tags[i])
                        dreq.tags.emplace_back(request->tags[i]);
                }
            }
        }

        auto result = daemon_call(ctx->state, std::move(dreq));
        if (!result) {
            set_last_error(result.error().message);
            return map_error_code(result.error().code);
        }

        auto& resp = result.value();
        nlohmann::json root;
        root["count"] = resp.items.size();
        root["totalFound"] = resp.totalCount;
        root["hasMore"] = resp.items.size() < resp.totalCount;

        nlohmann::json docs = nlohmann::json::array();
        for (const auto& item : resp.items) {
            nlohmann::json entry;
            entry["name"] = item.name;
            entry["fileName"] = item.fileName;
            entry["hash"] = item.hash;
            entry["path"] = item.path;
            entry["size"] = item.size;
            entry["mimeType"] = item.mimeType;
            entry["fileType"] = item.fileType;
            entry["extension"] = item.extension;
            entry["created"] = item.created;
            entry["modified"] = item.modified;
            entry["indexed"] = item.indexed;
            if (!item.tags.empty())
                entry["tags"] = item.tags;
            if (!item.metadata.empty())
                entry["metadata"] = item.metadata;
            if (!item.snippet.empty())
                entry["snippet"] = item.snippet;
            if (!item.changeType.empty())
                entry["changeType"] = item.changeType;
            if (!item.matchReason.empty())
                entry["matchReason"] = item.matchReason;
            if (item.relevanceScore != 0.0)
                entry["relevanceScore"] = item.relevanceScore;
            docs.push_back(std::move(entry));
        }
        root["documents"] = std::move(docs);

        auto wrapper = std::make_unique<yams_mobile_list_result_t>();
        wrapper->json = root.dump();
        if (out_result)
            *out_result = wrapper.release();

        nlohmann::json telemetry;
        telemetry["event"] = "list";
        telemetry["timestamp"] = iso_timestamp_now();
        telemetry["count"] = resp.items.size();
        telemetry["totalFound"] = resp.totalCount;
        emit_telemetry(ctx->state, telemetry);

        set_last_error("");
        return YAMS_MOBILE_STATUS_OK;
    }

    // Embedded mode
    auto service = ctx->state.document_service;
    if (!service) {
        set_last_error("document service not initialized");
        return YAMS_MOBILE_STATUS_NOT_INITIALIZED;
    }

    ListDocumentsRequest req;
    req.limit = 100;
    req.offset = 0;
    req.pathsOnly = false;
    req.matchAllTags = false;

    if (request) {
        if (auto status = validate_request_header(&request->header, "list_documents");
            status != YAMS_MOBILE_STATUS_OK)
            return status;
        if (request->pattern && *request->pattern)
            req.pattern = request->pattern;
        if (request->limit > 0)
            req.limit = static_cast<int>(request->limit);
        if (request->offset > 0)
            req.offset = static_cast<int>(request->offset);
        req.pathsOnly = request->paths_only != 0;
        req.matchAllTags = request->match_all_tags != 0;
        if (request->tags && request->tag_count > 0) {
            req.tags.reserve(request->tag_count);
            for (size_t i = 0; i < request->tag_count; ++i) {
                if (request->tags[i])
                    req.tags.emplace_back(request->tags[i]);
            }
        }
    }

    auto result = service->list(req);
    if (!result) {
        set_last_error(result.error().message);
        return map_error_code(result.error().code);
    }

    auto wrapper = std::make_unique<yams_mobile_list_result_t>();
    wrapper->json = list_response_to_json(result.value()).dump();

    if (out_result)
        *out_result = wrapper.release();

    nlohmann::json telemetry;
    telemetry["event"] = "list";
    telemetry["timestamp"] = iso_timestamp_now();
    telemetry["count"] = static_cast<std::uint64_t>(result.value().count);
    telemetry["totalFound"] = static_cast<std::uint64_t>(result.value().totalFound);
    emit_telemetry(ctx->state, telemetry);

    set_last_error("");
    return YAMS_MOBILE_STATUS_OK;
}

YAMS_MOBILE_API void yams_mobile_list_result_destroy(yams_mobile_list_result_t* result) {
    delete result;
}

YAMS_MOBILE_API yams_mobile_status yams_mobile_get_document(
    yams_mobile_context_t* ctx, const yams_mobile_document_get_request* request,
    yams_mobile_document_get_result_t** out_result) {
    if (out_result)
        *out_result = nullptr;
    if (!ctx) {
        set_last_error("context is null");
        return YAMS_MOBILE_STATUS_INVALID_ARGUMENT;
    }

    // Daemon mode
    if (ctx->state.daemon_client) {
        if (request) {
            if (auto status = validate_request_header(&request->header, "get_document");
                status != YAMS_MOBILE_STATUS_OK)
                return status;
        }

        yams::daemon::GetRequest dreq;
        if (request) {
            if (request->document_hash && *request->document_hash)
                dreq.hash = request->document_hash;
            else if (request->name && *request->name) {
                dreq.name = request->name;
                dreq.byName = true;
            }
            dreq.metadataOnly = request->metadata_only != 0;
            dreq.raw = request->raw != 0;
            dreq.extract = request->include_extracted_text != 0 && !dreq.raw;
            dreq.latest = request->latest != 0;
            dreq.oldest = request->oldest != 0;
            if (request->max_bytes > 0)
                dreq.maxBytes = request->max_bytes;
            if (dreq.metadataOnly)
                dreq.raw = false;
        }

        auto result = daemon_call(ctx->state, std::move(dreq));
        if (!result) {
            set_last_error(result.error().message);
            return map_error_code(result.error().code);
        }

        auto& resp = result.value();
        nlohmann::json root;
        root["totalFound"] = 1;
        root["hasMore"] = false;
        root["graphEnabled"] = resp.graphEnabled;
        root["totalBytes"] = resp.totalBytes;
        if (resp.outputWritten)
            root["outputWritten"] = true;

        nlohmann::json doc;
        doc["hash"] = resp.hash;
        doc["path"] = resp.path;
        doc["name"] = resp.name;
        doc["fileName"] = resp.fileName;
        doc["mimeType"] = resp.mimeType;
        doc["fileType"] = resp.fileType;
        doc["size"] = resp.size;
        doc["created"] = resp.created;
        doc["modified"] = resp.modified;
        doc["indexed"] = resp.indexed;
        if (!resp.metadata.empty())
            doc["metadata"] = resp.metadata;
        root["document"] = std::move(doc);

        if (!resp.related.empty()) {
            nlohmann::json relArr = nlohmann::json::array();
            for (const auto& rel : resp.related) {
                nlohmann::json r;
                r["hash"] = rel.hash;
                r["path"] = rel.path;
                r["name"] = rel.name;
                r["distance"] = rel.distance;
                r["relevanceScore"] = rel.relevanceScore;
                relArr.push_back(std::move(r));
            }
            root["related"] = std::move(relArr);
        }

        auto wrapper = std::make_unique<yams_mobile_document_get_result_t>();
        wrapper->json = root.dump();
        if (resp.hasContent && !resp.content.empty())
            wrapper->content = resp.content;
        if (out_result)
            *out_result = wrapper.release();

        nlohmann::json telemetry;
        telemetry["event"] = "get";
        telemetry["timestamp"] = iso_timestamp_now();
        telemetry["hasDocument"] = !resp.hash.empty();
        emit_telemetry(ctx->state, telemetry);

        set_last_error("");
        return YAMS_MOBILE_STATUS_OK;
    }

    // Embedded mode
    auto service = ctx->state.document_service;
    if (!service) {
        set_last_error("document service not initialized");
        return YAMS_MOBILE_STATUS_NOT_INITIALIZED;
    }

    RetrieveDocumentRequest req;
    req.includeContent = false;
    req.raw = false;
    req.extract = true;
    req.metadataOnly = false;

    if (request) {
        if (auto status = validate_request_header(&request->header, "get_document");
            status != YAMS_MOBILE_STATUS_OK)
            return status;
        if (request->document_hash && *request->document_hash)
            req.hash = request->document_hash;
        else if (request->name && *request->name)
            req.name = request->name;

        req.metadataOnly = request->metadata_only != 0;
        req.includeContent = request->include_content != 0 && !req.metadataOnly;
        req.raw = request->raw != 0;
        req.extract = request->include_extracted_text != 0 && !req.raw;
        req.latest = request->latest != 0;
        req.oldest = request->oldest != 0;
        if (request->max_bytes > 0)
            req.maxBytes = request->max_bytes;
        if (req.metadataOnly)
            req.includeContent = false;
    }

    auto result = service->retrieve(req);
    if (!result) {
        set_last_error(result.error().message);
        return map_error_code(result.error().code);
    }

    auto response = std::move(result.value());
    auto wrapper = std::make_unique<yams_mobile_document_get_result_t>();

    const bool includeExtractedText = request ? (request->include_extracted_text != 0) : true;
    wrapper->json = retrieve_response_to_json(response, includeExtractedText).dump();

    if (response.document && req.includeContent && response.document->content) {
        wrapper->content = *response.document->content;
    }

    if (out_result)
        *out_result = wrapper.release();

    nlohmann::json telemetry;
    telemetry["event"] = "get";
    telemetry["timestamp"] = iso_timestamp_now();
    telemetry["hasDocument"] = response.document.has_value();
    telemetry["documents"] = static_cast<std::uint64_t>(response.documents.size());
    emit_telemetry(ctx->state, telemetry);

    set_last_error("");
    return YAMS_MOBILE_STATUS_OK;
}

YAMS_MOBILE_API void
yams_mobile_document_get_result_destroy(yams_mobile_document_get_result_t* result) {
    delete result;
}

YAMS_MOBILE_API yams_mobile_string_view
yams_mobile_metadata_result_json(const yams_mobile_metadata_result_t* result) {
    if (!result)
        return {nullptr, 0};
    return {result->json.c_str(), result->json.size()};
}

YAMS_MOBILE_API yams_mobile_string_view
yams_mobile_vector_status_result_json(const yams_mobile_vector_status_result_t* result) {
    if (!result)
        return {nullptr, 0};
    return {result->json.c_str(), result->json.size()};
}

YAMS_MOBILE_API yams_mobile_string_view
yams_mobile_grep_result_stats_json(const yams_mobile_grep_result_t* result) {
    if (!result)
        return {nullptr, 0};
    if (result->pre_serialized) {
        return {result->stats_json.c_str(), result->stats_json.size()};
    }
    nlohmann::json j;
    j["executionTimeMs"] = result->response.executionTimeMs;
    j["totalMatches"] = result->response.totalMatches;
    j["filesSearched"] = result->response.filesSearched;
    j["stats"] = result->response.searchStats;
    g_temp_string = j.dump();
    return {g_temp_string.c_str(), g_temp_string.size()};
}

YAMS_MOBILE_API yams_mobile_string_view
yams_mobile_search_result_stats_json(const yams_mobile_search_result_t* result) {
    if (!result)
        return {nullptr, 0};
    if (result->pre_serialized) {
        return {result->stats_json.c_str(), result->stats_json.size()};
    }
    nlohmann::json j;
    j["executionTimeMs"] = result->response.executionTimeMs;
    j["total"] = result->response.total;
    j["type"] = result->response.type;
    j["usedHybrid"] = result->response.usedHybrid;
    j["stats"] = result->response.searchStats;
    g_temp_string = j.dump();
    return {g_temp_string.c_str(), g_temp_string.size()};
}

YAMS_MOBILE_API yams_mobile_string_view
yams_mobile_search_result_json(const yams_mobile_search_result_t* result) {
    if (!result)
        return {nullptr, 0};
    if (result->pre_serialized) {
        return {result->json.c_str(), result->json.size()};
    }

    nlohmann::json root;
    root["total"] = result->response.total;
    root["type"] = result->response.type;
    root["executionTimeMs"] = result->response.executionTimeMs;
    root["usedHybrid"] = result->response.usedHybrid;
    if (!result->response.detectedHashQuery.empty())
        root["detectedHashQuery"] = result->response.detectedHashQuery;
    if (!result->response.appliedFormat.empty())
        root["appliedFormat"] = result->response.appliedFormat;
    if (!result->response.queryInfo.empty())
        root["queryInfo"] = result->response.queryInfo;

    nlohmann::json arr = nlohmann::json::array();
    for (const auto& item : result->response.results) {
        nlohmann::json entry;
        entry["id"] = item.id;
        entry["hash"] = item.hash;
        entry["title"] = item.title;
        entry["path"] = item.path;
        entry["fileName"] = item.fileName;
        entry["score"] = item.score;
        if (!item.snippet.empty())
            entry["snippet"] = item.snippet;
        entry["mimeType"] = item.mimeType;
        entry["fileType"] = item.fileType;
        entry["size"] = item.size;
        entry["created"] = item.created;
        entry["modified"] = item.modified;
        entry["indexed"] = item.indexed;
        if (!item.tags.empty())
            entry["tags"] = item.tags;
        if (!item.metadata.empty())
            entry["metadata"] = item.metadata;
        if (item.vectorScore)
            entry["vectorScore"] = *item.vectorScore;
        if (item.keywordScore)
            entry["keywordScore"] = *item.keywordScore;
        if (item.kgEntityScore)
            entry["kgEntityScore"] = *item.kgEntityScore;
        if (item.structuralScore)
            entry["structuralScore"] = *item.structuralScore;
        if (item.matchReason)
            entry["matchReason"] = *item.matchReason;
        if (item.searchMethod)
            entry["searchMethod"] = *item.searchMethod;

        if (!item.matches.empty()) {
            nlohmann::json matches = nlohmann::json::array();
            for (const auto& m : item.matches) {
                nlohmann::json mJson;
                mJson["lineNumber"] = static_cast<std::uint64_t>(m.lineNumber);
                mJson["line"] = m.line;
                mJson["columnStart"] = static_cast<std::uint64_t>(m.columnStart);
                mJson["columnEnd"] = static_cast<std::uint64_t>(m.columnEnd);
                if (!m.beforeLines.empty())
                    mJson["beforeLines"] = m.beforeLines;
                if (!m.afterLines.empty())
                    mJson["afterLines"] = m.afterLines;
                matches.push_back(std::move(mJson));
            }
            entry["matches"] = std::move(matches);
        }

        arr.push_back(std::move(entry));
    }
    root["results"] = std::move(arr);

    if (!result->response.paths.empty())
        root["paths"] = result->response.paths;
    if (!result->response.searchStats.empty())
        root["stats"] = result->response.searchStats;

    g_temp_string = root.dump();
    return {g_temp_string.c_str(), g_temp_string.size()};
}

YAMS_MOBILE_API yams_mobile_string_view
yams_mobile_grep_result_json(const yams_mobile_grep_result_t* result) {
    if (!result)
        return {nullptr, 0};
    if (result->pre_serialized) {
        return {result->json.c_str(), result->json.size()};
    }

    nlohmann::json root;
    root["totalMatches"] = static_cast<std::uint64_t>(result->response.totalMatches);
    root["filesSearched"] = static_cast<std::uint64_t>(result->response.filesSearched);
    root["regexMatches"] = static_cast<std::uint64_t>(result->response.regexMatches);
    root["semanticMatches"] = static_cast<std::uint64_t>(result->response.semanticMatches);
    root["executionTimeMs"] = result->response.executionTimeMs;
    if (!result->response.queryInfo.empty())
        root["queryInfo"] = result->response.queryInfo;
    if (!result->response.searchStats.empty())
        root["stats"] = result->response.searchStats;

    nlohmann::json files = nlohmann::json::array();
    for (const auto& fr : result->response.results) {
        nlohmann::json entry;
        entry["file"] = fr.file;
        entry["fileName"] = fr.fileName;
        entry["matchCount"] = static_cast<std::uint64_t>(fr.matchCount);
        entry["mimeType"] = fr.mimeType;
        entry["fileType"] = fr.fileType;
        entry["size"] = fr.size;
        if (!fr.tags.empty())
            entry["tags"] = fr.tags;
        if (!fr.metadata.empty())
            entry["metadata"] = fr.metadata;
        if (!fr.searchMethod.empty())
            entry["searchMethod"] = fr.searchMethod;
        entry["wasSemanticSearch"] = fr.wasSemanticSearch;

        if (!fr.matches.empty()) {
            nlohmann::json matches = nlohmann::json::array();
            for (const auto& match : fr.matches) {
                nlohmann::json mJson;
                mJson["lineNumber"] = static_cast<std::uint64_t>(match.lineNumber);
                mJson["line"] = match.line;
                mJson["columnStart"] = static_cast<std::uint64_t>(match.columnStart);
                mJson["columnEnd"] = static_cast<std::uint64_t>(match.columnEnd);
                if (!match.before.empty())
                    mJson["before"] = match.before;
                if (!match.after.empty())
                    mJson["after"] = match.after;
                if (!match.matchText.empty())
                    mJson["match"] = match.matchText;
                if (!match.matchType.empty())
                    mJson["matchType"] = match.matchType;
                mJson["confidence"] = match.confidence;
                matches.push_back(std::move(mJson));
            }
            entry["matches"] = std::move(matches);
        }

        files.push_back(std::move(entry));
    }
    root["files"] = std::move(files);

    if (!result->response.filesWith.empty())
        root["filesWith"] = result->response.filesWith;
    if (!result->response.filesWithout.empty())
        root["filesWithout"] = result->response.filesWithout;
    if (!result->response.pathsOnly.empty())
        root["pathsOnly"] = result->response.pathsOnly;
    root["format"] = result->response.format;

    g_temp_string = root.dump();
    return {g_temp_string.c_str(), g_temp_string.size()};
}

YAMS_MOBILE_API yams_mobile_string_view
yams_mobile_list_result_json(const yams_mobile_list_result_t* result) {
    if (!result)
        return {nullptr, 0};
    return {result->json.c_str(), result->json.size()};
}

YAMS_MOBILE_API yams_mobile_string_view
yams_mobile_document_get_result_json(const yams_mobile_document_get_result_t* result) {
    if (!result)
        return {nullptr, 0};
    return {result->json.c_str(), result->json.size()};
}

YAMS_MOBILE_API yams_mobile_string_view
yams_mobile_document_get_result_content(const yams_mobile_document_get_result_t* result) {
    if (!result || result->content.empty())
        return {nullptr, 0};
    return {result->content.c_str(), result->content.size()};
}

YAMS_MOBILE_API const char* yams_mobile_last_error_message(void) {
    return g_last_error.c_str();
}
