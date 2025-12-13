#include <yams/api/mobile_bindings.h>

#include <yams/api/content_store_builder.h>
#include <yams/app/services/factory.hpp>
#include <yams/app/services/services.hpp>
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
using yams::app::services::DocumentEntry;
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
    uint32_t max_worker_threads = 0;
    uint32_t flags = 0;
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
    TelemetrySinkType telemetry_sink = TelemetrySinkType::None;
    std::ofstream telemetry_stream;
    std::mutex telemetry_mutex;
};

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

struct yams_mobile_list_result_t {
    std::string json;
};

struct yams_mobile_document_get_result_t {
    std::string json;
    std::string content;
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
        ctx->state.config.api_version = config->version != 0U ? config->version : kPackedVersion;

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
    if (auto status = validate_request_header(&request->header, "grep");
        status != YAMS_MOBILE_STATUS_OK)
        return status;
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
    if (auto status = validate_request_header(&request->header, "search");
        status != YAMS_MOBILE_STATUS_OK)
        return status;
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
    if (auto status = validate_request_header(&request->header, "store_document");
        status != YAMS_MOBILE_STATUS_OK)
        return status;
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

void yams_mobile_vector_status_result_destroy(yams_mobile_vector_status_result_t* result) {
    delete result;
}

yams_mobile_status yams_mobile_list_documents(yams_mobile_context_t* ctx,
                                              const yams_mobile_list_request* request,
                                              yams_mobile_list_result_t** out_result) {
    if (out_result)
        *out_result = nullptr;
    if (!ctx) {
        set_last_error("context is null");
        return YAMS_MOBILE_STATUS_INVALID_ARGUMENT;
    }
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

void yams_mobile_list_result_destroy(yams_mobile_list_result_t* result) {
    delete result;
}

yams_mobile_status yams_mobile_get_document(yams_mobile_context_t* ctx,
                                            const yams_mobile_document_get_request* request,
                                            yams_mobile_document_get_result_t** out_result) {
    if (out_result)
        *out_result = nullptr;
    if (!ctx) {
        set_last_error("context is null");
        return YAMS_MOBILE_STATUS_INVALID_ARGUMENT;
    }
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

void yams_mobile_document_get_result_destroy(yams_mobile_document_get_result_t* result) {
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

yams_mobile_string_view yams_mobile_search_result_json(const yams_mobile_search_result_t* result) {
    if (!result)
        return {nullptr, 0};

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

yams_mobile_string_view yams_mobile_grep_result_json(const yams_mobile_grep_result_t* result) {
    if (!result)
        return {nullptr, 0};

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

yams_mobile_string_view yams_mobile_list_result_json(const yams_mobile_list_result_t* result) {
    if (!result)
        return {nullptr, 0};
    return {result->json.c_str(), result->json.size()};
}

yams_mobile_string_view
yams_mobile_document_get_result_json(const yams_mobile_document_get_result_t* result) {
    if (!result)
        return {nullptr, 0};
    return {result->json.c_str(), result->json.size()};
}

yams_mobile_string_view
yams_mobile_document_get_result_content(const yams_mobile_document_get_result_t* result) {
    if (!result || result->content.empty())
        return {nullptr, 0};
    return {result->content.c_str(), result->content.size()};
}

const char* yams_mobile_last_error_message(void) {
    return g_last_error.c_str();
}
