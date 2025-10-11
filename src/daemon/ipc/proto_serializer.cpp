// Templated, trait-driven serializer to minimize if/else and ease extension
#include <yams/daemon/ipc/proto_serializer.h>

#include <yams/common/utf8_utils.h>
#include <yams/core/types.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/proto/ipc_envelope.pb.h>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <chrono>
#include <concepts>
#include <cstdint>
#include <map>
#include <string>
#include <type_traits>
#include <variant>
#include <vector>
#include <google/protobuf/repeated_field.h>

namespace yams {
namespace daemon {

using Envelope = yams::daemon::ipc::Envelope;
namespace pb = yams::daemon::ipc;

// (bindings inserted later, after ProtoBinding primary template)

// Map helpers for common structures
static void to_kv_pairs(const std::map<std::string, std::string>& in,
                        google::protobuf::RepeatedPtrField<pb::KvPair>* out) {
    out->Clear();
    for (const auto& [k, v] : in) {
        auto* kv = out->Add();
        kv->set_key(yams::common::sanitizeUtf8(k));
        kv->set_value(yams::common::sanitizeUtf8(v));
    }
}

static std::map<std::string, std::string>
from_kv_pairs(const google::protobuf::RepeatedPtrField<pb::KvPair>& in) {
    std::map<std::string, std::string> out;
    for (const auto& kv : in)
        out[kv.key()] = kv.value();
    return out;
}

// Repeated string helpers (common in many messages)
static void set_string_list(const std::vector<std::string>& in,
                            google::protobuf::RepeatedPtrField<std::string>* out) {
    out->Clear();
    for (const auto& s : in)
        out->Add(std::string{s});
}

static std::vector<std::string>
get_string_list(const google::protobuf::RepeatedPtrField<std::string>& in) {
    std::vector<std::string> out;
    out.reserve(in.size());
    for (const auto& s : in)
        out.emplace_back(s);
    return out;
}

// Primary template for per-type protobuf bindings. Specialize for each supported type.
template <typename T, typename = void> struct ProtoBinding; // no default implementation

// Concept to detect if a binding is available for T (C++20)
template <typename T>
concept HasProtoBinding = requires(Envelope& e, const Envelope& ce, const T& t) {
    { ProtoBinding<T>::set(e, t) } -> std::same_as<void>;
    { ProtoBinding<T>::case_v } -> std::convertible_to<Envelope::PayloadCase>;
    { ProtoBinding<T>::get(ce) } -> std::same_as<T>;
};

// Core request/response bindings always present
template <> struct ProtoBinding<PingRequest> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kPingRequest;
    static void set(Envelope& env, const PingRequest& pr) {
        auto* ping = env.mutable_ping_request();
        ping->set_timestamp_ns(static_cast<uint64_t>(pr.timestamp.time_since_epoch().count()));
    }
    static PingRequest get(const Envelope& env) {
        PingRequest pr{};
        pr.timestamp = std::chrono::steady_clock::time_point(
            std::chrono::steady_clock::duration(env.ping_request().timestamp_ns()));
        return pr;
    }
};

template <> struct ProtoBinding<PongResponse> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kPongResponse;
    static void set(Envelope& env, const PongResponse& pr) {
        auto* pong = env.mutable_pong_response();
        pong->set_server_time_ns(static_cast<uint64_t>(pr.serverTime.time_since_epoch().count()));
        pong->set_round_trip_ms(static_cast<int64_t>(pr.roundTrip.count()));
    }
    static PongResponse get(const Envelope& env) {
        PongResponse pr{};
        pr.serverTime = std::chrono::steady_clock::time_point(
            std::chrono::steady_clock::duration(env.pong_response().server_time_ns()));
        pr.roundTrip = std::chrono::milliseconds(env.pong_response().round_trip_ms());
        return pr;
    }
};

template <> struct ProtoBinding<ErrorResponse> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kError;
    static void set(Envelope& env, const ErrorResponse& er) {
        auto* pe = env.mutable_error();
        pe->set_code(static_cast<uint32_t>(er.code));
        pe->set_message(er.message);
    }
    static ErrorResponse get(const Envelope& env) {
        ErrorResponse er{};
        er.code = static_cast<ErrorCode>(env.error().code());
        er.message = env.error().message();
        return er;
    }
};

// Minimal mapping for GetResponse (proto schema is simplified vs C++)
template <> struct ProtoBinding<GetResponse> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kGetResponse;
    static void set(Envelope& env, const GetResponse& r) {
        auto* o = env.mutable_get_response();
        o->set_hash(r.hash);
        o->set_name(r.name);
        o->set_path(r.path);
        o->set_compressed(r.compressed);
        if (r.compressionAlgorithm)
            o->set_compression_algorithm(*r.compressionAlgorithm);
        if (r.compressionLevel)
            o->set_compression_level(*r.compressionLevel);
        if (r.uncompressedSize)
            o->set_uncompressed_size(*r.uncompressedSize);
        if (r.compressedCrc32)
            o->set_compressed_crc32(*r.compressedCrc32);
        if (r.uncompressedCrc32)
            o->set_uncompressed_crc32(*r.uncompressedCrc32);
        if (!r.compressionHeader.empty())
            o->set_compression_header(r.compressionHeader.data(),
                                      static_cast<int>(r.compressionHeader.size()));
        if (r.centroidWeight)
            o->set_centroid_weight(*r.centroidWeight);
        if (r.centroidDims)
            o->set_centroid_dims(*r.centroidDims);
        if (!r.centroidPreview.empty()) {
            o->clear_centroid_preview();
            for (float v : r.centroidPreview)
                o->add_centroid_preview(v);
        }
        o->set_has_content(r.hasContent);
        if (r.hasContent)
            o->set_content(r.content);
        to_kv_pairs(r.metadata, o->mutable_metadata());
    }
    static GetResponse get(const Envelope& env) {
        const auto& i = env.get_response();
        GetResponse r{};
        r.hash = i.hash();
        r.name = i.name();
        r.path = i.path();
        r.compressed = i.compressed();
        if (i.has_compression_algorithm())
            r.compressionAlgorithm = static_cast<uint8_t>(i.compression_algorithm());
        if (i.has_compression_level())
            r.compressionLevel = static_cast<uint8_t>(i.compression_level());
        if (i.has_uncompressed_size())
            r.uncompressedSize = i.uncompressed_size();
        if (i.has_compressed_crc32())
            r.compressedCrc32 = i.compressed_crc32();
        if (i.has_uncompressed_crc32())
            r.uncompressedCrc32 = i.uncompressed_crc32();
        if (!i.compression_header().empty())
            r.compressionHeader.assign(i.compression_header().begin(),
                                       i.compression_header().end());
        if (i.has_centroid_weight())
            r.centroidWeight = i.centroid_weight();
        if (i.has_centroid_dims())
            r.centroidDims = i.centroid_dims();
        if (!i.centroid_preview().empty()) {
            r.centroidPreview.assign(i.centroid_preview().begin(), i.centroid_preview().end());
        }
        r.content = i.content();
        r.hasContent = i.has_content(); // Use explicit protobuf field
        r.metadata = from_kv_pairs(i.metadata());
        return r;
    }
};

// --------------------------- Requests ---------------------------
template <> struct ProtoBinding<SearchRequest> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kSearchRequest;
    static void set(Envelope& env, const SearchRequest& r) {
        auto* o = env.mutable_search_request();
        o->set_query(r.query);
        o->set_limit(static_cast<uint32_t>(r.limit));
        o->set_fuzzy(r.fuzzy);
        o->set_literal_text(r.literalText);
        o->set_similarity(r.similarity);
        o->set_timeout_ms(static_cast<int64_t>(r.timeout.count()));
        o->set_search_type(r.searchType);
        o->set_paths_only(r.pathsOnly);
        o->set_show_hash(r.showHash);
        o->set_verbose(r.verbose);
        o->set_json_output(r.jsonOutput);
        o->set_show_line_numbers(r.showLineNumbers);
        o->set_after_context(r.afterContext);
        o->set_before_context(r.beforeContext);
        o->set_context(r.context);
        o->set_hash_query(r.hashQuery);
        o->set_path_pattern(r.pathPattern);
        set_string_list(r.pathPatterns, o->mutable_path_patterns());
        set_string_list(r.tags, o->mutable_tags());
        o->set_match_all_tags(r.matchAllTags);
        o->set_extension(r.extension);
        o->set_mime_type(r.mimeType);
        o->set_file_type(r.fileType);
        o->set_text_only(r.textOnly);
        o->set_binary_only(r.binaryOnly);
        o->set_created_after(r.createdAfter);
        o->set_created_before(r.createdBefore);
        o->set_modified_after(r.modifiedAfter);
        o->set_modified_before(r.modifiedBefore);
        o->set_indexed_after(r.indexedAfter);
        o->set_indexed_before(r.indexedBefore);
        o->set_vector_stage_timeout_ms(r.vectorStageTimeoutMs);
        o->set_keyword_stage_timeout_ms(r.keywordStageTimeoutMs);
        o->set_snippet_hydration_timeout_ms(r.snippetHydrationTimeoutMs);
    }
    static SearchRequest get(const Envelope& env) {
        const auto& i = env.search_request();
        SearchRequest r{};
        r.query = i.query();
        r.limit = i.limit();
        r.fuzzy = i.fuzzy();
        r.literalText = i.literal_text();
        r.similarity = i.similarity();
        r.timeout = std::chrono::milliseconds{i.timeout_ms()};
        r.searchType = i.search_type();
        r.pathsOnly = i.paths_only();
        r.showHash = i.show_hash();
        r.verbose = i.verbose();
        r.jsonOutput = i.json_output();
        r.showLineNumbers = i.show_line_numbers();
        r.afterContext = i.after_context();
        r.beforeContext = i.before_context();
        r.context = i.context();
        r.hashQuery = i.hash_query();
        r.pathPattern = i.path_pattern();
        r.pathPatterns = get_string_list(i.path_patterns());
        r.tags = get_string_list(i.tags());
        r.matchAllTags = i.match_all_tags();
        r.extension = i.extension();
        r.mimeType = i.mime_type();
        r.fileType = i.file_type();
        r.textOnly = i.text_only();
        r.binaryOnly = i.binary_only();
        r.createdAfter = i.created_after();
        r.createdBefore = i.created_before();
        r.modifiedAfter = i.modified_after();
        r.modifiedBefore = i.modified_before();
        r.indexedAfter = i.indexed_after();
        r.indexedBefore = i.indexed_before();
        r.vectorStageTimeoutMs = i.vector_stage_timeout_ms();
        r.keywordStageTimeoutMs = i.keyword_stage_timeout_ms();
        r.snippetHydrationTimeoutMs = i.snippet_hydration_timeout_ms();
        return r;
    }
};

template <> struct ProtoBinding<GetRequest> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kGetRequest;
    static void set(Envelope& env, const GetRequest& r) {
        auto* o = env.mutable_get_request();
        o->set_hash(r.hash);
        o->set_accept_compressed(r.acceptCompressed);
        o->set_name(r.name);
        o->set_by_name(r.byName);
        o->set_file_type(r.fileType);
        o->set_mime_type(r.mimeType);
        o->set_extension(r.extension);
        o->set_binary_only(r.binaryOnly);
        o->set_text_only(r.textOnly);
        o->set_created_after(r.createdAfter);
        o->set_created_before(r.createdBefore);
        o->set_modified_after(r.modifiedAfter);
        o->set_modified_before(r.modifiedBefore);
        o->set_indexed_after(r.indexedAfter);
        o->set_indexed_before(r.indexedBefore);
        o->set_latest(r.latest);
        o->set_oldest(r.oldest);
        o->set_output_path(r.outputPath);
        o->set_metadata_only(r.metadataOnly);
        o->set_max_bytes(r.maxBytes);
        o->set_chunk_size(r.chunkSize);
        o->set_raw(r.raw);
        o->set_extract(r.extract);
        o->set_show_graph(r.showGraph);
        o->set_graph_depth(r.graphDepth);
        o->set_verbose(r.verbose);
    }
    static GetRequest get(const Envelope& env) {
        const auto& i = env.get_request();
        GetRequest r{};
        r.hash = i.hash();
        r.name = i.name();
        r.byName = i.by_name();
        r.fileType = i.file_type();
        r.mimeType = i.mime_type();
        r.extension = i.extension();
        r.binaryOnly = i.binary_only();
        r.textOnly = i.text_only();
        r.createdAfter = i.created_after();
        r.createdBefore = i.created_before();
        r.modifiedAfter = i.modified_after();
        r.modifiedBefore = i.modified_before();
        r.indexedAfter = i.indexed_after();
        r.indexedBefore = i.indexed_before();
        r.latest = i.latest();
        r.oldest = i.oldest();
        r.outputPath = i.output_path();
        r.metadataOnly = i.metadata_only();
        r.maxBytes = i.max_bytes();
        r.chunkSize = i.chunk_size();
        r.raw = i.raw();
        r.extract = i.extract();
        r.showGraph = i.show_graph();
        r.graphDepth = i.graph_depth();
        r.verbose = i.verbose();
        r.acceptCompressed = i.accept_compressed();
        return r;
    }
};

template <> struct ProtoBinding<GetInitRequest> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kGetInitRequest;
    static void set(Envelope& env, const GetInitRequest& r) {
        auto* o = env.mutable_get_init_request();
        o->set_hash(r.hash);
        o->set_name(r.name);
        o->set_by_name(r.byName);
        o->set_metadata_only(r.metadataOnly);
        o->set_max_bytes(r.maxBytes);
        o->set_chunk_size(r.chunkSize);
    }
    static GetInitRequest get(const Envelope& env) {
        const auto& i = env.get_init_request();
        GetInitRequest r{};
        r.hash = i.hash();
        r.name = i.name();
        r.byName = i.by_name();
        r.metadataOnly = i.metadata_only();
        r.maxBytes = i.max_bytes();
        r.chunkSize = i.chunk_size();
        return r;
    }
};

template <> struct ProtoBinding<GetChunkRequest> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kGetChunkRequest;
    static void set(Envelope& env, const GetChunkRequest& r) {
        auto* o = env.mutable_get_chunk_request();
        o->set_transfer_id(r.transferId);
        o->set_offset(r.offset);
        o->set_length(r.length);
    }
    static GetChunkRequest get(const Envelope& env) {
        const auto& i = env.get_chunk_request();
        GetChunkRequest r{};
        r.transferId = i.transfer_id();
        r.offset = i.offset();
        r.length = i.length();
        return r;
    }
};

template <> struct ProtoBinding<GetEndRequest> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kGetEndRequest;
    static void set(Envelope& env, const GetEndRequest& r) {
        auto* o = env.mutable_get_end_request();
        o->set_transfer_id(r.transferId);
    }
    static GetEndRequest get(const Envelope& env) {
        const auto& i = env.get_end_request();
        GetEndRequest r{};
        r.transferId = i.transfer_id();
        return r;
    }
};

template <> struct ProtoBinding<DeleteRequest> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kDeleteRequest;
    static void set(Envelope& env, const DeleteRequest& r) {
        auto* o = env.mutable_delete_request();
        o->set_hash(r.hash);
        o->set_name(r.name);
        set_string_list(r.names, o->mutable_names());
        o->set_pattern(r.pattern);
        o->set_directory(r.directory);
        o->set_purge(r.purge);
        o->set_force(r.force);
        o->set_dry_run(r.dryRun);
        o->set_keep_refs(r.keepRefs);
        o->set_recursive(r.recursive);
        o->set_verbose(r.verbose);
    }
    static DeleteRequest get(const Envelope& env) {
        const auto& i = env.delete_request();
        DeleteRequest r{};
        r.hash = i.hash();
        r.name = i.name();
        r.names = get_string_list(i.names());
        r.pattern = i.pattern();
        r.directory = i.directory();
        r.purge = i.purge();
        r.force = i.force();
        r.dryRun = i.dry_run();
        r.keepRefs = i.keep_refs();
        r.recursive = i.recursive();
        r.verbose = i.verbose();
        return r;
    }
};

template <> struct ProtoBinding<ListRequest> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kListRequest;
    static void set(Envelope& env, const ListRequest& r) {
        auto* o = env.mutable_list_request();
        o->set_limit(static_cast<uint32_t>(r.limit));
        o->set_offset(r.offset);
        o->set_recent_count(r.recentCount);
        o->set_recent(r.recent);
        o->set_format(r.format);
        o->set_sort_by(r.sortBy);
        o->set_reverse(r.reverse);
        o->set_verbose(r.verbose);
        o->set_show_snippets(r.showSnippets);
        o->set_show_metadata(r.showMetadata);
        o->set_show_tags(r.showTags);
        o->set_group_by_session(r.groupBySession);
        o->set_snippet_length(r.snippetLength);
        o->set_no_snippets(r.noSnippets);
        o->set_file_type(r.fileType);
        o->set_mime_type(r.mimeType);
        o->set_extensions(r.extensions);
        o->set_binary_only(r.binaryOnly);
        o->set_text_only(r.textOnly);
        o->set_created_after(r.createdAfter);
        o->set_created_before(r.createdBefore);
        o->set_modified_after(r.modifiedAfter);
        o->set_modified_before(r.modifiedBefore);
        o->set_indexed_after(r.indexedAfter);
        o->set_indexed_before(r.indexedBefore);
        o->set_show_changes(r.showChanges);
        o->set_since_time(r.sinceTime);
        o->set_show_diff_tags(r.showDiffTags);
        o->set_show_deleted(r.showDeleted);
        o->set_change_window(r.changeWindow);
        set_string_list(r.tags, o->mutable_tags());
        o->set_filter_tags(r.filterTags);
        o->set_match_all_tags(r.matchAllTags);
        o->set_name_pattern(r.namePattern);
        o->set_paths_only(r.pathsOnly);
    }
    static ListRequest get(const Envelope& env) {
        const auto& i = env.list_request();
        ListRequest r{};
        r.limit = i.limit();
        r.offset = i.offset();
        r.recentCount = i.recent_count();
        r.recent = i.recent();
        r.format = i.format();
        r.sortBy = i.sort_by();
        r.reverse = i.reverse();
        r.verbose = i.verbose();
        r.showSnippets = i.show_snippets();
        r.showMetadata = i.show_metadata();
        r.showTags = i.show_tags();
        r.groupBySession = i.group_by_session();
        r.snippetLength = i.snippet_length();
        r.noSnippets = i.no_snippets();
        r.fileType = i.file_type();
        r.mimeType = i.mime_type();
        r.extensions = i.extensions();
        r.binaryOnly = i.binary_only();
        r.textOnly = i.text_only();
        r.createdAfter = i.created_after();
        r.createdBefore = i.created_before();
        r.modifiedAfter = i.modified_after();
        r.modifiedBefore = i.modified_before();
        r.indexedAfter = i.indexed_after();
        r.indexedBefore = i.indexed_before();
        r.showChanges = i.show_changes();
        r.sinceTime = i.since_time();
        r.showDiffTags = i.show_diff_tags();
        r.showDeleted = i.show_deleted();
        r.changeWindow = i.change_window();
        r.tags = get_string_list(i.tags());
        r.filterTags = i.filter_tags();
        r.matchAllTags = i.match_all_tags();
        r.namePattern = i.name_pattern();
        r.pathsOnly = i.paths_only();
        return r;
    }
};

template <> struct ProtoBinding<ShutdownRequest> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kShutdownRequest;
    static void set(Envelope& env, const ShutdownRequest& r) {
        auto* o = env.mutable_shutdown_request();
        o->set_graceful(r.graceful);
        o->set_timeout_s(static_cast<int64_t>(r.timeout.count()));
    }
    static ShutdownRequest get(const Envelope& env) {
        const auto& i = env.shutdown_request();
        ShutdownRequest r{};
        r.graceful = i.graceful();
        r.timeout = std::chrono::seconds{i.timeout_s()};
        return r;
    }
};

template <> struct ProtoBinding<StatusRequest> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kStatusRequest;
    static void set(Envelope& env, const StatusRequest& r) {
        env.mutable_status_request()->set_detailed(r.detailed);
    }
    static StatusRequest get(const Envelope& env) {
        StatusRequest r{};
        r.detailed = env.status_request().detailed();
        return r;
    }
};

template <> struct ProtoBinding<GenerateEmbeddingRequest> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kGenerateEmbeddingRequest;
    static void set(Envelope& env, const GenerateEmbeddingRequest& r) {
        auto* o = env.mutable_generate_embedding_request();
        o->set_text(r.text);
        o->set_model_name(r.modelName);
        o->set_normalize(r.normalize);
    }
    static GenerateEmbeddingRequest get(const Envelope& env) {
        const auto& i = env.generate_embedding_request();
        GenerateEmbeddingRequest r{};
        r.text = i.text();
        r.modelName = i.model_name();
        r.normalize = i.normalize();
        return r;
    }
};

template <> struct ProtoBinding<BatchEmbeddingRequest> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kBatchEmbeddingRequest;
    static void set(Envelope& env, const BatchEmbeddingRequest& r) {
        auto* o = env.mutable_batch_embedding_request();
        set_string_list(r.texts, o->mutable_texts());
        o->set_model_name(r.modelName);
        o->set_normalize(r.normalize);
        o->set_batch_size(static_cast<uint32_t>(r.batchSize));
    }
    static BatchEmbeddingRequest get(const Envelope& env) {
        const auto& i = env.batch_embedding_request();
        BatchEmbeddingRequest r{};
        r.texts = get_string_list(i.texts());
        r.modelName = i.model_name();
        r.normalize = i.normalize();
        r.batchSize = i.batch_size();
        return r;
    }
};

// EmbedDocumentsRequest (daemon-side embedding/persist)
template <> struct ProtoBinding<EmbedDocumentsRequest> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kEmbedDocumentsRequest;
    static void set(Envelope& env, const EmbedDocumentsRequest& r) {
        auto* o = env.mutable_embed_documents_request();
        set_string_list(r.documentHashes, o->mutable_document_hashes());
        o->set_model_name(r.modelName);
        o->set_normalize(r.normalize);
        o->set_batch_size(static_cast<uint32_t>(r.batchSize));
        o->set_skip_existing(r.skipExisting);
    }
    static EmbedDocumentsRequest get(const Envelope& env) {
        const auto& i = env.embed_documents_request();
        EmbedDocumentsRequest r{};
        r.documentHashes = get_string_list(i.document_hashes());
        r.modelName = i.model_name();
        r.normalize = i.normalize();
        r.batchSize = static_cast<size_t>(i.batch_size());
        r.skipExisting = i.skip_existing();
        return r;
    }
};

template <> struct ProtoBinding<LoadModelRequest> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kLoadModelRequest;
    static void set(Envelope& env, const LoadModelRequest& r) {
        auto* o = env.mutable_load_model_request();
        o->set_model_name(r.modelName);
        o->set_preload(r.preload);
        o->set_options_json(r.optionsJson);
    }
    static LoadModelRequest get(const Envelope& env) {
        const auto& i = env.load_model_request();
        LoadModelRequest r{};
        r.modelName = i.model_name();
        r.preload = i.preload();
        r.optionsJson = i.options_json();
        return r;
    }
};

template <> struct ProtoBinding<UnloadModelRequest> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kUnloadModelRequest;
    static void set(Envelope& env, const UnloadModelRequest& r) {
        auto* o = env.mutable_unload_model_request();
        o->set_model_name(r.modelName);
        o->set_force(r.force);
    }
    static UnloadModelRequest get(const Envelope& env) {
        const auto& i = env.unload_model_request();
        UnloadModelRequest r{};
        r.modelName = i.model_name();
        r.force = i.force();
        return r;
    }
};

template <> struct ProtoBinding<ModelStatusRequest> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kModelStatusRequest;
    static void set(Envelope& env, const ModelStatusRequest& r) {
        auto* o = env.mutable_model_status_request();
        o->set_model_name(r.modelName);
        o->set_detailed(r.detailed);
    }
    static ModelStatusRequest get(const Envelope& env) {
        const auto& i = env.model_status_request();
        ModelStatusRequest r{};
        r.modelName = i.model_name();
        r.detailed = i.detailed();
        return r;
    }
};

template <> struct ProtoBinding<AddDocumentRequest> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kAddDocumentRequest;
    static void set(Envelope& env, const AddDocumentRequest& r) {
        auto* o = env.mutable_add_document_request();
        o->set_path(r.path);
        o->set_content(r.content);
        o->set_name(r.name);
        set_string_list(r.tags, o->mutable_tags());
        to_kv_pairs(r.metadata, o->mutable_metadata());
        o->set_recursive(r.recursive);
        o->set_include_hidden(r.includeHidden);
        set_string_list(r.includePatterns, o->mutable_include_patterns());
        set_string_list(r.excludePatterns, o->mutable_exclude_patterns());
        o->set_collection(r.collection);
        o->set_snapshot_id(r.snapshotId);
        o->set_snapshot_label(r.snapshotLabel);
        o->set_mime_type(r.mimeType);
        o->set_disable_auto_mime(r.disableAutoMime);
        o->set_no_embeddings(r.noEmbeddings);
    }
    static AddDocumentRequest get(const Envelope& env) {
        const auto& i = env.add_document_request();
        AddDocumentRequest r{};
        r.path = i.path();
        r.content = i.content();
        r.name = i.name();
        r.tags = get_string_list(i.tags());
        r.metadata = from_kv_pairs(i.metadata());
        r.recursive = i.recursive();
        r.includeHidden = i.include_hidden();
        r.includePatterns = get_string_list(i.include_patterns());
        r.excludePatterns = get_string_list(i.exclude_patterns());
        r.collection = i.collection();
        r.snapshotId = i.snapshot_id();
        r.snapshotLabel = i.snapshot_label();
        r.mimeType = i.mime_type();
        r.disableAutoMime = i.disable_auto_mime();
        r.noEmbeddings = i.no_embeddings();
        return r;
    }
};

template <> struct ProtoBinding<GrepRequest> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kGrepRequest;
    static void set(Envelope& env, const GrepRequest& r) {
        auto* o = env.mutable_grep_request();
        o->set_pattern(r.pattern);
        o->set_path(r.path);
        set_string_list(r.paths, o->mutable_paths());
        o->set_case_insensitive(r.caseInsensitive);
        o->set_invert_match(r.invertMatch);
        o->set_context_lines(static_cast<int32_t>(r.contextLines));
        o->set_max_matches(static_cast<uint64_t>(r.maxMatches));
        set_string_list(r.includePatterns, o->mutable_include_patterns());
        o->set_recursive(r.recursive);
        o->set_whole_word(r.wholeWord);
        o->set_show_line_numbers(r.showLineNumbers);
        o->set_show_filename(r.showFilename);
        o->set_no_filename(r.noFilename);
        o->set_count_only(r.countOnly);
        o->set_files_only(r.filesOnly);
        o->set_files_without_match(r.filesWithoutMatch);
        o->set_paths_only(r.pathsOnly);
        o->set_literal_text(r.literalText);
        o->set_regex_only(r.regexOnly);
        o->set_semantic_limit(static_cast<uint64_t>(r.semanticLimit));
        set_string_list(r.filterTags, o->mutable_filter_tags());
        o->set_match_all_tags(r.matchAllTags);
        o->set_color_mode(r.colorMode);
        o->set_before_context(r.beforeContext);
        o->set_after_context(r.afterContext);
        o->set_show_diff(r.showDiff);
    }
    static GrepRequest get(const Envelope& env) {
        const auto& i = env.grep_request();
        GrepRequest r{};
        r.pattern = i.pattern();
        r.path = i.path();
        r.paths = get_string_list(i.paths());
        r.caseInsensitive = i.case_insensitive();
        r.invertMatch = i.invert_match();
        r.contextLines = i.context_lines();
        r.maxMatches = i.max_matches();
        r.includePatterns = get_string_list(i.include_patterns());
        r.recursive = i.recursive();
        r.wholeWord = i.whole_word();
        r.showLineNumbers = i.show_line_numbers();
        r.showFilename = i.show_filename();
        r.noFilename = i.no_filename();
        r.countOnly = i.count_only();
        r.filesOnly = i.files_only();
        r.filesWithoutMatch = i.files_without_match();
        r.pathsOnly = i.paths_only();
        r.literalText = i.literal_text();
        r.regexOnly = i.regex_only();
        r.semanticLimit = i.semantic_limit();
        r.filterTags = get_string_list(i.filter_tags());
        r.matchAllTags = i.match_all_tags();
        r.colorMode = i.color_mode();
        r.beforeContext = i.before_context();
        r.afterContext = i.after_context();
        r.showDiff = i.show_diff();
        return r;
    }
};

template <> struct ProtoBinding<UpdateDocumentRequest> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kUpdateDocumentRequest;
    static void set(Envelope& env, const UpdateDocumentRequest& r) {
        auto* o = env.mutable_update_document_request();
        o->set_hash(r.hash);
        o->set_name(r.name);
        o->set_new_content(r.newContent);
        set_string_list(r.addTags, o->mutable_add_tags());
        set_string_list(r.removeTags, o->mutable_remove_tags());
        to_kv_pairs(r.metadata, o->mutable_metadata());
        o->set_atomic(r.atomic);
        o->set_create_backup(r.createBackup);
        o->set_verbose(r.verbose);
    }
    static UpdateDocumentRequest get(const Envelope& env) {
        const auto& i = env.update_document_request();
        UpdateDocumentRequest r{};
        r.hash = i.hash();
        r.name = i.name();
        r.newContent = i.new_content();
        r.addTags = get_string_list(i.add_tags());
        r.removeTags = get_string_list(i.remove_tags());
        r.metadata = from_kv_pairs(i.metadata());
        r.atomic = i.atomic();
        r.createBackup = i.create_backup();
        r.verbose = i.verbose();
        return r;
    }
};

template <> struct ProtoBinding<DownloadRequest> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kDownloadRequest;
    static void set(Envelope& env, const DownloadRequest& r) {
        auto* o = env.mutable_download_request();
        o->set_url(r.url);
        o->set_output_path(r.outputPath);
        set_string_list(r.tags, o->mutable_tags());
        to_kv_pairs(r.metadata, o->mutable_metadata());
        o->set_quiet(r.quiet);
    }
    static DownloadRequest get(const Envelope& env) {
        const auto& i = env.download_request();
        DownloadRequest r{};
        r.url = i.url();
        r.outputPath = i.output_path();
        r.tags = get_string_list(i.tags());
        r.metadata = from_kv_pairs(i.metadata());
        r.quiet = i.quiet();
        return r;
    }
};

template <> struct ProtoBinding<GetStatsRequest> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kGetStatsRequest;
    static void set(Envelope& env, const GetStatsRequest& r) {
        auto* o = env.mutable_get_stats_request();
        o->set_detailed(r.detailed);
        o->set_include_cache(r.includeCache);
        o->set_show_file_types(r.showFileTypes);
        o->set_show_compression(r.showCompression);
        o->set_show_duplicates(r.showDuplicates);
        o->set_show_dedup(r.showDedup);
        o->set_show_performance(r.showPerformance);
        o->set_include_health(r.includeHealth);
    }
    static GetStatsRequest get(const Envelope& env) {
        const auto& i = env.get_stats_request();
        GetStatsRequest r{};
        r.detailed = i.detailed();
        r.includeCache = i.include_cache();
        r.showFileTypes = i.show_file_types();
        r.showCompression = i.show_compression();
        r.showDuplicates = i.show_duplicates();
        r.showDedup = i.show_dedup();
        r.showPerformance = i.show_performance();
        r.includeHealth = i.include_health();
        return r;
    }
};

template <> struct ProtoBinding<PrepareSessionRequest> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kPrepareSessionRequest;
    static void set(Envelope& env, const PrepareSessionRequest& r) {
        auto* o = env.mutable_prepare_session_request();
        o->set_session_name(r.sessionName);
        o->set_cores(r.cores);
        o->set_memory_gb(r.memoryGb);
        o->set_time_ms(r.timeMs);
        o->set_aggressive(r.aggressive);
        o->set_limit(static_cast<uint64_t>(r.limit));
        o->set_snippet_len(static_cast<uint32_t>(r.snippetLen));
    }
    static PrepareSessionRequest get(const Envelope& env) {
        const auto& i = env.prepare_session_request();
        PrepareSessionRequest r{};
        r.sessionName = i.session_name();
        r.cores = i.cores();
        r.memoryGb = i.memory_gb();
        r.timeMs = i.time_ms();
        r.aggressive = i.aggressive();
        r.limit = static_cast<size_t>(i.limit());
        r.snippetLen = static_cast<size_t>(i.snippet_len());
        return r;
    }
};

// --------------------------- Plugin: Requests ---------------------------
template <> struct ProtoBinding<PluginScanRequest> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kPluginScanRequest;
    static void set(Envelope& env, const PluginScanRequest& r) {
        auto* o = env.mutable_plugin_scan_request();
        o->set_dir(r.dir);
        o->set_target(r.target);
    }
    static PluginScanRequest get(const Envelope& env) {
        const auto& i = env.plugin_scan_request();
        PluginScanRequest r{};
        r.dir = i.dir();
        r.target = i.target();
        return r;
    }
};

template <> struct ProtoBinding<PluginLoadRequest> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kPluginLoadRequest;
    static void set(Envelope& env, const PluginLoadRequest& r) {
        auto* o = env.mutable_plugin_load_request();
        o->set_path_or_name(r.pathOrName);
        o->set_config_json(r.configJson);
        o->set_dry_run(r.dryRun);
    }
    static PluginLoadRequest get(const Envelope& env) {
        const auto& i = env.plugin_load_request();
        PluginLoadRequest r{};
        r.pathOrName = i.path_or_name();
        r.configJson = i.config_json();
        r.dryRun = i.dry_run();
        return r;
    }
};

template <> struct ProtoBinding<PluginUnloadRequest> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kPluginUnloadRequest;
    static void set(Envelope& env, const PluginUnloadRequest& r) {
        auto* o = env.mutable_plugin_unload_request();
        o->set_name(r.name);
    }
    static PluginUnloadRequest get(const Envelope& env) {
        const auto& i = env.plugin_unload_request();
        PluginUnloadRequest r{};
        r.name = i.name();
        return r;
    }
};

template <> struct ProtoBinding<PluginTrustListRequest> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kPluginTrustListRequest;
    static void set(Envelope& env, const PluginTrustListRequest&) {
        (void)env.mutable_plugin_trust_list_request();
    }
    static PluginTrustListRequest get(const Envelope&) { return PluginTrustListRequest{}; }
};

template <> struct ProtoBinding<PluginTrustAddRequest> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kPluginTrustAddRequest;
    static void set(Envelope& env, const PluginTrustAddRequest& r) {
        auto* o = env.mutable_plugin_trust_add_request();
        o->set_path(r.path);
    }
    static PluginTrustAddRequest get(const Envelope& env) {
        const auto& i = env.plugin_trust_add_request();
        PluginTrustAddRequest r{};
        r.path = i.path();
        return r;
    }
};

template <> struct ProtoBinding<PluginTrustRemoveRequest> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kPluginTrustRemoveRequest;
    static void set(Envelope& env, const PluginTrustRemoveRequest& r) {
        auto* o = env.mutable_plugin_trust_remove_request();
        o->set_path(r.path);
    }
    static PluginTrustRemoveRequest get(const Envelope& env) {
        const auto& i = env.plugin_trust_remove_request();
        PluginTrustRemoveRequest r{};
        r.path = i.path();
        return r;
    }
};

// --------------------------- Responses ---------------------------
template <> struct ProtoBinding<SuccessResponse> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kSuccessResponse;
    static void set(Envelope& env, const SuccessResponse& r) {
        env.mutable_success_response()->set_message(r.message);
    }
    static SuccessResponse get(const Envelope& env) {
        SuccessResponse r{};
        r.message = env.success_response().message();
        return r;
    }
};

template <> struct ProtoBinding<PrepareSessionResponse> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kPrepareSessionResponse;
    static void set(Envelope& env, const PrepareSessionResponse& r) {
        auto* o = env.mutable_prepare_session_response();
        o->set_warmed_count(r.warmedCount);
        o->set_message(r.message);
    }
    static PrepareSessionResponse get(const Envelope& env) {
        const auto& i = env.prepare_session_response();
        PrepareSessionResponse r{};
        r.warmedCount = i.warmed_count();
        r.message = i.message();
        return r;
    }
};

template <> struct ProtoBinding<SearchResponse> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kSearchResponse;
    static void set(Envelope& env, const SearchResponse& r) {
        auto* o = env.mutable_search_response();
        o->set_total_count(static_cast<uint64_t>(r.totalCount));
        o->set_elapsed_ms(static_cast<int64_t>(r.elapsed.count()));
        for (const auto& s : r.results) {
            auto* pr = o->add_results();
            pr->set_id(yams::common::sanitizeUtf8(s.id));
            pr->set_path(yams::common::sanitizeUtf8(s.path));
            pr->set_title(yams::common::sanitizeUtf8(s.title));
            pr->set_snippet(yams::common::sanitizeUtf8(s.snippet));
            pr->set_score(s.score);
            to_kv_pairs(s.metadata, pr->mutable_metadata());
        }
    }
    static SearchResponse get(const Envelope& env) {
        SearchResponse r{};
        const auto& i = env.search_response();
        r.totalCount = i.total_count();
        r.elapsed = std::chrono::milliseconds{i.elapsed_ms()};
        r.results.reserve(i.results_size());
        for (const auto& pr : i.results()) {
            SearchResult sr{};
            sr.id = pr.id();
            sr.path = pr.path();
            sr.title = pr.title();
            sr.snippet = pr.snippet();
            sr.score = pr.score();
            sr.metadata = from_kv_pairs(pr.metadata());
            r.results.emplace_back(std::move(sr));
        }
        return r;
    }
};

template <> struct ProtoBinding<AddResponse> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kAddResponse;
    static void set(Envelope& env, const AddResponse& r) {
        auto* o = env.mutable_add_response();
        o->set_hash(r.hash);
        o->set_bytes_stored(static_cast<uint64_t>(r.bytesStored));
        o->set_bytes_deduped(static_cast<uint64_t>(r.bytesDeduped));
        o->set_dedup_ratio(r.dedupRatio);
    }
    static AddResponse get(const Envelope& env) {
        const auto& i = env.add_response();
        AddResponse r{};
        r.hash = i.hash();
        r.bytesStored = i.bytes_stored();
        r.bytesDeduped = i.bytes_deduped();
        r.dedupRatio = i.dedup_ratio();
        return r;
    }
};

template <> struct ProtoBinding<GetInitResponse> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kGetInitResponse;
    static void set(Envelope& env, const GetInitResponse& r) {
        auto* o = env.mutable_get_init_response();
        o->set_transfer_id(r.transferId);
        o->set_total_size(r.totalSize);
        o->set_chunk_size(r.chunkSize);
        to_kv_pairs(r.metadata, o->mutable_metadata());
    }
    static GetInitResponse get(const Envelope& env) {
        const auto& i = env.get_init_response();
        GetInitResponse r{};
        r.transferId = i.transfer_id();
        r.totalSize = i.total_size();
        r.chunkSize = i.chunk_size();
        r.metadata = from_kv_pairs(i.metadata());
        return r;
    }
};

template <> struct ProtoBinding<GetChunkResponse> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kGetChunkResponse;
    static void set(Envelope& env, const GetChunkResponse& r) {
        auto* o = env.mutable_get_chunk_response();
        o->set_data(r.data);
        o->set_bytes_remaining(r.bytesRemaining);
    }
    static GetChunkResponse get(const Envelope& env) {
        const auto& i = env.get_chunk_response();
        GetChunkResponse r{};
        r.data = i.data();
        r.bytesRemaining = i.bytes_remaining();
        return r;
    }
};

template <> struct ProtoBinding<ListResponse> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kListResponse;
    static void set(Envelope& env, const ListResponse& r) {
        auto* o = env.mutable_list_response();
        for (const auto& e : r.items) {
            auto* le = o->add_items();
            le->set_hash(e.hash);
            le->set_path(e.path);
            le->set_name(e.name);
            le->set_file_name(e.fileName);
            le->set_size(static_cast<uint64_t>(e.size));
            le->set_mime_type(e.mimeType);
            le->set_file_type(e.fileType);
            le->set_extension(e.extension);
            le->set_created(e.created);
            le->set_modified(e.modified);
            le->set_indexed(e.indexed);
            le->set_snippet(e.snippet);
            le->set_language(e.language);
            le->set_extraction_method(e.extractionMethod);
            set_string_list(e.tags, le->mutable_tags());
            to_kv_pairs(e.metadata, le->mutable_metadata());
            le->set_change_type(e.changeType);
            le->set_change_time(e.changeTime);
            le->set_relevance_score(e.relevanceScore);
            le->set_match_reason(e.matchReason);
        }
        o->set_total_count(static_cast<uint64_t>(r.totalCount));
    }
    static ListResponse get(const Envelope& env) {
        const auto& i = env.list_response();
        ListResponse r{};
        r.totalCount = i.total_count();
        r.items.reserve(i.items_size());
        for (const auto& le : i.items()) {
            ListEntry e{};
            e.hash = le.hash();
            e.path = le.path();
            e.name = le.name();
            e.fileName = le.file_name();
            e.size = le.size();
            e.mimeType = le.mime_type();
            e.fileType = le.file_type();
            e.extension = le.extension();
            e.created = le.created();
            e.modified = le.modified();
            e.indexed = le.indexed();
            e.snippet = le.snippet();
            e.language = le.language();
            e.extractionMethod = le.extraction_method();
            e.tags = get_string_list(le.tags());
            e.metadata = from_kv_pairs(le.metadata());
            e.changeType = le.change_type();
            e.changeTime = le.change_time();
            e.relevanceScore = le.relevance_score();
            e.matchReason = le.match_reason();
            r.items.emplace_back(std::move(e));
        }
        return r;
    }
};

template <> struct ProtoBinding<StatusResponse> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kStatusResponse;
    static void set(Envelope& env, const StatusResponse& r) {
        auto* o = env.mutable_status_response();
        // Preserve textual summary for legacy clients
        o->set_state(r.overallStatus.empty() ? (r.running ? "ready" : "stopped") : r.overallStatus);
        // Populate extended runtime fields (proto v2+)
        o->set_running(r.running);
        o->set_ready(r.ready);
        o->set_uptime_seconds(static_cast<uint64_t>(r.uptimeSeconds));
        o->set_requests_processed(static_cast<uint64_t>(r.requestsProcessed));
        o->set_active_connections(static_cast<uint64_t>(r.activeConnections));
        o->set_memory_mb(r.memoryUsageMb);
        o->set_cpu_pct(r.cpuUsagePercent);
        o->set_version(r.version);
        // v3 additions: readiness/progress/overall/request_counts
        o->set_overall_status(r.overallStatus);
        // lifecycle_state preferred by clients; mirror overallStatus if not provided
        // (no dedicated proto field available yet)
        // Encode last_error via request_counts with a reserved key for now
        // request_counts
        for (const auto& [k, v] : r.requestCounts) {
            auto* kv = o->add_request_counts();
            kv->set_key(k);
            kv->set_value(std::to_string(static_cast<uint64_t>(v)));
        }
        if (!r.lastError.empty()) {
            auto* kv = o->add_request_counts();
            kv->set_key("last_error");
            kv->set_value(r.lastError);
        }
        // Encode content store diagnostics in request_counts under reserved keys
        if (!r.contentStoreRoot.empty()) {
            auto* kv = o->add_request_counts();
            kv->set_key("content_store_root");
            kv->set_value(r.contentStoreRoot);
        }
        if (!r.contentStoreError.empty()) {
            auto* kv = o->add_request_counts();
            kv->set_key("content_store_error");
            kv->set_value(r.contentStoreError);
        }
        // Embedding runtime diagnostics (best-effort)
        {
            auto* kv = o->add_request_counts();
            kv->set_key("embedding_available");
            kv->set_value(r.embeddingAvailable ? "1" : "0");
        }
        if (!r.embeddingBackend.empty()) {
            auto* kv = o->add_request_counts();
            kv->set_key("embedding_backend");
            kv->set_value(r.embeddingBackend);
        }
        if (!r.embeddingModel.empty()) {
            auto* kv = o->add_request_counts();
            kv->set_key("embedding_model");
            kv->set_value(r.embeddingModel);
        }
        if (!r.embeddingModelPath.empty()) {
            auto* kv = o->add_request_counts();
            kv->set_key("embedding_model_path");
            kv->set_value(r.embeddingModelPath);
        }
        if (r.embeddingDim > 0) {
            auto* kv = o->add_request_counts();
            kv->set_key("embedding_dim");
            kv->set_value(std::to_string(r.embeddingDim));
        }
        if (r.embeddingThreadsIntra != 0) {
            auto* kv = o->add_request_counts();
            kv->set_key("embedding_threads_intra");
            kv->set_value(std::to_string(r.embeddingThreadsIntra));
        }
        if (r.embeddingThreadsInter != 0) {
            auto* kv = o->add_request_counts();
            kv->set_key("embedding_threads_inter");
            kv->set_value(std::to_string(r.embeddingThreadsInter));
        }
        // readiness: bool->string
        for (const auto& [k, v] : r.readinessStates) {
            auto* kv = o->add_readiness();
            kv->set_key(k);
            kv->set_value(v ? "true" : "false");
        }
        // progress
        for (const auto& [k, v] : r.initProgress) {
            auto* kv = o->add_progress();
            kv->set_key(k);
            kv->set_value(std::to_string(static_cast<uint32_t>(v)));
        }

        // providers (typed)
        for (const auto& p : r.providers) {
            auto* po = o->add_providers();
            po->set_name(p.name);
            po->set_ready(p.ready);
            po->set_degraded(p.degraded);
            po->set_error(p.error);
            po->set_models_loaded(p.modelsLoaded);
            po->set_is_provider(p.isProvider);
        }
        // skipped plugins
        for (const auto& s : r.skippedPlugins) {
            auto* sp = o->add_skipped();
            sp->set_path(s.path);
            sp->set_reason(s.reason);
        }
        // PBI-040, task 040-1: PostIngestQueue depth
        o->set_post_ingest_queue_depth(r.postIngestQueueDepth);
    }
    static StatusResponse get(const Envelope& env) {
        StatusResponse r{};
        const auto& i = env.status_response();
        // Preserve textual summary for back-compat
        r.overallStatus = i.state();
        // Hydrate extended fields when present
        r.running = i.running();
        r.ready = i.ready();
        r.uptimeSeconds = i.uptime_seconds();
        r.requestsProcessed = i.requests_processed();
        r.activeConnections = i.active_connections();
        r.memoryUsageMb = i.memory_mb();
        r.cpuUsagePercent = i.cpu_pct();
        r.version = i.version();
        // v3 additions
        if (!i.overall_status().empty()) {
            r.overallStatus = i.overall_status();
        }
        r.lifecycleState = r.overallStatus; // prefer explicit field for clients
        // request_counts
        for (const auto& kv : i.request_counts()) {
            if (kv.key() == "last_error") {
                r.lastError = kv.value();
                continue;
            }
            if (kv.key() == "content_store_root") {
                r.contentStoreRoot = kv.value();
                continue;
            }
            if (kv.key() == "content_store_error") {
                r.contentStoreError = kv.value();
                continue;
            }
            if (kv.key() == "embedding_available") {
                std::string v = kv.value();
                for (auto& c : v)
                    c = static_cast<char>(std::tolower(c));
                r.embeddingAvailable = (v == "1" || v == "true" || v == "yes");
                continue;
            }
            if (kv.key() == "embedding_backend") {
                r.embeddingBackend = kv.value();
                continue;
            }
            if (kv.key() == "embedding_model") {
                r.embeddingModel = kv.value();
                continue;
            }
            if (kv.key() == "embedding_model_path") {
                r.embeddingModelPath = kv.value();
                continue;
            }
            if (kv.key() == "embedding_dim") {
                try {
                    r.embeddingDim = static_cast<uint32_t>(std::stoul(kv.value()));
                } catch (...) {
                }
                continue;
            }
            if (kv.key() == "embedding_threads_intra") {
                try {
                    r.embeddingThreadsIntra = static_cast<int32_t>(std::stol(kv.value()));
                } catch (...) {
                }
                continue;
            }
            if (kv.key() == "embedding_threads_inter") {
                try {
                    r.embeddingThreadsInter = static_cast<int32_t>(std::stol(kv.value()));
                } catch (...) {
                }
                continue;
            }
            try {
                uint64_t n = static_cast<uint64_t>(std::stoull(kv.value()));
                r.requestCounts[kv.key()] = n;
            } catch (...) {
                // ignore parse errors for numeric map
            }
        }
        // readiness
        for (const auto& kv : i.readiness()) {
            std::string v = kv.value();
            for (auto& c : v)
                c = static_cast<char>(std::tolower(c));
            bool b = (v == "1" || v == "true" || v == "yes");
            r.readinessStates[kv.key()] = b;
        }
        // progress
        for (const auto& kv : i.progress()) {
            try {
                uint64_t n = static_cast<uint64_t>(std::stoull(kv.value()));
                if (n > 100)
                    n = 100;
                r.initProgress[kv.key()] = static_cast<uint8_t>(n);
            } catch (...) {
                // ignore parse errors
            }
        }

        // providers (typed)
        for (const auto& po : i.providers()) {
            StatusResponse::ProviderInfo p;
            p.name = po.name();
            p.ready = po.ready();
            p.degraded = po.degraded();
            p.error = po.error();
            p.modelsLoaded = static_cast<uint32_t>(po.models_loaded());
            p.isProvider = po.is_provider();
            r.providers.push_back(std::move(p));
        }
        for (const auto& sp : i.skipped()) {
            StatusResponse::PluginSkipInfo s;
            s.path = sp.path();
            s.reason = sp.reason();
            r.skippedPlugins.push_back(std::move(s));
        }
        // PBI-040, task 040-1: PostIngestQueue depth
        r.postIngestQueueDepth = i.post_ingest_queue_depth();
        // If older daemon (proto v1) set only state, derive minimal booleans
        if (r.version.empty() && r.uptimeSeconds == 0 && r.memoryUsageMb == 0 &&
            r.cpuUsagePercent == 0) {
            r.running = (r.overallStatus != "stopped");
            r.ready = (r.overallStatus == "ready");
        }
        return r;
    }
};

template <> struct ProtoBinding<EmbeddingResponse> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kEmbeddingResponse;
    static void set(Envelope& env, const EmbeddingResponse& r) {
        auto* o = env.mutable_embedding_response();
        for (float f : r.embedding)
            o->add_embedding(f);
        o->set_dimensions(static_cast<uint64_t>(r.dimensions));
        o->set_model_used(r.modelUsed);
        o->set_processing_time_ms(r.processingTimeMs);
    }
    static EmbeddingResponse get(const Envelope& env) {
        const auto& i = env.embedding_response();
        EmbeddingResponse r{};
        r.embedding.reserve(i.embedding_size());
        for (float f : i.embedding())
            r.embedding.push_back(f);
        r.dimensions = i.dimensions();
        r.modelUsed = i.model_used();
        r.processingTimeMs = i.processing_time_ms();
        return r;
    }
};

template <> struct ProtoBinding<BatchEmbeddingResponse> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kBatchEmbeddingResponse;
    static void set(Envelope& env, const BatchEmbeddingResponse& r) {
        auto* o = env.mutable_batch_embedding_response();
        o->set_dimensions(static_cast<uint64_t>(r.dimensions));
        o->set_model_used(r.modelUsed);
        o->set_processing_time_ms(r.processingTimeMs);
        o->set_success_count(static_cast<uint64_t>(r.successCount));
        o->set_failure_count(static_cast<uint64_t>(r.failureCount));
        for (const auto& emb : r.embeddings) {
            auto* pe = o->add_embeddings();
            for (float f : emb)
                pe->add_embedding(f);
            pe->set_dimensions(static_cast<uint64_t>(r.dimensions));
            pe->set_model_used(r.modelUsed);
            pe->set_processing_time_ms(r.processingTimeMs);
        }
    }
    static BatchEmbeddingResponse get(const Envelope& env) {
        const auto& i = env.batch_embedding_response();
        BatchEmbeddingResponse r{};
        r.dimensions = i.dimensions();
        r.modelUsed = i.model_used();
        r.processingTimeMs = i.processing_time_ms();
        r.successCount = i.success_count();
        r.failureCount = i.failure_count();
        r.embeddings.reserve(i.embeddings_size());
        for (const auto& pe : i.embeddings()) {
            std::vector<float> v;
            v.reserve(pe.embedding_size());
            for (float f : pe.embedding())
                v.push_back(f);
            r.embeddings.emplace_back(std::move(v));
        }
        return r;
    }
};

// Streaming events
template <> struct ProtoBinding<EmbeddingEvent> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kEmbedEvent;
    static void set(Envelope& env, const EmbeddingEvent& r) {
        auto* o = env.mutable_embed_event();
        o->set_model_name(r.modelName);
        o->set_processed(static_cast<uint64_t>(r.processed));
        o->set_total(static_cast<uint64_t>(r.total));
        o->set_success(static_cast<uint64_t>(r.success));
        o->set_failure(static_cast<uint64_t>(r.failure));
        o->set_inserted(static_cast<uint64_t>(r.inserted));
        o->set_phase(r.phase);
        o->set_message(r.message);
    }
    static EmbeddingEvent get(const Envelope& env) {
        const auto& i = env.embed_event();
        EmbeddingEvent r{};
        r.modelName = i.model_name();
        r.processed = i.processed();
        r.total = i.total();
        r.success = i.success();
        r.failure = i.failure();
        r.inserted = i.inserted();
        r.phase = i.phase();
        r.message = i.message();
        return r;
    }
};

template <> struct ProtoBinding<ModelLoadEvent> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kModelLoadEvent;
    static void set(Envelope& env, const ModelLoadEvent& r) {
        auto* o = env.mutable_model_load_event();
        o->set_model_name(r.modelName);
        o->set_phase(r.phase);
        o->set_bytes_total(r.bytesTotal);
        o->set_bytes_loaded(r.bytesLoaded);
        o->set_message(r.message);
    }
    static ModelLoadEvent get(const Envelope& env) {
        const auto& i = env.model_load_event();
        ModelLoadEvent r{};
        r.modelName = i.model_name();
        r.phase = i.phase();
        r.bytesTotal = i.bytes_total();
        r.bytesLoaded = i.bytes_loaded();
        r.message = i.message();
        return r;
    }
};

template <> struct ProtoBinding<ModelLoadResponse> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kModelLoadResponse;
    static void set(Envelope& env, const ModelLoadResponse& r) {
        auto* o = env.mutable_model_load_response();
        o->set_loaded(r.success);
        o->set_message(r.modelName);
    }
    static ModelLoadResponse get(const Envelope& env) {
        const auto& i = env.model_load_response();
        ModelLoadResponse r{};
        r.success = i.loaded();
        r.modelName = i.message();
        r.memoryUsageMb = 0;
        r.loadTimeMs = 0;
        return r;
    }
};

template <> struct ProtoBinding<ModelStatusResponse> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kModelStatusResponse;
    static void set(Envelope& env, const ModelStatusResponse& r) {
        auto* o = env.mutable_model_status_response();
        if (!r.models.empty()) {
            o->set_model_name(r.models.front().name);
            o->set_status(r.models.front().loaded ? "loaded" : "unloaded");
        }
    }
    static ModelStatusResponse get(const Envelope& env) {
        const auto& i = env.model_status_response();
        ModelStatusResponse r{};
        if (!i.model_name().empty()) {
            ModelStatusResponse::ModelDetails d{};
            d.name = i.model_name();
            d.loaded = (i.status() == "loaded");
            r.models.push_back(std::move(d));
        }
        r.totalMemoryMb = 0;
        r.maxMemoryMb = 0;
        return r;
    }
};

// EmbedDocumentsResponse
template <> struct ProtoBinding<EmbedDocumentsResponse> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kEmbedDocumentsResponse;
    static void set(Envelope& env, const EmbedDocumentsResponse& r) {
        auto* o = env.mutable_embed_documents_response();
        o->set_requested(static_cast<uint64_t>(r.requested));
        o->set_embedded(static_cast<uint64_t>(r.embedded));
        o->set_skipped(static_cast<uint64_t>(r.skipped));
        o->set_failed(static_cast<uint64_t>(r.failed));
    }
    static EmbedDocumentsResponse get(const Envelope& env) {
        const auto& i = env.embed_documents_response();
        EmbedDocumentsResponse r{};
        r.requested = i.requested();
        r.embedded = i.embedded();
        r.skipped = i.skipped();
        r.failed = i.failed();
        return r;
    }
};
template <> struct ProtoBinding<AddDocumentResponse> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kAddDocumentResponse;
    static void set(Envelope& env, const AddDocumentResponse& r) {
        auto* o = env.mutable_add_document_response();
        o->set_hash(r.hash);
        o->set_message(r.message);
        o->set_path(r.path);
        o->set_documents_added(static_cast<uint64_t>(r.documentsAdded));
        o->set_documents_updated(static_cast<uint64_t>(r.documentsUpdated));
        o->set_documents_skipped(static_cast<uint64_t>(r.documentsSkipped));
        o->set_size(static_cast<uint64_t>(r.size));
        o->set_snapshot_id(r.snapshotId);
        o->set_snapshot_label(r.snapshotLabel);
    }
    static AddDocumentResponse get(const Envelope& env) {
        const auto& i = env.add_document_response();
        AddDocumentResponse r{};
        r.hash = i.hash();
        r.message = i.message();
        r.path = i.path();
        r.documentsAdded = i.documents_added();
        r.documentsUpdated = i.documents_updated();
        r.documentsSkipped = i.documents_skipped();
        r.size = i.size();
        r.snapshotId = i.snapshot_id();
        r.snapshotLabel = i.snapshot_label();
        return r;
    }
};

template <> struct ProtoBinding<GrepResponse> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kGrepResponse;
    static void set(Envelope& env, const GrepResponse& r) {
        auto* o = env.mutable_grep_response();
        for (const auto& match : r.matches) {
            auto* m = o->add_matches();
            m->set_file(match.file);
            m->set_line_number(match.lineNumber);
            // Use bytes setter to support binary/non-UTF-8 content
            m->set_line(match.line.data(), match.line.size());
            for (const auto& before : match.contextBefore) {
                m->add_context_before(before.data(), before.size());
            }
            for (const auto& after : match.contextAfter) {
                m->add_context_after(after.data(), after.size());
            }
            m->set_match_type(match.matchType);
            m->set_confidence(match.confidence);
        }
        o->set_total_matches(r.totalMatches);
        o->set_files_searched(r.filesSearched);
        o->set_regex_matches(r.regexMatches);
        o->set_semantic_matches(r.semanticMatches);
        o->set_execution_time_ms(r.executionTimeMs);
        o->set_query_info(r.queryInfo);
        to_kv_pairs(r.searchStats, o->mutable_search_stats());
        set_string_list(r.filesWith, o->mutable_files_with());
        set_string_list(r.filesWithout, o->mutable_files_without());
        set_string_list(r.pathsOnly, o->mutable_paths_only());
    }
    static GrepResponse get(const Envelope& env) {
        const auto& i = env.grep_response();
        GrepResponse r{};
        r.matches.reserve(i.matches_size());
        for (const auto& m : i.matches()) {
            GrepMatch match{};
            match.file = m.file();
            match.lineNumber = m.line_number();
            // Preserve raw bytes for binary-safe output
            match.line.assign(m.line().data(), m.line().size());
            for (const auto& before : m.context_before()) {
                match.contextBefore.emplace_back(before.data(), before.size());
            }
            for (const auto& after : m.context_after()) {
                match.contextAfter.emplace_back(after.data(), after.size());
            }
            match.matchType = m.match_type();
            match.confidence = m.confidence();
            r.matches.push_back(std::move(match));
        }
        r.totalMatches = i.total_matches();
        r.filesSearched = i.files_searched();
        r.regexMatches = i.regex_matches();
        r.semanticMatches = i.semantic_matches();
        r.executionTimeMs = i.execution_time_ms();
        r.queryInfo = i.query_info();
        r.searchStats = from_kv_pairs(i.search_stats());
        r.filesWith = get_string_list(i.files_with());
        r.filesWithout = get_string_list(i.files_without());
        r.pathsOnly = get_string_list(i.paths_only());
        return r;
    }
};

// --------------------------- Plugin: Responses ---------------------------
template <> struct ProtoBinding<PluginScanResponse> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kPluginScanResponse;
    static void set(Envelope& env, const PluginScanResponse& r) {
        auto* o = env.mutable_plugin_scan_response();
        for (const auto& pr : r.plugins) {
            auto* rec = o->add_plugins();
            rec->set_name(pr.name);
            rec->set_version(pr.version);
            rec->set_abi_version(pr.abiVersion);
            rec->set_path(pr.path);
            rec->set_manifest_json(pr.manifestJson);
            set_string_list(pr.interfaces, rec->mutable_interfaces());
        }
    }
    static PluginScanResponse get(const Envelope& env) {
        const auto& i = env.plugin_scan_response();
        PluginScanResponse r{};
        r.plugins.reserve(i.plugins_size());
        for (const auto& rec : i.plugins()) {
            PluginRecord pr{};
            pr.name = rec.name();
            pr.version = rec.version();
            pr.abiVersion = rec.abi_version();
            pr.path = rec.path();
            pr.manifestJson = rec.manifest_json();
            pr.interfaces = get_string_list(rec.interfaces());
            r.plugins.emplace_back(std::move(pr));
        }
        return r;
    }
};

template <> struct ProtoBinding<PluginLoadResponse> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kPluginLoadResponse;
    static void set(Envelope& env, const PluginLoadResponse& r) {
        auto* o = env.mutable_plugin_load_response();
        o->set_loaded(r.loaded);
        o->set_message(r.message);
        auto* rec = o->mutable_record();
        rec->set_name(r.record.name);
        rec->set_version(r.record.version);
        rec->set_abi_version(r.record.abiVersion);
        rec->set_path(r.record.path);
        rec->set_manifest_json(r.record.manifestJson);
        set_string_list(r.record.interfaces, rec->mutable_interfaces());
    }
    static PluginLoadResponse get(const Envelope& env) {
        const auto& i = env.plugin_load_response();
        PluginLoadResponse r{};
        r.loaded = i.loaded();
        r.message = i.message();
        const auto& rec = i.record();
        r.record.name = rec.name();
        r.record.version = rec.version();
        r.record.abiVersion = rec.abi_version();
        r.record.path = rec.path();
        r.record.manifestJson = rec.manifest_json();
        r.record.interfaces = get_string_list(rec.interfaces());
        return r;
    }
};

template <> struct ProtoBinding<PluginTrustListResponse> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kPluginTrustListResponse;
    static void set(Envelope& env, const PluginTrustListResponse& r) {
        auto* o = env.mutable_plugin_trust_list_response();
        set_string_list(r.paths, o->mutable_paths());
    }
    static PluginTrustListResponse get(const Envelope& env) {
        const auto& i = env.plugin_trust_list_response();
        PluginTrustListResponse r{};
        r.paths = get_string_list(i.paths());
        return r;
    }
};

template <> struct ProtoBinding<UpdateDocumentResponse> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kUpdateDocumentResponse;
    static void set(Envelope& env, const UpdateDocumentResponse& r) {
        env.mutable_update_document_response()->set_updated(r.contentUpdated || r.metadataUpdated ||
                                                            r.tagsUpdated);
    }
    static UpdateDocumentResponse get(const Envelope& env) {
        UpdateDocumentResponse r{};
        bool updated = env.update_document_response().updated();
        r.contentUpdated = updated;
        r.metadataUpdated = false;
        r.tagsUpdated = false;
        return r;
    }
};

template <> struct ProtoBinding<GetStatsResponse> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kGetStatsResponse;
    static void set(Envelope& env, const GetStatsResponse& r) {
        auto* o = env.mutable_get_stats_response();
        // Best-effort: provide JSON string and smuggle selected additionalStats keys inside it
        std::string jstr = "{}";
        if (auto it = r.additionalStats.find("json"); it != r.additionalStats.end())
            jstr = it->second;
        nlohmann::json j = nlohmann::json::parse(jstr, nullptr, false);
        if (j.is_discarded())
            j = nlohmann::json::object();
        if (auto pit = r.additionalStats.find("plugins_json"); pit != r.additionalStats.end()) {
            // Try to embed as JSON; fallback to string
            nlohmann::json pj = nlohmann::json::parse(pit->second, nullptr, false);
            if (!pj.is_discarded())
                j["plugins_json"] = pj;
            else
                j["plugins_json"] = pit->second;
        }
        o->set_json(j.dump());
        // Populate numeric fields alongside JSON so non-JSON clients don't see zeros
        o->set_total_documents(static_cast<uint64_t>(r.totalDocuments));
        o->set_total_size(static_cast<uint64_t>(r.totalSize));
        o->set_indexed_documents(static_cast<uint64_t>(r.indexedDocuments));
        o->set_vector_index_size(static_cast<uint64_t>(r.vectorIndexSize));
        o->set_compression_ratio(r.compressionRatio);
    }
    static GetStatsResponse get(const Envelope& env) {
        GetStatsResponse r{};
        const auto& g = env.get_stats_response();
        if (!g.json().empty()) {
            r.additionalStats["json"] = g.json();
            // Extract embedded plugins_json if present
            nlohmann::json j = nlohmann::json::parse(g.json(), nullptr, false);
            if (!j.is_discarded() && j.contains("plugins_json")) {
                const auto& v = j["plugins_json"];
                if (v.is_string())
                    r.additionalStats["plugins_json"] = v.get<std::string>();
                else
                    r.additionalStats["plugins_json"] = v.dump();
            }
        }
        // Also hydrate numeric fields for non-JSON consumers
        r.totalDocuments = static_cast<size_t>(g.total_documents());
        r.totalSize = static_cast<size_t>(g.total_size());
        r.indexedDocuments = static_cast<size_t>(g.indexed_documents());
        r.vectorIndexSize = static_cast<size_t>(g.vector_index_size());
        r.compressionRatio = g.compression_ratio();
        return r;
    }
};

template <> struct ProtoBinding<DownloadResponse> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kDownloadResponse;
    static void set(Envelope& env, const DownloadResponse& r) {
        auto* o = env.mutable_download_response();
        o->set_url(r.url);
        o->set_path(r.localPath);
        o->set_hash(r.hash);
        o->set_size(static_cast<uint64_t>(r.size));
        o->set_success(r.success);
        o->set_error(r.error);
    }
    static DownloadResponse get(const Envelope& env) {
        const auto& i = env.download_response();
        DownloadResponse r{};
        r.url = i.url();
        r.localPath = i.path();
        r.hash = i.hash();
        r.size = i.size();
        r.success = i.success();
        r.error = i.error();
        return r;
    }
};

template <> struct ProtoBinding<DeleteResponse> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kDeleteResponse;
    static void set(Envelope& env, const DeleteResponse& r) {
        auto* o = env.mutable_delete_response();
        o->set_success(r.successCount > 0);
        o->set_count(static_cast<uint64_t>(r.successCount));
    }
    static DeleteResponse get(const Envelope& env) {
        const auto& i = env.delete_response();
        DeleteResponse r{};
        r.dryRun = false;
        r.successCount = i.count();
        r.failureCount = 0;
        return r;
    }
};

// --- Session / Cat bindings (must appear after ProtoBinding primary template) ---
template <> struct ProtoBinding<CatRequest> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kCatRequest;
    static void set(Envelope& env, const CatRequest& r) {
        auto* o = env.mutable_cat_request();
        o->set_hash(r.hash);
        o->set_name(r.name);
    }
    static CatRequest get(const Envelope& env) {
        const auto& i = env.cat_request();
        CatRequest r{};
        r.hash = i.hash();
        r.name = i.name();
        return r;
    }
};

template <> struct ProtoBinding<CatResponse> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kCatResponse;
    static void set(Envelope& env, const CatResponse& r) {
        auto* o = env.mutable_cat_response();
        o->set_hash(r.hash);
        o->set_name(r.name);
        o->set_content(r.content);
        o->set_size(r.size);
        o->set_has_content(r.hasContent);
    }
    static CatResponse get(const Envelope& env) {
        const auto& i = env.cat_response();
        CatResponse r{};
        r.hash = i.hash();
        r.name = i.name();
        r.content = i.content();
        r.size = i.size();
        r.hasContent = i.has_content();
        return r;
    }
};

template <> struct ProtoBinding<ListSessionsRequest> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kListSessionsRequest;
    static void set(Envelope& env, const ListSessionsRequest&) {
        (void)env.mutable_list_sessions_request();
    }
    static ListSessionsRequest get(const Envelope&) { return ListSessionsRequest{}; }
};

template <> struct ProtoBinding<ListSessionsResponse> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kListSessionsResponse;
    static void set(Envelope& env, const ListSessionsResponse& r) {
        auto* o = env.mutable_list_sessions_response();
        set_string_list(r.session_names, o->mutable_session_names());
        o->set_current_session(r.current_session);
    }
    static ListSessionsResponse get(const Envelope& env) {
        const auto& i = env.list_sessions_response();
        ListSessionsResponse r{};
        r.session_names = get_string_list(i.session_names());
        r.current_session = i.current_session();
        return r;
    }
};

template <> struct ProtoBinding<UseSessionRequest> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kUseSessionRequest;
    static void set(Envelope& env, const UseSessionRequest& r) {
        auto* o = env.mutable_use_session_request();
        o->set_session_name(r.session_name);
    }
    static UseSessionRequest get(const Envelope& env) {
        const auto& i = env.use_session_request();
        UseSessionRequest r{};
        r.session_name = i.session_name();
        return r;
    }
};

template <> struct ProtoBinding<AddPathSelectorRequest> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kAddPathSelectorRequest;
    static void set(Envelope& env, const AddPathSelectorRequest& r) {
        auto* o = env.mutable_add_path_selector_request();
        o->set_session_name(r.session_name);
        o->set_path(r.path);
        set_string_list(r.tags, o->mutable_tags());
        to_kv_pairs(r.metadata, o->mutable_metadata());
    }
    static AddPathSelectorRequest get(const Envelope& env) {
        const auto& i = env.add_path_selector_request();
        AddPathSelectorRequest r{};
        r.session_name = i.session_name();
        r.path = i.path();
        r.tags = get_string_list(i.tags());
        r.metadata = from_kv_pairs(i.metadata());
        return r;
    }
};

template <> struct ProtoBinding<RemovePathSelectorRequest> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kRemovePathSelectorRequest;
    static void set(Envelope& env, const RemovePathSelectorRequest& r) {
        auto* o = env.mutable_remove_path_selector_request();
        o->set_session_name(r.session_name);
        o->set_path(r.path);
    }
    static RemovePathSelectorRequest get(const Envelope& env) {
        const auto& i = env.remove_path_selector_request();
        RemovePathSelectorRequest r{};
        r.session_name = i.session_name();
        r.path = i.path();
        return r;
    }
};

// Tree diff bindings (PBI-043)
template <> struct ProtoBinding<ListTreeDiffRequest> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kListTreeDiffRequest;
    static void set(Envelope& env, const ListTreeDiffRequest& r) {
        auto* o = env.mutable_list_tree_diff_request();
        o->set_base_snapshot_id(r.baseSnapshotId);
        o->set_target_snapshot_id(r.targetSnapshotId);
        o->set_path_prefix(r.pathPrefix);
        o->set_type_filter(r.typeFilter);
        o->set_limit(r.limit);
        o->set_offset(r.offset);
    }
    static ListTreeDiffRequest get(const Envelope& env) {
        const auto& i = env.list_tree_diff_request();
        ListTreeDiffRequest r{};
        r.baseSnapshotId = i.base_snapshot_id();
        r.targetSnapshotId = i.target_snapshot_id();
        r.pathPrefix = i.path_prefix();
        r.typeFilter = i.type_filter();
        r.limit = i.limit();
        r.offset = i.offset();
        return r;
    }
};

template <> struct ProtoBinding<ListTreeDiffResponse> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kListTreeDiffResponse;
    static void set(Envelope& env, const ListTreeDiffResponse& r) {
        auto* o = env.mutable_list_tree_diff_response();
        for (const auto& change : r.changes) {
            auto* entry = o->add_changes();
            entry->set_change_type(change.changeType);
            entry->set_path(change.path);
            entry->set_old_path(change.oldPath);
            entry->set_hash(change.hash);
            entry->set_old_hash(change.oldHash);
            entry->set_size(change.size);
            entry->set_old_size(change.oldSize);
            entry->set_content_delta_hash(change.contentDeltaHash);
        }
        o->set_total_count(r.totalCount);
    }
    static ListTreeDiffResponse get(const Envelope& env) {
        const auto& i = env.list_tree_diff_response();
        ListTreeDiffResponse r{};
        r.changes.reserve(static_cast<size_t>(i.changes_size()));
        for (const auto& entry : i.changes()) {
            TreeChangeEntry ce;
            ce.changeType = entry.change_type();
            ce.path = entry.path();
            ce.oldPath = entry.old_path();
            ce.hash = entry.hash();
            ce.oldHash = entry.old_hash();
            ce.size = entry.size();
            ce.oldSize = entry.old_size();
            ce.contentDeltaHash = entry.content_delta_hash();
            r.changes.push_back(std::move(ce));
        }
        r.totalCount = i.total_count();
        return r;
    }
};

template <> struct ProtoBinding<FileHistoryRequest> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kFileHistoryRequest;
    static void set(Envelope& env, const FileHistoryRequest& r) {
        auto* o = env.mutable_file_history_request();
        o->set_filepath(r.filepath);
    }
    static FileHistoryRequest get(const Envelope& env) {
        const auto& i = env.file_history_request();
        FileHistoryRequest r{};
        r.filepath = i.filepath();
        return r;
    }
};

template <> struct ProtoBinding<FileHistoryResponse> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kFileHistoryResponse;
    static void set(Envelope& env, const FileHistoryResponse& r) {
        auto* o = env.mutable_file_history_response();
        o->set_filepath(r.filepath);
        for (const auto& version : r.versions) {
            auto* v = o->add_versions();
            v->set_snapshot_id(version.snapshotId);
            v->set_hash(version.hash);
            v->set_size(version.size);
            v->set_indexed_timestamp(version.indexedTimestamp);
        }
        o->set_total_versions(r.totalVersions);
        o->set_found(r.found);
        o->set_message(r.message);
    }
    static FileHistoryResponse get(const Envelope& env) {
        const auto& i = env.file_history_response();
        FileHistoryResponse r{};
        r.filepath = i.filepath();
        r.versions.reserve(static_cast<size_t>(i.versions_size()));
        for (const auto& v : i.versions()) {
            FileVersion fv;
            fv.snapshotId = v.snapshot_id();
            fv.hash = v.hash();
            fv.size = v.size();
            fv.indexedTimestamp = v.indexed_timestamp();
            r.versions.push_back(std::move(fv));
        }
        r.totalVersions = i.total_versions();
        r.found = i.found();
        r.message = i.message();
        return r;
    }
};

// Helper to encode Request/Response variants using bindings
template <typename Variant>
static Result<void> encode_variant_into(Envelope& env, const Variant& v) {
    bool encoded = false;
    std::visit(
        [&](const auto& x) {
            using T = std::decay_t<decltype(x)>;
            if constexpr (HasProtoBinding<T>) {
                ProtoBinding<T>::set(env, x);
                encoded = true;
            }
        },
        v);
    if (!encoded) {
        return Error{ErrorCode::InvalidArgument, "Unsupported message type for proto"};
    }
    return Result<void>();
}

namespace {

Result<Envelope> build_envelope(const Message& msg) {
    Envelope env;
    env.set_version(PROTOCOL_VERSION);
    env.set_request_id(msg.requestId);
    if (msg.sessionId)
        env.set_session_id(*msg.sessionId);
    if (msg.clientVersion)
        env.set_client_version(*msg.clientVersion);
    env.set_expects_streaming_response(msg.expectsStreamingResponse);
    spdlog::debug("encode_payload: payload={} expects_streaming_response={} request_id={}",
                  (std::holds_alternative<Request>(msg.payload)
                       ? static_cast<int>(getMessageType(std::get<Request>(msg.payload)))
                       : static_cast<int>(getMessageType(std::get<Response>(msg.payload)))),
                  msg.expectsStreamingResponse, msg.requestId);

    if (std::holds_alternative<Request>(msg.payload)) {
        const auto& req = std::get<Request>(msg.payload);
        // Best-effort debug: log message type before encoding
        spdlog::debug("encode_payload: request type={} request_id={}",
                      static_cast<int>(getMessageType(req)), msg.requestId);
        auto r = encode_variant_into(env, req);
        if (!r)
            return r.error();
    } else {
        const auto& res = std::get<Response>(msg.payload);
        spdlog::debug("encode_payload: response type={} request_id={}",
                      static_cast<int>(getMessageType(res)), msg.requestId);
        auto r = encode_variant_into(env, res);
        if (!r)
            return r.error();
    }

    return env;
}

} // namespace

Result<void> ProtoSerializer::encode_payload_into(const Message& msg,
                                                  std::vector<uint8_t>& buffer) {
    auto env_result = build_envelope(msg);
    if (!env_result)
        return env_result.error();

    Envelope env = std::move(env_result.value());
    const auto size = env.ByteSizeLong();
    if (size > MAX_MESSAGE_SIZE) {
        return Error{ErrorCode::InvalidData, "Serialized payload exceeds MAX_MESSAGE_SIZE"};
    }

    const auto base = buffer.size();
    buffer.resize(base + static_cast<std::size_t>(size));
    if (!env.SerializeToArray(buffer.data() + base, static_cast<int>(size))) {
        buffer.resize(base);
        return Error{ErrorCode::SerializationError, "Failed to serialize protobuf Envelope"};
    }

    return Result<void>();
}

Result<std::vector<uint8_t>> ProtoSerializer::encode_payload(const Message& msg) {
    std::vector<uint8_t> out;
    auto res = encode_payload_into(msg, out);
    if (!res)
        return res.error();
    return out;
}

Result<Message> ProtoSerializer::decode_payload(const std::vector<uint8_t>& bytes) {
    if (bytes.size() > MAX_MESSAGE_SIZE) {
        return Error{ErrorCode::InvalidData, "Payload exceeds MAX_MESSAGE_SIZE"};
    }
    Envelope env;
    if (!env.ParseFromArray(bytes.data(), static_cast<int>(bytes.size()))) {
        return Error{ErrorCode::SerializationError, "Failed to parse protobuf Envelope"};
    }
    // Debug visibility into the incoming payload
    spdlog::debug(
        "decode_payload: payload_case={} request_id={} size={}B expects_streaming_response={}",
        static_cast<int>(env.payload_case()), env.request_id(), bytes.size(),
        env.expects_streaming_response());

    Message m;
    m.version = env.version();
    m.timestamp = std::chrono::steady_clock::now();
    m.requestId = env.request_id();
    if (!env.session_id().empty())
        m.sessionId = env.session_id();
    if (!env.client_version().empty())
        m.clientVersion = env.client_version();
    m.expectsStreamingResponse = env.expects_streaming_response();

    switch (env.payload_case()) {
        case Envelope::kPingRequest: {
            PingRequest pr = ProtoBinding<PingRequest>::get(env);
            m.payload = Request{std::move(pr)};
            break;
        }
        case Envelope::kPongResponse: {
            PongResponse pr = ProtoBinding<PongResponse>::get(env);
            m.payload = Response{std::in_place_type<PongResponse>, std::move(pr)};
            break;
        }
        case Envelope::kError: {
            ErrorResponse er = ProtoBinding<ErrorResponse>::get(env);
            m.payload = Response{std::in_place_type<ErrorResponse>, std::move(er)};
            break;
        }
        // Additional requests
        case Envelope::kSearchRequest: {
            auto v = ProtoBinding<SearchRequest>::get(env);
            m.payload = Request{std::move(v)};
            break;
        }
        case Envelope::kGetRequest: {
            auto v = ProtoBinding<GetRequest>::get(env);
            m.payload = Request{std::move(v)};
            break;
        }
        case Envelope::kGetInitRequest: {
            auto v = ProtoBinding<GetInitRequest>::get(env);
            m.payload = Request{std::move(v)};
            break;
        }
        case Envelope::kGetChunkRequest: {
            auto v = ProtoBinding<GetChunkRequest>::get(env);
            m.payload = Request{std::move(v)};
            break;
        }
        case Envelope::kGetEndRequest: {
            auto v = ProtoBinding<GetEndRequest>::get(env);
            m.payload = Request{std::move(v)};
            break;
        }
        case Envelope::kDeleteRequest: {
            auto v = ProtoBinding<DeleteRequest>::get(env);
            m.payload = Request{std::move(v)};
            break;
        }
        case Envelope::kListRequest: {
            auto v = ProtoBinding<ListRequest>::get(env);
            m.payload = Request{std::move(v)};
            break;
        }
        case Envelope::kShutdownRequest: {
            auto v = ProtoBinding<ShutdownRequest>::get(env);
            m.payload = Request{std::move(v)};
            break;
        }
        case Envelope::kStatusRequest: {
            auto v = ProtoBinding<StatusRequest>::get(env);
            m.payload = Request{std::move(v)};
            break;
        }
        case Envelope::kGenerateEmbeddingRequest: {
            auto v = ProtoBinding<GenerateEmbeddingRequest>::get(env);
            m.payload = Request{std::move(v)};
            break;
        }
        case Envelope::kBatchEmbeddingRequest: {
            auto v = ProtoBinding<BatchEmbeddingRequest>::get(env);
            m.payload = Request{std::move(v)};
            break;
        }
        case Envelope::kLoadModelRequest: {
            auto v = ProtoBinding<LoadModelRequest>::get(env);
            m.payload = Request{std::move(v)};
            break;
        }
        case Envelope::kUnloadModelRequest: {
            auto v = ProtoBinding<UnloadModelRequest>::get(env);
            m.payload = Request{std::move(v)};
            break;
        }
        case Envelope::kModelStatusRequest: {
            auto v = ProtoBinding<ModelStatusRequest>::get(env);
            m.payload = Request{std::move(v)};
            break;
        }
        case Envelope::kAddDocumentRequest: {
            auto v = ProtoBinding<AddDocumentRequest>::get(env);
            m.payload = Request{std::move(v)};
            break;
        }
        case Envelope::kGrepRequest: {
            auto v = ProtoBinding<GrepRequest>::get(env);
            m.payload = Request{std::move(v)};
            break;
        }
        case Envelope::kUpdateDocumentRequest: {
            auto v = ProtoBinding<UpdateDocumentRequest>::get(env);
            m.payload = Request{std::move(v)};
            break;
        }
        case Envelope::kDownloadRequest: {
            auto v = ProtoBinding<DownloadRequest>::get(env);
            m.payload = Request{std::move(v)};
            break;
        }
        case Envelope::kGetStatsRequest: {
            auto v = ProtoBinding<GetStatsRequest>::get(env);
            m.payload = Request{std::move(v)};
            break;
        }
        case Envelope::kPrepareSessionRequest: {
            auto v = ProtoBinding<PrepareSessionRequest>::get(env);
            m.payload = Request{std::move(v)};
            break;
        }
        case Envelope::kEmbedDocumentsRequest: {
            auto v = ProtoBinding<EmbedDocumentsRequest>::get(env);
            m.payload = Request{std::move(v)};
            break;
        }
        case Envelope::kCatRequest: {
            auto v = ProtoBinding<CatRequest>::get(env);
            m.payload = Request{std::move(v)};
            break;
        }
        case Envelope::kListSessionsRequest: {
            auto v = ProtoBinding<ListSessionsRequest>::get(env);
            m.payload = Request{std::move(v)};
            break;
        }
        case Envelope::kUseSessionRequest: {
            auto v = ProtoBinding<UseSessionRequest>::get(env);
            m.payload = Request{std::move(v)};
            break;
        }
        case Envelope::kAddPathSelectorRequest: {
            auto v = ProtoBinding<AddPathSelectorRequest>::get(env);
            m.payload = Request{std::move(v)};
            break;
        }
        case Envelope::kRemovePathSelectorRequest: {
            auto v = ProtoBinding<RemovePathSelectorRequest>::get(env);
            m.payload = Request{std::move(v)};
            break;
        }
        case Envelope::kPluginScanRequest: {
            auto v = ProtoBinding<PluginScanRequest>::get(env);
            m.payload = Request{std::move(v)};
            break;
        }
        case Envelope::kPluginLoadRequest: {
            auto v = ProtoBinding<PluginLoadRequest>::get(env);
            m.payload = Request{std::move(v)};
            break;
        }
        case Envelope::kPluginUnloadRequest: {
            auto v = ProtoBinding<PluginUnloadRequest>::get(env);
            m.payload = Request{std::move(v)};
            break;
        }
        case Envelope::kPluginTrustListRequest: {
            auto v = ProtoBinding<PluginTrustListRequest>::get(env);
            m.payload = Request{std::move(v)};
            break;
        }
        case Envelope::kPluginTrustAddRequest: {
            auto v = ProtoBinding<PluginTrustAddRequest>::get(env);
            m.payload = Request{std::move(v)};
            break;
        }
        case Envelope::kPluginTrustRemoveRequest: {
            auto v = ProtoBinding<PluginTrustRemoveRequest>::get(env);
            m.payload = Request{std::move(v)};
            break;
        }
        case Envelope::kFileHistoryRequest: {
            auto v = ProtoBinding<FileHistoryRequest>::get(env);
            m.payload = Request{std::move(v)};
            break;
        }

        // Additional responses
        case Envelope::kSuccessResponse: {
            auto v = ProtoBinding<SuccessResponse>::get(env);
            m.payload = Response{std::in_place_type<SuccessResponse>, std::move(v)};
            break;
        }
        case Envelope::kSearchResponse: {
            auto v = ProtoBinding<SearchResponse>::get(env);
            m.payload = Response{std::in_place_type<SearchResponse>, std::move(v)};
            break;
        }
        case Envelope::kAddResponse: {
            auto v = ProtoBinding<AddResponse>::get(env);
            m.payload = Response{std::in_place_type<AddResponse>, std::move(v)};
            break;
        }
        case Envelope::kGetInitResponse: {
            auto v = ProtoBinding<GetInitResponse>::get(env);
            m.payload = Response{std::in_place_type<GetInitResponse>, std::move(v)};
            break;
        }
        case Envelope::kGetChunkResponse: {
            auto v = ProtoBinding<GetChunkResponse>::get(env);
            m.payload = Response{std::in_place_type<GetChunkResponse>, std::move(v)};
            break;
        }
        case Envelope::kGetResponse: {
            auto v = ProtoBinding<GetResponse>::get(env);
            m.payload = Response{std::in_place_type<GetResponse>, std::move(v)};
            break;
        }
        case Envelope::kListResponse: {
            auto v = ProtoBinding<ListResponse>::get(env);
            m.payload = Response{std::in_place_type<ListResponse>, std::move(v)};
            break;
        }
        case Envelope::kStatusResponse: {
            auto v = ProtoBinding<StatusResponse>::get(env);
            m.payload = Response{std::in_place_type<StatusResponse>, std::move(v)};
            break;
        }
        case Envelope::kEmbeddingResponse: {
            auto v = ProtoBinding<EmbeddingResponse>::get(env);
            m.payload = Response{std::in_place_type<EmbeddingResponse>, std::move(v)};
            break;
        }
        case Envelope::kBatchEmbeddingResponse: {
            auto v = ProtoBinding<BatchEmbeddingResponse>::get(env);
            m.payload = Response{std::in_place_type<BatchEmbeddingResponse>, std::move(v)};
            break;
        }
        case Envelope::kModelLoadResponse: {
            auto v = ProtoBinding<ModelLoadResponse>::get(env);
            m.payload = Response{std::in_place_type<ModelLoadResponse>, std::move(v)};
            break;
        }
        case Envelope::kModelStatusResponse: {
            auto v = ProtoBinding<ModelStatusResponse>::get(env);
            m.payload = Response{std::in_place_type<ModelStatusResponse>, std::move(v)};
            break;
        }
        case Envelope::kAddDocumentResponse: {
            auto v = ProtoBinding<AddDocumentResponse>::get(env);
            m.payload = Response{std::in_place_type<AddDocumentResponse>, std::move(v)};
            break;
        }
        case Envelope::kGrepResponse: {
            auto v = ProtoBinding<GrepResponse>::get(env);
            m.payload = Response{std::in_place_type<GrepResponse>, std::move(v)};
            break;
        }
        case Envelope::kUpdateDocumentResponse: {
            auto v = ProtoBinding<UpdateDocumentResponse>::get(env);
            m.payload = Response{std::in_place_type<UpdateDocumentResponse>, std::move(v)};
            break;
        }
        case Envelope::kGetStatsResponse: {
            auto v = ProtoBinding<GetStatsResponse>::get(env);
            m.payload = Response{std::in_place_type<GetStatsResponse>, std::move(v)};
            break;
        }
        case Envelope::kDownloadResponse: {
            auto v = ProtoBinding<DownloadResponse>::get(env);
            m.payload = Response{std::in_place_type<DownloadResponse>, std::move(v)};
            break;
        }
        case Envelope::kDeleteResponse: {
            auto v = ProtoBinding<DeleteResponse>::get(env);
            m.payload = Response{std::in_place_type<DeleteResponse>, std::move(v)};
            break;
        }
        case Envelope::kPrepareSessionResponse: {
            auto v = ProtoBinding<PrepareSessionResponse>::get(env);
            m.payload = Response{std::in_place_type<PrepareSessionResponse>, std::move(v)};
            break;
        }
        case Envelope::kEmbedDocumentsResponse: {
            auto v = ProtoBinding<EmbedDocumentsResponse>::get(env);
            m.payload = Response{std::in_place_type<EmbedDocumentsResponse>, std::move(v)};
            break;
        }
        case Envelope::kPluginScanResponse: {
            auto v = ProtoBinding<PluginScanResponse>::get(env);
            m.payload = Response{std::in_place_type<PluginScanResponse>, std::move(v)};
            break;
        }
        case Envelope::kPluginLoadResponse: {
            auto v = ProtoBinding<PluginLoadResponse>::get(env);
            m.payload = Response{std::in_place_type<PluginLoadResponse>, std::move(v)};
            break;
        }
        case Envelope::kPluginTrustListResponse: {
            auto v = ProtoBinding<PluginTrustListResponse>::get(env);
            m.payload = Response{std::in_place_type<PluginTrustListResponse>, std::move(v)};
            break;
        }
        case Envelope::kCatResponse: {
            auto v = ProtoBinding<CatResponse>::get(env);
            m.payload = Response{std::in_place_type<CatResponse>, std::move(v)};
            break;
        }
        case Envelope::kListSessionsResponse: {
            auto v = ProtoBinding<ListSessionsResponse>::get(env);
            m.payload = Response{std::in_place_type<ListSessionsResponse>, std::move(v)};
            break;
        }
        case Envelope::kFileHistoryResponse: {
            auto v = ProtoBinding<FileHistoryResponse>::get(env);
            m.payload = Response{std::in_place_type<FileHistoryResponse>, std::move(v)};
            break;
        }
        case Envelope::kEmbedEvent: {
            auto v = ProtoBinding<EmbeddingEvent>::get(env);
            m.payload = Response{std::in_place_type<EmbeddingEvent>, std::move(v)};
            break;
        }
        case Envelope::kModelLoadEvent: {
            auto v = ProtoBinding<ModelLoadEvent>::get(env);
            m.payload = Response{std::in_place_type<ModelLoadEvent>, std::move(v)};
            break;
        }
        case Envelope::PAYLOAD_NOT_SET:
        default:
            return Error{ErrorCode::InvalidData, "Unsupported or empty Envelope payload"};
    }

    return m;
}

} // namespace daemon
} // namespace yams
