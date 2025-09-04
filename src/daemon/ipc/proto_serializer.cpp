// Templated, trait-driven serializer to minimize if/else and ease extension
#include <yams/daemon/ipc/proto_serializer.h>

#include <yams/core/types.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/proto/ipc_envelope.pb.h>

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

// Map helpers for common structures
static void to_kv_pairs(const std::map<std::string, std::string>& in,
                        google::protobuf::RepeatedPtrField<pb::KvPair>* out) {
    out->Clear();
    for (const auto& [k, v] : in) {
        auto* kv = out->Add();
        kv->set_key(k);
        kv->set_value(v);
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
    out->Reserve(static_cast<int>(in.size()));
    for (const auto& s : in) {
        auto* elem = out->Add();
        elem->assign(s);
    }
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
        r.content = i.content();
        r.hasContent = !r.content.empty();
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
        return r;
    }
};

template <> struct ProtoBinding<AddRequest> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kAddRequest;
    static void set(Envelope& env, const AddRequest& r) {
        auto* o = env.mutable_add_request();
        o->set_path(r.path.string());
        set_string_list(r.tags, o->mutable_tags());
        to_kv_pairs(r.metadata, o->mutable_metadata());
        o->set_recursive(r.recursive);
        o->set_include_pattern(r.includePattern);
    }
    static AddRequest get(const Envelope& env) {
        const auto& i = env.add_request();
        AddRequest r{};
        r.path = i.path();
        r.tags = get_string_list(i.tags());
        r.metadata = from_kv_pairs(i.metadata());
        r.recursive = i.recursive();
        r.includePattern = i.include_pattern();
        return r;
    }
};

template <> struct ProtoBinding<GetRequest> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kGetRequest;
    static void set(Envelope& env, const GetRequest& r) {
        auto* o = env.mutable_get_request();
        o->set_hash(r.hash);
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

template <> struct ProtoBinding<LoadModelRequest> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kLoadModelRequest;
    static void set(Envelope& env, const LoadModelRequest& r) {
        auto* o = env.mutable_load_model_request();
        o->set_model_name(r.modelName);
        o->set_preload(r.preload);
    }
    static LoadModelRequest get(const Envelope& env) {
        const auto& i = env.load_model_request();
        LoadModelRequest r{};
        r.modelName = i.model_name();
        r.preload = i.preload();
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

template <> struct ProtoBinding<SearchResponse> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kSearchResponse;
    static void set(Envelope& env, const SearchResponse& r) {
        auto* o = env.mutable_search_response();
        o->set_total_count(static_cast<uint64_t>(r.totalCount));
        o->set_elapsed_ms(static_cast<int64_t>(r.elapsed.count()));
        for (const auto& s : r.results) {
            auto* pr = o->add_results();
            pr->set_id(s.id);
            pr->set_path(s.path);
            pr->set_title(s.title);
            pr->set_snippet(s.snippet);
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
        // DEPRECATION NOTICE: overallStatus/ready are legacy display hints only.
        // Lifecycle will be driven by DaemonLifecycleFsm and exposed via
        // StatusResponse.state/lastError. For now, map overallStatus to state string to preserve
        // behavior.
        env.mutable_status_response()->set_state(
            r.overallStatus.empty() ? (r.running ? "ready" : "stopped") : r.overallStatus);
        // TODO(PBI-007-06): When protocol adds lifecycle_state (enum) and lifecycle_last_error,
        // populate them from DaemonLifecycleFsm snapshot(); keep
        // ready/readinessStates/initProgress/overallStatus for compatibility.
    }
    static StatusResponse get(const Envelope& env) {
        StatusResponse r{};
        // Best-effort reconstruction from legacy 'state' string.
        r.overallStatus = env.status_response().state();
        r.running = (r.overallStatus != "stopped");
        r.ready = (r.overallStatus == "ready");
        r.uptimeSeconds = 0;
        r.requestsProcessed = 0;
        r.activeConnections = 0;
        r.memoryUsageMb = 0;
        r.cpuUsagePercent = 0;
        r.version = "";
        // TODO(lifecycle-fsm): read new fields (state enum, last_error, last_transition_time) when
        // available.
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
        // Best-effort: if any model present, emit first name and a simple status
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

template <> struct ProtoBinding<AddDocumentResponse> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kAddDocumentResponse;
    static void set(Envelope& env, const AddDocumentResponse& r) {
        auto* o = env.mutable_add_document_response();
        o->set_hash(r.hash);
        o->set_message(r.path);
    }
    static AddDocumentResponse get(const Envelope& env) {
        const auto& i = env.add_document_response();
        AddDocumentResponse r{};
        r.hash = i.hash();
        r.path = i.message();
        r.size = 0;
        r.documentsAdded = 0;
        return r;
    }
};

template <> struct ProtoBinding<GrepResponse> {
    static constexpr Envelope::PayloadCase case_v = Envelope::kGrepResponse;
    static void set(Envelope& env, const GrepResponse& r) {
        auto* o = env.mutable_grep_response();
        // Best-effort: only emit lines
        for (const auto& m : r.matches)
            o->add_lines(m.line);
    }
    static GrepResponse get(const Envelope& env) {
        const auto& i = env.grep_response();
        GrepResponse r{};
        r.matches.reserve(i.lines_size());
        for (const auto& line : i.lines()) {
            GrepMatch m{};
            m.line = line;
            r.matches.emplace_back(std::move(m));
        }
        r.totalMatches = r.matches.size();
        r.filesSearched = 0;
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
        // Best-effort: provide a minimal JSON string if additionalStats has a json key
        auto it = r.additionalStats.find("json");
        if (it != r.additionalStats.end())
            o->set_json(it->second);
    }
    static GetStatsResponse get(const Envelope& env) {
        GetStatsResponse r{};
        if (!env.get_stats_response().json().empty()) {
            r.additionalStats["json"] = env.get_stats_response().json();
        }
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

Result<std::vector<uint8_t>> ProtoSerializer::encode_payload(const Message& msg) {
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

    std::string out;
    if (!env.SerializeToString(&out)) {
        return Error{ErrorCode::SerializationError, "Failed to serialize protobuf Envelope"};
    }
    if (out.size() > MAX_MESSAGE_SIZE) {
        return Error{ErrorCode::InvalidData, "Serialized payload exceeds MAX_MESSAGE_SIZE"};
    }
    return std::vector<uint8_t>(out.begin(), out.end());
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
        case Envelope::kAddRequest: {
            auto v = ProtoBinding<AddRequest>::get(env);
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
        case Envelope::PAYLOAD_NOT_SET:
        default:
            return Error{ErrorCode::InvalidData, "Unsupported or empty Envelope payload"};
    }

    return m;
}

} // namespace daemon
} // namespace yams
