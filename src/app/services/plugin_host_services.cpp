
// --- Grep Service: grep ---

static yams::app::services::GrepRequest to_cpp_grep_request(const yams_grep_request_t* c_req) {
    yams::app::services::GrepRequest cpp_req;
    if (!c_req)
        return cpp_req;
    if (c_req->pattern)
        cpp_req.pattern = c_req->pattern;
    for (size_t i = 0; i < c_req->paths.count; ++i) {
        if (c_req->paths.strings[i]) {
            cpp_req.paths.push_back(c_req->paths.strings[i]);
        }
    }
    cpp_req.ignoreCase = c_req->ignore_case;
    cpp_req.word = c_req->word;
    cpp_req.invert = c_req->invert;
    return cpp_req;
}

static yams_grep_response_t*
from_cpp_grep_response(const yams::app::services::GrepResponse& cpp_res) {
    auto* c_res = (yams_grep_response_t*)malloc(sizeof(yams_grep_response_t));
    if (!c_res)
        return nullptr;
    memset(c_res, 0, sizeof(yams_grep_response_t));

    c_res->count = cpp_res.results.size();
    if (!cpp_res.results.empty()) {
        c_res->results =
            (yams_grep_file_result_t*)malloc(sizeof(yams_grep_file_result_t) * c_res->count);
        if (c_res->results) {
            memset(c_res->results, 0, sizeof(yams_grep_file_result_t) * c_res->count);
            for (size_t i = 0; i < c_res->count; ++i) {
                const auto& cpp_file_res = cpp_res.results[i];
                auto& c_file_res = c_res->results[i];
                c_file_res.file = c_string_from_cpp(cpp_file_res.file);
                c_file_res.match_count = cpp_file_res.matches.size();
                if (!cpp_file_res.matches.empty()) {
                    c_file_res.matches = (yams_grep_match_t*)malloc(sizeof(yams_grep_match_t) *
                                                                    c_file_res.match_count);
                    if (c_file_res.matches) {
                        memset(c_file_res.matches, 0,
                               sizeof(yams_grep_match_t) * c_file_res.match_count);
                        for (size_t j = 0; j < c_file_res.match_count; ++j) {
                            const auto& cpp_match = cpp_file_res.matches[j];
                            auto& c_match = c_file_res.matches[j];
                            c_match.line = c_string_from_cpp(cpp_match.line);
                            c_match.line_number = cpp_match.lineNumber;
                        }
                    }
                }
            }
        }
    }
    return c_res;
}

static void free_grep_response(yams_grep_response_t* res) {
    if (!res)
        return;
    if (res->results) {
        for (size_t i = 0; i < res->count; ++i) {
            auto& c_file_res = res->results[i];
            free(c_file_res.file);
            if (c_file_res.matches) {
                for (size_t j = 0; j < c_file_res.match_count; ++j) {
                    free(c_file_res.matches[j].line);
                }
                free(c_file_res.matches);
            }
        }
        free(res->results);
    }
    free(res);
}

static int grep_service_grep(void* handle, const yams_grep_request_t* req,
                             yams_grep_response_t** res) {
    if (!handle || !req || !res)
        return YAMS_PLUGIN_ERR_INVALID;
    auto service = static_cast<yams::app::services::IGrepService*>(handle);
    auto cpp_req = to_cpp_grep_request(req);
    auto cpp_result = service->grep(cpp_req);
    if (cpp_result) {
        *res = from_cpp_grep_response(cpp_result.value());
        return YAMS_PLUGIN_OK;
    }
    return YAMS_PLUGIN_ERR_INTERNAL;
}

static yams_grep_service_v1 g_grep_service_vtable = {
    .handle = nullptr, .grep = grep_service_grep, .free_grep_response = free_grep_response};
_cpp_store_request(req);
auto cpp_result = service->store(cpp_req);
if (cpp_result) {
    *res = from_cpp_store_response(cpp_result.value());
    return YAMS_PLUGIN_OK;
}
return YAMS_PLUGIN_ERR_INTERNAL;
}

// --- Document Service: retrieve ---

static yams::app::services::RetrieveDocumentRequest
to_cpp_retrieve_request(const yams_retrieve_document_request_t* c_req) {
    yams::app::services::RetrieveDocumentRequest cpp_req;
    if (!c_req)
        return cpp_req;
    if (c_req->hash)
        cpp_req.hash = c_req->hash;
    if (c_req->name)
        cpp_req.name = c_req->name;
    if (c_req->output_path)
        cpp_req.outputPath = c_req->output_path;
    cpp_req.includeContent = c_req->include_content;
    cpp_req.graph = c_req->graph;
    cpp_req.depth = c_req->depth;
    return cpp_req;
}

static yams_retrieve_document_response_t*
from_cpp_retrieve_response(const yams::app::services::RetrieveDocumentResponse& cpp_res) {
    auto* c_res =
        (yams_retrieve_document_response_t*)malloc(sizeof(yams_retrieve_document_response_t));
    if (!c_res)
        return nullptr;
    memset(c_res, 0, sizeof(yams_retrieve_document_response_t));

    if (cpp_res.document) {
        c_res->document = (yams_retrieved_document_t*)malloc(sizeof(yams_retrieved_document_t));
        if (c_res->document) {
            memset(c_res->document, 0, sizeof(yams_retrieved_document_t));
            const auto& cpp_doc = *cpp_res.document;
            c_res->document->hash = c_string_from_cpp(cpp_doc.hash);
            c_res->document->path = c_string_from_cpp(cpp_doc.path);
            c_res->document->name = c_string_from_cpp(cpp_doc.name);
            c_res->document->size = cpp_doc.size;
            c_res->document->mime_type = c_string_from_cpp(cpp_doc.mimeType);
            if (cpp_doc.content) {
                c_res->document->content = c_string_from_cpp(*cpp_doc.content);
            }
        }
    }

    if (!cpp_res.related.empty()) {
        c_res->related_count = cpp_res.related.size();
        c_res->related = (yams_related_document_t*)malloc(sizeof(yams_related_document_t) *
                                                          c_res->related_count);
        if (c_res->related) {
            memset(c_res->related, 0, sizeof(yams_related_document_t) * c_res->related_count);
            for (size_t i = 0; i < c_res->related_count; ++i) {
                const auto& cpp_rel = cpp_res.related[i];
                auto& c_rel = c_res->related[i];
                c_rel.hash = c_string_from_cpp(cpp_rel.hash);
                c_rel.path = c_string_from_cpp(cpp_rel.path);
                c_rel.distance = cpp_rel.distance;
            }
        }
    }

    return c_res;
}

static void free_retrieved_document(yams_retrieved_document_t* doc) {
    if (!doc)
        return;
    free(doc->hash);
    free(doc->path);
    free(doc->name);
    free(doc->mime_type);
    free(doc->content);
    free(doc);
}

static void free_retrieve_document_response(yams_retrieve_document_response_t* res) {
    if (!res)
        return;
    if (res->document) {
        free_retrieved_document(res->document);
    }
    if (res->related) {
        for (size_t i = 0; i < res->related_count; ++i) {
            free(res->related[i].hash);
            free(res->related[i].path);
        }
        free(res->related);
    }
    free(res);
}

static int document_service_retrieve(void* handle, const yams_retrieve_document_request_t* req,
                                     yams_retrieve_document_response_t** res) {
    if (!handle || !req || !res)
        return YAMS_PLUGIN_ERR_INVALID;
    auto service = static_cast<yams::app::services::IDocumentService*>(handle);
    auto cpp_req = to_cpp_retrieve_request(req);
    auto cpp_result = service->retrieve(cpp_req);
    if (cpp_result) {
        *res = from_cpp_retrieve_response(cpp_result.value());
        return YAMS_PLUGIN_OK;
    }
    return YAMS_PLUGIN_ERR_INTERNAL;
}

// --- Document Service: list ---

static yams::app::services::ListDocumentsRequest
to_cpp_list_request(const yams_list_documents_request_t* c_req) {
    yams::app::services::ListDocumentsRequest cpp_req;
    if (!c_req)
        return cpp_req;

    cpp_req.limit = c_req->limit;
    cpp_req.offset = c_req->offset;
    if (c_req->pattern)
        cpp_req.pattern = c_req->pattern;
    if (c_req->sort_by)
        cpp_req.sortBy = c_req->sort_by;
    if (c_req->sort_order)
        cpp_req.sortOrder = c_req->sort_order;
    cpp_req.pathsOnly = c_req->paths_only;

    if (c_req->tags.strings) {
        for (size_t i = 0; i < c_req->tags.count; ++i) {
            if (c_req->tags.strings[i]) {
                cpp_req.tags.push_back(c_req->tags.strings[i]);
            }
        }
    }
    cpp_req.matchAllTags = c_req->match_all_tags;

    return cpp_req;
}

static yams_list_documents_response_t*
from_cpp_list_response(const yams::app::services::ListDocumentsResponse& cpp_res) {
    auto* c_res = (yams_list_documents_response_t*)malloc(sizeof(yams_list_documents_response_t));
    if (!c_res)
        return nullptr;
    memset(c_res, 0, sizeof(yams_list_documents_response_t));

    c_res->count = cpp_res.documents.size();
    c_res->total_found = cpp_res.totalFound;

    if (!cpp_res.documents.empty()) {
        c_res->documents =
            (yams_document_entry_t*)malloc(sizeof(yams_document_entry_t) * c_res->count);
        if (c_res->documents) {
            memset(c_res->documents, 0, sizeof(yams_document_entry_t) * c_res->count);
            for (size_t i = 0; i < c_res->count; ++i) {
                const auto& cpp_doc = cpp_res.documents[i];
                auto& c_doc = c_res->documents[i];
                c_doc.name = c_string_from_cpp(cpp_doc.name);
                c_doc.hash = c_string_from_cpp(cpp_doc.hash);
                c_doc.path = c_string_from_cpp(cpp_doc.path);
                c_doc.size = cpp_doc.size;
                c_doc.mime_type = c_string_from_cpp(cpp_doc.mimeType);
                c_doc.indexed = cpp_doc.indexed;
                if (cpp_doc.snippet.has_value()) {
                    c_doc.snippet = c_string_from_cpp(*cpp_doc.snippet);
                }
                c_doc.tags = c_string_list_from_cpp(cpp_doc.tags);
            }
        }
    }

    if (!cpp_res.paths.empty()) {
        c_res->paths_count = cpp_res.paths.size();
        c_res->paths = (char**)malloc(sizeof(char*) * c_res->paths_count);
        if (c_res->paths) {
            for (size_t i = 0; i < c_res->paths_count; ++i) {
                c_res->paths[i] = c_string_from_cpp(cpp_res.paths[i]);
            }
        }
    }

    return c_res;
}

static void free_list_documents_response(yams_list_documents_response_t* res) {
    if (!res)
        return;
    if (res->documents) {
        for (size_t i = 0; i < res->count; ++i) {
            auto& c_doc = res->documents[i];
            free(c_doc.name);
            free(c_doc.hash);
            free(c_doc.path);
            free(c_doc.mime_type);
            free(c_doc.snippet);
            free_c_string_list(&c_doc.tags);
        }
        free(res->documents);
    }
    if (res->paths) {
        for (size_t i = 0; i < res->paths_count; ++i) {
            free(res->paths[i]);
        }
        free(res->paths);
    }
    free(res);
}

static int document_service_list(void* handle, const yams_list_documents_request_t* req,
                                 yams_list_documents_response_t** res) {
    if (!handle || !req || !res) {
        return YAMS_PLUGIN_ERR_INVALID;
    }
    auto service = static_cast<yams::app::services::IDocumentService*>(handle);
    auto cpp_req = to_cpp_list_request(req);

    yams::Result<yams::app::services::ListDocumentsResponse> cpp_result = service->list(cpp_req);

    if (cpp_result) {
        *res = from_cpp_list_response(cpp_result.value());
        return YAMS_PLUGIN_OK;
    } else {
        // TODO: Map error codes
        return YAMS_PLUGIN_ERR_INTERNAL;
    }
}

// --- Document Service: cat ---

static yams::app::services::CatDocumentRequest
to_cpp_cat_request(const yams_cat_document_request_t* c_req) {
    yams::app::services::CatDocumentRequest cpp_req;
    if (!c_req)
        return cpp_req;
    if (c_req->hash)
        cpp_req.hash = c_req->hash;
    if (c_req->name)
        cpp_req.name = c_req->name;
    return cpp_req;
}

static yams_cat_document_response_t*
from_cpp_cat_response(const yams::app::services::CatDocumentResponse& cpp_res) {
    auto* c_res = (yams_cat_document_response_t*)malloc(sizeof(yams_cat_document_response_t));
    if (!c_res)
        return nullptr;
    memset(c_res, 0, sizeof(yams_cat_document_response_t));
    c_res->content = (char*)malloc(cpp_res.content.size() + 1);
    if (c_res->content) {
        memcpy(c_res->content, cpp_res.content.c_str(), cpp_res.content.size() + 1);
        c_res->size = cpp_res.size;
    }
    c_res->hash = c_string_from_cpp(cpp_res.hash);
    c_res->name = c_string_from_cpp(cpp_res.name);
    return c_res;
}

static void free_cat_document_response(yams_cat_document_response_t* res) {
    if (!res)
        return;
    free(res->content);
    free(res->hash);
    free(res->name);
    free(res);
}

static int document_service_cat(void* handle, const yams_cat_document_request_t* req,
                                yams_cat_document_response_t** res) {
    if (!handle || !req || !res)
        return YAMS_PLUGIN_ERR_INVALID;
    auto service = static_cast<yams::app::services::IDocumentService*>(handle);
    auto cpp_req = to_cpp_cat_request(req);
    auto cpp_result = service->cat(cpp_req);
    if (cpp_result) {
        *res = from_cpp_cat_response(cpp_result.value());
        return YAMS_PLUGIN_OK;
    }
    return YAMS_PLUGIN_ERR_INTERNAL;
}

// --- Search Service: search ---

static yams::app::services::SearchRequest
to_cpp_search_request(const yams_search_request_t* c_req) {
    yams::app::services::SearchRequest cpp_req;
    if (!c_req)
        return cpp_req;
    if (c_req->query)
        cpp_req.query = c_req->query;
    cpp_req.limit = c_req->limit;
    cpp_req.fuzzy = c_req->fuzzy;
    cpp_req.similarity = c_req->similarity;
    if (c_req->hash)
        cpp_req.hash = c_req->hash;
    if (c_req->type)
        cpp_req.type = c_req->type;
    cpp_req.pathsOnly = c_req->paths_only;
    return cpp_req;
}

static yams_search_response_t*
from_cpp_search_response(const yams::app::services::SearchResponse& cpp_res) {
    auto* c_res = (yams_search_response_t*)malloc(sizeof(yams_search_response_t));
    if (!c_res)
        return nullptr;
    memset(c_res, 0, sizeof(yams_search_response_t));

    c_res->total = cpp_res.total;
    c_res->type = c_string_from_cpp(cpp_res.type);

    if (!cpp_res.results.empty()) {
        c_res->results_count = cpp_res.results.size();
        c_res->results =
            (yams_search_item_t*)malloc(sizeof(yams_search_item_t) * c_res->results_count);
        if (c_res->results) {
            memset(c_res->results, 0, sizeof(yams_search_item_t) * c_res->results_count);
            for (size_t i = 0; i < c_res->results_count; ++i) {
                const auto& cpp_item = cpp_res.results[i];
                auto& c_item = c_res->results[i];
                c_item.hash = c_string_from_cpp(cpp_item.hash);
                c_item.title = c_string_from_cpp(cpp_item.title);
                c_item.path = c_string_from_cpp(cpp_item.path);
                c_item.score = cpp_item.score;
                c_item.snippet = c_string_from_cpp(cpp_item.snippet);
            }
        }
    }

    if (!cpp_res.paths.empty()) {
        c_res->paths_count = cpp_res.paths.size();
        c_res->paths = (char**)malloc(sizeof(char*) * c_res->paths_count);
        if (c_res->paths) {
            for (size_t i = 0; i < c_res->paths_count; ++i) {
                c_res->paths[i] = c_string_from_cpp(cpp_res.paths[i]);
            }
        }
    }

    return c_res;
}

static void free_search_response(yams_search_response_t* res) {
    if (!res)
        return;
    free(res->type);
    if (res->results) {
        for (size_t i = 0; i < res->results_count; ++i) {
            auto& c_item = res->results[i];
            free(c_item.hash);
            free(c_item.title);
            free(c_item.path);
            free(c_item.snippet);
        }
        free(res->results);
    }
    if (res->paths) {
        for (size_t i = 0; i < res->paths_count; ++i) {
            free(res->paths[i]);
        }
        free(res->paths);
    }
    free(res);
}

static boost::asio::any_io_executor g_executor = boost::asio::system_executor{};

static int search_service_search(void* handle, const yams_search_request_t* req,
                                 yams_search_callback_t callback, void* user_data) {
    if (!handle || !req || !callback)
        return YAMS_PLUGIN_ERR_INVALID;
    auto service = static_cast<yams::app::services::ISearchService*>(handle);
    auto cpp_req = to_cpp_search_request(req);

    boost::asio::co_spawn(
        g_executor,
        [service, cpp_req, callback, user_data]() -> boost::asio::awaitable<void> {
            auto cpp_result = co_await service->search(cpp_req);
            yams_search_response_t* c_res = nullptr;
            if (cpp_result) {
                c_res = from_cpp_search_response(cpp_result.value());
            }
            callback(c_res, user_data);
        },
        boost::asio::detached);

    return YAMS_PLUGIN_OK;
}

// --- V-table and context creation ---

static yams_document_service_v1 g_doc_service_vtable = {
    .handle = nullptr, // will be set to the service instance
    .store = document_service_store,
    .retrieve = document_service_retrieve,
    .list = document_service_list,
    .cat = document_service_cat,
    .free_store_document_response = free_store_document_response,
    .free_retrieve_document_response = free_retrieve_document_response,
    .free_list_documents_response = free_list_documents_response,
    .free_cat_document_response = free_cat_document_response};

static yams_search_service_v1 g_search_service_vtable = {.handle = nullptr,
                                                         .search = search_service_search,
                                                         .free_search_response =
                                                             free_search_response};

struct HostContextImpl {
    std::shared_ptr<yams::app::services::IDocumentService> doc_service;
    std::shared_ptr<yams::app::services::ISearchService> search_service;
    std::shared_ptr<yams::app::services::IGrepService> grep_service;
};

yams_plugin_host_context_v1*
yams_create_host_context(const yams::app::services::AppContext& app_ctx) {
    auto* host_ctx = (yams_plugin_host_context_v1*)malloc(sizeof(yams_plugin_host_context_v1));
    if (!host_ctx)
        return nullptr;

    auto* impl = new HostContextImpl();
    impl->doc_service = yams::app::services::makeDocumentService(app_ctx);
    impl->search_service = yams::app::services::makeSearchService(app_ctx);
    impl->grep_service = yams::app::services::makeGrepService(app_ctx);

    host_ctx->version = YAMS_PLUGIN_HOST_SERVICES_API_VERSION;
    host_ctx->impl = impl;
    host_ctx->document_service = &g_doc_service_vtable;
    host_ctx->search_service = &g_search_service_vtable;
    host_ctx->grep_service = &g_grep_service_vtable;

    g_doc_service_vtable.handle = impl->doc_service.get();
    g_search_service_vtable.handle = impl->search_service.get();
    g_grep_service_vtable.handle = impl->grep_service.get();

    return host_ctx;
}

void yams_free_host_context(void* host_ctx) {
    if (!host_ctx)
        return;
    auto* ctx = (yams_plugin_host_context_v1*)host_ctx;
    auto* impl = (HostContextImpl*)ctx->impl;
    delete impl;
    free(ctx);
}
andle is only valid as long as the service
    // instance created here exists. The plugin host/loader will need to own
    // the service instances and this context.

    return host_ctx;
}

void yams_free_host_context(void* host_ctx) {
    if (!host_ctx)
        return;
    // In a real implementation, this would need to free the services as well.
    // For now, we just free the context struct itself.
    free(host_ctx);
}