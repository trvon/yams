#include <yams/api/http/content_controller.h>
#include <yams/api/http/models.h>
#include <yams/api/content_store_error.h>
#include <yams/search/search_results.h>

#include <drogon/utils/Utilities.h>
#include <spdlog/spdlog.h>

#include <fstream>
#include <sstream>
#include <chrono>

namespace yams::api::http {

// Static member initialization
std::shared_ptr<AsyncContentStore> ContentController::store_;
std::shared_ptr<search::SearchExecutor> ContentController::searchExecutor_;

void ContentController::initController(std::shared_ptr<AsyncContentStore> store) {
    store_ = std::move(store);
    spdlog::info("ContentController initialized");
}

void ContentController::initSearchExecutor(std::shared_ptr<search::SearchExecutor> searchExecutor) {
    searchExecutor_ = std::move(searchExecutor);
    spdlog::info("ContentController search executor initialized");
}

// Store content from multipart upload
void ContentController::storeContent(const drogon::HttpRequestPtr& req,
                                   std::function<void(const drogon::HttpResponsePtr&)>&& callback) {
    
    // Check if multipart
    if (req->getContentType() != drogon::CT_MULTIPART_FORM_DATA) {
        callback(makeErrorResponse(drogon::k400BadRequest, 
            "Content-Type must be multipart/form-data"));
        return;
    }
    
    // Parse multipart data
    drogon::MultiPartParser parser;
    if (parser.parse(req) != 0) {
        callback(makeErrorResponse(drogon::k400BadRequest, "Invalid multipart data"));
        return;
    }
    
    // Get file part
    const auto& files = parser.getFiles();
    if (files.empty()) {
        callback(makeErrorResponse(drogon::k400BadRequest, "No file uploaded"));
        return;
    }
    
    const auto& file = files[0];
    
    // Extract metadata from form fields
    ContentMetadata metadata;
    metadata.originalName = file.getFileName();
    metadata.mimeType = file.getContentType();
    
    // Check for metadata JSON in form
    const auto& params = parser.getParameters();
    auto metaIt = params.find("metadata");
    if (metaIt != params.end()) {
        try {
            Json::Reader reader;
            Json::Value metaJson;
            if (reader.parse(metaIt->second, metaJson)) {
                metadata = jsonToContentMetadata(metaJson);
            }
        } catch (const std::exception& e) {
            spdlog::warn("Failed to parse metadata JSON: {}", e.what());
        }
    }
    
    // Create progress callback for SSE if requested
    ProgressCallback progress = nullptr;
    if (req->getHeader("X-Progress-Updates") == "true") {
        // TODO: Implement SSE progress updates
    }
    
    // Store file asynchronously
    store_->storeAsync(file.getFileLocation(), 
        [callback, metadata](const Result<StoreResult>& result) {
            if (result.has_value()) {
                Json::Value response;
                response["success"] = true;
                response["data"] = storeResultToJson(result.value());
                
                auto resp = drogon::HttpResponse::newHttpJsonResponse(response);
                resp->setStatusCode(drogon::k201Created);
                callback(resp);
            } else {
                auto error = contentStoreErrorToString(toContentStoreError(result.error()));
                callback(ContentController::makeErrorResponse(drogon::k500InternalServerError, error));
            }
        },
        metadata,
        progress
    );
}

// Retrieve content by hash
void ContentController::retrieveContent(const drogon::HttpRequestPtr& req,
                                      std::function<void(const drogon::HttpResponsePtr&)>&& callback,
                                      const std::string& hash) {
    
    // Check if client wants JSON metadata only
    if (req->getHeader("Accept") == "application/json") {
        store_->getMetadataAsync(hash,
            [callback, hash](const Result<ContentMetadata>& result) {
                if (result.has_value()) {
                    Json::Value response;
                    response["success"] = true;
                    response["data"]["hash"] = hash;
                    response["data"]["metadata"] = contentMetadataToJson(result.value());
                    callback(drogon::HttpResponse::newHttpJsonResponse(response));
                } else {
                    callback(ContentController::makeErrorResponse(drogon::k404NotFound, "Content not found"));
                }
            }
        );
        return;
    }
    
    // Retrieve actual content
    auto tempPath = std::filesystem::temp_directory_path() / 
                   ("kronos_http_" + hash + "_" + 
                    std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    
    store_->retrieveAsync(hash, tempPath,
        [callback, tempPath, hash](const Result<RetrieveResult>& result) {
            if (result.has_value() && result.value().found) {
                // Send file as response
                auto resp = drogon::HttpResponse::newFileResponse(
                    tempPath.string(),
                    "",  // Let Drogon determine filename
                    drogon::CT_NONE,
                    result.value().metadata.originalName
                );
                
                // Set content type if available
                if (!result.value().metadata.mimeType.empty()) {
                    resp->setContentTypeString(result.value().metadata.mimeType);
                }
                
                // Add metadata headers
                resp->addHeader("X-Content-Hash", hash);
                resp->addHeader("X-Content-Size", std::to_string(result.value().size));
                
                // Clean up temp file after sending
                resp->setExpiredTime(0);
                resp->setCloseConnection(false);
                
                callback(resp);
                
                // Schedule cleanup
                std::thread([tempPath]() {
                    std::this_thread::sleep_for(std::chrono::seconds(5));
                    std::filesystem::remove(tempPath);
                }).detach();
                
            } else {
                callback(ContentController::makeErrorResponse(drogon::k404NotFound, "Content not found"));
            }
        }
    );
}

// Delete content by hash
void ContentController::deleteContent(const drogon::HttpRequestPtr& req,
                                    std::function<void(const drogon::HttpResponsePtr&)>&& callback,
                                    const std::string& hash) {
    
    store_->removeAsync(hash,
        [callback, hash](const Result<bool>& result) {
            if (result.has_value()) {
                Json::Value response;
                response["success"] = true;
                response["data"]["deleted"] = result.value();
                response["data"]["hash"] = hash;
                
                auto status = result.value() ? drogon::k200OK : drogon::k404NotFound;
                callback(drogon::HttpResponse::newHttpJsonResponse(response, status));
            } else {
                auto error = contentStoreErrorToString(toContentStoreError(result.error()));
                callback(ContentController::makeErrorResponse(drogon::k500InternalServerError, error));
            }
        }
    );
}

// Check if content exists
void ContentController::checkExists(const drogon::HttpRequestPtr& req,
                                  std::function<void(const drogon::HttpResponsePtr&)>&& callback,
                                  const std::string& hash) {
    
    store_->existsAsync(hash,
        [callback](const Result<bool>& result) {
            if (result.has_value()) {
                auto resp = drogon::HttpResponse::newHttpResponse();
                resp->setStatusCode(result.value() ? drogon::k204NoContent : drogon::k404NotFound);
                callback(resp);
            } else {
                callback(ContentController::makeErrorResponse(drogon::k500InternalServerError, "Check failed"));
            }
        }
    );
}

// Get content metadata
void ContentController::getMetadata(const drogon::HttpRequestPtr& req,
                                  std::function<void(const drogon::HttpResponsePtr&)>&& callback,
                                  const std::string& hash) {
    
    store_->getMetadataAsync(hash,
        [callback](const Result<ContentMetadata>& result) {
            if (result.has_value()) {
                Json::Value response;
                response["success"] = true;
                response["data"] = contentMetadataToJson(result.value());
                callback(drogon::HttpResponse::newHttpJsonResponse(response));
            } else {
                callback(ContentController::makeErrorResponse(drogon::k404NotFound, "Metadata not found"));
            }
        }
    );
}

// Update content metadata
void ContentController::updateMetadata(const drogon::HttpRequestPtr& req,
                                     std::function<void(const drogon::HttpResponsePtr&)>&& callback,
                                     const std::string& hash) {
    
    // Parse JSON body
    auto json = req->getJsonObject();
    if (!json) {
        callback(makeErrorResponse(drogon::k400BadRequest, "Invalid JSON"));
        return;
    }
    
    ContentMetadata metadata = jsonToContentMetadata(*json);
    
    store_->updateMetadataAsync(hash, metadata,
        [callback](const Result<void>& result) {
            if (result.has_value()) {
                Json::Value response;
                response["success"] = true;
                response["message"] = "Metadata updated successfully";
                callback(drogon::HttpResponse::newHttpJsonResponse(response));
            } else {
                auto error = contentStoreErrorToString(toContentStoreError(result.error()));
                callback(ContentController::makeErrorResponse(drogon::k500InternalServerError, error));
            }
        }
    );
}

// Batch store operation
void ContentController::storeBatch(const drogon::HttpRequestPtr& req,
                                 std::function<void(const drogon::HttpResponsePtr&)>&& callback) {
    
    auto json = req->getJsonObject();
    if (!json) {
        callback(makeErrorResponse(drogon::k400BadRequest, "Invalid JSON"));
        return;
    }
    
    BatchStoreRequest batchReq = BatchStoreRequest::fromJson(*json);
    if (batchReq.items.empty()) {
        callback(makeErrorResponse(drogon::k400BadRequest, "No items to store"));
        return;
    }
    
    // Create temporary files for each item
    std::vector<std::filesystem::path> tempFiles;
    std::vector<ContentMetadata> metadataList;
    
    for (const auto& item : batchReq.items) {
        // Decode base64 content
        auto decoded = drogon::utils::base64Decode(item.base64Content);
        
        // Create temp file
        auto tempPath = std::filesystem::temp_directory_path() / 
                       ("batch_" + std::to_string(tempFiles.size()));
        
        std::ofstream file(tempPath, std::ios::binary);
        file.write(decoded.data(), decoded.size());
        file.close();
        
        tempFiles.push_back(tempPath);
        metadataList.push_back(item.metadata);
    }
    
    // Store batch
    auto future = store_->storeBatchAsync(tempFiles, metadataList);
    
    // Clean up temp files and send response
    std::thread([future = std::move(future), callback, tempFiles]() mutable {
        auto results = future.get();
        
        // Clean up temp files
        for (const auto& file : tempFiles) {
            std::filesystem::remove(file);
        }
        
        // Build response
        Json::Value response;
        response["success"] = true;
        Json::Value items(Json::arrayValue);
        
        for (const auto& result : results) {
            Json::Value item;
            if (result.has_value()) {
                item["success"] = true;
                item["data"] = storeResultToJson(result.value());
            } else {
                item["success"] = false;
                item["error"] = contentStoreErrorToString(toContentStoreError(result.error()));
            }
            items.append(item);
        }
        
        response["data"]["items"] = items;
        callback(drogon::HttpResponse::newHttpJsonResponse(response));
    }).detach();
}

// Batch delete operation
void ContentController::deleteBatch(const drogon::HttpRequestPtr& req,
                                  std::function<void(const drogon::HttpResponsePtr&)>&& callback) {
    
    auto json = req->getJsonObject();
    if (!json) {
        callback(makeErrorResponse(drogon::k400BadRequest, "Invalid JSON"));
        return;
    }
    
    BatchDeleteRequest batchReq = BatchDeleteRequest::fromJson(*json);
    if (batchReq.hashes.empty()) {
        callback(makeErrorResponse(drogon::k400BadRequest, "No hashes to delete"));
        return;
    }
    
    auto future = store_->removeBatchAsync(batchReq.hashes);
    
    std::thread([future = std::move(future), callback, hashes = batchReq.hashes]() mutable {
        auto results = future.get();
        
        Json::Value response;
        response["success"] = true;
        Json::Value items(Json::arrayValue);
        
        for (size_t i = 0; i < results.size(); ++i) {
            Json::Value item;
            item["hash"] = hashes[i];
            if (results[i].has_value()) {
                item["deleted"] = results[i].value();
            } else {
                item["deleted"] = false;
                item["error"] = contentStoreErrorToString(toContentStoreError(results[i].error()));
            }
            items.append(item);
        }
        
        response["data"]["items"] = items;
        callback(drogon::HttpResponse::newHttpJsonResponse(response));
    }).detach();
}

// Search content by metadata and full-text
void ContentController::searchContent(const drogon::HttpRequestPtr& req,
                                    std::function<void(const drogon::HttpResponsePtr&)>&& callback) {
    
    // Check if search executor is initialized
    if (!searchExecutor_) {
        callback(makeErrorResponse(drogon::k503ServiceUnavailable, 
            "Search service not available"));
        return;
    }
    
    auto startTime = std::chrono::high_resolution_clock::now();
    
    // Parse query parameters
    auto params = req->getParameters();
    
    // Extract search query
    std::string query;
    auto queryIt = params.find("q");
    if (queryIt != params.end()) {
        query = queryIt->second;
    }
    
    if (query.empty()) {
        queryIt = params.find("query");
        if (queryIt != params.end()) {
            query = queryIt->second;
        }
    }
    
    // Parse pagination parameters
    size_t offset = 0;
    size_t limit = 20;
    
    auto offsetIt = params.find("offset");
    if (offsetIt != params.end()) {
        try {
            offset = std::stoull(offsetIt->second);
        } catch (...) {
            offset = 0;
        }
    }
    
    auto limitIt = params.find("limit");
    if (limitIt != params.end()) {
        try {
            limit = std::stoull(limitIt->second);
            // Cap limit at 100 to prevent abuse
            limit = std::min(limit, size_t(100));
        } catch (...) {
            limit = 20;
        }
    }
    
    // Build search request
    search::SearchRequest searchReq;
    searchReq.query = query;
    searchReq.offset = offset;
    searchReq.limit = limit;
    
    // Parse filter parameters
    auto contentTypeIt = params.find("contentType");
    if (contentTypeIt != params.end()) {
        searchReq.filters.contentTypes.push_back(contentTypeIt->second);
    }
    
    auto languageIt = params.find("language");
    if (languageIt != params.end()) {
        searchReq.filters.languages.push_back(languageIt->second);
    }
    
    // Parse date range filters
    auto afterIt = params.find("createdAfter");
    if (afterIt != params.end()) {
        // Parse ISO date string (simplified for now)
        searchReq.filters.dateRangeStart = std::chrono::system_clock::now() - std::chrono::hours(24*30); // Last 30 days default
    }
    
    auto beforeIt = params.find("createdBefore");
    if (beforeIt != params.end()) {
        searchReq.filters.dateRangeEnd = std::chrono::system_clock::now();
    }
    
    // Parse options
    auto highlightIt = params.find("highlight");
    if (highlightIt != params.end() && highlightIt->second == "false") {
        searchReq.includeHighlights = false;
    }
    
    auto snippetsIt = params.find("snippets");
    if (snippetsIt != params.end() && snippetsIt->second == "false") {
        searchReq.includeSnippets = false;
    }
    
    // Execute search
    auto searchResult = searchExecutor_->search(searchReq);
    
    auto endTime = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
    
    if (!searchResult.has_value()) {
        spdlog::error("Search failed: {}", searchResult.error().message);
        callback(makeErrorResponse(drogon::k500InternalServerError, 
            "Search failed: " + searchResult.error().message));
        return;
    }
    
    const auto& searchResponse = searchResult.value();
    
    // Build JSON response
    Json::Value response;
    response["success"] = true;
    
    // Add search results
    Json::Value items(Json::arrayValue);
    for (const auto& item : searchResponse.results.getItems()) {
        Json::Value itemJson;
        itemJson["documentId"] = item.documentId;
        itemJson["title"] = item.title;
        itemJson["path"] = item.path;
        itemJson["contentType"] = item.contentType;
        itemJson["fileSize"] = static_cast<Json::UInt64>(item.fileSize);
        itemJson["relevanceScore"] = item.relevanceScore;
        
        // Add highlights if available
        if (!item.highlights.empty()) {
            Json::Value highlights(Json::arrayValue);
            for (const auto& highlight : item.highlights) {
                Json::Value highlightJson;
                highlightJson["field"] = highlight.field;
                highlightJson["snippet"] = highlight.snippet;
                highlights.append(highlightJson);
            }
            itemJson["highlights"] = highlights;
        }
        
        // Add content preview if available
        if (!item.contentPreview.empty()) {
            itemJson["preview"] = item.contentPreview;
        }
        
        // Add metadata
        if (!item.metadata.empty()) {
            Json::Value metadata;
            for (const auto& [key, value] : item.metadata) {
                metadata[key] = value;
            }
            itemJson["metadata"] = metadata;
        }
        
        items.append(itemJson);
    }
    
    response["data"]["items"] = items;
    response["data"]["total"] = searchResponse.totalResults;
    response["data"]["offset"] = searchResponse.offset;
    response["data"]["limit"] = searchResponse.limit;
    response["data"]["hasNextPage"] = searchResponse.hasNextPage;
    response["data"]["hasPreviousPage"] = searchResponse.hasPreviousPage;
    
    // Add query information
    response["query"]["original"] = searchResponse.originalQuery;
    response["query"]["processed"] = searchResponse.processedQuery;
    if (!searchResponse.suggestedQuery.empty()) {
        response["query"]["suggested"] = searchResponse.suggestedQuery;
    }
    response["query"]["wasRewritten"] = searchResponse.queryWasRewritten;
    
    // Add facets if available
    if (!searchResponse.results.getFacets().empty()) {
        Json::Value facets(Json::arrayValue);
        for (const auto& facet : searchResponse.results.getFacets()) {
            Json::Value facetJson;
            facetJson["name"] = facet.name;
            facetJson["displayName"] = facet.displayName;
            
            Json::Value values(Json::arrayValue);
            for (const auto& value : facet.values) {
                Json::Value valueJson;
                valueJson["value"] = value.value;
                valueJson["display"] = value.display;
                valueJson["count"] = static_cast<Json::UInt64>(value.count);
                valueJson["selected"] = value.selected;
                values.append(valueJson);
            }
            facetJson["values"] = values;
            
            facets.append(facetJson);
        }
        response["data"]["facets"] = facets;
    }
    
    // Add performance metrics
    response["performance"]["queryTime"] = static_cast<Json::Int64>(searchResponse.queryParseTime.count());
    response["performance"]["searchTime"] = static_cast<Json::Int64>(searchResponse.searchTime.count());
    response["performance"]["totalTime"] = static_cast<Json::Int64>(duration.count());
    
    // Check if response time is under 1 second (CoS requirement)
    if (duration.count() > 1000) {
        spdlog::warn("Search took {}ms, exceeding 1s target", duration.count());
    }
    
    callback(drogon::HttpResponse::newHttpJsonResponse(response));
}

// Get statistics
void ContentController::getStats(const drogon::HttpRequestPtr& req,
                               std::function<void(const drogon::HttpResponsePtr&)>&& callback) {
    
    auto stats = store_->getStore()->getStats();
    
    Json::Value response;
    response["success"] = true;
    response["data"] = contentStoreStatsToJson(stats);
    
    callback(drogon::HttpResponse::newHttpJsonResponse(response));
}

// Health check endpoint
void ContentController::healthCheck(const drogon::HttpRequestPtr& req,
                                  std::function<void(const drogon::HttpResponsePtr&)>&& callback) {
    
    auto health = store_->getStore()->checkHealth();
    
    Json::Value response;
    response["success"] = health.isHealthy;
    response["data"] = healthStatusToJson(health);
    
    auto status = health.isHealthy ? drogon::k200OK : drogon::k503ServiceUnavailable;
    callback(drogon::HttpResponse::newHttpJsonResponse(response, status));
}

// Helper methods
drogon::HttpResponsePtr ContentController::makeErrorResponse(
    drogon::HttpStatusCode status, const std::string& message) {
    
    ErrorResponse error;
    error.error = drogon::statusCodeToString(status);
    error.message = message;
    error.code = static_cast<int>(status);
    error.requestId = drogon::utils::getUuid();
    
    auto resp = drogon::HttpResponse::newHttpJsonResponse(error.toJson());
    resp->setStatusCode(status);
    return resp;
}

drogon::HttpResponsePtr ContentController::makeJsonResponse(
    const Json::Value& data, drogon::HttpStatusCode status) {
    
    Json::Value response;
    response["success"] = (status >= 200 && status < 300);
    response["data"] = data;
    
    return drogon::HttpResponse::newHttpJsonResponse(response, status);
}

} // namespace yams::api::http