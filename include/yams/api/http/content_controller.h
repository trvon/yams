#pragma once

#include <drogon/HttpController.h>
#include <yams/api/content_store.h>
#include <yams/api/async_content_store.h>
#include <yams/search/search_executor.h>
#include <yams/metadata/database.h>
#include <yams/metadata/metadata_repository.h>

#include <memory>

namespace yams::api::http {

/**
 * REST API controller for content operations
 * 
 * Endpoints:
 * - POST   /api/v1/content             - Store content
 * - GET    /api/v1/content/{hash}      - Retrieve content
 * - DELETE /api/v1/content/{hash}      - Delete content
 * - HEAD   /api/v1/content/{hash}      - Check if content exists
 * - GET    /api/v1/content/{hash}/metadata - Get metadata
 * - PUT    /api/v1/content/{hash}/metadata - Update metadata
 * - GET    /api/v1/content/stats       - Get statistics
 * - GET    /api/v1/health              - Health check
 */
class ContentController : public drogon::HttpController<ContentController> {
public:
    static constexpr bool isAutoCreation = false;
    
    METHOD_LIST_BEGIN
    
    // Content operations
    METHOD_ADD(ContentController::storeContent, "/api/v1/content", drogon::Post);
    METHOD_ADD(ContentController::retrieveContent, "/api/v1/content/{hash}", drogon::Get);
    METHOD_ADD(ContentController::deleteContent, "/api/v1/content/{hash}", drogon::Delete);
    METHOD_ADD(ContentController::checkExists, "/api/v1/content/{hash}", drogon::Head);
    
    // Metadata operations
    METHOD_ADD(ContentController::getMetadata, "/api/v1/content/{hash}/metadata", drogon::Get);
    METHOD_ADD(ContentController::updateMetadata, "/api/v1/content/{hash}/metadata", drogon::Put);
    
    // Batch operations
    METHOD_ADD(ContentController::storeBatch, "/api/v1/content/batch", drogon::Post);
    METHOD_ADD(ContentController::deleteBatch, "/api/v1/content/batch", drogon::Delete);
    
    // Query and stats
    METHOD_ADD(ContentController::searchContent, "/api/v1/content/search", drogon::Get);
    METHOD_ADD(ContentController::getStats, "/api/v1/content/stats", drogon::Get);
    
    // Health check
    METHOD_ADD(ContentController::healthCheck, "/api/v1/health", drogon::Get);
    
    METHOD_LIST_END
    
    // Initialize with content store and search components
    static void initController(std::shared_ptr<AsyncContentStore> store);
    static void initSearchExecutor(std::shared_ptr<search::SearchExecutor> searchExecutor);
    
    // Store content from multipart upload
    void storeContent(const drogon::HttpRequestPtr& req,
                     std::function<void(const drogon::HttpResponsePtr&)>&& callback);
    
    // Retrieve content by hash
    void retrieveContent(const drogon::HttpRequestPtr& req,
                        std::function<void(const drogon::HttpResponsePtr&)>&& callback,
                        const std::string& hash);
    
    // Delete content by hash
    void deleteContent(const drogon::HttpRequestPtr& req,
                      std::function<void(const drogon::HttpResponsePtr&)>&& callback,
                      const std::string& hash);
    
    // Check if content exists
    void checkExists(const drogon::HttpRequestPtr& req,
                    std::function<void(const drogon::HttpResponsePtr&)>&& callback,
                    const std::string& hash);
    
    // Get content metadata
    void getMetadata(const drogon::HttpRequestPtr& req,
                    std::function<void(const drogon::HttpResponsePtr&)>&& callback,
                    const std::string& hash);
    
    // Update content metadata
    void updateMetadata(const drogon::HttpRequestPtr& req,
                       std::function<void(const drogon::HttpResponsePtr&)>&& callback,
                       const std::string& hash);
    
    // Batch store operation
    void storeBatch(const drogon::HttpRequestPtr& req,
                   std::function<void(const drogon::HttpResponsePtr&)>&& callback);
    
    // Batch delete operation
    void deleteBatch(const drogon::HttpRequestPtr& req,
                    std::function<void(const drogon::HttpResponsePtr&)>&& callback);
    
    // Search content by metadata
    void searchContent(const drogon::HttpRequestPtr& req,
                      std::function<void(const drogon::HttpResponsePtr&)>&& callback);
    
    // Get statistics
    void getStats(const drogon::HttpRequestPtr& req,
                 std::function<void(const drogon::HttpResponsePtr&)>&& callback);
    
    // Health check endpoint
    void healthCheck(const drogon::HttpRequestPtr& req,
                    std::function<void(const drogon::HttpResponsePtr&)>&& callback);
    
private:
    static std::shared_ptr<AsyncContentStore> store_;
    static std::shared_ptr<search::SearchExecutor> searchExecutor_;
    
    // Helper methods
    drogon::HttpResponsePtr makeErrorResponse(drogon::HttpStatusCode status, 
                                            const std::string& message);
    
    drogon::HttpResponsePtr makeJsonResponse(const Json::Value& data,
                                           drogon::HttpStatusCode status = drogon::k200OK);
    
    Json::Value metadataToJson(const ContentMetadata& metadata);
    ContentMetadata jsonToMetadata(const Json::Value& json);
    
    // Progress callback that sends SSE updates
    void createProgressCallback(const drogon::HttpResponsePtr& resp,
                              std::function<void(uint64_t, uint64_t)>& callback);
};

} // namespace yams::api::http