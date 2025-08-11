#pragma once

#include <yams/api/metadata_request.h>
#include <yams/api/metadata_response.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/indexing/document_indexer.h>
#include <memory>
#include <functional>
#include <future>

namespace yams::api {

/**
 * @brief Configuration for metadata API
 */
struct MetadataApiConfig {
    size_t maxBulkOperations = 1000;    // Maximum items in bulk operation
    size_t maxQueryResults = 10000;     // Maximum query results
    size_t defaultQueryLimit = 100;     // Default query page size
    
    bool enableVersioning = true;       // Track version history
    bool enableAuditLog = true;         // Log all operations
    bool enableValidation = true;       // Validate all inputs
    bool enableAsync = true;            // Support async operations
    
    std::chrono::milliseconds operationTimeout{30000};
    std::chrono::milliseconds exportTimeout{300000};  // 5 minutes for exports
    std::chrono::milliseconds importTimeout{600000};  // 10 minutes for imports
    
    // Rate limiting
    size_t maxRequestsPerMinute = 1000;
    size_t maxConcurrentRequests = 100;
};

/**
 * @brief Callback types for async operations
 */
using CreateCallback = std::function<void(const CreateMetadataResponse&)>;
using UpdateCallback = std::function<void(const UpdateMetadataResponse&)>;
using DeleteCallback = std::function<void(const DeleteMetadataResponse&)>;
using QueryCallback = std::function<void(const QueryMetadataResponse&)>;
using ExportCallback = std::function<void(const ExportMetadataResponse&)>;
using ImportCallback = std::function<void(const ImportMetadataResponse&)>;

/**
 * @brief Main metadata API class
 */
class MetadataApi {
public:
    /**
     * @brief Constructor
     */
    MetadataApi(std::shared_ptr<metadata::MetadataRepository> repository,
                std::shared_ptr<indexing::IDocumentIndexer> indexer = nullptr,
                const MetadataApiConfig& config = {});
    
    virtual ~MetadataApi() = default;
    
    // ===== CRUD Operations =====
    
    /**
     * @brief Create new document metadata
     */
    CreateMetadataResponse createMetadata(const CreateMetadataRequest& request);
    std::future<CreateMetadataResponse> createMetadataAsync(const CreateMetadataRequest& request);
    
    /**
     * @brief Get document metadata
     */
    GetMetadataResponse getMetadata(const GetMetadataRequest& request);
    std::future<GetMetadataResponse> getMetadataAsync(const GetMetadataRequest& request);
    
    /**
     * @brief Update document metadata
     */
    UpdateMetadataResponse updateMetadata(const UpdateMetadataRequest& request);
    std::future<UpdateMetadataResponse> updateMetadataAsync(const UpdateMetadataRequest& request);
    
    /**
     * @brief Delete document metadata
     */
    DeleteMetadataResponse deleteMetadata(const DeleteMetadataRequest& request);
    std::future<DeleteMetadataResponse> deleteMetadataAsync(const DeleteMetadataRequest& request);
    
    // ===== Bulk Operations =====
    
    /**
     * @brief Bulk create documents
     */
    BulkCreateResponse bulkCreate(const BulkCreateRequest& request);
    std::future<BulkCreateResponse> bulkCreateAsync(const BulkCreateRequest& request);
    
    /**
     * @brief Bulk update documents
     */
    BulkUpdateResponse bulkUpdate(const BulkUpdateRequest& request);
    std::future<BulkUpdateResponse> bulkUpdateAsync(const BulkUpdateRequest& request);
    
    /**
     * @brief Bulk delete documents
     */
    BulkDeleteResponse bulkDelete(const BulkDeleteRequest& request);
    std::future<BulkDeleteResponse> bulkDeleteAsync(const BulkDeleteRequest& request);
    
    // ===== Query Operations =====
    
    /**
     * @brief Query metadata with filters
     */
    QueryMetadataResponse queryMetadata(const QueryMetadataRequest& request);
    std::future<QueryMetadataResponse> queryMetadataAsync(const QueryMetadataRequest& request);
    
    // ===== Import/Export Operations =====
    
    /**
     * @brief Export metadata to file
     */
    ExportMetadataResponse exportMetadata(const ExportMetadataRequest& request);
    std::future<ExportMetadataResponse> exportMetadataAsync(const ExportMetadataRequest& request);
    
    /**
     * @brief Import metadata from file
     */
    ImportMetadataResponse importMetadata(const ImportMetadataRequest& request);
    std::future<ImportMetadataResponse> importMetadataAsync(const ImportMetadataRequest& request);
    
    // ===== Utility Operations =====
    
    /**
     * @brief Get metadata statistics
     */
    GetStatisticsResponse getStatistics(const GetStatisticsRequest& request);
    
    /**
     * @brief Validate metadata
     */
    ValidateMetadataResponse validateMetadata(const ValidateMetadataRequest& request);
    
    /**
     * @brief Get document history
     */
    GetHistoryResponse getHistory(const GetHistoryRequest& request);
    
    // ===== Batch Operations =====
    
    /**
     * @brief Execute multiple operations in a batch
     */
    template<typename... Requests>
    BatchMetadataResponse batch(Requests&&... requests);
    
    // ===== Configuration =====
    
    /**
     * @brief Set API configuration
     */
    void setConfig(const MetadataApiConfig& config) { config_ = config; }
    
    /**
     * @brief Get current configuration
     */
    const MetadataApiConfig& getConfig() const { return config_; }
    
    // ===== Health and Monitoring =====
    
    /**
     * @brief API statistics
     */
    struct ApiStatistics {
        size_t totalRequests = 0;
        size_t successfulRequests = 0;
        size_t failedRequests = 0;
        std::chrono::milliseconds avgResponseTime{0};
        std::chrono::milliseconds maxResponseTime{0};
        
        std::unordered_map<std::string, size_t> requestsByType;
        std::unordered_map<ErrorCode, size_t> errorCounts;
    };
    
    ApiStatistics getStatistics() const { return stats_; }
    void resetStatistics() { stats_ = ApiStatistics{}; }
    
    /**
     * @brief Check API health
     */
    bool isHealthy() const;
    
private:
    std::shared_ptr<metadata::MetadataRepository> repository_;
    std::shared_ptr<indexing::IDocumentIndexer> indexer_;
    MetadataApiConfig config_;
    mutable ApiStatistics stats_;
    
    // Internal helper methods
    
    /**
     * @brief Validate request
     */
    template<typename Request>
    bool validateRequest(const Request& request, MetadataResponse& response);
    
    /**
     * @brief Apply metadata filter to query
     */
    std::vector<metadata::DocumentMetadata> applyFilter(
        const std::vector<metadata::DocumentMetadata>& documents,
        const MetadataFilter& filter);
    
    /**
     * @brief Sort documents
     */
    void sortDocuments(std::vector<metadata::DocumentMetadata>& documents,
                      QueryMetadataRequest::SortField sortBy,
                      bool ascending);
    
    /**
     * @brief Export to JSON
     */
    Result<void> exportToJson(const std::vector<metadata::DocumentMetadata>& documents,
                              const std::string& path,
                              bool compress);
    
    /**
     * @brief Export to CSV
     */
    Result<void> exportToCsv(const std::vector<metadata::DocumentMetadata>& documents,
                            const std::string& path,
                            bool compress);
    
    /**
     * @brief Import from JSON
     */
    Result<std::vector<metadata::DocumentMetadata>> importFromJson(const std::string& path);
    
    /**
     * @brief Import from CSV
     */
    Result<std::vector<metadata::DocumentMetadata>> importFromCsv(const std::string& path);
    
    /**
     * @brief Validate metadata fields
     */
    std::vector<ValidateMetadataResponse::ValidationError> validateFields(
        const metadata::DocumentMetadata& metadata);
    
    /**
     * @brief Update statistics
     */
    void updateStatistics(const std::string& operationType,
                         bool success,
                         std::chrono::milliseconds duration);
    
    /**
     * @brief Log operation for audit
     */
    void logOperation(const std::string& operation,
                      const std::string& details,
                      bool success);
    
    /**
     * @brief Check rate limits
     */
    bool checkRateLimit(const std::string& clientId);
    
    /**
     * @brief Generate unique request ID
     */
    std::string generateRequestId();
    
    /**
     * @brief Measure operation time
     */
    template<typename Func>
    auto measureTime(Func&& func, std::chrono::milliseconds& duration);
};

/**
 * @brief Factory for creating metadata API instances
 */
class MetadataApiFactory {
public:
    /**
     * @brief Create default metadata API
     */
    static std::unique_ptr<MetadataApi> create(
        std::shared_ptr<metadata::MetadataRepository> repository);
    
    /**
     * @brief Create metadata API with indexer
     */
    static std::unique_ptr<MetadataApi> create(
        std::shared_ptr<metadata::MetadataRepository> repository,
        std::shared_ptr<indexing::IDocumentIndexer> indexer);
    
    /**
     * @brief Create metadata API with custom config
     */
    static std::unique_ptr<MetadataApi> create(
        std::shared_ptr<metadata::MetadataRepository> repository,
        std::shared_ptr<indexing::IDocumentIndexer> indexer,
        const MetadataApiConfig& config);
};

} // namespace yams::api