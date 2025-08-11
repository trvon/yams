# YAMS Search Troubleshooting Guide

This guide helps diagnose and resolve common issues with the YAMS search system.

## Table of Contents

1. [Quick Diagnostics](#quick-diagnostics)
2. [Search Query Issues](#search-query-issues)
3. [Performance Problems](#performance-problems)
4. [Indexing Issues](#indexing-issues)
5. [Database Problems](#database-problems)
6. [Cache Issues](#cache-issues)
7. [Memory Problems](#memory-problems)
8. [Configuration Issues](#configuration-issues)
9. [Logging and Debugging](#logging-and-debugging)
10. [Error Codes Reference](#error-codes-reference)

## Quick Diagnostics

### System Health Check

Run this quick diagnostic to identify common issues:

```cpp
class SystemHealthChecker {
public:
    struct HealthReport {
        bool databaseOK = false;
        bool indexOK = false;
        bool cacheOK = false;
        bool performanceOK = false;
        std::vector<std::string> issues;
        std::vector<std::string> recommendations;
    };
    
    HealthReport checkSystemHealth() {
        HealthReport report;
        
        // Check database connectivity
        report.databaseOK = checkDatabaseHealth();
        if (!report.databaseOK) {
            report.issues.push_back("Database connection failed");
            report.recommendations.push_back("Check database file permissions and path");
        }
        
        // Check index integrity
        report.indexOK = checkIndexIntegrity();
        if (!report.indexOK) {
            report.issues.push_back("Search index corrupted or missing");
            report.recommendations.push_back("Rebuild search index");
        }
        
        // Check cache functionality
        report.cacheOK = checkCacheHealth();
        if (!report.cacheOK) {
            report.issues.push_back("Cache not functioning properly");
            report.recommendations.push_back("Clear cache and restart");
        }
        
        // Check performance metrics
        report.performanceOK = checkPerformanceHealth();
        if (!report.performanceOK) {
            report.issues.push_back("Performance below acceptable thresholds");
            report.recommendations.push_back("Review performance tuning guide");
        }
        
        return report;
    }

private:
    bool checkDatabaseHealth() {
        try {
            auto db = std::make_shared<Database>("search.db");
            auto stmt = db->prepare("SELECT 1");
            auto result = stmt->execute();
            return result.isSuccess();
        } catch (...) {
            return false;
        }
    }
    
    bool checkIndexIntegrity() {
        try {
            auto db = std::make_shared<Database>("search.db");
            auto stmt = db->prepare("SELECT COUNT(*) FROM search_fts");
            auto result = stmt->execute();
            return result.isSuccess();
        } catch (...) {
            return false;
        }
    }
    
    bool checkCacheHealth() {
        // Test cache put/get operations
        return true;  // Simplified
    }
    
    bool checkPerformanceHealth() {
        // Run performance test
        return true;  // Simplified
    }
};
```

### Environment Validation

Validate your environment setup:

```cpp
class EnvironmentValidator {
public:
    struct ValidationResult {
        bool valid = true;
        std::vector<std::string> errors;
        std::vector<std::string> warnings;
    };
    
    ValidationResult validateEnvironment() {
        ValidationResult result;
        
        // Check required files
        if (!std::filesystem::exists("search.db")) {
            result.warnings.push_back("Database file not found - will be created");
        }
        
        // Check permissions
        if (!checkFilePermissions("search.db")) {
            result.errors.push_back("Insufficient permissions for database file");
            result.valid = false;
        }
        
        // Check disk space
        auto freeSpace = std::filesystem::space(".").free;
        if (freeSpace < 100 * 1024 * 1024) {  // 100MB
            result.errors.push_back("Insufficient disk space (less than 100MB available)");
            result.valid = false;
        }
        
        // Check memory
        if (getAvailableMemory() < 512 * 1024 * 1024) {  // 512MB
            result.warnings.push_back("Low available memory (less than 512MB)");
        }
        
        return result;
    }

private:
    bool checkFilePermissions(const std::string& path) {
        // Check read/write permissions
        return std::filesystem::exists(path) ? 
               (access(path.c_str(), R_OK | W_OK) == 0) : 
               (access(".", W_OK) == 0);  // Check directory write permission
    }
    
    size_t getAvailableMemory() {
        // Platform-specific implementation
        return 1024 * 1024 * 1024;  // Simplified: 1GB
    }
};
```

## Search Query Issues

### Problem: No Results Found

**Symptoms:**
- Search returns empty results for queries that should match
- `totalResults = 0` for known existing content

**Causes and Solutions:**

1. **Index not built or outdated**
   ```cpp
   // Check if index exists
   auto stmt = db->prepare("SELECT COUNT(*) FROM search_fts");
   auto result = stmt->execute();
   
   if (result.isSuccess() && result.getInt(0) == 0) {
       std::cout << "Index is empty - rebuild required" << std::endl;
       // Rebuild index
       indexer->rebuildIndex();
   }
   ```

2. **Query syntax errors**
   ```cpp
   QueryParser parser;
   if (!parser.validate(query)) {
       std::cout << "Query syntax error: " << parser.getLastError() << std::endl;
       // Suggest corrected query
       auto corrected = parser.suggestCorrection(query);
       std::cout << "Did you mean: " << corrected << std::endl;
   }
   ```

3. **Case sensitivity issues**
   ```cpp
   // Test with normalized query
   std::string normalizedQuery = toLowerCase(query);
   SearchRequest request;
   request.query = normalizedQuery;
   auto result = executor->search(request);
   ```

4. **Filters too restrictive**
   ```cpp
   // Test without filters
   SearchRequest testRequest = originalRequest;
   testRequest.filters.reset();
   auto result = executor->search(testRequest);
   
   if (result.isSuccess() && result.value().totalResults > 0) {
       std::cout << "Filters are too restrictive" << std::endl;
       // Review filter settings
   }
   ```

### Problem: Incorrect Results

**Symptoms:**
- Results don't match the query intent
- Irrelevant documents in results
- Missing expected documents

**Solutions:**

1. **Check query parsing**
   ```cpp
   auto parsed = parser.parse(query);
   if (parsed.isSuccess()) {
       std::cout << "Parsed query AST:" << std::endl;
       printAST(parsed.value().get());
   }
   ```

2. **Analyze result relevance**
   ```cpp
   void analyzeResults(const SearchResponse& response, const std::string& originalQuery) {
       std::cout << "Query: " << originalQuery << std::endl;
       std::cout << "Results: " << response.results.size() << std::endl;
       
       for (const auto& result : response.results) {
           std::cout << "Score: " << result.score 
                     << " Title: " << result.title << std::endl;
           
           // Show term frequencies
           for (const auto& [term, freq] : result.termFrequencies) {
               std::cout << "  Term '" << term << "': " << freq << " occurrences" << std::endl;
           }
       }
   }
   ```

3. **Adjust ranking parameters**
   ```cpp
   SearchConfig config;
   config.relevanceThreshold = 0.1;  // Lower threshold for more results
   config.enableQueryExpansion = true;  // Enable query expansion
   ```

### Problem: Special Characters in Queries

**Symptoms:**
- Queries with special characters fail or return unexpected results
- Parsing errors for queries with punctuation

**Solutions:**

```cpp
class QuerySanitizer {
public:
    std::string sanitizeQuery(const std::string& query) {
        std::string sanitized = query;
        
        // Escape special FTS5 characters
        const std::string specialChars = "\"*(){}[]+-:";
        for (char c : specialChars) {
            std::string from(1, c);
            std::string to = "\\" + from;
            replaceAll(sanitized, from, to);
        }
        
        return sanitized;
    }
    
    std::string normalizeWhitespace(const std::string& query) {
        std::regex multipleSpaces(R"(\s+)");
        std::string normalized = std::regex_replace(query, multipleSpaces, " ");
        
        // Trim leading/trailing spaces
        normalized.erase(0, normalized.find_first_not_of(" \t\n\r"));
        normalized.erase(normalized.find_last_not_of(" \t\n\r") + 1);
        
        return normalized;
    }

private:
    void replaceAll(std::string& str, const std::string& from, const std::string& to) {
        size_t pos = 0;
        while ((pos = str.find(from, pos)) != std::string::npos) {
            str.replace(pos, from.length(), to);
            pos += to.length();
        }
    }
};
```

## Performance Problems

### Problem: Slow Search Queries

**Symptoms:**
- Query response times > 100ms consistently
- Timeouts on complex queries
- High CPU usage during searches

**Diagnosis:**

```cpp
class PerformanceDiagnoser {
public:
    struct QueryProfile {
        std::chrono::milliseconds parseTime;
        std::chrono::milliseconds cacheCheckTime;
        std::chrono::milliseconds dbQueryTime;
        std::chrono::milliseconds rankingTime;
        std::chrono::milliseconds totalTime;
        bool cacheHit;
        size_t resultCount;
    };
    
    QueryProfile profileQuery(const SearchRequest& request) {
        QueryProfile profile;
        auto startTotal = std::chrono::high_resolution_clock::now();
        
        // Time query parsing
        auto parseStart = std::chrono::high_resolution_clock::now();
        auto parsed = parser_->parse(request.query);
        profile.parseTime = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::high_resolution_clock::now() - parseStart);
        
        // Time cache check
        auto cacheStart = std::chrono::high_resolution_clock::now();
        auto cacheKey = CacheKey::fromQuery(request.query, nullptr, request.offset, request.maxResults);
        auto cached = cache_->get(cacheKey);
        profile.cacheCheckTime = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::high_resolution_clock::now() - cacheStart);
        
        profile.cacheHit = cached.has_value();
        
        if (!profile.cacheHit) {
            // Time database query
            auto dbStart = std::chrono::high_resolution_clock::now();
            auto result = executor_->search(request);
            profile.dbQueryTime = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::high_resolution_clock::now() - dbStart);
            
            if (result.isSuccess()) {
                profile.resultCount = result.value().results.size();
            }
        }
        
        profile.totalTime = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::high_resolution_clock::now() - startTotal);
        
        return profile;
    }
    
    void printProfile(const QueryProfile& profile) {
        std::cout << "Query Performance Profile:" << std::endl
                  << "  Parse time: " << profile.parseTime.count() << "ms" << std::endl
                  << "  Cache check: " << profile.cacheCheckTime.count() << "ms" << std::endl
                  << "  Database query: " << profile.dbQueryTime.count() << "ms" << std::endl
                  << "  Total time: " << profile.totalTime.count() << "ms" << std::endl
                  << "  Cache hit: " << (profile.cacheHit ? "Yes" : "No") << std::endl
                  << "  Result count: " << profile.resultCount << std::endl;
    }

private:
    std::shared_ptr<QueryParser> parser_;
    std::shared_ptr<SearchCache> cache_;
    std::shared_ptr<SearchExecutor> executor_;
};
```

**Solutions:**

1. **Query optimization**
   ```cpp
   // Avoid leading wildcards
   std::string optimizeQuery(const std::string& query) {
       if (query.starts_with("*")) {
           std::cout << "Warning: Leading wildcard queries are slow" << std::endl;
           // Suggest alternative
           return query.substr(1) + "*";  // Convert *term to term*
       }
       return query;
   }
   ```

2. **Index optimization**
   ```cpp
   void optimizeIndex() {
       auto db = std::make_shared<Database>("search.db");
       
       // Rebuild FTS5 index
       auto stmt = db->prepare("INSERT INTO search_fts(search_fts) VALUES('rebuild')");
       stmt->execute();
       
       // Optimize FTS5 index
       stmt = db->prepare("INSERT INTO search_fts(search_fts) VALUES('optimize')");
       stmt->execute();
       
       std::cout << "Index optimization complete" << std::endl;
   }
   ```

3. **Cache configuration**
   ```cpp
   SearchCacheConfig improvedCacheConfig() {
       SearchCacheConfig config;
       config.maxEntries = 10000;  // Increase cache size
       config.maxMemoryMB = 1000;  // 1GB cache
       config.defaultTTL = std::chrono::minutes(15);  // Longer TTL
       return config;
   }
   ```

### Problem: High Memory Usage

**Symptoms:**
- Memory usage grows continuously
- Out of memory errors
- System becomes unresponsive

**Diagnosis and Solutions:**

```cpp
class MemoryLeakDetector {
public:
    void startMonitoring() {
        baselineMemory_ = getCurrentMemoryUsage();
        monitoringActive_ = true;
        
        monitoringThread_ = std::thread([this]() {
            while (monitoringActive_) {
                auto currentMemory = getCurrentMemoryUsage();
                auto growth = currentMemory - baselineMemory_;
                
                if (growth > 100 * 1024 * 1024) {  // 100MB growth
                    std::cout << "Memory growth detected: " 
                              << (growth / 1024 / 1024) << "MB" << std::endl;
                    
                    // Log top memory consumers
                    logMemoryUsage();
                    
                    // Force garbage collection if available
                    forceGC();
                }
                
                std::this_thread::sleep_for(std::chrono::seconds(60));
            }
        });
    }
    
    void logMemoryUsage() {
        auto cacheMemory = cache_->getMemoryUsage();
        auto dbMemory = database_->getMemoryUsage();
        
        std::cout << "Memory usage breakdown:" << std::endl
                  << "  Cache: " << (cacheMemory / 1024 / 1024) << "MB" << std::endl
                  << "  Database: " << (dbMemory / 1024 / 1024) << "MB" << std::endl;
    }

private:
    size_t getCurrentMemoryUsage() {
        // Platform-specific implementation
        return 0;  // Simplified
    }
    
    void forceGC() {
        // Clear expired cache entries
        cache_->evictExpired();
        
        // Compact database
        database_->vacuum();
    }
    
    size_t baselineMemory_ = 0;
    std::atomic<bool> monitoringActive_{false};
    std::thread monitoringThread_;
    std::shared_ptr<SearchCache> cache_;
    std::shared_ptr<Database> database_;
};
```

## Indexing Issues

### Problem: Documents Not Being Indexed

**Symptoms:**
- New documents don't appear in search results
- Index size doesn't grow with new documents
- Indexing process appears stalled

**Diagnosis:**

```cpp
class IndexingDiagnoser {
public:
    struct IndexingStatus {
        size_t totalDocuments;
        size_t indexedDocuments;
        size_t pendingDocuments;
        std::chrono::system_clock::time_point lastIndexTime;
        std::vector<std::string> indexingErrors;
        bool indexingActive;
    };
    
    IndexingStatus checkIndexingStatus() {
        IndexingStatus status;
        
        // Check total documents in storage
        status.totalDocuments = getTotalDocumentCount();
        
        // Check indexed documents
        status.indexedDocuments = getIndexedDocumentCount();
        
        status.pendingDocuments = status.totalDocuments - status.indexedDocuments;
        
        // Check last indexing time
        status.lastIndexTime = getLastIndexTime();
        
        // Check for errors
        status.indexingErrors = getRecentIndexingErrors();
        
        // Check if indexing is active
        status.indexingActive = isIndexingActive();
        
        return status;
    }
    
    void printIndexingStatus(const IndexingStatus& status) {
        std::cout << "Indexing Status:" << std::endl
                  << "  Total documents: " << status.totalDocuments << std::endl
                  << "  Indexed documents: " << status.indexedDocuments << std::endl
                  << "  Pending documents: " << status.pendingDocuments << std::endl
                  << "  Indexing active: " << (status.indexingActive ? "Yes" : "No") << std::endl;
        
        if (!status.indexingErrors.empty()) {
            std::cout << "Recent errors:" << std::endl;
            for (const auto& error : status.indexingErrors) {
                std::cout << "  " << error << std::endl;
            }
        }
    }

private:
    size_t getTotalDocumentCount() { return 0; }  // Implementation specific
    size_t getIndexedDocumentCount() { return 0; }
    std::chrono::system_clock::time_point getLastIndexTime() { return {}; }
    std::vector<std::string> getRecentIndexingErrors() { return {}; }
    bool isIndexingActive() { return false; }
};
```

**Solutions:**

1. **Restart indexing process**
   ```cpp
   void restartIndexing() {
       indexer_->stop();
       std::this_thread::sleep_for(std::chrono::seconds(1));
       indexer_->start();
       
       std::cout << "Indexing process restarted" << std::endl;
   }
   ```

2. **Clear indexing queue**
   ```cpp
   void clearIndexingQueue() {
       indexer_->clearQueue();
       indexer_->rebuildIndex();
       
       std::cout << "Indexing queue cleared and rebuild initiated" << std::endl;
   }
   ```

3. **Check file permissions**
   ```cpp
   void checkIndexingPermissions() {
       auto documentsPath = "/path/to/documents";
       
       if (!std::filesystem::exists(documentsPath)) {
           std::cout << "Documents path does not exist: " << documentsPath << std::endl;
           return;
       }
       
       // Check read permissions
       if (access(documentsPath.c_str(), R_OK) != 0) {
           std::cout << "No read permission for documents path" << std::endl;
           return;
       }
       
       std::cout << "Document path permissions OK" << std::endl;
   }
   ```

### Problem: Indexing Performance Issues

**Symptoms:**
- Indexing takes too long
- High CPU/memory usage during indexing
- Indexing process appears stuck

**Solutions:**

```cpp
void optimizeIndexingPerformance() {
    IndexingConfig config;
    
    // Increase batch size for better throughput
    config.batchSize = 2000;
    
    // Use more worker threads (but not too many)
    config.maxWorkerThreads = std::min(8u, std::thread::hardware_concurrency());
    
    // Enable incremental synchronization
    config.enableIncrementalSync = true;
    
    // Increase indexing interval for batch processing
    config.indexingInterval = std::chrono::seconds(30);
    
    indexer_ = std::make_shared<DocumentIndexer>(database_, config);
}
```

## Database Problems

### Problem: Database Corruption

**Symptoms:**
- SQLite error messages about database corruption
- Search queries fail with database errors
- Application crashes on database operations

**Solutions:**

```cpp
class DatabaseRecovery {
public:
    bool checkDatabaseIntegrity(const std::string& dbPath) {
        try {
            auto db = std::make_shared<Database>(dbPath);
            auto stmt = db->prepare("PRAGMA integrity_check");
            auto result = stmt->execute();
            
            if (result.isSuccess()) {
                auto integrity = result.getString(0);
                if (integrity == "ok") {
                    std::cout << "Database integrity OK" << std::endl;
                    return true;
                } else {
                    std::cout << "Database integrity issue: " << integrity << std::endl;
                    return false;
                }
            }
        } catch (const std::exception& e) {
            std::cout << "Database integrity check failed: " << e.what() << std::endl;
            return false;
        }
        return false;
    }
    
    bool repairDatabase(const std::string& dbPath) {
        try {
            // Create backup
            std::string backupPath = dbPath + ".backup";
            std::filesystem::copy_file(dbPath, backupPath);
            
            // Try to repair using SQLite's recovery mechanisms
            auto db = std::make_shared<Database>(dbPath);
            
            // Vacuum the database
            auto stmt = db->prepare("VACUUM");
            stmt->execute();
            
            // Reindex all indexes
            stmt = db->prepare("REINDEX");
            stmt->execute();
            
            // Check integrity again
            return checkDatabaseIntegrity(dbPath);
            
        } catch (const std::exception& e) {
            std::cout << "Database repair failed: " << e.what() << std::endl;
            return false;
        }
    }
    
    void restoreFromBackup(const std::string& dbPath) {
        std::string backupPath = dbPath + ".backup";
        
        if (std::filesystem::exists(backupPath)) {
            std::filesystem::copy_file(backupPath, dbPath, 
                std::filesystem::copy_options::overwrite_existing);
            std::cout << "Database restored from backup" << std::endl;
        } else {
            std::cout << "No backup file found" << std::endl;
        }
    }
};
```

### Problem: Database Lock Issues

**Symptoms:**
- "Database is locked" errors
- Timeouts on database operations
- Concurrent access failures

**Solutions:**

```cpp
void configureDatabaseForConcurrency() {
    DatabaseConfig config;
    
    // Enable WAL mode for better concurrency
    config.pragmas["journal_mode"] = "WAL";
    
    // Increase busy timeout
    config.pragmas["busy_timeout"] = "30000";  // 30 seconds
    
    // Increase connection pool size
    config.connectionPoolSize = 20;
    
    database_ = std::make_shared<Database>(config);
}

// Implement retry logic
template<typename F>
auto retryDatabaseOperation(F&& operation, int maxRetries = 3) {
    for (int attempt = 0; attempt < maxRetries; ++attempt) {
        try {
            return operation();
        } catch (const DatabaseLockException& e) {
            if (attempt == maxRetries - 1) {
                throw;  // Re-throw on final attempt
            }
            
            // Exponential backoff
            auto delay = std::chrono::milliseconds(100 * (1 << attempt));
            std::this_thread::sleep_for(delay);
        }
    }
}
```

## Cache Issues

### Problem: Cache Not Working

**Symptoms:**
- Cache hit rate is 0%
- All queries go to database
- No performance improvement from caching

**Diagnosis:**

```cpp
void diagnoseCacheIssues() {
    auto stats = cache_->getStats();
    
    std::cout << "Cache Statistics:" << std::endl
              << "  Total requests: " << stats.totalRequests << std::endl
              << "  Cache hits: " << stats.hits << std::endl
              << "  Cache misses: " << stats.misses << std::endl
              << "  Hit rate: " << (stats.hitRate * 100) << "%" << std::endl
              << "  Current entries: " << stats.currentEntries << std::endl
              << "  Memory usage: " << stats.memoryUsage << " bytes" << std::endl;
    
    if (stats.hitRate < 0.1) {
        std::cout << "Low cache hit rate - investigating..." << std::endl;
        
        // Check TTL settings
        if (cacheConfig_.defaultTTL.count() < 60) {
            std::cout << "TTL too short: " << cacheConfig_.defaultTTL.count() << "s" << std::endl;
        }
        
        // Check cache size
        if (stats.currentEntries < 10) {
            std::cout << "Cache has very few entries" << std::endl;
        }
    }
}
```

**Solutions:**

```cpp
void fixCacheConfiguration() {
    SearchCacheConfig newConfig;
    
    // Increase TTL
    newConfig.defaultTTL = std::chrono::minutes(10);
    
    // Increase cache size
    newConfig.maxEntries = 5000;
    newConfig.maxMemoryMB = 500;
    
    // Enable statistics
    newConfig.enableStatistics = true;
    
    cache_ = std::make_shared<SearchCache>(newConfig);
    
    std::cout << "Cache configuration updated" << std::endl;
}
```

## Error Codes Reference

### Search Error Codes

| Code | Name | Description | Solution |
|------|------|-------------|----------|
| 1001 | ParseError | Query parsing failed | Check query syntax |
| 1002 | DatabaseError | Database operation failed | Check database connectivity |
| 1003 | IndexError | Search index error | Rebuild search index |
| 1004 | CacheError | Cache operation failed | Clear cache and restart |
| 1005 | TimeoutError | Operation timed out | Increase timeout or optimize query |
| 1006 | MemoryError | Out of memory | Reduce cache size or add more RAM |
| 1007 | ConfigError | Configuration invalid | Review configuration settings |

### Error Handling Example

```cpp
void handleSearchError(const SearchError& error) {
    switch (error.code) {
        case ErrorCode::ParseError:
            std::cout << "Query syntax error: " << error.message << std::endl;
            std::cout << "Suggestion: Check query syntax documentation" << std::endl;
            break;
            
        case ErrorCode::DatabaseError:
            std::cout << "Database error: " << error.message << std::endl;
            std::cout << "Suggestion: Check database connectivity and permissions" << std::endl;
            // Attempt database recovery
            if (error.message.find("corrupt") != std::string::npos) {
                DatabaseRecovery recovery;
                recovery.repairDatabase("search.db");
            }
            break;
            
        case ErrorCode::TimeoutError:
            std::cout << "Query timeout: " << error.message << std::endl;
            std::cout << "Suggestion: Simplify query or increase timeout" << std::endl;
            break;
            
        default:
            std::cout << "Unknown error: " << error.message << std::endl;
            break;
    }
}
```

## Logging and Debugging

### Enable Debug Logging

```cpp
class SearchLogger {
public:
    enum LogLevel {
        DEBUG = 0,
        INFO = 1,
        WARN = 2,
        ERROR = 3
    };
    
    void setLogLevel(LogLevel level) {
        logLevel_ = level;
    }
    
    void debug(const std::string& message) {
        if (logLevel_ <= DEBUG) {
            log("DEBUG", message);
        }
    }
    
    void info(const std::string& message) {
        if (logLevel_ <= INFO) {
            log("INFO", message);
        }
    }
    
    void warn(const std::string& message) {
        if (logLevel_ <= WARN) {
            log("WARN", message);
        }
    }
    
    void error(const std::string& message) {
        if (logLevel_ <= ERROR) {
            log("ERROR", message);
        }
    }

private:
    void log(const std::string& level, const std::string& message) {
        auto now = std::chrono::system_clock::now();
        auto time_t = std::chrono::system_clock::to_time_t(now);
        
        std::cout << std::put_time(std::localtime(&time_t), "%Y-%m-%d %H:%M:%S")
                  << " [" << level << "] " << message << std::endl;
    }
    
    LogLevel logLevel_ = INFO;
};

// Global logger instance
SearchLogger logger;

// Usage in search operations
void executeSearch(const SearchRequest& request) {
    logger.debug("Executing search query: " + request.query);
    
    try {
        auto result = searchExecutor_->search(request);
        
        if (result.isSuccess()) {
            logger.info("Search completed successfully, " + 
                       std::to_string(result.value().totalResults) + " results found");
        } else {
            logger.error("Search failed: " + result.error().message);
        }
    } catch (const std::exception& e) {
        logger.error("Search exception: " + std::string(e.what()));
    }
}
```

### Debugging Tools

```cpp
class SearchDebugger {
public:
    void dumpQueryAST(const std::string& query) {
        auto parsed = parser_->parse(query);
        if (parsed.isSuccess()) {
            std::cout << "Query AST for '" << query << "':" << std::endl;
            printAST(parsed.value().get(), 0);
        } else {
            std::cout << "Failed to parse query: " << parsed.error().message << std::endl;
        }
    }
    
    void dumpCacheContents() {
        auto stats = cache_->getStats();
        std::cout << "Cache contents:" << std::endl;
        std::cout << "  Entries: " << stats.currentEntries << std::endl;
        std::cout << "  Memory: " << stats.memoryUsage << " bytes" << std::endl;
        
        // Dump cache keys (if debugging interface available)
        // for (const auto& key : cache_->getKeys()) {
        //     std::cout << "  Key: " << key.toString() << std::endl;
        // }
    }

private:
    void printAST(QueryNode* node, int depth) {
        std::string indent(depth * 2, ' ');
        
        switch (node->type) {
            case QueryNodeType::TERM:
                std::cout << indent << "TERM: " << node->value << std::endl;
                break;
            case QueryNodeType::AND:
                std::cout << indent << "AND" << std::endl;
                for (auto& child : node->children) {
                    printAST(child.get(), depth + 1);
                }
                break;
            case QueryNodeType::OR:
                std::cout << indent << "OR" << std::endl;
                for (auto& child : node->children) {
                    printAST(child.get(), depth + 1);
                }
                break;
            // ... other node types
        }
    }
    
    std::shared_ptr<QueryParser> parser_;
    std::shared_ptr<SearchCache> cache_;
};
```

This troubleshooting guide provides comprehensive solutions for common issues with the YAMS search system. Use the diagnostic tools and systematic approach to quickly identify and resolve problems.