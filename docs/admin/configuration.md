# YAMS Search Configuration Guide

This document provides comprehensive configuration options for the YAMS search system.

## Table of Contents

1. [Overview](#overview)
2. [Basic Configuration](#basic-configuration)
3. [Database Configuration](#database-configuration)
4. [Search Engine Settings](#search-engine-settings)
5. [Cache Configuration](#cache-configuration)
6. [Indexing Configuration](#indexing-configuration)
7. [Performance Settings](#performance-settings)
8. [Security Configuration](#security-configuration)
9. [Deployment Options](#deployment-options)
10. [Environment Variables](#environment-variables)

## Overview

The YAMS search system can be configured through various configuration objects, environment variables, and runtime parameters. This guide covers all available options and best practices for different deployment scenarios.

## Basic Configuration

### Minimal Setup

```cpp
#include <yams/search/search_executor.h>
#include <yams/database/database.h>

// Basic configuration
auto database = std::make_shared<Database>("search_index.db");
auto repository = std::make_shared<MetadataRepository>(database);
auto indexer = std::make_shared<DocumentIndexer>(database);
auto parser = std::make_shared<QueryParser>();

SearchExecutor executor(database, repository, indexer, parser);
```

### Configuration Files

You can use JSON configuration files for complex setups:

```json
{
  "database": {
    "path": "/var/lib/yams/search.db",
    "pragmas": {
      "journal_mode": "WAL",
      "synchronous": "NORMAL",
      "cache_size": "10000",
      "temp_store": "MEMORY"
    }
  },
  "search": {
    "maxResults": 1000,
    "defaultLimit": 20,
    "timeoutMs": 30000,
    "enableHighlighting": true,
    "enableSpellCorrection": false,
    "enableFaceting": true
  },
  "cache": {
    "maxEntries": 10000,
    "maxMemoryMB": 1000,
    "defaultTTLSeconds": 300,
    "evictionPolicy": "LRU",
    "enableStatistics": true
  },
  "indexing": {
    "batchSize": 1000,
    "maxWorkerThreads": 4,
    "enableIncrementalSync": true,
    "indexingIntervalMs": 5000
  }
}
```

Loading configuration:

```cpp
#include <nlohmann/json.hpp>
#include <fstream>

// Load configuration from file
std::ifstream configFile("config.json");
nlohmann::json config;
configFile >> config;

// Apply database configuration
DatabaseConfig dbConfig;
dbConfig.path = config["database"]["path"];
dbConfig.pragmas = config["database"]["pragmas"];

// Apply search configuration
SearchConfig searchConfig;
searchConfig.maxResults = config["search"]["maxResults"];
searchConfig.defaultLimit = config["search"]["defaultLimit"];
searchConfig.timeout = std::chrono::milliseconds(config["search"]["timeoutMs"]);
searchConfig.enableHighlighting = config["search"]["enableHighlighting"];
```

## Database Configuration

### SQLite Configuration

The search system uses SQLite with FTS5 for full-text indexing. Key configuration options:

```cpp
struct DatabaseConfig {
    std::string path = "search.db";
    std::unordered_map<std::string, std::string> pragmas;
    size_t connectionPoolSize = 10;
    std::chrono::seconds connectionTimeout{30};
    bool enableWAL = true;
    size_t cacheSize = 10000;  // Number of pages
};

// Configure database
DatabaseConfig config;
config.path = "/var/lib/yams/search.db";
config.pragmas = {
    {"journal_mode", "WAL"},
    {"synchronous", "NORMAL"},
    {"cache_size", "10000"},
    {"temp_store", "MEMORY"},
    {"mmap_size", "268435456"},  // 256MB
    {"page_size", "4096"}
};

auto database = std::make_shared<Database>(config);
```

### Connection Pooling

For high-concurrency scenarios:

```cpp
DatabaseConfig config;
config.connectionPoolSize = 20;  // Max concurrent connections
config.connectionTimeout = std::chrono::seconds(60);

// Connection pool automatically manages SQLite connections
```

### Database Pragmas

Important SQLite pragmas for search performance:

| Pragma | Value | Description |
|--------|--------|-------------|
| `journal_mode` | `WAL` | Write-Ahead Logging for better concurrency |
| `synchronous` | `NORMAL` | Balance durability and performance |
| `cache_size` | `10000` | Number of pages to cache in memory |
| `temp_store` | `MEMORY` | Store temporary data in memory |
| `mmap_size` | `268435456` | Memory-mapped I/O size (256MB) |
| `optimize` | - | Optimize FTS5 indexes periodically |

## Search Engine Settings

### Search Configuration

```cpp
struct SearchConfig {
    size_t maxResults = 1000;
    size_t defaultLimit = 20;
    std::chrono::milliseconds timeout{30000};
    bool enableHighlighting = true;
    bool enableSpellCorrection = false;
    bool enableFaceting = true;
    bool enableQueryExpansion = false;
    float relevanceThreshold = 0.0f;
    size_t maxSnippetLength = 200;
    std::string highlightPrefix = "<mark>";
    std::string highlightSuffix = "</mark>";
};

SearchConfig config;
config.maxResults = 5000;           // Maximum results to return
config.defaultLimit = 50;           // Default page size
config.timeout = std::chrono::seconds(60);  // Query timeout
config.enableHighlighting = true;    // Enable result highlighting
config.enableFaceting = true;        // Enable faceted search
config.maxSnippetLength = 300;       // Maximum snippet length
```

### Query Parser Configuration

```cpp
struct QueryParserConfig {
    bool enableFuzzyMatching = true;
    size_t maxFuzzyDistance = 2;
    bool enableWildcards = true;
    bool enableProximityQueries = true;
    bool enableRangeQueries = true;
    size_t maxQueryTerms = 100;
    size_t maxQueryDepth = 10;
    bool caseSensitive = false;
};

QueryParserConfig parserConfig;
parserConfig.enableFuzzyMatching = true;
parserConfig.maxFuzzyDistance = 2;    // Edit distance for fuzzy matching
parserConfig.maxQueryTerms = 50;      // Limit query complexity
```

## Cache Configuration

### Search Result Cache

```cpp
struct SearchCacheConfig {
    size_t maxEntries = 1000;
    size_t maxMemoryMB = 100;
    std::chrono::seconds defaultTTL{300};
    EvictionPolicy evictionPolicy = EvictionPolicy::LRU;
    bool enableStatistics = true;
    bool persistentCache = false;
    std::string persistentCachePath;
    double memoryPressureThreshold = 0.8;
};

SearchCacheConfig cacheConfig;
cacheConfig.maxEntries = 10000;      // Maximum cached entries
cacheConfig.maxMemoryMB = 1000;      // Memory limit in MB
cacheConfig.defaultTTL = std::chrono::minutes(10);  // 10-minute TTL
cacheConfig.evictionPolicy = EvictionPolicy::LRU;
cacheConfig.enableStatistics = true; // Track hit/miss rates
```

### Cache Policies

#### LRU (Least Recently Used)
Best for general-purpose caching with mixed access patterns:

```cpp
cacheConfig.evictionPolicy = EvictionPolicy::LRU;
```

#### LFU (Least Frequently Used)
Better for workloads with clear popularity patterns:

```cpp
cacheConfig.evictionPolicy = EvictionPolicy::LFU;
```

#### TTL-Based Eviction
Combine with time-based eviction:

```cpp
cacheConfig.defaultTTL = std::chrono::minutes(15);
cacheConfig.memoryPressureThreshold = 0.75;  // Evict early under pressure
```

### Persistent Cache

Enable persistent caching for faster startup:

```cpp
cacheConfig.persistentCache = true;
cacheConfig.persistentCachePath = "/var/cache/yams/search_cache.db";
```

## Indexing Configuration

### Document Indexing

```cpp
struct IndexingConfig {
    size_t batchSize = 1000;
    size_t maxWorkerThreads = 4;
    bool enableIncrementalSync = true;
    std::chrono::milliseconds indexingInterval{5000};
    std::vector<std::string> supportedExtensions;
    size_t maxDocumentSize = 10 * 1024 * 1024;  // 10MB
    bool enableContentExtraction = true;
    bool enableLanguageDetection = true;
    bool enableMetadataExtraction = true;
};

IndexingConfig indexConfig;
indexConfig.batchSize = 2000;         // Documents per batch
indexConfig.maxWorkerThreads = 8;     // Parallel indexing threads
indexConfig.enableIncrementalSync = true;  // Only index changed docs
indexConfig.indexingInterval = std::chrono::seconds(10);

// Supported file types
indexConfig.supportedExtensions = {
    ".txt", ".md", ".cpp", ".h", ".py", ".js", ".html",
    ".pdf", ".doc", ".docx", ".xlsx", ".pptx"
};
```

### Content Extraction

```cpp
struct ContentExtractionConfig {
    std::unordered_map<std::string, std::string> extractorPaths;
    size_t maxExtractionTimeMs = 30000;
    size_t maxExtractedContentSize = 5 * 1024 * 1024;  // 5MB
    bool enableOCR = false;
    std::string ocrLanguage = "eng";
    bool preserveFormatting = false;
};

ContentExtractionConfig extractConfig;
extractConfig.maxExtractionTimeMs = 60000;  // 1-minute timeout
extractConfig.maxExtractedContentSize = 10 * 1024 * 1024;  // 10MB
extractConfig.enableOCR = true;         // Enable OCR for images/PDFs
```

## Performance Settings

### Thread Configuration

```cpp
struct ThreadConfig {
    size_t searchThreads = 4;
    size_t indexingThreads = 2;
    size_t ioThreads = 2;
    bool enableThreadPoolOptimization = true;
    size_t threadStackSize = 1024 * 1024;  // 1MB
};

ThreadConfig threadConfig;
threadConfig.searchThreads = std::thread::hardware_concurrency();
threadConfig.indexingThreads = std::max(2u, std::thread::hardware_concurrency() / 2);
```

### Memory Management

```cpp
struct MemoryConfig {
    size_t maxTotalMemoryMB = 2048;     // 2GB total limit
    size_t maxCacheMemoryMB = 1024;     // 1GB for caches
    size_t maxIndexMemoryMB = 512;      // 512MB for indexing
    double memoryWarningThreshold = 0.8;
    double memoryCriticalThreshold = 0.95;
    bool enableMemoryMapping = true;
};
```

### I/O Configuration

```cpp
struct IOConfig {
    size_t readBufferSize = 64 * 1024;   // 64KB read buffer
    size_t writeBufferSize = 64 * 1024;  // 64KB write buffer
    bool enableDirectIO = false;
    bool enableAsyncIO = true;
    size_t maxConcurrentReads = 10;
    size_t maxConcurrentWrites = 5;
};
```

## Security Configuration

### Access Control

```cpp
struct SecurityConfig {
    bool enableAccessControl = false;
    std::string accessControlProvider;
    std::vector<std::string> allowedPaths;
    std::vector<std::string> blockedPaths;
    bool enablePathTraversal = false;
    bool enableSymlinkFollowing = false;
};

SecurityConfig securityConfig;
securityConfig.enableAccessControl = true;
securityConfig.allowedPaths = {"/home/user/documents", "/var/data"};
securityConfig.blockedPaths = {"/etc", "/var/log"};
```

### Encryption

```cpp
struct EncryptionConfig {
    bool enableDatabaseEncryption = false;
    std::string encryptionKey;
    std::string encryptionAlgorithm = "AES256";
    bool enableTransportEncryption = true;
};
```

## Deployment Options

### Development Environment

```cpp
// Development configuration
DatabaseConfig devDbConfig;
devDbConfig.path = "dev_search.db";
devDbConfig.pragmas = {{"synchronous", "OFF"}};  // Fast but not durable

SearchConfig devSearchConfig;
devSearchConfig.enableHighlighting = true;
devSearchConfig.enableSpellCorrection = true;  // Help with typos

SearchCacheConfig devCacheConfig;
devCacheConfig.maxEntries = 100;      // Small cache for development
devCacheConfig.defaultTTL = std::chrono::minutes(1);  // Short TTL
```

### Production Environment

```cpp
// Production configuration
DatabaseConfig prodDbConfig;
prodDbConfig.path = "/var/lib/yams/search.db";
prodDbConfig.pragmas = {
    {"journal_mode", "WAL"},
    {"synchronous", "NORMAL"},
    {"cache_size", "50000"},
    {"mmap_size", "1073741824"}  // 1GB
};

SearchConfig prodSearchConfig;
prodSearchConfig.maxResults = 10000;
prodSearchConfig.timeout = std::chrono::seconds(30);

SearchCacheConfig prodCacheConfig;
prodCacheConfig.maxEntries = 100000;
prodCacheConfig.maxMemoryMB = 4096;   // 4GB cache
prodCacheConfig.defaultTTL = std::chrono::hours(1);
```

### High-Availability Setup

```cpp
// HA configuration with read replicas
struct HAConfig {
    std::vector<std::string> readReplicaPaths;
    std::string writeMasterPath;
    std::chrono::milliseconds replicationDelay{1000};
    bool enableAutomaticFailover = true;
    size_t maxReplicationLag = 1000;  // milliseconds
};

HAConfig haConfig;
haConfig.readReplicaPaths = {
    "/var/lib/yams/replica1.db",
    "/var/lib/yams/replica2.db"
};
haConfig.writeMasterPath = "/var/lib/yams/master.db";
```

## Environment Variables

The search system supports configuration through environment variables:

### Database Settings

```bash
# Database configuration
export YAMS_DB_PATH="/var/lib/yams/search.db"
export YAMS_DB_CACHE_SIZE="20000"
export YAMS_DB_MMAP_SIZE="2147483648"  # 2GB

# Connection settings
export YAMS_DB_POOL_SIZE="15"
export YAMS_DB_TIMEOUT="60"
```

### Search Settings

```bash
# Search configuration
export YAMS_SEARCH_MAX_RESULTS="5000"
export YAMS_SEARCH_DEFAULT_LIMIT="50"
export YAMS_SEARCH_TIMEOUT="45000"  # milliseconds

# Feature flags
export YAMS_ENABLE_HIGHLIGHTING="true"
export YAMS_ENABLE_SPELL_CORRECTION="false"
export YAMS_ENABLE_FACETING="true"
```

### Cache Settings

```bash
# Cache configuration
export YAMS_CACHE_MAX_ENTRIES="50000"
export YAMS_CACHE_MAX_MEMORY_MB="2048"
export YAMS_CACHE_DEFAULT_TTL="900"  # seconds
export YAMS_CACHE_EVICTION_POLICY="LRU"
```

### Indexing Settings

```bash
# Indexing configuration
export YAMS_INDEX_BATCH_SIZE="2000"
export YAMS_INDEX_WORKER_THREADS="6"
export YAMS_INDEX_INTERVAL="10000"  # milliseconds
export YAMS_ENABLE_INCREMENTAL_SYNC="true"
```

### Performance Settings

```bash
# Performance configuration
export YAMS_SEARCH_THREADS="8"
export YAMS_INDEXING_THREADS="4"
export YAMS_MAX_MEMORY_MB="4096"
```

### Loading Environment Configuration

```cpp
#include <cstdlib>

// Load configuration from environment
SearchConfig loadSearchConfigFromEnv() {
    SearchConfig config;
    
    if (const char* maxResults = std::getenv("YAMS_SEARCH_MAX_RESULTS")) {
        config.maxResults = std::stoull(maxResults);
    }
    
    if (const char* timeout = std::getenv("YAMS_SEARCH_TIMEOUT")) {
        config.timeout = std::chrono::milliseconds(std::stoull(timeout));
    }
    
    if (const char* highlighting = std::getenv("YAMS_ENABLE_HIGHLIGHTING")) {
        config.enableHighlighting = (std::string(highlighting) == "true");
    }
    
    return config;
}
```

## Configuration Validation

Validate configuration before system startup:

```cpp
bool validateConfiguration(const SearchConfig& config) {
    if (config.maxResults == 0 || config.maxResults > 100000) {
        std::cerr << "Invalid maxResults: " << config.maxResults << std::endl;
        return false;
    }
    
    if (config.defaultLimit > config.maxResults) {
        std::cerr << "defaultLimit cannot exceed maxResults" << std::endl;
        return false;
    }
    
    if (config.timeout.count() <= 0) {
        std::cerr << "Invalid timeout value" << std::endl;
        return false;
    }
    
    return true;
}
```

## Configuration Best Practices

1. **Start Simple**: Begin with default configurations and adjust based on performance testing
2. **Monitor Resources**: Track memory usage, CPU utilization, and I/O patterns
3. **Test Thoroughly**: Validate configuration changes in a staging environment
4. **Document Changes**: Keep a record of configuration modifications and their reasons
5. **Use Version Control**: Store configuration files in version control
6. **Regular Reviews**: Periodically review and optimize configuration settings
7. **Environment Separation**: Use different configurations for development, staging, and production

## Troubleshooting Configuration Issues

### Common Problems

1. **High Memory Usage**: Reduce cache sizes or result limits
2. **Slow Queries**: Increase database cache size, enable WAL mode
3. **Indexing Delays**: Increase worker threads, reduce batch size
4. **Connection Timeouts**: Increase connection pool size and timeout values
5. **Cache Misses**: Adjust TTL values and cache size limits

### Configuration Testing

```cpp
// Test configuration with sample data
bool testConfiguration(const SearchConfig& config) {
    try {
        // Create test search executor
        auto executor = createSearchExecutor(config);
        
        // Run test queries
        SearchRequest testRequest;
        testRequest.query = "test query";
        testRequest.maxResults = config.defaultLimit;
        
        auto result = executor->search(testRequest);
        return result.isSuccess();
        
    } catch (const std::exception& e) {
        std::cerr << "Configuration test failed: " << e.what() << std::endl;
        return false;
    }
}
```

This configuration guide provides comprehensive options for optimizing the YAMS search system for your specific deployment requirements. Adjust settings based on your hardware resources, expected load, and performance requirements.