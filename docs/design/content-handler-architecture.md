# Content Handler Architecture Design

## Executive Summary

This document describes the technical design for transforming YAMS's text-only extraction system into a universal content handler architecture that supports all file types with appropriate metadata extraction and indexing.

## Current State

### Existing Architecture

```cpp
// Current text-only approach
ITextExtractor* extractor = factory.create(".txt");
ExtractionResult result = extractor->extract(path);
// Only processes result.text, ignores other file types
```

### Limitations

1. **Narrow Interface**: `ITextExtractor` assumes text output
2. **Limited Metadata**: `ExtractionResult` focuses on text content
3. **No Routing Logic**: Manual extractor selection based on extension
4. **Missing Handlers**: No support for images, audio, video, binaries

## Proposed Architecture

### Core Interfaces

```cpp
namespace yams::content {

/**
 * @brief File content handler interface
 * 
 * Base interface for all content handlers. Each handler specializes
 * in processing specific file types and extracting relevant metadata.
 */
class IContentHandler {
public:
    virtual ~IContentHandler() = default;
    
    /**
     * @brief Check if this handler can process the given file
     * @param signature File type signature from FileTypeDetector
     * @return True if this handler supports the file type
     */
    virtual bool canHandle(const detection::FileSignature& signature) const = 0;
    
    /**
     * @brief Get handler priority (higher = preferred)
     * @return Priority value, default 0
     */
    virtual int priority() const { return 0; }
    
    /**
     * @brief Get supported MIME types
     * @return Vector of MIME type patterns this handler supports
     */
    virtual std::vector<std::string> supportedMimeTypes() const = 0;
    
    /**
     * @brief Process file and extract content/metadata
     * @param path File path to process
     * @param config Processing configuration
     * @return Processing result or error
     */
    virtual Result<ContentResult> process(
        const std::filesystem::path& path,
        const ContentConfig& config) = 0;
    
    /**
     * @brief Process from memory buffer
     * @param data File data in memory
     * @param hint Optional file type hint
     * @param config Processing configuration
     * @return Processing result or error
     */
    virtual Result<ContentResult> processBuffer(
        std::span<const std::byte> data,
        const std::string& hint,
        const ContentConfig& config) {
        return Error{ErrorCode::NotImplemented, 
                    "Buffer processing not supported by this handler"};
    }
};

/**
 * @brief Configuration for content processing
 */
struct ContentConfig {
    // General settings
    size_t maxFileSize = 100 * 1024 * 1024;  // 100MB default
    std::chrono::milliseconds timeout{30000}; // 30s timeout
    bool extractText = true;                  // Extract text content
    bool extractMetadata = true;              // Extract metadata
    bool generateThumbnail = false;           // Generate preview
    bool deepInspection = false;              // Thorough analysis
    
    // Text extraction settings (if applicable)
    bool preserveFormatting = false;
    bool detectLanguage = true;
    std::string preferredEncoding;
    
    // Media settings (if applicable)  
    bool extractFrames = false;               // Extract video frames
    bool extractAudioWaveform = false;        // Generate waveform
    int thumbnailSize = 256;                  // Thumbnail dimension
    
    // Performance settings
    bool useCache = true;                     // Use cached results
    bool asyncProcessing = false;             // Process asynchronously
    size_t maxMemoryUsage = 512 * 1024 * 1024; // 512MB limit
};

/**
 * @brief Result from content processing
 */
struct ContentResult {
    // Core content
    std::optional<std::string> text;          // Extracted text (if any)
    std::vector<ContentChunk> chunks;         // Text chunks for indexing
    
    // Metadata (key-value pairs)
    std::unordered_map<std::string, std::string> metadata;
    
    // Structured metadata
    std::optional<ImageMetadata> imageData;
    std::optional<AudioMetadata> audioData;
    std::optional<VideoMetadata> videoData;
    std::optional<DocumentMetadata> documentData;
    
    // Binary data
    std::vector<std::byte> thumbnail;         // Preview image
    std::vector<std::byte> extractedData;     // Other extracted data
    
    // Processing info
    std::string handlerName;                  // Handler that processed
    std::string contentType;                  // MIME type
    std::string fileFormat;                   // Specific format
    bool isComplete = true;                   // Full extraction done
    bool shouldIndex = true;                  // Create search index
    size_t processingTimeMs = 0;              // Processing duration
    std::vector<std::string> warnings;        // Non-fatal issues
    
    // Helper methods
    bool hasText() const { return text.has_value() && !text->empty(); }
    bool hasMetadata() const { return !metadata.empty(); }
    size_t getContentSize() const { return text ? text->size() : 0; }
};

/**
 * @brief Image-specific metadata
 */
struct ImageMetadata {
    uint32_t width = 0;
    uint32_t height = 0;
    uint32_t bitsPerPixel = 0;
    std::string colorSpace;
    std::string compression;
    std::optional<ExifData> exif;
    std::vector<std::string> embeddedText;    // OCR results
    bool hasAlpha = false;
    bool isAnimated = false;
};

/**
 * @brief Audio-specific metadata  
 */
struct AudioMetadata {
    double durationSeconds = 0.0;
    uint32_t bitrate = 0;
    uint32_t sampleRate = 0;
    uint8_t channels = 0;
    std::string codec;
    std::string encoder;
    std::unordered_map<std::string, std::string> tags; // ID3, Vorbis, etc.
};

/**
 * @brief Video-specific metadata
 */
struct VideoMetadata {
    double durationSeconds = 0.0;
    uint32_t width = 0;
    uint32_t height = 0;
    double frameRate = 0.0;
    std::string videoCodec;
    std::string audioCodec;
    std::string container;
    uint64_t bitrate = 0;
    std::vector<SubtitleTrack> subtitles;
    std::vector<AudioTrack> audioTracks;
};

} // namespace yams::content
```

### Handler Registry

```cpp
namespace yams::content {

/**
 * @brief Registry for content handlers
 * 
 * Manages handler registration and selection based on file type.
 * Thread-safe singleton implementation.
 */
class ContentHandlerRegistry {
public:
    static ContentHandlerRegistry& instance();
    
    /**
     * @brief Register a handler
     * @param handler Handler instance
     * @param name Unique handler name
     */
    void registerHandler(
        std::shared_ptr<IContentHandler> handler,
        const std::string& name);
    
    /**
     * @brief Get handler for file
     * @param signature File type signature
     * @return Best matching handler or nullptr
     */
    std::shared_ptr<IContentHandler> getHandler(
        const detection::FileSignature& signature) const;
    
    /**
     * @brief Get handler by name
     * @param name Handler name
     * @return Handler or nullptr
     */
    std::shared_ptr<IContentHandler> getHandlerByName(
        const std::string& name) const;
    
    /**
     * @brief Get all compatible handlers
     * @param signature File type signature
     * @return Vector of compatible handlers, sorted by priority
     */
    std::vector<std::shared_ptr<IContentHandler>> getCompatibleHandlers(
        const detection::FileSignature& signature) const;
    
    /**
     * @brief List all registered handlers
     * @return Map of handler names to handlers
     */
    std::unordered_map<std::string, std::shared_ptr<IContentHandler>> 
        getAllHandlers() const;
    
    /**
     * @brief Initialize default handlers
     */
    void initializeDefaultHandlers();
    
private:
    ContentHandlerRegistry() = default;
    
    mutable std::shared_mutex mutex_;
    std::unordered_map<std::string, std::shared_ptr<IContentHandler>> handlers_;
    std::multimap<std::string, std::string> mimeToHandler_; // MIME -> handler name
};

} // namespace yams::content
```

### Concrete Handler Implementations

#### TextContentHandler

```cpp
/**
 * @brief Handler for text files
 * 
 * Wraps existing PlainTextExtractor functionality
 */
class TextContentHandler : public IContentHandler {
public:
    bool canHandle(const detection::FileSignature& sig) const override {
        return !sig.isBinary || 
               sig.fileType == "text" || 
               sig.fileType == "code";
    }
    
    std::vector<std::string> supportedMimeTypes() const override {
        return {"text/*", "application/json", "application/xml", 
                "application/x-yaml", "application/x-sh"};
    }
    
    Result<ContentResult> process(
        const std::filesystem::path& path,
        const ContentConfig& config) override;
    
private:
    std::unique_ptr<extraction::PlainTextExtractor> textExtractor_;
};
```

#### ImageContentHandler

```cpp
/**
 * @brief Handler for image files
 * 
 * Extracts dimensions, EXIF, performs optional OCR
 */
class ImageContentHandler : public IContentHandler {
public:
    bool canHandle(const detection::FileSignature& sig) const override {
        return sig.fileType == "image";
    }
    
    std::vector<std::string> supportedMimeTypes() const override {
        return {"image/*"};
    }
    
    Result<ContentResult> process(
        const std::filesystem::path& path,
        const ContentConfig& config) override;
    
private:
    Result<ImageMetadata> extractImageMetadata(
        const std::filesystem::path& path);
    
    Result<std::string> performOCR(
        const std::filesystem::path& path);
    
    Result<std::vector<std::byte>> generateThumbnail(
        const std::filesystem::path& path,
        int size);
};
```

#### BinaryContentHandler

```cpp
/**
 * @brief Fallback handler for binary files
 * 
 * Extracts basic metadata for any file type
 */
class BinaryContentHandler : public IContentHandler {
public:
    bool canHandle(const detection::FileSignature& sig) const override {
        return true; // Can handle anything
    }
    
    int priority() const override { 
        return -100; // Lowest priority (fallback)
    }
    
    std::vector<std::string> supportedMimeTypes() const override {
        return {"*/*"};
    }
    
    Result<ContentResult> process(
        const std::filesystem::path& path,
        const ContentConfig& config) override;
};
```

### Integration with DocumentIndexer

```cpp
class DocumentIndexer : public IDocumentIndexer {
public:
    DocumentIndexer(
        std::shared_ptr<metadata::MetadataRepository> metadataRepo,
        std::shared_ptr<IContentProcessor> contentProcessor)
        : metadataRepo_(std::move(metadataRepo)),
          contentProcessor_(std::move(contentProcessor)) {
        // Initialize content handler system
        content::ContentHandlerRegistry::instance().initializeDefaultHandlers();
    }
    
    Result<IndexingResult> indexDocument(
        const std::filesystem::path& path,
        const IndexingConfig& config) override {
        
        // Detect file type
        auto& detector = detection::FileTypeDetector::instance();
        auto sigResult = detector.detectFromFile(path);
        if (!sigResult) {
            return Error{ErrorCode::InvalidData, 
                        "Failed to detect file type: " + sigResult.error().message};
        }
        
        // Get appropriate handler
        auto& registry = content::ContentHandlerRegistry::instance();
        auto handler = registry.getHandler(sigResult.value());
        if (!handler) {
            // Use binary handler as fallback
            handler = registry.getHandlerByName("BinaryHandler");
        }
        
        // Process content
        content::ContentConfig contentConfig;
        contentConfig.maxFileSize = config.maxDocumentSize;
        contentConfig.extractMetadata = config.extractMetadata;
        
        auto contentResult = handler->process(path, contentConfig);
        if (!contentResult) {
            return Error{ErrorCode::ProcessingError,
                        "Content processing failed: " + contentResult.error().message};
        }
        
        auto& content = contentResult.value();
        
        // Store metadata
        metadata::DocumentInfo docInfo;
        docInfo.filePath = path.string();
        docInfo.mimeType = content.contentType;
        docInfo.metadata = content.metadata;
        
        // Store in repository
        auto insertResult = metadataRepo_->insertDocument(docInfo);
        if (!insertResult) {
            return insertResult.error();
        }
        
        // Create chunks if text content exists
        if (content.hasText()) {
            auto chunks = contentProcessor_->chunkContent(
                content.text.value(),
                std::to_string(insertResult.value()),
                config);
            // Store chunks...
        }
        
        // Return success
        IndexingResult result;
        result.status = IndexingStatus::Completed;
        result.documentId = std::to_string(insertResult.value());
        result.hasTextContent = content.hasText();
        result.metadataExtracted = content.hasMetadata();
        
        return result;
    }
};
```

## Migration Strategy

### Phase 1: Parallel Implementation

1. Implement new interfaces alongside existing ones
2. Create adapter classes for backward compatibility
3. No breaking changes to public API

### Phase 2: Gradual Migration

```cpp
// Adapter for existing ITextExtractor
class TextExtractorAdapter : public IContentHandler {
private:
    std::shared_ptr<ITextExtractor> legacyExtractor_;
    
public:
    Result<ContentResult> process(
        const std::filesystem::path& path,
        const ContentConfig& config) override {
        
        // Convert config
        ExtractionConfig legacyConfig;
        legacyConfig.maxFileSize = config.maxFileSize;
        
        // Call legacy extractor
        auto legacyResult = legacyExtractor_->extract(path, legacyConfig);
        
        // Convert result
        ContentResult result;
        if (legacyResult) {
            result.text = legacyResult.value().text;
            result.metadata = legacyResult.value().metadata;
        }
        
        return result;
    }
};
```

### Phase 3: Deprecation

1. Mark old interfaces as deprecated
2. Update all internal usage
3. Provide migration guide for external users

## Performance Considerations

### Caching Strategy

```cpp
class ContentCache {
public:
    struct CacheKey {
        std::string path;
        std::string hash;
        std::time_t modTime;
    };
    
    std::optional<ContentResult> get(const CacheKey& key);
    void put(const CacheKey& key, const ContentResult& result);
    void evict(const CacheKey& key);
    void clear();
    
private:
    LRUCache<CacheKey, ContentResult> cache_{1000}; // 1000 entries
};
```

### Async Processing

```cpp
class AsyncContentProcessor {
public:
    std::future<Result<ContentResult>> processAsync(
        std::shared_ptr<IContentHandler> handler,
        const std::filesystem::path& path,
        const ContentConfig& config) {
        
        return std::async(std::launch::async, [=]() {
            return handler->process(path, config);
        });
    }
};
```

### Resource Management

- Memory limits per handler
- Timeout enforcement
- Graceful degradation for large files
- Stream processing for supported formats

## Testing Strategy

### Unit Testing

```cpp
class MockContentHandler : public IContentHandler {
public:
    MOCK_METHOD(bool, canHandle, (const FileSignature&), (const, override));
    MOCK_METHOD(Result<ContentResult>, process, 
                (const std::filesystem::path&, const ContentConfig&), (override));
};

TEST(ContentHandlerTest, HandlerSelection) {
    ContentHandlerRegistry registry;
    auto mockHandler = std::make_shared<MockContentHandler>();
    
    EXPECT_CALL(*mockHandler, canHandle(_))
        .WillOnce(Return(true));
    
    registry.registerHandler(mockHandler, "test");
    
    FileSignature sig;
    sig.mimeType = "text/plain";
    
    auto selected = registry.getHandler(sig);
    EXPECT_EQ(selected, mockHandler);
}
```

### Integration Testing

- End-to-end file processing
- Handler chain validation
- Performance benchmarks
- Memory leak detection

## Security Considerations

1. **Input Validation**: Validate file types before processing
2. **Resource Limits**: Enforce timeouts and memory limits
3. **Sandboxing**: Run handlers in restricted environment
4. **Path Traversal**: Prevent directory traversal attacks
5. **Malformed Files**: Graceful handling of corrupt data

## Configuration

```yaml
# config.yaml
content_handlers:
  enabled:
    - text
    - pdf
    - image
    - binary
  
  text:
    max_file_size: 104857600  # 100MB
    detect_language: true
    preserve_formatting: false
  
  image:
    extract_exif: true
    generate_thumbnail: true
    thumbnail_size: 256
    enable_ocr: false
  
  audio:
    extract_tags: true
    max_duration: 3600  # 1 hour
  
  video:
    extract_metadata: true
    generate_thumbnail: true
    max_file_size: 1073741824  # 1GB
  
  binary:
    compute_hash: true
    extract_strings: false
```

## Future Enhancements

1. **Plugin System**: Dynamic handler loading
2. **Machine Learning**: Content classification
3. **Cloud Processing**: Offload heavy processing
4. **Streaming Support**: Process files without full load
5. **Incremental Updates**: Update only changed portions

## Conclusion

This architecture provides:

- **Extensibility**: Easy to add new handlers
- **Flexibility**: Configurable processing pipeline
- **Performance**: Efficient resource usage
- **Compatibility**: Backward compatible migration
- **Completeness**: Handles all file types

The design maintains YAMS's core strengths while extending its capabilities to handle any content type effectively.