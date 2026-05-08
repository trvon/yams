# YAMS Content Extraction & Repair Pipeline - Architecture Map

**Date:** 2025-11-02  
**Purpose:** Document where content extraction happens, identify consolidation opportunities  
**Status:** Analysis Complete

---

## Executive Summary

Content extraction in YAMS is scattered across **5 major locations**:

1. **PostIngestQueue** - Primary extraction path for newly added documents
2. **RepairCoordinator** - Background repair for missing FTS5/embeddings
3. **CLI repair command** - Manual FTS5 rebuild via `yams repair --fts5`
4. **CLI doctor command** - Doctor diagnostic FTS5 repair
5. **TUI reindex** - Single-document reindex via Browse interface

All paths converge on:
- `yams::extraction::util::extractDocumentText()` - Central extraction utility
- `AppContext.contentExtractors` - Unified extractor registry (plugins + built-ins)

---

## 1. Content Extraction Entry Points

### 1.1 PostIngestQueue (Primary Path)
**File:** `src/daemon/components/PostIngestQueue.cpp`  
**Method:** `indexDocumentSync()`  
**Flow:**
```
Document Added → PostIngestQueue → indexDocumentSync()
  ↓
extractDocumentText(store_, hash, mime, extension, extractors_)
  ↓
persist_content_and_index() → FTS5 + fuzzy index
  ↓
Optional: Queue for embedding generation
```

**Key Code:**
```cpp
auto txt = extractDocumentText(store_, hash, resolvedMime, extension, extractors_);
if (txt && !txt->empty()) {
    persist_content_and_index(meta_, docId, d.fileName, *txt, resolvedMime);
}
```

**Scope:** Handles all new documents as they're added to YAMS

---

### 1.2 RepairCoordinator (Background Repair)
**File:** `src/daemon/components/RepairCoordinator.cpp`  
**Method:** `runAsync()`  
**Flow:**
```
Periodic scan → Detect missing FTS5/embeddings
  ↓
canExtractDocument() → Filter to extractable types
  ↓
Queue FTS5Job → InternalEventBus
  ↓
[Separate worker] → extractDocumentText() → Index
```

**Key Code:**
```cpp
bool canExtractDocument(
    const std::string& mimeType,
    const std::string& extension,
    const std::vector<std::shared_ptr<extraction::IContentExtractor>>& customExtractors,
    std::shared_ptr<yams::api::IContentStore> contentStore,
    const std::string& hash) {
    
    // 1) Check if any custom plugin extractor supports this format
    for (const auto& extractor : customExtractors) {
        if (extractor && extractor->supports(mimeType, extension)) {
            return true;
        }
    }
    
    // 2) Use FileTypeDetector (magic_numbers.hpp)
    auto& detector = yams::detection::FileTypeDetector::instance();
    if (!mimeType.empty() && detector.isTextMimeType(mimeType)) {
        return true;
    }
    
    // 3) Extension-based fallback
    // ...
}
```

**Scope:** Background repair of documents missing FTS5 content or embeddings

---

### 1.3 CLI Repair Command
**File:** `src/cli/commands/repair_command.cpp`  
**Method:** `rebuildFts5Index()`  
**Flow:**
```
yams repair --fts5 → rebuildFts5Index()
  ↓
queryDocumentsByPattern("%") → Get all documents
  ↓
For each document:
    extractDocumentText(ctx.store, hash, mime, ext, ctx.contentExtractors)
    ↓
    insertContent() → Store extracted text
    ↓
    indexDocumentContent() → FTS5
    ↓
    updateFuzzyIndex() → Fuzzy search
```

**Key Code:**
```cpp
auto extractedOpt = yams::extraction::util::extractDocumentText(
    ctx.store, d.sha256Hash, d.mimeType, ext, ctx.contentExtractors);

if (extractedOpt && !extractedOpt->empty()) {
    metadata::DocumentContent content;
    content.documentId = d.id;
    content.contentText = *extractedOpt;
    content.extractionMethod = "repair";
    
    auto contentResult = ctx.metadataRepo->insertContent(content);
    auto ir = ctx.metadataRepo->indexDocumentContent(d.id, d.fileName, *extractedOpt, d.mimeType);
    ctx.metadataRepo->updateFuzzyIndex(d.id);
}
```

**Scope:** Manual full-database FTS5 rebuild triggered by user

---

### 1.4 CLI Doctor Command
**File:** `src/cli/commands/doctor_command.cpp`  
**Method:** `runRepair()`  
**Flow:**
```
yams doctor --repair → runRepair()
  ↓
Similar to repair_command, but part of diagnostic suite
```

**Scope:** Diagnostic repair, rarely used compared to dedicated repair command

---

### 1.5 TUI Reindex
**File:** `src/cli/tui/tui_services.cpp`  
**Method:** `reindexDocument()`  
**Flow:**
```
Browse UI → Reindex single document
  ↓
extractDocumentText(contentStore_, hash, info.mimeType, extension, extractors)
  ↓
insertContent() → indexDocumentContent() → updateFuzzyIndex()
```

**Scope:** Single-document reindex from Browse interface

---

### 1.6 Embedding Repair Utility
**File:** `src/repair/embedding_repair_util.cpp`  
**Method:** `repairMissingEmbeddings()`  
**Flow:**
```
Batch of documents missing embeddings
  ↓
For each: extractDocumentText(..., extractors)
  ↓
Store content + generate embeddings + update status
```

**Scope:** Generate embeddings for documents (also extracts text if missing)

---

## 2. Central Extraction Function

### `extractDocumentText()` - Universal Extraction Utility

**File:** `src/extraction/extraction_util.cpp`  
**Signature:**
```cpp
std::optional<std::string> extractDocumentText(
    std::shared_ptr<yams::api::IContentStore> store,
    const std::string& hash,
    const std::string& mime,
    const std::string& extension,
    const ContentExtractorList& extractors  // ← Unified registry
);
```

**Extraction Strategy (Priority Order):**
1. **Plugin extractors** (highest priority):
   - Iterate through `extractors` parameter
   - Call `extractor->supports(mime, extension)`
   - If match: `extractor->extractText(bytes, mime, extension)`
   
2. **HTML fallback**:
   - If MIME is `text/html` or extension is `.html`
   - Use `HtmlTextExtractor`
   
3. **Text MIME types** (via FileTypeDetector):
   - Check `detector.isTextMimeType(mime)`
   - Return raw bytes as UTF-8 string
   
4. **Extension-based fallback**:
   - Use `FileTypeDetector::getMimeTypeFromExtension()`
   - If detected MIME is text → return raw content

**Returns:** `std::optional<std::string>` (nullopt if no extraction possible)

---

## 3. Extractor Registry Architecture

### ContentExtractorList Population

**Source:** `ServiceManager::getAppContext()` @ line 3058

```cpp
yams::app::services::AppContext ServiceManager::getAppContext() const {
    app::services::AppContext ctx;
    // ...
    ctx.contentExtractors = contentExtractors_;  // ← Unified registry
    // ...
}
```

### contentExtractors_ Initialization

**Location:** `ServiceManager::initializeAsyncAwaitable()` @ lines 2042-2059

```cpp
// 1. Adopt plugin-based extractors (ABI plugins)
auto extractorResult = init::step<size_t>(
    "adopt_extractors", [&]() { return adoptContentExtractorsFromHosts(); });

if (extractorResult) {
    spdlog::info("ServiceManager: Adopted {} content extractors from plugins.",
                 extractorResult.value());

    // 2. Add built-in content extractors from ContentExtractorFactory
    try {
        auto builtInExtractors = yams::extraction::ContentExtractorFactory::instance().createAll();
        if (!builtInExtractors.empty()) {
            contentExtractors_.insert(contentExtractors_.end(),
                                     builtInExtractors.begin(),
                                     builtInExtractors.end());
            spdlog::info("ServiceManager: Added {} built-in content extractors.",
                        builtInExtractors.size());
        }
    } catch (const std::exception& e) {
        spdlog::warn("ServiceManager: Failed to initialize built-in content extractors: {}", e.what());
    }
}
```

### Extractor Types

1. **Plugin Extractors** (via ABI):
   - Loaded from shared libraries (.so/.dll/.dylib)
   - Registered via `adoptContentExtractorsFromHosts()`
   - Wrapped in `AbiContentExtractorAdapter`
   - Examples: PDF extractors from plugins

2. **Built-in Extractors** (via ContentExtractorFactory):
   - Compiled into YAMS binary
   - Registered via `REGISTER_CONTENT_EXTRACTOR` macro
   - Created via `ContentExtractorFactory::instance().createAll()`
   - Examples: BinaryExtractor (commented out pending Ghidra)

---

## 4. Consolidation Opportunities

### 4.1 ✅ DONE: Unified Extractor Registry
- **Status:** Complete
- **Implementation:** ContentExtractorFactory merges built-ins with plugins in ServiceManager
- **Benefit:** Single source of truth for all extractors

### 4.2 🔄 OPPORTUNITY: Consolidate FTS5 Indexing Logic

**Current State:** FTS5 indexing duplicated across:
1. PostIngestQueue::persist_content_and_index()
2. RepairCommand::rebuildFts5Index()
3. DoctorCommand::runRepair()
4. TUIServices::reindexDocument()
5. SearchService::lightIndexForHash_impl()

**Proposed:** Create `IndexingService::indexContentForDocument()` utility:
```cpp
namespace yams::app::services {

/**
 * @brief Unified FTS5 + fuzzy indexing for a single document
 * 
 * Handles:
 * - Text extraction (via extractDocumentText)
 * - Content storage (insertContent/updateContent)
 * - FTS5 indexing (indexDocumentContent)
 * - Fuzzy index update (updateFuzzyIndex)
 * - Status updates (contentExtracted, extractionStatus)
 * 
 * @param ctx Application context (store, metadataRepo, contentExtractors)
 * @param hash Document hash
 * @param documentId Document ID (if known, else -1 to query)
 * @param extractionMethod Tag for tracking (e.g., "repair", "post-ingest", "tui")
 * @return Result<IndexingStats> with success/failure counts
 */
Result<IndexingStats> indexContentForDocument(
    const AppContext& ctx,
    const std::string& hash,
    int64_t documentId = -1,
    const std::string& extractionMethod = "default"
);

}
```

**Benefits:**
- ✅ Eliminate code duplication
- ✅ Consistent error handling
- ✅ Unified logging/metrics
- ✅ Easier to add new features (e.g., knowledge graph extraction)

### 4.3 🔄 OPPORTUNITY: Extraction Pipeline Abstraction

**Current State:** Extraction logic mixed with indexing/repair logic

**Proposed:** Separate concerns into pipeline stages:

```cpp
namespace yams::extraction {

/**
 * @brief Multi-stage extraction pipeline
 */
class ExtractionPipeline {
public:
    struct Stage {
        virtual Result<std::string> process(const std::string& input) = 0;
        virtual std::string name() const = 0;
    };
    
    void addStage(std::unique_ptr<Stage> stage);
    Result<std::string> execute(const DocumentInfo& doc);
};

// Example stages:
class TextExtractionStage : public Stage { /* uses extractDocumentText */ };
class HtmlCleanupStage : public Stage { /* removes scripts/style */ };
class LanguageDetectionStage : public Stage { /* detects language */ };
class ContentNormalizationStage : public Stage { /* UTF-8 sanitization */ };

}
```

**Benefits:**
- ✅ Testable stages
- ✅ Configurable pipeline per document type
- ✅ Easy to add new stages (e.g., binary disassembly post-processing)

### 4.4 🔄 OPPORTUNITY: Repair Logic Consolidation

**Current State:** Repair scattered across:
- RepairCoordinator (daemon background)
- RepairCommand (CLI manual)
- DoctorCommand (CLI diagnostic)
- TUIServices (Browse UI)

**Proposed:** Create `RepairService` in `app/services/`:

```cpp
namespace yams::app::services {

enum class RepairScope {
    FTS5Only,           // Rebuild FTS5 index
    EmbeddingsOnly,     // Generate missing embeddings
    Full,               // Both FTS5 + embeddings
    SingleDocument,     // Target one document
};

struct RepairOptions {
    RepairScope scope = RepairScope::Full;
    bool force = false;              // Re-extract even if exists
    size_t batchSize = 100;          // Documents per batch
    std::chrono::seconds timeout{600}; // Per-document timeout
    ProgressCallback progress;
};

class IRepairService {
public:
    virtual ~IRepairService() = default;
    
    virtual Result<RepairStats> repair(const RepairOptions& opts) = 0;
    virtual Result<RepairStats> repairDocument(const std::string& hash, const RepairOptions& opts) = 0;
};

std::shared_ptr<IRepairService> makeRepairService(const AppContext& ctx);

}
```

**Benefits:**
- ✅ CLI commands become thin wrappers
- ✅ Daemon uses same logic as CLI
- ✅ TUI uses same logic as CLI
- ✅ Single place for repair improvements
- ✅ Easier to add metrics/monitoring

---

## 5. Dataflow Diagram

```
┌──────────────────────────────────────────────────────────────┐
│                     Document Ingestion                        │
│                                                               │
│  User adds document → ContentStore → DocumentAddedEvent      │
└──────────────────────┬───────────────────────────────────────┘
                       │
                       ↓
         ┌─────────────────────────────┐
         │    PostIngestQueue          │
         │  (Primary Extraction Path)  │
         └──────────────┬──────────────┘
                        │
                        ↓
    ┌───────────────────────────────────────┐
    │  extractDocumentText()                │
    │  (Central Extraction Utility)         │
    │                                       │
    │  Inputs:                              │
    │   - hash, MIME type, extension        │
    │   - ContentExtractorList (unified)    │
    │                                       │
    │  Strategy:                            │
    │   1. Plugin extractors (priority)     │
    │   2. HTML fallback                    │
    │   3. Text MIME types                  │
    │   4. Extension-based fallback         │
    └───────────────┬───────────────────────┘
                    │
                    ↓ (extracted text)
         ┌──────────────────────────┐
         │  FTS5 + Fuzzy Indexing   │
         │  - insertContent()       │
         │  - indexDocumentContent()│
         │  - updateFuzzyIndex()    │
         └──────────┬───────────────┘
                    │
                    ↓
         ┌──────────────────────────┐
         │  Optional: Embeddings    │
         │  - Queue for generation  │
         └──────────────────────────┘

┌──────────────────────────────────────────────────────────────┐
│                 Background Repair Path                        │
│                                                               │
│  RepairCoordinator scans → Detect missing FTS5/embeddings    │
│         ↓                                                     │
│  canExtractDocument() → Filter to extractable                │
│         ↓                                                     │
│  Queue FTS5Job → Worker → extractDocumentText() → Index      │
└───────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────┐
│                    Manual Repair Paths                        │
│                                                               │
│  CLI: yams repair --fts5 → RepairCommand::rebuildFts5Index() │
│  CLI: yams doctor --repair → DoctorCommand::runRepair()      │
│  TUI: Browse → Reindex → TUIServices::reindexDocument()      │
│                                                               │
│  All call: extractDocumentText() → Index                     │
└───────────────────────────────────────────────────────────────┘
```

---

## 6. ContentExtractor Interface

### IContentExtractor (Base Interface)

**File:** `include/yams/extraction/content_extractor.h`

```cpp
class IContentExtractor {
public:
    virtual ~IContentExtractor() = default;
    
    // Check if this extractor can handle the given format
    virtual bool supports(const std::string& mime, const std::string& extension) const = 0;
    
    // Extract text from binary content
    virtual std::optional<std::string> extractText(
        const std::vector<std::byte>& bytes,
        const std::string& mime,
        const std::string& extension
    ) = 0;
};

using ContentExtractorList = std::vector<std::shared_ptr<IContentExtractor>>;
```

### Concrete Implementations

1. **BinaryExtractor** (Built-in, conditionally enabled):
   - File: `src/extraction/binary_extractor.cpp`
   - Supports: ELF, PE, Mach-O, Java class, WASM
   - Delegate: ExternalPluginExtractor → Ghidra plugin
   - Status: Compiled, registration commented out (requires Ghidra)

2. **AbiContentExtractorAdapter** (Plugin wrapper):
   - File: `src/daemon/resource/abi_content_extractor_adapter.h`
   - Wraps: Plugin-provided extractors via ABI
   - Examples: PDF extractors from dynamically loaded plugins

---

## 7. Recommendations

<<<<<<< HEAD
### Short-Term (External extraction plugin work completion)
=======
### Short-Term (external extraction plugin work Completion)
>>>>>>> origin/main
1. ✅ **DONE:** ContentExtractorFactory integration
2. ⏳ **IN PROGRESS:** Document extraction pipeline (this document)
3. 🔄 **NEXT:** Create `IndexingService::indexContentForDocument()` utility
4. 🔄 **NEXT:** Test BinaryExtractor end-to-end with Ghidra

### Medium-Term (Code Quality)
5. 🔄 Consolidate RepairCommand + DoctorCommand repair logic
6. 🔄 Create RepairService in app/services/
7. 🔄 Extract ExtractionPipeline abstraction

### Long-Term (Architecture)
8. 🔄 Plugin marketplace / discovery
9. 🔄 Extraction metrics / telemetry
10. 🔄 Incremental re-extraction (only changed content)

---

## 8. Testing Strategy

### Unit Tests Needed
- ✅ ContentExtractorFactory registration
- ⏳ IndexingService utility functions
- ⏳ RepairService (when created)
- ⏳ ExtractionPipeline stages (when created)

### Integration Tests Needed
- ⏳ BinaryExtractor with real Ghidra plugin
- ⏳ Full extraction pipeline (add → extract → index → search)
- ⏳ Repair flow (break FTS5 → repair → verify)

### End-to-End Tests Needed
- ⏳ PostIngestQueue with various file types
- ⏳ RepairCoordinator background repair
- ⏳ CLI repair command full rebuild
- ⏳ TUI single-document reindex

---

## 9. Metrics & Monitoring

### Extraction Metrics (Proposed)
- `extraction_attempts_total` (counter by mime_type, success/failure)
- `extraction_duration_seconds` (histogram)
- `extraction_content_size_bytes` (histogram)
- `extractor_plugin_calls_total` (counter by plugin_name)

### Repair Metrics (Existing)
- `repair_queue_depth` (gauge)
- `repair_documents_processed_total` (counter)
- `repair_fts5_operations_total` (counter by success/failure)
- `repair_embeddings_generated_total` (counter)

---

## 10. References

- **external extraction plugin work:** Binary File Extraction via Ghidra Plugin Integration
- **Extraction Utility:** `src/extraction/extraction_util.cpp`
- **ServiceManager:** `src/daemon/components/ServiceManager.cpp`
- **RepairCoordinator:** `src/daemon/components/RepairCoordinator.cpp`
- **PostIngestQueue:** `src/daemon/components/PostIngestQueue.cpp`
- **ContentExtractorFactory:** `include/yams/extraction/content_extractor_factory.h`

---

**Conclusion:**

The YAMS extraction pipeline is now **unified at the registry level** (ContentExtractorFactory + plugin system), but **duplicated at the call-site level** (5+ locations calling extractDocumentText + indexing logic).

The **primary consolidation opportunity** is creating utility services (`IndexingService`, `RepairService`) that wrap common patterns and reduce duplication across CLI commands, daemon components, and TUI interfaces.

With ContentExtractorFactory integrated, **BinaryExtractor is ready to be enabled** once Ghidra is available in the environment.
