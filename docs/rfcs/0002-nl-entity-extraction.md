# RFC-0002: Natural Language Entity Extraction for Search Enhancement

- Status: Draft
- Authors: @trevon
- Tracking: yams-3c1k

## Summary

Extend YAMS entity extraction to support natural language documents (prose, scientific papers, PDFs) alongside existing code symbol extraction. This will populate the `entity_vectors` table for non-code content, enabling the new entity vector search component to improve retrieval quality on document corpora like SciFact.

## Motivation

### Current State
- **EntityGraphService** orchestrates symbol extraction via TreeSitter-based plugins
- **symbol_extractor_v1** plugin interface extracts code symbols (functions, classes, methods)
- **entity_vectors** table stores entity embeddings for semantic entity search
- **SearchEngine** has a new `entity_vector` search component (weight 0.05 default)

### Problem
The `entity_vectors` table remains **empty** for natural language documents because:
1. TreeSitter only parses programming languages with formal grammars
2. No equivalent extraction exists for prose, scientific documents, or PDFs
3. SciFact benchmark (scientific claims) shows **no improvement** from entity vector search

### Benchmark Evidence
```
SciFact Results (300 queries):
MRR:  0.6303
R@K:  0.7988
nDCG: 0.6687
```
Entity vector component contributes 0.0 because `entity_vectors` is empty for SciFact.

### Opportunity
Natural language documents contain rich extractable entities:
- **Named Entities**: People, organizations, locations, dates
- **Scientific Concepts**: Diseases, chemicals, genes, proteins
- **Key Phrases**: Important n-grams, technical terms
- **Relations**: Subject-predicate-object triples (knowledge graph edges)
- **Claims/Propositions**: Atomic facts that can be verified

## Proposal

### Architecture Overview

```
EntityGraphService
├── Code Extractors (existing)
│   └── symbol_extractor_treesitter (functions, classes, imports)
│
└── NL Extractors (NEW)
    ├── NER Extractor (GLiNER/ONNX)
    │   └── Named entities with configurable types
    ├── Keyphrase Extractor (KeyBERT-style/ONNX)
    │   └── Document-representative phrases
    └── Relation Extractor (REBEL/ONNX) [future]
        └── Subject-predicate-object triples
```

### Plugin Interface: `nl_entity_extractor_v1`

New plugin interface parallel to `symbol_extractor_v1`:

```c
// nl_entity_extractor_v1.h

#define YAMS_IFACE_NL_ENTITY_EXTRACTOR_V1 "nl_entity_extractor_v1"
#define YAMS_IFACE_NL_ENTITY_EXTRACTOR_V1_VERSION 1u

// Entity types this extractor can produce
typedef enum yams_nl_entity_type_e {
    YAMS_NL_ENTITY_NAMED = 0,      // Named entities (PER, ORG, LOC, etc.)
    YAMS_NL_ENTITY_KEYPHRASE = 1,  // Key phrases/concepts
    YAMS_NL_ENTITY_RELATION = 2,   // Subject-predicate-object triple
    YAMS_NL_ENTITY_CLAIM = 3       // Atomic proposition/claim
} yams_nl_entity_type_t;

typedef struct yams_nl_entity_v1 {
    char* text;           // Entity surface form ("Albert Einstein")
    char* normalized;     // Normalized/canonical form (optional)
    char* entity_type;    // Type label ("PERSON", "DISEASE", etc.)
    yams_nl_entity_type_t kind;
    float confidence;     // Model confidence [0.0, 1.0]
    uint32_t start_offset;
    uint32_t end_offset;
    // For relations only:
    char* subject;        // Subject entity (nullable)
    char* predicate;      // Relation type (nullable)
    char* object;         // Object entity (nullable)
} yams_nl_entity_v1;

typedef struct yams_nl_extraction_result_v1 {
    yams_nl_entity_v1* entities;
    size_t entity_count;
    char* error;
} yams_nl_extraction_result_v1;

typedef struct yams_nl_entity_extractor_v1 {
    uint32_t abi_version;
    void* self;
    
    // Check if extractor supports mime type
    bool (*supports_mime)(void* self, const char* mime_type);
    
    // Get extractable entity types
    int (*get_entity_types)(void* self, const char** types, size_t* count);
    
    // Extract entities from text content
    int (*extract_entities)(
        void* self,
        const char* content,
        size_t content_len,
        const char* mime_type,
        const char* entity_types_json,  // Filter: ["PERSON", "ORG"] or null for all
        float threshold,                 // Confidence threshold
        yams_nl_extraction_result_v1** result
    );
    
    void (*free_result)(void* self, yams_nl_extraction_result_v1* result);
    int (*get_capabilities_json)(void* self, char** out_json);
    void (*free_string)(void* self, char* s);
} yams_nl_entity_extractor_v1;
```

### Model Selection Analysis

| Model | Task | ONNX Support | Size | Latency | License |
|-------|------|--------------|------|---------|---------|
| **GLiNER-medium-v2.1** | Zero-shot NER | ✅ Native | ~400MB | ~50ms/doc | Apache-2.0 |
| **GLiNER-small** | Zero-shot NER | ✅ Native | ~150MB | ~20ms/doc | Apache-2.0 |
| **KeyBERT** | Keyphrases | ⚠️ Via embeddings | N/A* | ~30ms/doc | MIT |
| **REBEL-large** | Relations | ⚠️ BART-based | ~1.6GB | ~200ms/doc | CC-BY-NC-SA |
| gemma-scope-2b-pt-res | Interpretability SAE | ❌ Not applicable | N/A | N/A | CC-BY-4.0 |

\* KeyBERT uses existing embedding model + cosine similarity, no separate ONNX model needed.

#### Recommendation: GLiNER for Phase 1

**GLiNER** is the best fit for initial implementation:
1. **Native ONNX support** with `convert_to_onnx.py` script
2. **Zero-shot capability** - extract arbitrary entity types without retraining
3. **Compact size** - fits YAMS's local-first philosophy
4. **Production-ready** - quantization-aware training, NAACL 2024 paper
5. **Apache-2.0 license** - compatible with YAMS

#### Note on gemma-scope-2b-pt-res

The user-suggested `google/gemma-scope-2b-pt-res` is **not suitable** for entity extraction:
- It's a **Sparse Autoencoder (SAE)** for model interpretability research
- Designed to decompose Gemma 2B's internal representations into interpretable features
- Used with SAELens library for mechanistic interpretability
- Does not perform NER, keyphrase extraction, or relation extraction

## Design Details

### Phase 1: GLiNER NER Integration

#### 1.1 ONNX Model Management

Extend existing `IModelProvider` infrastructure:

```cpp
// New model type in model_registry.h
enum class ModelPurpose {
    Embedding,      // existing
    EntityNER,      // NEW: NER models like GLiNER
    EntityRelation  // future: relation extraction
};

// Model download/discovery
struct NLModelInfo : ModelInfo {
    std::vector<std::string> supported_entity_types;
    std::string default_threshold;
};
```

#### 1.2 GLiNER ONNX Plugin

```
plugins/
└── nl_entity_gliner/
    ├── meson.build
    ├── gliner_plugin.cpp      # Plugin entry point
    ├── gliner_extractor.cpp   # ONNX inference wrapper
    ├── gliner_extractor.h
    └── models/
        └── README.md          # Model download instructions
```

GLiNER ONNX export (from upstream):
```python
from gliner import GLiNER
model = GLiNER.from_pretrained("urchade/gliner_medium-v2.1")
model.to_onnx("gliner_medium.onnx", quantize=True)
```

#### 1.3 EntityGraphService Extension

```cpp
// In EntityGraphService::process()
bool EntityGraphService::process(Job& job) {
    // Existing code symbol extraction
    if (isCodeLanguage(job.language)) {
        return processCodeSymbols(job);
    }
    
    // NEW: Natural language entity extraction
    if (isNaturalLanguage(job.mimeType)) {
        return processNLEntities(job);
    }
    
    return true;
}

bool EntityGraphService::processNLEntities(Job& job) {
    auto* extractor = findNLExtractor(job.mimeType);
    if (!extractor) return true;  // no-op if no extractor
    
    yams_nl_extraction_result_v1* result = nullptr;
    int rc = extractor->extract_entities(
        extractor->self,
        job.contentUtf8.data(),
        job.contentUtf8.size(),
        job.mimeType.c_str(),
        nullptr,  // all entity types
        0.5f,     // confidence threshold
        &result
    );
    
    if (rc != 0 || !result) return false;
    
    // Populate KG and entity_vectors
    populateNLEntities(job, result);
    generateNLEntityEmbeddings(job, result);
    
    extractor->free_result(extractor->self, result);
    return true;
}
```

#### 1.4 Entity Storage

Entities stored in two places:

**Knowledge Graph** (kg_nodes table):
```sql
INSERT INTO kg_nodes (node_key, label, type, properties)
VALUES (
    'entity:scifact:12345:DISEASE:diabetes',  -- unique key
    'diabetes',                                -- label
    'nl_entity',                              -- type
    '{"kind":"DISEASE","confidence":0.92,"source_doc":"abc123"}'
);
```

**Entity Vectors** (entity_vectors table):
```sql
INSERT INTO entity_vectors (entity_key, entity_type, embedding)
VALUES (
    'diabetes',
    'DISEASE',
    <768-dim vector from embedding model>
);
```

### Phase 2: Keyphrase Extraction (Future)

KeyBERT-style extraction using existing embedding infrastructure:

```cpp
class KeyphraseExtractor {
    // Reuses EmbeddingService for document/phrase embeddings
    std::vector<Keyphrase> extract(
        const std::string& document,
        size_t top_n = 10,
        std::pair<size_t, size_t> ngram_range = {1, 3}
    ) {
        // 1. Generate document embedding
        auto doc_emb = embeddingService_->embed(document);
        
        // 2. Extract candidate n-grams
        auto candidates = extractCandidates(document, ngram_range);
        
        // 3. Embed candidates (batch)
        auto cand_embs = embeddingService_->embedBatch(candidates);
        
        // 4. Rank by cosine similarity to document
        return rankBySimilarity(candidates, cand_embs, doc_emb, top_n);
    }
};
```

### Phase 3: Relation Extraction (Future)

REBEL for knowledge graph population:
- Extracts (subject, predicate, object) triples
- Directly populates `kg_edges` table
- Enables graph-based retrieval enhancements

## Configuration

```toml
# config.toml
[entity_extraction]
enabled = true

[entity_extraction.ner]
enabled = true
model = "gliner-medium-v2.1"
threshold = 0.5
entity_types = ["PERSON", "ORG", "LOC", "DISEASE", "CHEMICAL", "GENE"]

[entity_extraction.keyphrases]
enabled = false  # Phase 2
top_n = 10
ngram_range = [1, 3]

[entity_extraction.relations]
enabled = false  # Phase 3
```

## Search Integration

Entity vector search already integrated (yams-y839). This RFC enables it for NL:

```cpp
// SearchEngine already has:
SearchEngineConfig config;
config.entityVectorWeight = 0.05;  // 5% of final score
config.entityVectorMaxResults = 50;

// For CODE corpus: uses code symbols
// For PROSE corpus: will now use NL entities
config.corpusProfile = CorpusProfile::PROSE;
// Sets entityVectorWeight = 0.0 currently (no NL entities)
// After this RFC: entityVectorWeight = 0.10 for PROSE
```

## Alternatives Considered

### 1. LLM-based Extraction
- **Pros**: Highest quality, flexible
- **Cons**: High latency, API costs, no offline support
- **Verdict**: Rejected for core pipeline, may add as optional

### 2. SpaCy NER
- **Pros**: Fast, well-tested
- **Cons**: Fixed entity types, requires Python runtime
- **Verdict**: Rejected, want ONNX-native C++ integration

### 3. Fine-tuned BERT NER
- **Pros**: High accuracy on target domain
- **Cons**: Requires training data, not zero-shot
- **Verdict**: Future consideration for domain-specific models

### 4. Gemma Scope SAE Features
- **Pros**: Interpretable features, Google-backed
- **Cons**: Not designed for NER, requires full Gemma model
- **Verdict**: Not applicable for this use case

## Migration / Compatibility

- **Backward compatible**: New plugin interface, existing code unchanged
- **Schema**: No changes to existing tables
- **Config**: New optional `[entity_extraction]` section

## Security Considerations

- ONNX models run in sandboxed ONNX Runtime
- No network calls during inference
- Model files verified by hash on download

## Rollout Plan

1. **Phase 1** (2 weeks): GLiNER ONNX plugin
   - Plugin skeleton with `nl_entity_extractor_v1` interface
   - GLiNER ONNX model loading and inference
   - EntityGraphService integration
   - Basic tests

2. **Phase 2** (1 week): SciFact validation
   - Ingest SciFact with NL extraction enabled
   - Benchmark retrieval quality improvement
   - Tune entity vector weight for PROSE corpus

3. **Phase 3** (future): KeyBERT + REBEL
   - Keyphrase extraction using existing embeddings
   - Relation extraction for KG population

## Drawbacks / Risks

1. **Model size**: GLiNER-medium is ~400MB, increases distribution size
   - Mitigation: On-demand download like embedding models

2. **Latency**: NER adds ~50ms per document
   - Mitigation: Async processing in existing pipeline

3. **Entity type mismatch**: Different domains need different types
   - Mitigation: Configurable entity types, zero-shot flexibility

## Unresolved Questions

1. How to handle entity deduplication across documents?
2. Should entity confidence affect embedding weight in search?
3. Optimal entity types for different corpus profiles (CODE, PROSE, DOCS)?
4. Should we link entities to external KBs (Wikidata, UMLS)?

## References

- [GLiNER Paper (NAACL 2024)](https://aclanthology.org/2024.naacl-long.300/)
- [GLiNER GitHub](https://github.com/urchade/GLiNER)
- [REBEL Paper (EMNLP 2021)](https://aclanthology.org/2021.findings-emnlp.204)
- [KeyBERT](https://github.com/MaartenGr/KeyBERT)
- [YAMS symbol_extractor_v1](../spec/symbol_extractor_v1.md)
