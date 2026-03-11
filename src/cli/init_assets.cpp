#include <yams/cli/init_assets.hpp>

namespace yams::cli::init_assets {

namespace {

const std::vector<EmbeddingModel> kEmbeddingModels = {
    {"mxbai-edge-colbert-v0-17m",
     "https://huggingface.co/ryandono/mxbai-edge-colbert-v0-17m-onnx-int8/resolve/main/onnx/"
     "model_quantized.onnx",
     "Lightweight ColBERT (token-level, MaxSim) optimized for edge use", 17, 48},
    {"all-MiniLM-L6-v2",
     "https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2/resolve/main/onnx/model.onnx",
     "Lightweight model for semantic search", 90, 384},
    {"multi-qa-MiniLM-L6-cos-v1",
     "https://huggingface.co/sentence-transformers/multi-qa-MiniLM-L6-cos-v1/resolve/main/onnx/"
     "model.onnx",
     "Optimized for semantic search on QA pairs (215M training samples)", 90, 384},
    {"embeddinggemma-300m",
     "https://huggingface.co/onnx-community/embeddinggemma-300m-ONNX/resolve/main/onnx/model.onnx",
     "Google EmbeddingGemma 768-dim, SentencePiece tokenizer (2048 max tokens)", 1200, 768}};

const std::vector<GlinerModel> kGlinerModels = {
    {"gliner_small-v2.1-quantized", "onnx-community/gliner_small-v2.1", "model_quantized.onnx",
     "Small GLiNER quantized (fast, recommended)", 175},
    {"gliner_small-v2.1", "onnx-community/gliner_small-v2.1", "model.onnx",
     "Small GLiNER full precision (~580MB)", 580},
    {"gliner_medium-v2.1-quantized", "onnx-community/gliner_medium-v2.1", "model_quantized.onnx",
     "Medium GLiNER quantized (balanced)", 220},
    {"gliner_medium-v2.1", "onnx-community/gliner_medium-v2.1", "model.onnx",
     "Medium GLiNER full precision (~745MB)", 745}};

const std::vector<std::string> kGlinerTokenizerFiles = {"tokenizer.json", "config.json",
                                                        "gliner_config.json"};

const std::vector<RerankerModel> kRerankerModels = {
    {"bge-reranker-base", "BAAI/bge-reranker-base", "onnx/model.onnx",
     "Cross-encoder reranker for hybrid search (recommended)", 278, 512},
    {"bge-reranker-large", "BAAI/bge-reranker-large", "onnx/model.onnx",
     "High-quality cross-encoder reranker (larger, more accurate)", 560, 512}};

const std::vector<std::string> kRerankerTokenizerFiles = {"tokenizer.json", "config.json",
                                                          "tokenizer_config.json"};

const std::vector<GrammarInfo> kSupportedGrammars = {
    {"c", "tree-sitter/tree-sitter-c", "C language", true},
    {"cpp", "tree-sitter/tree-sitter-cpp", "C++ language", true},
    {"python", "tree-sitter/tree-sitter-python", "Python language", true},
    {"javascript", "tree-sitter/tree-sitter-javascript", "JavaScript/JSX", true},
    {"typescript", "tree-sitter/tree-sitter-typescript", "TypeScript/TSX", true},
    {"rust", "tree-sitter/tree-sitter-rust", "Rust language", true},
    {"go", "tree-sitter/tree-sitter-go", "Go language", true},
    {"swift", "alex-pinkus/tree-sitter-swift", "Swift language", true},
    {"java", "tree-sitter/tree-sitter-java", "Java language", false},
    {"csharp", "tree-sitter/tree-sitter-c-sharp", "C# language", false},
    {"php", "tree-sitter/tree-sitter-php", "PHP language", false},
    {"kotlin", "fwcd/tree-sitter-kotlin", "Kotlin language", false},
    {"perl", "tree-sitter-perl/tree-sitter-perl", "Perl language", false},
    {"r", "r-lib/tree-sitter-r", "R language", false},
    {"dart", "UserNobody14/tree-sitter-dart", "Dart/Flutter", false},
    {"sql", "DerekStride/tree-sitter-sql", "SQL queries", false},
    {"solidity", "JoranHonig/tree-sitter-solidity", "Solidity (Ethereum)", false},
    {"p4", "prona-p4-learning-platform/tree-sitter-p4", "P4 network language", false},
    {"zig", "maxxnino/tree-sitter-zig", "Zig language", false},
};

constexpr std::string_view kYamsSkillContent = R"skill(---
name: yams
description: Code indexing, semantic search, and knowledge graph for project memory
license: GPL-3.0
compatibility: claude-code, opencode
metadata:
  tools: cli, mcp
  categories: search, indexing, memory, knowledge-graph
---

# YAMS Skill

## Quick Reference

```bash
# Status & Health
yams status                    # Check daemon and index status
yams daemon start              # Start background daemon
yams doctor                    # Diagnose issues

# Indexing
yams add <file>                # Index single file
yams add . -r --include "*.py" # Index directory recursively
yams watch                     # Auto-index on file changes

# Search (use grep first, search for semantic)
yams grep "pattern"            # Code pattern search (fast, exact)
yams search "query"            # Semantic/hybrid search

# Graph
yams graph --name <file>       # Show file relationships
yams graph --list-type symbol  # List symbols
yams graph --relations         # List relation types with counts
yams graph --search "pattern"  # Search nodes by label pattern
```

## Code Indexing

### Index Project Files

```bash
# Index specific file types
yams add . -r --include "*.ts,*.tsx,*.js"

# Index with exclusions
yams add . -r --include "*.py" --exclude "venv/**,__pycache__/**"

# Index with metadata for tracking
yams add src/ -r --metadata "task=feature-auth"
```

### Auto-Index with Watch

```bash
yams watch                     # Start watching current directory
yams watch --interval 2000     # Custom interval (ms)
yams watch --stop              # Stop watching
```

## Search Patterns

### Decision Tree

1. **Code patterns** -> `yams grep` (fast, regex)
2. **Semantic/concept** -> `yams search` (embeddings)
3. **No results from grep** -> Try `yams search`

### grep (Code Search)

```bash
# Exact pattern
yams grep "function authenticate"

# Regex pattern
yams grep "async.*await.*fetch"

# Fuzzy matching
yams grep "authentcation" --fuzzy

# With context lines
yams grep "TODO" -A 2 -B 2

# Filter by extension
yams grep "import" --ext py
```

### search (Semantic Search)

```bash
# Concept search
yams search "error handling patterns"

# Hybrid search (default)
yams search "authentication flow" --type hybrid

# Limit results
yams search "database connection" --limit 5
```

## Knowledge Management

### Store Research

```bash
# Index documentation
curl -s "https://docs.example.com/api" | yams add - --name "api-docs.md"

# Store with metadata
yams add notes.md --metadata "source=research,topic=auth"
```

## Session Management

```bash
# Start named session
yams session start --name "feature-auth"

# List sessions
yams session ls

# Switch session
yams session use "feature-auth"

# Warm session cache (faster searches)
yams session warm --limit 100
```

## Graph Queries

```bash
# Show file dependencies
yams graph --name src/auth/login.ts --depth 2

# List all symbols of type
yams graph --list-type function --limit 50

# Find isolated nodes (potential dead code)
yams graph --list-type symbol --isolated

# Explore graph structure
yams graph --relations              # List all relation types with counts
yams graph --search "Request*"      # Find nodes matching pattern (wildcards: *, ?)
yams graph --search "*Controller"   # Find all controller nodes
```

## MCP Integration

YAMS exposes tools via Model Context Protocol for programmatic access.

```bash
yams serve                     # Start MCP server (quiet mode)
```

### MCP Configuration

```json
{
  "mcpServers": {
    "yams": {
      "command": "yams",
      "args": ["serve"]
    }
  }
}
```

## Troubleshooting

```bash
yams daemon status -d          # Check daemon status
yams daemon log -n 50          # View daemon logs
yams doctor                    # Full diagnostic
yams doctor repair --all       # Repair index
```

## Environment Variables

| Variable | Purpose |
|----------|---------|
| `YAMS_DATA_DIR` | Storage directory |
| `YAMS_SOCKET` | Daemon socket path |
| `YAMS_LOG_LEVEL` | Logging verbosity |
| `YAMS_SESSION_CURRENT` | Default session |
)skill";

} // namespace

const std::vector<EmbeddingModel>& embeddingModels() {
    return kEmbeddingModels;
}

const std::vector<GlinerModel>& glinerModels() {
    return kGlinerModels;
}

const std::vector<std::string>& glinerTokenizerFiles() {
    return kGlinerTokenizerFiles;
}

const std::vector<RerankerModel>& rerankerModels() {
    return kRerankerModels;
}

const std::vector<std::string>& rerankerTokenizerFiles() {
    return kRerankerTokenizerFiles;
}

const std::vector<GrammarInfo>& supportedGrammars() {
    return kSupportedGrammars;
}

std::string_view yamsSkillContent() {
    return kYamsSkillContent;
}

} // namespace yams::cli::init_assets
