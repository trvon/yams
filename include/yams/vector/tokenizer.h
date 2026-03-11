#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace yams::vector {

/**
 * Abstract tokenizer interface.
 *
 * Provides a model-agnostic contract for text tokenization so that
 * embedding, reranker and ColBERT sessions can share a single
 * tokenizer implementation without being coupled to any specific
 * vocabulary format (WordPiece, Unigram/SentencePiece, BPE, etc.).
 */
class ITokenizer {
public:
    virtual ~ITokenizer() = default;

    /// Load vocabulary / model data from a file (e.g. tokenizer.json).
    virtual bool load(const std::string& path) = 0;

    /// Encode text into a sequence of token ids.
    virtual std::vector<int32_t> encode(const std::string& text) const = 0;

    /// Return the unknown-token id.
    virtual int32_t unkTokenId() const = 0;

    /// Map a token string to its id (returns unkTokenId() if absent).
    virtual int32_t tokenToId(const std::string& token) const = 0;

    /// Map a token id back to its string (returns "" for out-of-range).
    virtual std::string idToToken(int32_t id) const = 0;

    /// True after a successful load().
    virtual bool isLoaded() const = 0;

    /// Number of entries in the vocabulary.
    virtual size_t vocabSize() const = 0;
};

/**
 * Concrete tokenizer that reads HuggingFace tokenizer.json files.
 *
 * Supports two vocabulary formats auto-detected at load time:
 *   - WordPiece  (BERT-style, `##` continuations)
 *   - Unigram    (SentencePiece-style, U+2581 word prefixes)
 *
 * Suitable for models such as:
 *   - all-MiniLM-L6-v2, all-mpnet-base-v2 (WordPiece/BERT)
 *   - bge-reranker-v2-m3 (WordPiece/BERT)
 *   - answerai-colbert-small-v1 (WordPiece/BERT)
 *   - EmbeddingGemma-300M (Unigram/SentencePiece)
 *   - jina-embeddings-v3 (Unigram/SentencePiece)
 */
class HuggingFaceTokenizer : public ITokenizer {
public:
    bool load(const std::string& path) override;
    std::vector<int32_t> encode(const std::string& text) const override;
    int32_t unkTokenId() const override;
    int32_t tokenToId(const std::string& token) const override;
    std::string idToToken(int32_t id) const override;
    bool isLoaded() const override;
    size_t vocabSize() const override;

private:
    /// UTF-8 bytes for the SentencePiece word-boundary marker U+2581.
    static constexpr const char* kSpieceUnderline = "\xe2\x96\x81";

    // Unigram (SentencePiece) encoding helpers
    std::vector<int32_t> encodeUnigram(const std::string& text) const;
    void encodeUnigramWord(const std::string& word, std::vector<int32_t>& ids) const;

    // WordPiece (BERT) encoding helpers
    std::vector<int32_t> encodeWordPiece(const std::string& text) const;
    std::vector<int32_t> encodeWordPieceWord(const std::string& word) const;

    std::unordered_map<std::string, int> vocab_;
    std::vector<std::string> idToToken_;
    int unkTokenId_ = 0;
    bool isUnigram_ = false;
    bool loaded_ = false;
};

/**
 * Factory: create a tokenizer from a HuggingFace tokenizer.json file.
 *
 * Returns a loaded ITokenizer on success, or nullptr if the file could
 * not be parsed.  Callers that need a specific concrete type can use
 * HuggingFaceTokenizer directly.
 */
std::unique_ptr<ITokenizer> createTokenizer(const std::string& tokenizerJsonPath);

} // namespace yams::vector
