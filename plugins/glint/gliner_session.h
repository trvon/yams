#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace yams::glint {

// ============================================================================
// Entity Span Result
// ============================================================================

struct EntitySpan {
    size_t start_word; // Word index (0-based)
    size_t end_word;   // Word index (inclusive)
    size_t start_char; // Character offset in original text
    size_t end_char;   // Character offset (exclusive)
    std::string text;  // Extracted entity text
    std::string label; // Entity type label
    float score;       // Confidence score (0-1)
};

// ============================================================================
// GLiNER Configuration
// ============================================================================

struct GlinerConfig {
    std::string model_path;     // Path to ONNX model
    std::string tokenizer_path; // Path to tokenizer.json (optional, inferred from model_path)
    std::vector<std::string> entity_labels; // Default entity types to extract
    float threshold = 0.5f;                 // Confidence threshold
    size_t max_width = 12;                  // Maximum span width (in words)
    size_t max_sequence_length = 512;       // Maximum input sequence length
    bool flat_ner = true;                   // If true, no overlapping entities
    int num_threads = 4;                    // ONNX Runtime threads
};

// ============================================================================
// GLiNER Session
// ============================================================================

/// GLiNER ONNX session for named entity recognition
///
/// This class handles:
/// - Loading GLiNER ONNX model
/// - Tokenization with entity type prompts
/// - Running inference
/// - Decoding span logits to entity spans
class GlinerSession {
public:
    explicit GlinerSession(const GlinerConfig& config);
    ~GlinerSession();

    // Non-copyable
    GlinerSession(const GlinerSession&) = delete;
    GlinerSession& operator=(const GlinerSession&) = delete;

    // Movable
    GlinerSession(GlinerSession&&) noexcept;
    GlinerSession& operator=(GlinerSession&&) noexcept;

    /// Initialize the session (load model, tokenizer)
    /// @return true if successful
    bool initialize();

    /// Check if session is ready for inference
    bool is_ready() const noexcept;

    /// Extract entities from text using configured entity labels
    /// @param text Input text
    /// @return Vector of extracted entity spans
    std::vector<EntitySpan> extract(std::string_view text);

    /// Extract entities from text with custom entity labels
    /// @param text Input text
    /// @param labels Entity type labels to extract
    /// @return Vector of extracted entity spans
    std::vector<EntitySpan> extract(std::string_view text, std::span<const std::string> labels);

    /// Get the configured entity labels
    const std::vector<std::string>& entity_labels() const noexcept;

    /// Get the current threshold
    float threshold() const noexcept;

    /// Set the confidence threshold
    void set_threshold(float threshold) noexcept;

    /// Get last error message
    std::string_view last_error() const noexcept;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

// ============================================================================
// Word Splitter
// ============================================================================

/// Split text into words, tracking character offsets
struct WordInfo {
    std::string text;
    size_t start_char;
    size_t end_char;
};

/// Split text into words using whitespace
/// @param text Input text
/// @return Vector of word info with character offsets
std::vector<WordInfo> split_into_words(std::string_view text);

// ============================================================================
// Span Utilities
// ============================================================================

/// Greedy search to remove overlapping spans, keeping highest confidence
/// @param spans Input spans (will be sorted by score)
/// @param flat_ner If true, remove all overlaps; if false, allow nested entities
/// @return Filtered spans sorted by position
std::vector<EntitySpan> greedy_search(std::vector<EntitySpan> spans, bool flat_ner = true);

} // namespace yams::glint
