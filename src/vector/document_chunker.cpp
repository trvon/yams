#include <yams/vector/document_chunker.h>
#include <yams/profiling.h>

#include <algorithm>
#include <sstream>
#include <regex>
#include <cmath>
#include <numeric>
#include <future>
#include <thread>

namespace yams::vector {

// =============================================================================
// DocumentChunker Base Class Implementation
// =============================================================================

DocumentChunker::DocumentChunker(const ChunkingConfig& config)
    : config_(config) {}

std::vector<DocumentChunk> DocumentChunker::chunkDocument(
    const std::string& content,
    const std::string& document_hash) {
    YAMS_ZONE_SCOPED_N("DocumentChunker::chunkDocument");
    
    if (content.empty()) {
        last_error_ = "Empty content provided";
        has_error_ = true;
        return {};
    }
    
    auto start = std::chrono::high_resolution_clock::now();
    
    try {
        // Perform chunking using the derived class implementation
        auto chunks = doChunking(content, document_hash);
        
        // Post-process chunks
        if (config_.overlap_size > 0 || config_.overlap_percentage > 0) {
            chunks = addOverlap(chunks);
        }
        
        // Merge small chunks if needed
        chunks = mergeSmallChunks(chunks);
        
        // Split large chunks if needed
        chunks = splitLargeChunks(chunks);
        
        // Link chunks together
        linkChunks(chunks);
        
        // Validate chunks
        if (!validateChunks(chunks)) {
            last_error_ = "Chunk validation failed";
            has_error_ = true;
            return {};
        }
        
        // Update statistics
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        
        size_t total_size = 0;
        for (const auto& chunk : chunks) {
            total_size += chunk.content.size();
        }
        
        stats_.update(chunks.size(), total_size, duration);
        
        has_error_ = false;
        return chunks;
        
    } catch (const std::exception& e) {
        last_error_ = "Chunking failed: " + std::string(e.what());
        has_error_ = true;
        return {};
    }
}

std::future<std::vector<DocumentChunk>> DocumentChunker::chunkDocumentAsync(
    const std::string& content,
    const std::string& document_hash) {
    
    return std::async(std::launch::async, [this, content, document_hash]() {
        return chunkDocument(content, document_hash);
    });
}

std::vector<std::vector<DocumentChunk>> DocumentChunker::chunkDocuments(
    const std::vector<std::string>& contents,
    const std::vector<std::string>& document_hashes) {
    
    if (contents.size() != document_hashes.size()) {
        last_error_ = "Content and hash vectors must have same size";
        has_error_ = true;
        return {};
    }
    
    std::vector<std::vector<DocumentChunk>> results;
    results.reserve(contents.size());
    
    for (size_t i = 0; i < contents.size(); ++i) {
        results.push_back(chunkDocument(contents[i], document_hashes[i]));
    }
    
    return results;
}

void DocumentChunker::setConfig(const ChunkingConfig& config) {
    config_ = config;
}

const ChunkingConfig& DocumentChunker::getConfig() const {
    return config_;
}

std::vector<DocumentChunk> DocumentChunker::mergeSmallChunks(
    const std::vector<DocumentChunk>& chunks) {
    
    if (chunks.empty() || config_.min_chunk_size == 0) {
        return chunks;
    }
    
    std::vector<DocumentChunk> merged;
    DocumentChunk current_chunk;
    bool has_current = false;
    
    for (const auto& chunk : chunks) {
        size_t chunk_size = config_.use_token_count ? chunk.token_count : chunk.content.size();
        
        if (!has_current) {
            current_chunk = chunk;
            has_current = true;
        } else if (chunk_size < config_.min_chunk_size) {
            // Merge with current chunk
            current_chunk.content += config_.chunk_separator + chunk.content;
            current_chunk.end_offset = chunk.end_offset;
            current_chunk.token_count += chunk.token_count;
            current_chunk.sentence_count += chunk.sentence_count;
            current_chunk.word_count += chunk.word_count;
        } else {
            // Save current and start new
            if (current_chunk.content.size() >= config_.min_chunk_size) {
                merged.push_back(current_chunk);
            }
            current_chunk = chunk;
        }
    }
    
    // Add the last chunk
    if (has_current && current_chunk.content.size() >= config_.min_chunk_size) {
        merged.push_back(current_chunk);
    }
    
    return merged;
}

std::vector<DocumentChunk> DocumentChunker::splitLargeChunks(
    const std::vector<DocumentChunk>& chunks) {
    
    if (chunks.empty() || config_.max_chunk_size == 0) {
        return chunks;
    }
    
    std::vector<DocumentChunk> split_chunks;
    
    for (const auto& chunk : chunks) {
        size_t chunk_size = config_.use_token_count ? chunk.token_count : chunk.content.size();
        
        if (chunk_size <= config_.max_chunk_size) {
            split_chunks.push_back(chunk);
        } else {
            // Split the chunk
            size_t num_splits = (chunk_size + config_.max_chunk_size - 1) / config_.max_chunk_size;
            size_t split_size = chunk_size / num_splits;
            
            size_t start = 0;
            for (size_t i = 0; i < num_splits; ++i) {
                size_t end = (i == num_splits - 1) ? chunk.content.size() : start + split_size;
                
                // Find word boundary if needed
                if (config_.preserve_words && end < chunk.content.size()) {
                    while (end > start && !std::isspace(chunk.content[end])) {
                        --end;
                    }
                }
                
                DocumentChunk split_chunk;
                split_chunk.chunk_id = generateChunkId(chunk.document_hash, split_chunks.size());
                split_chunk.document_hash = chunk.document_hash;
                split_chunk.content = chunk.content.substr(start, end - start);
                split_chunk.chunk_index = split_chunks.size();
                split_chunk.start_offset = chunk.start_offset + start;
                split_chunk.end_offset = chunk.start_offset + end;
                split_chunk.token_count = estimateTokenCount(split_chunk.content);
                split_chunk.strategy_used = chunk.strategy_used;
                
                updateChunkMetadata(split_chunk, split_chunk.content);
                split_chunks.push_back(split_chunk);
                
                start = end;
            }
        }
    }
    
    return split_chunks;
}

std::vector<DocumentChunk> DocumentChunker::addOverlap(
    const std::vector<DocumentChunk>& chunks) {
    
    if (chunks.size() <= 1) {
        return chunks;
    }
    
    std::vector<DocumentChunk> overlapped;
    
    for (size_t i = 0; i < chunks.size(); ++i) {
        DocumentChunk chunk = chunks[i];
        
        // Calculate overlap size
        size_t overlap_size = config_.overlap_size;
        if (config_.overlap_percentage > 0) {
            size_t chunk_size = config_.use_token_count ? chunk.token_count : chunk.content.size();
            overlap_size = static_cast<size_t>(chunk_size * config_.overlap_percentage);
        }
        
        // Add overlap from previous chunk
        if (i > 0 && overlap_size > 0) {
            const auto& prev = chunks[i - 1];
            size_t prev_size = prev.content.size();
            
            if (prev_size > overlap_size) {
                std::string overlap_text = prev.content.substr(prev_size - overlap_size);
                chunk.content = overlap_text + config_.chunk_separator + chunk.content;
                chunk.start_offset = std::max(chunk.start_offset - overlap_size, size_t(0));
                chunk.overlapping_chunks.push_back(prev.chunk_id);
            }
        }
        
        // Note: Overlap with next chunk will be added when processing next chunk
        
        overlapped.push_back(chunk);
    }
    
    return overlapped;
}

bool DocumentChunker::validateChunks(const std::vector<DocumentChunk>& chunks) const {
    for (const auto& chunk : chunks) {
        if (!validateChunkSize(chunk)) {
            return false;
        }
        
        if (chunk.chunk_id.empty() || chunk.document_hash.empty()) {
            return false;
        }
        
        if (chunk.start_offset > chunk.end_offset) {
            return false;
        }
    }
    
    return true;
}

bool DocumentChunker::validateChunkSize(const DocumentChunk& chunk) const {
    size_t size = config_.use_token_count ? chunk.token_count : chunk.content.size();
    
    if (config_.min_chunk_size > 0 && size < config_.min_chunk_size) {
        return false;
    }
    
    if (config_.max_chunk_size > 0 && size > config_.max_chunk_size) {
        return false;
    }
    
    return true;
}

ChunkingStats DocumentChunker::getStats() const {
    return stats_;
}

void DocumentChunker::resetStats() {
    stats_ = ChunkingStats{};
}

size_t DocumentChunker::estimateTokenCount(const std::string& text) const {
    // Simple estimation: average 4 characters per token
    return std::max(size_t(1), text.size() / 4);
}

std::vector<size_t> DocumentChunker::findSentenceBoundaries(const std::string& text) const {
    std::vector<size_t> boundaries;
    
    // Simple sentence detection using regex
    std::regex sentence_end(R"([.!?]+\s+)");
    auto begin = std::sregex_iterator(text.begin(), text.end(), sentence_end);
    auto end = std::sregex_iterator();
    
    for (auto it = begin; it != end; ++it) {
        boundaries.push_back(it->position() + it->length());
    }
    
    // Add end of text as boundary
    if (!text.empty()) {
        boundaries.push_back(text.size());
    }
    
    return boundaries;
}

std::vector<size_t> DocumentChunker::findParagraphBoundaries(const std::string& text) const {
    std::vector<size_t> boundaries;
    
    // Find double newlines
    size_t pos = 0;
    while ((pos = text.find("\n\n", pos)) != std::string::npos) {
        boundaries.push_back(pos);
        pos += 2;
    }
    
    // Add end of text
    if (!text.empty()) {
        boundaries.push_back(text.size());
    }
    
    return boundaries;
}

std::vector<size_t> DocumentChunker::findWordBoundaries(const std::string& text) const {
    std::vector<size_t> boundaries;
    
    bool in_word = false;
    for (size_t i = 0; i < text.size(); ++i) {
        bool is_space = std::isspace(text[i]);
        
        if (in_word && is_space) {
            boundaries.push_back(i);
            in_word = false;
        } else if (!in_word && !is_space) {
            in_word = true;
        }
    }
    
    if (!text.empty()) {
        boundaries.push_back(text.size());
    }
    
    return boundaries;
}

std::string DocumentChunker::getLastError() const {
    return last_error_;
}

bool DocumentChunker::hasError() const {
    return has_error_;
}

std::string DocumentChunker::generateChunkId(const std::string& document_hash, size_t index) const {
    return document_hash + "_chunk_" + std::to_string(index);
}

void DocumentChunker::updateChunkMetadata(DocumentChunk& chunk, const std::string& content) const {
    // Count sentences
    chunk.sentence_count = std::count(content.begin(), content.end(), '.') +
                          std::count(content.begin(), content.end(), '!') +
                          std::count(content.begin(), content.end(), '?');
    
    // Count words (simple approximation)
    chunk.word_count = 1; // Start with 1 for the first word
    for (char c : content) {
        if (std::isspace(c)) {
            chunk.word_count++;
        }
    }
    
    // Count lines
    chunk.line_count = std::count(content.begin(), content.end(), '\n') + 1;
    
    // Update token count
    if (chunk.token_count == 0) {
        chunk.token_count = estimateTokenCount(content);
    }
}

void DocumentChunker::linkChunks(std::vector<DocumentChunk>& chunks) const {
    for (size_t i = 0; i < chunks.size(); ++i) {
        if (i > 0) {
            chunks[i].previous_chunk_id = chunks[i - 1].chunk_id;
        }
        
        if (i < chunks.size() - 1) {
            chunks[i].next_chunk_id = chunks[i + 1].chunk_id;
        }
        
        chunks[i].chunk_index = i;
    }
}

// =============================================================================
// FixedSizeChunker Implementation
// =============================================================================

FixedSizeChunker::FixedSizeChunker(const ChunkingConfig& config)
    : DocumentChunker(config) {
    config_.strategy = ChunkingStrategy::FIXED_SIZE;
}

std::vector<DocumentChunk> FixedSizeChunker::doChunking(
    const std::string& content,
    const std::string& document_hash) {
    
    std::vector<DocumentChunk> chunks;
    size_t chunk_size = config_.target_chunk_size;
    size_t start = 0;
    size_t chunk_index = 0;
    
    while (start < content.size()) {
        size_t end = std::min(start + chunk_size, content.size());
        
        // Adjust to word boundary if needed
        if (config_.preserve_words && end < content.size()) {
            while (end > start && !std::isspace(content[end])) {
                --end;
            }
            
            // If we couldn't find a space, force break at original position
            if (end == start) {
                end = std::min(start + chunk_size, content.size());
            }
        }
        
        DocumentChunk chunk;
        chunk.chunk_id = generateChunkId(document_hash, chunk_index);
        chunk.document_hash = document_hash;
        chunk.content = content.substr(start, end - start);
        chunk.chunk_index = chunk_index;
        chunk.start_offset = start;
        chunk.end_offset = end;
        chunk.strategy_used = ChunkingStrategy::FIXED_SIZE;
        
        updateChunkMetadata(chunk, chunk.content);
        chunks.push_back(chunk);
        
        start = end;
        chunk_index++;
    }
    
    return chunks;
}

// =============================================================================
// SentenceBasedChunker Implementation
// =============================================================================

SentenceBasedChunker::SentenceBasedChunker(const ChunkingConfig& config)
    : DocumentChunker(config) {
    config_.strategy = ChunkingStrategy::SENTENCE_BASED;
}

std::vector<DocumentChunk> SentenceBasedChunker::doChunking(
    const std::string& content,
    const std::string& document_hash) {
    
    std::vector<DocumentChunk> chunks;
    auto sentences = splitIntoSentences(content);
    
    if (sentences.empty()) {
        return chunks;
    }
    
    DocumentChunk current_chunk;
    current_chunk.chunk_id = generateChunkId(document_hash, 0);
    current_chunk.document_hash = document_hash;
    current_chunk.chunk_index = 0;
    current_chunk.start_offset = 0;
    current_chunk.strategy_used = ChunkingStrategy::SENTENCE_BASED;
    
    size_t current_size = 0;
    size_t position = 0;
    
    for (const auto& sentence : sentences) {
        size_t sentence_size = config_.use_token_count ? 
                               estimateTokenCount(sentence) : sentence.size();
        
        // Check if adding this sentence would exceed target size
        if (current_size > 0 && current_size + sentence_size > config_.target_chunk_size) {
            // Save current chunk
            current_chunk.end_offset = position;
            updateChunkMetadata(current_chunk, current_chunk.content);
            chunks.push_back(current_chunk);
            
            // Start new chunk
            current_chunk = DocumentChunk();
            current_chunk.chunk_id = generateChunkId(document_hash, chunks.size());
            current_chunk.document_hash = document_hash;
            current_chunk.chunk_index = chunks.size();
            current_chunk.start_offset = position;
            current_chunk.strategy_used = ChunkingStrategy::SENTENCE_BASED;
            current_chunk.content = "";
            current_size = 0;
        }
        
        // Add sentence to current chunk
        if (!current_chunk.content.empty()) {
            current_chunk.content += " ";
        }
        current_chunk.content += sentence;
        current_size += sentence_size;
        position += sentence.size();
    }
    
    // Add the last chunk
    if (!current_chunk.content.empty()) {
        current_chunk.end_offset = content.size();
        updateChunkMetadata(current_chunk, current_chunk.content);
        chunks.push_back(current_chunk);
    }
    
    return chunks;
}

std::vector<std::string> SentenceBasedChunker::splitIntoSentences(const std::string& text) const {
    std::vector<std::string> sentences;
    
    size_t start = 0;
    for (size_t i = 0; i < text.size(); ++i) {
        if (isSentenceEnd(text, i)) {
            // Include the punctuation in the sentence
            size_t end = i + 1;
            
            // Skip trailing whitespace
            while (end < text.size() && std::isspace(text[end])) {
                ++end;
            }
            
            std::string sentence = text.substr(start, i + 1 - start);
            
            // Trim leading/trailing whitespace
            sentence.erase(0, sentence.find_first_not_of(" \t\n\r"));
            sentence.erase(sentence.find_last_not_of(" \t\n\r") + 1);
            
            if (!sentence.empty()) {
                sentences.push_back(sentence);
            }
            
            start = end;
            i = end - 1; // -1 because loop will increment
        }
    }
    
    // Add remaining text as last sentence
    if (start < text.size()) {
        std::string sentence = text.substr(start);
        sentence.erase(0, sentence.find_first_not_of(" \t\n\r"));
        sentence.erase(sentence.find_last_not_of(" \t\n\r") + 1);
        
        if (!sentence.empty()) {
            sentences.push_back(sentence);
        }
    }
    
    return sentences;
}

bool SentenceBasedChunker::isSentenceEnd(const std::string& text, size_t pos) const {
    if (pos >= text.size()) {
        return false;
    }
    
    char c = text[pos];
    
    // Check for sentence-ending punctuation
    if (c != '.' && c != '!' && c != '?') {
        return false;
    }
    
    // Look ahead for space or end of text
    if (pos + 1 >= text.size()) {
        return true; // End of text
    }
    
    char next = text[pos + 1];
    
    // Check for space, newline, or quotation marks after punctuation
    if (std::isspace(next) || next == '"' || next == '\'' || next == ')') {
        // Avoid splitting on abbreviations (simple heuristic)
        if (c == '.' && pos > 0) {
            // Check if previous character is uppercase (possible abbreviation)
            char prev = text[pos - 1];
            if (std::isupper(prev) && pos > 1) {
                char prev2 = text[pos - 2];
                if (std::isspace(prev2) || prev2 == '.') {
                    return false; // Likely an abbreviation
                }
            }
        }
        
        return true;
    }
    
    return false;
}

// =============================================================================
// ParagraphBasedChunker Implementation
// =============================================================================

ParagraphBasedChunker::ParagraphBasedChunker(const ChunkingConfig& config)
    : DocumentChunker(config) {
    config_.strategy = ChunkingStrategy::PARAGRAPH_BASED;
}

std::vector<DocumentChunk> ParagraphBasedChunker::doChunking(
    const std::string& content,
    const std::string& document_hash) {
    
    std::vector<DocumentChunk> chunks;
    auto paragraphs = splitIntoParagraphs(content);
    
    if (paragraphs.empty()) {
        return chunks;
    }
    
    DocumentChunk current_chunk;
    current_chunk.chunk_id = generateChunkId(document_hash, 0);
    current_chunk.document_hash = document_hash;
    current_chunk.chunk_index = 0;
    current_chunk.start_offset = 0;
    current_chunk.strategy_used = ChunkingStrategy::PARAGRAPH_BASED;
    
    size_t current_size = 0;
    size_t position = 0;
    
    for (const auto& paragraph : paragraphs) {
        size_t para_size = config_.use_token_count ? 
                          estimateTokenCount(paragraph) : paragraph.size();
        
        // Check if adding this paragraph would exceed target size
        if (current_size > 0 && current_size + para_size > config_.target_chunk_size) {
            // Save current chunk
            current_chunk.end_offset = position;
            updateChunkMetadata(current_chunk, current_chunk.content);
            chunks.push_back(current_chunk);
            
            // Start new chunk
            current_chunk = DocumentChunk();
            current_chunk.chunk_id = generateChunkId(document_hash, chunks.size());
            current_chunk.document_hash = document_hash;
            current_chunk.chunk_index = chunks.size();
            current_chunk.start_offset = position;
            current_chunk.strategy_used = ChunkingStrategy::PARAGRAPH_BASED;
            current_chunk.content = "";
            current_size = 0;
        }
        
        // Add paragraph to current chunk
        if (!current_chunk.content.empty()) {
            current_chunk.content += "\n\n";
        }
        current_chunk.content += paragraph;
        current_size += para_size;
        position += paragraph.size() + 2; // +2 for \n\n
    }
    
    // Add the last chunk
    if (!current_chunk.content.empty()) {
        current_chunk.end_offset = content.size();
        updateChunkMetadata(current_chunk, current_chunk.content);
        chunks.push_back(current_chunk);
    }
    
    return chunks;
}

std::vector<std::string> ParagraphBasedChunker::splitIntoParagraphs(const std::string& text) const {
    std::vector<std::string> paragraphs;
    
    // Split on double newlines
    std::regex paragraph_separator(R"(\n\s*\n)");
    std::sregex_token_iterator it(text.begin(), text.end(), paragraph_separator, -1);
    std::sregex_token_iterator end;
    
    for (; it != end; ++it) {
        std::string paragraph = *it;
        
        // Trim whitespace
        paragraph.erase(0, paragraph.find_first_not_of(" \t\n\r"));
        paragraph.erase(paragraph.find_last_not_of(" \t\n\r") + 1);
        
        if (!paragraph.empty()) {
            paragraphs.push_back(paragraph);
        }
    }
    
    return paragraphs;
}

// =============================================================================
// RecursiveTextSplitter Implementation
// =============================================================================

RecursiveTextSplitter::RecursiveTextSplitter(const ChunkingConfig& config)
    : DocumentChunker(config) {
    config_.strategy = ChunkingStrategy::RECURSIVE;
}

std::vector<DocumentChunk> RecursiveTextSplitter::doChunking(
    const std::string& content,
    const std::string& document_hash) {
    
    // Use recursive splitting to get text segments
    auto segments = recursiveSplit(content, config_.separators, config_.target_chunk_size);
    
    std::vector<DocumentChunk> chunks;
    size_t position = 0;
    
    for (size_t i = 0; i < segments.size(); ++i) {
        DocumentChunk chunk;
        chunk.chunk_id = generateChunkId(document_hash, i);
        chunk.document_hash = document_hash;
        chunk.content = segments[i];
        chunk.chunk_index = i;
        chunk.start_offset = position;
        chunk.end_offset = position + segments[i].size();
        chunk.strategy_used = ChunkingStrategy::RECURSIVE;
        
        updateChunkMetadata(chunk, chunk.content);
        chunks.push_back(chunk);
        
        position = chunk.end_offset;
    }
    
    return chunks;
}

std::vector<std::string> RecursiveTextSplitter::recursiveSplit(
    const std::string& text,
    const std::vector<std::string>& separators,
    size_t target_size) {
    
    std::vector<std::string> results;
    
    if (text.size() <= target_size) {
        if (!text.empty()) {
            results.push_back(text);
        }
        return results;
    }
    
    // Try each separator in order
    for (const auto& separator : separators) {
        if (separator.empty()) {
            // Last resort: split by character
            size_t start = 0;
            while (start < text.size()) {
                size_t end = std::min(start + target_size, text.size());
                results.push_back(text.substr(start, end - start));
                start = end;
            }
            break;
        }
        
        // Try splitting with current separator
        splitOnSeparator(text, separator, results, target_size);
        
        // Check if we got reasonable chunks
        bool needs_further_split = false;
        for (const auto& chunk : results) {
            if (chunk.size() > target_size * 1.5) {
                needs_further_split = true;
                break;
            }
        }
        
        if (!needs_further_split) {
            break; // Good enough
        }
        
        // Clear and try next separator
        results.clear();
    }
    
    return results;
}

void RecursiveTextSplitter::splitOnSeparator(
    const std::string& text,
    const std::string& separator,
    std::vector<std::string>& results,
    size_t target_size) {
    
    size_t start = 0;
    size_t pos = 0;
    std::string current_chunk;
    
    while ((pos = text.find(separator, start)) != std::string::npos) {
        std::string segment = text.substr(start, pos - start);
        
        if (current_chunk.size() + segment.size() + separator.size() > target_size) {
            // Save current chunk
            if (!current_chunk.empty()) {
                results.push_back(current_chunk);
                current_chunk.clear();
            }
        }
        
        // Add segment to current chunk
        if (!current_chunk.empty()) {
            current_chunk += separator;
        }
        current_chunk += segment;
        
        start = pos + separator.size();
    }
    
    // Add remaining text
    if (start < text.size()) {
        std::string segment = text.substr(start);
        
        if (current_chunk.size() + segment.size() > target_size && !current_chunk.empty()) {
            results.push_back(current_chunk);
            current_chunk = segment;
        } else {
            if (!current_chunk.empty()) {
                current_chunk += separator;
            }
            current_chunk += segment;
        }
    }
    
    // Add final chunk
    if (!current_chunk.empty()) {
        results.push_back(current_chunk);
    }
}

// =============================================================================
// SlidingWindowChunker Implementation
// =============================================================================

SlidingWindowChunker::SlidingWindowChunker(const ChunkingConfig& config)
    : DocumentChunker(config) {
    config_.strategy = ChunkingStrategy::SLIDING_WINDOW;
}

std::vector<DocumentChunk> SlidingWindowChunker::doChunking(
    const std::string& content,
    const std::string& document_hash) {
    
    std::vector<DocumentChunk> chunks;
    size_t window_size = config_.target_chunk_size;
    size_t stride = calculateStride();
    
    size_t start = 0;
    size_t chunk_index = 0;
    size_t last_start = SIZE_MAX; // Track last position to prevent infinite loops
    
    while (start < content.size()) {
        // Safety check to prevent infinite loops
        if (start == last_start) {
            break;
        }
        last_start = start;
        size_t end = std::min(start + window_size, content.size());
        
        // Adjust to word boundary if needed (with reasonable limits)
        if (config_.preserve_words && end < content.size()) {
            size_t original_end = end;
            size_t max_backup = std::min(window_size / 4, size_t(20)); // Max 25% or 20 chars
            
            // Look backwards for a word boundary, but not too far
            while (end > start && end > original_end - max_backup && !std::isspace(content[end])) {
                --end;
            }
            
            // If we couldn't find a reasonable word boundary, use original end
            if (end <= start || end <= original_end - max_backup) {
                end = original_end;
            }
        }
        
        DocumentChunk chunk;
        chunk.chunk_id = generateChunkId(document_hash, chunk_index);
        chunk.document_hash = document_hash;
        chunk.content = content.substr(start, end - start);
        chunk.chunk_index = chunk_index;
        chunk.start_offset = start;
        chunk.end_offset = end;
        chunk.strategy_used = ChunkingStrategy::SLIDING_WINDOW;
        
        updateChunkMetadata(chunk, chunk.content);
        
        // Mark overlapping chunks
        if (chunk_index > 0) {
            chunk.overlapping_chunks.push_back(generateChunkId(document_hash, chunk_index - 1));
        }
        
        chunks.push_back(chunk);
        
        // Move window by stride
        start += stride;
        chunk_index++;
        
        // Check if we're close to the end and handle final chunk
        if (start + stride >= content.size()) {
            // We're at the last iteration, let the loop end naturally
            break;
        }
    }
    
    return chunks;
}

size_t SlidingWindowChunker::calculateStride() const {
    size_t window_size = config_.target_chunk_size;
    size_t overlap = config_.overlap_size;
    
    if (config_.overlap_percentage > 0) {
        overlap = static_cast<size_t>(window_size * config_.overlap_percentage);
    }
    
    size_t stride = window_size - overlap;
    
    // Ensure stride is at least 1
    return std::max(size_t(1), stride);
}

// =============================================================================
// Factory Function
// =============================================================================

std::unique_ptr<DocumentChunker> createChunker(
    ChunkingStrategy strategy,
    const ChunkingConfig& config,
    std::shared_ptr<EmbeddingGenerator> embedder) {
    
    switch (strategy) {
        case ChunkingStrategy::FIXED_SIZE:
            return std::make_unique<FixedSizeChunker>(config);
            
        case ChunkingStrategy::SENTENCE_BASED:
            return std::make_unique<SentenceBasedChunker>(config);
            
        case ChunkingStrategy::PARAGRAPH_BASED:
            return std::make_unique<ParagraphBasedChunker>(config);
            
        case ChunkingStrategy::RECURSIVE:
            return std::make_unique<RecursiveTextSplitter>(config);
            
        case ChunkingStrategy::SLIDING_WINDOW:
            return std::make_unique<SlidingWindowChunker>(config);
            
        case ChunkingStrategy::SEMANTIC:
            if (embedder) {
                return std::make_unique<SemanticChunker>(config, embedder);
            }
            // Fall back to sentence-based if no embedder provided
            return std::make_unique<SentenceBasedChunker>(config);
            
        case ChunkingStrategy::MARKDOWN_AWARE:
            return std::make_unique<MarkdownChunker>(config);
            
        default:
            return std::make_unique<SentenceBasedChunker>(config);
    }
}

// =============================================================================
// Utility Functions
// =============================================================================

namespace chunking_utils {

size_t calculateOptimalChunkSize(const std::string& model_name) {
    // Model-specific optimal chunk sizes
    if (model_name.find("all-MiniLM-L6-v2") != std::string::npos) {
        return 512;
    } else if (model_name.find("all-mpnet-base-v2") != std::string::npos) {
        return 384;
    } else if (model_name.find("gpt") != std::string::npos) {
        return 2048;
    }
    
    // Default chunk size
    return 512;
}

size_t estimateTokens(const std::string& text) {
    // Simple estimation: ~4 characters per token on average
    return std::max(size_t(1), text.size() / 4);
}

std::string preprocessText(const std::string& text) {
    std::string processed = text;
    
    // Normalize whitespace
    std::regex multiple_spaces(R"(\s+)");
    processed = std::regex_replace(processed, multiple_spaces, " ");
    
    // Remove control characters
    processed.erase(
        std::remove_if(processed.begin(), processed.end(),
                      [](char c) { return std::iscntrl(c) && c != '\n' && c != '\t'; }),
        processed.end()
    );
    
    return processed;
}

std::string detectLanguage(const std::string& text) {
    // Simplified language detection
    // In a real implementation, use a proper language detection library
    
    // Check for common English words
    std::vector<std::string> english_words = {"the", "and", "is", "in", "to", "of"};
    int english_count = 0;
    
    std::string lower_text = text;
    std::transform(lower_text.begin(), lower_text.end(), lower_text.begin(), ::tolower);
    
    for (const auto& word : english_words) {
        if (lower_text.find(word) != std::string::npos) {
            english_count++;
        }
    }
    
    if (english_count >= 3) {
        return "en";
    }
    
    return "unknown";
}

double calculateSemanticCoherence(const std::string& text) {
    // Simplified coherence calculation
    // In a real implementation, use embeddings to measure semantic similarity
    
    // For now, use sentence count as a proxy for coherence
    size_t sentence_count = std::count(text.begin(), text.end(), '.') +
                           std::count(text.begin(), text.end(), '!') +
                           std::count(text.begin(), text.end(), '?');
    
    if (sentence_count == 0) {
        return 0.0;
    }
    
    // Average words per sentence as coherence metric
    size_t word_count = 1;
    for (char c : text) {
        if (std::isspace(c)) {
            word_count++;
        }
    }
    
    double avg_words_per_sentence = static_cast<double>(word_count) / sentence_count;
    
    // Normalize to 0-1 range (assuming 10-20 words per sentence is optimal)
    double coherence = 1.0 - std::abs(avg_words_per_sentence - 15.0) / 15.0;
    
    return std::max(0.0, std::min(1.0, coherence));
}

std::vector<size_t> findNaturalBreaks(const std::string& text) {
    std::vector<size_t> breaks;
    
    // Find paragraph breaks
    size_t pos = 0;
    while ((pos = text.find("\n\n", pos)) != std::string::npos) {
        breaks.push_back(pos);
        pos += 2;
    }
    
    // Find section breaks (markdown headers)
    std::regex header_regex(R"(^#{1,6}\s+.*$)", std::regex::multiline);
    auto begin = std::sregex_iterator(text.begin(), text.end(), header_regex);
    auto end = std::sregex_iterator();
    
    for (auto it = begin; it != end; ++it) {
        breaks.push_back(it->position());
    }
    
    // Sort and remove duplicates
    std::sort(breaks.begin(), breaks.end());
    breaks.erase(std::unique(breaks.begin(), breaks.end()), breaks.end());
    
    return breaks;
}

std::vector<DocumentChunk> mergeOverlappingChunks(
    const std::vector<DocumentChunk>& chunks,
    double overlap_threshold) {
    
    if (chunks.empty()) {
        return chunks;
    }
    
    std::vector<DocumentChunk> merged;
    DocumentChunk current = chunks[0];
    
    for (size_t i = 1; i < chunks.size(); ++i) {
        const auto& chunk = chunks[i];
        
        // Calculate overlap percentage
        size_t overlap_start = std::max(current.start_offset, chunk.start_offset);
        size_t overlap_end = std::min(current.end_offset, chunk.end_offset);
        
        if (overlap_start < overlap_end) {
            size_t overlap_size = overlap_end - overlap_start;
            size_t min_size = std::min(current.content.size(), chunk.content.size());
            double overlap_ratio = static_cast<double>(overlap_size) / min_size;
            
            if (overlap_ratio > overlap_threshold) {
                // Merge chunks
                current.end_offset = std::max(current.end_offset, chunk.end_offset);
                
                // Merge content (avoiding duplication)
                size_t non_overlap_start = current.end_offset - current.start_offset;
                if (non_overlap_start < chunk.content.size()) {
                    current.content += chunk.content.substr(non_overlap_start);
                }
                
                // Update metadata
                current.token_count += chunk.token_count;
                current.sentence_count += chunk.sentence_count;
                current.word_count += chunk.word_count;
                
                continue;
            }
        }
        
        // No significant overlap, save current and move to next
        merged.push_back(current);
        current = chunk;
    }
    
    // Add the last chunk
    merged.push_back(current);
    
    return merged;
}

std::vector<DocumentChunk> deduplicateChunks(
    const std::vector<DocumentChunk>& chunks,
    double similarity_threshold) {
    
    std::vector<DocumentChunk> unique_chunks;
    
    for (const auto& chunk : chunks) {
        bool is_duplicate = false;
        
        for (const auto& unique : unique_chunks) {
            // Simple similarity based on content overlap
            // In a real implementation, use embeddings for semantic similarity
            
            size_t common_chars = 0;
            size_t min_len = std::min(chunk.content.size(), unique.content.size());
            
            for (size_t i = 0; i < min_len; ++i) {
                if (chunk.content[i] == unique.content[i]) {
                    common_chars++;
                }
            }
            
            double similarity = static_cast<double>(common_chars) / min_len;
            
            if (similarity > similarity_threshold) {
                is_duplicate = true;
                break;
            }
        }
        
        if (!is_duplicate) {
            unique_chunks.push_back(chunk);
        }
    }
    
    return unique_chunks;
}

std::string createChunkingSummary(const std::vector<DocumentChunk>& chunks) {
    if (chunks.empty()) {
        return "No chunks created";
    }
    
    std::ostringstream summary;
    summary << "Chunking Summary:\n";
    summary << "  Total chunks: " << chunks.size() << "\n";
    
    // Calculate statistics
    size_t total_size = 0;
    size_t min_size = std::numeric_limits<size_t>::max();
    size_t max_size = 0;
    size_t total_tokens = 0;
    
    for (const auto& chunk : chunks) {
        size_t size = chunk.content.size();
        total_size += size;
        min_size = std::min(min_size, size);
        max_size = std::max(max_size, size);
        total_tokens += chunk.token_count;
    }
    
    double avg_size = static_cast<double>(total_size) / chunks.size();
    
    summary << "  Average chunk size: " << static_cast<size_t>(avg_size) << " characters\n";
    summary << "  Min chunk size: " << min_size << " characters\n";
    summary << "  Max chunk size: " << max_size << " characters\n";
    summary << "  Total tokens: " << total_tokens << "\n";
    
    // Count strategies used
    std::map<ChunkingStrategy, size_t> strategy_counts;
    for (const auto& chunk : chunks) {
        strategy_counts[chunk.strategy_used]++;
    }
    
    summary << "  Strategies used:\n";
    for (const auto& [strategy, count] : strategy_counts) {
        summary << "    ";
        switch (strategy) {
            case ChunkingStrategy::FIXED_SIZE:
                summary << "Fixed Size";
                break;
            case ChunkingStrategy::SENTENCE_BASED:
                summary << "Sentence Based";
                break;
            case ChunkingStrategy::PARAGRAPH_BASED:
                summary << "Paragraph Based";
                break;
            case ChunkingStrategy::SEMANTIC:
                summary << "Semantic";
                break;
            case ChunkingStrategy::SLIDING_WINDOW:
                summary << "Sliding Window";
                break;
            case ChunkingStrategy::RECURSIVE:
                summary << "Recursive";
                break;
            case ChunkingStrategy::MARKDOWN_AWARE:
                summary << "Markdown Aware";
                break;
        }
        summary << ": " << count << " chunks\n";
    }
    
    return summary.str();
}

} // namespace chunking_utils

// =============================================================================
// Placeholder implementations for remaining chunkers
// =============================================================================

// SemanticChunker
SemanticChunker::SemanticChunker(const ChunkingConfig& config,
                                 std::shared_ptr<EmbeddingGenerator> embedder)
    : DocumentChunker(config), embedder_(embedder) {
    config_.strategy = ChunkingStrategy::SEMANTIC;
}

std::vector<DocumentChunk> SemanticChunker::doChunking(
    const std::string& content,
    const std::string& document_hash) {
    // Simplified implementation - creates basic chunks
    // Full implementation would use embeddings to group semantically similar sentences
    std::vector<DocumentChunk> chunks;
    
    size_t chunk_size = config_.target_chunk_size;
    size_t start = 0;
    size_t chunk_index = 0;
    
    while (start < content.size()) {
        size_t end = std::min(start + chunk_size, content.size());
        
        DocumentChunk chunk;
        chunk.chunk_id = generateChunkId(document_hash, chunk_index);
        chunk.document_hash = document_hash;
        chunk.content = content.substr(start, end - start);
        chunk.chunk_index = chunk_index;
        chunk.start_offset = start;
        chunk.end_offset = end;
        chunk.strategy_used = ChunkingStrategy::SEMANTIC;
        
        chunks.push_back(std::move(chunk));
        
        start = end;
        chunk_index++;
    }
    
    linkChunks(chunks);
    return chunks;
}

double SemanticChunker::computeSimilarity(const std::string& text1, const std::string& text2) {
    // Compute embeddings and calculate cosine similarity
    // Placeholder implementation
    return 0.5;
}

std::vector<size_t> SemanticChunker::findSemanticBoundaries(const std::string& text) {
    // Find boundaries where semantic similarity drops
    // Placeholder implementation
    return findSentenceBoundaries(text);
}

// MarkdownChunker
MarkdownChunker::MarkdownChunker(const ChunkingConfig& config)
    : DocumentChunker(config) {
    config_.strategy = ChunkingStrategy::MARKDOWN_AWARE;
}

std::vector<DocumentChunk> MarkdownChunker::doChunking(
    const std::string& content,
    const std::string& document_hash) {
    // Simplified implementation - creates basic chunks  
    // Full implementation would parse markdown structure
    std::vector<DocumentChunk> chunks;
    
    size_t chunk_size = config_.target_chunk_size;
    size_t start = 0;
    size_t chunk_index = 0;
    
    while (start < content.size()) {
        size_t end = std::min(start + chunk_size, content.size());
        
        DocumentChunk chunk;
        chunk.chunk_id = generateChunkId(document_hash, chunk_index);
        chunk.document_hash = document_hash;
        chunk.content = content.substr(start, end - start);
        chunk.chunk_index = chunk_index;
        chunk.start_offset = start;
        chunk.end_offset = end;
        chunk.strategy_used = ChunkingStrategy::MARKDOWN_AWARE;
        
        chunks.push_back(std::move(chunk));
        
        start = end;
        chunk_index++;
    }
    
    linkChunks(chunks);
    return chunks;
}

std::vector<MarkdownChunker::MarkdownSection> MarkdownChunker::parseMarkdownStructure(
    const std::string& text) const {
    // Parse markdown headers and sections
    // Placeholder implementation
    return {};
}

std::vector<DocumentChunk> MarkdownChunker::chunksFromSections(
    const std::vector<MarkdownSection>& sections,
    const std::string& document_hash) const {
    // Convert markdown sections to chunks
    // Placeholder implementation
    return {};
}

} // namespace yams::vector