/**
 * @file zpdf_wrapper.h
 * @brief C++ RAII wrapper for zpdf library
 */

#pragma once

#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>

namespace yams::zyp {

/**
 * RAII wrapper for zpdf text buffer.
 * Automatically frees the buffer on destruction.
 */
class TextBuffer {
public:
    TextBuffer() = default;
    TextBuffer(uint8_t* data, size_t len);
    ~TextBuffer();

    // Move-only
    TextBuffer(TextBuffer&& other) noexcept;
    TextBuffer& operator=(TextBuffer&& other) noexcept;
    TextBuffer(const TextBuffer&) = delete;
    TextBuffer& operator=(const TextBuffer&) = delete;

    /** Get text as string_view */
    [[nodiscard]] std::string_view text() const;

    /** Check if buffer is empty */
    [[nodiscard]] bool empty() const;

    /** Check if buffer is valid */
    [[nodiscard]] explicit operator bool() const;

    /** Get raw data pointer */
    [[nodiscard]] const uint8_t* data() const { return data_; }

    /** Get buffer length */
    [[nodiscard]] size_t size() const { return len_; }

private:
    uint8_t* data_ = nullptr;
    size_t len_ = 0;
};

/**
 * Page information structure.
 */
struct PageInfo {
    double width;  /**< Page width in points */
    double height; /**< Page height in points */
    int rotation;  /**< Rotation in degrees (0, 90, 180, 270) */
};

/**
 * RAII wrapper for zpdf document handle.
 */
class Document {
public:
    /**
     * Open a PDF document from a file path.
     * @param path File path
     * @return Document if successful, nullopt on error
     */
    [[nodiscard]] static std::optional<Document> open(std::string_view path);

    /**
     * Open a PDF document from a memory buffer.
     * @param data PDF data
     * @return Document if successful, nullopt on error
     */
    [[nodiscard]] static std::optional<Document> openMemory(std::span<const uint8_t> data);

    ~Document();

    // Move-only
    Document(Document&& other) noexcept;
    Document& operator=(Document&& other) noexcept;
    Document(const Document&) = delete;
    Document& operator=(const Document&) = delete;

    /** Get page count */
    [[nodiscard]] int pageCount() const;

    /** Get page information */
    [[nodiscard]] std::optional<PageInfo> pageInfo(int pageNum) const;

    /** Extract text from a single page */
    [[nodiscard]] TextBuffer extractPage(int pageNum) const;

    /** Extract text from all pages (sequential) */
    [[nodiscard]] TextBuffer extractAll() const;

    /** Extract text from all pages (parallel - fastest) */
    [[nodiscard]] TextBuffer extractAllParallel() const;

    /** Extract text in visual reading order (experimental) */
    [[nodiscard]] TextBuffer extractAllReadingOrder() const;

    /** Check if document handle is valid */
    [[nodiscard]] bool valid() const;

private:
    explicit Document(void* handle);
    void* handle_ = nullptr;
};

} // namespace yams::zyp
