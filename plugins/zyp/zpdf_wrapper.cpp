/**
 * @file zpdf_wrapper.cpp
 * @brief C++ RAII wrapper implementation for zpdf library
 */

#include "zpdf_wrapper.h"
#include "zpdf.h"

#include <cstring>
#include <utility>

namespace yams::zyp {

// ============================================================================
// TextBuffer
// ============================================================================

TextBuffer::TextBuffer(uint8_t* data, size_t len) : data_(data), len_(len) {}

TextBuffer::~TextBuffer() {
    if (data_) {
        zpdf_free_buffer(data_, len_);
    }
}

TextBuffer::TextBuffer(TextBuffer&& other) noexcept
    : data_(std::exchange(other.data_, nullptr)), len_(std::exchange(other.len_, 0)) {}

TextBuffer& TextBuffer::operator=(TextBuffer&& other) noexcept {
    if (this != &other) {
        if (data_) {
            zpdf_free_buffer(data_, len_);
        }
        data_ = std::exchange(other.data_, nullptr);
        len_ = std::exchange(other.len_, 0);
    }
    return *this;
}

std::string_view TextBuffer::text() const {
    if (!data_ || len_ == 0) {
        return {};
    }
    return {reinterpret_cast<const char*>(data_), len_};
}

bool TextBuffer::empty() const {
    return !data_ || len_ == 0;
}

TextBuffer::operator bool() const {
    return data_ != nullptr;
}

// ============================================================================
// Document
// ============================================================================

Document::Document(void* handle) : handle_(handle) {}

Document::~Document() {
    if (handle_) {
        zpdf_close(static_cast<ZpdfDocument*>(handle_));
    }
}

Document::Document(Document&& other) noexcept : handle_(std::exchange(other.handle_, nullptr)) {}

Document& Document::operator=(Document&& other) noexcept {
    if (this != &other) {
        if (handle_) {
            zpdf_close(static_cast<ZpdfDocument*>(handle_));
        }
        handle_ = std::exchange(other.handle_, nullptr);
    }
    return *this;
}

std::optional<Document> Document::open(std::string_view path) {
    // Ensure null-terminated string
    std::string pathStr(path);
    auto* handle = zpdf_open(pathStr.c_str());
    if (!handle) {
        return std::nullopt;
    }
    return Document(handle);
}

std::optional<Document> Document::openMemory(std::span<const uint8_t> data) {
    if (data.empty()) {
        return std::nullopt;
    }
    auto* handle = zpdf_open_memory(data.data(), data.size());
    if (!handle) {
        return std::nullopt;
    }
    return Document(handle);
}

int Document::pageCount() const {
    if (!handle_) {
        return 0;
    }
    int count = zpdf_page_count(static_cast<ZpdfDocument*>(handle_));
    return count >= 0 ? count : 0;
}

std::optional<PageInfo> Document::pageInfo(int pageNum) const {
    if (!handle_) {
        return std::nullopt;
    }

    PageInfo info{};
    int result = zpdf_get_page_info(static_cast<ZpdfDocument*>(handle_), pageNum, &info.width,
                                    &info.height, &info.rotation);

    if (result != 0) {
        return std::nullopt;
    }
    return info;
}

TextBuffer Document::extractPage(int pageNum) const {
    if (!handle_) {
        return {};
    }

    size_t len = 0;
    uint8_t* data = zpdf_extract_page(static_cast<ZpdfDocument*>(handle_), pageNum, &len);

    return TextBuffer(data, len);
}

TextBuffer Document::extractAll() const {
    if (!handle_) {
        return {};
    }

    size_t len = 0;
    uint8_t* data = zpdf_extract_all(static_cast<ZpdfDocument*>(handle_), &len);

    return TextBuffer(data, len);
}

TextBuffer Document::extractAllParallel() const {
    if (!handle_) {
        return {};
    }

    size_t len = 0;
    uint8_t* data = zpdf_extract_all_parallel(static_cast<ZpdfDocument*>(handle_), &len);

    return TextBuffer(data, len);
}

TextBuffer Document::extractAllReadingOrder() const {
    if (!handle_) {
        return {};
    }

    size_t len = 0;
    uint8_t* data =
        zpdf_extract_all_reading_order_parallel(static_cast<ZpdfDocument*>(handle_), &len);

    return TextBuffer(data, len);
}

bool Document::valid() const {
    return handle_ != nullptr;
}

} // namespace yams::zyp
