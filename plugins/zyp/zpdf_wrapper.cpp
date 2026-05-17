/**
 * @file zpdf_wrapper.cpp
 * @brief C++ RAII wrapper implementation for zpdf library
 */

#include "zpdf_wrapper.h"
#include "zpdf.h"

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <future>
#include <sstream>
#include <thread>
#include <utility>

namespace yams::zyp {

// ============================================================================
// TextBuffer
// ============================================================================

TextBuffer::TextBuffer(uint8_t* data, size_t len)
    : TextBuffer(data, len, TextBuffer::Deallocator::Zpdf) {}

TextBuffer::TextBuffer(uint8_t* data, size_t len, TextBuffer::Deallocator deallocator)
    : data_(data), len_(len), deallocator_(deallocator) {}

TextBuffer TextBuffer::fromMalloc(uint8_t* data, size_t len) {
    return TextBuffer(data, len, TextBuffer::Deallocator::Malloc);
}

TextBuffer::~TextBuffer() {
    release();
}

void TextBuffer::release() noexcept {
    if (data_) {
        if (deallocator_ == TextBuffer::Deallocator::Malloc) {
            std::free(data_);
        } else {
            zpdf_free_buffer(data_, len_);
        }
        data_ = nullptr;
        len_ = 0;
    }
}

TextBuffer::TextBuffer(TextBuffer&& other) noexcept
    : data_(std::exchange(other.data_, nullptr)), len_(std::exchange(other.len_, 0)),
      deallocator_(std::exchange(other.deallocator_, TextBuffer::Deallocator::Zpdf)) {}

TextBuffer& TextBuffer::operator=(TextBuffer&& other) noexcept {
    if (this != &other) {
        release();
        data_ = std::exchange(other.data_, nullptr);
        len_ = std::exchange(other.len_, 0);
        deallocator_ = std::exchange(other.deallocator_, TextBuffer::Deallocator::Zpdf);
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

TextBuffer Document::extractAllParallelized(std::span<const uint8_t> data, int numThreads) {
    if (data.empty())
        return {};

    if (numThreads <= 0)
        numThreads = static_cast<int>(std::thread::hardware_concurrency());
    if (numThreads < 1)
        numThreads = 1;

    auto probeDoc = Document::openMemory(data);
    if (!probeDoc)
        return {};
    const int totalPages = probeDoc->pageCount();
    probeDoc.reset();
    if (totalPages <= 0)
        return {};

    if (numThreads > totalPages)
        numThreads = totalPages;
    if (numThreads <= 1) {
        auto doc = Document::openMemory(data);
        if (!doc)
            return {};
        return doc->extractAll();
    }

    // Open per-thread documents and distribute pages
    const int pagesPerThread = (totalPages + numThreads - 1) / numThreads;
    std::vector<std::future<std::pair<int, std::string>>> futures;
    futures.reserve(static_cast<size_t>(numThreads));

    for (int t = 0; t < numThreads; ++t) {
        const int startPage = t * pagesPerThread;
        const int endPage = std::min(startPage + pagesPerThread, totalPages);
        if (startPage >= totalPages)
            break;

        futures.push_back(std::async(
            std::launch::async, [data, startPage, endPage]() -> std::pair<int, std::string> {
                auto doc = Document::openMemory(data);
                if (!doc)
                    return {startPage, {}};

                std::ostringstream oss;
                for (int p = startPage; p < endPage; ++p) {
                    auto pageText = doc->extractPage(p);
                    auto view = pageText.text();
                    if (!view.empty())
                        oss.write(view.data(), static_cast<std::streamsize>(view.size()));
                    oss << '\n';
                }
                return {startPage, oss.str()};
            }));
    }

    // Collect results in page order
    std::vector<std::pair<int, std::string>> results;
    results.reserve(futures.size());
    for (auto& f : futures) {
        try {
            results.push_back(f.get());
        } catch (const std::exception& e) {
            (void)e; // thread failed; skip its pages
        }
    }
    std::sort(results.begin(), results.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });

    std::ostringstream combined;
    for (auto& [_, text] : results) {
        if (!text.empty())
            combined << text;
    }
    const std::string finalText = combined.str();
    if (finalText.empty())
        return {};

    auto* buf = static_cast<uint8_t*>(std::malloc(finalText.size() + 1));
    if (!buf)
        return {};
    std::memcpy(buf, finalText.data(), finalText.size());
    buf[finalText.size()] = '\0';
    return TextBuffer::fromMalloc(buf, finalText.size());
}

} // namespace yams::zyp
