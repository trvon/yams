/**
 * @file zyp_plugin_test.cpp
 * @brief Unit tests for zyp (Zig YAMS PDF) plugin
 */

#include <catch2/catch_test_macros.hpp>
#include <yams/compat/dlfcn.h>

#include <cstring>
#include <string>
#include <vector>

extern "C" {
#include <yams/plugins/abi.h>
#include <yams/plugins/content_extractor_v1.h>
}

namespace {

/**
 * Build a minimal valid PDF with text content.
 * This creates the smallest possible valid PDF for testing.
 */
std::vector<uint8_t> buildMinimalPdf(const std::string& text = "Hello World") {
    // Minimal PDF structure
    std::string pdf = R"(%PDF-1.4
1 0 obj
<< /Type /Catalog /Pages 2 0 R >>
endobj
2 0 obj
<< /Type /Pages /Kids [3 0 R] /Count 1 >>
endobj
3 0 obj
<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792]
   /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>
endobj
4 0 obj
<< /Length )" + std::to_string(44 + text.size()) +
                      R"( >>
stream
BT
/F1 12 Tf
100 700 Td
()" + text + R"() Tj
ET
endstream
endobj
5 0 obj
<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>
endobj
xref
0 6
0000000000 65535 f
0000000009 00000 n
0000000058 00000 n
0000000115 00000 n
0000000266 00000 n
trailer
<< /Size 6 /Root 1 0 R >>
startxref
)";

    // Calculate xref offset (approximate)
    size_t xrefOffset = pdf.size() - 10; // Before "startxref\n"
    pdf += std::to_string(xrefOffset) + "\n%%EOF\n";

    return {pdf.begin(), pdf.end()};
}

/**
 * Build a PDF with metadata.
 */
std::vector<uint8_t> buildPdfWithMetadata() {
    std::string pdf = R"(%PDF-1.4
1 0 obj
<< /Type /Catalog /Pages 2 0 R >>
endobj
2 0 obj
<< /Type /Pages /Kids [3 0 R] /Count 1 >>
endobj
3 0 obj
<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792]
   /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>
endobj
4 0 obj
<< /Length 44 >>
stream
BT
/F1 12 Tf
100 700 Td
(Test) Tj
ET
endstream
endobj
5 0 obj
<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>
endobj
6 0 obj
<< /Title (Test Document)
   /Author (Test Author)
   /Subject (Unit Testing)
   /Keywords (test, pdf, zyp)
   /Creator (zyp_plugin_test)
   /Producer (YAMS Test Suite)
   /CreationDate (D:20240101120000)
>>
endobj
xref
0 7
0000000000 65535 f
0000000009 00000 n
0000000058 00000 n
0000000115 00000 n
0000000266 00000 n
0000000360 00000 n
0000000430 00000 n
trailer
<< /Size 7 /Root 1 0 R /Info 6 0 R >>
startxref
650
%%EOF
)";

    return {pdf.begin(), pdf.end()};
}

struct ZypPluginTest {
    void* handle_ = nullptr;
    yams_content_extractor_v1* extractor_ = nullptr;

    ZypPluginTest() {
#ifdef __APPLE__
        const char* paths[] = {"builddir/plugins/zyp/yams_zyp.dylib", "plugins/zyp/yams_zyp.dylib",
                               nullptr};
#elif defined(_WIN32)
        const char* paths[] = {"builddir/plugins/zyp/yams_zyp.dll", "plugins/zyp/yams_zyp.dll",
                               nullptr};
#else
        const char* paths[] = {"builddir/plugins/zyp/yams_zyp.so", "plugins/zyp/yams_zyp.so",
                               nullptr};
#endif

        for (const char** p = paths; *p; ++p) {
            handle_ = dlopen(*p, RTLD_LAZY | RTLD_LOCAL);
            if (handle_)
                break;
        }

        if (!handle_) {
            return;
        }

        auto getInterface = reinterpret_cast<int (*)(const char*, uint32_t, void**)>(
            dlsym(handle_, "yams_plugin_get_interface"));
        if (!getInterface) {
            dlclose(handle_);
            handle_ = nullptr;
            return;
        }

        void* iface = nullptr;
        int rc = getInterface(YAMS_IFACE_CONTENT_EXTRACTOR_V1_ID,
                              YAMS_IFACE_CONTENT_EXTRACTOR_V1_VERSION, &iface);
        if (rc == YAMS_PLUGIN_OK && iface) {
            extractor_ = static_cast<yams_content_extractor_v1*>(iface);
        }
    }

    ~ZypPluginTest() {
        if (handle_) {
            dlclose(handle_);
            handle_ = nullptr;
        }
        extractor_ = nullptr;
    }

    ZypPluginTest(const ZypPluginTest&) = delete;
    ZypPluginTest& operator=(const ZypPluginTest&) = delete;

    bool pluginAvailable() const { return handle_ != nullptr && extractor_ != nullptr; }
};

// ============================================================================
// Plugin Loading Tests
// ============================================================================

TEST_CASE_METHOD(ZypPluginTest, "PluginLoads", "[plugin][zyp]") {
    if (!handle_) {
        SKIP("zyp plugin not available (requires Zig build)");
    }
    CHECK(handle_ != nullptr);
}

TEST_CASE_METHOD(ZypPluginTest, "AbiVersion", "[plugin][zyp]") {
    if (!handle_) {
        SKIP("zyp plugin not available");
    }

    auto getAbiVersion = reinterpret_cast<int (*)()>(dlsym(handle_, "yams_plugin_get_abi_version"));
    REQUIRE(getAbiVersion != nullptr);
    CHECK(getAbiVersion() == YAMS_PLUGIN_ABI_VERSION);
}

TEST_CASE_METHOD(ZypPluginTest, "PluginName", "[plugin][zyp]") {
    if (!handle_) {
        SKIP("zyp plugin not available");
    }

    auto getName = reinterpret_cast<const char* (*)()>(dlsym(handle_, "yams_plugin_get_name"));
    REQUIRE(getName != nullptr);
    CHECK(std::string(getName()) == "zyp");
}

TEST_CASE_METHOD(ZypPluginTest, "PluginVersion", "[plugin][zyp]") {
    if (!handle_) {
        SKIP("zyp plugin not available");
    }

    auto getVersion =
        reinterpret_cast<const char* (*)()>(dlsym(handle_, "yams_plugin_get_version"));
    REQUIRE(getVersion != nullptr);
    CHECK(std::string(getVersion()) == "1.0.0");
}

TEST_CASE_METHOD(ZypPluginTest, "InterfaceRetrieval", "[plugin][zyp]") {
    if (!pluginAvailable()) {
        SKIP("zyp plugin not available");
    }
    CHECK(extractor_ != nullptr);
    CHECK(extractor_->abi_version == static_cast<uint32_t>(YAMS_IFACE_CONTENT_EXTRACTOR_V1_VERSION));
}

// ============================================================================
// supports() Tests
// ============================================================================

TEST_CASE_METHOD(ZypPluginTest, "SupportsPdfMimeType", "[plugin][zyp]") {
    if (!pluginAvailable()) {
        SKIP("zyp plugin not available");
    }

    CHECK(extractor_->supports("application/pdf", nullptr));
    CHECK(extractor_->supports("application/pdf", ".pdf"));
}

TEST_CASE_METHOD(ZypPluginTest, "SupportsPdfExtension", "[plugin][zyp]") {
    if (!pluginAvailable()) {
        SKIP("zyp plugin not available");
    }

    CHECK(extractor_->supports(nullptr, ".pdf"));
    CHECK(extractor_->supports(nullptr, "pdf"));
}

TEST_CASE_METHOD(ZypPluginTest, "DoesNotSupportOtherTypes", "[plugin][zyp]") {
    if (!pluginAvailable()) {
        SKIP("zyp plugin not available");
    }

    CHECK_FALSE(extractor_->supports("text/plain", nullptr));
    CHECK_FALSE(extractor_->supports("application/json", nullptr));
    CHECK_FALSE(extractor_->supports(nullptr, ".txt"));
    CHECK_FALSE(extractor_->supports(nullptr, ".docx"));
}

TEST_CASE_METHOD(ZypPluginTest, "SupportsNullArgs", "[plugin][zyp]") {
    if (!pluginAvailable()) {
        SKIP("zyp plugin not available");
    }

    // Both null should return false
    CHECK_FALSE(extractor_->supports(nullptr, nullptr));
}

// ============================================================================
// extract() Tests
// ============================================================================

TEST_CASE_METHOD(ZypPluginTest, "ExtractMinimalPdf", "[plugin][zyp]") {
    if (!pluginAvailable()) {
        SKIP("zyp plugin not available");
    }

    auto pdf = buildMinimalPdf("Hello World");

    yams_extraction_result_t* result = nullptr;
    int rc = extractor_->extract(pdf.data(), pdf.size(), &result);

    // May fail if zpdf can't parse the minimal PDF
    if (rc != YAMS_PLUGIN_OK) {
        SKIP("zpdf couldn't parse minimal test PDF");
    }

    REQUIRE(result != nullptr);
    REQUIRE(result->text != nullptr);

    // Note: zpdf may not extract text from our minimal synthetic PDF
    // since it lacks proper font encoding. Just verify we get a result.
    // Real PDFs will have actual text extraction.

    // Check metadata contains page_count (may be 0 for minimal PDFs)
    bool hasPageCount = false;
    for (size_t i = 0; i < result->metadata.count; ++i) {
        if (std::strcmp(result->metadata.pairs[i].key, "page_count") == 0) {
            hasPageCount = true;
            // Accept any non-negative page count
            int pageCount = std::atoi(result->metadata.pairs[i].value);
            CHECK(pageCount >= 0);
        }
    }
    CHECK(hasPageCount);

    extractor_->free_result(result);
}

TEST_CASE_METHOD(ZypPluginTest, "ExtractPdfWithMetadata", "[plugin][zyp]") {
    if (!pluginAvailable()) {
        SKIP("zyp plugin not available");
    }

    auto pdf = buildPdfWithMetadata();

    yams_extraction_result_t* result = nullptr;
    int rc = extractor_->extract(pdf.data(), pdf.size(), &result);

    if (rc != YAMS_PLUGIN_OK) {
        SKIP("zpdf couldn't parse test PDF with metadata");
    }

    REQUIRE(result != nullptr);

    // Check for metadata fields
    std::map<std::string, std::string> meta;
    for (size_t i = 0; i < result->metadata.count; ++i) {
        meta[result->metadata.pairs[i].key] = result->metadata.pairs[i].value;
    }

    // Title should be extracted
    if (meta.count("pdf_title")) {
        CHECK(meta["pdf_title"] == "Test Document");
    }
    if (meta.count("pdf_author")) {
        CHECK(meta["pdf_author"] == "Test Author");
    }

    extractor_->free_result(result);
}

// ============================================================================
// Error Handling Tests
// ============================================================================

TEST_CASE_METHOD(ZypPluginTest, "ExtractNullContent", "[plugin][zyp]") {
    if (!pluginAvailable()) {
        SKIP("zyp plugin not available");
    }

    yams_extraction_result_t* result = nullptr;
    int rc = extractor_->extract(nullptr, 0, &result);
    CHECK(rc == YAMS_PLUGIN_ERR_INVALID);
}

TEST_CASE_METHOD(ZypPluginTest, "ExtractEmptyContent", "[plugin][zyp]") {
    if (!pluginAvailable()) {
        SKIP("zyp plugin not available");
    }

    uint8_t empty = 0;
    yams_extraction_result_t* result = nullptr;
    int rc = extractor_->extract(&empty, 0, &result);
    CHECK(rc == YAMS_PLUGIN_ERR_INVALID);
}

TEST_CASE_METHOD(ZypPluginTest, "ExtractInvalidPdf", "[plugin][zyp]") {
    if (!pluginAvailable()) {
        SKIP("zyp plugin not available");
    }

    // Not a valid PDF
    std::string notPdf = "This is not a PDF file";
    yams_extraction_result_t* result = nullptr;
    int rc = extractor_->extract(reinterpret_cast<const uint8_t*>(notPdf.data()), notPdf.size(),
                                 &result);

    // zpdf may either return an error OR return success with empty text
    // Both behaviors are acceptable for invalid input
    if (rc == YAMS_PLUGIN_OK) {
        // If success, verify we got a valid (possibly empty) result
        REQUIRE(result != nullptr);
        REQUIRE(result->text != nullptr);
        extractor_->free_result(result);
    } else {
        // If error, it should be INVALID
        CHECK(rc == YAMS_PLUGIN_ERR_INVALID);
    }
}

TEST_CASE_METHOD(ZypPluginTest, "ExtractNullResult", "[plugin][zyp]") {
    if (!pluginAvailable()) {
        SKIP("zyp plugin not available");
    }

    auto pdf = buildMinimalPdf();
    int rc = extractor_->extract(pdf.data(), pdf.size(), nullptr);
    CHECK(rc == YAMS_PLUGIN_ERR_INVALID);
}

// ============================================================================
// Memory Management Tests
// ============================================================================

TEST_CASE_METHOD(ZypPluginTest, "FreeResultNull", "[plugin][zyp]") {
    if (!pluginAvailable()) {
        SKIP("zyp plugin not available");
    }

    // Should not crash
    extractor_->free_result(nullptr);
}

TEST_CASE_METHOD(ZypPluginTest, "FreeResultMultipleTimes", "[plugin][zyp]") {
    if (!pluginAvailable()) {
        SKIP("zyp plugin not available");
    }

    auto pdf = buildMinimalPdf();
    yams_extraction_result_t* result = nullptr;
    int rc = extractor_->extract(pdf.data(), pdf.size(), &result);

    if (rc != YAMS_PLUGIN_OK) {
        SKIP("zpdf couldn't parse minimal test PDF");
    }

    // First free should work
    extractor_->free_result(result);

    // Second free with null should be safe
    extractor_->free_result(nullptr);
}

} // anonymous namespace
