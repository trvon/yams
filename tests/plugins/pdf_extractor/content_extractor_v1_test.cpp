#include <gtest/gtest.h>
#include <yams/daemon/resource/abi_plugin_loader.h>
#include <yams/plugins/abi.h>
#include <yams/plugins/content_extractor_v1.h>

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <optional>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace {

// Return first directory that looks like the plugin build output.
// Order:
// 1) YAMS_PLUGIN_DIR env (if set)
// 2) CWD
// 3) CWD/plugins/pdf_extractor_build
// 4) CWD/../plugins/pdf_extractor_build
// 5) CWD/../../plugins/pdf_extractor_build
std::optional<fs::path> findCandidatePluginDir() {
    if (const char* env = std::getenv("YAMS_PLUGIN_DIR")) {
        fs::path p(env);
        std::error_code ec;
        if (fs::exists(p, ec) && fs::is_directory(p, ec))
            return p;
    }

    std::vector<fs::path> candidates;
    const fs::path cwd = fs::current_path();
    candidates.emplace_back(cwd);
    candidates.emplace_back(cwd / "plugins" / "pdf_extractor_build");
    candidates.emplace_back(cwd.parent_path() / "plugins" / "pdf_extractor_build");
    candidates.emplace_back(cwd.parent_path().parent_path() / "plugins" / "pdf_extractor_build");

    std::error_code ec;
    for (const auto& c : candidates) {
        if (fs::exists(c, ec) && fs::is_directory(c, ec)) {
            return c;
        }
    }
    return std::nullopt;
}

// Find a plugin binary whose name contains "pdf_extractor" and extension is platform-appropriate.
std::optional<fs::path> findPdfExtractorBinary(const fs::path& dir) {
#if defined(_WIN32)
    const std::string ext = ".dll";
#elif defined(__APPLE__)
    const std::string ext = ".dylib";
#else
    const std::string ext = ".so";
#endif

    std::error_code ec;
    for (const auto& entry : fs::directory_iterator(dir, ec)) {
        if (ec)
            break;
        if (!entry.is_regular_file(ec))
            continue;
        const auto p = entry.path();
        if (p.extension().string() == ext) {
            std::string file = p.filename().string();
            if (file.find("pdf_extractor") != std::string::npos) {
                return p;
            }
        }
    }
    return std::nullopt;
}

// Create a minimal PDF file content with a line of text.
// This is a simplified PDF sufficient for PDFium to parse and extract "textContent".
std::vector<uint8_t> buildMinimalPdfWithText(const std::string& textContent) {
    std::string pdfContent = "%PDF-1.4\n"
                             "1 0 obj\n"
                             "<<\n"
                             "/Type /Catalog\n"
                             "/Pages 2 0 R\n"
                             ">>\n"
                             "endobj\n"
                             "2 0 obj\n"
                             "<<\n"
                             "/Type /Pages\n"
                             "/Kids [3 0 R]\n"
                             "/Count 1\n"
                             ">>\n"
                             "endobj\n"
                             "3 0 obj\n"
                             "<<\n"
                             "/Type /Page\n"
                             "/Parent 2 0 R\n"
                             "/MediaBox [0 0 612 792]\n"
                             "/Contents 4 0 R\n"
                             "/Resources << /Font << /F1 5 0 R >> >>\n"
                             ">>\n"
                             "endobj\n";

    // Build the stream with textContent. The length is approximate but sufficient for parsers.
    std::string stream = "4 0 obj\n"
                         "<<\n"
                         "/Length ";
    // Fudge factor for operators
    size_t stream_payload_len =
        std::string("BT\n/F1 12 Tf\n100 700 Td\n() Tj\nET\n").size() + textContent.size();
    stream += std::to_string(stream_payload_len) +
              "\n"
              ">>\n"
              "stream\n"
              "BT\n"
              "/F1 12 Tf\n"
              "100 700 Td\n"
              "(" +
              textContent +
              ") Tj\n"
              "ET\n"
              "endstream\n"
              "endobj\n";

    std::string rest = "5 0 obj\n"
                       "<<\n"
                       "/Type /Font\n"
                       "/Subtype /Type1\n"
                       "/BaseFont /Helvetica\n"
                       ">>\n"
                       "endobj\n"
                       "xref\n"
                       "0 6\n"
                       "0000000000 65535 f \n"
                       "0000000009 00000 n \n"
                       "0000000058 00000 n \n"
                       "0000000115 00000 n \n"
                       "0000000268 00000 n \n"
                       "0000000400 00000 n \n"
                       "trailer\n"
                       "<<\n"
                       "/Size 6\n"
                       "/Root 1 0 R\n"
                       ">>\n"
                       "startxref\n"
                       "484\n"
                       "%%EOF\n";

    std::string all = pdfContent + stream + rest;
    return std::vector<uint8_t>(all.begin(), all.end());
}

fs::path writeTempPdf(const std::vector<uint8_t>& bytes, const std::string& name) {
    auto tmp = fs::temp_directory_path() / name;
    std::ofstream out(tmp, std::ios::binary);
    out.write(reinterpret_cast<const char*>(bytes.data()),
              static_cast<std::streamsize>(bytes.size()));
    out.close();
    return tmp;
}

} // namespace

TEST(ContentExtractorV1, LoadPdfExtractorAndExtractFromBufferAndFile) {
    using yams::daemon::AbiPluginLoader;

    // Locate plugin directory and binary
    auto pluginDirOpt = findCandidatePluginDir();
    ASSERT_TRUE(pluginDirOpt.has_value()) << "Could not locate candidate plugin dir. "
                                          << "Set YAMS_PLUGIN_DIR to override search.";
    const fs::path pluginDir = *pluginDirOpt;

    auto pluginFileOpt = findPdfExtractorBinary(pluginDir);
    ASSERT_TRUE(pluginFileOpt.has_value())
        << "No pdf_extractor plugin binary found in: " << pluginDir;
    const fs::path pluginFile = *pluginFileOpt;

    AbiPluginLoader loader;

    // Trust policy: use a per-test trust file and trust the pluginDir
    const fs::path trustFile = fs::current_path() / "yams_test_trust_plugins.txt";
    loader.setTrustFile(trustFile);
    auto tr = loader.trustAdd(pluginDir);
    ASSERT_TRUE(tr) << "trustAdd failed";

    // Load plugin
    auto loadRes = loader.load(pluginFile, "{}");
    ASSERT_TRUE(loadRes) << "Failed to load plugin: " << loadRes.error().message;
    EXPECT_EQ(loadRes.value().name, "pdf_extractor");

    // Get the content_extractor_v1 interface
    auto ifaceRes = loader.getInterface("pdf_extractor", YAMS_IFACE_CONTENT_EXTRACTOR_V1_ID,
                                        YAMS_IFACE_CONTENT_EXTRACTOR_V1_VERSION);
    ASSERT_TRUE(ifaceRes) << "getInterface failed: " << ifaceRes.error().message;
    auto* ce = reinterpret_cast<yams_content_extractor_v1*>(ifaceRes.value());
    ASSERT_NE(ce, nullptr);
    EXPECT_EQ(ce->abi_version, static_cast<uint32_t>(YAMS_IFACE_CONTENT_EXTRACTOR_V1_VERSION));

    // Supports should recognize PDFs
    EXPECT_TRUE(ce->supports("application/pdf", ".pdf"));
    EXPECT_FALSE(ce->supports("text/plain", ".txt"));

    // Build a minimal PDF and extract from buffer
    const std::string kText = "Hello World from PDF";
    auto pdfBytes = buildMinimalPdfWithText(kText);
    yams_extraction_result_t* result = nullptr;
    int rc =
        ce->extract(reinterpret_cast<const uint8_t*>(pdfBytes.data()), pdfBytes.size(), &result);
    ASSERT_EQ(rc, YAMS_PLUGIN_OK);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->error, nullptr);
    ASSERT_NE(result->text, nullptr);
    std::string extracted = result->text;
    EXPECT_NE(extracted.find("Hello"), std::string::npos);
    // Free plugin-allocated result
    ce->free_result(result);
    result = nullptr;

    // Extract from "file" (read back into memory; ABI provides buffer-based extraction)
    auto tmpPdf = writeTempPdf(pdfBytes, "yams_pdf_extractor_abi_test.pdf");
    ASSERT_TRUE(fs::exists(tmpPdf));
    // Read file into memory
    std::ifstream in(tmpPdf, std::ios::binary | std::ios::ate);
    ASSERT_TRUE(in.good());
    auto sz = in.tellg();
    in.seekg(0, std::ios::beg);
    std::vector<uint8_t> fileBuf(static_cast<size_t>(sz));
    in.read(reinterpret_cast<char*>(fileBuf.data()), sz);
    in.close();

    rc = ce->extract(fileBuf.data(), fileBuf.size(), &result);
    ASSERT_EQ(rc, YAMS_PLUGIN_OK);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->error, nullptr);
    ASSERT_NE(result->text, nullptr);
    std::string extracted2 = result->text;
    EXPECT_NE(extracted2.find("Hello"), std::string::npos);
    ce->free_result(result);
    result = nullptr;

    // Cleanup temp file
    std::error_code ec;
    fs::remove(tmpPdf, ec);

    // Unload plugin
    auto unloadRes = loader.unload("pdf_extractor");
    EXPECT_TRUE(unloadRes) << "Unload failed: " << unloadRes.error().message;
}