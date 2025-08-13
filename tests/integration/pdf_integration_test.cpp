#include <gtest/gtest.h>
#include <yams/api/content_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/database.h>
#include <yams/extraction/pdf_extractor.h>
#include "../../common/test_data_generator.h"
#include "../../common/fixture_manager.h"
#include <filesystem>
#include <chrono>

using namespace yams;
using namespace yams::api;
using namespace yams::metadata;
using namespace yams::extraction;
using namespace yams::test;

class PDFIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        testDir_ = std::filesystem::temp_directory_path() / "yams_pdf_integration_test";
        std::filesystem::create_directories(testDir_);
        
        initializeYAMS();
        generator_ = std::make_unique<TestDataGenerator>();
        fixtureManager_ = std::make_unique<FixtureManager>();
    }
    
    void TearDown() override {
        contentStore_.reset();
        metadataRepo_.reset();
        database_.reset();
        fixtureManager_->cleanupFixtures();
        std::filesystem::remove_all(testDir_);
    }
    
    void initializeYAMS() {
        auto dbPath = testDir_ / "yams.db";
        database_ = std::make_shared<Database>(dbPath.string());
        database_->initialize();
        metadataRepo_ = std::make_shared<MetadataRepository>(database_);
        contentStore_ = std::make_unique<ContentStore>(testDir_.string());
    }
    
    // Helper to create a test PDF
    std::filesystem::path createTestPDF(const std::string& name, size_t pages) {
        auto pdfData = generator_->generatePDF(pages);
        auto pdfPath = testDir_ / name;
        std::ofstream file(pdfPath, std::ios::binary);
        file.write(reinterpret_cast<const char*>(pdfData.data()), pdfData.size());
        file.close();
        return pdfPath;
    }
    
    std::filesystem::path testDir_;
    std::shared_ptr<Database> database_;
    std::shared_ptr<MetadataRepository> metadataRepo_;
    std::unique_ptr<ContentStore> contentStore_;
    std::unique_ptr<TestDataGenerator> generator_;
    std::unique_ptr<FixtureManager> fixtureManager_;
};

TEST_F(PDFIntegrationTest, PDFIngestionAndSearch) {
    // Create test PDFs with different content
    auto pdf1 = createTestPDF("research_paper.pdf", 10);
    auto pdf2 = createTestPDF("technical_report.pdf", 5);
    auto pdf3 = createTestPDF("user_manual.pdf", 20);
    
    // Ingest PDFs
    auto result1 = contentStore_->addFile(pdf1, {"research", "pdf"});
    ASSERT_TRUE(result1.has_value()) << "Failed to ingest first PDF";
    
    auto result2 = contentStore_->addFile(pdf2, {"technical", "pdf"});
    ASSERT_TRUE(result2.has_value()) << "Failed to ingest second PDF";
    
    auto result3 = contentStore_->addFile(pdf3, {"manual", "pdf"});
    ASSERT_TRUE(result3.has_value()) << "Failed to ingest third PDF";
    
    // Search for PDF content
    auto searchResult = contentStore_->search("Page");
    ASSERT_TRUE(searchResult.has_value()) << "Search failed";
    
    // Search with tags
    search::SearchOptions options;
    options.tags = {"pdf"};
    auto taggedResults = contentStore_->searchWithOptions("*", options);
    EXPECT_GE(taggedResults.size(), 3) << "Not all PDFs found";
    
    // Verify extracted text is searchable
    for (const auto& result : taggedResults) {
        auto content = contentStore_->getContent(result.document.hash);
        ASSERT_TRUE(content.has_value()) << "Failed to retrieve PDF content";
        
        // Content should contain extracted text, not binary PDF data
        std::string textContent(content->begin(), content->end());
        EXPECT_TRUE(textContent.find("Page") != std::string::npos) 
            << "Extracted text not found in content";
    }
}

TEST_F(PDFIntegrationTest, PDFMetadataExtraction) {
    // Use fixture PDF with known metadata
    auto fixture = FixtureManager::getComplexPDF();
    
    // Copy to test directory
    auto pdfPath = testDir_ / "metadata_test.pdf";
    std::filesystem::copy_file(fixture.path, pdfPath);
    
    // Ingest PDF
    auto result = contentStore_->addFile(pdfPath);
    ASSERT_TRUE(result.has_value()) << "Failed to ingest PDF";
    
    // Get document metadata
    auto docResult = metadataRepo_->getDocumentByHash(result->contentHash);
    ASSERT_TRUE(docResult.has_value() && docResult->has_value()) << "Document not found";
    
    auto docId = docResult->value()->id;
    
    // Check for extracted PDF metadata
    std::vector<std::string> expectedMetadataKeys = {
        "pdf_title",
        "pdf_author", 
        "pdf_subject",
        "pdf_keywords",
        "pdf_creator",
        "pdf_producer",
        "pdf_pages"
    };
    
    for (const auto& key : expectedMetadataKeys) {
        auto metadata = metadataRepo_->getMetadata(docId, key);
        // Some fields might not be present in all PDFs
        if (metadata.has_value()) {
            std::cout << key << ": " << metadata->toString() << std::endl;
        }
    }
    
    // At minimum, page count should be extracted
    auto pageCount = metadataRepo_->getMetadata(docId, "pdf_pages");
    if (!pageCount.has_value()) {
        pageCount = metadataRepo_->getMetadata(docId, "page_count");
    }
    EXPECT_TRUE(pageCount.has_value()) << "Page count not extracted";
}

TEST_F(PDFIntegrationTest, LargePDFHandling) {
    // Create a large PDF (100 pages)
    auto largePdf = createTestPDF("large_document.pdf", 100);
    
    // Measure ingestion time
    auto startTime = std::chrono::high_resolution_clock::now();
    auto result = contentStore_->addFile(largePdf, {"large", "performance"});
    auto endTime = std::chrono::high_resolution_clock::now();
    
    ASSERT_TRUE(result.has_value()) << "Failed to ingest large PDF";
    
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
    std::cout << "Large PDF (100 pages) ingestion time: " << duration.count() << "ms" << std::endl;
    
    // Verify content is searchable
    auto searchResult = contentStore_->search("Page 50");
    ASSERT_TRUE(searchResult.has_value()) << "Search in large PDF failed";
    
    // Check memory usage didn't explode
    auto content = contentStore_->getContent(result->contentHash);
    ASSERT_TRUE(content.has_value());
    
    // Extracted text should be reasonable size (not the full PDF binary)
    EXPECT_LT(content->size(), 10 * 1024 * 1024) << "Extracted content too large";
}

TEST_F(PDFIntegrationTest, MixedDocumentTypeWorkflow) {
    // Test workflow with mixed document types (PDF, text, markdown)
    
    // Add PDF
    auto pdfPath = createTestPDF("document.pdf", 5);
    auto pdfResult = contentStore_->addFile(pdfPath, {"mixed", "pdf"});
    ASSERT_TRUE(pdfResult.has_value());
    
    // Add text document
    auto textContent = generator_->generateTextDocument(1024);
    auto textPath = testDir_ / "document.txt";
    std::ofstream(textPath) << textContent;
    auto textResult = contentStore_->addFile(textPath, {"mixed", "text"});
    ASSERT_TRUE(textResult.has_value());
    
    // Add markdown document
    auto mdContent = generator_->generateMarkdown(3);
    auto mdPath = testDir_ / "document.md";
    std::ofstream(mdPath) << mdContent;
    auto mdResult = contentStore_->addFile(mdPath, {"mixed", "markdown"});
    ASSERT_TRUE(mdResult.has_value());
    
    // Search across all document types
    search::SearchOptions options;
    options.tags = {"mixed"};
    auto results = contentStore_->searchWithOptions("*", options);
    
    EXPECT_EQ(results.size(), 3) << "Not all document types found";
    
    // Verify each document type
    std::map<std::string, bool> foundTypes = {
        {"pdf", false},
        {"text", false},
        {"markdown", false}
    };
    
    for (const auto& result : results) {
        auto docResult = metadataRepo_->getDocumentByHash(result.document.hash);
        ASSERT_TRUE(docResult.has_value() && docResult->has_value());
        
        auto doc = docResult->value();
        std::string extension = doc->path.extension().string();
        
        if (extension == ".pdf") foundTypes["pdf"] = true;
        else if (extension == ".txt") foundTypes["text"] = true;
        else if (extension == ".md") foundTypes["markdown"] = true;
    }
    
    for (const auto& [type, found] : foundTypes) {
        EXPECT_TRUE(found) << "Document type not found: " << type;
    }
}

TEST_F(PDFIntegrationTest, PDFWithImagesOnly) {
    // Test handling of PDFs that contain primarily images (no extractable text)
    // Note: This would require a special test PDF with images
    
    // For now, create a minimal PDF
    std::string minimalPdf = "%PDF-1.4\n"
                            "1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n"
                            "2 0 obj<</Type/Pages/Count 1/Kids[3 0 R]>>endobj\n"
                            "3 0 obj<</Type/Page/Parent 2 0 R/MediaBox[0 0 612 792]>>endobj\n"
                            "xref\n0 4\n"
                            "0000000000 65535 f\n"
                            "0000000009 00000 n\n"
                            "0000000056 00000 n\n"
                            "0000000108 00000 n\n"
                            "trailer<</Size 4/Root 1 0 R>>\n"
                            "startxref\n178\n%%EOF";
    
    auto pdfPath = testDir_ / "image_only.pdf";
    std::ofstream(pdfPath) << minimalPdf;
    
    // Should still ingest successfully
    auto result = contentStore_->addFile(pdfPath);
    ASSERT_TRUE(result.has_value()) << "Failed to ingest image-only PDF";
    
    // Content might be empty or minimal
    auto content = contentStore_->getContent(result->contentHash);
    ASSERT_TRUE(content.has_value());
    
    // Should handle gracefully even if no text extracted
    std::cout << "Image-only PDF content size: " << content->size() << " bytes" << std::endl;
}

TEST_F(PDFIntegrationTest, CorruptedPDFHandling) {
    // Test handling of corrupted PDFs
    
    // Create a corrupted PDF
    auto corruptedPdf = generator_->generateCorruptedPDF();
    auto pdfPath = testDir_ / "corrupted.pdf";
    std::ofstream file(pdfPath, std::ios::binary);
    file.write(reinterpret_cast<const char*>(corruptedPdf.data()), corruptedPdf.size());
    file.close();
    
    // Attempt to ingest
    auto result = contentStore_->addFile(pdfPath);
    
    // Should either fail gracefully or extract what it can
    if (result.has_value()) {
        // If it succeeded, verify we can retrieve something
        auto content = contentStore_->getContent(result->contentHash);
        EXPECT_TRUE(content.has_value()) << "Content retrieval failed for corrupted PDF";
    } else {
        // Failure is acceptable for corrupted files
        EXPECT_TRUE(true) << "Corrupted PDF correctly rejected";
    }
}

TEST_F(PDFIntegrationTest, PDFUpdateWorkflow) {
    // Test updating PDF documents with new versions
    
    // Add initial version
    auto v1Path = createTestPDF("document_v1.pdf", 5);
    auto v1Result = contentStore_->addFile(v1Path, {"document", "v1"});
    ASSERT_TRUE(v1Result.has_value());
    
    // Get document info
    auto v1DocResult = metadataRepo_->getDocumentByHash(v1Result->contentHash);
    ASSERT_TRUE(v1DocResult.has_value() && v1DocResult->has_value());
    auto v1DocId = v1DocResult->value()->id;
    
    // Set version metadata
    metadataRepo_->setMetadata(v1DocId, "version", MetadataValue(1.0));
    metadataRepo_->setMetadata(v1DocId, "status", MetadataValue("current"));
    
    // Add updated version
    auto v2Path = createTestPDF("document_v2.pdf", 8);
    auto v2Result = contentStore_->addFile(v2Path, {"document", "v2"});
    ASSERT_TRUE(v2Result.has_value());
    
    // Get v2 document info
    auto v2DocResult = metadataRepo_->getDocumentByHash(v2Result->contentHash);
    ASSERT_TRUE(v2DocResult.has_value() && v2DocResult->has_value());
    auto v2DocId = v2DocResult->value()->id;
    
    // Update metadata to reflect versioning
    metadataRepo_->setMetadata(v2DocId, "version", MetadataValue(2.0));
    metadataRepo_->setMetadata(v2DocId, "status", MetadataValue("current"));
    metadataRepo_->setMetadata(v2DocId, "previous_version", MetadataValue(v1Result->contentHash));
    
    // Update v1 status
    metadataRepo_->setMetadata(v1DocId, "status", MetadataValue("archived"));
    metadataRepo_->setMetadata(v1DocId, "next_version", MetadataValue(v2Result->contentHash));
    
    // Query for current version
    search::SearchOptions currentOptions;
    currentOptions.metadataFilter = "status:current";
    currentOptions.tags = {"document"};
    auto currentResults = contentStore_->searchWithOptions("*", currentOptions);
    
    EXPECT_EQ(currentResults.size(), 1) << "Should find exactly one current version";
    if (!currentResults.empty()) {
        EXPECT_EQ(currentResults[0].document.hash, v2Result->contentHash) 
            << "Current version should be v2";
    }
    
    // Query for all versions
    search::SearchOptions allOptions;
    allOptions.tags = {"document"};
    auto allResults = contentStore_->searchWithOptions("*", allOptions);
    
    EXPECT_EQ(allResults.size(), 2) << "Should find both versions";
}

TEST_F(PDFIntegrationTest, BatchPDFProcessing) {
    // Test batch processing of multiple PDFs
    
    const size_t numPDFs = 20;
    std::vector<std::string> hashes;
    
    auto startTime = std::chrono::high_resolution_clock::now();
    
    for (size_t i = 0; i < numPDFs; ++i) {
        auto pdfPath = createTestPDF("batch_" + std::to_string(i) + ".pdf", 3 + i % 5);
        auto result = contentStore_->addFile(pdfPath, {"batch", "pdf"});
        
        ASSERT_TRUE(result.has_value()) << "Failed to ingest PDF " << i;
        hashes.push_back(result->contentHash);
    }
    
    auto endTime = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
    
    std::cout << "Batch processing " << numPDFs << " PDFs took: " 
              << duration.count() << "ms" << std::endl;
    std::cout << "Average time per PDF: " << duration.count() / numPDFs << "ms" << std::endl;
    
    // Verify all PDFs are searchable
    search::SearchOptions options;
    options.tags = {"batch"};
    options.limit = numPDFs;
    auto results = contentStore_->searchWithOptions("Page", options);
    
    EXPECT_EQ(results.size(), numPDFs) << "Not all batch PDFs are searchable";
    
    // Verify deduplication for identical PDFs
    auto duplicatePath = createTestPDF("duplicate.pdf", 3);
    auto dup1 = contentStore_->addFile(duplicatePath, {"duplicate"});
    auto dup2 = contentStore_->addFile(duplicatePath, {"duplicate"});
    
    ASSERT_TRUE(dup1.has_value() && dup2.has_value());
    EXPECT_EQ(dup1->contentHash, dup2->contentHash) << "Duplicate PDFs should have same hash";
    EXPECT_GT(dup2->dedupRatio(), 0.9) << "Deduplication should be detected";
}