#pragma once

#include <filesystem>
#include <string>
#include <vector>
#include <map>
#include <fstream>
#include <memory>
#include <mutex>
#include "test_data_generator.h"

namespace yams::test {

class FixtureManager {
public:
    struct Fixture {
        std::string name;
        std::filesystem::path path;
        std::string hash;
        size_t size;
        std::map<std::string, std::string> metadata;
        
        bool exists() const {
            return std::filesystem::exists(path);
        }
        
        std::string readContent() const {
            std::ifstream file(path, std::ios::binary);
            return std::string(std::istreambuf_iterator<char>(file),
                             std::istreambuf_iterator<char>());
        }
        
        std::vector<uint8_t> readBinary() const {
            std::ifstream file(path, std::ios::binary);
            return std::vector<uint8_t>(std::istreambuf_iterator<char>(file),
                                       std::istreambuf_iterator<char>());
        }
    };
    
    FixtureManager() {
        // Create fixture directory in temp
        fixtureDir_ = std::filesystem::temp_directory_path() / "yams_fixtures";
        std::filesystem::create_directories(fixtureDir_);
        
        // Initialize data generator
        generator_ = std::make_unique<TestDataGenerator>();
    }
    
    ~FixtureManager() {
        cleanupFixtures();
    }
    
    // Fixture operations
    Fixture createFixture(const std::string& name, const std::string& content) {
        Fixture fixture;
        fixture.name = name;
        fixture.path = fixtureDir_ / name;
        fixture.size = content.size();
        
        // Write content to file
        std::ofstream file(fixture.path, std::ios::binary);
        file.write(content.data(), content.size());
        file.close();
        
        // Calculate simple hash (for testing purposes)
        fixture.hash = calculateHash(content);
        
        // Thread-safe addition to activeFixtures_
        {
            std::lock_guard<std::mutex> lock(mutex_);
            activeFixtures_.push_back(fixture);
        }
        return fixture;
    }
    
    Fixture createBinaryFixture(const std::string& name, const std::vector<uint8_t>& data) {
        Fixture fixture;
        fixture.name = name;
        fixture.path = fixtureDir_ / name;
        fixture.size = data.size();
        
        // Write binary data to file
        std::ofstream file(fixture.path, std::ios::binary);
        file.write(reinterpret_cast<const char*>(data.data()), data.size());
        file.close();
        
        // Calculate hash
        fixture.hash = calculateHash(std::string(data.begin(), data.end()));
        
        // Thread-safe addition to activeFixtures_
        {
            std::lock_guard<std::mutex> lock(mutex_);
            activeFixtures_.push_back(fixture);
        }
        return fixture;
    }
    
    Fixture loadFixture(const std::string& name) {
        auto path = fixtureDir_ / name;
        if (!std::filesystem::exists(path)) {
            throw std::runtime_error("Fixture not found: " + name);
        }
        
        Fixture fixture;
        fixture.name = name;
        fixture.path = path;
        fixture.size = std::filesystem::file_size(path);
        
        return fixture;
    }
    
    void cleanupFixtures() {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& fixture : activeFixtures_) {
            if (fixture.exists()) {
                std::filesystem::remove(fixture.path);
            }
        }
        activeFixtures_.clear();
        
        // Remove fixture directory if empty
        if (std::filesystem::is_empty(fixtureDir_)) {
            std::filesystem::remove(fixtureDir_);
        }
    }
    
    // Predefined fixtures
    static Fixture getSimplePDF() {
        static FixtureManager manager;
        static Fixture fixture;
        static std::once_flag initialized;
        
        std::call_once(initialized, []() {
            TestDataGenerator gen;
            auto pdfData = gen.generatePDF(1);
            fixture = manager.createBinaryFixture("simple.pdf", pdfData);
            fixture.metadata["pages"] = "1";
            fixture.metadata["type"] = "pdf";
        });
        
        return fixture;
    }
    
    static Fixture getComplexPDF() {
        static FixtureManager manager;
        static Fixture fixture;
        static std::once_flag initialized;
        
        std::call_once(initialized, []() {
            TestDataGenerator gen;
            auto pdfData = gen.generatePDF(10);
            fixture = manager.createBinaryFixture("complex.pdf", pdfData);
            fixture.metadata["pages"] = "10";
            fixture.metadata["type"] = "pdf";
            fixture.metadata["has_metadata"] = "true";
        });
        
        return fixture;
    }
    
    static Fixture getCorruptedPDF() {
        static FixtureManager manager;
        static Fixture fixture;
        static std::once_flag initialized;
        
        std::call_once(initialized, []() {
            TestDataGenerator gen;
            auto pdfData = gen.generateCorruptedPDF();
            fixture = manager.createBinaryFixture("corrupted.pdf", pdfData);
            fixture.metadata["type"] = "pdf";
            fixture.metadata["corrupted"] = "true";
        });
        
        return fixture;
    }
    
    static Fixture getLargeTextFile() {
        static FixtureManager manager;
        static Fixture fixture;
        static std::once_flag initialized;
        
        std::call_once(initialized, []() {
            TestDataGenerator gen;
            std::string content = gen.generateTextDocument(1024 * 1024); // 1MB
            fixture = manager.createFixture("large_text.txt", content);
            fixture.metadata["type"] = "text";
            fixture.metadata["size_mb"] = "1";
        });
        
        return fixture;
    }
    
    static Fixture getUnicodeDocument() {
        static FixtureManager manager;
        static Fixture fixture;
        static std::once_flag initialized;
        
        std::call_once(initialized, []() {
            TestDataGenerator gen;
            std::string content = gen.generateUnicode({"emoji", "chinese", "arabic", 
                                                      "russian", "japanese", "korean"});
            content += "\n\n" + gen.generateLoremIpsum(50);
            fixture = manager.createFixture("unicode.txt", content);
            fixture.metadata["type"] = "text";
            fixture.metadata["encoding"] = "utf-8";
            fixture.metadata["has_unicode"] = "true";
        });
        
        return fixture;
    }
    
    static Fixture getMarkdownDocument() {
        static FixtureManager manager;
        static Fixture fixture;
        static std::once_flag initialized;
        
        std::call_once(initialized, []() {
            TestDataGenerator gen;
            std::string content = gen.generateMarkdown(5);
            fixture = manager.createFixture("test.md", content);
            fixture.metadata["type"] = "markdown";
            fixture.metadata["sections"] = "5";
        });
        
        return fixture;
    }
    
    static Fixture getJSONDocument() {
        static FixtureManager manager;
        static Fixture fixture;
        static std::once_flag initialized;
        
        std::call_once(initialized, []() {
            TestDataGenerator gen;
            std::string content = gen.generateJSON(3, 4);
            fixture = manager.createFixture("test.json", content);
            fixture.metadata["type"] = "json";
            fixture.metadata["depth"] = "3";
        });
        
        return fixture;
    }
    
    // Get fixture directory for custom file creation
    std::filesystem::path getFixtureDirectory() const {
        return fixtureDir_;
    }
    
    // Create a temporary directory for test isolation
    std::filesystem::path createTempDirectory(const std::string& prefix = "test") {
        std::lock_guard<std::mutex> lock(mutex_);
        auto tempPath = fixtureDir_ / (prefix + "_" + std::to_string(tempDirCounter_++));
        std::filesystem::create_directories(tempPath);
        tempDirectories_.push_back(tempPath);
        return tempPath;
    }
    
    // Clean up temporary directories
    void cleanupTempDirectories() {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& dir : tempDirectories_) {
            if (std::filesystem::exists(dir)) {
                std::filesystem::remove_all(dir);
            }
        }
        tempDirectories_.clear();
    }
    
    // Batch fixture creation
    std::vector<Fixture> createCorpus(size_t numDocs, size_t avgSize = 1024) {
        TestDataGenerator gen;
        auto docs = gen.generateCorpus(numDocs, avgSize, 0.1); // 10% duplicates
        
        std::vector<Fixture> fixtures;
        for (const auto& doc : docs) {
            auto fixture = createFixture(doc.name, doc.content);
            for (const auto& [key, value] : doc.metadata) {
                fixture.metadata[key] = value.asString();
            }
            fixtures.push_back(fixture);
        }
        
        return fixtures;
    }

private:
    std::filesystem::path fixtureDir_;
    std::vector<Fixture> activeFixtures_;
    std::vector<std::filesystem::path> tempDirectories_;
    std::unique_ptr<TestDataGenerator> generator_;
    mutable std::mutex mutex_;  // Protects activeFixtures_ and tempDirectories_
    static inline size_t tempDirCounter_ = 0;
    
    std::string calculateHash(const std::string& content) {
        // Simple hash for testing (not cryptographic)
        std::hash<std::string> hasher;
        size_t hash = hasher(content);
        
        // Convert to hex string
        std::stringstream ss;
        ss << std::hex << hash;
        return ss.str();
    }
};

} // namespace yams::test