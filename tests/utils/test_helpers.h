#pragma once

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <yams/core/types.h>
#include <yams/core/format.h>
#include <filesystem>
#include <random>
#include <fstream>

namespace yams::test {

// Base test fixture with common utilities
class YamsTest : public ::testing::Test {
protected:
    void SetUp() override {
        testDir = std::filesystem::temp_directory_path() / generateTestId();
        std::filesystem::create_directories(testDir);
    }
    
    void TearDown() override {
        std::error_code ec;
        std::filesystem::remove_all(testDir, ec);
    }
    
    // Test data generation
    static std::vector<std::byte> generateRandomBytes(size_t size) {
        static std::mt19937_64 rng{std::random_device{}()};
        std::uniform_int_distribution<int> dist(0, 255);
        
        std::vector<std::byte> data;
        data.reserve(size);
        
        for (size_t i = 0; i < size; ++i) {
            data.push_back(static_cast<std::byte>(dist(rng)));
        }
        
        return data;
    }
    
    // Generate repeating pattern for deduplication tests
    static std::vector<std::byte> generatePattern(size_t size, size_t patternLength = 256) {
        std::vector<std::byte> pattern = generateRandomBytes(patternLength);
        std::vector<std::byte> data;
        data.reserve(size);
        
        while (data.size() < size) {
            size_t copySize = std::min(patternLength, size - data.size());
            data.insert(data.end(), pattern.begin(), pattern.begin() + copySize);
        }
        
        return data;
    }
    
    // File creation helpers
    std::filesystem::path createTestFile(size_t size, std::string_view name = "test") {
        auto path = testDir / name;
        auto data = generateRandomBytes(size);
        
        std::ofstream file(path, std::ios::binary);
        file.write(reinterpret_cast<const char*>(data.data()), data.size());
        file.close();
        
        if (!file) {
            throw std::runtime_error("Failed to create test file");
        }
        
        return path;
    }
    
    std::filesystem::path createTestFileWithContent(
        const std::vector<std::byte>& content, 
        std::string_view name = "test") {
        auto path = testDir / name;
        
        std::ofstream file(path, std::ios::binary);
        file.write(reinterpret_cast<const char*>(content.data()), content.size());
        file.close();
        
        if (!file) {
            throw std::runtime_error("Failed to create test file");
        }
        
        return path;
    }
    
    // Verify file content
    bool verifyFileContent(const std::filesystem::path& path, 
                          const std::vector<std::byte>& expected) {
        std::ifstream file(path, std::ios::binary);
        if (!file) return false;
        
        std::vector<std::byte> actual(std::filesystem::file_size(path));
        file.read(reinterpret_cast<char*>(actual.data()), actual.size());
        
        return actual == expected;
    }
    
protected:
    std::filesystem::path testDir;
    
private:
    static std::string generateTestId() {
        auto now = std::chrono::system_clock::now().time_since_epoch().count();
        return yams::format("kronos_test_{}", now);
    }
};

// Global helper function for getting temp directory
inline std::filesystem::path getTempDir() {
    static std::mt19937_64 rng{std::random_device{}()};
    auto timestamp = std::chrono::system_clock::now().time_since_epoch().count();
    auto random = rng();
    auto dirname = yams::format("kronos_test_{}_{}", timestamp, random);
    auto path = std::filesystem::temp_directory_path() / dirname;
    std::filesystem::create_directories(path);
    return path;
}

// Custom matchers
MATCHER_P(BytesEqual, expected, "Byte vectors are equal") {
    return std::ranges::equal(arg, expected);
}

MATCHER_P(HasErrorCode, code, "Result has expected error code") {
    return !arg.has_value() && arg.error() == code;
}

MATCHER_P(HasValue, value, "Result has expected value") {
    return arg.has_value() && *arg == value;
}

// Test constants
struct TestVectors {
    // SHA-256 test vectors
    static constexpr std::string_view EMPTY_SHA256 = 
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    
    static constexpr std::string_view ABC_SHA256 = 
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
    
    static constexpr std::string_view HELLO_WORLD_SHA256 = 
        "a591a6d40bf420404a011733cfb7b190d62c65bf0bcda32b57b277d9ad9f146e";
    
    // Common test data
    static std::vector<std::byte> getEmptyData() { return {}; }
    
    static std::vector<std::byte> getABCData() {
        return {std::byte{'a'}, std::byte{'b'}, std::byte{'c'}};
    }
    
    static std::vector<std::byte> getHelloWorldData() {
        const char* str = "Hello World";
        return std::vector<std::byte>(
            reinterpret_cast<const std::byte*>(str),
            reinterpret_cast<const std::byte*>(str + 11)
        );
    }
};

// Performance measurement helper
class ScopedTimer {
public:
    explicit ScopedTimer(std::string_view name) 
        : name_(name), start_(std::chrono::high_resolution_clock::now()) {}
    
    ~ScopedTimer() {
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start_);
        std::cout << yams::format("{} took {}ms\n", name_, duration.count());
    }
    
private:
    std::string name_;
    std::chrono::high_resolution_clock::time_point start_;
};

} // namespace yams::test