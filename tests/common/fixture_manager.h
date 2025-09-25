#pragma once

#include "test_data_generator.h"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <random>
#include <string>
#include <string_view>
#include <vector>

namespace yams::test {

struct Fixture {
    std::filesystem::path path;
    std::string description;
    std::vector<std::string> tags;
};

class FixtureManager {
public:
    FixtureManager() : FixtureManager(make_root_path()) {}

    explicit FixtureManager(std::filesystem::path root) : root_(std::move(root)) {
        std::error_code ec;
        std::filesystem::create_directories(root_, ec);
    }

    FixtureManager(FixtureManager&&) noexcept = default;
    FixtureManager& operator=(FixtureManager&&) noexcept = default;
    FixtureManager(const FixtureManager&) = delete;
    FixtureManager& operator=(const FixtureManager&) = delete;

    ~FixtureManager() { cleanupFixtures(); }

    [[nodiscard]] const std::filesystem::path& root() const noexcept { return root_; }

    Fixture createTextFixture(std::string_view name, std::string_view content,
                              std::vector<std::string> tags = {}) {
        const auto relative = std::filesystem::path(name);
        const auto target = ensurePath(relative);
        std::ofstream file(target, std::ios::binary);
        file.write(content.data(), static_cast<std::streamsize>(content.size()));
        file.close();
        created_.push_back(target);
        return Fixture{target, std::string{name}, std::move(tags)};
    }

    Fixture createBinaryFixture(std::string_view name, std::vector<std::byte> data,
                                std::vector<std::string> tags = {}) {
        const auto relative = std::filesystem::path(name);
        const auto target = ensurePath(relative);
        std::ofstream file(target, std::ios::binary);
        file.write(reinterpret_cast<const char*>(data.data()),
                   static_cast<std::streamsize>(data.size()));
        file.close();
        created_.push_back(target);
        return Fixture{target, std::string{name}, std::move(tags)};
    }

    std::vector<Fixture> createCorpus(std::size_t count, std::size_t averageSizeBytes,
                                      double duplicationRate = 0.1) {
        std::vector<Fixture> fixtures;
        fixtures.reserve(count);
        TestDataGenerator generator;
        auto corpus = generator.generateCorpus(count, averageSizeBytes, duplicationRate);
        for (std::size_t i = 0; i < corpus.size(); ++i) {
            const auto name = "doc_" + std::to_string(i) + ".txt";
            fixtures.push_back(createTextFixture(name, corpus[i], {"corpus"}));
        }
        return fixtures;
    }

    void cleanupFixtures() {
        if (root_.empty()) {
            return;
        }

        std::error_code ec;
        std::filesystem::remove_all(root_, ec);
        created_.clear();
        root_.clear();
    }

    static Fixture getSimplePDF() { return catalog().simple; }
    static Fixture getComplexPDF() { return catalog().complex; }
    static Fixture getCorruptedPDF() { return catalog().corrupted; }
    static Fixture getLargeTextFile(std::size_t size = 512 * 1024) {
        auto& cat = catalog();
        if (!std::filesystem::exists(cat.largeText.path) ||
            std::filesystem::file_size(cat.largeText.path) != size) {
            TestDataGenerator generator;
            cat.largeText = cat.manager.createTextFixture(
                "large_text.txt", generator.generateTextDocument(size, "fixture"),
                {"text", "large"});
        }
        return cat.largeText;
    }

private:
    struct FixtureCatalog {
        FixtureManager manager;
        Fixture simple;
        Fixture complex;
        Fixture corrupted;
        Fixture largeText;

        FixtureCatalog() : manager(make_root_path()) {
            TestDataGenerator generator;
            simple = manager.createPdfFixture("simple.pdf", 2, {"pdf", "simple"});
            complex = manager.createPdfFixture("complex.pdf", 8, {"pdf", "complex"});
            corrupted = manager.createBinaryFixture(
                "corrupted.pdf", generator.generateCorruptedPDF(), {"pdf", "corrupted"});
            largeText = manager.createTextFixture(
                "large_text.txt", generator.generateTextDocument(512 * 1024, "large fixture"),
                {"text", "large"});
        }
    };

    Fixture createPdfFixture(std::string_view name, std::size_t pages,
                             std::vector<std::string> tags = {}) {
        TestDataGenerator generator;
        auto data = generator.generatePDF(pages);
        return createBinaryFixture(name, std::move(data), std::move(tags));
    }

    static FixtureCatalog& catalog() {
        static FixtureCatalog instance;
        return instance;
    }

    static std::filesystem::path make_root_path() {
        auto base = std::filesystem::temp_directory_path();
        auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
        std::uniform_int_distribution<int> dist(0, 9999);
        thread_local std::mt19937_64 rng{std::random_device{}()};
        auto suffix = dist(rng);
        return base / ("yams_fixtures_" + std::to_string(stamp) + "_" + std::to_string(suffix));
    }

    std::filesystem::path ensurePath(const std::filesystem::path& relative) {
        auto target = root_ / relative;
        std::error_code ec;
        std::filesystem::create_directories(target.parent_path(), ec);
        return target;
    }

    std::filesystem::path root_;
    std::vector<std::filesystem::path> created_;
};

} // namespace yams::test
