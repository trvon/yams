// Rich test data generation utilities shared across the test suite.
#pragma once

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <map>
#include <random>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <yams/metadata/document_metadata.h>

namespace yams::test {

class TestDataGenerator {
public:
    TestDataGenerator() : TestDataGenerator(std::random_device{}()) {}

    explicit TestDataGenerator(uint64_t seed) : rng_(seed) {}

    [[nodiscard]] std::string generateTextDocument(std::size_t bytes,
                                                   std::string_view topic = "yams") {
        static constexpr std::array words{
            "search",       "daemon",      "vector",      "chunk",     "metadata",
            "storage",      "content",     "index",       "retrieval", "pipeline",
            "ingest",       "stream",      "compression", "wal",       "fixture",
            "builder",      "document",    "embedding",   "semantic",  "analysis",
            "orchestrator", "coordinator", "backoff",     "resilient", "replay",
        };

        if (bytes == 0) {
            return {};
        }

        std::uniform_int_distribution<std::size_t> word_dist(0, words.size() - 1);
        std::ostringstream oss;
        oss << "# " << topic << " log\n";
        while (oss.tellp() < static_cast<std::streamoff>(bytes)) {
            oss << words[word_dist(rng_)] << ' ';
            if (std::bernoulli_distribution(0.15)(rng_)) {
                oss << topic << ' ';
            }
        }

        std::string result = oss.str();
        if (result.size() > bytes) {
            result.resize(bytes);
        }
        return result;
    }

    [[nodiscard]] std::string generateMarkdown(int sections,
                                               std::string_view title = "Generated Document") {
        if (sections <= 0) {
            sections = 1;
        }

        std::ostringstream oss;
        oss << "# " << title << "\n\n";
        for (int i = 0; i < sections; ++i) {
            oss << "## Section " << (i + 1) << "\n";
            oss << generateTextDocument(256, "section" + std::to_string(i + 1)) << "\n\n";
            oss << "- item A" << i << "\n";
            oss << "- item B" << i << "\n\n";
        }
        return oss.str();
    }

    [[nodiscard]] std::vector<std::byte> generateRandomBytes(std::size_t count) {
        std::vector<std::byte> data(count);
        std::uniform_int_distribution<int> dist(0, 255);
        std::generate(data.begin(), data.end(), [&] { return std::byte(dist(rng_)); });
        return data;
    }

    [[nodiscard]] std::vector<std::byte> generatePDF(std::size_t pages) {
        pages = std::max<std::size_t>(pages, 1);

        const std::size_t font_index = 3 + pages * 2;
        const std::size_t total_objects = 3 + pages * 2;

        std::string pdf;
        pdf.reserve(2048 * pages);
        pdf.append("%PDF-1.4\n");

        std::vector<std::size_t> offsets;
        offsets.reserve(total_objects + 1);
        offsets.push_back(0U);

        auto add_object = [&](const std::string& object) {
            offsets.push_back(pdf.size());
            pdf.append(object);
        };

        add_object("1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n");

        std::ostringstream pages_stream;
        pages_stream << "2 0 obj\n<< /Type /Pages /Kids [";
        for (std::size_t i = 0; i < pages; ++i) {
            if (i > 0) {
                pages_stream << ' ';
            }
            pages_stream << (3 + i) << " 0 R";
        }
        pages_stream << "] /Count " << pages << " >>\nendobj\n";
        add_object(pages_stream.str());

        for (std::size_t i = 0; i < pages; ++i) {
            const std::size_t page_object_index = 3 + i;
            const std::size_t content_index = 3 + pages + i;

            std::ostringstream page_obj;
            page_obj << page_object_index
                     << " 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
                     << "/Resources << /Font << /F1 " << font_index << " 0 R >> >> /Contents "
                     << content_index << " 0 R >>\nendobj\n";
            add_object(page_obj.str());

            std::string text = "Page " + std::to_string(i + 1) + " of " + std::to_string(pages);
            std::ostringstream stream_obj;
            stream_obj << "BT /F1 18 Tf 72 720 Td (" << text << ") Tj ET";
            const std::string stream_payload = stream_obj.str();

            std::ostringstream content_obj;
            content_obj << content_index << " 0 obj\n<< /Length " << stream_payload.size()
                        << " >>\nstream\n"
                        << stream_payload << "\nendstream\nendobj\n";
            add_object(content_obj.str());
        }

        std::ostringstream font_obj;
        font_obj << font_index
                 << " 0 obj\n<< /Type /Font /Subtype /Type1 /Name /F1 /BaseFont /Helvetica >>\n"
                 << "endobj\n";
        add_object(font_obj.str());

        const std::size_t xref_offset = pdf.size();
        std::ostringstream xref;
        xref << "xref\n0 " << (offsets.size()) << "\n";
        xref << "0000000000 65535 f \n";
        for (std::size_t i = 1; i < offsets.size(); ++i) {
            xref << std::setw(10) << std::setfill('0') << offsets[i] << " 00000 n \n";
        }
        xref << "trailer\n<< /Size " << offsets.size() << " /Root 1 0 R >>\n";
        xref << "startxref\n" << xref_offset << "\n%%EOF\n";
        pdf.append(xref.str());

        std::vector<std::byte> bytes(pdf.size());
        std::transform(pdf.begin(), pdf.end(), bytes.begin(),
                       [](unsigned char c) { return std::byte(c); });
        return bytes;
    }

    [[nodiscard]] std::vector<std::byte> generateCorruptedPDF() {
        auto data = generatePDF(1);
        if (data.size() > 16) {
            data.resize(data.size() / 2); // truncate to break structure
        }
        if (!data.empty()) {
            data[0] = std::byte{0x00};
        }
        return data;
    }

    [[nodiscard]] std::vector<std::pair<std::string, metadata::MetadataValue>>
    generateMetadata(std::size_t count, std::string_view prefix = "field") {
        std::vector<std::pair<std::string, metadata::MetadataValue>> fields;
        fields.reserve(count);

        std::uniform_int_distribution<int> type_dist(0, 4);
        std::uniform_int_distribution<int> int_dist(0, 10'000);
        std::uniform_real_distribution<double> real_dist(0.0, 1.0);
        std::bernoulli_distribution bool_dist(0.5);

        for (std::size_t i = 0; i < count; ++i) {
            const auto key = std::string(prefix) + "_" + std::to_string(i);
            switch (type_dist(rng_)) {
                case 0:
                    fields.emplace_back(key, metadata::MetadataValue("value_" + key));
                    break;
                case 1:
                    fields.emplace_back(
                        key, metadata::MetadataValue(static_cast<int64_t>(int_dist(rng_))));
                    break;
                case 2:
                    fields.emplace_back(key, metadata::MetadataValue(real_dist(rng_)));
                    break;
                case 3:
                    fields.emplace_back(key, metadata::MetadataValue(bool_dist(rng_)));
                    break;
                default:
                    fields.emplace_back(key, metadata::MetadataValue("meta:" + key));
                    break;
            }
        }

        return fields;
    }

    [[nodiscard]] std::vector<std::string>
    generateCorpus(std::size_t count, std::size_t averageSizeBytes, double duplicationRate = 0.1) {
        std::vector<std::string> corpus;
        corpus.reserve(count);

        if (averageSizeBytes == 0) {
            averageSizeBytes = 256;
        }

        std::poisson_distribution<std::size_t> size_dist(
            std::max<std::size_t>(64, averageSizeBytes));
        std::bernoulli_distribution duplicate_dist(std::clamp(duplicationRate, 0.0, 1.0));

        for (std::size_t i = 0; i < count; ++i) {
            if (!corpus.empty() && duplicate_dist(rng_)) {
                std::uniform_int_distribution<std::size_t> pick(0, corpus.size() - 1);
                corpus.push_back(corpus[pick(rng_)]);
            } else {
                const auto target_size =
                    std::min<std::size_t>(size_dist(rng_), averageSizeBytes * 2);
                corpus.push_back(generateTextDocument(target_size));
            }
        }

        return corpus;
    }

private:
    std::mt19937_64 rng_;
};

} // namespace yams::test
