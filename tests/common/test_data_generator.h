#pragma once

#include <chrono>
#include <filesystem>
#include <map>
#include <random>
#include <string>
#include <vector>
#include <yams/metadata/document_metadata.h>

namespace yams::test {

class TestDataGenerator {
public:
    TestDataGenerator(uint32_t seed = 0)
        : rng_(seed ? seed : std::chrono::steady_clock::now().time_since_epoch().count()) {}

    // Document generators
    std::string generateTextDocument(size_t sizeBytes) {
        return generateLoremIpsum(sizeBytes / 5); // Approx 5 bytes per word
    }

    std::string generateMarkdown(size_t sections) {
        std::string content;
        for (size_t i = 0; i < sections; ++i) {
            content += "# Section " + std::to_string(i + 1) + "\n\n";
            content += generateLoremIpsum(50) + "\n\n";

            if (i % 2 == 0) {
                content += "## Subsection\n\n";
                content += "- **Bold text**: " + generateWord() + "\n";
                content += "- *Italic text*: " + generateWord() + "\n";
                content += "- `Code`: " + generateCode(Language::CPP, 1) + "\n\n";
            }

            if (i % 3 == 0) {
                content += "```cpp\n";
                content += generateCode(Language::CPP, 5);
                content += "\n```\n\n";
            }
        }
        return content;
    }

    std::vector<uint8_t> generatePDF(size_t pages) {
        // Generate a minimal valid PDF structure
        std::string pdf;
        std::vector<size_t> offsets; // Track byte offsets for xref table

        // PDF header
        pdf = "%PDF-1.4\n";

        // Catalog object (object 1)
        offsets.push_back(pdf.size());
        pdf += "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n";

        // Build kids array for Pages object
        std::string kids = "[";
        for (size_t i = 0; i < pages; ++i) {
            if (i > 0)
                kids += " ";
            kids += std::to_string(3 + i * 2) + " 0 R";
        }
        kids += "]";

        // Pages object (object 2)
        offsets.push_back(pdf.size());
        pdf += "2 0 obj\n<< /Type /Pages /Kids " + kids + " /Count " + std::to_string(pages) +
               " >>\nendobj\n";

        size_t objNum = 3;
        for (size_t i = 0; i < pages; ++i) {
            // Page object
            offsets.push_back(pdf.size());
            pdf += std::to_string(objNum) + " 0 obj\n";
            pdf += "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] ";
            pdf += "/Contents " + std::to_string(objNum + 1) + " 0 R ";
            pdf += "/Resources << /Font << /F1 << /Type /Font /Subtype /Type1 /BaseFont /Helvetica "
                   ">> >> >> >>\n";
            pdf += "endobj\n";
            objNum++;

            // Content stream
            offsets.push_back(pdf.size());
            std::string content = "BT /F1 12 Tf 100 700 Td (Page " + std::to_string(i + 1) + ": " +
                                  generateLoremIpsum(20) + ") Tj ET";
            pdf += std::to_string(objNum) + " 0 obj\n";
            pdf += "<< /Length " + std::to_string(content.size()) + " >>\n";
            pdf += "stream\n" + content + "\nendstream\nendobj\n";
            objNum++;
        }

        // Add metadata
        offsets.push_back(pdf.size());
        pdf += std::to_string(objNum) + " 0 obj\n";
        pdf += "<< /Title (Test PDF Document) /Author (TestDataGenerator) ";
        pdf += "/Subject (Unit Testing) /Keywords (test, pdf, yams) ";
        pdf += "/Creator (YAMS Test Suite) /Producer (TestDataGenerator) >>\n";
        pdf += "endobj\n";

        // Store xref table position
        size_t xref_pos = pdf.size();

        // xref table
        pdf += "xref\n";
        pdf += "0 " + std::to_string(objNum + 1) + "\n";
        pdf += "0000000000 65535 f \n";

        // Write object offsets (10 digits, zero-padded)
        for (size_t offset : offsets) {
            char buf[32];
            snprintf(buf, sizeof(buf), "%010zu 00000 n \n", offset);
            pdf += buf;
        }

        // Trailer
        pdf += "trailer\n";
        pdf += "<< /Size " + std::to_string(objNum + 1) + " /Root 1 0 R ";
        pdf += "/Info " + std::to_string(objNum) + " 0 R >>\n";
        pdf += "startxref\n";
        pdf += std::to_string(xref_pos) + "\n";
        pdf += "%%EOF";

        return std::vector<uint8_t>(pdf.begin(), pdf.end());
    }

    std::string generateJSON(size_t depth, size_t breadth) {
        return generateJSONObject(depth, breadth, 0);
    }

    // Content generators
    std::string generateLoremIpsum(size_t words) {
        static const std::vector<std::string> loremWords = {
            "lorem",      "ipsum",        "dolor",
            "sit",        "amet",         "consectetur",
            "adipiscing", "elit",         "sed",
            "do",         "eiusmod",      "tempor",
            "incididunt", "ut",           "labore",
            "et",         "dolore",       "magna",
            "aliqua",     "enim",         "ad",
            "minim",      "veniam",       "quis",
            "nostrud",    "exercitation", "ullamco",
            "laboris",    "nisi",         "aliquip",
            "ex",         "ea",           "commodo",
            "consequat",  "duis",         "aute",
            "irure",      "in",           "reprehenderit",
            "voluptate",  "velit",        "esse",
            "cillum",     "fugiat",       "nulla",
            "pariatur",   "excepteur",    "sint",
            "occaecat",   "cupidatat",    "non",
            "proident",   "sunt",         "culpa",
            "qui",        "officia",      "deserunt",
            "mollit",     "anim"};

        std::string result;
        std::uniform_int_distribution<size_t> dist(0, loremWords.size() - 1);

        for (size_t i = 0; i < words; ++i) {
            if (i > 0)
                result += " ";
            result += loremWords[dist(rng_)];
        }

        return result;
    }

    enum class Language { CPP, PYTHON, JAVASCRIPT, GO, RUST };

    std::string generateCode(Language lang, size_t lines) {
        std::string code;

        switch (lang) {
            case Language::CPP:
                code = "#include <iostream>\n#include <vector>\n\n";
                code += "int main() {\n";
                for (size_t i = 0; i < lines; ++i) {
                    code += "    std::cout << \"Line " + std::to_string(i) + "\" << std::endl;\n";
                }
                code += "    return 0;\n}\n";
                break;

            case Language::PYTHON:
                code = "#!/usr/bin/env python3\n\n";
                code += "def main():\n";
                for (size_t i = 0; i < lines; ++i) {
                    code += "    print(f'Line " + std::to_string(i) + "')\n";
                }
                code += "\nif __name__ == '__main__':\n    main()\n";
                break;

            case Language::JAVASCRIPT:
                code = "function main() {\n";
                for (size_t i = 0; i < lines; ++i) {
                    code += "    console.log(`Line " + std::to_string(i) + "`);\n";
                }
                code += "}\n\nmain();\n";
                break;

            case Language::GO:
                code = "package main\n\nimport \"fmt\"\n\n";
                code += "func main() {\n";
                for (size_t i = 0; i < lines; ++i) {
                    code += "    fmt.Println(\"Line " + std::to_string(i) + "\")\n";
                }
                code += "}\n";
                break;

            case Language::RUST:
                code = "fn main() {\n";
                for (size_t i = 0; i < lines; ++i) {
                    code += "    println!(\"Line " + std::to_string(i) + "\");\n";
                }
                code += "}\n";
                break;
        }

        return code;
    }

    std::string generateUnicode(const std::vector<std::string>& scripts) {
        std::string result;

        for (const auto& script : scripts) {
            if (script == "emoji") {
                result += "🚀 🎉 ✨ 🔥 💡 ";
            } else if (script == "chinese") {
                result += "你好世界 ";
            } else if (script == "arabic") {
                result += "مرحبا بالعالم ";
            } else if (script == "russian") {
                result += "Привет мир ";
            } else if (script == "japanese") {
                result += "こんにちは世界 ";
            } else if (script == "korean") {
                result += "안녕하세요 세계 ";
            }
        }

        return result;
    }

    // Metadata generators
    std::map<std::string, metadata::MetadataValue>
    generateMetadata(size_t numFields,
                     const std::vector<std::string>& fieldTypes = {"string", "number", "boolean"}) {
        std::map<std::string, metadata::MetadataValue> metadata;
        std::uniform_int_distribution<size_t> typeDist(0, fieldTypes.size() - 1);

        for (size_t i = 0; i < numFields; ++i) {
            std::string key = "field_" + std::to_string(i);
            std::string type = fieldTypes[typeDist(rng_)];

            if (type == "string") {
                metadata[key] = metadata::MetadataValue(generateWord());
            } else if (type == "number") {
                std::uniform_real_distribution<double> numDist(0, 1000);
                metadata[key] = metadata::MetadataValue(numDist(rng_));
            } else if (type == "boolean") {
                std::uniform_int_distribution<int> boolDist(0, 1);
                metadata[key] = metadata::MetadataValue(static_cast<bool>(boolDist(rng_)));
            }
        }

        return metadata;
    }

    // Corpus generators
    struct Document {
        std::string name;
        std::string content;
        std::map<std::string, metadata::MetadataValue> metadata;
        std::vector<std::string> tags;
    };

    std::vector<Document> generateCorpus(size_t numDocs, size_t avgSize, double duplicateRatio) {
        std::vector<Document> corpus;
        corpus.reserve(numDocs);

        size_t numUnique = static_cast<size_t>(numDocs * (1.0 - duplicateRatio));
        size_t numDuplicates = numDocs - numUnique;

        // Generate unique documents
        for (size_t i = 0; i < numUnique; ++i) {
            Document doc;
            doc.name = "doc_" + std::to_string(i) + ".txt";
            doc.content = generateTextDocument(avgSize);
            doc.metadata = generateMetadata(5);
            doc.tags = generateTags(3);
            corpus.push_back(doc);
        }

        // Add duplicates
        if (numUnique > 0) {
            std::uniform_int_distribution<size_t> dupDist(0, numUnique - 1);
            for (size_t i = 0; i < numDuplicates; ++i) {
                size_t srcIdx = dupDist(rng_);
                Document doc = corpus[srcIdx];
                doc.name = "dup_" + std::to_string(i) + "_" + doc.name;
                corpus.push_back(doc);
            }
        }

        return corpus;
    }

    // Helper to generate binary data
    std::vector<std::byte> generateRandomBytes(size_t size) {
        std::vector<std::byte> data;
        data.reserve(size);
        std::uniform_int_distribution<int> byteDist(0, 255);

        for (size_t i = 0; i < size; ++i) {
            data.push_back(static_cast<std::byte>(byteDist(rng_)));
        }

        return data;
    }

    // Generate corrupted PDF for error testing
    std::vector<uint8_t> generateCorruptedPDF() {
        std::string pdf = "%PDF-1.4\n";
        pdf += "This is not a valid PDF structure\n";
        pdf += "Random data: " + generateLoremIpsum(100);
        // Missing required PDF objects and structure
        return std::vector<uint8_t>(pdf.begin(), pdf.end());
    }

private:
    std::mt19937 rng_;

    std::string generateWord() {
        std::uniform_int_distribution<size_t> lengthDist(3, 10);
        std::uniform_int_distribution<int> charDist('a', 'z');

        size_t length = lengthDist(rng_);
        std::string word;
        word.reserve(length);

        for (size_t i = 0; i < length; ++i) {
            word += static_cast<char>(charDist(rng_));
        }

        return word;
    }

    std::vector<std::string> generateTags(size_t count) {
        static const std::vector<std::string> tagPool = {
            "test", "document",    "sample",      "data",   "benchmark",
            "unit", "integration", "performance", "stress", "mock"};

        std::vector<std::string> tags;
        std::uniform_int_distribution<size_t> tagDist(0, tagPool.size() - 1);

        for (size_t i = 0; i < count && i < tagPool.size(); ++i) {
            tags.push_back(tagPool[tagDist(rng_)]);
        }

        return tags;
    }

    std::string generateJSONObject(size_t depth, size_t breadth, size_t currentDepth) {
        if (currentDepth >= depth) {
            // Leaf value
            std::uniform_int_distribution<int> typeDist(0, 2);
            switch (typeDist(rng_)) {
                case 0:
                    return "\"" + generateWord() + "\"";
                case 1:
                    return std::to_string(std::uniform_int_distribution<int>(0, 100)(rng_));
                case 2:
                    return std::uniform_int_distribution<int>(0, 1)(rng_) ? "true" : "false";
            }
        }

        std::string json = "{\n";
        for (size_t i = 0; i < breadth; ++i) {
            if (i > 0)
                json += ",\n";
            json += "  \"" + generateWord() + "\": ";
            json += generateJSONObject(depth, breadth / 2, currentDepth + 1);
        }
        json += "\n}";

        return json;
    }
};

} // namespace yams::test