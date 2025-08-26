#pragma once

// TextBasicHandler: basic, format-agnostic text extraction with line-range and substring search.
// Phase 1 (PBI-006): Supports
// - scope: "all" (default) and "range" (line ranges like "1-3,10")
// - search: substring search with optional case sensitivity
// (formatOptions["case_sensitive"]="false")
// - format: "text" (default) | "markdown" | "json"
// Non-goals in Phase 1 for this handler:
// - section/selector scopes (return NotImplemented)
// - bounding boxes (bboxes)
// - regex search (future extension)
//
// This handler treats input bytes as UTF-8 text without transcoding. For binary or other encodings,
// prefer upstream extractors or specialized handlers.

#include <yams/extraction/format_handlers/format_handler.hpp>

#include <algorithm>
#include <cctype>
#include <charconv>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

namespace yams::extraction::format {

class TextBasicHandler final : public IFormatHandler {
public:
    TextBasicHandler() = default;
    ~TextBasicHandler() override = default;

    bool supports(std::string_view mime, std::string_view ext) const override {
        // Accept any text/* mime
        if (!mime.empty()) {
            if (startsWithIgnoreCase(mime, "text/"))
                return true;
            // Common textual mimes outside text/*
            static const std::vector<std::string_view> textualMimes = {
                "application/json",   "application/xml",        "application/yaml",
                "application/x-yaml", "application/javascript", "application/x-javascript",
                "application/sql",    "application/markdown",   "text/markdown"};
            for (auto m : textualMimes) {
                if (iequals(mime, m))
                    return true;
            }
        }

        // Accept known textual extensions
        if (!ext.empty()) {
            std::string e = toLowerDotExt(ext);
            const auto& exts = supportedExtensions();
            return exts.find(e) != exts.end();
        }

        return false;
    }

    CapabilityDescriptor capabilities() const override {
        CapabilityDescriptor cap;
        cap.name = "TextBasicHandler";
        cap.version = "0.1.0";
        cap.mimes = {"text/plain",       "text/markdown",  "text/x-c",  "text/x-c++hdr",
                     "text/x-c++src",    "text/x-python",  "text/x-go", "text/x-java",
                     "application/json", "application/xml"};
        cap.extensions =
            std::vector<std::string>(supportedExtensions().begin(), supportedExtensions().end());
        cap.supportsRange = true; // line ranges
        cap.supportsSection = false;
        cap.supportsSelector = false;
        cap.canSearch = true;
        cap.supportsBBoxes = false;
        cap.priority = 55; // Slightly above generic text to prefer line-range capability
        return cap;
    }

    std::string name() const override { return "TextBasicHandler"; }

    Result<ExtractionResult> extract(std::span<const std::byte> bytes,
                                     const ExtractionQuery& query) override {
        // Reject unsupported scopes for this handler
        switch (query.scope) {
            case Scope::All:
            case Scope::Range:
                break;
            case Scope::Section:
            case Scope::Selector:
                return Error{ErrorCode::NotImplemented, "TextBasicHandler: scope not supported"};
        }

        // Convert bytes to string (assume UTF-8)
        std::string content;
        content.assign(reinterpret_cast<const char*>(bytes.data()),
                       reinterpret_cast<const char*>(bytes.data()) + bytes.size());

        // Split into lines and track line-start offsets for match global offsets
        std::vector<std::string_view> allLines;
        std::vector<size_t> allLineStartOffsets;
        allLines.reserve(1024);
        allLineStartOffsets.reserve(1024);
        {
            size_t offset = 0;
            size_t start = 0;
            for (size_t i = 0; i < content.size(); ++i) {
                if (content[i] == '\n') {
                    allLines.emplace_back(&content[start], i - start);
                    allLineStartOffsets.push_back(offset);
                    // next line starts after '\n'
                    offset = i + 1;
                    start = i + 1;
                }
            }
            // tail (last line without trailing newline)
            if (start <= content.size()) {
                allLines.emplace_back(&content[start], content.size() - start);
                allLineStartOffsets.push_back(start);
            }
        }

        // Compute selected line indices based on query.range (1-based)
        std::vector<int> selectedIndices; // store 0-based line indices
        selectedIndices.reserve(allLines.size());
        if (query.scope == Scope::All || query.range.empty()) {
            for (int i = 0; i < static_cast<int>(allLines.size()); ++i) {
                selectedIndices.push_back(i);
            }
        } else {
            auto rangesRes = parseClosedRanges(query.range);
            if (!rangesRes) {
                return Error{ErrorCode::InvalidArgument,
                             "Invalid range expression: " + query.range + " (" +
                                 rangesRes.error().message + ")"};
            }
            const auto& ranges = rangesRes.value();
            for (int idx = 1; idx <= static_cast<int>(allLines.size()); ++idx) {
                if (inAnyRange(idx, ranges)) {
                    selectedIndices.push_back(idx - 1);
                }
            }
        }

        // Join selected lines and keep a map from selected line order -> global offset in result
        std::string resultText;
        resultText.reserve(content.size());
        std::vector<size_t> selectedLineGlobalOffsets; // offset within resultText
        selectedLineGlobalOffsets.reserve(selectedIndices.size());
        for (size_t k = 0; k < selectedIndices.size(); ++k) {
            const int lineIdx = selectedIndices[k];
            selectedLineGlobalOffsets.push_back(resultText.size());
            resultText.append(allLines[lineIdx].data(), allLines[lineIdx].size());
            if (k + 1 < selectedIndices.size()) {
                resultText.push_back('\n');
            }
        }

        // Prepare result container and mime/format selection
        ExtractionResult out;
        const std::string fmt = toLower(query.format.empty() ? "text" : query.format);
        if (fmt == "json") {
            out.mime = "application/json";
        } else if (fmt == "markdown") {
            out.mime = "text/markdown";
        } else {
            out.mime = "text/plain";
        }

        // Search (substring) across selected lines if provided
        if (!query.search.empty()) {
            const bool caseSensitive = !hasOptionFalse(query.formatOptions, "case_sensitive");
            std::string needle = query.search;
            if (!caseSensitive) {
                toLowerInPlace(needle);
            }

            size_t matchesAdded = 0;
            for (size_t k = 0; k < selectedIndices.size(); ++k) {
                if (matchesAdded >= static_cast<size_t>(std::max(0, query.maxMatches)))
                    break;

                const int lineIdx = selectedIndices[k];
                std::string_view hay = allLines[lineIdx];
                std::string hayLower;
                std::string_view hayView = hay;

                if (!caseSensitive) {
                    hayLower.assign(hay.data(), hay.size());
                    toLowerInPlace(hayLower);
                    hayView = hayLower;
                }

                size_t pos = 0;
                while (matchesAdded < static_cast<size_t>(std::max(0, query.maxMatches))) {
                    pos = hayView.find(needle, pos);
                    if (pos == std::string::npos)
                        break;

                    Match m;
                    // line is 1-based in output for user ergonomics
                    m.line = lineIdx + 1;

                    // Global offsets are relative to the final "resultText" buffer
                    size_t globalLineStart = selectedLineGlobalOffsets[k];
                    m.start = globalLineStart + pos;
                    m.end = m.start + needle.size();

                    out.matches.push_back(std::move(m));
                    ++matchesAdded;
                    ++pos; // continue searching after current position
                }
            }
        }

        // Serialize output according to requested format
        if (fmt == "json") {
            out.json = buildJson(resultText, out.matches);
        } else {
            out.text = resultText;
        }

        // No pages/sections for plain text; leave vectors empty
        return out;
    }

    // Helper to register this handler in a registry
    static inline void registerWith(HandlerRegistry& registry) {
        registry.registerHandler(std::make_shared<TextBasicHandler>());
    }

private:
    static std::unordered_set<std::string> supportedExtensions() {
        // Common textual/code extensions
        static const std::unordered_set<std::string> exts = {
            ".txt",  ".md",  ".log", ".json", ".jsonl", ".xml", ".yml",  ".yaml", ".csv",
            ".tsv",  ".ini", ".cfg", ".conf", ".toml",  ".c",   ".h",    ".hpp",  ".hh",
            ".hxx",  ".cc",  ".cpp", ".cxx",  ".py",    ".js",  ".ts",   ".tsx",  ".jsx",
            ".java", ".go",  ".rs",  ".rb",   ".php",   ".sql", ".sh",   ".bash", ".zsh",
            ".ps1",  ".bat", ".cmd", ".scss", ".less",  ".css", ".rst",  ".adoc", ".org",
            ".tex",  ".bib", ".m",   ".mm",   ".swift", ".kt",  ".scala"};
        return exts;
    }

    // Utilities
    static bool iequals(std::string_view a, std::string_view b) {
        if (a.size() != b.size())
            return false;
        for (size_t i = 0; i < a.size(); ++i) {
            if (static_cast<unsigned char>(std::tolower(a[i])) !=
                static_cast<unsigned char>(std::tolower(b[i]))) {
                return false;
            }
        }
        return true;
    }

    static bool startsWithIgnoreCase(std::string_view s, std::string_view prefix) {
        if (s.size() < prefix.size())
            return false;
        return iequals(s.substr(0, prefix.size()), prefix);
    }

    static std::string toLower(std::string s) {
        toLowerInPlace(s);
        return s;
    }

    static void toLowerInPlace(std::string& s) {
        std::transform(s.begin(), s.end(), s.begin(),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    }

    static std::string toLowerDotExt(std::string_view ext) {
        std::string e(ext);
        if (!e.empty() && e[0] != '.')
            e.insert(e.begin(), '.');
        toLowerInPlace(e);
        return e;
    }

    static bool inAnyRange(int lineOneBased, const std::vector<std::pair<int, int>>& ranges) {
        for (const auto& [a, b] : ranges) {
            if (lineOneBased >= a && lineOneBased <= b)
                return true;
        }
        return false;
    }

    static bool hasOptionFalse(const std::unordered_map<std::string, std::string>& opts,
                               std::string_view key) {
        auto it = opts.find(std::string(key));
        if (it == opts.end())
            return false;
        std::string v = toLower(it->second);
        return (v == "0" || v == "false" || v == "no" || v == "off");
    }

    static std::optional<std::string> buildJson(const std::string& text,
                                                const std::vector<Match>& matches) {
        // Minimal JSON builder to avoid dependencies; escape a few characters.
        auto escape = [](std::string_view s) {
            std::string out;
            out.reserve(s.size() + 16);
            for (char c : s) {
                switch (c) {
                    case '\\':
                        out += "\\\\";
                        break;
                    case '\"':
                        out += "\\\"";
                        break;
                    case '\b':
                        out += "\\b";
                        break;
                    case '\f':
                        out += "\\f";
                        break;
                    case '\n':
                        out += "\\n";
                        break;
                    case '\r':
                        out += "\\r";
                        break;
                    case '\t':
                        out += "\\t";
                        break;
                    default:
                        if (static_cast<unsigned char>(c) < 0x20) {
                            // Control chars -> \u00XX
                            char buf[7] = {'\\', 'u', '0', '0', '0', '0', 0};
                            const char* hex = "0123456789abcdef";
                            buf[4] = hex[(c >> 4) & 0xF];
                            buf[5] = hex[c & 0xF];
                            out += buf;
                        } else {
                            out += c;
                        }
                }
            }
            return out;
        };

        std::ostringstream oss;
        oss << "{\"text\":\"" << escape(text) << "\",\"matches\":[";
        for (size_t i = 0; i < matches.size(); ++i) {
            const auto& m = matches[i];
            oss << "{";
            bool first = true;
            if (m.line.has_value()) {
                oss << "\"line\":" << *m.line;
                first = false;
            }
            // start/end are always provided
            oss << (first ? "" : ",") << "\"start\":" << m.start << ",\"end\":" << m.end;
            oss << "}";

            if (i + 1 < matches.size())
                oss << ",";
        }
        oss << "]}";
        return oss.str();
    }
};

// Convenience registration function
inline void registerTextBasicHandler(HandlerRegistry& registry) {
    TextBasicHandler::registerWith(registry);
}

} // namespace yams::extraction::format