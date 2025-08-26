#pragma once

// PBI-006 Phase 1: Format-agnostic extraction interfaces and registry.
// This header defines a minimal, stable ABI for in-proc format handlers,
// plus a thread-safe registry used by the service layer to select and run
// the appropriate handler based on MIME type and/or extension.
//
// NOTE:
// - Keep handlers stateless; prefer pure functions operating on provided buffers.
// - If caching is needed (e.g., open PDFs), do so outside handlers via an LRU keyed by content
// hash.
// - Phase 1 targets built-ins (text, markdown, html-basic, code line-range). Plugins arrive in
// Phase 2.

#include <yams/core/types.h>

#include <cstddef> // std::byte
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace yams::extraction::format {

// Scope types supported by the query model (format-agnostic)
enum class Scope {
    All,     // Entire document
    Range,   // Page/line/row ranges (semantics depend on format)
    Section, // Section(s) by path/title
    Selector // CSS/XPath/JSONPath (format-specific)
};

// Bounding box for match visualization (when available)
struct BBox {
    int x{0};
    int y{0};
    int w{0};
    int h{0};
};

// Match descriptor used in structured outputs
struct Match {
    std::optional<int> page; // Page index (0-based) if applicable
    std::optional<int> line; // Line index (0-based) if applicable
    std::size_t start{0};    // Character start (UTF-8 byte offset unless specified otherwise)
    std::size_t end{0};      // Character end (exclusive)
    std::vector<BBox> bboxes;
};

// Phase 1 query DTO (intentionally simple; formatOptions is a string KV map)
struct ExtractionQuery {
    Scope scope{Scope::All};
    std::string range;                    // "1-3,5-7" (pages/lines/rows by format)
    std::vector<std::string> sectionPath; // e.g., ["Chapter 2","Section 2.3"]
    std::string selector;                 // CSS/XPath/JSONPath (by format)
    std::string search;                   // plain or regex (handler decides)
    int maxMatches{50};
    bool includeBBoxes{false};

    // Output formatting
    // "text" | "markdown" | "json" (handlers may ignore unsupported formats)
    std::string format{"text"};

    // Format-specific options as simple string KV pairs (e.g., {"pdf.around":"2"})
    std::unordered_map<std::string, std::string> formatOptions;
};

// Phase 1 result DTO (normalized across handlers)
// For simplicity, "json" is returned as a serialized JSON string when format=json.
struct ExtractionResult {
    std::string mime;                  // Result MIME (e.g., "text/plain", "application/json")
    std::optional<std::string> text;   // Text or Markdown content
    std::optional<std::string> json;   // Serialized structured JSON (when requested)
    std::vector<Match> matches;        // Matches (when search applied)
    std::vector<int> pages;            // Pages included (if applicable)
    std::vector<std::string> sections; // Section titles included (if applicable)
};

// Handler capability descriptor for feature introspection and routing
struct CapabilityDescriptor {
    // Identification
    std::string name;    // Human-readable name (e.g., "PDFiumHandler")
    std::string version; // Handler version string

    // Supported types
    std::vector<std::string> mimes;      // e.g., {"application/pdf", "text/html"}
    std::vector<std::string> extensions; // e.g., {".pdf", ".html", ".md", ".cpp"}

    // Features
    bool supportsRange{false};
    bool supportsSection{false};
    bool supportsSelector{false};
    bool canSearch{false};
    bool supportsBBoxes{false};

    // Routing
    // Higher values take precedence when multiple handlers can process the same format.
    // Built-ins should typically sit around 50; plugins may override with 60-100.
    int priority{50};
};

// Abstract interface for all format handlers
class IFormatHandler {
public:
    virtual ~IFormatHandler() = default;

    // Returns true if this handler can process a document with the given MIME or extension.
    // Either argument may be empty; handlers should handle best-effort checks.
    virtual bool supports(std::string_view mime, std::string_view ext) const = 0;

    // Returns the handler capability descriptor
    virtual CapabilityDescriptor capabilities() const = 0;

    // Human-readable handler name
    virtual std::string name() const = 0;

    // Perform extraction on the provided buffer according to the query.
    // Handlers should:
    //  - Be robust to empty/partial queries (treat as Scope::All when unspecified).
    //  - Respect 'format' when feasible ("text"|"markdown"|"json").
    //  - Return Error with ErrorCode::NotImplemented for unsupported features (e.g., selector).
    //  - Keep CPU and memory usage bounded (respect maxMatches; avoid O(n^2) scans).
    virtual Result<ExtractionResult> extract(std::span<const std::byte> bytes,
                                             const ExtractionQuery& query) = 0;
};

// Thread-safe registry for handler selection and routing
class HandlerRegistry {
public:
    HandlerRegistry() = default;
    ~HandlerRegistry() = default;

    // Register a handler. Later registrations with higher priority can override earlier ones.
    void registerHandler(std::shared_ptr<IFormatHandler> handler) {
        if (!handler)
            return;
        std::lock_guard<std::mutex> lock(mutex_);
        handlers_.push_back(std::move(handler));
    }

    // Return the best available handler for mime/ext based on supports() and priority.
    // nullptr when none can handle the provided descriptors.
    std::shared_ptr<IFormatHandler> selectBest(std::string_view mime, std::string_view ext) const {
        std::lock_guard<std::mutex> lock(mutex_);
        std::shared_ptr<IFormatHandler> best;
        int bestPriority = std::numeric_limits<int>::min();

        for (const auto& h : handlers_) {
            if (!h)
                continue;
            if (!h->supports(mime, ext))
                continue;

            const int p = h->capabilities().priority;
            if (p > bestPriority) {
                bestPriority = p;
                best = h;
            }
        }
        return best;
    }

    // Convenience: select and extract in one call.
    Result<ExtractionResult> extract(std::string_view mime, std::string_view ext,
                                     std::span<const std::byte> bytes,
                                     const ExtractionQuery& query) const {
        auto h = selectBest(mime, ext);
        if (!h) {
            return Error{ErrorCode::NotFound, "No format handler available for mime='" +
                                                  std::string(mime) + "', ext='" +
                                                  std::string(ext) + "'"};
        }
        return h->extract(bytes, query);
    }

    // Introspection helper: list capabilities for all registered handlers
    std::vector<CapabilityDescriptor> listCapabilities() const {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<CapabilityDescriptor> caps;
        caps.reserve(handlers_.size());
        for (const auto& h : handlers_) {
            if (h)
                caps.push_back(h->capabilities());
        }
        return caps;
    }

private:
    mutable std::mutex mutex_;
    std::vector<std::shared_ptr<IFormatHandler>> handlers_;
};

// Utility: parse "1-3,5,7-9" into closed integer ranges [start,end] (1-based indexes).
// Handlers can convert semantics (pages vs lines) as needed.
// Returns ErrorCode::InvalidArgument on malformed inputs.
Result<std::vector<std::pair<int, int>>> parseClosedRanges(const std::string& expr);

// Phase 1: register built-in handlers (text, markdown, html-basic, code line-range)
// Implementations are provided in the extraction module.
// - Text/Markdown/Code: backed by PlainTextExtractor with optional line-range support
// - HTML-basic: backed by HtmlTextExtractor
// - PDF: optional in Phase 1; if available, handler should be registered externally
void registerBuiltinHandlers(HandlerRegistry& registry);

} // namespace yams::extraction::format