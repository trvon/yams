#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <yams/cli/tui/browse_state.hpp>

namespace yams::cli {
class YamsCLI;
}

namespace yams::cli::tui {

/**
 * Services and helpers used by the TUI browser.
 * - Fetch document listings (all, fuzzy, basic filter)
 * - Load document content (text via metadata or raw bytes via content store)
 * - Utilities for binary detection, line splitting, and hex dump creation
 * - Open content in external pager (with optional suspend callback)
 */
class BrowseServices {
public:
    using SuspendRunner = std::function<void(const std::function<void()>&)>;

    explicit BrowseServices(yams::cli::YamsCLI* cli) noexcept;

    // ------------- Documents listing -------------

    /**
     * Load all documents from the metadata repository (falls back to content store if needed).
     * Updates status_message with a short summary (e.g., "N documents loaded").
     */
    std::vector<DocEntry> loadAllDocuments(std::string* status_message);

    /**
     * Fuzzy search using the metadata repository (if available). Returns converted entries.
     * min_similarity in [0,1], limit caps number of entries returned.
     */
    std::vector<DocEntry> fuzzySearch(std::string_view query, float min_similarity, int limit);

    /**
     * Case-insensitive basic filter on the provided list by name or hash substring.
     */
    std::vector<DocEntry> filterBasic(const std::vector<DocEntry>& all, std::string_view needle);

    // ------------- Content loading -------------

    /**
     * Try to load extracted text content for a document from the metadata repository.
     * Returns std::nullopt if unavailable.
     */
    std::optional<std::string> loadTextContent(int64_t doc_id);

    /**
     * Load raw bytes for a document from the content store, capped at cap_bytes.
     * Returns empty vector if unavailable.
     */
    std::vector<std::byte> loadRawBytes(std::string_view hash, size_t cap_bytes);

    // ------------- Content utilities -------------

    /**
     * Heuristic check whether a sample string appears to be binary (non-printable ratio).
     */
    bool looksBinary(std::string_view sample) const;

    /**
     * Split text into lines, normalizing CRLF and trimming trailing CR.
     * Caps the number of lines to max_lines.
     */
    std::vector<std::string> splitLines(const std::string& content, size_t max_lines) const;

    /**
     * Create a classic hex dump with ASCII gutter.
     * bytes_per_line typically 16. Caps total lines to max_lines.
     */
    std::vector<std::string> toHexDump(const std::vector<std::byte>& bytes, size_t bytes_per_line,
                                       size_t max_lines) const;

    /**
     * Convenience function to build preview lines based on the selected mode.
     * - PreviewMode::Auto: prefer metadata text; fallback to bytes (text-like) else "Binary
     * content..."
     * - PreviewMode::Text: force text view (fall back to message if binary)
     * - PreviewMode::Hex: hex dump
     *
     * max_bytes caps raw bytes retrieval; max_lines caps resulting lines.
     */
    std::vector<std::string> makePreviewLines(const DocEntry& doc, PreviewMode mode,
                                              size_t max_bytes, size_t max_lines);

    // ------------- External pager -------------

    /**
     * Open content in an external pager.
     * - Uses $PAGER if set, otherwise falls back to "less -R".
     * - If 'text' has value, it is written to a temporary file; otherwise raw_bytes are used.
     * - Returns true on success; on failure, writes error message when provided.
     *
     * Note: This variant does NOT suspend the TUI. Prefer openInPagerWithSuspend when possible.
     */
    bool openInPager(const std::string& name, const std::optional<std::string>& text,
                     const std::vector<std::byte>& raw_bytes, std::string* error_message);

    /**
     * Open content in an external pager while suspending the TUI via the provided suspend callback.
     * The suspend callback is responsible for restoring terminal state after pager exits.
     */
    bool openInPagerWithSuspend(const std::string& name, const std::optional<std::string>& text,
                                const std::vector<std::byte>& raw_bytes,
                                const SuspendRunner& suspend, std::string* error_message);

private:
    yams::cli::YamsCLI* _cli; // non-owning
};

} // namespace yams::cli::tui