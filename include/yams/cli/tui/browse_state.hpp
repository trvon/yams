#pragma once

#include <chrono>
#include <string>
#include <vector>

namespace yams::cli::tui {

// Layout modes for different screen sizes
enum class LayoutMode {
    MultiPane,  // 3-column layout for wide screens (>=100 cols)
    SinglePane, // Single pane with navigation for medium screens (60-99 cols)
    Compact     // Minimal list-only view for small screens (<60 cols)
};

// Column focus in the multi-pane layout
enum class Column { Collections = 0, Documents = 1, Preview = 2 };

// Rendering/preview mode for content
enum class PreviewMode { Auto, Text, Hex };

// Lightweight document entry used by the TUI
struct DocEntry {
    std::string hash;
    std::string name;
    size_t size = 0;
    std::string type;
    std::chrono::system_clock::time_point createdAt{};
    int64_t id = 0;
};

// Centralized UI state for the browser TUI
struct BrowseState {
    // Data sets
    std::vector<DocEntry> allDocuments;
    std::vector<DocEntry> documents;
    std::vector<std::string> collections;

    // Layout and display
    LayoutMode layoutMode = LayoutMode::MultiPane;
    int terminalWidth = 120;
    int terminalHeight = 30;

    // Selection and focus
    int selected = 0;
    int selectedCollection = 0;
    Column activeColumn = Column::Documents;

    // Modes
    bool showHelp = false;
    bool searchMode = false;
    bool fuzzySearch = true;
    bool commandMode = false;
    bool deleteConfirm = false;

    // Input buffers
    std::string searchQuery;
    std::string commandBuffer;

    // Status line message
    std::string statusMessage;

    // Preview pane
    std::vector<std::string> previewLines;
    int previewScrollOffset = 0;
    PreviewMode previewMode = PreviewMode::Auto;

    // Virtual scrolling and pagination
    int documentScrollOffset = 0;  // For virtual scrolling in document list
    int documentsPerPage = 100;    // Number of documents to load per page
    int currentPage = 0;           // Current page for pagination
    bool hasMoreDocuments = false; // Whether more documents are available

    // Viewer overlay
    bool viewerOpen = false;
    std::vector<std::string> viewerLines;
    int viewerScrollOffset = 0;
    bool viewerWrap = false; // when true, soft-wrap long lines in viewer
};

} // namespace yams::cli::tui