#include <yams/cli/browse_command.h>
#include <yams/cli/yams_cli.h>
#include <yams/metadata/metadata_repository.h>
#include <spdlog/spdlog.h>

#include <ftxui/component/captured_mouse.hpp>
#include <ftxui/component/component.hpp>
#include <ftxui/component/component_base.hpp>
#include <ftxui/component/component_options.hpp>
#include <ftxui/component/screen_interactive.hpp>
#include <ftxui/dom/elements.hpp>
#include <ftxui/screen/color.hpp>

#include <iostream>
#include <sstream>
#include <vector>
#include <iomanip>
#include <algorithm>
#include <fstream>
#include <chrono>
#include <cstddef>
#include <cctype>
#include <unistd.h>  // For isatty and STDOUT_FILENO

namespace yams::cli {

using namespace ftxui;

struct DocumentInfo {
    std::string hash;
    std::string name;
    size_t size;
    std::string type;
    std::chrono::system_clock::time_point createdAt;
    int64_t id;
    
    std::string getFormattedSize() const {
        if (size < 1024) return std::to_string(size) + "B";
        if (size < 1024 * 1024) return std::to_string(size / 1024) + "K";
        if (size < 1024 * 1024 * 1024) return std::to_string(size / (1024 * 1024)) + "M";
        return std::to_string(size / (1024 * 1024 * 1024)) + "G";
    }
    
    std::string getFormattedDate() const {
        auto time_t = std::chrono::system_clock::to_time_t(createdAt);
        std::tm* tm = std::localtime(&time_t);
        char buffer[20];
        std::strftime(buffer, sizeof(buffer), "%m/%d %H:%M", tm);
        return std::string(buffer);
    }
};

class BrowseCommand::Impl {
public:
    YamsCLI* cli;
    ScreenInteractive* screen = nullptr;
    std::vector<DocumentInfo> documents;
    std::vector<DocumentInfo> allDocuments;  // All documents for searching
    std::vector<std::string> collections;
    int selected = 0;
    int selectedCollection = 0;
    int activeColumn = 1;  // 0=left, 1=middle, 2=right
    std::string searchQuery;
    std::string statusMessage;
    bool showHelp = false;
    bool deleteConfirm = false;
    bool searchMode = false;
    bool useFuzzySearch = true;
    bool commandMode = false;
    std::string commandBuffer;
    std::vector<std::string> previewLines;
    
    Component CreateUI() {
        RefreshDocuments();
        RefreshCollections();
        
        
        
        // Create renderer
        auto renderer = Renderer([&] {
            if (showHelp) {
                return RenderHelp();
            }
            return RenderThreeColumns();
        });
        
        // Event handler
        renderer |= CatchEvent([&](Event event) {
            return HandleEvent(event);
        });
        
        return renderer;
    }
    
    bool HandleEvent(Event event) {
        // Help toggle
        if (event == Event::Character('?')) {
            showHelp = !showHelp;
            return true;
        }
        
        if (showHelp) {
            showHelp = false;
            return true;
        }

        // Command mode handling (process before other keys)
        if (commandMode) {
            if (event == Event::Escape) {
                commandMode = false;
                commandBuffer.clear();
                statusMessage.clear();
                return true;
            }
            if (event == Event::Return) {
                // Process command
                std::string cmd = commandBuffer;
                // Trim spaces
                while (!cmd.empty() && std::isspace(static_cast<unsigned char>(cmd.front()))) cmd.erase(cmd.begin());
                while (!cmd.empty() && std::isspace(static_cast<unsigned char>(cmd.back()))) cmd.pop_back();

                if (cmd == "q" || cmd == "quit" || cmd == "wq") {
                    if (screen) screen->ExitLoopClosure()();
                    return true;
                }
                statusMessage = "Unknown command: " + cmd;
                commandMode = false;
                commandBuffer.clear();
                return true;
            }
            if (event == Event::Backspace) {
                if (!commandBuffer.empty()) {
                    commandBuffer.pop_back();
                }
                return true;
            }
            auto character = event.character();
            if (character.size() == 1 && std::isprint(static_cast<unsigned char>(character[0]))) {
                commandBuffer += character;
                return true;
            }
            return true; // consume all events in command mode
        }
        
        // Quit
        if (event == Event::Character('q') || event == Event::Escape) {
            if (screen) screen->ExitLoopClosure()();
            return true;
        }

        // Enter command mode (':' or 'p')
        if (event == Event::Character(':') || event == Event::Character('p')) {
            if (!searchMode && !commandMode) {
                commandMode = true;
                commandBuffer.clear();
                statusMessage.clear();
            }
            return true;
        }
        
        // Vertical navigation
        if (event == Event::Character('j') || event == Event::ArrowDown) {
            NavigateDown();
            return true;
        }
        
        if (event == Event::Character('k') || event == Event::ArrowUp) {
            NavigateUp();
            return true;
        }
        
        // Horizontal navigation
        if (event == Event::Character('h') || event == Event::ArrowLeft) {
            if (activeColumn > 0) {
                activeColumn--;
            }
            return true;
        }
        
        if (event == Event::Character('l') || event == Event::ArrowRight) {
            if (activeColumn < 2) {
                activeColumn++;
            }
            return true;
        }
        
        // Jump navigation
        if (event == Event::Character('g')) {
            JumpToTop();
            return true;
        }
        
        if (event == Event::Character('G')) {
            JumpToBottom();
            return true;
        }
        
        // Delete
        if (event == Event::Character('d')) {
            if (!deleteConfirm) {
                deleteConfirm = true;
                statusMessage = "Press D to confirm deletion";
            }
            return true;
        }
        
        if (event == Event::Character('D') && deleteConfirm) {
            DeleteSelected();
            deleteConfirm = false;
            return true;
        }
        
        if (deleteConfirm && event != Event::Character('D')) {
            deleteConfirm = false;
            statusMessage = "";
            return true;
        }
        
        // Refresh
        if (event == Event::Character('r')) {
            RefreshDocuments();
            RefreshCollections();
            statusMessage = "Refreshed";
            return true;
        }
        
        // Command mode is separate (':', 'p'), Search mode toggle here
        if (event == Event::Character('/')) {
            if (!searchMode) {
                searchMode = true;
                searchQuery = "";
                statusMessage = "Search: ";
            }
            return true;
        }
        
        // Search mode handling
        if (searchMode) {
            if (event == Event::Escape) {
                searchMode = false;
                searchQuery = "";
                statusMessage = "";
                ApplySearch();
                return true;
            }
            
            if (event == Event::Return) {
                searchMode = false;
                statusMessage = "Search: " + searchQuery;
                ApplySearch();
                return true;
            }
            
            if (event == Event::Backspace) {
                if (!searchQuery.empty()) {
                    searchQuery.pop_back();
                    ApplySearch();
                }
                return true;
            }
            
            // Toggle fuzzy search with Ctrl+F
            if (event == Event::Character('\x06')) {  // Ctrl+F
                useFuzzySearch = !useFuzzySearch;
                statusMessage = "Search (" + std::string(useFuzzySearch ? "fuzzy" : "exact") + "): " + searchQuery;
                ApplySearch();
                return true;
            }
            
            // Handle character input in search mode
            auto character = event.character();
            if (character.size() == 1 && std::isprint(character[0])) {
                searchQuery += character;
                ApplySearch();
                return true;
            }
            
            return true;  // Consume all events in search mode
        }
        
        // Enter
        if (event == Event::Return) {
            if (activeColumn == 1 && selected < static_cast<int>(documents.size())) {
                ViewDocument(documents[selected]);
            }
            return true;
        }
        
        return false;
    }
    
    Element RenderThreeColumns() {
        auto filteredDocs = FilterDocuments();
        
        // Left column - Collections
        Elements leftElements;
        leftElements.push_back(text("Collections") | bold | dim);
        leftElements.push_back(text(""));
        
        for (size_t i = 0; i < collections.size(); ++i) {
            auto entry = text(" " + collections[i]);
            if (static_cast<int>(i) == selectedCollection && activeColumn == 0) {
                entry = text(">" + collections[i]) | color(Color::Cyan);
            } else if (activeColumn != 0) {
                entry = entry | dim;
            }
            leftElements.push_back(entry);
        }
        
        if (collections.empty()) {
            leftElements.push_back(text(" All") | (activeColumn == 0 ? color(Color::Cyan) : dim));
        }
        
        // Middle column - Documents
        Elements middleElements;
        for (size_t i = 0; i < filteredDocs.size(); ++i) {
            const auto& doc = filteredDocs[i];
            
            // Format name to fit
            std::string displayName = doc.name;
            if (displayName.length() > 25) {
                displayName = displayName.substr(0, 22) + "...";
            }
            
            auto entry = hbox({
                text(displayName) | size(WIDTH, EQUAL, 26),
                text(doc.getFormattedSize()) | size(WIDTH, EQUAL, 5) | dim,
                text(" "),
                text(doc.getFormattedDate()) | dim,
            });
            
            if (static_cast<int>(i) == selected && activeColumn == 1) {
                entry = hbox({
                    text(">"),
                    entry,
                }) | color(Color::Cyan);
            } else if (activeColumn != 1) {
                entry = hbox({
                    text(" "),
                    entry,
                }) | dim;
            } else {
                entry = hbox({
                    text(" "),
                    entry,
                });
            }
            
            middleElements.push_back(entry);
        }
        
        if (middleElements.empty()) {
            middleElements.push_back(text(" (empty)") | dim);
        }
        
        // Right column - Preview
        Elements rightElements;
        rightElements.push_back(text("Preview") | bold | dim);
        rightElements.push_back(text(""));
        
        if (!previewLines.empty()) {
            for (size_t i = 0; i < previewLines.size() && i < 35; ++i) {
                std::string line = previewLines[i];
                if (line.length() > 45) {
                    line = line.substr(0, 42) + "...";
                }
                rightElements.push_back(
                    text(line) | (activeColumn == 2 ? color(Color::White) : dim)
                );
            }
        } else if (selected < static_cast<int>(filteredDocs.size())) {
            const auto& doc = filteredDocs[selected];
            rightElements.push_back(text("Name: " + doc.name));
            rightElements.push_back(text(""));
            rightElements.push_back(text("Hash:") | dim);
            rightElements.push_back(text(doc.hash.substr(0, 32) + "..."));
            rightElements.push_back(text(""));
            rightElements.push_back(text("Size: " + doc.getFormattedSize()));
            rightElements.push_back(text("Type: " + (doc.type.empty() ? "unknown" : doc.type)));
            rightElements.push_back(text("Date: " + doc.getFormattedDate()));
        }
        
        // Build columns
        auto leftColumn = vbox(leftElements) | size(WIDTH, EQUAL, 20);
        auto middleColumn = vbox(middleElements) | flex;
        auto rightColumn = vbox(rightElements) | size(WIDTH, EQUAL, 50);
        
        // Status line
        std::string status;
        if (searchMode) {
            status = "Search (" + std::string(useFuzzySearch ? "fuzzy" : "exact") + "): " + searchQuery + "_";
        } else if (commandMode) {
            status = ":" + commandBuffer + "_";
        } else if (!statusMessage.empty()) {
            status = statusMessage;
        } else {
            status = std::to_string(selected + 1) + "/" + std::to_string(filteredDocs.size());
        }
        
        // Main layout - ranger style
        return vbox({
            hbox({
                leftColumn,
                separatorLight() | dim,
                middleColumn,
                separatorLight() | dim,
                rightColumn,
            }) | flex,
            separator() | dim,
            hbox({
                text(" " + status),
                filler(),
                text("j/k:↑↓ ") | dim,
                text("h/l:← → ") | dim,
                text("d:del ") | dim,
                text("r:refresh ") | dim,
                text(":/p:cmd ") | dim,
                text("?:help ") | dim,
                text("q:quit ") | dim,
            }) | size(HEIGHT, EQUAL, 1),
        });
    }
    
    Element RenderHelp() {
        return vbox({
            text(""),
            text("  YAMS Browser - Help") | bold | center,
            text(""),
            separator() | dim,
            text(""),
            text("  Navigation:"),
            text("    j/↓        Move down"),
            text("    k/↑        Move up"),
            text("    h/←        Move left column"),
            text("    l/→        Move right column"),
            text("    g          Jump to top"),
            text("    G          Jump to bottom"),
            text(""),
            text("  Actions:"),
            text("    Enter      Open/view document"),
            text("    d          Delete (press D to confirm)"),
            text("    r          Refresh list"),
            text("    /          Start search (Ctrl+F: toggle fuzzy/exact)"),
            text(""),
            text("  General:"),
            text("    ?          Toggle this help"),
            text("    : or p     Command prompt (e.g., :q to quit)"),
            text("    q/Esc      Quit"),
            text(""),
            separator() | dim,
            text(""),
            text("  Press any key to continue") | dim | center,
            text(""),
        }) | border | center;
    }
    
    void NavigateDown() {
        if (activeColumn == 0 && selectedCollection < static_cast<int>(collections.size()) - 1) {
            selectedCollection++;
        } else if (activeColumn == 1 && selected < static_cast<int>(documents.size()) - 1) {
            selected++;
            LoadPreview();
        }
    }
    
    void NavigateUp() {
        if (activeColumn == 0 && selectedCollection > 0) {
            selectedCollection--;
        } else if (activeColumn == 1 && selected > 0) {
            selected--;
            LoadPreview();
        }
    }
    
    void JumpToTop() {
        if (activeColumn == 0) {
            selectedCollection = 0;
        } else if (activeColumn == 1) {
            selected = 0;
            LoadPreview();
        }
    }
    
    void JumpToBottom() {
        if (activeColumn == 0 && !collections.empty()) {
            selectedCollection = collections.size() - 1;
        } else if (activeColumn == 1 && !documents.empty()) {
            selected = documents.size() - 1;
            LoadPreview();
        }
    }
    
    void DeleteSelected() {
        if (activeColumn == 1 && selected < static_cast<int>(documents.size())) {
            DeleteDocument(documents[selected]);
            RefreshDocuments();
            if (selected >= static_cast<int>(documents.size()) && selected > 0) {
                selected--;
            }
            LoadPreview();
        }
    }
    
    void ApplySearch() {
        if (searchQuery.empty()) {
            documents = allDocuments;
            selected = 0;
            return;
        }
        
        if (useFuzzySearch) {
            // Use fuzzy search from metadata repository
            auto metadataRepo = cli->getMetadataRepository();
            if (metadataRepo) {
                auto fuzzyResult = metadataRepo->fuzzySearch(searchQuery, 0.6f, 50);
                if (fuzzyResult.has_value() && fuzzyResult.value().isSuccess()) {
                    documents.clear();
                    for (const auto& result : fuzzyResult.value().results) {
                        // Convert SearchResult DocumentInfo to our BrowseDocumentInfo
                        DocumentInfo doc;
                        doc.hash = result.document.sha256Hash;
                        doc.name = result.document.fileName;
                        doc.size = static_cast<size_t>(result.document.fileSize);
                        doc.type = result.document.mimeType;
                        doc.createdAt = result.document.createdTime;
                        doc.id = result.document.id;
                        documents.push_back(doc);
                    }
                } else {
                    // Fallback to basic search
                    documents = FilterDocumentsBasic();
                }
            } else {
                documents = FilterDocumentsBasic();
            }
        } else {
            documents = FilterDocumentsBasic();
        }
        
        selected = 0;
        LoadPreview();
    }
    
    std::vector<DocumentInfo> FilterDocumentsBasic() {
        std::vector<DocumentInfo> filtered;
        std::string lowerQuery = searchQuery;
        std::transform(lowerQuery.begin(), lowerQuery.end(), lowerQuery.begin(), ::tolower);
        
        for (const auto& doc : allDocuments) {
            std::string lowerName = doc.name;
            std::transform(lowerName.begin(), lowerName.end(), lowerName.begin(), ::tolower);
            
            if (lowerName.find(lowerQuery) != std::string::npos ||
                doc.hash.find(searchQuery) != std::string::npos) {
                filtered.push_back(doc);
            }
        }
        
        return filtered;
    }
    
    std::vector<DocumentInfo> FilterDocuments() {
        return documents;  // Already filtered by ApplySearch
    }
    
    void LoadPreview() {
        previewLines.clear();
        if (selected >= 0 && selected < static_cast<int>(documents.size())) {
            const auto& doc = documents[selected];

            // Try to load text content from metadata repository first
            auto metadataRepo = cli->getMetadataRepository();
            if (metadataRepo) {
                auto contentRes = metadataRepo->getContent(doc.id);
                if (contentRes.has_value()) {
                    const auto& optContent = contentRes.value();
                    if (optContent.has_value()) {
                        std::istringstream iss(optContent->contentText);
                        std::string line;
                        while (std::getline(iss, line) && previewLines.size() < 200) {
                            if (!line.empty() && line.back() == '\r') line.pop_back();
                            previewLines.push_back(line);
                        }
                        if (!previewLines.empty()) {
                            return;
                        }
                    }
                }
            }

            // Fallback to raw bytes from content store and attempt to show as text
            auto store = cli->getContentStore();
            if (store) {
                auto bytesRes = store->retrieveBytes(doc.hash);
                if (bytesRes && !bytesRes.value().empty()) {
                    const auto& bytes = bytesRes.value();
                    size_t max_bytes = std::min<size_t>(bytes.size(), 64 * 1024);
                    std::string content;
                    content.resize(max_bytes);
                    for (size_t i = 0; i < max_bytes; ++i) {
                        content[i] = static_cast<char>(std::to_integer<unsigned char>(bytes[i]));
                    }
                    // Simple binary detection
                    size_t check = std::min<size_t>(max_bytes, 1024);
                    size_t nonprint = 0;
                    for (size_t i = 0; i < check; ++i) {
                        unsigned char c = static_cast<unsigned char>(content[i]);
                        if (c == '\n' || c == '\r' || c == '\t') continue;
                        if (!std::isprint(c)) nonprint++;
                    }
                    if (nonprint > check / 10) {
                        previewLines.push_back("Binary content. Preview unavailable.");
                        return;
                    }
                    std::istringstream iss(content);
                    std::string line;
                    while (std::getline(iss, line) && previewLines.size() < 200) {
                        if (!line.empty() && line.back() == '\r') line.pop_back();
                        previewLines.push_back(line);
                    }
                    if (previewLines.empty()) {
                        previewLines.push_back("No preview available.");
                    }
                } else {
                    previewLines.push_back("No preview available.");
                }
            }
        }
    }
    
    void ViewDocument(const DocumentInfo& doc) {
        statusMessage = "Viewing: " + doc.name;
        // In future, could open in pager or editor
    }
    
    void DeleteDocument(const DocumentInfo& doc) {
        auto store = cli->getContentStore();
        if (store) {
            auto result = store->remove(doc.hash);
            if (result && result.value()) {
                statusMessage = "Deleted: " + doc.name;
            } else {
                statusMessage = "Failed to delete: " + doc.name;
            }
        }
    }
    
    void RefreshDocuments() {
        allDocuments.clear();
        documents.clear();
        
        auto metadataRepo = cli->getMetadataRepository();
        if (metadataRepo) {
            // Try to get real documents from metadata repository
            // First check if we have any documents
            auto countResult = metadataRepo->getDocumentCount();
            if (countResult.has_value() && countResult.value() > 0) {
                // Get documents by using wildcard path pattern
                auto documentsResult = metadataRepo->findDocumentsByPath("%");
                if (documentsResult.has_value()) {
                    for (const auto& docInfo : documentsResult.value()) {
                        DocumentInfo doc;
                        doc.hash = docInfo.sha256Hash;
                        doc.name = docInfo.fileName;
                        doc.size = static_cast<size_t>(docInfo.fileSize);
                        doc.type = docInfo.mimeType;
                        doc.createdAt = docInfo.createdTime;
                        doc.id = docInfo.id;
                        allDocuments.push_back(doc);
                    }
                    
                    documents = allDocuments;  // Initially show all documents
                    statusMessage = std::to_string(allDocuments.size()) + " documents loaded";
                } else {
                    statusMessage = "Failed to query documents: " + documentsResult.error().message;
                }
            } else {
                // No documents in metadata repository
                auto countValue = countResult.has_value() ? countResult.value() : 0;
                statusMessage = std::to_string(countValue) + " documents in metadata DB";
            }
        } else {
            auto store = cli->getContentStore();
            if (store) {
                auto stats = store->getStats();
                // Create placeholder documents for content store objects
                for (size_t i = 0; i < std::min(stats.totalObjects, static_cast<uint64_t>(50)); ++i) {
                    DocumentInfo doc;
                    doc.hash = std::string(64, 'a' + (i % 26));
                    doc.name = "object_" + std::to_string(i + 1);
                    doc.size = 1024 * (i + 1);
                    doc.type = "unknown";
                    doc.createdAt = std::chrono::system_clock::now() - std::chrono::hours(i);
                    doc.id = static_cast<int64_t>(i);
                    allDocuments.push_back(doc);
                }
                
                documents = allDocuments;
                statusMessage = std::to_string(allDocuments.size()) + " objects (no metadata DB)";
            } else {
                statusMessage = "Storage not initialized";
            }
        }
        LoadPreview();
    }
    
    void RefreshCollections() {
        collections.clear();
        // TODO: Query real collections from metadata
        // For now, just show some example collections
        collections.push_back("recent");
        collections.push_back("documents");
        collections.push_back("images");
        collections.push_back("archives");
    }
    
    std::string FormatSize(size_t size) {
        if (size < 1024) {
            return std::to_string(size) + " B";
        } else if (size < 1024 * 1024) {
            return std::to_string(size / 1024) + " KB";
        } else if (size < 1024 * 1024 * 1024) {
            return std::to_string(size / (1024 * 1024)) + " MB";
        } else {
            return std::to_string(size / (1024 * 1024 * 1024)) + " GB";
        }
    }
};

BrowseCommand::BrowseCommand() : pImpl(std::make_unique<Impl>()) {}
BrowseCommand::~BrowseCommand() = default;

std::string BrowseCommand::getName() const { 
    return "browse"; 
}

std::string BrowseCommand::getDescription() const { 
    return "Browse documents in TUI (ranger-style interface)";
}

void BrowseCommand::registerCommand(CLI::App& app, YamsCLI* cli) {
    pImpl->cli = cli;
    
    auto* cmd = app.add_subcommand("browse", getDescription());
    
    cmd->callback([this]() {
        auto result = execute();
        if (!result) {
            spdlog::error("Browse failed: {}", result.error().message);
            std::exit(1);
        }
    });
}

Result<void> BrowseCommand::execute() {
    try {
        // Ensure storage is initialized
        auto ensured = pImpl->cli->ensureStorageInitialized();
        if (!ensured) {
            return ensured;
        }
        
        // Check if we're in a TTY
        if (!isatty(STDOUT_FILENO)) {
            return Error{ErrorCode::InvalidState, 
                        "Browse command requires an interactive terminal"};
        }
        
        // Create and run the TUI
        auto screen = ScreenInteractive::Fullscreen();
        pImpl->screen = &screen;
        auto ui = pImpl->CreateUI();
        screen.Loop(ui);
        pImpl->screen = nullptr;
        
        return Result<void>();
    } catch (const std::exception& e) {
        return Error{ErrorCode::Unknown, std::string("Browse error: ") + e.what()};
    }
}

// Factory function
std::unique_ptr<ICommand> createBrowseCommand() {
    return std::make_unique<BrowseCommand>();
}

} // namespace yams::cli