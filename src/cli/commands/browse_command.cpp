#include <yams/cli/browse_command.h>
#include <yams/cli/yams_cli.h>
#include <yams/cli/tui/browse_services.hpp>
#include <yams/cli/tui/browse_state.hpp>
#include <yams/cli/tui/browse_commands.hpp>

#include <spdlog/spdlog.h>

// ImTUI includes
#include <imtui/imtui.h>
#include <imtui/imtui-impl-ncurses.h>
#include <imtui/imtui-impl-text.h>
#include <imgui.h>

#include <vector>
#include <algorithm>
#include <chrono>
#include <optional>
#include <cstddef>
#include <cctype>
#include <atomic>
#include <thread>
#include <mutex>
#include <unistd.h>  // For isatty, STDIN_FILENO, STDOUT_FILENO
#include <cstdlib>  // For getenv
#include <cstring>  // For strlen

namespace yams::cli {

class BrowseCommand::Impl {
public:
    YamsCLI* cli;
    
    // State
    tui::BrowseState state;
    tui::BrowseServices services;
    tui::BrowseCommands cmdParser;
    
    // Viewer state
    bool viewerOpen = false;
    std::vector<std::string> viewerLines;
    int viewerScrollOffset = 0;
    bool viewerHexMode = false;
    
    // Preview state
    int previewScrollOffset = 0;
    
    // Async loading
    std::atomic<bool> isLoading{true};
    std::atomic<int> loadProgress{0};
    std::atomic<int> totalDocuments{0};
    std::thread loadThread;
    std::mutex documentsMutex;
    std::string loadingMessage = "Loading documents...";
    
    // ImTUI screen
    ImTui::TScreen* screen = nullptr;
    
    // Window sizes
    float columnWidths[3] = {0.2f, 0.4f, 0.4f}; // Collections, Documents, Preview
    
    // Exit flag
    bool shouldExit = false;
    
    Impl() : services(nullptr) {}
    
    ~Impl() {
        if (loadThread.joinable()) {
            loadThread.join();
        }
    }
    
    void LoadDocumentsAsync(YamsCLI* cli) {
        loadThread = std::thread([this, cli]() {
            try {
                spdlog::debug("Starting async document load");
                
                // Ensure cli is valid
                if (!cli) {
                    spdlog::error("CLI is null in LoadDocumentsAsync");
                    loadingMessage = "Error: CLI not initialized";
                    isLoading = false;
                    return;
                }
                
                // Get total count first
                auto metadataRepo = cli->getMetadataRepository();
                if (!metadataRepo) {
                    spdlog::error("MetadataRepository is null");
                    loadingMessage = "Error: Database not initialized";
                    isLoading = false;
                    return;
                }
                auto countResult = metadataRepo->getDocumentCount();
                totalDocuments = countResult.has_value() ? countResult.value() : 0;
                
                // Load documents
                services = tui::BrowseServices(cli);
                auto docsResult = services.loadAllDocuments(&loadingMessage);
                
                if (!docsResult.empty()) {
                    auto docs = docsResult;
                    
                    // Convert to DocEntry format and update progress
                    for (size_t i = 0; i < docs.size(); ++i) {
                        tui::DocEntry entry;
                        entry.hash = docs[i].hash;
                        entry.name = docs[i].name;
                        entry.size = docs[i].size;
                        entry.type = docs[i].type;
                        entry.createdAt = docs[i].createdAt;
                        entry.id = docs[i].id;
                        
                        {
                            std::lock_guard<std::mutex> lock(documentsMutex);
                            state.documents.push_back(entry);
                            state.allDocuments.push_back(entry);
                        }
                        
                        loadProgress = static_cast<int>(i + 1);
                        
                        // Yield periodically for UI responsiveness
                        if (i % 10 == 0) {
                            std::this_thread::sleep_for(std::chrono::milliseconds(1));
                        }
                    }
                    
                    loadingMessage = "Loaded " + std::to_string(docs.size()) + " documents";
                } else {
                    loadingMessage = "Failed to load documents";
                }
                
                isLoading = false;
                spdlog::debug("Async document load complete");
                
            } catch (const std::exception& e) {
                spdlog::error("Error loading documents: {}", e.what());
                loadingMessage = "Error: " + std::string(e.what());
                isLoading = false;
            }
        });
    }
    
    void RenderMainUI() {
        // Render viewer overlay if open
        if (viewerOpen) {
            RenderViewer();
            return;
        }
        
        // Menu bar
        if (ImGui::BeginMainMenuBar()) {
            ImGui::Text("YAMS Browser");
            ImGui::Separator();
            ImGui::Text("Documents: %zu", state.documents.size());
            if (!loadingMessage.empty()) {
                ImGui::Separator();
                ImGui::Text("%s", loadingMessage.c_str());
            }
            ImGui::EndMainMenuBar();
        }
        
        // Loading progress overlay
        if (isLoading) {
            ImGui::SetNextWindowPos(ImVec2(ImGui::GetIO().DisplaySize.x * 0.5f, ImGui::GetIO().DisplaySize.y * 0.5f), ImGuiCond_Always, ImVec2(0.5f, 0.5f));
            ImGui::SetNextWindowSize(ImVec2(400, 100));
            
            if (ImGui::Begin("Loading", nullptr, ImGuiWindowFlags_NoResize | ImGuiWindowFlags_NoMove | ImGuiWindowFlags_NoCollapse)) {
                ImGui::Text("Loading documents from database...");
                
                float progress = totalDocuments > 0 ? 
                    static_cast<float>(loadProgress) / static_cast<float>(totalDocuments) : 0.0f;
                ImGui::ProgressBar(progress, ImVec2(-1, 0));
                
                if (totalDocuments > 0) {
                    ImGui::Text("%d / %d documents", loadProgress.load(), totalDocuments.load());
                }
                
                ImGui::Separator();
                ImGui::Text("Press ESC to cancel");
            }
            ImGui::End();
        }
        
        // Main content area - three columns
        ImGui::SetNextWindowPos(ImVec2(0, 20)); // Below menu bar
        ImGui::SetNextWindowSize(ImVec2(ImGui::GetIO().DisplaySize.x, ImGui::GetIO().DisplaySize.y - 40)); // Leave space for status bar
        
        if (ImGui::Begin("Browse", nullptr, ImGuiWindowFlags_NoTitleBar | ImGuiWindowFlags_NoResize | ImGuiWindowFlags_NoMove)) {
            ImGui::Columns(3, "BrowseColumns");
            ImGui::SetColumnWidth(0, ImGui::GetWindowWidth() * columnWidths[0]);
            ImGui::SetColumnWidth(1, ImGui::GetWindowWidth() * columnWidths[1]);
            
            // Collections column
            RenderCollections();
            ImGui::NextColumn();
            
            // Documents column
            RenderDocuments();
            ImGui::NextColumn();
            
            // Preview column
            RenderPreview();
        }
        ImGui::End();
        
        // Status bar
        RenderStatusBar();
        
        // Help overlay
        if (state.showHelp) {
            RenderHelp();
        }
    }
    
    void RenderCollections() {
        ImGui::Text("Collections");
        ImGui::Separator();
        
        if (ImGui::Selectable("All Documents", state.selectedCollection == 0)) {
            state.selectedCollection = 0;
            // Reset document filter
            state.documents = state.allDocuments;
        }
        
        // TODO: Add actual collections support
        for (size_t i = 0; i < state.collections.size(); ++i) {
            if (ImGui::Selectable(state.collections[i].c_str(), state.selectedCollection == static_cast<int>(i + 1))) {
                state.selectedCollection = static_cast<int>(i + 1);
                // Filter documents by collection
            }
        }
    }
    
    void RenderDocuments() {
        ImGui::Text("Documents");
        ImGui::Separator();
        
        // Search input
        if (state.searchMode) {
            static char searchBuf[256] = {};
            if (ImGui::InputText("Search", searchBuf, sizeof(searchBuf))) {
                state.searchQuery = searchBuf;
                ApplySearch();
            }
        }
        
        // Document list
        ImGui::BeginChild("DocumentList");
        
        std::lock_guard<std::mutex> lock(documentsMutex);
        for (size_t i = 0; i < state.documents.size(); ++i) {
            const auto& doc = state.documents[i];
            
            bool isSelected = (state.selected == static_cast<int>(i));
            if (ImGui::Selectable(doc.name.c_str(), isSelected)) {
                state.selected = static_cast<int>(i);
                UpdatePreview();
            }
            
            if (isSelected) {
                ImGui::SetItemDefaultFocus();
            }
            
            // Show size and date on same line
            ImGui::SameLine(ImGui::GetWindowWidth() - 150);
            ImGui::TextDisabled("%zu bytes", doc.size);
        }
        
        ImGui::EndChild();
    }
    
    void RenderPreview() {
        ImGui::Text("Preview");
        ImGui::Separator();
        
        if (state.selected >= 0 && state.selected < static_cast<int>(state.documents.size())) {
            const auto& doc = state.documents[state.selected];
            
            ImGui::Text("Name: %s", doc.name.c_str());
            ImGui::Text("Hash: %.8s...", doc.hash.c_str());
            ImGui::Text("Size: %zu bytes", doc.size);
            ImGui::Text("Type: %s", doc.type.c_str());
            
            ImGui::Separator();
            
            // Show scroll info if in preview column
            if (state.activeColumn == tui::Column::Preview && !state.previewLines.empty()) {
                ImGui::Text("Lines %d-%d of %zu (j/k to scroll)", 
                           previewScrollOffset + 1,
                           std::min(previewScrollOffset + 20, static_cast<int>(state.previewLines.size())),
                           state.previewLines.size());
                ImGui::Separator();
            }
            
            // Preview content with scrolling
            ImGui::BeginChild("PreviewContent");
            
            // Calculate visible lines
            float lineHeight = ImGui::GetTextLineHeightWithSpacing();
            int visibleLines = static_cast<int>(ImGui::GetContentRegionAvail().y / lineHeight);
            
            // Render only visible lines
            for (int i = 0; i < visibleLines && (previewScrollOffset + i) < static_cast<int>(state.previewLines.size()); ++i) {
                ImGui::TextUnformatted(state.previewLines[previewScrollOffset + i].c_str());
            }
            
            ImGui::EndChild();
        } else {
            ImGui::TextDisabled("Select a document to preview");
        }
    }
    
    void RenderStatusBar() {
        ImGui::SetNextWindowPos(ImVec2(0, ImGui::GetIO().DisplaySize.y - 20));
        ImGui::SetNextWindowSize(ImVec2(ImGui::GetIO().DisplaySize.x, 20));
        
        if (ImGui::Begin("StatusBar", nullptr, ImGuiWindowFlags_NoTitleBar | ImGuiWindowFlags_NoResize | ImGuiWindowFlags_NoMove | ImGuiWindowFlags_NoScrollbar)) {
            if (!state.statusMessage.empty()) {
                ImGui::Text("%s", state.statusMessage.c_str());
            } else {
                ImGui::Text("Ready | Documents: %zu | Press ? for help", state.documents.size());
            }
        }
        ImGui::End();
    }
    
    void RenderHelp() {
        ImGui::SetNextWindowPos(ImVec2(ImGui::GetIO().DisplaySize.x * 0.5f, ImGui::GetIO().DisplaySize.y * 0.5f), ImGuiCond_Always, ImVec2(0.5f, 0.5f));
        ImGui::SetNextWindowSize(ImVec2(600, 400));
        
        if (ImGui::Begin("Help", &state.showHelp)) {
            ImGui::Text("YAMS Browser - Keyboard Shortcuts");
            ImGui::Separator();
            
            ImGui::Text("Navigation:");
            ImGui::BulletText("j/Down - Move down");
            ImGui::BulletText("k/Up - Move up");
            ImGui::BulletText("Tab - Next column");
            ImGui::BulletText("Shift+Tab - Previous column");
            
            ImGui::Separator();
            ImGui::Text("Actions:");
            ImGui::BulletText("Enter - View document (full screen)");
            ImGui::BulletText("o - Open in external pager");
            ImGui::BulletText("d - Delete document");
            ImGui::BulletText("/ - Search");
            ImGui::BulletText(": - Command mode");
            
            ImGui::Separator();
            ImGui::Text("Viewer Mode:");
            ImGui::BulletText("j/k - Scroll line by line");
            ImGui::BulletText("PageUp/PageDown - Scroll by page");
            ImGui::BulletText("g/G - Go to top/bottom");
            ImGui::BulletText("x - Toggle hex view");
            ImGui::BulletText("ESC/q - Close viewer");
            
            ImGui::Separator();
            ImGui::Text("Commands:");
            ImGui::BulletText(":q - Quit");
            ImGui::BulletText(":refresh - Refresh document list");
            ImGui::BulletText(":hex - Toggle hex view");
            ImGui::BulletText(":help - Show this help");
            
            ImGui::Separator();
            if (ImGui::Button("Close")) {
                state.showHelp = false;
            }
        }
        ImGui::End();
    }
    
    void RenderViewer() {
        // Full screen viewer overlay
        ImGui::SetNextWindowPos(ImVec2(0, 0));
        ImGui::SetNextWindowSize(ImGui::GetIO().DisplaySize);
        
        if (ImGui::Begin("Document Viewer", &viewerOpen, ImGuiWindowFlags_NoTitleBar | ImGuiWindowFlags_NoResize | ImGuiWindowFlags_NoMove | ImGuiWindowFlags_NoCollapse)) {
            // Header
            if (state.selected >= 0 && state.selected < static_cast<int>(state.documents.size())) {
                const auto& doc = state.documents[state.selected];
                ImGui::Text("Viewing: %s", doc.name.c_str());
                ImGui::SameLine();
                ImGui::Text(" | Size: %zu bytes", doc.size);
                ImGui::SameLine();
                ImGui::Text(" | Mode: %s", viewerHexMode ? "HEX" : "TEXT");
                ImGui::SameLine();
                ImGui::Text(" | Press ESC or q to close, x for hex mode");
            }
            ImGui::Separator();
            
            // Content area
            ImGui::BeginChild("ViewerContent", ImVec2(0, ImGui::GetContentRegionAvail().y - 20));
            
            // Calculate visible lines
            float lineHeight = ImGui::GetTextLineHeightWithSpacing();
            int visibleLines = static_cast<int>(ImGui::GetContentRegionAvail().y / lineHeight);
            
            // Render visible lines
            for (int i = 0; i < visibleLines && (viewerScrollOffset + i) < static_cast<int>(viewerLines.size()); ++i) {
                ImGui::TextUnformatted(viewerLines[viewerScrollOffset + i].c_str());
            }
            
            ImGui::EndChild();
            
            // Status bar
            ImGui::Separator();
            ImGui::Text("Line %d/%zu | Use j/k to scroll, g/G for top/bottom", 
                       viewerScrollOffset + 1, viewerLines.size());
        }
        ImGui::End();
    }
    
    void HandleInput() {
        ImGuiIO& io = ImGui::GetIO();
        
        // Handle viewer input first if open
        if (viewerOpen) {
            HandleViewerInput();
            return;
        }
        
        // ESC to cancel loading or exit modes
        if (ImGui::IsKeyPressed(ImGuiKey_Escape)) {
            if (isLoading) {
                isLoading = false;
            } else if (state.showHelp) {
                state.showHelp = false;
            } else if (state.searchMode) {
                state.searchMode = false;
            } else if (state.commandMode) {
                state.commandMode = false;
            }
        }
        
        // Help
        if (ImGui::IsKeyPressed('?')) {
            state.showHelp = !state.showHelp;
        }
        
        // Navigation
        if (!state.commandMode && !state.searchMode) {
            // Check if we're in preview column for scrolling
            if (state.activeColumn == tui::Column::Preview) {
                // Preview scrolling
                int maxScroll = std::max(0, static_cast<int>(state.previewLines.size()) - 10);
                
                if (ImGui::IsKeyPressed('j') || ImGui::IsKeyPressed(ImGuiKey_DownArrow)) {
                    previewScrollOffset = std::min(previewScrollOffset + 1, maxScroll);
                } else if (ImGui::IsKeyPressed('k') || ImGui::IsKeyPressed(ImGuiKey_UpArrow)) {
                    previewScrollOffset = std::max(previewScrollOffset - 1, 0);
                } else if (ImGui::IsKeyPressed(ImGuiKey_PageDown)) {
                    previewScrollOffset = std::min(previewScrollOffset + 10, maxScroll);
                } else if (ImGui::IsKeyPressed(ImGuiKey_PageUp)) {
                    previewScrollOffset = std::max(previewScrollOffset - 10, 0);
                } else if (ImGui::IsKeyPressed('g')) {
                    previewScrollOffset = 0;
                } else if (ImGui::IsKeyPressed('G')) {
                    previewScrollOffset = maxScroll;
                }
            } else {
                // Document list navigation
                if (ImGui::IsKeyPressed('j') || ImGui::IsKeyPressed(ImGuiKey_DownArrow)) {
                    state.selected = std::min(state.selected + 1, static_cast<int>(state.documents.size()) - 1);
                    UpdatePreview();
                }
                if (ImGui::IsKeyPressed('k') || ImGui::IsKeyPressed(ImGuiKey_UpArrow)) {
                    state.selected = std::max(state.selected - 1, 0);
                    UpdatePreview();
                }
            }
            
            // Tab navigation
            if (ImGui::IsKeyPressed(ImGuiKey_Tab)) {
                if (io.KeyShift) {
                    state.activeColumn = static_cast<tui::Column>((static_cast<int>(state.activeColumn) + 2) % 3);
                } else {
                    state.activeColumn = static_cast<tui::Column>((static_cast<int>(state.activeColumn) + 1) % 3);
                }
            }
            
            // Search mode
            if (ImGui::IsKeyPressed('/')) {
                state.searchMode = true;
            }
            
            // Command mode
            if (ImGui::IsKeyPressed(':')) {
                state.commandMode = true;
                state.commandBuffer.clear();
            }
            
            // Enter - Open viewer
            if (ImGui::IsKeyPressed(ImGuiKey_Enter)) {
                OpenViewer();
            }
            
            // o - Open in external pager
            if (ImGui::IsKeyPressed('o')) {
                OpenInExternalPager();
            }
            
            // Quit - use false to prevent key repeat
            if (ImGui::IsKeyPressed('q', false)) {
                shouldExit = true;
            }
        }
        
        // Command mode input
        if (state.commandMode) {
            static char cmdBuf[256] = {};
            if (ImGui::InputText(":", cmdBuf, sizeof(cmdBuf), ImGuiInputTextFlags_EnterReturnsTrue)) {
                state.commandBuffer = cmdBuf;
                ExecuteCommand();
                state.commandMode = false;
                cmdBuf[0] = '\0';
            }
        }
    }
    
    void HandleViewerInput() {
        // ESC or q to close viewer
        if (ImGui::IsKeyPressed(ImGuiKey_Escape) || ImGui::IsKeyPressed('q', false)) {
            viewerOpen = false;
            return;
        }
        
        // x to toggle hex mode
        if (ImGui::IsKeyPressed('x')) {
            viewerHexMode = !viewerHexMode;
            LoadViewerContent();
            return;
        }
        
        // Navigation
        int maxScroll = std::max(0, static_cast<int>(viewerLines.size()) - 10);
        
        // j/Down - scroll down one line
        if (ImGui::IsKeyPressed('j') || ImGui::IsKeyPressed(ImGuiKey_DownArrow)) {
            viewerScrollOffset = std::min(viewerScrollOffset + 1, maxScroll);
        }
        
        // k/Up - scroll up one line
        if (ImGui::IsKeyPressed('k') || ImGui::IsKeyPressed(ImGuiKey_UpArrow)) {
            viewerScrollOffset = std::max(viewerScrollOffset - 1, 0);
        }
        
        // PageDown - scroll down one page
        if (ImGui::IsKeyPressed(ImGuiKey_PageDown)) {
            viewerScrollOffset = std::min(viewerScrollOffset + 20, maxScroll);
        }
        
        // PageUp - scroll up one page
        if (ImGui::IsKeyPressed(ImGuiKey_PageUp)) {
            viewerScrollOffset = std::max(viewerScrollOffset - 20, 0);
        }
        
        // g - go to top
        if (ImGui::IsKeyPressed('g')) {
            viewerScrollOffset = 0;
        }
        
        // G - go to bottom
        if (ImGui::IsKeyPressed('G')) {
            viewerScrollOffset = maxScroll;
        }
    }
    
    void OpenViewer() {
        if (state.selected >= 0 && state.selected < static_cast<int>(state.documents.size())) {
            viewerOpen = true;
            viewerScrollOffset = 0;
            viewerHexMode = false;
            LoadViewerContent();
        }
    }
    
    void LoadViewerContent() {
        if (state.selected >= 0 && state.selected < static_cast<int>(state.documents.size())) {
            const auto& doc = state.documents[state.selected];
            
            // Load full content
            auto contentResult = services.loadTextContent(doc.id);
            if (contentResult.has_value()) {
                const auto& content = contentResult.value();
                
                if (viewerHexMode) {
                    // Generate hex dump - convert string to bytes
                    std::vector<std::byte> bytes;
                    bytes.reserve(content.size());
                    for (char c : content) {
                        bytes.push_back(static_cast<std::byte>(c));
                    }
                    viewerLines = services.toHexDump(bytes, 16, 0); // 16 bytes per line, no limit
                } else {
                    // Split into lines for text view
                    viewerLines = services.splitLines(content, 0); // 0 for no limit
                }
            } else {
                viewerLines = {"Error loading document content"};
            }
        }
    }
    
    void OpenInExternalPager() {
        if (state.selected >= 0 && state.selected < static_cast<int>(state.documents.size())) {
            const auto& doc = state.documents[state.selected];
            
            // Load content
            auto contentResult = services.loadTextContent(doc.id);
            if (contentResult.has_value()) {
                // Suspend ImTUI and open in pager
                ImTui_ImplNcurses_Shutdown();
                
                // Use the services pager function
                std::string error;
                services.openInPager(doc.name, contentResult.value(), {}, &error);
                if (!error.empty()) {
                    state.statusMessage = "Pager error: " + error;
                }
                
                // Reinitialize ImTUI
                screen = ImTui_ImplNcurses_Init(true);
                ImTui_ImplText_Init();
            } else {
                state.statusMessage = "Error loading document content";
            }
        }
    }
    
    void ExecuteCommand() {
        bool exitRequested = false;
        tui::CommandActions actions;
        
        actions.refresh = [this](tui::BrowseState&) {
            RefreshDocuments();
        };
        
        actions.showHelp = [](tui::BrowseState& s) {
            s.showHelp = true;
        };
        
        // Add hex command support
        if (state.commandBuffer == "hex") {
            if (viewerOpen) {
                viewerHexMode = !viewerHexMode;
                LoadViewerContent();
                state.statusMessage = viewerHexMode ? "Hex mode enabled" : "Text mode enabled";
            } else {
                state.statusMessage = "Open a document first (press Enter)";
            }
        } else if (state.commandBuffer == "open") {
            OpenInExternalPager();
        } else if (cmdParser.execute(state.commandBuffer, state, exitRequested, actions)) {
            if (exitRequested) {
                shouldExit = true;
            }
        } else {
            state.statusMessage = "Unknown command: " + state.commandBuffer;
        }
    }
    
    void UpdatePreview() {
        if (state.selected >= 0 && state.selected < static_cast<int>(state.documents.size())) {
            const auto& doc = state.documents[state.selected];
            
            // Reset scroll when changing documents
            previewScrollOffset = 0;
            
            // Load preview content - load more lines for scrolling
            auto contentResult = services.loadTextContent(doc.id);
            if (contentResult.has_value()) {
                state.previewLines = services.splitLines(
                    contentResult.value(), 
                    1000  // Load up to 1000 lines for preview
                );
            }
        }
    }
    
    void ApplySearch() {
        if (state.searchQuery.empty()) {
            state.documents = state.allDocuments;
        } else {
            state.documents.clear();
            for (const auto& doc : state.allDocuments) {
                if (doc.name.find(state.searchQuery) != std::string::npos) {
                    state.documents.push_back(doc);
                }
            }
        }
        state.selected = 0;
        UpdatePreview();
    }
    
    void RefreshDocuments() {
        isLoading = true;
        loadProgress = 0;
        state.documents.clear();
        state.allDocuments.clear();
        
        if (loadThread.joinable()) {
            loadThread.join();
        }
        
        LoadDocumentsAsync(cli);
    }
    
    void Run() {
        // Main ImTUI loop
        int frameCount = 0;
        spdlog::debug("Starting main UI loop");
        
        while (!shouldExit) {
            frameCount++;
            if (frameCount == 1 || frameCount % 60 == 0) {
                spdlog::debug("Frame {}, loading: {}", frameCount, isLoading.load());
            }
            
            ImTui_ImplNcurses_NewFrame();
            ImTui_ImplText_NewFrame();
            ImGui::NewFrame();
            
            HandleInput();
            RenderMainUI();
            
            ImGui::Render();
            ImTui_ImplText_RenderDrawData(ImGui::GetDrawData(), screen);
            ImTui_ImplNcurses_DrawScreen();
        }
        
        spdlog::debug("Exited main loop after {} frames", frameCount);
    }
};

BrowseCommand::BrowseCommand() : pImpl(std::make_unique<Impl>()) {}
BrowseCommand::~BrowseCommand() = default;

std::string BrowseCommand::getName() const { 
    return "browse"; 
}

std::string BrowseCommand::getDescription() const { 
    return "Browse documents with TUI interface"; 
}

void BrowseCommand::registerCommand(CLI::App& app, YamsCLI* cli) {
    auto* cmd = app.add_subcommand("browse", getDescription());
    cmd->callback([this, cli]() {
        pImpl->cli = cli;
        auto result = execute();
        if (!result) {
            spdlog::error("Browse command failed: {}", result.error().message);
            exit(1);
        }
    });
}

Result<void> BrowseCommand::execute() {
    try {
        spdlog::debug("BrowseCommand::execute() starting");
        
        // Check if we're in an interactive terminal - need both stdin and stdout
        if (!isatty(STDIN_FILENO) || !isatty(STDOUT_FILENO)) {
            std::string message = "Browse command requires an interactive terminal. ";
            if (!isatty(STDIN_FILENO)) {
                message += "STDIN is not a terminal. ";
            }
            if (!isatty(STDOUT_FILENO)) {
                message += "STDOUT is not a terminal. ";
            }
            message += "Please run this command in a terminal.";
            
            spdlog::info("{}", message);
            return Result<void>(Error(
                ErrorCode::InvalidOperation,
                message
            ));
        }
        
        // Additional terminal environment check
        const char* term = getenv("TERM");
        if (!term || strlen(term) == 0) {
            spdlog::info("TERM environment variable not set. Please run in a proper terminal.");
            return Result<void>(Error(
                ErrorCode::InvalidOperation,
                "TERM environment variable not set"
            ));
        }
        
        spdlog::debug("Terminal environment: TERM={}", term);
        spdlog::debug("Initializing ImTUI");
        
        // Initialize ImTUI
        IMGUI_CHECKVERSION();
        ImGui::CreateContext();
        
        pImpl->screen = ImTui_ImplNcurses_Init(true);
        if (!pImpl->screen) {
            spdlog::error("Failed to initialize ncurses");
            ImGui::DestroyContext();
            return Result<void>(Error(
                ErrorCode::InternalError,
                "Failed to initialize terminal UI. Please ensure your terminal supports ncurses."
            ));
        }
        
        spdlog::debug("Ncurses initialized, screen pointer: {}", static_cast<void*>(pImpl->screen));
        
        ImTui_ImplText_Init();
        
        spdlog::debug("ImTUI initialized successfully");
        
        // Ensure CLI is initialized
        if (!pImpl->cli) {
            spdlog::error("CLI context not initialized");
            ImTui_ImplText_Shutdown();
            ImTui_ImplNcurses_Shutdown();
            ImGui::DestroyContext();
            return Result<void>(Error(
                ErrorCode::InternalError,
                "CLI context not initialized"
            ));
        }
        
        // Initialize storage before starting async operations
        spdlog::debug("Ensuring storage is initialized");
        auto storageResult = pImpl->cli->ensureStorageInitialized();
        if (!storageResult) {
            spdlog::error("Failed to initialize storage: {}", storageResult.error().message);
            ImTui_ImplText_Shutdown();
            ImTui_ImplNcurses_Shutdown();
            ImGui::DestroyContext();
            return Result<void>(Error(
                ErrorCode::InternalError,
                "Failed to initialize storage: " + storageResult.error().message
            ));
        }
        
        // Start async document loading
        spdlog::debug("Starting async document load");
        pImpl->LoadDocumentsAsync(pImpl->cli);
        
        // Run the main UI loop
        spdlog::debug("Starting main UI loop");
        try {
            pImpl->Run();
        } catch (const std::exception& e) {
            spdlog::error("Error in main UI loop: {}", e.what());
            // Ensure cleanup happens even on error
            ImTui_ImplText_Shutdown();
            ImTui_ImplNcurses_Shutdown();
            ImGui::DestroyContext();
            return Result<void>(Error(
                ErrorCode::InternalError,
                std::string("UI loop error: ") + e.what()
            ));
        }
        
        spdlog::debug("Main UI loop ended, cleaning up");
        
        // Cleanup
        ImTui_ImplText_Shutdown();
        ImTui_ImplNcurses_Shutdown();
        ImGui::DestroyContext();
        
        return Result<void>();
        
    } catch (const std::exception& e) {
        spdlog::error("Browse command exception: {}", e.what());
        return Result<void>(Error(
            ErrorCode::InternalError,
            std::string("Browse command failed: ") + e.what()
        ));
    }
}

std::unique_ptr<ICommand> createBrowseCommand() {
    return std::make_unique<BrowseCommand>();
}

} // namespace yams::cli