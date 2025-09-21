#include <yams/cli/browse_command.h>
#include <yams/cli/tui/browse_commands.hpp>
#include <yams/cli/tui/browse_services.hpp>
#include <yams/cli/tui/browse_state.hpp>
#include <yams/cli/yams_cli.h>
#include <yams/version.hpp>

#include <spdlog/spdlog.h>

using namespace yams::cli::tui;

// ImTUI includes
#include <imgui.h>
#include <imtui/imtui-impl-ncurses.h>
#include <imtui/imtui-impl-text.h>
#include <imtui/imtui.h>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstddef>
#include <cstdlib> // For getenv
#include <cstring> // For strlen
#include <mutex>
#include <optional>
#include <thread>
#include <unistd.h> // For isatty, STDIN_FILENO, STDOUT_FILENO
#include <vector>

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
    std::atomic<bool> loadingComplete{false};
    std::atomic<int> loadProgress{0};
    std::atomic<int> totalDocuments{0};
    std::thread loadThread;
    std::mutex documentsMutex;
    std::string loadingMessage = "Loading documents...";

    // UI state tracking
    bool documentsDisplayed = false;
    std::chrono::steady_clock::time_point lastLoadUpdate;

    // ImTUI screen
    ImTui::TScreen* screen = nullptr;

    // Window sizes
    float columnWidths[3] = {0.2f, 0.4f, 0.4f}; // Collections, Documents, Preview

    // Exit flag
    bool shouldExit = false;

    Impl() : services(nullptr) { lastLoadUpdate = std::chrono::steady_clock::now(); }

    void UpdateLayoutMode() {
        // Get terminal dimensions from ImGui
        ImVec2 displaySize = ImGui::GetIO().DisplaySize;
        state.terminalWidth = static_cast<int>(displaySize.x);
        state.terminalHeight = static_cast<int>(displaySize.y);

        // Determine layout mode based on terminal width
        LayoutMode newMode;
        if (state.terminalWidth >= 100) {
            newMode = LayoutMode::MultiPane;
        } else if (state.terminalWidth >= 60) {
            newMode = LayoutMode::SinglePane;
        } else {
            newMode = LayoutMode::Compact;
        }

        // Log layout changes for debugging
        if (newMode != state.layoutMode) {
            spdlog::debug("TUI: Layout mode changed from {} to {} ({}x{})",
                          static_cast<int>(state.layoutMode), static_cast<int>(newMode),
                          state.terminalWidth, state.terminalHeight);
            state.layoutMode = newMode;
        }

        // Adjust documents per page based on screen height
        int availableLines = state.terminalHeight - 6; // Account for header, status, etc.
        if (state.layoutMode == LayoutMode::Compact) {
            state.documentsPerPage = std::max(10, availableLines - 2);
        } else {
            state.documentsPerPage = std::max(20, availableLines * 2);
        }
    }

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
                    spdlog::info("TUI: Successfully loaded {} documents", docs.size());
                } else {
                    loadingMessage = "Failed to load documents";
                    spdlog::warn("TUI: Failed to load documents from services");
                }

                loadingComplete = true;
                isLoading = false;
                spdlog::debug("Async document load complete, total docs: {}", docsResult.size());

            } catch (const std::exception& e) {
                spdlog::error("Error loading documents: {}", e.what());
                loadingMessage = "Error: " + std::string(e.what());
                loadingComplete = true;
                isLoading = false;
            }
        });
    }

    void RenderMainUI() {
        // Update layout mode based on terminal size
        UpdateLayoutMode();

        // Render viewer overlay if open
        if (viewerOpen) {
            RenderViewer();
            return;
        }

        // Render based on layout mode
        switch (state.layoutMode) {
            case LayoutMode::MultiPane:
                RenderMultiPaneLayout();
                break;
            case LayoutMode::SinglePane:
                RenderSinglePaneLayout();
                break;
            case LayoutMode::Compact:
                RenderCompactLayout();
                break;
        }
    }

    void RenderMultiPaneLayout() {
        // Retro header bar with ASCII styling
        if (ImGui::BeginMainMenuBar()) {
            // Clean header
            ImGui::Text("YAMS Browser %s", YAMS_VERSION_STRING);

            ImGui::Separator();

            // Show document count
            {
                std::lock_guard<std::mutex> lock(documentsMutex);
                if (isLoading) {
                    ImGui::Text("Loading... [%d/%d]", loadProgress.load(), totalDocuments.load());
                } else {
                    ImGui::Text("Documents: %zu", state.documents.size());
                }
            }

            // Show status message
            if (!state.statusMessage.empty()) {
                ImGui::Separator();
                ImGui::Text("%s", state.statusMessage.c_str());
            }

            // Terminal size indicator
            ImGui::SameLine();
            ImGui::TextDisabled("[%dx%d]", state.terminalWidth, state.terminalHeight);

            ImGui::EndMainMenuBar();
        }

        // Retro loading overlay with ASCII styling
        if (isLoading) {
            RenderRetroLoadingOverlay();
        }

        // Main content area - three columns
        // Use responsive positioning - leave small margin for terminal border
        float marginTop = 2.0f;
        float marginBottom = 2.0f;
        ImGui::SetNextWindowPos(ImVec2(0, marginTop));
        ImGui::SetNextWindowSize(ImVec2(ImGui::GetIO().DisplaySize.x,
                                        ImGui::GetIO().DisplaySize.y - marginTop - marginBottom));

        if (ImGui::Begin("Browse", nullptr,
                         ImGuiWindowFlags_NoTitleBar | ImGuiWindowFlags_NoResize |
                             ImGuiWindowFlags_NoMove)) {
            ImGui::Columns(3, "BrowseColumns");
            ImGui::SetColumnWidth(0, ImGui::GetWindowWidth() * columnWidths[0]);
            ImGui::SetColumnWidth(1, ImGui::GetWindowWidth() * columnWidths[1]);

            // Collections column with border
            RenderRetroCollections();
            ImGui::NextColumn();

            // Documents column with virtual scrolling
            RenderRetroDocuments();
            ImGui::NextColumn();

            // Preview column
            RenderRetroPreview();
        }
        ImGui::End();

        // Retro status bar
        RenderRetroStatusBar();

        // Help overlay with retro styling
        if (state.showHelp) {
            RenderRetroHelp();
        }
    }

    void RenderSinglePaneLayout() {
        // Single pane mode for medium screens
        RenderRetroHeader();

        if (isLoading) {
            RenderRetroLoadingOverlay();
        }

        // Main window
        // Use responsive positioning for single pane
        float marginTop = 2.0f;
        float marginBottom = 2.0f;
        ImGui::SetNextWindowPos(ImVec2(0, marginTop));
        ImGui::SetNextWindowSize(ImVec2(ImGui::GetIO().DisplaySize.x,
                                        ImGui::GetIO().DisplaySize.y - marginTop - marginBottom));

        if (ImGui::Begin("Browse", nullptr,
                         ImGuiWindowFlags_NoTitleBar | ImGuiWindowFlags_NoResize |
                             ImGuiWindowFlags_NoMove)) {
            // Navigation breadcrumb
            RenderBreadcrumb();

            // Content based on active pane
            switch (state.activeColumn) {
                case Column::Collections:
                    RenderRetroCollections();
                    break;
                case Column::Documents:
                    RenderRetroDocuments();
                    break;
                case Column::Preview:
                    RenderRetroPreview();
                    break;
            }
        }
        ImGui::End();

        RenderRetroStatusBar();

        if (state.showHelp) {
            RenderRetroHelp();
        }
    }

    void RenderCompactLayout() {
        // Compact mode for small screens
        RenderMinimalHeader();

        if (isLoading) {
            RenderMinimalLoadingIndicator();
        }

        // Simple document list
        // Use minimal margins for compact layout
        float margin = 1.0f;
        ImGui::SetNextWindowPos(ImVec2(0, margin));
        ImGui::SetNextWindowSize(
            ImVec2(ImGui::GetIO().DisplaySize.x, ImGui::GetIO().DisplaySize.y - 2 * margin));

        if (ImGui::Begin("Files", nullptr,
                         ImGuiWindowFlags_NoTitleBar | ImGuiWindowFlags_NoResize |
                             ImGuiWindowFlags_NoMove)) {
            RenderCompactDocumentList();
        }
        ImGui::End();

        RenderMinimalStatusBar();
    }

    // Retro-styled rendering functions
    void RenderRetroLoadingOverlay() {
        // Responsive loading overlay - use percentage of screen
        ImVec2 screenSize = ImGui::GetIO().DisplaySize;
        float overlayWidth = std::min(screenSize.x * 0.8f, 400.0f);
        float overlayHeight = std::min(screenSize.y * 0.3f, 120.0f);
        ImGui::SetNextWindowPos(ImVec2(screenSize.x * 0.5f, screenSize.y * 0.5f), ImGuiCond_Always,
                                ImVec2(0.5f, 0.5f));
        ImGui::SetNextWindowSize(ImVec2(overlayWidth, overlayHeight));

        if (ImGui::Begin("Loading", nullptr,
                         ImGuiWindowFlags_NoResize | ImGuiWindowFlags_NoMove |
                             ImGuiWindowFlags_NoCollapse)) {
            // Clean loading message
            ImGui::Text("Loading documents...");

            // Progress bar
            float progress = totalDocuments > 0 ? static_cast<float>(loadProgress) /
                                                      static_cast<float>(totalDocuments)
                                                : 0.0f;

            ImGui::ProgressBar(progress, ImVec2(-1, 0), "");
            ImGui::Text("Progress: %.1f%%", progress * 100.0f);

            if (totalDocuments > 0) {
                ImGui::Text("%d / %d documents", loadProgress.load(), totalDocuments.load());
            }

            ImGui::Separator();
            ImGui::Text("Press ESC to cancel");
        }
        ImGui::End();
    }

    void RenderRetroHeader() {
        if (ImGui::BeginMainMenuBar()) {
            ImGui::Text("YAMS %s", YAMS_VERSION_STRING);
            ImGui::Separator();

            {
                std::lock_guard<std::mutex> lock(documentsMutex);
                if (isLoading) {
                    ImGui::Text("Loading...");
                } else {
                    ImGui::Text("%zu docs", state.documents.size());
                }
            }

            ImGui::EndMainMenuBar();
        }
    }

    void RenderMinimalHeader() {
        if (ImGui::BeginMainMenuBar()) {
            ImGui::Text("YAMS %s", YAMS_VERSION_STRING);

            {
                std::lock_guard<std::mutex> lock(documentsMutex);
                ImGui::SameLine();
                if (isLoading) {
                    ImGui::Text(" [LOADING]");
                } else {
                    ImGui::Text(" [%zu]", state.documents.size());
                }
            }

            ImGui::EndMainMenuBar();
        }
    }

    void RenderBreadcrumb() {
        // Show current navigation context
        ImGui::Text("> ");
        ImGui::SameLine();

        switch (state.activeColumn) {
            case Column::Collections:
                ImGui::Text("[COLLECTIONS]");
                break;
            case Column::Documents:
                ImGui::Text("[DOCUMENTS]");
                if (state.selectedCollection > 0) {
                    ImGui::SameLine();
                    ImGui::TextDisabled(" > Collection #%d", state.selectedCollection);
                }
                break;
            case Column::Preview:
                ImGui::Text("[PREVIEW]");
                if (state.selected >= 0 &&
                    state.selected < static_cast<int>(state.documents.size())) {
                    ImGui::SameLine();
                    ImGui::TextDisabled(" > %.20s...",
                                        state.documents[state.selected].name.c_str());
                }
                break;
        }

        ImGui::Separator();
        ImGui::TextDisabled("Tab: Navigate | Enter: Select | ?: Help");
        ImGui::Separator();
    }

    void RenderMinimalLoadingIndicator() {
        if (isLoading) {
            ImGui::Text("Loading...");
        }
    }

    void RenderRetroCollections() {
        // Collections header
        bool isActive = (state.activeColumn == Column::Collections);

        if (isActive) {
            ImGui::Text("> COLLECTIONS <");
        } else {
            ImGui::Text("COLLECTIONS");
        }
        ImGui::Separator();

        // Show loading state in collections too
        if (isLoading) {
            ImGui::Text("Loading...");
            return;
        }

        // Get document count for display
        size_t totalDocs = 0;
        {
            std::lock_guard<std::mutex> lock(documentsMutex);
            totalDocs = state.allDocuments.size();
        }

        // All Documents collection
        bool allSelected = (state.selectedCollection == 0);
        if (ImGui::Selectable("All Documents", allSelected)) {
            state.selectedCollection = 0;
            std::lock_guard<std::mutex> lock(documentsMutex);
            state.documents = state.allDocuments;
            state.selected = 0;
            UpdatePreview();
        }
        ImGui::SameLine();
        ImGui::TextDisabled("(%zu)", totalDocs);

        // Smart collections based on actual document data
        if (totalDocs > 0) {
            // TODO: Implement smart collections
            ImGui::TextDisabled("Recent Files");
            ImGui::TextDisabled("Text Files");
            ImGui::TextDisabled("Large Files");
        }

        // Show empty state if no documents
        if (totalDocs == 0 && loadingComplete) {
            ImGui::Separator();
            ImGui::Text("No documents");
            ImGui::TextDisabled("to categorize");
        }
    }

    void RenderRetroDocuments() {
        // Documents header
        bool isActive = (state.activeColumn == Column::Documents);

        if (isActive) {
            ImGui::Text("> DOCUMENTS <");
        } else {
            ImGui::Text("DOCUMENTS");
        }
        ImGui::Separator();

        // Search input with retro styling
        if (state.searchMode) {
            static char searchBuf[256] = {};
            ImGui::Text("Search: ");
            ImGui::SameLine();
            if (ImGui::InputText("##search", searchBuf, sizeof(searchBuf))) {
                state.searchQuery = searchBuf;
                ApplySearch();
            }
        }

        // Document list with improved rendering
        ImGui::BeginChild("DocumentList");
        RenderDocumentListContent();
        ImGui::EndChild();
    }

    void RenderRetroPreview() {
        // Retro preview pane with active indicator
        bool isActive = (state.activeColumn == Column::Preview);
        ImVec4 borderColor =
            isActive ? ImVec4(1.0f, 1.0f, 0.0f, 1.0f) : ImVec4(0.0f, 1.0f, 0.0f, 1.0f);

        ImGui::TextColored(borderColor, "╔═══════════════════════╗");
        if (isActive) {
            ImGui::TextColored(borderColor, "║ ▶     PREVIEW     ◀   ║");
        } else {
            ImGui::TextColored(borderColor, "║       PREVIEW         ║");
        }
        ImGui::TextColored(borderColor, "╚═══════════════════════╝");

        if (state.selected >= 0 && state.selected < static_cast<int>(state.documents.size())) {
            const auto& doc = state.documents[state.selected];

            ImGui::Text("File: %s", doc.name.c_str());
            ImGui::Text("Hash: %.8s...", doc.hash.c_str());
            ImGui::Text("Size: %zu bytes", doc.size);
            ImGui::Text("Type: %s", doc.type.c_str());

            ImGui::Separator();

            // Preview content
            ImGui::BeginChild("PreviewContent");
            RenderPreviewContent();
            ImGui::EndChild();
        } else {
            ImGui::TextDisabled("Select a document to preview");
        }
    }

    void RenderRetroStatusBar() {
        // Responsive status bar - adaptive height
        float statusHeight = std::max(ImGui::GetTextLineHeightWithSpacing() + 4.0f, 16.0f);
        ImGui::SetNextWindowPos(ImVec2(0, ImGui::GetIO().DisplaySize.y - statusHeight));
        ImGui::SetNextWindowSize(ImVec2(ImGui::GetIO().DisplaySize.x, statusHeight));

        if (ImGui::Begin("StatusBar", nullptr,
                         ImGuiWindowFlags_NoTitleBar | ImGuiWindowFlags_NoResize |
                             ImGuiWindowFlags_NoMove | ImGuiWindowFlags_NoScrollbar)) {
            if (!state.statusMessage.empty()) {
                ImGui::Text("%s", state.statusMessage.c_str());
            } else {
                ImGui::Text("Ready");
                ImGui::SameLine();
                ImGui::TextDisabled(" | Docs: %zu", state.documents.size());
                ImGui::SameLine();
                ImGui::TextDisabled(" | Press ? for help");
            }
        }
        ImGui::End();
    }

    void RenderRetroHelp() {
        // Responsive help dialog
        ImVec2 screenSize = ImGui::GetIO().DisplaySize;
        float helpWidth = std::min(screenSize.x * 0.9f, 600.0f);
        float helpHeight = std::min(screenSize.y * 0.8f, 400.0f);
        ImGui::SetNextWindowPos(ImVec2(screenSize.x * 0.5f, screenSize.y * 0.5f), ImGuiCond_Always,
                                ImVec2(0.5f, 0.5f));
        ImGui::SetNextWindowSize(ImVec2(helpWidth, helpHeight));

        if (ImGui::Begin("Help", &state.showHelp)) {
            ImGui::Text("YAMS Browser - Help");
            ImGui::Separator();

            ImGui::Separator();
            ImGui::Text("Navigation:");
            ImGui::BulletText("j/k/↑/↓ - Move up/down");
            ImGui::BulletText("Tab - Next column/pane");
            ImGui::BulletText("Shift+Tab - Previous column/pane");

            ImGui::Separator();
            ImGui::Text("Actions:");
            ImGui::BulletText("Enter - View document (full screen)");
            ImGui::BulletText("/ - Search");
            ImGui::BulletText(": - Command mode");
            ImGui::BulletText("q - Quit");

            ImGui::Separator();
            if (ImGui::Button("Close")) {
                state.showHelp = false;
            }
        }
        ImGui::End();
    }

    void RenderCompactDocumentList() {
        // Simple list for very small screens
        std::lock_guard<std::mutex> lock(documentsMutex);

        if (state.documents.empty()) {
            ImGui::Text("No files");
            return;
        }

        for (size_t i = 0; i < state.documents.size(); ++i) {
            const auto& doc = state.documents[i];
            bool isSelected = (state.selected == static_cast<int>(i));

            if (isSelected) {
                ImGui::Text("> %s", doc.name.c_str());
            } else {
                if (ImGui::Selectable(doc.name.c_str(), false)) {
                    state.selected = static_cast<int>(i);
                    UpdatePreview();
                }
            }
        }
    }

    void RenderMinimalStatusBar() {
        // Minimal status bar for compact mode
        float statusHeight = std::max(ImGui::GetTextLineHeightWithSpacing(), 16.0f);
        ImGui::SetNextWindowPos(ImVec2(0, ImGui::GetIO().DisplaySize.y - statusHeight));
        ImGui::SetNextWindowSize(ImVec2(ImGui::GetIO().DisplaySize.x, statusHeight));

        if (ImGui::Begin("Status", nullptr,
                         ImGuiWindowFlags_NoTitleBar | ImGuiWindowFlags_NoResize |
                             ImGuiWindowFlags_NoMove | ImGuiWindowFlags_NoScrollbar)) {
            ImGui::Text("Ready");
        }
        ImGui::End();
    }

    void RenderDocumentListContent() {
        // Enhanced document list with virtual scrolling support
        bool hasDocuments = false;
        size_t documentCount = 0;

        {
            std::lock_guard<std::mutex> lock(documentsMutex);
            hasDocuments = !state.documents.empty();
            documentCount = state.documents.size();

            // Track if we've displayed documents for debugging
            if (hasDocuments && !documentsDisplayed) {
                documentsDisplayed = true;
                spdlog::info("TUI: Rendering {} documents for first time", documentCount);
                lastLoadUpdate = std::chrono::steady_clock::now();
            }
        }

        // Handle different UI states
        if (isLoading) {
            ImGui::Text("Loading documents...");
            if (totalDocuments > 0) {
                ImGui::Text("Progress: %d / %d", loadProgress.load(), totalDocuments.load());
            }
            ImGui::Text("Press ESC to cancel");
        } else if (!hasDocuments && loadingComplete) {
            ImGui::Text("No documents found");
            ImGui::TextDisabled("Database may be empty");
            ImGui::Separator();
            ImGui::Text("Tips:");
            ImGui::BulletText("Add files: yams store <file>");
            ImGui::BulletText("Check stats: yams stats");
        } else if (!hasDocuments) {
            ImGui::Text("No documents loaded");
            ImGui::TextDisabled("Loading may have failed");
        } else {
            // Render documents with retro styling
            std::lock_guard<std::mutex> lock(documentsMutex);
            for (size_t i = 0; i < state.documents.size(); ++i) {
                const auto& doc = state.documents[i];

                bool isSelected = (state.selected == static_cast<int>(i));
                if (isSelected) {
                    ImGui::Text(">");
                    ImGui::SameLine();
                    ImGui::Text("%s", doc.name.c_str());
                    ImGui::SameLine();
                    ImGui::TextDisabled(" (%zu bytes)", doc.size);
                } else {
                    if (ImGui::Selectable(doc.name.c_str(), false)) {
                        state.selected = static_cast<int>(i);
                        UpdatePreview();
                    }
                    ImGui::SameLine();
                    ImGui::TextDisabled(" (%zu bytes)", doc.size);
                }
            }
        }
    }

    void RenderPreviewContent() {
        // Enhanced preview rendering
        if (!state.previewLines.empty()) {
            // Calculate visible lines
            float lineHeight = ImGui::GetTextLineHeightWithSpacing();
            int visibleLines = static_cast<int>(ImGui::GetContentRegionAvail().y / lineHeight);

            // Render visible lines with retro styling
            for (int i = 0; i < visibleLines &&
                            (previewScrollOffset + i) < static_cast<int>(state.previewLines.size());
                 ++i) {
                ImGui::Text("%s", state.previewLines[previewScrollOffset + i].c_str());
            }
        }
    }

    void RenderCollections() {
        // Legacy function for compatibility
        ImGui::Text("Collections");
        ImGui::Separator();

        // Show loading state in collections too
        if (isLoading) {
            ImGui::TextDisabled("Loading...");
            return;
        }

        // Get document count for display
        size_t totalDocs = 0;
        {
            std::lock_guard<std::mutex> lock(documentsMutex);
            totalDocs = state.allDocuments.size();
        }

        // All Documents collection
        if (ImGui::Selectable("All Documents", state.selectedCollection == 0)) {
            state.selectedCollection = 0;
            // Reset document filter
            std::lock_guard<std::mutex> lock(documentsMutex);
            state.documents = state.allDocuments;
            state.selected = 0;
            UpdatePreview();
        }

        ImGui::SameLine();
        ImGui::TextDisabled("(%zu)", totalDocs);

        // Smart collections based on actual document data
        if (totalDocs > 0) {
            // Smart collections - simplified for now
            ImGui::Selectable("Recent Files", state.selectedCollection == 1);
            ImGui::Selectable("Text Files", state.selectedCollection == 2);
            ImGui::Selectable("Large Files", state.selectedCollection == 3);
        }

        // Show empty state if no documents
        if (totalDocs == 0 && loadingComplete) {
            ImGui::Separator();
            ImGui::TextDisabled("No documents");
            ImGui::TextDisabled("to categorize");
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

        // Check loading state and document availability
        bool hasDocuments = false;
        size_t documentCount = 0;

        {
            std::lock_guard<std::mutex> lock(documentsMutex);
            hasDocuments = !state.documents.empty();
            documentCount = state.documents.size();

            // Track if we've displayed documents for debugging
            if (hasDocuments && !documentsDisplayed) {
                documentsDisplayed = true;
                spdlog::info("TUI: Rendering {} documents for first time", documentCount);
                lastLoadUpdate = std::chrono::steady_clock::now();
            }
        }

        // Handle different UI states
        if (isLoading) {
            // Still loading - show progress
            ImGui::TextDisabled("Loading documents...");
            if (totalDocuments > 0) {
                ImGui::Text("Progress: %d / %d", loadProgress.load(), totalDocuments.load());
            }
            ImGui::Text("Press ESC to cancel");
        } else if (!hasDocuments && loadingComplete) {
            // Loading finished but no documents
            ImGui::TextDisabled("No documents found");
            ImGui::Text("Database may be empty or inaccessible");
            ImGui::Separator();
            ImGui::Text("Tips:");
            ImGui::BulletText("Add files with: yams store <file>");
            ImGui::BulletText("Check storage with: yams stats");
        } else if (!hasDocuments) {
            // Unknown state - loading may have failed
            ImGui::TextDisabled("No documents loaded");
            ImGui::Text("Loading may have failed");
        } else {
            // We have documents - render them with virtual scrolling
            std::lock_guard<std::mutex> lock(documentsMutex);
            RenderVirtualDocumentList();
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
                ImGui::Text(
                    "Lines %d-%d of %zu (j/k to scroll)", previewScrollOffset + 1,
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
            for (int i = 0; i < visibleLines &&
                            (previewScrollOffset + i) < static_cast<int>(state.previewLines.size());
                 ++i) {
                ImGui::TextUnformatted(state.previewLines[previewScrollOffset + i].c_str());
            }

            ImGui::EndChild();
        } else {
            ImGui::TextDisabled("Select a document to preview");
        }
    }

    void RenderStatusBar() {
        // Status bar with adaptive height
        float statusHeight = std::max(ImGui::GetTextLineHeightWithSpacing() + 4.0f, 18.0f);
        ImGui::SetNextWindowPos(ImVec2(0, ImGui::GetIO().DisplaySize.y - statusHeight));
        ImGui::SetNextWindowSize(ImVec2(ImGui::GetIO().DisplaySize.x, statusHeight));

        if (ImGui::Begin("StatusBar", nullptr,
                         ImGuiWindowFlags_NoTitleBar | ImGuiWindowFlags_NoResize |
                             ImGuiWindowFlags_NoMove | ImGuiWindowFlags_NoScrollbar)) {
            if (!state.statusMessage.empty()) {
                ImGui::Text("%s", state.statusMessage.c_str());
            } else {
                ImGui::Text("Ready | Documents: %zu | Press ? for help", state.documents.size());
            }
        }
        ImGui::End();
    }

    void RenderHelp() {
        // Responsive help dialog
        ImVec2 screenSize = ImGui::GetIO().DisplaySize;
        float helpWidth = std::min(screenSize.x * 0.9f, 600.0f);
        float helpHeight = std::min(screenSize.y * 0.8f, 400.0f);
        ImGui::SetNextWindowPos(ImVec2(screenSize.x * 0.5f, screenSize.y * 0.5f), ImGuiCond_Always,
                                ImVec2(0.5f, 0.5f));
        ImGui::SetNextWindowSize(ImVec2(helpWidth, helpHeight));

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

        if (ImGui::Begin("Document Viewer", &viewerOpen,
                         ImGuiWindowFlags_NoTitleBar | ImGuiWindowFlags_NoResize |
                             ImGuiWindowFlags_NoMove | ImGuiWindowFlags_NoCollapse)) {
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
            // Responsive viewer content - leave space for controls
            float controlsHeight = ImGui::GetTextLineHeightWithSpacing() + 8.0f;
            ImGui::BeginChild("ViewerContent",
                              ImVec2(0, ImGui::GetContentRegionAvail().y - controlsHeight));

            // Calculate visible lines
            float lineHeight = ImGui::GetTextLineHeightWithSpacing();
            int visibleLines = static_cast<int>(ImGui::GetContentRegionAvail().y / lineHeight);

            // Render visible lines
            for (int i = 0; i < visibleLines &&
                            (viewerScrollOffset + i) < static_cast<int>(viewerLines.size());
                 ++i) {
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

        // Handle layout-aware input
        HandleLayoutAwareInput();
    }

    void HandleLayoutAwareInput() {
        ImGuiIO& io = ImGui::GetIO();

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
            } else if (state.layoutMode == LayoutMode::SinglePane &&
                       state.activeColumn != Column::Documents) {
                // In single pane, ESC goes back to documents view
                state.activeColumn = Column::Documents;
                spdlog::debug("TUI: ESC - returning to Documents view in single pane mode");
            }
        }

        // Help
        if (ImGui::IsKeyPressed('?')) {
            state.showHelp = !state.showHelp;
        }

        // Global commands available in all modes
        if (!state.commandMode && !state.searchMode) {
            // Search mode
            if (ImGui::IsKeyPressed('/')) {
                state.searchMode = true;
                spdlog::debug("TUI: Entering search mode");
            }

            // Command mode
            if (ImGui::IsKeyPressed(':')) {
                state.commandMode = true;
                state.commandBuffer.clear();
                spdlog::debug("TUI: Entering command mode");
            }

            // Handle navigation based on layout mode
            switch (state.layoutMode) {
                case LayoutMode::MultiPane:
                    HandleMultiPaneNavigation();
                    break;
                case LayoutMode::SinglePane:
                    HandleSinglePaneNavigation();
                    break;
                case LayoutMode::Compact:
                    HandleCompactNavigation();
                    break;
            }
        }

        // Handle command mode input
        if (state.commandMode) {
            static char cmdBuf[256] = {};
            if (ImGui::InputText(":", cmdBuf, sizeof(cmdBuf),
                                 ImGuiInputTextFlags_EnterReturnsTrue)) {
                state.commandBuffer = cmdBuf;
                ExecuteCommand();
                state.commandMode = false;
                cmdBuf[0] = '\0';
            }
        }

        // Quit command - available in all modes except command/search
        if (ImGui::IsKeyPressed('q', false) && !state.commandMode && !state.searchMode) {
            shouldExit = true;
            spdlog::debug("TUI: Quit requested");
        }
    }

    void HandleMultiPaneNavigation() {
        ImGuiIO& io = ImGui::GetIO();

        // Tab navigation between columns
        if (ImGui::IsKeyPressed(ImGuiKey_Tab)) {
            if (io.KeyShift) {
                state.activeColumn =
                    static_cast<Column>((static_cast<int>(state.activeColumn) + 2) % 3);
            } else {
                state.activeColumn =
                    static_cast<Column>((static_cast<int>(state.activeColumn) + 1) % 3);
            }
            spdlog::debug("TUI: Multi-pane tab to column {}", static_cast<int>(state.activeColumn));
        }

        // Vim-style horizontal navigation
        if (ImGui::IsKeyPressed('h')) {
            // Move left (previous column)
            state.activeColumn =
                static_cast<Column>((static_cast<int>(state.activeColumn) + 2) % 3);
            spdlog::debug("TUI: Vim 'h' - moved to column {}",
                          static_cast<int>(state.activeColumn));
        } else if (ImGui::IsKeyPressed('l')) {
            // Move right (next column)
            state.activeColumn =
                static_cast<Column>((static_cast<int>(state.activeColumn) + 1) % 3);
            spdlog::debug("TUI: Vim 'l' - moved to column {}",
                          static_cast<int>(state.activeColumn));
        }

        // Vertical navigation within active column
        HandleVerticalNavigation();

        // Enter - Open viewer or select item
        if (ImGui::IsKeyPressed(ImGuiKey_Enter)) {
            if (state.activeColumn == Column::Documents || state.activeColumn == Column::Preview) {
                OpenViewer();
            } else if (state.activeColumn == Column::Collections) {
                // Activate selected collection
                // TODO: Implement collection activation
            }
        }

        // o - Open in external pager
        if (ImGui::IsKeyPressed('o')) {
            OpenInExternalPager();
        }
    }

    void HandleSinglePaneNavigation() {
        ImGuiIO& io = ImGui::GetIO();

        // Tab navigation between views (full-screen switching)
        if (ImGui::IsKeyPressed(ImGuiKey_Tab)) {
            if (io.KeyShift) {
                state.activeColumn =
                    static_cast<Column>((static_cast<int>(state.activeColumn) + 2) % 3);
            } else {
                state.activeColumn =
                    static_cast<Column>((static_cast<int>(state.activeColumn) + 1) % 3);
            }
            spdlog::debug("TUI: Single-pane tab to view {}", static_cast<int>(state.activeColumn));
        }

        // Vertical navigation within current view
        HandleVerticalNavigation();

        // Enter - Context-dependent action
        if (ImGui::IsKeyPressed(ImGuiKey_Enter)) {
            switch (state.activeColumn) {
                case Column::Collections:
                    // Switch to documents view
                    state.activeColumn = Column::Documents;
                    break;
                case Column::Documents:
                    // Open viewer or switch to preview
                    if (state.documents.empty()) {
                        state.statusMessage = "No documents to preview";
                    } else {
                        state.activeColumn = Column::Preview;
                    }
                    break;
                case Column::Preview:
                    // Open full viewer
                    OpenViewer();
                    break;
            }
        }

        // o - Open in external pager
        if (ImGui::IsKeyPressed('o')) {
            OpenInExternalPager();
        }
    }

    void HandleCompactNavigation() {
        // Only document navigation in compact mode
        HandleVerticalNavigation();

        // Enter - Open viewer
        if (ImGui::IsKeyPressed(ImGuiKey_Enter)) {
            OpenViewer();
        }

        // o - Open in external pager
        if (ImGui::IsKeyPressed('o')) {
            OpenInExternalPager();
        }
    }

    void HandleVerticalNavigation() {
        bool navigationHandled = false;

        // Vim-style and arrow key navigation
        if (ImGui::IsKeyPressed('j') || ImGui::IsKeyPressed(ImGuiKey_DownArrow)) {
            HandleNavigateDown();
            navigationHandled = true;
        } else if (ImGui::IsKeyPressed('k') || ImGui::IsKeyPressed(ImGuiKey_UpArrow)) {
            HandleNavigateUp();
            navigationHandled = true;
        } else if (ImGui::IsKeyPressed(ImGuiKey_LeftArrow) || ImGui::IsKeyPressed('h')) {
            // Navigate to previous column (left)
            if (state.activeColumn == Column::Documents) {
                state.activeColumn = Column::Collections;
            } else if (state.activeColumn == Column::Preview) {
                state.activeColumn = Column::Documents;
            }
            navigationHandled = true;
        } else if (ImGui::IsKeyPressed(ImGuiKey_RightArrow) || ImGui::IsKeyPressed('l')) {
            // Navigate to next column (right)
            if (state.activeColumn == Column::Collections) {
                state.activeColumn = Column::Documents;
            } else if (state.activeColumn == Column::Documents) {
                state.activeColumn = Column::Preview;
            }
            navigationHandled = true;
        }

        // Page navigation
        if (ImGui::IsKeyPressed(ImGuiKey_PageDown) ||
            (ImGui::IsKeyPressed('d') && ImGui::GetIO().KeyCtrl)) {
            HandlePageDown();
            navigationHandled = true;
        } else if (ImGui::IsKeyPressed(ImGuiKey_PageUp) ||
                   (ImGui::IsKeyPressed('u') && ImGui::GetIO().KeyCtrl)) {
            HandlePageUp();
            navigationHandled = true;
        }

        // Jump to top/bottom
        if (ImGui::IsKeyPressed('g')) {
            static bool firstG = false;
            static auto lastGTime = std::chrono::steady_clock::now();
            auto now = std::chrono::steady_clock::now();

            if (firstG && (now - lastGTime) < std::chrono::milliseconds(500)) {
                // Double 'g' - go to top
                HandleJumpToTop();
                firstG = false;
                navigationHandled = true;
            } else {
                firstG = true;
                lastGTime = now;
            }
        } else if (ImGui::IsKeyPressed('G')) {
            // Go to bottom
            HandleJumpToBottom();
            navigationHandled = true;
        } else {
            // Reset double-g state if any other key is pressed
            static bool firstG = false;
            firstG = false;
        }

        if (navigationHandled) {
            UpdatePreview();
        }
    }

    void HandleNavigateDown() {
        if (state.activeColumn == Column::Preview) {
            // Scroll preview content down
            int maxScroll = std::max(0, static_cast<int>(state.previewLines.size()) - 10);
            previewScrollOffset = std::min(previewScrollOffset + 1, maxScroll);
        } else if (state.activeColumn == Column::Documents) {
            // Navigate to next document with virtual scrolling support
            if (!state.documents.empty()) {
                state.selected =
                    std::min(state.selected + 1, static_cast<int>(state.documents.size()) - 1);
                // Virtual scrolling will auto-adjust viewport in RenderVirtualDocumentList
            }
        } else if (state.activeColumn == Column::Collections) {
            // Navigate collections (TODO: implement when collections are enhanced)
            state.selectedCollection = std::min(state.selectedCollection + 1, 4); // Max 4 for now
        }
    }

    void HandleNavigateUp() {
        if (state.activeColumn == Column::Preview) {
            // Scroll preview content up
            previewScrollOffset = std::max(previewScrollOffset - 1, 0);
        } else if (state.activeColumn == Column::Documents) {
            // Navigate to previous document
            state.selected = std::max(state.selected - 1, 0);
        } else if (state.activeColumn == Column::Collections) {
            // Navigate collections
            state.selectedCollection = std::max(state.selectedCollection - 1, 0);
        }
    }

    void HandlePageDown() {
        if (state.activeColumn == Column::Preview) {
            int maxScroll = std::max(0, static_cast<int>(state.previewLines.size()) - 10);
            previewScrollOffset = std::min(previewScrollOffset + 10, maxScroll);
        } else if (state.activeColumn == Column::Documents) {
            // Jump down by page size - ImGui will handle the scrolling
            if (!state.documents.empty()) {
                int pageSize = 10; // Simple page size
                state.selected = std::min(state.selected + pageSize,
                                          static_cast<int>(state.documents.size()) - 1);
            }
        }
    }

    void HandlePageUp() {
        if (state.activeColumn == Column::Preview) {
            previewScrollOffset = std::max(previewScrollOffset - 10, 0);
        } else if (state.activeColumn == Column::Documents) {
            // Jump up by page size - ImGui will handle the scrolling
            if (!state.documents.empty()) {
                int pageSize = 10; // Simple page size
                state.selected = std::max(state.selected - pageSize, 0);
            }
        }
    }

    void HandleJumpToTop() {
        if (state.activeColumn == Column::Preview) {
            previewScrollOffset = 0;
        } else if (state.activeColumn == Column::Documents) {
            state.selected = 0;
        } else if (state.activeColumn == Column::Collections) {
            state.selectedCollection = 0;
        }
        spdlog::debug("TUI: Jump to top in column {}", static_cast<int>(state.activeColumn));
    }

    void HandleJumpToBottom() {
        if (state.activeColumn == Column::Preview) {
            int maxScroll = std::max(0, static_cast<int>(state.previewLines.size()) - 10);
            previewScrollOffset = maxScroll;
        } else if (state.activeColumn == Column::Documents) {
            if (!state.documents.empty()) {
                state.selected = static_cast<int>(state.documents.size()) - 1;
            }
        } else if (state.activeColumn == Column::Collections) {
            state.selectedCollection = 4; // Max collections for now
        }
        spdlog::debug("TUI: Jump to bottom in column {}", static_cast<int>(state.activeColumn));
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

        actions.refresh = [this](tui::BrowseState&) { RefreshDocuments(); };

        actions.showHelp = [](tui::BrowseState& s) { s.showHelp = true; };

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

    void RenderVirtualDocumentList() {
        int totalItems = static_cast<int>(state.documents.size());
        if (totalItems == 0) {
            ImGui::TextDisabled("No documents to display");
            return;
        }

        // Determine paging window
        int perPage = std::max(1, state.documentsPerPage);
        int maxPage = (totalItems - 1) / perPage;
        if (state.currentPage < 0)
            state.currentPage = 0;
        if (state.currentPage > maxPage)
            state.currentPage = maxPage;
        int startIndex = state.currentPage * perPage;
        int endIndex = std::min(startIndex + perPage, totalItems);
        state.hasMoreDocuments = (endIndex < totalItems);

        // Header with count and page controls
        ImGui::Text("Documents: %d  ", totalItems);
        ImGui::SameLine();
        ImGui::TextDisabled(" Page %d / %d ", state.currentPage + 1, maxPage + 1);
        ImGui::SameLine();
        bool prevDisabled = (state.currentPage == 0);
        if (prevDisabled)
            ImGui::BeginDisabled();
        if (ImGui::Button("◀ Prev")) {
            state.currentPage = std::max(0, state.currentPage - 1);
        }
        if (prevDisabled)
            ImGui::EndDisabled();
        ImGui::SameLine();
        bool nextDisabled = (state.currentPage >= maxPage);
        if (nextDisabled)
            ImGui::BeginDisabled();
        if (ImGui::Button("Next ▶")) {
            state.currentPage = std::min(maxPage, state.currentPage + 1);
        }
        if (nextDisabled)
            ImGui::EndDisabled();

        ImGui::Separator();

        // Scrollable region for the current page
        ImGui::BeginChild("DocumentList", ImVec2(0, 0), false,
                          ImGuiWindowFlags_AlwaysVerticalScrollbar);

        for (int i = startIndex; i < endIndex; ++i) {
            const auto& doc = state.documents[i];

            // Check if this item should be selected
            bool isSelected = (state.selected == i);

            // Render the document item
            std::string itemLabel = doc.name + "##" + std::to_string(i); // Unique ID
            if (ImGui::Selectable(itemLabel.c_str(), isSelected,
                                  ImGuiSelectableFlags_SpanAllColumns)) {
                state.selected = i;
                UpdatePreview();
            }

            if (isSelected) {
                ImGui::SetItemDefaultFocus();
                // Auto-scroll to keep selected item visible
                ImGui::SetScrollHereY(0.2f);
            }

            // Show metadata on the same line - use available width
            float availableWidth = ImGui::GetContentRegionAvail().x;
            if (availableWidth > 150) {
                ImGui::SameLine(ImGui::GetCursorPosX() + availableWidth - 150);

                // Format size
                std::string sizeStr;
                if (doc.size > 1024 * 1024) {
                    sizeStr = std::to_string(doc.size / (1024 * 1024)) + "MB";
                } else if (doc.size > 1024) {
                    sizeStr = std::to_string(doc.size / 1024) + "KB";
                } else {
                    sizeStr = std::to_string(doc.size) + "B";
                }

                ImGui::TextDisabled("%s", sizeStr.c_str());

                // Show hash preview if there's space
                if (availableWidth > 220) {
                    ImGui::SameLine();
                    ImGui::TextDisabled("%.6s", doc.hash.c_str());
                }
            }
        }

        ImGui::EndChild();
    }

    void UpdatePreview() {
        if (state.selected >= 0 && state.selected < static_cast<int>(state.documents.size())) {
            const auto& doc = state.documents[state.selected];

            // Reset scroll when changing documents
            previewScrollOffset = 0;

            // Load preview content - load more lines for scrolling
            auto contentResult = services.loadTextContent(doc.id);
            if (contentResult.has_value()) {
                state.previewLines = services.splitLines(contentResult.value(),
                                                         1000 // Load up to 1000 lines for preview
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

    void FilterByRecent() {
        std::lock_guard<std::mutex> lock(documentsMutex);
        state.documents.clear();

        auto now = std::chrono::system_clock::now();
        auto dayAgo = now - std::chrono::hours(24);

        for (const auto& doc : state.allDocuments) {
            if (doc.createdAt >= dayAgo) {
                state.documents.push_back(doc);
            }
        }

        state.selected = 0;
        UpdatePreview();
    }

    void FilterByType(const std::string& typeFilter) {
        std::lock_guard<std::mutex> lock(documentsMutex);
        state.documents.clear();

        for (const auto& doc : state.allDocuments) {
            if (doc.type.find(typeFilter) != std::string::npos) {
                state.documents.push_back(doc);
            }
        }

        state.selected = 0;
        UpdatePreview();
    }

    void FilterBySize(size_t minSize) {
        std::lock_guard<std::mutex> lock(documentsMutex);
        state.documents.clear();

        for (const auto& doc : state.allDocuments) {
            if (doc.size >= minSize) {
                state.documents.push_back(doc);
            }
        }

        state.selected = 0;
        UpdatePreview();
    }

    void RefreshDocuments() {
        spdlog::debug("TUI: Refreshing documents");

        isLoading = true;
        loadingComplete = false;
        loadProgress = 0;
        totalDocuments = 0;
        documentsDisplayed = false;
        loadingMessage = "Refreshing documents...";

        {
            std::lock_guard<std::mutex> lock(documentsMutex);
            state.documents.clear();
            state.allDocuments.clear();
            state.selected = 0;
        }

        if (loadThread.joinable()) {
            loadThread.join();
        }

        LoadDocumentsAsync(cli);
    }

    void Run() {
        // Main ImTUI loop
        int frameCount = 0;
        auto startTime = std::chrono::steady_clock::now();
        spdlog::debug("Starting main UI loop");

        while (!shouldExit) {
            frameCount++;

            // Enhanced debug logging every 60 frames or on state changes
            if (frameCount == 1 || frameCount % 60 == 0) {
                size_t docCount = 0;
                {
                    std::lock_guard<std::mutex> lock(documentsMutex);
                    docCount = state.documents.size();
                }
                spdlog::debug("Frame {}, loading: {}, complete: {}, docs: {}", frameCount,
                              isLoading.load(), loadingComplete.load(), docCount);
            }

            // Log significant state changes
            if (loadingComplete && !documentsDisplayed) {
                auto elapsed = std::chrono::steady_clock::now() - startTime;
                auto elapsedMs =
                    std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
                spdlog::info("TUI: Loading completed after {}ms, frame {}", elapsedMs, frameCount);
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
            return Result<void>(Error(ErrorCode::InvalidOperation, message));
        }

        // Additional terminal environment check
        const char* term = getenv("TERM");
        if (!term || strlen(term) == 0) {
            spdlog::info("TERM environment variable not set. Please run in a proper terminal.");
            return Result<void>(
                Error(ErrorCode::InvalidOperation, "TERM environment variable not set"));
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
                "Failed to initialize terminal UI. Please ensure your terminal supports ncurses."));
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
            return Result<void>(Error(ErrorCode::InternalError, "CLI context not initialized"));
        }

        // Initialize storage before starting async operations
        spdlog::debug("Ensuring storage is initialized");
        auto storageResult = pImpl->cli->ensureStorageInitialized();
        if (!storageResult) {
            spdlog::error("Failed to initialize storage: {}", storageResult.error().message);
            ImTui_ImplText_Shutdown();
            ImTui_ImplNcurses_Shutdown();
            ImGui::DestroyContext();
            return Result<void>(Error(ErrorCode::InternalError, "Failed to initialize storage: " +
                                                                    storageResult.error().message));
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
            return Result<void>(
                Error(ErrorCode::InternalError, std::string("UI loop error: ") + e.what()));
        }

        spdlog::debug("Main UI loop ended, cleaning up");

        // Cleanup
        ImTui_ImplText_Shutdown();
        ImTui_ImplNcurses_Shutdown();
        ImGui::DestroyContext();

        return Result<void>();

    } catch (const std::exception& e) {
        spdlog::error("Browse command exception: {}", e.what());
        return Result<void>(
            Error(ErrorCode::InternalError, std::string("Browse command failed: ") + e.what()));
    }
}

std::unique_ptr<ICommand> createBrowseCommand() {
    return std::make_unique<BrowseCommand>();
}

} // namespace yams::cli
