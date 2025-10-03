#include <yams/cli/browse_command.h>
#include <yams/cli/tui/browse_commands.hpp>
#include <yams/cli/tui/browse_services.hpp>
#include <yams/cli/tui/browse_state.hpp>
#include <yams/cli/tui/highlight_utils.hpp>
#include <yams/cli/tui/tui_services.hpp>
#include <yams/cli/yams_cli.h>
#include <yams/version.hpp>

#include <ftxui/component/component.hpp>
#include <ftxui/component/component_base.hpp>
#include <ftxui/component/event.hpp>
#include <ftxui/component/screen_interactive.hpp>
#include <ftxui/dom/elements.hpp>
#include <ftxui/screen/string.hpp>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <future>
#include <iomanip>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>
#include <sys/ioctl.h>

namespace yams::cli {
namespace {
std::string humanReadableBytes(std::size_t bytes) {
    constexpr std::size_t kUnit = 1024;
    if (bytes < kUnit) {
        return std::to_string(bytes) + " B";
    }
    static const char* suffixes[] = {"KB", "MB", "GB", "TB"};
    double count = static_cast<double>(bytes);
    int index = 0;
    while (count >= kUnit && index < 4) {
        count /= static_cast<double>(kUnit);
        ++index;
    }
    std::ostringstream oss;
    oss.setf(std::ios::fixed);
    oss.precision(count < 10.0 ? 1 : 0);
    oss << count << ' ' << suffixes[index];
    return oss.str();
}

std::string truncateMiddle(std::string_view value, std::size_t max) {
    if (value.size() <= max) {
        return std::string(value);
    }
    if (max <= 3) {
        return std::string(max, '.');
    }
    const std::size_t head = (max - 3) / 2;
    const std::size_t tail = max - 3 - head;
    return std::string(value.substr(0, head)) + "..." +
           std::string(value.substr(value.size() - tail));
}

std::wstring toWide(const std::string& text);

std::string buildProgressBar(int percent, int width = 20) {
    width = std::max(1, width);
    percent = std::clamp(percent, 0, 100);
    const int filled = percent * width / 100;
    std::string bar(width, '-');
    for (int i = 0; i < filled && i < width; ++i) {
        bar[i] = '#';
    }
    std::ostringstream oss;
    oss << '[' << bar << ']';
    return oss.str();
}

std::string formatPreviewProgressLabel(int percent, std::size_t producedLines,
                                       std::size_t totalLines, std::size_t producedBytes,
                                       std::size_t totalBytes, std::size_t docSize) {
    std::ostringstream oss;
    percent = std::clamp(percent, 0, 100);
    oss << std::setw(3) << percent << "%";

    if (producedBytes > 0 || docSize > 0 || totalBytes > 0) {
        oss << " | " << humanReadableBytes(producedBytes);
        std::string totalLabel;
        if (docSize > 0) {
            totalLabel = humanReadableBytes(docSize);
        } else if (totalBytes > 0) {
            totalLabel = humanReadableBytes(totalBytes);
        }
        if (!totalLabel.empty()) {
            oss << " / " << totalLabel;
        }
    }

    if (totalLines > 0) {
        oss << " | " << producedLines << '/' << totalLines << " lines";
    }

    return oss.str();
}

inline void ltrim(std::string& s) {
    s.erase(s.begin(),
            std::find_if(s.begin(), s.end(), [](unsigned char ch) { return !std::isspace(ch); }));
}

inline void rtrim(std::string& s) {
    s.erase(std::find_if(s.rbegin(), s.rend(), [](unsigned char ch) { return !std::isspace(ch); })
                .base(),
            s.end());
}

inline void trim(std::string& s) {
    ltrim(s);
    rtrim(s);
}

inline std::string toLower(std::string_view sv) {
    std::string out;
    out.reserve(sv.size());
    for (char c : sv) {
        out.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
    }
    return out;
}

inline bool containsCi(std::string_view haystack, std::string_view needle) {
    if (needle.empty()) {
        return true;
    }
    auto it = std::search(haystack.begin(), haystack.end(), needle.begin(), needle.end(),
                          [](char a, char b) {
                              return std::tolower(static_cast<unsigned char>(a)) ==
                                     std::tolower(static_cast<unsigned char>(b));
                          });
    return it != haystack.end();
}

ftxui::Element renderHighlighted(const std::string& text, std::string_view query) {
    auto segments = tui::computeHighlightSegments(text, query);
    if (segments.size() == 1 && !segments.front().highlighted) {
        return ftxui::text(toWide(text));
    }

    ftxui::Elements parts;
    parts.reserve(segments.size());
    for (const auto& seg : segments) {
        auto element = ftxui::text(ftxui::to_wstring(seg.text));
        if (seg.highlighted) {
            element = element | ftxui::bgcolor(ftxui::Color::Yellow) | ftxui::bold;
        }
        parts.push_back(std::move(element));
    }
    return ftxui::hbox(std::move(parts));
}

std::string formatPreviewMode(tui::PreviewMode mode) {
    switch (mode) {
        case tui::PreviewMode::Auto:
            return "auto";
        case tui::PreviewMode::Text:
            return "text";
        case tui::PreviewMode::Hex:
            return "hex";
    }
    return "auto";
}

std::string formatLayoutMode(tui::LayoutMode mode) {
    switch (mode) {
        case tui::LayoutMode::MultiPane:
            return "multi";
        case tui::LayoutMode::SinglePane:
            return "single";
        case tui::LayoutMode::Compact:
            return "compact";
    }
    return "multi";
}

std::wstring toWide(const std::string& text) {
    return ftxui::to_wstring(text);
}
} // namespace

struct PreviewJob {
    std::future<void> worker;
    std::shared_ptr<std::vector<std::string>> lines;
    std::shared_ptr<std::mutex> mutex;
    std::shared_ptr<std::atomic<size_t>> producedLines;
    std::shared_ptr<std::atomic<size_t>> totalLines;
    std::shared_ptr<std::atomic<size_t>> producedBytes;
    std::shared_ptr<std::atomic<size_t>> totalBytes;
    std::shared_ptr<std::atomic<bool>> done;
};

class BrowseCommand::Impl {
public:
    Impl() : services(&serviceAdapter) {}

    void setCli(YamsCLI* cliPtr) {
        cli = cliPtr;
        serviceAdapter.attach(cli);
        services.attach(&serviceAdapter);
    }

    Result<void> run();

private:
    YamsCLI* cli = nullptr;
    tui::BrowseState state;
    tui::TUIServices serviceAdapter;
    tui::BrowseServices services;
    tui::BrowseCommands commandParser;
    ftxui::ScreenInteractive* screen = nullptr;

    bool searchDirty = false;
    std::chrono::steady_clock::time_point lastSearchEdit;
    std::string lastAppliedQuery;

    void updateLayout(int width, int height);
    void applySearchFilter();
    void applyFilters(std::vector<tui::DocEntry>& docs);
    void ensureSelectionValid();
    void loadPreviewForSelection();
    void refreshDocuments(bool preserveSelection);
    void pollAsyncTasks();
    bool executeCustomCommand(const std::string& command);
    Result<std::size_t> deleteSelectedDocument(bool force);
    Result<std::size_t> addDocumentFromPath(const std::string& path);
    Result<std::size_t> updateDocumentTags(const std::vector<std::string>& add,
                                           const std::vector<std::string>& remove,
                                           bool clearExisting);
    Result<void> performReindex();
    std::vector<std::string> currentTagsForDoc(const tui::DocEntry& doc) const;
    bool parseTagArguments(const std::string& input, std::vector<std::string>& add,
                           std::vector<std::string>& remove, bool& clear, std::string& error) const;
    void markSearchDirty();
    void toggleViewer();
    void loadViewerContent(bool forceReload);
    bool handleViewerEvent(const ftxui::Event& event);
    ftxui::Element buildViewerOverlay(const ftxui::Element& base) const;
    ftxui::Element buildModalOverlay(const ftxui::Element& base) const;
    bool handleModalEvent(const ftxui::Event& event);
    void openAddDocumentDialog();
    void openTagEditDialog();
    void openReindexDialog();
    void closeModal();
    Result<void> ensureTerminalReady();
    bool selectDocumentByHash(const std::string& hash);
    bool selectDocumentByPath(const std::string& path);

    bool handleSearchEvent(const ftxui::Event& event);
    bool handleCommandEvent(const ftxui::Event& event,
                            const std::function<void()>& exitLoopClosure);
    bool handleGeneralEvent(const ftxui::Event& event,
                            const std::function<void()>& exitLoopClosure);

    bool openSelectedInPager();
    void setStatus(std::string message,
                   tui::BrowseState::Status::Level level = tui::BrowseState::Status::Info,
                   std::chrono::seconds duration = std::chrono::seconds(5));

    ftxui::Element buildDocumentPane(bool focused, int maxHeight) const;
    ftxui::Element buildPreviewPane(bool focused, int maxHeight) const;
    ftxui::Element buildStatusBar() const;
    ftxui::Element buildHelpOverlay(const ftxui::Element& base) const;

    std::shared_ptr<PreviewJob> previewJob;
    size_t previewPublishedLines = 0;
    std::chrono::steady_clock::time_point lastPreviewTick;
    std::size_t previewSpinnerPhase = 0;
    std::size_t previewDocSize = 0;
    int previewPercent = 0;
};

Result<void> BrowseCommand::Impl::ensureTerminalReady() {
    const char* termEnv = std::getenv("TERM");
    if (!termEnv || std::string_view(termEnv).empty() || std::string_view(termEnv) == "dumb") {
        return Result<void>(
            Error(ErrorCode::InvalidOperation, "Current terminal does not expose capabilities "
                                               "required for the TUI (TERM is unset or 'dumb')."));
    }

    struct winsize ws{};
    if (ioctl(STDOUT_FILENO, TIOCGWINSZ, &ws) == -1 || ws.ws_col == 0 || ws.ws_row == 0) {
        return Result<void>(Error(ErrorCode::InvalidOperation,
                                  "Unable to detect terminal dimensions; ensure you are running "
                                  "inside an interactive terminal window."));
    }

    static constexpr int kMinWidth = 60;
    static constexpr int kMinHeight = 18;
    if (ws.ws_col < kMinWidth || ws.ws_row < kMinHeight) {
        std::ostringstream oss;
        oss << "Terminal too small for the interactive browser (minimum " << kMinWidth << "x"
            << kMinHeight << ", detected " << ws.ws_col << "x" << ws.ws_row
            << "). Resize the window and retry.";
        return Result<void>(Error(ErrorCode::InvalidOperation, oss.str()));
    }

    state.terminalWidth = ws.ws_col;
    state.terminalHeight = ws.ws_row;
    updateLayout(state.terminalWidth, state.terminalHeight);
    return Result<void>();
}

Result<void> BrowseCommand::Impl::run() {
    if (!cli) {
        return Result<void>(Error(ErrorCode::InvalidOperation, "CLI context not initialized"));
    }

    auto storageResult = cli->ensureStorageInitialized();
    if (!storageResult) {
        return Result<void>(storageResult.error());
    }

    auto terminalReady = ensureTerminalReady();
    if (!terminalReady) {
        return terminalReady;
    }

    refreshDocuments(false);
    if (state.documents.empty()) {
        setStatus("No documents available", tui::BrowseState::Status::Warning,
                  std::chrono::seconds(3));
    }

    ftxui::ScreenInteractive interactive = ftxui::ScreenInteractive::TerminalOutput();
    screen = &interactive;
    auto exitLoop = interactive.ExitLoopClosure();

    auto renderer = ftxui::Renderer([&] {
        pollAsyncTasks();
        updateLayout(interactive.dimx(), interactive.dimy());

        const int contentHeight = std::max(5, state.terminalHeight - 6);
        const bool documentsFocused = state.activeColumn == tui::Column::Documents;
        const bool previewFocused = state.activeColumn == tui::Column::Preview;

        std::string filterLabel;
        if (!state.filterExt.empty() || !state.filterMime.empty() || !state.filterTag.empty()) {
            filterLabel = "filters:";
            if (!state.filterExt.empty()) {
                filterLabel += " ext=" + state.filterExt;
            }
            if (!state.filterMime.empty()) {
                filterLabel += " mime=" + state.filterMime;
            }
            if (!state.filterTag.empty()) {
                filterLabel += " tag=" + state.filterTag;
            }
        }

        std::vector<ftxui::Element> headerItems;
        headerItems.push_back(
            ftxui::text(toWide("YAMS Browser " + std::string(YAMS_VERSION_STRING))) | ftxui::bold);
        headerItems.push_back(ftxui::separator());
        headerItems.push_back(ftxui::text(toWide("layout:" + formatLayoutMode(state.layoutMode))) |
                              ftxui::dim);
        headerItems.push_back(ftxui::separator());
        headerItems.push_back(
            ftxui::text(toWide("preview:" + formatPreviewMode(state.previewMode))) | ftxui::dim);
        headerItems.push_back(ftxui::separator());
        headerItems.push_back(
            ftxui::text(toWide(std::string("fuzzy:") + (state.useFuzzySearch ? "on" : "off"))) |
            ftxui::dim);
        if (!filterLabel.empty()) {
            headerItems.push_back(ftxui::separator());
            headerItems.push_back(ftxui::text(toWide(filterLabel)) | ftxui::dim);
        }
        headerItems.push_back(ftxui::filler());
        headerItems.push_back(ftxui::text(toWide("docs:" + std::to_string(state.documents.size()) +
                                                 "/" + std::to_string(state.allDocuments.size()))) |
                              ftxui::dim);

        ftxui::Element header = ftxui::hbox(std::move(headerItems)) | ftxui::border;

        ftxui::Element body;
        switch (state.layoutMode) {
            case tui::LayoutMode::MultiPane: {
                auto documents = buildDocumentPane(documentsFocused, contentHeight) | ftxui::flex;
                auto preview = buildPreviewPane(previewFocused, contentHeight) | ftxui::flex;
                body = ftxui::hbox({std::move(documents), ftxui::separator(), std::move(preview)});
                break;
            }
            case tui::LayoutMode::SinglePane: {
                auto documents =
                    buildDocumentPane(documentsFocused, contentHeight / 2) | ftxui::flex;
                auto preview = buildPreviewPane(previewFocused, contentHeight / 2) | ftxui::flex;
                body = ftxui::vbox({std::move(documents), ftxui::separator(), std::move(preview)});
                break;
            }
            case tui::LayoutMode::Compact: {
                auto documents =
                    buildDocumentPane(documentsFocused, contentHeight) | ftxui::flex_grow;
                body = std::move(documents);
                break;
            }
        }

        auto layout = ftxui::vbox({
                          std::move(header),
                          ftxui::separator(),
                          std::move(body) | ftxui::flex,
                          ftxui::separator(),
                          buildStatusBar(),
                      }) |
                      ftxui::flex;

        auto decorated = buildHelpOverlay(layout);
        if (state.viewerOpen) {
            decorated = buildViewerOverlay(decorated);
        }
        decorated = buildModalOverlay(decorated);
        return decorated;
    });

    auto controller = ftxui::CatchEvent(renderer, [&](const ftxui::Event& event) {
        if (state.searchMode) {
            return handleSearchEvent(event);
        }
        if (state.commandMode) {
            return handleCommandEvent(event, exitLoop);
        }
        return handleGeneralEvent(event, exitLoop);
    });

    interactive.Loop(controller);
    screen = nullptr;
    return Result<void>();
}

void BrowseCommand::Impl::updateLayout(int width, int height) {
    state.terminalWidth = width;
    state.terminalHeight = height;

    tui::LayoutMode newMode = tui::LayoutMode::MultiPane;
    if (width < 80) {
        newMode = tui::LayoutMode::Compact;
    } else if (width < 110) {
        newMode = tui::LayoutMode::SinglePane;
    }
    if (state.layoutMode != newMode) {
        state.layoutMode = newMode;
    }

    switch (state.layoutMode) {
        case tui::LayoutMode::MultiPane:
            state.documentsPerPage = std::max(20, height - 12);
            break;
        case tui::LayoutMode::SinglePane:
            state.documentsPerPage = std::max(12, height / 2);
            break;
        case tui::LayoutMode::Compact:
            state.documentsPerPage = std::max(10, height - 6);
            break;
    }
}

void BrowseCommand::Impl::setStatus(std::string message, tui::BrowseState::Status::Level level,
                                    std::chrono::seconds duration) {
    state.statusMessage = message;
    state.setStatus(message, level, duration);
    if (screen) {
        screen->RequestAnimationFrame();
    }
}

void BrowseCommand::Impl::applySearchFilter() {
    if (state.useFuzzySearch && !state.searchQuery.empty()) {
        state.documents = services.fuzzySearch(state.searchQuery, 0.6f, 500);
    } else {
        state.documents = state.searchQuery.empty()
                              ? state.allDocuments
                              : services.filterBasic(state.allDocuments, state.searchQuery);
    }

    applyFilters(state.documents);

    if (state.searchQuery.empty() && state.filterExt.empty() && state.filterMime.empty() &&
        state.filterTag.empty()) {
        if (!state.documents.empty()) {
            setStatus("Listing " + std::to_string(state.documents.size()) + " document(s)",
                      tui::BrowseState::Status::Info, std::chrono::seconds(1));
        }
    } else {
        std::string label = state.useFuzzySearch ? "Fuzzy" : "Filter";
        setStatus(label + ": " + state.searchQuery + " -> " +
                      std::to_string(state.documents.size()) + " result(s)",
                  state.documents.empty() ? tui::BrowseState::Status::Warning
                                          : tui::BrowseState::Status::Info,
                  std::chrono::seconds(2));
    }
    ensureSelectionValid();
    loadPreviewForSelection();
    searchDirty = false;
    lastAppliedQuery = state.searchQuery;
}

void BrowseCommand::Impl::applyFilters(std::vector<tui::DocEntry>& docs) {
    if (state.filterExt.empty() && state.filterMime.empty() && state.filterTag.empty()) {
        return;
    }

    auto metadataRepo = cli ? cli->getMetadataRepository() : nullptr;

    docs.erase(std::remove_if(
                   docs.begin(), docs.end(),
                   [&](const auto& entry) {
                       if (!state.filterExt.empty()) {
                           auto pos = entry.name.rfind('.');
                           std::string_view ext = (pos != std::string::npos)
                                                      ? std::string_view(entry.name).substr(pos + 1)
                                                      : std::string_view();
                           if (!containsCi(ext, state.filterExt)) {
                               return true;
                           }
                       }

                       if (!state.filterMime.empty() && !containsCi(entry.type, state.filterMime)) {
                           return true;
                       }

                       if (!state.filterTag.empty()) {
                           if (!metadataRepo) {
                               return true;
                           }
                           auto tagsRes = metadataRepo->getDocumentTags(entry.id);
                           if (!tagsRes.has_value()) {
                               return true;
                           }
                           bool tagMatch = false;
                           for (const auto& tag : tagsRes.value()) {
                               if (containsCi(tag, state.filterTag)) {
                                   tagMatch = true;
                                   break;
                               }
                           }
                           if (!tagMatch) {
                               return true;
                           }
                       }

                       return false;
                   }),
               docs.end());
}

void BrowseCommand::Impl::ensureSelectionValid() {
    if (state.documents.empty()) {
        state.selected = 0;
        state.currentPage = 0;
        return;
    }
    if (state.selected < 0) {
        state.selected = 0;
    }
    if (state.selected >= static_cast<int>(state.documents.size())) {
        state.selected = static_cast<int>(state.documents.size()) - 1;
    }
    if (state.documentsPerPage <= 0) {
        state.documentsPerPage = 20;
    }
    state.currentPage = state.selected / state.documentsPerPage;
    state.documentScrollOffset = state.currentPage * state.documentsPerPage;
}

void BrowseCommand::Impl::loadPreviewForSelection() {
    state.previewLines.clear();
    state.previewScrollOffset = 0;
    previewJob.reset();
    previewPublishedLines = 0;
    previewDocSize = 0;
    previewPercent = 0;

    if (state.documents.empty()) {
        return;
    }

    const auto doc = state.documents[state.selected];
    const auto previewMode = state.previewMode;
    constexpr std::size_t kPreviewBytes = 256 * 1024;
    constexpr std::size_t kPreviewLines = 400;
    constexpr std::size_t kAsyncThreshold = 512 * 1024;

    auto makeLines = [this, doc, previewMode]() {
        return services.makePreviewLines(doc, previewMode, kPreviewBytes, kPreviewLines);
    };

    if (doc.size <= kAsyncThreshold) {
        state.previewLines = makeLines();
        previewPercent = 100;
        if (state.viewerOpen) {
            loadViewerContent(true);
        }
        return;
    }

    auto job = std::make_shared<PreviewJob>();
    job->lines = std::make_shared<std::vector<std::string>>();
    job->mutex = std::make_shared<std::mutex>();
    job->producedLines = std::make_shared<std::atomic<size_t>>(0);
    job->totalLines = std::make_shared<std::atomic<size_t>>(0);
    job->producedBytes = std::make_shared<std::atomic<size_t>>(0);
    job->totalBytes = std::make_shared<std::atomic<size_t>>(0);
    job->done = std::make_shared<std::atomic<bool>>(false);

    job->worker = std::async(std::launch::async, [job, makeLines]() {
        auto lines = makeLines();
        const std::size_t total = lines.size();
        job->totalLines->store(total);
        std::size_t totalBytes = 0;
        for (const auto& line : lines) {
            totalBytes += line.size();
        }
        job->totalBytes->store(totalBytes);
        {
            std::lock_guard<std::mutex> guard(*job->mutex);
            job->lines->reserve(lines.size());
        }
        const std::size_t chunk = 64;
        std::size_t producedBytes = 0;
        for (std::size_t i = 0; i < lines.size(); ++i) {
            {
                std::lock_guard<std::mutex> guard(*job->mutex);
                job->lines->push_back(lines[i]);
            }
            producedBytes += lines[i].size();
            job->producedLines->store(i + 1);
            job->producedBytes->store(producedBytes);
            if ((i % chunk) == 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        }
        job->producedBytes->store(totalBytes);
        job->done->store(true);
    });

    previewJob = job;
    previewPublishedLines = 0;
    previewDocSize = doc.size;
    state.previewLines = {"Loading preview... 0% |"};
    lastPreviewTick = std::chrono::steady_clock::now();
    previewSpinnerPhase = 0;
    setStatus("Loading preview...", tui::BrowseState::Status::Info, std::chrono::seconds(1));
}

void BrowseCommand::Impl::refreshDocuments(bool preserveSelection) {
    int64_t previousId = -1;
    std::string previousHash;
    if (preserveSelection && !state.documents.empty() && state.selected >= 0 &&
        state.selected < static_cast<int>(state.documents.size())) {
        const auto& selectedDoc = state.documents[state.selected];
        previousId = selectedDoc.id;
        previousHash = selectedDoc.hash;
    }

    state.allDocuments = services.loadAllDocuments(state);
    if (!state.status.message.empty()) {
        state.statusMessage = state.status.message;
    }

    bool restoredSelection = false;
    if (previousId >= 0) {
        auto it = std::find_if(state.allDocuments.begin(), state.allDocuments.end(),
                               [&](const tui::DocEntry& entry) { return entry.id == previousId; });
        if (it != state.allDocuments.end()) {
            state.selected = static_cast<int>(std::distance(state.allDocuments.begin(), it));
            restoredSelection = true;
        }
    }

    if (!restoredSelection && !previousHash.empty()) {
        auto it =
            std::find_if(state.allDocuments.begin(), state.allDocuments.end(),
                         [&](const tui::DocEntry& entry) { return entry.hash == previousHash; });
        if (it != state.allDocuments.end()) {
            state.selected = static_cast<int>(std::distance(state.allDocuments.begin(), it));
        }
    }

    applySearchFilter();
}

void BrowseCommand::Impl::pollAsyncTasks() {
    auto now = std::chrono::steady_clock::now();
    if (searchDirty && now - lastSearchEdit >= std::chrono::milliseconds(200)) {
        if (state.searchQuery != lastAppliedQuery) {
            applySearchFilter();
        } else {
            searchDirty = false;
        }
    }

    if (!previewJob) {
        return;
    }

    const size_t produced = previewJob->producedLines->load();
    const size_t total = previewJob->totalLines->load();
    const size_t producedBytes = previewJob->producedBytes ? previewJob->producedBytes->load() : 0;
    const size_t totalBytes = previewJob->totalBytes ? previewJob->totalBytes->load() : 0;
    const bool done = previewJob->done->load();

    bool requestFrame = false;

    if (produced > previewPublishedLines) {
        std::lock_guard<std::mutex> guard(*previewJob->mutex);
        if (previewPublishedLines == 0 && !previewJob->lines->empty()) {
            state.previewLines.clear();
        }
        for (size_t i = previewPublishedLines; i < produced && i < previewJob->lines->size(); ++i) {
            state.previewLines.push_back(previewJob->lines->at(i));
        }
        previewPublishedLines = produced;
        if (state.viewerOpen) {
            loadViewerContent(false);
        }
        requestFrame = true;
    }

    if (!done) {
        int percent = previewPercent;
        if (totalBytes > 0) {
            percent = static_cast<int>((std::min(producedBytes, totalBytes) * 100) / totalBytes);
        } else if (total > 0) {
            percent = static_cast<int>((std::min(produced, total) * 100) / total);
        }
        percent = std::clamp(percent, 0, 99);

        if (percent > previewPercent) {
            previewPercent = percent;
            setStatus("Preview " + std::to_string(previewPercent) + "% loaded",
                      tui::BrowseState::Status::Info, std::chrono::seconds(1));
            requestFrame = true;
        }

        if (previewPublishedLines == 0) {
            if (state.previewLines.empty()) {
                state.previewLines.emplace_back();
            }
            static const char* spinner[] = {"|", "/", "-", "\\"};
            if (now - lastPreviewTick >= std::chrono::milliseconds(150)) {
                lastPreviewTick = now;
                previewSpinnerPhase = (previewSpinnerPhase + 1) % 4;
                requestFrame = true;
            }
            std::string message =
                "Loading preview " +
                formatPreviewProgressLabel(previewPercent, produced, total, producedBytes,
                                           totalBytes, previewDocSize) +
                " " + buildProgressBar(previewPercent) + " " +
                std::string(spinner[previewSpinnerPhase]);
            if (state.previewLines[0] != message) {
                state.previewLines[0] = message;
                requestFrame = true;
            }
        }

        if (requestFrame && screen) {
            screen->RequestAnimationFrame();
        }
        return;
    }

    // Job completed.
    previewDocSize = 0;
    previewPercent = 100;
    previewJob.reset();
    if (previewPublishedLines == 0) {
        state.previewLines.emplace_back("[preview] No content available");
    }
    setStatus("Preview loaded", tui::BrowseState::Status::Info, std::chrono::seconds(1));
    if (state.viewerOpen) {
        loadViewerContent(true);
    }
    if (screen) {
        screen->RequestAnimationFrame();
    }
}

void BrowseCommand::Impl::markSearchDirty() {
    searchDirty = true;
    lastSearchEdit = std::chrono::steady_clock::now();
}

void BrowseCommand::Impl::toggleViewer() {
    if (state.viewerOpen) {
        state.viewerOpen = false;
        setStatus("Viewer closed", tui::BrowseState::Status::Info, std::chrono::seconds(1));
        return;
    }
    loadViewerContent(true);
    state.viewerOpen = true;
    setStatus("Viewer opened", tui::BrowseState::Status::Info, std::chrono::seconds(1));
}

void BrowseCommand::Impl::loadViewerContent(bool forceReload) {
    const bool jobActive = previewJob && !previewJob->done->load();
    if (!forceReload && !jobActive && !state.viewerLines.empty()) {
        return;
    }
    state.viewerLines.clear();
    state.viewerScrollOffset = std::max(0, state.viewerScrollOffset);

    if (state.documents.empty()) {
        return;
    }

    if (jobActive) {
        const size_t produced = previewJob->producedLines ? previewJob->producedLines->load() : 0;
        const size_t total = previewJob->totalLines ? previewJob->totalLines->load() : 0;
        const size_t producedBytes =
            previewJob->producedBytes ? previewJob->producedBytes->load() : 0;
        const size_t totalBytes = previewJob->totalBytes ? previewJob->totalBytes->load() : 0;

        std::lock_guard<std::mutex> guard(*previewJob->mutex);
        state.viewerLines.assign(previewJob->lines->begin(), previewJob->lines->end());
        if (state.viewerLines.empty()) {
            std::string message =
                "[viewer] Loading preview " +
                formatPreviewProgressLabel(previewPercent, produced, total, producedBytes,
                                           totalBytes, previewDocSize) +
                " " + buildProgressBar(previewPercent);
            state.viewerLines.emplace_back(std::move(message));
        }
        return;
    }

    const auto doc = state.documents[state.selected];
    constexpr std::size_t kViewerBytes = 4 * 1024 * 1024;
    constexpr std::size_t kViewerLines = 4000;

    if (state.previewMode == tui::PreviewMode::Hex) {
        state.viewerLines =
            services.makePreviewLines(doc, tui::PreviewMode::Hex, kViewerBytes, kViewerLines);
    } else {
        auto text = services.loadTextContent(doc);
        if (text && !text->empty()) {
            state.viewerLines = services.splitLines(*text, kViewerLines);
        } else {
            state.viewerLines =
                services.makePreviewLines(doc, state.previewMode, kViewerBytes, kViewerLines);
        }
    }

    if (state.viewerLines.empty()) {
        state.viewerLines.emplace_back("[viewer] No content available");
    }
}

bool BrowseCommand::Impl::handleViewerEvent(const ftxui::Event& event) {
    if (!state.viewerOpen) {
        return false;
    }

    const int totalLines = static_cast<int>(state.viewerLines.size());

    auto clampOffset = [&]() {
        if (state.viewerScrollOffset < 0) {
            state.viewerScrollOffset = 0;
        }
        if (totalLines > 0) {
            state.viewerScrollOffset =
                std::min(state.viewerScrollOffset, std::max(0, totalLines - 1));
        } else {
            state.viewerScrollOffset = 0;
        }
    };

    if (event == ftxui::Event::Escape || event == ftxui::Event::Character('q') ||
        event == ftxui::Event::Character('v')) {
        state.viewerOpen = false;
        setStatus("Viewer closed", tui::BrowseState::Status::Info, std::chrono::seconds(1));
        return true;
    }
    if (event == ftxui::Event::Character('w')) {
        state.viewerWrap = !state.viewerWrap;
        setStatus(state.viewerWrap ? "Viewer wrap enabled" : "Viewer wrap disabled",
                  tui::BrowseState::Status::Info, std::chrono::seconds(2));
        return true;
    }
    if (event == ftxui::Event::ArrowDown || event == ftxui::Event::Character('j')) {
        ++state.viewerScrollOffset;
        clampOffset();
        return true;
    }
    if (event == ftxui::Event::ArrowUp || event == ftxui::Event::Character('k')) {
        --state.viewerScrollOffset;
        clampOffset();
        return true;
    }
    if (event == ftxui::Event::PageDown || event == ftxui::Event::Character('d')) {
        state.viewerScrollOffset += std::max(1, state.documentsPerPage);
        clampOffset();
        return true;
    }
    if (event == ftxui::Event::PageUp || event == ftxui::Event::Character('u')) {
        state.viewerScrollOffset -= std::max(1, state.documentsPerPage);
        clampOffset();
        return true;
    }
    if (event == ftxui::Event::Character('g') || event == ftxui::Event::Home) {
        state.viewerScrollOffset = 0;
        return true;
    }
    if (event == ftxui::Event::Character('G') || event == ftxui::Event::End) {
        state.viewerScrollOffset = totalLines - 1;
        clampOffset();
        return true;
    }

    return false;
}

ftxui::Element BrowseCommand::Impl::buildViewerOverlay(const ftxui::Element& base) const {
    if (!state.viewerOpen) {
        return base;
    }

    int height = std::max(10, state.terminalHeight - 6);
    int width = std::max(40, state.terminalWidth - 10);

    ftxui::Element body;
    if (state.viewerLines.empty()) {
        body = ftxui::text(L"No content") | ftxui::dim;
    } else if (state.viewerWrap) {
        std::string joined;
        const int start = std::clamp(state.viewerScrollOffset, 0,
                                     std::max(0, static_cast<int>(state.viewerLines.size()) - 1));
        const int end = std::min(static_cast<int>(state.viewerLines.size()), start + height * 2);
        for (int i = start; i < end; ++i) {
            joined += state.viewerLines[i];
            joined.push_back('\n');
        }
        body = ftxui::paragraph(joined);
    } else {
        ftxui::Elements lines;
        const int limit = std::max(1, height * 2);
        const int start = std::clamp(state.viewerScrollOffset, 0,
                                     std::max(0, static_cast<int>(state.viewerLines.size()) - 1));
        const int end = std::min(static_cast<int>(state.viewerLines.size()), start + limit);
        for (int i = start; i < end; ++i) {
            lines.push_back(ftxui::text(ftxui::to_wstring(state.viewerLines[i])));
        }
        body = ftxui::vbox(std::move(lines));
    }

    auto contentBox = ftxui::vbox({body | ftxui::vscroll_indicator | ftxui::frame}) |
                      ftxui::size(ftxui::WIDTH, ftxui::LESS_THAN, width) |
                      ftxui::size(ftxui::HEIGHT, ftxui::LESS_THAN, height);

    auto footer =
        ftxui::text(L"Esc/q/v close - w wrap - j/k scroll - PageUp/PageDown jump") | ftxui::dim;

    ftxui::Elements dialogElements;
    if (previewJob && !previewJob->done->load()) {
        const double ratio = static_cast<double>(std::clamp(previewPercent, 0, 100)) / 100.0;
        const size_t produced = previewJob->producedLines ? previewJob->producedLines->load() : 0;
        const size_t total = previewJob->totalLines ? previewJob->totalLines->load() : 0;
        const size_t producedBytes =
            previewJob->producedBytes ? previewJob->producedBytes->load() : 0;
        const size_t totalBytes = previewJob->totalBytes ? previewJob->totalBytes->load() : 0;
        auto gaugeRow = ftxui::hbox(
            {ftxui::gauge(ratio) | ftxui::flex, ftxui::text(L" "),
             ftxui::text(toWide(formatPreviewProgressLabel(
                 previewPercent, produced, total, producedBytes, totalBytes, previewDocSize))) |
                 ftxui::dim});
        dialogElements.push_back(std::move(gaugeRow) | ftxui::border);
    }
    dialogElements.push_back(std::move(contentBox));
    dialogElements.push_back(ftxui::separator());
    dialogElements.push_back(std::move(footer));
    auto dialogBody = ftxui::vbox(std::move(dialogElements));

    std::string title = "Viewer";
    if (state.viewerWrap) {
        title += " (wrap)";
    }
    if (!state.documents.empty()) {
        title += " - " + state.documents[state.selected].name;
    }
    if (previewJob && !previewJob->done->load()) {
        title += " (" + std::to_string(std::clamp(previewPercent, 0, 100)) + "%)";
    }

    auto overlay = ftxui::window(ftxui::text(toWide(title)), std::move(dialogBody));
    auto centered = ftxui::center(overlay);

    return ftxui::dbox({base, centered});
}

bool BrowseCommand::Impl::handleSearchEvent(const ftxui::Event& event) {
    if (state.modal.type != tui::ModalState::Type::None) {
        return handleModalEvent(event);
    }
    if (event == ftxui::Event::Escape) {
        state.searchMode = false;
        state.searchQuery.clear();
        applySearchFilter();
        return true;
    }
    if (event == ftxui::Event::Return) {
        state.searchMode = false;
        if (searchDirty) {
            applySearchFilter();
        }
        return true;
    }
    if (event == ftxui::Event::Backspace && !state.searchQuery.empty()) {
        state.searchQuery.pop_back();
        markSearchDirty();
        return true;
    }
    if (event.is_character()) {
        auto ch = event.character();
        if (!ch.empty() && std::isprint(static_cast<unsigned char>(ch.front()))) {
            state.searchQuery += ch;
            markSearchDirty();
            return true;
        }
    }
    return false;
}

bool BrowseCommand::Impl::handleCommandEvent(const ftxui::Event& event,
                                             const std::function<void()>& exitLoopClosure) {
    if (state.modal.type != tui::ModalState::Type::None) {
        return handleModalEvent(event);
    }
    (void)exitLoopClosure;
    if (event == ftxui::Event::Escape) {
        state.commandMode = false;
        state.commandBuffer.clear();
        return true;
    }
    if (event == ftxui::Event::Return) {
        std::string command = state.commandBuffer;
        state.commandBuffer.clear();
        state.commandMode = false;

        tui::CommandActions actions;
        actions.openPager = [this](tui::BrowseState&) { return openSelectedInPager(); };
        actions.refresh = [this](tui::BrowseState&) { refreshDocuments(true); };
        actions.showHelp = [this](tui::BrowseState& s) {
            s.showHelp = true;
            if (screen) {
                screen->RequestAnimationFrame();
            }
        };

        if (executeCustomCommand(command)) {
            return true;
        }

        if (!commandParser.execute(command, state, actions)) {
            setStatus("Unknown command: " + command, tui::BrowseState::Status::Warning,
                      std::chrono::seconds(3));
        }
        return true;
    }
    if (event == ftxui::Event::Backspace && !state.commandBuffer.empty()) {
        state.commandBuffer.pop_back();
        return true;
    }
    if (event.is_character()) {
        auto ch = event.character();
        if (!ch.empty() && std::isprint(static_cast<unsigned char>(ch.front()))) {
            state.commandBuffer += ch;
            return true;
        }
    }
    return false;
}

bool BrowseCommand::Impl::executeCustomCommand(const std::string& rawCommand) {
    std::string command = rawCommand;
    trim(command);
    if (command.empty()) {
        return true;
    }

    std::string lowered = toLower(command);

    auto applyAndAnnounce = [&](std::string_view msg) {
        applySearchFilter();
        setStatus(std::string(msg));
    };

    if (lowered == "fuzzy" || lowered == "fuzzy toggle") {
        state.useFuzzySearch = !state.useFuzzySearch;
        applyAndAnnounce(state.useFuzzySearch ? "Fuzzy search enabled" : "Fuzzy search disabled");
        return true;
    }
    if (lowered == "fuzzy on") {
        state.useFuzzySearch = true;
        applyAndAnnounce("Fuzzy search enabled");
        return true;
    }
    if (lowered == "fuzzy off") {
        state.useFuzzySearch = false;
        applyAndAnnounce("Fuzzy search disabled");
        return true;
    }

    if (lowered == "viewer" || lowered == "view") {
        toggleViewer();
        return true;
    }
    if (lowered == "viewer on") {
        if (!state.viewerOpen) {
            toggleViewer();
        }
        return true;
    }
    if (lowered == "viewer off") {
        if (state.viewerOpen) {
            toggleViewer();
        }
        return true;
    }

    if (lowered == "delete" || lowered == "delete!") {
        const bool force = lowered.back() == '!';
        auto result = deleteSelectedDocument(force);
        if (result) {
            const std::size_t removed = result.value();
            std::ostringstream oss;
            oss << "Deleted " << removed << (removed == 1 ? " document" : " documents");
            setStatus(oss.str(), tui::BrowseState::Status::Info, std::chrono::seconds(3));
        } else {
            setStatus("Delete failed: " + result.error().message, tui::BrowseState::Status::Error,
                      std::chrono::seconds(4));
        }
        return true;
    }

    if (lowered.rfind("add", 0) == 0) {
        std::string args = command.size() > 3 ? command.substr(3) : std::string{};
        trim(args);
        if (args.empty()) {
            setStatus("Usage: :add <file-or-directory>", tui::BrowseState::Status::Warning,
                      std::chrono::seconds(4));
            return true;
        }
        auto result = addDocumentFromPath(args);
        if (result) {
            std::ostringstream oss;
            oss << "Added " << result.value() << (result.value() == 1 ? " item" : " items");
            setStatus(oss.str(), tui::BrowseState::Status::Info, std::chrono::seconds(3));
        } else {
            setStatus("Add failed: " + result.error().message, tui::BrowseState::Status::Error,
                      std::chrono::seconds(4));
        }
        return true;
    }

    if (lowered.rfind("tag", 0) == 0) {
        std::string args = command.size() > 3 ? command.substr(3) : std::string{};
        trim(args);
        std::vector<std::string> additions;
        std::vector<std::string> removals;
        bool clearExisting = false;
        std::string parseError;
        if (!parseTagArguments(args, additions, removals, clearExisting, parseError)) {
            setStatus("Tag parse error: " + parseError, tui::BrowseState::Status::Warning,
                      std::chrono::seconds(4));
            return true;
        }

        auto result = updateDocumentTags(additions, removals, clearExisting);
        if (result) {
            std::ostringstream oss;
            oss << "Tags updated (" << result.value() << " change"
                << (result.value() == 1 ? "" : "s") << ")";
            setStatus(oss.str(), tui::BrowseState::Status::Info, std::chrono::seconds(3));
        } else {
            setStatus("Tag update failed: " + result.error().message,
                      tui::BrowseState::Status::Error, std::chrono::seconds(4));
        }
        return true;
    }

    if (lowered == "reindex") {
        openReindexDialog();
        return true;
    }

    if (lowered.rfind("filter", 0) == 0) {
        std::string args;
        if (auto space = command.find(' '); space != std::string::npos) {
            args = command.substr(space + 1);
        }
        trim(args);
        if (args.empty() || toLower(args) == "clear") {
            state.filterExt.clear();
            state.filterMime.clear();
            state.filterTag.clear();
            applyAndAnnounce("Filters cleared");
            return true;
        }

        auto colon = args.find(':');
        if (colon == std::string::npos) {
            setStatus("Expected filter syntax ext:<value>", tui::BrowseState::Status::Warning,
                      std::chrono::seconds(3));
            return true;
        }

        std::string key = args.substr(0, colon);
        std::string value = args.substr(colon + 1);
        trim(key);
        trim(value);
        std::string keyLower = toLower(key);
        if (keyLower == "ext" || keyLower == "extension") {
            state.filterExt = value;
            applyAndAnnounce("Extension filter applied");
            return true;
        }
        if (keyLower == "mime") {
            state.filterMime = value;
            applyAndAnnounce("MIME filter applied");
            return true;
        }
        if (keyLower == "tag" || keyLower == "tags") {
            state.filterTag = value;
            applyAndAnnounce("Tag filter applied");
            return true;
        }

        setStatus("Unknown filter key: " + key, tui::BrowseState::Status::Warning,
                  std::chrono::seconds(3));
        return true;
    }

    return false;
}

Result<std::size_t> BrowseCommand::Impl::deleteSelectedDocument(bool force) {
    if (state.documents.empty() || state.selected < 0 ||
        state.selected >= static_cast<int>(state.documents.size())) {
        return Error{ErrorCode::InvalidOperation, "No document selected"};
    }

    auto ensure = serviceAdapter.ensureReady();
    if (!ensure) {
        return ensure.error();
    }

    auto documentService = serviceAdapter.documentService();
    if (!documentService) {
        return Error{ErrorCode::NotInitialized, "Document service unavailable"};
    }

    const auto& doc = state.documents[state.selected];
    if (doc.hash.empty() && doc.path.empty() && doc.name.empty()) {
        return Error{ErrorCode::InvalidOperation,
                     "Selected document has no hash or path information"};
    }

    app::services::DeleteByNameRequest request;
    if (!doc.hash.empty()) {
        request.hash = doc.hash;
    } else if (!doc.path.empty()) {
        request.name = doc.path;
    } else {
        request.name = doc.name;
    }
    request.force = force;
    request.recursive = false;
    request.verbose = true;

    auto response = documentService->deleteByName(request);
    if (!response) {
        return response.error();
    }

    const auto& payload = response.value();
    std::size_t removed = 0;
    for (const auto& entry : payload.deleted) {
        if (entry.deleted) {
            ++removed;
        }
    }

    if (removed == 0 && payload.count == 0) {
        return Error{ErrorCode::NotFound, "No matching documents deleted"};
    }

    refreshDocuments(false);
    ensureSelectionValid();
    return removed ? removed : payload.count;
}

Result<std::size_t> BrowseCommand::Impl::updateDocumentTags(const std::vector<std::string>& add,
                                                            const std::vector<std::string>& remove,
                                                            bool clearExisting) {
    if (state.documents.empty() || state.selected < 0 ||
        state.selected >= static_cast<int>(state.documents.size())) {
        return Error{ErrorCode::InvalidOperation, "No document selected"};
    }

    auto ensure = serviceAdapter.ensureReady();
    if (!ensure) {
        return ensure.error();
    }

    auto documentService = serviceAdapter.documentService();
    if (!documentService) {
        return Error{ErrorCode::NotInitialized, "Document service unavailable"};
    }

    const auto& doc = state.documents[state.selected];

    std::vector<std::string> additions;
    additions.reserve(add.size());
    for (const auto& tag : add) {
        if (!tag.empty()) {
            additions.push_back(tag);
        }
    }

    std::vector<std::string> removals;
    removals.reserve(remove.size());
    for (const auto& tag : remove) {
        if (!tag.empty()) {
            removals.push_back(tag);
        }
    }

    if (clearExisting) {
        auto metadataRepo = serviceAdapter.metadataRepo();
        if (!metadataRepo) {
            return Error{ErrorCode::NotInitialized, "Metadata repository unavailable"};
        }
        auto currentTags = currentTagsForDoc(doc);
        removals.assign(currentTags.begin(), currentTags.end());
    }

    if (additions.empty() && removals.empty()) {
        return Error{ErrorCode::InvalidArgument, "No tag changes specified"};
    }

    app::services::UpdateMetadataRequest request;
    request.atomic = true;
    request.addTags = additions;
    request.removeTags = removals;

    if (!doc.hash.empty()) {
        request.hash = doc.hash;
    } else if (!doc.path.empty()) {
        request.name = doc.path;
    } else {
        request.name = doc.name;
    }

    auto response = documentService->updateMetadata(request);
    if (!response) {
        return response.error();
    }

    refreshDocuments(true);
    if (!doc.hash.empty()) {
        selectDocumentByHash(doc.hash);
    } else if (!doc.path.empty()) {
        selectDocumentByPath(doc.path);
    }

    return response.value().updatesApplied;
}

Result<void> BrowseCommand::Impl::performReindex() {
    if (state.documents.empty() || state.selected < 0 ||
        state.selected >= static_cast<int>(state.documents.size())) {
        return Error{ErrorCode::InvalidOperation, "No document selected"};
    }

    auto ensure = serviceAdapter.ensureReady();
    if (!ensure) {
        return ensure.error();
    }

    const auto& doc = state.documents[state.selected];
    if (doc.hash.empty()) {
        return Error{ErrorCode::InvalidOperation, "Document hash unavailable; cannot reindex"};
    }

    auto result = serviceAdapter.reindexDocument(doc.hash);
    if (result) {
        refreshDocuments(true);
        selectDocumentByHash(doc.hash);
        std::string label = !doc.name.empty() ? doc.name : doc.hash;
        setStatus("Reindexed " + label, tui::BrowseState::Status::Info, std::chrono::seconds(3));
    }
    return result;
}

std::vector<std::string> BrowseCommand::Impl::currentTagsForDoc(const tui::DocEntry& doc) const {
    std::vector<std::string> tags;
    auto metadataRepo = serviceAdapter.metadataRepo();
    if (!metadataRepo) {
        return tags;
    }

    auto loadTagsById = [&](int64_t id) -> std::optional<std::vector<std::string>> {
        if (id < 0) {
            return std::nullopt;
        }
        auto tagsRes = metadataRepo->getDocumentTags(id);
        if (!tagsRes) {
            return std::nullopt;
        }
        return tagsRes.value();
    };

    std::optional<std::vector<std::string>> direct = loadTagsById(doc.id);
    if (!direct && !doc.hash.empty()) {
        auto infoRes = metadataRepo->getDocumentByHash(doc.hash);
        if (infoRes && infoRes.value()) {
            direct = loadTagsById(infoRes.value()->id);
        }
    }

    if (direct) {
        tags = std::move(*direct);
    }
    return tags;
}

bool BrowseCommand::Impl::parseTagArguments(const std::string& input, std::vector<std::string>& add,
                                            std::vector<std::string>& remove, bool& clear,
                                            std::string& error) const {
    add.clear();
    remove.clear();
    clear = false;
    error.clear();

    std::string trimmed = input;
    trim(trimmed);
    if (trimmed.empty()) {
        error = "No tag input provided";
        return false;
    }

    std::istringstream iss(trimmed);
    bool hasToken = false;
    for (std::string token; iss >> token;) {
        hasToken = true;
        if (token == "clear") {
            clear = true;
            continue;
        }
        if (token.size() > 1 && token.front() == '+') {
            add.push_back(token.substr(1));
        } else if (token.size() > 1 && token.front() == '-') {
            remove.push_back(token.substr(1));
        } else {
            add.push_back(token);
        }
    }

    if (!hasToken) {
        error = "No tag tokens found";
        return false;
    }

    if (!clear && add.empty() && remove.empty()) {
        error = "Specify tags to add or remove";
        return false;
    }

    return true;
}

Result<std::size_t> BrowseCommand::Impl::addDocumentFromPath(const std::string& rawPath) {
    std::string pathInput = rawPath;
    trim(pathInput);
    if (pathInput.empty()) {
        return Error{ErrorCode::InvalidArgument, "Path is required"};
    }

    std::filesystem::path path(pathInput);
    std::error_code ec;
    if (!std::filesystem::exists(path, ec)) {
        return Error{ErrorCode::FileNotFound, "Path not found: " + pathInput};
    }

    auto ensure = serviceAdapter.ensureReady();
    if (!ensure) {
        return ensure.error();
    }

    if (std::filesystem::is_directory(path, ec)) {
        auto indexingService = serviceAdapter.indexingService();
        if (!indexingService) {
            return Error{ErrorCode::NotInitialized, "Indexing service unavailable"};
        }

        app::services::AddDirectoryRequest request;
        request.directoryPath = path.string();
        request.recursive = true;

        auto response = indexingService->addDirectory(request);
        if (!response) {
            return response.error();
        }

        refreshDocuments(true);
        ensureSelectionValid();
        loadPreviewForSelection();
        return response.value().filesIndexed;
    }

    auto documentService = serviceAdapter.documentService();
    if (!documentService) {
        return Error{ErrorCode::NotInitialized, "Document service unavailable"};
    }

    app::services::StoreDocumentRequest request;
    request.path = path.string();

    auto response = documentService->store(request);
    if (!response) {
        return response.error();
    }

    const std::string newHash = response.value().hash;
    refreshDocuments(false);
    if (!selectDocumentByHash(newHash)) {
        selectDocumentByPath(path.string());
    }
    return static_cast<std::size_t>(1);
}

bool BrowseCommand::Impl::selectDocumentByHash(const std::string& hash) {
    if (hash.empty()) {
        return false;
    }
    auto it = std::find_if(state.documents.begin(), state.documents.end(),
                           [&](const tui::DocEntry& entry) { return entry.hash == hash; });
    if (it == state.documents.end()) {
        return false;
    }
    state.selected = static_cast<int>(std::distance(state.documents.begin(), it));
    ensureSelectionValid();
    loadPreviewForSelection();
    return true;
}

bool BrowseCommand::Impl::selectDocumentByPath(const std::string& path) {
    if (path.empty()) {
        return false;
    }

    auto normalize = [](const std::string& value) -> std::string {
        std::error_code ec;
        auto p = std::filesystem::path(value);
        auto normalized = p.lexically_normal();
        return normalized.string();
    };

    const std::string normalizedTarget = normalize(path);

    auto it = std::find_if(state.documents.begin(), state.documents.end(),
                           [&](const tui::DocEntry& entry) {
                               if (!entry.path.empty()) {
                                   return normalize(entry.path) == normalizedTarget;
                               }
                               return entry.name == path;
                           });
    if (it == state.documents.end()) {
        return false;
    }
    state.selected = static_cast<int>(std::distance(state.documents.begin(), it));
    ensureSelectionValid();
    loadPreviewForSelection();
    return true;
}

bool BrowseCommand::Impl::handleGeneralEvent(const ftxui::Event& event,
                                             const std::function<void()>& exitLoopClosure) {
    if (state.modal.type != tui::ModalState::Type::None) {
        return handleModalEvent(event);
    }

    if (state.viewerOpen && handleViewerEvent(event)) {
        return true;
    }

    if (event == ftxui::Event::Character('q') || event == ftxui::Event::Escape) {
        exitLoopClosure();
        return true;
    }

    if (event == ftxui::Event::Character('/') && !state.commandMode) {
        state.searchMode = true;
        state.searchQuery.clear();
        markSearchDirty();
        setStatus("Search mode", tui::BrowseState::Status::Info, std::chrono::seconds(2));
        return true;
    }

    if (event == ftxui::Event::Character(':')) {
        state.commandMode = true;
        state.commandBuffer.clear();
        return true;
    }

    if (event == ftxui::Event::Character('a')) {
        openAddDocumentDialog();
        return true;
    }

    if (event == ftxui::Event::Character('u')) {
        openTagEditDialog();
        return true;
    }

    if (event == ftxui::Event::Character('R')) {
        openReindexDialog();
        return true;
    }

    if (event == ftxui::Event::Character('r')) {
        refreshDocuments(true);
        setStatus("Document list refreshed");
        return true;
    }
    if (event == ftxui::Event::Character('o') || event == ftxui::Event::Return) {
        if (openSelectedInPager()) {
            setStatus("Opened in pager");
        }
        return true;
    }
    if (event == ftxui::Event::Character('h')) {
        state.showHelp = !state.showHelp;
        return true;
    }
    if (event == ftxui::Event::Character('f')) {
        state.useFuzzySearch = !state.useFuzzySearch;
        applySearchFilter();
        setStatus(std::string("Fuzzy search ") + (state.useFuzzySearch ? "enabled" : "disabled"));
        return true;
    }
    if (event == ftxui::Event::Character('v')) {
        toggleViewer();
        return true;
    }

    const bool documentsFocused = state.activeColumn == tui::Column::Documents;

    if (documentsFocused) {
        if (event == ftxui::Event::ArrowDown || event == ftxui::Event::Character('j')) {
            if (state.selected + 1 < static_cast<int>(state.documents.size())) {
                ++state.selected;
                ensureSelectionValid();
                loadPreviewForSelection();
            }
            return true;
        }
        if (event == ftxui::Event::ArrowUp || event == ftxui::Event::Character('k')) {
            if (state.selected > 0) {
                --state.selected;
                ensureSelectionValid();
                loadPreviewForSelection();
            }
            return true;
        }
        if (event == ftxui::Event::PageDown || event == ftxui::Event::Character('d')) {
            state.selected = std::min(static_cast<int>(state.documents.size()) - 1,
                                      state.selected + state.documentsPerPage);
            ensureSelectionValid();
            loadPreviewForSelection();
            return true;
        }
        if (event == ftxui::Event::PageUp || event == ftxui::Event::Character('u')) {
            state.selected = std::max(0, state.selected - state.documentsPerPage);
            ensureSelectionValid();
            loadPreviewForSelection();
            return true;
        }
        if (event == ftxui::Event::Home || event == ftxui::Event::Character('g')) {
            state.selected = 0;
            ensureSelectionValid();
            loadPreviewForSelection();
            return true;
        }
        if (event == ftxui::Event::End || event == ftxui::Event::Character('G')) {
            state.selected = static_cast<int>(state.documents.size()) - 1;
            ensureSelectionValid();
            loadPreviewForSelection();
            return true;
        }
        if (event == ftxui::Event::ArrowRight || event == ftxui::Event::Character('l')) {
            state.activeColumn = tui::Column::Preview;
            return true;
        }
    } else {
        if (event == ftxui::Event::ArrowLeft || event == ftxui::Event::Character('h')) {
            state.activeColumn = tui::Column::Documents;
            return true;
        }
        if (event == ftxui::Event::ArrowDown || event == ftxui::Event::Character('j')) {
            state.previewScrollOffset = std::min(static_cast<int>(state.previewLines.size()),
                                                 state.previewScrollOffset + 1);
            return true;
        }
        if (event == ftxui::Event::ArrowUp || event == ftxui::Event::Character('k')) {
            state.previewScrollOffset = std::max(0, state.previewScrollOffset - 1);
            return true;
        }
        if (event == ftxui::Event::PageDown) {
            state.previewScrollOffset =
                std::min(static_cast<int>(state.previewLines.size()),
                         state.previewScrollOffset + state.documentsPerPage);
            return true;
        }
        if (event == ftxui::Event::PageUp) {
            state.previewScrollOffset =
                std::max(0, state.previewScrollOffset - state.documentsPerPage);
            return true;
        }
    }

    return false;
}

bool BrowseCommand::Impl::openSelectedInPager() {
    if (state.documents.empty()) {
        setStatus("No document selected", tui::BrowseState::Status::Warning,
                  std::chrono::seconds(2));
        return false;
    }

    const auto& doc = state.documents[state.selected];
    auto text = services.loadTextContent(doc);
    auto raw = services.loadRawBytes(doc.hash, 2 * 1024 * 1024);

    auto suspend = [this](const std::function<void()>& fn) {
        if (screen) {
            screen->WithRestoredIO(fn);
            screen->PostEvent(ftxui::Event::Custom);
        } else {
            fn();
        }
    };

    return services.openInPagerWithSuspend(state, doc.name, text, raw, suspend);
}

ftxui::Element BrowseCommand::Impl::buildDocumentPane(bool focused, int maxHeight) const {
    std::vector<ftxui::Element> rows;
    if (state.documents.empty()) {
        rows.push_back(ftxui::text(L"No documents") | ftxui::dim);
    } else {
        const int perPage = std::max(1, state.documentsPerPage);
        const int startIndex =
            std::clamp(state.selected / perPage * perPage, 0,
                       std::max(0, static_cast<int>(state.documents.size()) - 1));
        const int endIndex =
            std::min(static_cast<int>(state.documents.size()), startIndex + perPage);

        for (int index = startIndex; index < endIndex; ++index) {
            const auto& doc = state.documents[index];
            const bool selected = index == state.selected;
            std::string label = truncateMiddle(doc.name, std::max(20, state.terminalWidth / 2)) +
                                " - " + humanReadableBytes(doc.size);
            ftxui::Element labelElement;
            if (!state.useFuzzySearch && !state.searchQuery.empty()) {
                labelElement = renderHighlighted(label, state.searchQuery);
            } else {
                labelElement = ftxui::text(toWide(label));
            }
            auto line = ftxui::hbox({std::move(labelElement)});
            if (selected) {
                line = line | ftxui::inverted;
            }
            rows.push_back(std::move(line));
        }
    }

    auto content = ftxui::vbox(std::move(rows));
    content = ftxui::vbox({content | ftxui::vscroll_indicator | ftxui::frame}) |
              ftxui::size(ftxui::HEIGHT, ftxui::LESS_THAN, std::max(5, maxHeight));

    std::string title = focused ? "Documents - focus" : "Documents";
    return ftxui::window(ftxui::text(toWide(title)), std::move(content));
}

ftxui::Element BrowseCommand::Impl::buildPreviewPane(bool focused, int maxHeight) const {
    std::vector<ftxui::Element> lines;
    if (state.previewLines.empty()) {
        lines.push_back(ftxui::text(L"No preview available") | ftxui::dim);
    } else {
        const int limit = std::max(1, maxHeight - 2);
        const int start = std::clamp(state.previewScrollOffset, 0,
                                     std::max(0, static_cast<int>(state.previewLines.size()) - 1));
        const int end = std::min(static_cast<int>(state.previewLines.size()), start + limit);
        for (int i = start; i < end; ++i) {
            lines.push_back(ftxui::text(toWide(state.previewLines[i])));
        }
    }

    auto listBody = ftxui::vbox(std::move(lines));
    auto body = ftxui::vbox({listBody | ftxui::vscroll_indicator | ftxui::frame}) |
                ftxui::size(ftxui::HEIGHT, ftxui::LESS_THAN, std::max(5, maxHeight));

    ftxui::Elements content;
    if (previewJob && !previewJob->done->load()) {
        const double ratio = static_cast<double>(std::clamp(previewPercent, 0, 100)) / 100.0;
        const size_t produced = previewJob->producedLines ? previewJob->producedLines->load() : 0;
        const size_t total = previewJob->totalLines ? previewJob->totalLines->load() : 0;
        const size_t producedBytes =
            previewJob->producedBytes ? previewJob->producedBytes->load() : 0;
        const size_t totalBytes = previewJob->totalBytes ? previewJob->totalBytes->load() : 0;
        auto gaugeRow = ftxui::hbox(
            {ftxui::gauge(ratio) | ftxui::flex, ftxui::text(L" "),
             ftxui::text(toWide(formatPreviewProgressLabel(
                 previewPercent, produced, total, producedBytes, totalBytes, previewDocSize))) |
                 ftxui::dim});
        content.push_back(std::move(gaugeRow) | ftxui::border);
    }
    content.push_back(std::move(body));
    auto stacked = ftxui::vbox(std::move(content));

    std::string title = "Preview - " + formatPreviewMode(state.previewMode);
    if (focused) {
        title += " - focus";
    }
    return ftxui::window(ftxui::text(toWide(title)), std::move(stacked));
}

ftxui::Element BrowseCommand::Impl::buildStatusBar() const {
    auto status = state.status;
    if (status.isExpired()) {
        status.message.clear();
    }

    std::string left;
    if (state.commandMode) {
        left = ":" + state.commandBuffer + "_";
    } else if (state.searchMode) {
        left = "/" + state.searchQuery + "_";
    } else if (!status.message.empty()) {
        left = status.message;
    } else {
        left = state.statusMessage;
    }

    std::ostringstream oss;
    oss << "sel " << (state.documents.empty() ? 0 : state.selected + 1) << "/"
        << state.documents.size() << " page "
        << (state.documentsPerPage ? state.currentPage + 1 : 1);
    if (!state.filterExt.empty() || !state.filterMime.empty() || !state.filterTag.empty()) {
        oss << " filters";
        if (!state.filterExt.empty()) {
            oss << " ext=" << state.filterExt;
        }
        if (!state.filterMime.empty()) {
            oss << " mime=" << state.filterMime;
        }
        if (!state.filterTag.empty()) {
            oss << " tag=" << state.filterTag;
        }
    }
    std::string right = oss.str();

    return ftxui::hbox(
               {ftxui::text(toWide(left)) | ftxui::flex, ftxui::text(toWide(right)) | ftxui::dim}) |
           ftxui::border;
}

ftxui::Element BrowseCommand::Impl::buildHelpOverlay(const ftxui::Element& base) const {
    if (!state.showHelp) {
        return base;
    }

    std::vector<ftxui::Element> helpLines = {
        ftxui::text(L"Help") | ftxui::bold,
        ftxui::separator(),
        ftxui::text(L"j/k, Up/Down : Navigate documents"),
        ftxui::text(L"g/G, Home/End : Jump to start/end"),
        ftxui::text(L"PageUp/PageDown : Page navigation"),
        ftxui::text(L"o or Enter : Open in $PAGER"),
        ftxui::text(L"/ : Search mode - : : Command mode"),
        ftxui::text(L"f : Toggle fuzzy search"),
        ftxui::text(L":filter ext:md | mime:text | tag:project"),
        ftxui::text(L":filter clear : Clear filters"),
        ftxui::text(L"v : Toggle viewer - w : Wrap toggle (viewer)"),
        ftxui::text(L"a : Add document dialog"),
        ftxui::text(L"u : Edit tags dialog"),
        ftxui::text(L"Shift+R : Reindex document"),
        ftxui::text(L":auto | :text | :hex : Preview mode"),
        ftxui::text(L"r : Refresh documents"),
        ftxui::text(L"h : Toggle this help"),
        ftxui::text(L"q or Esc : Quit"),
    };

    auto dialog = ftxui::window(ftxui::text(L"Keybindings"),
                                ftxui::vbox(std::move(helpLines)) |
                                    ftxui::size(ftxui::WIDTH, ftxui::LESS_THAN, 60));

    return ftxui::dbox({base, ftxui::center(dialog)});
}

ftxui::Element BrowseCommand::Impl::buildModalOverlay(const ftxui::Element& base) const {
    using ModalType = tui::ModalState::Type;
    const auto& modal = state.modal;
    if (modal.type == ModalType::None) {
        return base;
    }

    ftxui::Elements lines;
    if (!modal.hint.empty()) {
        lines.push_back(ftxui::text(toWide(modal.hint)) | ftxui::dim);
        lines.push_back(ftxui::separator());
    }

    if (modal.type == ModalType::Reindex) {
        lines.push_back(ftxui::text(L"Press Enter to confirm, Esc to cancel."));
    } else {
        std::string inputWithCursor = modal.input;
        inputWithCursor.push_back('_');
        lines.push_back(
            ftxui::hbox({ftxui::text(L"> "), ftxui::text(ftxui::to_wstring(inputWithCursor))}));
    }

    if (!modal.error.empty()) {
        lines.push_back(ftxui::separator());
        lines.push_back(ftxui::text(toWide("Error: " + modal.error)) |
                        ftxui::color(ftxui::Color::Red) | ftxui::bold);
    }

    lines.push_back(ftxui::separator());
    lines.push_back(ftxui::text(L"Enter to submit · Esc to cancel") | ftxui::dim);

    auto body = ftxui::vbox(std::move(lines)) | ftxui::size(ftxui::WIDTH, ftxui::LESS_THAN, 64) |
                ftxui::size(ftxui::HEIGHT, ftxui::LESS_THAN, 16);
    auto window = ftxui::window(ftxui::text(toWide(modal.title)), std::move(body));
    return ftxui::dbox({base, ftxui::center(window)});
}

void BrowseCommand::Impl::openAddDocumentDialog() {
    auto ensure = serviceAdapter.ensureReady();
    if (!ensure) {
        setStatus("Storage not initialised: " + ensure.error().message,
                  tui::BrowseState::Status::Error, std::chrono::seconds(4));
        return;
    }
    state.commandMode = false;
    state.modal.reset();
    state.modal.type = tui::ModalState::Type::AddDocument;
    state.modal.title = "Add Document";
    state.modal.hint = "Enter file or directory path."
                       " Submit with Enter, cancel with Esc.";
    if (!state.documents.empty()) {
        const auto& doc = state.documents[state.selected];
        if (!doc.path.empty()) {
            state.modal.input = doc.path;
        } else {
            state.modal.input = doc.name;
        }
    }
    if (screen) {
        screen->RequestAnimationFrame();
    }
}

void BrowseCommand::Impl::openTagEditDialog() {
    if (state.documents.empty() || state.selected < 0 ||
        state.selected >= static_cast<int>(state.documents.size())) {
        setStatus("No document selected", tui::BrowseState::Status::Warning,
                  std::chrono::seconds(3));
        return;
    }

    auto ensure = serviceAdapter.ensureReady();
    if (!ensure) {
        setStatus("Storage not initialised: " + ensure.error().message,
                  tui::BrowseState::Status::Error, std::chrono::seconds(4));
        return;
    }

    const auto& doc = state.documents[state.selected];
    auto tags = currentTagsForDoc(doc);
    std::string existing;
    if (tags.empty()) {
        existing = "No existing tags";
    } else {
        std::ostringstream oss;
        for (size_t i = 0; i < tags.size(); ++i) {
            if (i > 0) {
                oss << ", ";
            }
            oss << tags[i];
        }
        existing = "Existing tags: " + oss.str();
    }

    state.commandMode = false;
    state.modal.reset();
    state.modal.type = tui::ModalState::Type::TagEdit;
    state.modal.title = "Edit Tags";
    state.modal.hint = "Use +tag to add, -tag to remove, 'clear' to replace. " + existing;
    if (screen) {
        screen->RequestAnimationFrame();
    }
}

void BrowseCommand::Impl::openReindexDialog() {
    if (state.documents.empty() || state.selected < 0 ||
        state.selected >= static_cast<int>(state.documents.size())) {
        setStatus("No document selected", tui::BrowseState::Status::Warning,
                  std::chrono::seconds(3));
        return;
    }

    auto ensure = serviceAdapter.ensureReady();
    if (!ensure) {
        setStatus("Storage not initialised: " + ensure.error().message,
                  tui::BrowseState::Status::Error, std::chrono::seconds(4));
        return;
    }

    const auto& doc = state.documents[state.selected];
    std::string target = !doc.path.empty() ? doc.path : (!doc.name.empty() ? doc.name : doc.hash);

    state.commandMode = false;
    state.modal.reset();
    state.modal.type = tui::ModalState::Type::Reindex;
    state.modal.title = "Reindex Document";
    state.modal.hint = "Re-index will refresh search metadata for: " + target +
                       ". Press Enter to queue, Esc to cancel.";
    if (screen) {
        screen->RequestAnimationFrame();
    }
}

void BrowseCommand::Impl::closeModal() {
    state.modal.reset();
    if (screen) {
        screen->RequestAnimationFrame();
    }
}

bool BrowseCommand::Impl::handleModalEvent(const ftxui::Event& event) {
    using ModalType = tui::ModalState::Type;
    auto& modal = state.modal;
    if (modal.type == ModalType::None) {
        return false;
    }

    if (event == ftxui::Event::Escape) {
        closeModal();
        return true;
    }

    if (event == ftxui::Event::Backspace) {
        if (modal.type == ModalType::Reindex) {
            return true;
        }
        if (!modal.input.empty()) {
            modal.input.pop_back();
            modal.error.clear();
            if (screen) {
                screen->RequestAnimationFrame();
            }
        }
        return true;
    }

    if (event.is_character()) {
        auto ch = event.character();
        if (!ch.empty()) {
            char c = ch.front();
            if (modal.type == ModalType::Reindex) {
                return true;
            }
            if (c >= 32 && c != 127) {
                modal.input += ch;
                modal.error.clear();
                if (screen) {
                    screen->RequestAnimationFrame();
                }
                return true;
            }
        }
    }

    if (event == ftxui::Event::Tab) {
        return true;
    }

    if (event == ftxui::Event::Return) {
        if (modal.type == ModalType::AddDocument) {
            auto result = addDocumentFromPath(modal.input);
            if (result) {
                std::ostringstream oss;
                oss << "Added " << result.value() << (result.value() == 1 ? " item" : " items");
                setStatus(oss.str(), tui::BrowseState::Status::Info, std::chrono::seconds(3));
                closeModal();
            } else {
                modal.error = result.error().message;
            }
        } else if (modal.type == ModalType::TagEdit) {
            std::vector<std::string> additions;
            std::vector<std::string> removals;
            bool clearExisting = false;
            std::string parseError;
            if (!parseTagArguments(modal.input, additions, removals, clearExisting, parseError)) {
                modal.error = parseError;
            } else {
                auto result = updateDocumentTags(additions, removals, clearExisting);
                if (result) {
                    std::ostringstream oss;
                    oss << "Tags updated (" << result.value() << " change"
                        << (result.value() == 1 ? "" : "s") << ")";
                    setStatus(oss.str(), tui::BrowseState::Status::Info, std::chrono::seconds(3));
                    closeModal();
                } else {
                    modal.error = result.error().message;
                }
            }
        } else if (modal.type == ModalType::Reindex) {
            auto result = performReindex();
            if (result) {
                closeModal();
            } else {
                modal.error = result.error().message;
            }
        }

        if (screen) {
            screen->RequestAnimationFrame();
        }
        return true;
    }

    return true;
}

BrowseCommand::BrowseCommand() : pImpl(std::make_unique<Impl>()) {}
BrowseCommand::~BrowseCommand() = default;

std::string BrowseCommand::getName() const {
    return "browse";
}

std::string BrowseCommand::getDescription() const {
    return "Browse documents with FTXUI interface";
}

void BrowseCommand::registerCommand(CLI::App& app, YamsCLI* cli) {
    auto* cmd = app.add_subcommand("browse", getDescription());
    cmd->callback([this, cli]() {
        pImpl->setCli(cli);
        auto result = execute();
        if (!result) {
            spdlog::error("Browse command failed: {}", result.error().message);
            exit(1);
        }
    });
}

Result<void> BrowseCommand::execute() {
    if (!isatty(STDIN_FILENO) || !isatty(STDOUT_FILENO)) {
        return Result<void>(
            Error(ErrorCode::InvalidOperation, "browse command requires an interactive terminal"));
    }

    try {
        return pImpl->run();
    } catch (const std::exception& ex) {
        spdlog::error("Browse command exception: {}", ex.what());
        return Result<void>(
            Error(ErrorCode::InternalError, std::string("Browse command failed: ") + ex.what()));
    }
}

std::unique_ptr<ICommand> createBrowseCommand() {
    return std::make_unique<BrowseCommand>();
}

} // namespace yams::cli
