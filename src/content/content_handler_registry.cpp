#include <spdlog/spdlog.h>
#include <algorithm>
#include <array>
#include <functional>
#include <ranges>
#include <yams/content/archive_content_handler.h>
#include <yams/content/audio_content_handler.h>
#include <yams/content/binary_content_handler.h>
#include <yams/content/content_handler_registry.h>
#include <yams/content/image_content_handler.h>
#include <yams/content/pdf_content_handler.h>
#include <yams/content/text_content_handler.h>
#if defined(YAMS_HAVE_FFPROBE) || defined(YAMS_HAVE_MEDIAINFO)
#include <yams/content/video_content_handler.h>
#endif
#include <yams/detection/file_type_detector.h>

namespace yams::content {

ContentHandlerRegistry& ContentHandlerRegistry::instance() {
    static ContentHandlerRegistry instance;
    return instance;
}

Result<void> ContentHandlerRegistry::registerHandler(std::shared_ptr<IContentHandler> handler) {
    if (!handler) {
        return Error{ErrorCode::InvalidArgument, "Cannot register null handler"};
    }

    const std::string name = handler->name();
    if (name.empty()) {
        return Error{ErrorCode::InvalidArgument, "Handler name cannot be empty"};
    }

    std::unique_lock lock(mutex_);

    // Check if handler already exists
    if (handlers_.find(name) != handlers_.end()) {
        return Error{ErrorCode::InvalidState, "Handler already registered: " + name};
    }

    // Register handler
    handlers_[name] = handler;

    // Register MIME type mappings
    for (const auto& mimeType : handler->supportedMimeTypes()) {
        mimeToHandler_.emplace(mimeType, name);
    }

    spdlog::debug("Registered content handler: {}", name);
    return Result<void>();
}

Result<void> ContentHandlerRegistry::unregisterHandler(const std::string& name) {
    std::unique_lock lock(mutex_);

    auto it = handlers_.find(name);
    if (it == handlers_.end()) {
        return Error{ErrorCode::NotFound, "Handler not found: " + name};
    }

    // Remove MIME type mappings
    for (auto mit = mimeToHandler_.begin(); mit != mimeToHandler_.end();) {
        if (mit->second == name) {
            mit = mimeToHandler_.erase(mit);
        } else {
            ++mit;
        }
    }

    // Remove handler
    handlers_.erase(it);

    spdlog::debug("Unregistered content handler: {}", name);
    return Result<void>();
}

std::shared_ptr<IContentHandler>
ContentHandlerRegistry::getHandler(const detection::FileSignature& signature) const {
    std::shared_lock lock(mutex_);

    // Get all compatible handlers
    std::vector<std::pair<int, std::shared_ptr<IContentHandler>>> candidates;

    for (const auto& [name, handler] : handlers_) {
        if (handler->canHandle(signature)) {
            candidates.emplace_back(handler->priority(), handler);
        }
    }

    // Sort by priority (highest first)
    std::sort(candidates.begin(), candidates.end(),
              [](const auto& a, const auto& b) { return a.first > b.first; });

    // Return highest priority handler
    return candidates.empty() ? nullptr : candidates.front().second;
}

std::shared_ptr<IContentHandler>
ContentHandlerRegistry::getHandlerByName(const std::string& name) const {
    std::shared_lock lock(mutex_);

    auto it = handlers_.find(name);
    return (it != handlers_.end()) ? it->second : nullptr;
}

std::vector<std::shared_ptr<IContentHandler>>
ContentHandlerRegistry::getCompatibleHandlers(const detection::FileSignature& signature) const {
    std::shared_lock lock(mutex_);

    // C++20: Filter and transform handlers
    std::vector<std::pair<int, std::shared_ptr<IContentHandler>>> compatibleHandlers;
    for (const auto& [name, handler] : handlers_) {
        if (handler->canHandle(signature)) {
            compatibleHandlers.emplace_back(handler->priority(), handler);
        }
    }

    // Sort by priority (highest first)
    std::ranges::sort(compatibleHandlers,
                      [](const auto& a, const auto& b) { return a.first > b.first; });

    // Extract handlers from pairs
    std::vector<std::shared_ptr<IContentHandler>> result;
    result.reserve(compatibleHandlers.size());
    for (const auto& [priority, handler] : compatibleHandlers) {
        result.push_back(handler);
    }
    return result;
}

std::unordered_map<std::string, std::shared_ptr<IContentHandler>>
ContentHandlerRegistry::getAllHandlers() const {
    std::shared_lock lock(mutex_);
    return handlers_;
}

std::vector<std::string> ContentHandlerRegistry::getHandlerNames() const {
    std::shared_lock lock(mutex_);

    // C++20: Extract and sort handler names
    std::vector<std::string> names;
    names.reserve(handlers_.size());
    for (const auto& [name, handler] : handlers_) {
        names.push_back(name);
    }

    std::ranges::sort(names);
    return names;
}

bool ContentHandlerRegistry::hasHandler(const std::string& name) const {
    std::shared_lock lock(mutex_);
    return handlers_.find(name) != handlers_.end();
}

void ContentHandlerRegistry::clear() {
    std::unique_lock lock(mutex_);
    handlers_.clear();
    mimeToHandler_.clear();
    spdlog::debug("Cleared all content handlers");
}

void ContentHandlerRegistry::initializeDefaultHandlers() {
    spdlog::info("Initializing default content handlers with C++20 features");

    // First, initialize FileTypeDetector with magic_numbers.json
    // This ensures all handlers can use data-driven file type detection
    auto result = detection::FileTypeDetector::initializeWithMagicNumbers();
    if (!result) {
        spdlog::warn("Failed to initialize FileTypeDetector: {}", result.error().message);
    } else {
        spdlog::info("FileTypeDetector initialized successfully");
    }

    // C++20: Define handler factories with priority order (highest to lowest)
    // Using structured bindings and lambda factories for clean initialization
    using HandlerFactory = std::function<std::shared_ptr<IContentHandler>()>;
    std::vector<std::pair<std::string, HandlerFactory>> handlerFactories = {
        {"ImageContentHandler", []() { return std::shared_ptr<IContentHandler>(createImageHandler().release()); }},
#if defined(YAMS_HAVE_FFPROBE) || defined(YAMS_HAVE_MEDIAINFO)
        {"VideoContentHandler", []() { return std::shared_ptr<IContentHandler>(createVideoHandler().release()); }},
#endif
        {"AudioContentHandler", []() { return std::shared_ptr<IContentHandler>(createAudioHandler().release()); }},
        {"ArchiveContentHandler", []() { return std::shared_ptr<IContentHandler>(createArchiveHandler().release()); }},
        {"TextContentHandler", []() { return std::make_shared<TextContentHandler>(); }},
        {"PdfContentHandler", []() { return std::make_shared<PdfContentHandler>(); }},
        {"BinaryContentHandler", []() { return std::make_shared<BinaryContentHandler>(); }}
    };

    // C++20: Use ranges to register all handlers
    size_t successCount = 0;
    size_t failureCount = 0;

    for (const auto& [name, factory] : handlerFactories) {
        try {
            auto handler = factory();
            if (!handler) {
                spdlog::error("Factory for {} returned null handler", name);
                failureCount++;
                continue;
            }

            auto registerResult = registerHandler(std::move(handler));
            if (!registerResult) {
                spdlog::error("Failed to register {}: {}", name, registerResult.error().message);
                failureCount++;
            } else {
                spdlog::info("Successfully registered {} (priority: {})", name,
                             registerResult ? handlers_[name]->priority() : 0);
                successCount++;
            }
        } catch (const std::exception& e) {
            spdlog::error("Exception while creating {}: {}", name, e.what());
            failureCount++;
        }
    }

    // Log final registration summary
    spdlog::info("Handler registration complete: {} successful, {} failed", successCount,
                 failureCount);

    // Validate that we have essential handlers
    const auto essentialHandlers = std::array{"BinaryContentHandler", "TextContentHandler"};
    const bool hasEssentials = std::ranges::all_of(
        essentialHandlers, [this](const std::string& name) { return hasHandler(name); });

    if (!hasEssentials) {
        spdlog::warn("Missing essential handlers - some file types may not be processed");
    }

    // Log media handler availability
    const auto mediaHandlers = std::array{"ImageContentHandler", "VideoContentHandler",
                                          "AudioContentHandler", "ArchiveContentHandler"};
    const auto availableMedia = std::ranges::count_if(
        mediaHandlers, [this](const std::string& name) { return hasHandler(name); });

    spdlog::info("Media handlers available: {}/{}", availableMedia, mediaHandlers.size());
}

ContentHandlerRegistry::RegistryStats ContentHandlerRegistry::getStats() const {
    std::shared_lock lock(mutex_);

    RegistryStats stats;
    stats.totalHandlers = handlers_.size();

    // C++20: Extract handler names
    stats.handlerNames.reserve(handlers_.size());
    for (const auto& [name, handler] : handlers_) {
        stats.handlerNames.push_back(name);
    }

    std::ranges::sort(stats.handlerNames);

    // Categorize handlers based on supported MIME types
    for (const auto& [name, handler] : handlers_) {
        auto mimeTypes = handler->supportedMimeTypes();

        // C++20: Use ranges algorithms for MIME type checking
        if (std::ranges::any_of(mimeTypes, [](const auto& mime) {
                return mime.find("text/") != std::string::npos;
            })) {
            stats.textHandlers++;
        } else if (std::ranges::any_of(mimeTypes, [](const auto& mime) {
                       return mime.find("image/") != std::string::npos;
                   })) {
            stats.imageHandlers++;
        } else if (std::ranges::any_of(mimeTypes, [](const auto& mime) {
                       return mime.find("audio/") != std::string::npos;
                   })) {
            stats.audioHandlers++;
        } else if (std::ranges::any_of(mimeTypes, [](const auto& mime) {
                       return mime.find("video/") != std::string::npos;
                   })) {
            stats.videoHandlers++;
        } else if (std::ranges::any_of(mimeTypes, [](const auto& mime) {
                       return mime.find("application/pdf") != std::string::npos ||
                              mime.find("application/vnd.") != std::string::npos;
                   })) {
            stats.documentHandlers++;
        } else if (std::ranges::any_of(mimeTypes, [](const auto& mime) {
                       return mime.find("application/zip") != std::string::npos ||
                              mime.find("application/x-rar") != std::string::npos ||
                              mime.find("application/x-tar") != std::string::npos ||
                              mime.find("application/x-7z") != std::string::npos ||
                              mime.find("application/gzip") != std::string::npos;
                   })) {
            stats.archiveHandlers++;
        } else if (std::ranges::any_of(mimeTypes, [](const auto& mime) {
                       return mime == "*/*" ||
                              mime.find("application/octet-stream") != std::string::npos;
                   })) {
            stats.binaryHandlers++;
        }
    }

    return stats;
}

bool ContentHandlerRegistry::matchesMimePattern(const std::string& pattern,
                                                const std::string& mimeType) const {
    // Handle wildcard patterns
    if (pattern == "*/*") {
        return true;
    }

    // Handle partial wildcards like "text/*"
    if (pattern.back() == '*') {
        std::string prefix = pattern.substr(0, pattern.length() - 1);
        return mimeType.find(prefix) == 0;
    }

    // Exact match
    return pattern == mimeType;
}

} // namespace yams::content