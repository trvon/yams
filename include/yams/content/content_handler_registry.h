#pragma once

#include <concepts>
#include <memory>
#include <ranges>
#include <shared_mutex>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>
#include <yams/content/content_handler.h>
#include <yams/detection/file_type_detector.h>

#include <array>
#include <yams/common/format.h>

namespace yams::content {

/**
 * @brief  concept for content handler validation
 */
template <typename T>
concept ContentHandlerType = requires(T handler, const detection::FileSignature& sig) {
    { handler.name() } -> std::convertible_to<std::string>;
    { handler.priority() } -> std::convertible_to<int>;
    { handler.canHandle(sig) } -> std::same_as<bool>;
    { handler.supportedMimeTypes() } -> std::convertible_to<std::vector<std::string>>;
    requires std::derived_from<T, IContentHandler>;
};

/**
 * @brief concept for handler factories
 */
template <typename T>
concept HandlerFactory = requires {
    { T::create() } -> std::convertible_to<std::unique_ptr<IContentHandler>>;
    typename T::ConfigType;
    {
        T::create(std::declval<typename T::ConfigType>())
    } -> std::convertible_to<std::unique_ptr<IContentHandler>>;
};

/**
 * @brief concept for handler configuration types
 */
template <typename T>
concept HandlerConfigType = requires(T config) {
    requires std::default_initializable<T>;
    requires std::move_constructible<T>;
};

/**
 * @brief Registry for content handlers
 *
 * Manages handler registration and selection based on file type.
 * Thread-safe singleton implementation.
 */
class ContentHandlerRegistry {
public:
    /**
     * @brief Get singleton instance
     * @return Registry instance
     */
    static ContentHandlerRegistry& instance();

    /**
     * @brief Register a handler
     * @param handler Handler instance
     * @return Success or error if handler with same name exists
     */
    Result<void> registerHandler(std::shared_ptr<IContentHandler> handler);

    /**
     * @brief Get handler by name
     * @param name Handler name
     * @return Handler or nullptr
     */
    [[nodiscard]] std::shared_ptr<IContentHandler> getHandlerByName(const std::string& name) const;

    /**
     * @brief List all registered handlers
     * @return Map of handler names to handlers
     */
    [[nodiscard]] std::unordered_map<std::string, std::shared_ptr<IContentHandler>>
    getAllHandlers() const;

    /**
     * @brief Check if a handler is registered
     * @param name Handler name
     * @return True if registered
     */
    [[nodiscard]] bool hasHandler(const std::string& name) const;

    /**
     * @brief Clear all registered handlers
     */
    void clear();

    /**
     * @brief Initialize with default handlers
     *
     * Registers standard handlers:
     * - ImageContentHandler (priority 200)
     * - VideoContentHandler (priority 150)
     * - AudioContentHandler (priority 120)
     * - ArchiveContentHandler (priority 100)
     * - TextContentHandler
     * - BinaryContentHandler (fallback)
     */
    void initializeDefaultHandlers();

    /**
     * @brief Get statistics about registered handlers
     */
    struct RegistryStats {
        size_t totalHandlers = 0;
        size_t textHandlers = 0;
        size_t imageHandlers = 0;
        size_t audioHandlers = 0;
        size_t videoHandlers = 0;
        size_t documentHandlers = 0;
        size_t archiveHandlers = 0;
        size_t binaryHandlers = 0;
        std::vector<std::string> handlerNames;
    };

    [[nodiscard]] RegistryStats getStats() const;

private:
    ContentHandlerRegistry() = default;
    ~ContentHandlerRegistry() = default;

    // Disable copy and move
    ContentHandlerRegistry(const ContentHandlerRegistry&) = delete;
    ContentHandlerRegistry& operator=(const ContentHandlerRegistry&) = delete;
    ContentHandlerRegistry(ContentHandlerRegistry&&) = delete;
    ContentHandlerRegistry& operator=(ContentHandlerRegistry&&) = delete;

    // Thread-safe storage
    mutable std::shared_mutex mutex_;
    std::unordered_map<std::string, std::shared_ptr<IContentHandler>> handlers_;

    // MIME type to handler mapping for quick lookup
    std::unordered_multimap<std::string, std::string> mimeToHandler_;
};

} // namespace yams::content