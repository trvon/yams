#pragma once

#include <concepts>
#include <format>
#include <memory>
#include <ranges>
#include <shared_mutex>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>
#include <yams/content/content_handler.h>
#include <yams/detection/file_type_detector.h>

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
     * @brief Unregister a handler
     * @param name Handler name to remove
     * @return Success or error if handler not found
     */
    Result<void> unregisterHandler(const std::string& name);

    /**
     * @brief Get best handler for file
     * @param signature File type signature
     * @return Best matching handler or nullptr
     */
    [[nodiscard]] std::shared_ptr<IContentHandler>
    getHandler(const detection::FileSignature& signature) const;

    /**
     * @brief Get handler by name
     * @param name Handler name
     * @return Handler or nullptr
     */
    [[nodiscard]] std::shared_ptr<IContentHandler> getHandlerByName(const std::string& name) const;

    /**
     * @brief Get all compatible handlers
     * @param signature File type signature
     * @return Vector of compatible handlers, sorted by priority (highest first)
     */
    [[nodiscard]] std::vector<std::shared_ptr<IContentHandler>>
    getCompatibleHandlers(const detection::FileSignature& signature) const;

    /**
     * @brief List all registered handlers
     * @return Map of handler names to handlers
     */
    [[nodiscard]] std::unordered_map<std::string, std::shared_ptr<IContentHandler>>
    getAllHandlers() const;

    /**
     * @brief Get all registered handler names
     * @return Vector of handler names
     */
    [[nodiscard]] std::vector<std::string> getHandlerNames() const;

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
     * - PdfContentHandler
     * - BinaryContentHandler (fallback)
     */
    void initializeDefaultHandlers();

    /**
     * @brief Register a handler using factory with default config
     * @tparam HandlerType Handler class type
     * @tparam FactoryFunc Factory function type
     * @param name Handler name for registration
     * @param factory Factory function that creates the handler
     * @return Success or error if registration fails
     */
    template <ContentHandlerType HandlerType, typename FactoryFunc>
    Result<void> registerHandlerWithFactory(const std::string& name, FactoryFunc&& factory) {
        static_assert(std::is_invocable_r_v<std::unique_ptr<IContentHandler>, FactoryFunc>,
                      "Factory must return std::unique_ptr<IContentHandler>");

        try {
            auto handler = std::invoke(std::forward<FactoryFunc>(factory));
            if (!handler) {
                return Error{ErrorCode::InvalidArgument,
                             std::format("Factory for {} returned null handler", name)};
            }

            return registerHandler(std::move(handler));
        } catch (const std::exception& e) {
            return Error{ErrorCode::InternalError,
                         std::format("Failed to create handler {}: {}", name, e.what())};
        }
    }

    /**
     * @brief Register a handler with custom configuration
     * @tparam HandlerType Handler class type
     * @tparam ConfigType Configuration type
     * @tparam FactoryFunc Factory function type
     * @param name Handler name for registration
     * @param config Configuration for the handler
     * @param factory Factory function that accepts config and creates handler
     * @return Success or error if registration fails
     */
    template <ContentHandlerType HandlerType, HandlerConfigType ConfigType, typename FactoryFunc>
    Result<void> registerHandlerWithConfig(const std::string& name, ConfigType config,
                                           FactoryFunc&& factory) {
        static_assert(
            std::is_invocable_r_v<std::unique_ptr<IContentHandler>, FactoryFunc, ConfigType>,
            "Factory must accept ConfigType and return std::unique_ptr<IContentHandler>");

        try {
            auto handler = std::invoke(std::forward<FactoryFunc>(factory), std::move(config));
            if (!handler) {
                return Error{ErrorCode::InvalidArgument,
                             std::format("Factory for {} returned null handler", name)};
            }

            return registerHandler(std::move(handler));
        } catch (const std::exception& e) {
            return Error{
                ErrorCode::InternalError,
                std::format("Failed to create handler {} with config: {}", name, e.what())};
        }
    }

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

        // C++20: Helper methods for statistics analysis
        [[nodiscard]] double getHandlerTypeRatio(size_t typeCount) const noexcept {
            return totalHandlers > 0 ? static_cast<double>(typeCount) / totalHandlers : 0.0;
        }

        [[nodiscard]] bool hasMediaHandlers() const noexcept {
            return audioHandlers > 0 || videoHandlers > 0 || imageHandlers > 0;
        }

        [[nodiscard]] std::string getMostCommonType() const noexcept {
            const auto counts = std::array{std::make_pair(textHandlers, "text"),
                                           std::make_pair(imageHandlers, "image"),
                                           std::make_pair(audioHandlers, "audio"),
                                           std::make_pair(videoHandlers, "video"),
                                           std::make_pair(documentHandlers, "document"),
                                           std::make_pair(archiveHandlers, "archive"),
                                           std::make_pair(binaryHandlers, "binary")};

            auto maxElement = std::max_element(
                counts.begin(), counts.end(), [](const auto& a, const auto& b) { return a.first < b.first; });

            return maxElement != counts.end() ? maxElement->second : "unknown";
        }
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

    // Helper to match MIME type patterns
    [[nodiscard]] bool matchesMimePattern(const std::string& pattern,
                                          const std::string& mimeType) const;
};

} // namespace yams::content