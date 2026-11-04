#include <yams/config/static_embedding_table.h>

#include <system_error>

namespace yams::config {

std::optional<std::filesystem::path>
findStaticEmbeddingTable(const std::optional<std::filesystem::path>& configured,
                         const std::filesystem::path&) {
    // Model presence is not consent: release packages and older automatic init
    // flows can leave tables behind. Explicit download/configuration records the
    // chosen table; never replace a missing choice with an unrelated model.
    if (!configured || configured->empty()) {
        return std::nullopt;
    }
    std::error_code error;
    if (!std::filesystem::is_regular_file(*configured, error)) {
        return std::nullopt;
    }
    return configured;
}

} // namespace yams::config
