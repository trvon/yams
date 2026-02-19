#pragma once

#include <optional>
#include <string>

#include <yams/daemon/components/ConfigResolver.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/vector/document_chunker.h>

namespace yams::daemon::embed {

struct EmbedSourceDoc {
    std::string hash;
    std::string extractedText;
    std::string fileName;
    std::string filePath;
    std::string mimeType;
};

std::optional<InternalEventBus::EmbedPreparedDoc>
prepareEmbedPreparedDoc(const EmbedSourceDoc& src, yams::vector::DocumentChunker& chunker,
                        const ConfigResolver::EmbeddingSelectionPolicy& selectionPolicy);

} // namespace yams::daemon::embed
