#include <spdlog/spdlog.h>
#include <algorithm>
#include <cctype>
#include <yams/extraction/text_extractor.h>

namespace yams::extraction {

// Constructor - built-in extractors are registered by their own TUs via REGISTER_EXTRACTOR
TextExtractorFactory::TextExtractorFactory() {
    // Log all registered extensions
    auto exts = supportedExtensions();
    if (!exts.empty()) {
        std::string extList;
        for (const auto& e : exts) {
            if (!extList.empty())
                extList += ", ";
            extList += e;
        }
        spdlog::debug("TextExtractorFactory initialized with extensions: {}", extList);
    }
}

TextExtractorFactory& TextExtractorFactory::instance() {
    static TextExtractorFactory instance;
    return instance;
}

std::unique_ptr<ITextExtractor> TextExtractorFactory::create(const std::string& extension) {
    std::string ext = extension;
    std::transform(ext.begin(), ext.end(), ext.begin(), ::tolower);

    std::lock_guard<std::mutex> lock(mutex_);
    auto it = extractors_.find(ext);
    if (it != extractors_.end()) {
        return it->second();
    }
    return nullptr;
}

std::unique_ptr<ITextExtractor>
TextExtractorFactory::createForFile(const std::filesystem::path& path) {
    return create(path.extension().string());
}

void TextExtractorFactory::registerExtractor(const std::vector<std::string>& extensions,
                                             ExtractorCreator creator) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& ext : extensions) {
        std::string normalized = ext;
        std::transform(normalized.begin(), normalized.end(), normalized.begin(), ::tolower);
        extractors_[normalized] = creator;
    }
}

std::vector<std::string> TextExtractorFactory::supportedExtensions() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string> extensions;
    extensions.reserve(extractors_.size());
    for (const auto& [ext, _] : extractors_) {
        extensions.push_back(ext);
    }
    std::sort(extensions.begin(), extensions.end());
    return extensions;
}

bool TextExtractorFactory::isSupported(const std::string& extension) const {
    std::string ext = extension;
    std::transform(ext.begin(), ext.end(), ext.begin(), ::tolower);

    std::lock_guard<std::mutex> lock(mutex_);
    return extractors_.find(ext) != extractors_.end();
}

// (utility implementations moved to src/extraction/text_extractor_utils.cpp)

} // namespace yams::extraction
