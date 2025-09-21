#pragma once

#include <memory>
#include <string>
#include <vector>
#include <yams/cli/command.h>
#include <yams/core/types.h>
#include <yams/metadata/metadata_repository.h>

namespace yams {
namespace api {
class IContentStore; // Forward declaration
}
} // namespace yams

namespace yams::cli {

using yams::Result;

class UpdateCommand : public ICommand {
public:
    // Constructors for testing
    UpdateCommand() = default;
    UpdateCommand(std::shared_ptr<metadata::MetadataRepository> metadataRepo,
                  std::shared_ptr<api::IContentStore> contentStore)
        : metadataRepo_(metadataRepo), contentStore_(contentStore) {}

    // ICommand interface
    std::string getName() const override;
    std::string getDescription() const override;
    void registerCommand(CLI::App& app, YamsCLI* cli) override;
    Result<void> execute() override;
    // Async path to avoid blocking the client pipeline
    boost::asio::awaitable<Result<void>> executeAsync() override;

    // Testing interface
    void setHash(const std::string& hash) { hash_ = hash; }
    void setName(const std::string& name) { name_ = name; }
    void setKey(const std::string& key) { metadata_.push_back(key + "="); }
    void setValue(const std::string& value) {
        if (!metadata_.empty() && metadata_.back().back() == '=') {
            metadata_.back() += value;
        }
    }
    void parseArguments(const std::vector<std::string>& args);

private:
    Result<void> executeLocal();
    YamsCLI* cli_ = nullptr;
    std::string hash_;
    std::string name_;
    std::vector<std::string> metadata_;
    bool verbose_ = false;
    bool latest_ = false;
    bool oldest_ = false;
    bool noSession_ = false;

    // Dependencies for testing
    std::shared_ptr<metadata::MetadataRepository> metadataRepo_;
    std::shared_ptr<api::IContentStore> contentStore_;

    Result<metadata::MetadataValue> parseMetadataValue(const std::string& value);
    Result<metadata::DocumentInfo> resolveNameToDocument(const std::string& name);
    Result<std::string> resolveNameToHashSmart(const std::string& name);
};

// Factory function
std::unique_ptr<ICommand> createUpdateCommand();

} // namespace yams::cli
