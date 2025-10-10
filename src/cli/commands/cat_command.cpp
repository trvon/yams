#include <spdlog/spdlog.h>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <vector>
#include <yams/app/services/retrieval_service.h>
#include <yams/app/services/services.hpp>
#include <yams/cli/command.h>
#include <yams/cli/yams_cli.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/extraction/html_text_extractor.h>
#include <yams/extraction/text_extractor.h>
#include <yams/profiling.h>

namespace yams::cli {

using yams::app::services::utils::normalizeLookupPath;

namespace {

bool looksLikeHashPrefix(const std::string& input) {
    if (input.size() < 6 || input.size() > 64)
        return false;
    return std::all_of(input.begin(), input.end(),
                       [](unsigned char c) { return std::isxdigit(c) != 0; });
}

} // namespace

class CatCommand : public ICommand {
public:
    std::string getName() const override { return "cat"; }

    std::string getDescription() const override { return "Display document content to stdout"; }

    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;

        auto* cmd = app.add_subcommand("cat", getDescription());

        cmd->add_option("target", target_, "Document hash or path to display")
            ->type_name("HASH|PATH")
            ->required();
        // Disambiguation flags: select newest/oldest when multiple matches exist
        cmd->add_flag(
            "--latest", getLatest_,
            "Select the most recently indexed match when multiple documents share the same name");
        cmd->add_flag(
            "--oldest", getOldest_,
            "Select the oldest indexed match when multiple documents share the same name");

        // Add flag for raw content output
        cmd->add_flag("--raw", raw_, "Output raw content without text extraction");

        // No output option - cat always goes to stdout
        // This is intentional for piping and viewing content directly

        cmd->callback([this]() {
            auto result = execute();
            if (!result) {
                spdlog::error("Cat failed: {}", result.error().message);
                throw CLI::RuntimeError(1);
            }
        });
    }

    Result<void> execute() override {
        YAMS_ZONE_SCOPED_N("CatCommand::execute");

        try {
            std::optional<std::string> hashCandidate;

            if (!hash_.empty()) {
                hashCandidate = hash_;
            } else if (!target_.empty() && name_.empty()) {
                if (looksLikeHashPrefix(target_)) {
                    hashCandidate = target_;
                } else {
                    name_ = target_;
                }
            }

            yams::app::services::RetrievalService rsvc;
            yams::app::services::RetrievalOptions ropts;
            if (cli_->hasExplicitDataDir()) {
                ropts.explicitDataDir = cli_->getDataPath();
            }
            ropts.requestTimeoutMs = 60000;

            auto renderResponse = [&](const yams::daemon::GetResponse& resp) -> Result<void> {
                if (!resp.hasContent) {
                    return Error{ErrorCode::NotFound, "Document content not found"};
                }
                std::cout.write(resp.content.data(), resp.content.size());
                return Result<void>();
            };

            yams::app::services::GetOptions dreq;
            dreq.raw = raw_;
            dreq.extract = !raw_;
            dreq.latest = getLatest_;
            dreq.oldest = getOldest_;

            if (hashCandidate) {
                std::string candidate = *hashCandidate;
                if (candidate.size() != 64) {
                    auto resolved =
                        rsvc.resolveHashPrefix(candidate, getOldest_, getLatest_, ropts);
                    if (!resolved)
                        return resolved.error();
                    candidate = resolved.value();
                }
                if (candidate.size() != 64) {
                    return Error{ErrorCode::InvalidArgument,
                                 "Invalid hash value provided (expected 64 hex characters)."};
                }
                dreq.hash = candidate;
            } else if (!name_.empty()) {
                auto normalized = normalizeLookupPath(name_);
                if (normalized.hasWildcards) {
                    return Error{ErrorCode::InvalidArgument,
                                 "cat does not support wildcard paths: " + name_};
                }
                if (normalized.changed) {
                    name_ = normalized.normalized;
                }

                if (!name_.empty() && name_[0] != '/' && !looksLikeHashPrefix(name_)) {
                    try {
                        name_ = std::filesystem::weakly_canonical(name_).string();
                    } catch (const std::filesystem::filesystem_error&) {
                        // Not a path, treat as name
                    }
                }

                dreq.name = name_;
                dreq.byName = true;

                auto smart = rsvc.getByNameSmart(name_, getOldest_, !raw_, /*useSession=*/false,
                                                 std::string{}, ropts);
                if (smart) {
                    return renderResponse(smart.value());
                }
                spdlog::debug("cat: getByNameSmart fallback ({})", smart.error().message);
            } else {
                return Error{ErrorCode::InvalidArgument, "No document specified"};
            }

            auto gres = rsvc.get(dreq, ropts);
            if (!gres) {
                if (!name_.empty() && std::filesystem::exists(name_)) {
                    std::ifstream file(name_, std::ios::binary);
                    if (file) {
                        std::cout << file.rdbuf();
                        return Result<void>();
                    }
                }
                return gres.error();
            }

            return renderResponse(gres.value());

        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string("Unexpected error: ") + e.what()};
        }
    }

private:
    YamsCLI* cli_ = nullptr;
    std::string hash_;
    std::string name_;
    std::string target_;
    bool raw_ = false;       // Flag to output raw content without text extraction
    bool getLatest_ = false; // When ambiguous, select newest
    bool getOldest_ = false; // When ambiguous, select oldest
};

// Factory function
std::unique_ptr<ICommand> createCatCommand() {
    return std::make_unique<CatCommand>();
}

} // namespace yams::cli
