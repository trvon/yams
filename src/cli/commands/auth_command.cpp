#include <yams/cli/command.h>
#include <yams/cli/yams_cli.h>
#include <spdlog/spdlog.h>
#include <iostream>

namespace yams::cli {

class AuthCommand : public ICommand {
public:
    std::string getName() const override { return "auth"; }
    
    std::string getDescription() const override {
        return "Manage authentication keys and tokens";
    }
    
    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;
        
        auto* cmd = app.add_subcommand("auth", getDescription());
        cmd->require_subcommand();
        
        // Keygen subcommand
        auto* keygenCmd = cmd->add_subcommand("keygen", "Generate authentication keys");
        keygenCmd->add_option("--type", keyType_, "Key type (ed25519, rsa)")
            ->default_val("ed25519")
            ->check(CLI::IsMember({"ed25519", "rsa"}));
        keygenCmd->add_option("--output", outputPath_, "Output directory for keys")
            ->default_val("~/.yams/keys/");
        keygenCmd->add_flag("--force", force_, "Overwrite existing keys");
        keygenCmd->callback([this]() {
            spdlog::info("Auth keygen command - Coming in Phase 2");
            spdlog::info("This will generate {} keys in: {}", keyType_, outputPath_);
            std::cout << "Status: Phase 2 - Not yet implemented\n";
            std::cout << "Expected usage: yams auth keygen [--type ed25519|rsa] [--output path]\n";
            std::cout << "\nNote: The 'init' command already generates Ed25519 keys.\n";
            std::cout << "This command will provide additional key management capabilities.\n";
            std::exit(0);
        });
        
        // List-keys subcommand
        auto* listKeysCmd = cmd->add_subcommand("list-keys", "List authentication keys");
        listKeysCmd->add_flag("--verbose", verbose_, "Show detailed key information");
        listKeysCmd->add_option("--format", format_, "Output format (table, json)")
            ->default_val("table")
            ->check(CLI::IsMember({"table", "json"}));
        listKeysCmd->callback([this]() {
            spdlog::info("Auth list-keys command - Coming in Phase 2");
            std::cout << "Status: Phase 2 - Not yet implemented\n";
            std::cout << "Expected usage: yams auth list-keys [--verbose] [--format table|json]\n";
            std::cout << "\nThis will list:\n";
            std::cout << "  - Ed25519 keys\n";
            std::cout << "  - RSA keys\n";
            std::cout << "  - API keys\n";
            std::cout << "  - Key fingerprints and creation dates\n";
            std::exit(0);
        });
        
        // Revoke subcommand
        auto* revokeCmd = cmd->add_subcommand("revoke", "Revoke an authentication key");
        revokeCmd->add_option("key-id", keyId_, "Key ID or fingerprint to revoke")
            ->required();
        revokeCmd->add_flag("--force", force_, "Skip confirmation prompt");
        revokeCmd->callback([this]() {
            spdlog::info("Auth revoke command - Coming in Phase 2");
            spdlog::info("This will revoke key: {}", keyId_);
            std::cout << "Status: Phase 2 - Not yet implemented\n";
            std::cout << "Expected usage: yams auth revoke <key-id> [--force]\n";
            std::cout << "\nThis will:\n";
            std::cout << "  - Mark the key as revoked\n";
            std::cout << "  - Invalidate associated tokens\n";
            std::cout << "  - Update revocation list\n";
            std::exit(0);
        });
        
        // Token subcommand (JWT generation)
        auto* tokenCmd = cmd->add_subcommand("token", "Generate JWT token");
        tokenCmd->add_option("--key", keyPath_, "Private key path for signing")
            ->default_val("~/.yams/keys/ed25519.pem");
        tokenCmd->add_option("--validity", validity_, "Token validity duration (e.g., '24h', '7d')")
            ->default_val("24h");
        tokenCmd->add_option("--claims", claims_, "Additional JWT claims (JSON)")
            ->default_val("{}");
        tokenCmd->callback([this]() {
            spdlog::info("Auth token command - Coming in Phase 2");
            spdlog::info("This will generate JWT with validity: {}", validity_);
            std::cout << "Status: Phase 2 - Not yet implemented\n";
            std::cout << "Expected usage: yams auth token [--key path] [--validity duration] [--claims json]\n";
            std::cout << "\nThis will generate a JWT token for API access.\n";
            std::exit(0);
        });
        
        // API key subcommand
        auto* apiKeyCmd = cmd->add_subcommand("api-key", "Generate API key");
        apiKeyCmd->add_option("--name", keyName_, "API key name/description")
            ->required();
        apiKeyCmd->add_option("--permissions", permissions_, "Comma-separated permissions")
            ->default_val("read,write");
        apiKeyCmd->add_option("--expires", expiry_, "Expiry date (ISO 8601 or 'never')")
            ->default_val("never");
        apiKeyCmd->callback([this]() {
            spdlog::info("Auth api-key command - Coming in Phase 2");
            spdlog::info("This will generate API key: {}", keyName_);
            std::cout << "Status: Phase 2 - Not yet implemented\n";
            std::cout << "Expected usage: yams auth api-key --name <name> [--permissions read,write] [--expires date]\n";
            std::cout << "\nThis will:\n";
            std::cout << "  - Generate a secure random API key\n";
            std::cout << "  - Store it with metadata\n";
            std::cout << "  - Return the key (shown only once)\n";
            std::exit(0);
        });
    }
    
    Result<void> execute() override {
        // This shouldn't be reached as subcommands handle execution
        return Error{ErrorCode::NotImplemented, "Auth command requires a subcommand"};
    }
    
private:
    YamsCLI* cli_ = nullptr;
    std::string keyType_;
    std::string outputPath_;
    std::string keyId_;
    std::string keyPath_;
    std::string validity_;
    std::string claims_;
    std::string keyName_;
    std::string permissions_;
    std::string expiry_;
    std::string format_;
    bool force_ = false;
    bool verbose_ = false;
};

// Factory function
std::unique_ptr<ICommand> createAuthCommand() {
    return std::make_unique<AuthCommand>();
}

} // namespace yams::cli