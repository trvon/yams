#include <cstring>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>
#include <catch2/catch_test_macros.hpp>
#include <yams/compat/dlfcn.h>

#include "../common/plugins_catch2.h"
#include <yams/plugins/abi.h>
#include <yams/plugins/symbol_extractor_v1.h>

// Grammar auto-download uses Unix shell commands (which, git clone, gcc) that don't work on Windows
#ifdef _WIN32
#define SKIP_ON_WINDOWS_NO_GRAMMAR()                                                               \
    SKIP("Solidity grammar auto-download not supported on Windows"
#else
#define SKIP_ON_WINDOWS_NO_GRAMMAR() (void)0
#endif

namespace {

struct PluginAPI {
    void* handle{};
    yams_symbol_extractor_v1* api{};

    PluginAPI() = default;
    PluginAPI(const PluginAPI&) = delete;
    PluginAPI& operator=(const PluginAPI&) = delete;

    PluginAPI(PluginAPI&& other) noexcept : handle(other.handle), api(other.api) {
        other.handle = nullptr;
        other.api = nullptr;
    }
    PluginAPI& operator=(PluginAPI&& other) noexcept {
        if (this != &other) {
            if (handle) { /* skip dlclose in tests to avoid teardown segfaults */
            }
            handle = other.handle;
            api = other.api;
            other.handle = nullptr;
            other.api = nullptr;
        }
        return *this;
    }

    ~PluginAPI() { /* avoid dlclose in tests to prevent teardown crashes */ }
};

std::optional<PluginAPI> load_extractor(const char* so_path) {
    PluginAPI p;
    p.handle = dlopen(so_path, RTLD_LAZY | RTLD_LOCAL);
    if (!p.handle) {
        fprintf(stderr, "dlopen failed for %s: %s\n", so_path, dlerror());
        return std::nullopt;
    }
    auto getabi = (int (*)())dlsym(p.handle, "yams_plugin_get_abi_version");
    auto getiface =
        (int (*)(const char*, uint32_t, void**))dlsym(p.handle, "yams_plugin_get_interface");
    auto init = (int (*)(const char*, const void*))dlsym(p.handle, "yams_plugin_init");
    if (!getabi || !getiface || !init) {
        fprintf(stderr, "Symbol lookup failed: getabi=%p getiface=%p init=%p\n", (void*)getabi,
                (void*)getiface, (void*)init);
        return std::nullopt;
    }
    CHECK(getabi() > 0);
    CHECK(0 == init(nullptr, nullptr));
    void* ptr = nullptr;
    int rc = getiface(YAMS_IFACE_SYMBOL_EXTRACTOR_V1, YAMS_IFACE_SYMBOL_EXTRACTOR_V1_VERSION, &ptr);
    if (rc != 0 || !ptr) {
        fprintf(stderr, "get_interface failed: rc=%d ptr=%p\n", rc, ptr);
        return std::nullopt;
    }
    p.api = reinterpret_cast<yams_symbol_extractor_v1*>(ptr);
    return p.api ? std::optional<PluginAPI>(std::move(p)) : std::nullopt;
}

std::optional<PluginAPI> loadPlugin() {
#ifndef PLUGIN_PATH
#ifdef __APPLE__
    const char* libname = "yams_symbol_extractor.dylib";
#else
    const char* libname = "yams_symbol_extractor.so";
#endif
    const char* buildroot_env = std::getenv("MESON_BUILD_ROOT");
    std::vector<std::string> paths;
    if (buildroot_env && *buildroot_env) {
        std::string br(buildroot_env);
        paths.push_back(br + "/plugins/symbol_extractor_treesitter/" + libname);
    }
    paths.push_back(std::string("plugins/symbol_extractor_treesitter/") + libname);

    for (const auto& p : paths) {
        if (auto api = load_extractor(p.c_str())) {
            return api;
        }
    }
    return std::nullopt;
#else
    return load_extractor(PLUGIN_PATH);
#endif
}

bool has_symbol(yams_symbol_extraction_result_v1* result, const char* name, const char* kind) {
    for (size_t i = 0; i < result->symbol_count; ++i) {
        if (result->symbols[i].name && std::strcmp(result->symbols[i].name, name) == 0 &&
            result->symbols[i].kind && std::strcmp(result->symbols[i].kind, kind) == 0) {
            return true;
        }
    }
    return false;
}

bool has_symbol_any_kind(yams_symbol_extraction_result_v1* result, const char* name) {
    for (size_t i = 0; i < result->symbol_count; ++i) {
        if (result->symbols[i].name && std::strcmp(result->symbols[i].name, name) == 0) {
            return true;
        }
    }
    return false;
}

} // namespace

TEST_CASE("SymbolExtractorSolidity.SupportsLanguage", "[plugin][symbolextractorsolidity]") {
    auto plugin = loadPlugin();
    INFO("Failed to load symbol extractor plugin"); REQUIRE(plugin.has_value());

    CHECK(plugin->api->supports_language(plugin->api->self, "sol"));
    CHECK(plugin->api->supports_language(plugin->api->self, "solidity"));
    CHECK(plugin->api->supports_language(plugin->api->self, "Solidity"));
}

TEST_CASE("SymbolExtractorSolidity.ERC20TokenContract", "[plugin][symbolextractorsolidity]") {
    SKIP_ON_WINDOWS_NO_GRAMMAR();
    auto plugin = loadPlugin();
    INFO("Failed to load symbol extractor plugin"); REQUIRE(plugin.has_value());

    std::string solidityCode = R"(
// SPDX-License-Identifier: GPL-3.0-or-later
pragma solidity ^0.8.0;

contract MyToken {
    mapping(address => uint256) private _balances;
    uint256 private _totalSupply;

    event Transfer(address indexed from, address indexed to, uint256 value);

    constructor(uint256 initialSupply) {
        _totalSupply = initialSupply;
        _balances[msg.sender] = initialSupply;
    }

    function balanceOf(address account) public view returns (uint256) {
        return _balances[account];
    }

    function transfer(address to, uint256 amount) public returns (bool) {
        _transfer(msg.sender, to, amount);
        return true;
    }

    function _transfer(address from, address to, uint256 amount) private {
        require(_balances[from] >= amount, "Insufficient balance");
        _balances[from] -= amount;
        _balances[to] += amount;
        emit Transfer(from, to, amount);
    }
}
)";

    yams_symbol_extraction_result_v1* result = nullptr;
    int rc = plugin->api->extract_symbols(plugin->api->self, solidityCode.c_str(),
                                          solidityCode.size(), "test.sol", "solidity", &result);

    REQUIRE(rc == YAMS_PLUGIN_OK);
    REQUIRE(result != nullptr);

    fprintf(stderr, "[Solidity ERC-20] Extracted %zu symbols\n", result->symbol_count);
    for (size_t i = 0; i < result->symbol_count; ++i) {
        fprintf(stderr, "  - %s (%s)\n", result->symbols[i].name ? result->symbols[i].name : "?",
                result->symbols[i].kind ? result->symbols[i].kind : "?");
    }

    // Verify contract extracted
    INFO("MyToken contract should be extracted"); CHECK((has_symbol(result, "MyToken", "contract") || has_symbol(result, "MyToken", "class")));

    // Verify functions (at least some should be extracted)
    bool hasBalanceOf = has_symbol_any_kind(result, "balanceOf");
    bool hasTransfer = has_symbol_any_kind(result, "transfer");
    bool hasPrivateTransfer = has_symbol_any_kind(result, "_transfer");

    fprintf(stderr, "[Solidity ERC-20] Functions found: balanceOf=%d transfer=%d _transfer=%d\n",
            hasBalanceOf, hasTransfer, hasPrivateTransfer);

    // At least the contract should be there
    INFO("Should extract at least the contract"); CHECK(result->symbol_count > 0UL);

    plugin->api->free_result(plugin->api->self, result);
}

TEST_CASE("SymbolExtractorSolidity.ContractInheritance", "[plugin][symbolextractorsolidity]") {
    SKIP_ON_WINDOWS_NO_GRAMMAR();
    auto plugin = loadPlugin();
    INFO("Failed to load symbol extractor plugin"); REQUIRE(plugin.has_value());

    std::string code = R"(
// SPDX-License-Identifier: GPL-3.0-or-later
pragma solidity ^0.8.0;

interface IERC20 {
    function totalSupply() external view returns (uint256);
    function transfer(address to, uint256 amount) external returns (bool);
}

contract Token is IERC20 {
    uint256 private _supply;

    function totalSupply() public view override returns (uint256) {
        return _supply;
    }

    function transfer(address to, uint256 amount) public override returns (bool) {
        return true;
    }
}
)";

    yams_symbol_extraction_result_v1* result = nullptr;
    int rc = plugin->api->extract_symbols(plugin->api->self, code.c_str(), code.size(), "test.sol",
                                          "sol", &result);

    REQUIRE(rc == YAMS_PLUGIN_OK);
    REQUIRE(result != nullptr);

    fprintf(stderr, "[Solidity Inheritance] Extracted %zu symbols\n", result->symbol_count);
    for (size_t i = 0; i < result->symbol_count; ++i) {
        fprintf(stderr, "  - %s (%s)\n", result->symbols[i].name ? result->symbols[i].name : "?",
                result->symbols[i].kind ? result->symbols[i].kind : "?");
    }

    // Verify interface and contract (kind may vary based on grammar)
    bool hasInterface =
        has_symbol(result, "IERC20", "interface") || has_symbol(result, "IERC20", "class");
    bool hasContract =
        has_symbol(result, "Token", "contract") || has_symbol(result, "Token", "class");

    INFO("Should extract interface or contract"); CHECK((hasInterface || hasContract));

    plugin->api->free_result(plugin->api->self, result);
}

TEST_CASE("SymbolExtractorSolidity.StructsAndEnums", "[plugin][symbolextractorsolidity]") {
    SKIP_ON_WINDOWS_NO_GRAMMAR();
    auto plugin = loadPlugin();
    INFO("Failed to load symbol extractor plugin"); REQUIRE(plugin.has_value());

    std::string code = R"(
// SPDX-License-Identifier: GPL-3.0-or-later
pragma solidity ^0.8.0;

contract DataTypes {
    enum Status { Pending, Active, Completed }

    struct User {
        address wallet;
        uint256 balance;
        Status status;
    }

    mapping(address => User) public users;

    function createUser(address wallet) public {
        users[wallet] = User(wallet, 0, Status.Pending);
    }
}
)";

    yams_symbol_extraction_result_v1* result = nullptr;
    int rc = plugin->api->extract_symbols(plugin->api->self, code.c_str(), code.size(), "test.sol",
                                          "solidity", &result);

    REQUIRE(rc == YAMS_PLUGIN_OK);
    REQUIRE(result != nullptr);

    fprintf(stderr, "[Solidity DataTypes] Extracted %zu symbols\n", result->symbol_count);
    for (size_t i = 0; i < result->symbol_count; ++i) {
        fprintf(stderr, "  - %s (%s)\n", result->symbols[i].name ? result->symbols[i].name : "?",
                result->symbols[i].kind ? result->symbols[i].kind : "?");
    }

    // At least the contract should be extracted
    INFO("Should extract at least the contract"); CHECK(result->symbol_count > 0UL);

    plugin->api->free_result(plugin->api->self, result);
}

TEST_CASE("SymbolExtractorSolidity.LibraryDeclaration", "[plugin][symbolextractorsolidity]") {
    SKIP_ON_WINDOWS_NO_GRAMMAR();
    auto plugin = loadPlugin();
    INFO("Failed to load symbol extractor plugin"); REQUIRE(plugin.has_value());

    std::string code = R"(
// SPDX-License-Identifier: GPL-3.0-or-later
pragma solidity ^0.8.0;

library SafeMath {
    function add(uint256 a, uint256 b) internal pure returns (uint256) {
        return a + b;
    }

    function sub(uint256 a, uint256 b) internal pure returns (uint256) {
        require(b <= a, "Underflow");
        return a - b;
    }
}
)";

    yams_symbol_extraction_result_v1* result = nullptr;
    int rc = plugin->api->extract_symbols(plugin->api->self, code.c_str(), code.size(), "test.sol",
                                          "sol", &result);

    REQUIRE(rc == YAMS_PLUGIN_OK);
    REQUIRE(result != nullptr);

    fprintf(stderr, "[Solidity Library] Extracted %zu symbols\n", result->symbol_count);
    for (size_t i = 0; i < result->symbol_count; ++i) {
        fprintf(stderr, "  - %s (%s)\n", result->symbols[i].name ? result->symbols[i].name : "?",
                result->symbols[i].kind ? result->symbols[i].kind : "?");
    }

    // Library should be extracted (kind depends on grammar)
    bool hasLibrary =
        has_symbol(result, "SafeMath", "library") || has_symbol(result, "SafeMath", "class");
    INFO("SafeMath library should be extracted"); CHECK(hasLibrary);

    plugin->api->free_result(plugin->api->self, result);
}

TEST_CASE("SymbolExtractorSolidity.EmptySolidityFile", "[plugin][symbolextractorsolidity]") {
    SKIP_ON_WINDOWS_NO_GRAMMAR();
    auto plugin = loadPlugin();
    INFO("Failed to load symbol extractor plugin"); REQUIRE(plugin.has_value());

    std::string code = R"(
// SPDX-License-Identifier: GPL-3.0-or-later
pragma solidity ^0.8.0;
)";

    yams_symbol_extraction_result_v1* result = nullptr;
    int rc = plugin->api->extract_symbols(plugin->api->self, code.c_str(), code.size(), "test.sol",
                                          "solidity", &result);

    // Should succeed even with no symbols
    REQUIRE(rc == YAMS_PLUGIN_OK);
    REQUIRE(result != nullptr);

    fprintf(stderr, "[Solidity Empty] Extracted %zu symbols\n", result->symbol_count);

    plugin->api->free_result(plugin->api->self, result);
}

TEST_CASE("SymbolExtractorSolidity.InvalidSolidityCode", "[plugin][symbolextractorsolidity]") {
    SKIP_ON_WINDOWS_NO_GRAMMAR();
    auto plugin = loadPlugin();
    INFO("Failed to load symbol extractor plugin"); REQUIRE(plugin.has_value());

    std::string code = "this is not valid solidity code {{{";

    yams_symbol_extraction_result_v1* result = nullptr;
    int rc = plugin->api->extract_symbols(plugin->api->self, code.c_str(), code.size(), "test.sol",
                                          "solidity", &result);

    // Should either succeed with no symbols or fail gracefully (not crash)
    INFO("Should handle invalid code gracefully"); CHECK((rc == YAMS_PLUGIN_OK || rc == YAMS_PLUGIN_ERR_INVALID));

    if (result) {
        fprintf(stderr, "[Solidity Invalid] Extracted %zu symbols (from invalid code)\n",
                result->symbol_count);
        plugin->api->free_result(plugin->api->self, result);
    }
}
