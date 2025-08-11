#include <yams/cli/tui/browse_services.hpp>
#include <yams/cli/tui/browse_state.hpp>

#include <cassert>
#include <cstddef>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>

using yams::cli::tui::BrowseServices;
using yams::cli::tui::DocEntry;
using yams::cli::tui::PreviewMode;

static void test_filter_basic() {
    BrowseServices svc(nullptr);

    std::vector<DocEntry> docs;
    {
        DocEntry d;
        d.name = "Alpha.txt";
        d.hash = "aaaaaaaa";
        docs.push_back(d);
    }
    {
        DocEntry d;
        d.name = "beta.log";
        d.hash = "bbbbbbbb";
        docs.push_back(d);
    }
    {
        DocEntry d;
        d.name = "README.md";
        d.hash = "cccccccc";
        docs.push_back(d);
    }

    {
        auto filtered = svc.filterBasic(docs, "alpha");
        assert(filtered.size() == 1 && "Expected case-insensitive match for 'alpha'");
        assert(filtered[0].name == "Alpha.txt");
    }
    {
        auto filtered = svc.filterBasic(docs, "MD"); // case-insensitive for file name
        assert(filtered.size() == 1 && "Expected case-insensitive match for 'MD'");
        assert(filtered[0].name == "README.md");
    }
    {
        auto filtered = svc.filterBasic(docs, "bbbb"); // case-sensitive hash match
        assert(filtered.size() == 1 && "Expected hash substring match");
        assert(filtered[0].hash == "bbbbbbbb");
    }
}

static void test_looks_binary_and_split_lines() {
    BrowseServices svc(nullptr);

    {
        std::string text = "Hello, World!\nThis is fine.\n";
        assert(!svc.looksBinary(text) && "Plain text should not look binary");
    }
    {
        std::string mostly_binary(100, '\0');
        for (size_t i = 0; i < mostly_binary.size(); i += 10) {
            mostly_binary[i] = 'A';
        }
        assert(svc.looksBinary(mostly_binary) && "Binary-like data should be detected");
    }

    {
        std::string crlf = "line1\r\nline2\nline3\r\n";
        auto lines = svc.splitLines(crlf, 10);
        assert(lines.size() == 3 && "Expected 3 lines split");
        assert(lines[0] == "line1");
        assert(lines[1] == "line2");
        assert(lines[2] == "line3");
    }
}

static void test_hex_dump() {
    BrowseServices svc(nullptr);

    // Data: "ABC\n\x01"
    std::vector<std::byte> bytes;
    bytes.push_back(static_cast<std::byte>('A')); // 0x41
    bytes.push_back(static_cast<std::byte>('B')); // 0x42
    bytes.push_back(static_cast<std::byte>('C')); // 0x43
    bytes.push_back(static_cast<std::byte>('\n'));
    bytes.push_back(static_cast<std::byte>(0x01));

    auto dump = svc.toHexDump(bytes, 16, 4);
    assert(!dump.empty() && "Hex dump should produce at least one line");
    const std::string& line = dump.front();

    // Expect ASCII gutter and hex '41' for 'A'
    assert(line.find('|') != std::string::npos && "Expected ASCII gutter in hex dump");
    assert(line.find("41") != std::string::npos && "Expected hex nibble for 'A' (0x41)");
}

static void test_make_preview_lines_auto_without_cli() {
    BrowseServices svc(nullptr);

    DocEntry d;
    d.name = "dummy";
    d.hash = "deadbeef";
    d.id = 123;

    auto lines = svc.makePreviewLines(d, PreviewMode::Auto, 1024, 10);
    // With nullptr CLI, there is no metadata or raw bytes, so we expect a fallback message.
    assert(!lines.empty());
    bool has_fallback = false;
    for (const auto& l : lines) {
        if (l.find("No preview available.") != std::string::npos ||
            l.find("Binary content.") != std::string::npos) {
            has_fallback = true;
            break;
        }
    }
    assert(has_fallback && "Expected a fallback preview message when no CLI/store is available");
}

int main() {
    try {
        test_filter_basic();
        test_looks_binary_and_split_lines();
        test_hex_dump();
        test_make_preview_lines_auto_without_cli();
    } catch (const std::exception& e) {
        std::cerr << "Exception in TUI services tests: " << e.what() << std::endl;
        return 1;
    }
    std::cout << "TUI services smoke tests passed." << std::endl;
    return 0;
}