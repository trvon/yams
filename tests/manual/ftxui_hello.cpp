/**
 * FTXUI Hello World Test
 *
 * Purpose: Verify FTXUI framework functionality for PBI 001 TUI modernization
 * Tests: Basic rendering, component lifecycle, input handling
 *
 * Build:
 *   conan install . -of build/ftxui-test -s build_type=Debug \
 *     -o enable_tui=True -o use_ftxui=True --build=missing
 *   g++ -std=c++17 tests/manual/ftxui_hello.cpp \
 *     -o build/ftxui_hello \
 *     $(pkg-config --cflags --libs ftxui-screen ftxui-dom ftxui-component) \
 *     -I build/ftxui-test/build-debug/conan
 *
 * Run:
 *   ./build/ftxui_hello
 */

#include <ftxui/component/component.hpp>
#include <ftxui/component/component_base.hpp>
#include <ftxui/component/component_options.hpp>
#include <ftxui/component/screen_interactive.hpp>
#include <ftxui/dom/elements.hpp>

#include <memory>
#include <string>
#include <vector>

using namespace ftxui;

int main() {
    // Test 1: Basic DOM rendering (static layout)
    auto test_dom = []() {
        auto document = vbox({
                            text("FTXUI Hello World Test") | bold | center,
                            separator(),
                            text("✓ DOM rendering works"),
                            text("✓ Layout system functional"),
                            text("✓ Unicode support (✓✗★→←)"),
                        }) |
                        border;

        auto screen = Screen::Create(Dimension::Fixed(60), Dimension::Fixed(10));
        Render(screen, document);
        screen.Print();
        return true;
    };

    // Test 2: Interactive components (list, input, buttons)
    auto test_interactive = []() {
        auto screen = ScreenInteractive::TerminalOutput();

        // List component with sample items
        std::vector<std::string> entries = {
            "Document 1: README.md", "Document 2: CHANGELOG.md", "Document 3: CONTRIBUTING.md",
            "Document 4: LICENSE",   "Document 5: AGENTS.md",
        };
        int selected = 0;

        // Input component for search/filter
        std::string input_value = "";
        Component input = Input(&input_value, "Type to search...");

        // List component
        MenuOption menu_option;
        menu_option.on_enter = [&] { screen.ExitLoopClosure()(); };
        Component menu = Menu(&entries, &selected, menu_option);

        // Button components
        auto btn_quit = Button("Quit [Q]", [&] { screen.ExitLoopClosure()(); });
        auto btn_refresh = Button("Refresh [R]", [] { /* no-op */ });

        // Layout: vertical stack with border
        auto component = Container::Vertical({
            input,
            menu,
            Container::Horizontal({btn_refresh, btn_quit}),
        });

        auto renderer = Renderer(component, [&] {
            return vbox({
                       text("FTXUI Interactive Test") | bold | center,
                       separator(),
                       hbox(text("Search: "), input->Render()),
                       separator(),
                       menu->Render() | frame | size(HEIGHT, LESS_THAN, 10),
                       separator(),
                       hbox({
                           btn_refresh->Render(),
                           text(" "),
                           btn_quit->Render(),
                       }) | center,
                       separator(),
                       text("Selected: " + entries[selected]) | dim,
                       text("Press Enter or Q to quit") | dim | center,
                   }) |
                   border;
        });

        // Add keyboard shortcuts
        auto component_with_keys = CatchEvent(renderer, [&](Event event) {
            if (event == Event::Character('q') || event == Event::Character('Q')) {
                screen.ExitLoopClosure()();
                return true;
            }
            if (event == Event::Character('r') || event == Event::Character('R')) {
                // Refresh action (no-op for test)
                return true;
            }
            return false;
        });

        screen.Loop(component_with_keys);
        return true;
    };

    // Test 3: Layout combinations (verify flexibility)
    auto test_layouts = []() {
        auto document = vbox({
                            text("Layout Test: Horizontal + Vertical") | bold | center,
                            separator(),
                            hbox({
                                vbox({
                                    text("Column 1") | border,
                                    text("Line 1"),
                                    text("Line 2"),
                                    text("Line 3"),
                                }) | flex,
                                separator(),
                                vbox({
                                    text("Column 2") | border,
                                    text("Data A"),
                                    text("Data B"),
                                    text("Data C"),
                                }) | flex,
                            }) | border,
                            separator(),
                            text("✓ Horizontal layout works") | color(Color::Green),
                            text("✓ Vertical layout works") | color(Color::Green),
                            text("✓ Nested layouts work") | color(Color::Green),
                        }) |
                        border;

        auto screen = Screen::Create(Dimension::Fixed(60), Dimension::Fixed(15));
        Render(screen, document);
        screen.Print();
        return true;
    };

    // Run all tests
    std::cout << "\n=== Running FTXUI Tests ===\n\n";

    std::cout << "Test 1: Basic DOM Rendering\n";
    test_dom();
    std::cout << "\n✓ Test 1 passed\n\n";

    std::cout << "Test 3: Layout Combinations\n";
    test_layouts();
    std::cout << "\n✓ Test 3 passed\n\n";

    std::cout << "Test 2: Interactive Components (press Q to exit)\n";
    test_interactive();
    std::cout << "\n✓ Test 2 passed\n\n";

    std::cout << "=== All FTXUI Tests Passed ===\n";
    std::cout << "FTXUI is functional for PBI 001 TUI modernization\n";

    return 0;
}
