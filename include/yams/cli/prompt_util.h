/**
 * Prompt utility helpers for interactive CLI workflows.
 *
 * These helpers are intentionally minimal (std::cout/std::cin) to avoid
 * adding third-party dependencies while centralizing prompt behavior and
 * ensuring consistent default handling across commands.
 */
#pragma once
#include <cctype>
#include <functional>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace yams::cli {

// --------------------------- Yes / No ---------------------------- //
struct YesNoOptions {
    bool defaultYes{true};      // What to return if user presses enter or invalid
    bool allowEmpty{true};      // If false, reprompt on empty input
    std::string yesChars{"yY"}; // Acceptable yes characters
    std::string noChars{"nN"};  // Acceptable no characters
    bool retryOnInvalid{false}; // If true, keep asking until valid
};

inline bool prompt_yes_no(const std::string& prompt, const YesNoOptions& opts = {}) {
    for (;;) {
        std::cout << prompt;
        std::string line;
        if (!std::getline(std::cin, line)) {
            return opts.defaultYes; // EOF -> default
        }
        if (line.empty()) {
            if (opts.allowEmpty)
                return opts.defaultYes;
            if (!opts.retryOnInvalid)
                return opts.defaultYes;
            continue; // reprompt
        }
        char c = static_cast<char>(line[0]);
        if (opts.yesChars.find(c) != std::string::npos)
            return true;
        if (opts.noChars.find(c) != std::string::npos)
            return false;
        if (!opts.retryOnInvalid)
            return opts.defaultYes;
        // Otherwise reprompt
    }
}

// --------------------------- Generic Input ---------------------------- //
struct InputOptions {
    std::string defaultValue{};                          // Returned on empty when allowEmpty true
    bool allowEmpty{true};                               // Accept empty and return defaultValue
    bool trimWhitespace{true};                           // Trim leading/trailing whitespace
    bool retryOnInvalid{true};                           // Reprompt if validator fails
    std::function<bool(const std::string&)> validator{}; // Return true if acceptable
};

inline std::string prompt_input(const std::string& prompt, const InputOptions& opts = {}) {
    for (;;) {
        std::cout << prompt;
        std::string line;
        if (!std::getline(std::cin, line)) {
            return opts.defaultValue; // EOF -> default
        }
        if (opts.trimWhitespace) {
            size_t start = line.find_first_not_of(" \t\r\n");
            size_t end = line.find_last_not_of(" \t\r\n");
            if (start == std::string::npos)
                line.clear();
            else
                line = line.substr(start, end - start + 1);
        }
        if (line.empty()) {
            if (opts.allowEmpty)
                return opts.defaultValue;
            if (!opts.retryOnInvalid)
                return opts.defaultValue; // fallback
            continue;
        }
        if (opts.validator && !opts.validator(line)) {
            if (!opts.retryOnInvalid)
                return opts.defaultValue;
            continue; // reprompt
        }
        return line;
    }
}

// --------------------------- Choice / Menu ---------------------------- //
struct ChoiceItem {
    std::string value;       // Returned value
    std::string label;       // Shown label (if empty use value)
    std::string description; // Additional lines printed under the choice
};

struct ChoiceOptions {
    size_t defaultIndex{0};     // 0-based index default
    bool allowEmpty{true};      // Enter to accept default
    bool retryOnInvalid{true};  // Reprompt on invalid index
    bool showNumericHint{true}; // Show numbers before each option
};

inline size_t prompt_choice(const std::string& header, const std::vector<ChoiceItem>& items,
                            const ChoiceOptions& opts = {}) {
    if (items.empty())
        throw std::invalid_argument("prompt_choice: items cannot be empty");
    for (;;) {
        if (!header.empty())
            std::cout << header << "\n";
        for (size_t i = 0; i < items.size(); ++i) {
            const auto& it = items[i];
            if (opts.showNumericHint)
                std::cout << "  " << (i + 1) << ". ";
            std::cout << (it.label.empty() ? it.value : it.label) << "\n";
            if (!it.description.empty()) {
                std::cout << "     " << it.description << "\n";
            }
        }
        std::cout << "Select a number (1-" << items.size() << ")";
        if (opts.allowEmpty)
            std::cout << " [" << (opts.defaultIndex + 1) << "]";
        std::cout << ": ";
        std::string line;
        if (!std::getline(std::cin, line))
            return opts.defaultIndex; // EOF
        if (line.empty() && opts.allowEmpty)
            return opts.defaultIndex;
        try {
            size_t choice = static_cast<size_t>(std::stoul(line));
            if (choice >= 1 && choice <= items.size())
                return choice - 1;
        } catch (...) {
            // ignore parse error
        }
        if (!opts.retryOnInvalid)
            return opts.defaultIndex;
        // else loop again
    }
}

} // namespace yams::cli
