// Copyright (c) 2026 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later
#include <yams/cli/static_tables.h>

#include <yams/config/config_helpers.h>
#include <yams/vector/simeon_embedding_backend.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <iterator>
#include <limits>
#include <optional>
#include <string_view>
#include <system_error>

namespace yams::cli {
namespace {
namespace fs = std::filesystem;

Result<std::string> readConfig(const fs::path& path) {
    std::ifstream input(path, std::ios::binary);
    if (!input) {
        return Error{ErrorCode::IOError, "Cannot read model-selection config: " + path.string()};
    }
    std::string contents{std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
    if (input.bad()) {
        return Error{ErrorCode::IOError, "Failed reading model-selection config: " + path.string()};
    }
    return contents;
}

std::string_view trim(std::string_view text) {
    const auto begin = text.find_first_not_of(" \t\r");
    if (begin == std::string_view::npos) {
        return {};
    }
    return text.substr(begin, text.find_last_not_of(" \t\r") - begin + 1);
}

Result<std::string> withSelection(std::string contents, const std::string& path) {
    // The existing flat config reader preserves escape sequences rather than
    // decoding them. Prefer a TOML literal string; never write an ambiguous path.
    std::string value;
    if (path.find_first_of("\r\n") != std::string::npos || path.find('\0') != std::string::npos) {
        return Error{ErrorCode::InvalidArgument, "Static table path must be single-line"};
    }
    if (path.find('\'') == std::string::npos) {
        value = "'" + path + "'";
    } else if (path.find_first_of("\"\\") == std::string::npos) {
        value = "\"" + path + "\"";
    } else {
        return Error{ErrorCode::InvalidArgument,
                     "Static table path cannot be represented by the flat config reader"};
    }
    // Do not mistake section-looking lines inside multiline values for real
    // tables. Leave complex configs untouched and ask for an explicit manual edit.
    if (contents.find("\"\"\"") != std::string::npos || contents.find("'''") != std::string::npos) {
        return Error{ErrorCode::InvalidArgument,
                     "Set embeddings.simeon.static_table manually in a multiline config"};
    }

    constexpr std::string_view kKey = "embeddings.simeon.static_table";
    std::string section;
    std::optional<std::size_t> nestedEnd, parentEnd, rootEnd;
    std::optional<std::pair<std::size_t, std::size_t>> target;
    std::string targetKey;
    for (std::size_t offset = 0; offset < contents.size();) {
        const auto newline = contents.find('\n', offset);
        const auto end = newline == std::string::npos ? contents.size() : newline;
        const auto next = newline == std::string::npos ? end : end + 1;
        const auto line = trim(std::string_view(contents).substr(offset, end - offset));
        if (!line.empty() && line.front() == '[') {
            if (line.find("embeddings") != std::string_view::npos &&
                (line.starts_with("[[") || line.find_first_of("\"'") != std::string_view::npos)) {
                return Error{ErrorCode::InvalidArgument,
                             "Set static_table manually for quoted or array embedding tables"};
            }
            if (!rootEnd) {
                rootEnd = offset;
            }
            const auto close = line.find(']');
            section = close == std::string_view::npos
                          ? std::string{}
                          : std::string(trim(line.substr(1, close - 1)));
        } else if (!line.empty() && line.front() != '#') {
            const auto equals = line.find('=');
            if (equals != std::string_view::npos) {
                const auto key = trim(line.substr(0, equals));
                const auto qualified =
                    section.empty() ? std::string(key) : section + "." + std::string(key);
                if (qualified == "embeddings" || qualified == "embeddings.simeon" ||
                    ((section == "embeddings" || section == "embeddings.simeon" ||
                      (section.empty() && key.find("embeddings") != std::string_view::npos)) &&
                     key.find_first_of("\"'") != std::string_view::npos)) {
                    return Error{ErrorCode::InvalidArgument,
                                 "Set static_table manually for inline or quoted embedding keys"};
                }
                if (qualified == kKey) {
                    if (target) {
                        return Error{ErrorCode::InvalidArgument,
                                     "Duplicate static table selections in config"};
                    }
                    target = std::pair{offset, end};
                    targetKey = key;
                }
            }
        }
        if (section == "embeddings.simeon") {
            nestedEnd = next;
        } else if (section == "embeddings") {
            parentEnd = next;
        }
        offset = next;
    }
    if (target) {
        contents.replace(target->first, target->second - target->first, targetKey + " = " + value);
    } else {
        const auto insertion =
            nestedEnd.value_or(parentEnd.value_or(rootEnd.value_or(contents.size())));
        const auto key = nestedEnd   ? "static_table"
                         : parentEnd ? "simeon.static_table"
                                     : "embeddings.simeon.static_table";
        std::string addition;
        if (insertion > 0 && contents[insertion - 1] != '\n') {
            addition += '\n';
        }
        addition += std::string(key) + " = " + value + "\n";
        contents.insert(insertion, addition);
    }
    return contents;
}

struct SelectionStage {
    fs::path path;
    ~SelectionStage() {
        std::error_code ignored;
        if (!path.empty()) {
            fs::remove_all(path, ignored);
        }
    }
};
} // namespace

Result<void> selectStaticTable(const fs::path& table, const fs::path& configPath) {
    if (!vector::loadSharedStaticEmbeddingTable(table)) {
        return Error{ErrorCode::InvalidArgument, "Cannot select an invalid static embedding table"};
    }
    std::error_code error;
    const auto selected = fs::absolute(table, error);
    if (error) {
        return Error{ErrorCode::IOError,
                     "Cannot resolve selected static table: " + error.message()};
    }
    auto destination = fs::absolute(configPath, error);
    if (error) {
        return Error{ErrorCode::IOError,
                     "Cannot resolve model-selection config: " + error.message()};
    }
    const auto originalStatus = fs::symlink_status(destination, error);
    if (error && error != std::errc::no_such_file_or_directory) {
        return Error{ErrorCode::IOError,
                     "Cannot inspect model-selection config: " + error.message()};
    }
    error.clear();
    // A dangling symlink is an existing operator entry, not a new file to replace.
    const bool existed = fs::exists(originalStatus);
    std::string original;
    auto permissions = fs::perms::owner_read | fs::perms::owner_write;
    if (existed) {
        // Preserve an operator's config symlink instead of replacing the link.
        destination = fs::canonical(destination, error);
        if (error || !fs::is_regular_file(destination, error)) {
            return Error{ErrorCode::IOError, "Model-selection config is not a regular file"};
        }
        permissions = fs::status(destination, error).permissions();
        if (error) {
            return Error{ErrorCode::IOError, "Cannot read model-selection config permissions"};
        }
        const auto read = readConfig(destination);
        if (!read) {
            return read.error();
        }
        original = read.value();
    }
    const auto changed = withSelection(original, selected.generic_string());
    if (!changed) {
        return changed.error();
    }
    fs::create_directories(destination.parent_path(), error);
    if (error) {
        return Error{ErrorCode::IOError, "Cannot create model-selection config directory"};
    }
    SelectionStage stage;
    static std::atomic<std::uint64_t> sequence{0};
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    for (unsigned int attempt = 0; attempt < 8; ++attempt) {
        const auto candidate = destination.parent_path() /
                               (".yams-model-selection-" + std::to_string(stamp) + "-" +
                                std::to_string(sequence.fetch_add(1, std::memory_order_relaxed)));
        if (fs::create_directory(candidate, error)) {
            stage.path = candidate;
            break;
        }
        if (error) {
            return Error{ErrorCode::IOError, "Cannot stage model selection: " + error.message()};
        }
    }
    if (stage.path.empty()) {
        return Error{ErrorCode::IOError,
                     "Cannot reserve private model-selection staging directory"};
    }
    fs::permissions(stage.path, fs::perms::owner_all, error);
    if (error) {
        return Error{ErrorCode::IOError, "Cannot protect model-selection staging directory"};
    }
    const auto staged = stage.path / "config.toml";
    {
        std::ofstream output(staged, std::ios::binary | std::ios::trunc);
        if (!output || changed.value().size() >
                           static_cast<std::size_t>(std::numeric_limits<std::streamsize>::max())) {
            return Error{ErrorCode::IOError, "Cannot write staged model selection"};
        }
        output.write(changed.value().data(), static_cast<std::streamsize>(changed.value().size()));
        output.flush();
        if (!output) {
            return Error{ErrorCode::IOError, "Failed flushing staged model selection"};
        }
        output.close();
        if (!output) {
            return Error{ErrorCode::IOError, "Failed closing staged model selection"};
        }
    }
    auto expected = config::parse_simple_toml(destination);
    expected["embeddings.simeon.static_table"] = selected.generic_string();
    if (config::parse_simple_toml(staged) != expected) {
        return Error{ErrorCode::InvalidState, "Staged model selection changed unrelated config"};
    }
    fs::permissions(staged, permissions, error);
    if (error) {
        return Error{ErrorCode::IOError, "Cannot preserve model-selection config permissions"};
    }
    // Detect intervening edits before publication. This is not a cross-writer
    // transaction or power-loss durability guarantee; other config writers do
    // not share a lock. The old file stays intact on every checked failure.
    if (existed) {
        const auto current = readConfig(destination);
        if (!current || current.value() != original) {
            return Error{ErrorCode::InvalidState, "Config changed during model selection; retry"};
        }
    } else if (fs::exists(destination, error) || error) {
        return Error{ErrorCode::InvalidState, "Config appeared during model selection; retry"};
    }
    fs::rename(staged, destination, error);
    if (error) {
        return Error{ErrorCode::IOError, "Cannot publish model selection: " + error.message()};
    }
    return {};
}

} // namespace yams::cli
