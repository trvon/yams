#include <yams/cli/live_grep.h>
#include <yams/cli/repo_files.h>

#include <nlohmann/json.hpp>
#include <deque>
#include <map>
#include <set>
#include <stdexcept>
#include <system_error>

namespace yams::cli {
LiveGrepResult liveGrep(const LiveGrepOptions& options) {
    LiveGrepResult result;
#ifdef _WIN32
    (void)options;
    return result; // retain the portable C++ scan on hosts without the POSIX subprocess runner
#else
    std::vector<std::string> arguments{"rg", "--color", "never", "--no-ignore-parent"};
    if (options.filesWithoutMatch) {
        arguments.push_back("--files-without-match");
        arguments.push_back("--null");
    } else {
        arguments.push_back("--json");
    }
    if (options.literal)
        arguments.push_back("--fixed-strings");
    if (options.ignoreCase)
        arguments.push_back("--ignore-case");
    if (options.wholeWord)
        arguments.push_back("--word-regexp");
    if (options.invert)
        arguments.push_back("--invert-match");
    const auto perFile = options.pathsOnly ? std::size_t{1} : options.perFileLimit;
    if (perFile != 0) {
        arguments.push_back("--max-count");
        arguments.push_back(std::to_string(perFile));
    }
    if (options.beforeContext != 0) {
        arguments.push_back("--before-context");
        arguments.push_back(std::to_string(options.beforeContext));
    }
    if (options.afterContext != 0) {
        arguments.push_back("--after-context");
        arguments.push_back(std::to_string(options.afterContext));
    }
    for (const auto& glob : options.globs) {
        arguments.push_back("--glob");
        arguments.push_back(glob);
    }
    arguments.push_back("--regexp");
    arguments.push_back(options.pattern);
    arguments.push_back("--");
    arguments.push_back(std::filesystem::absolute(options.root).string());
    std::set<std::string> paths;
    std::map<std::string, std::deque<std::pair<std::size_t, std::string>>> before;
    std::map<std::string, std::deque<std::size_t>> recentHits;
    CliProcessResult process;
    try {
        if (options.filesWithoutMatch) {
            process = runCliProcess(arguments);
            for (std::size_t start = 0; start < process.output.size();) {
                const auto end = process.output.find('\0', start);
                if (end == std::string::npos)
                    throw std::runtime_error("Invalid live path output");
                const auto path = process.output.substr(start, end - start);
                start = end + 1;
                if (options.acceptPath && !options.acceptPath(path))
                    continue;
                result.hits.push_back(LiveGrepHit{path, 0, {}, {}, {}});
                if (options.resultLimit != 0 && result.hits.size() >= options.resultLimit)
                    break;
            }
        } else {
            process =
                runCliProcess(arguments, std::size_t{8} * 1024 * 1024, [&](std::string_view line) {
                    const auto record = nlohmann::json::parse(line);
                    const auto type = record.value("type", "");
                    if (type == "end") {
                        return options.resultLimit == 0 || result.hits.size() < options.resultLimit;
                    }
                    if (type != "match" && type != "context")
                        return true;
                    const auto& data = record.at("data");
                    // rg encodes non-UTF8 paths/content as base64. Never emit a wrong path.
                    if (!data.at("path").contains("text") || !data.at("lines").contains("text"))
                        return true;
                    const auto path = data.at("path").at("text").get<std::string>();
                    if (options.acceptPath && !options.acceptPath(path))
                        return true;
                    const auto number = data.at("line_number").get<std::size_t>();
                    auto content = data.at("lines").at("text").get<std::string>();
                    if (!content.empty() && content.back() == '\n')
                        content.pop_back();
                    if (!content.empty() && content.back() == '\r')
                        content.pop_back();

                    auto& recent = recentHits[path];
                    while (!recent.empty() && number > result.hits[recent.front()].lineNumber &&
                           number - result.hits[recent.front()].lineNumber > options.afterContext)
                        recent.pop_front();
                    for (auto index : recent) {
                        if (number > result.hits[index].lineNumber)
                            result.hits[index].contextAfter.push_back(content);
                    }
                    auto& context = before[path];
                    while (!context.empty() && number > context.front().first &&
                           number - context.front().first > options.beforeContext)
                        context.pop_front();
                    const bool capped =
                        options.resultLimit != 0 && result.hits.size() >= options.resultLimit;
                    if (type == "match" && !capped &&
                        (!options.pathsOnly || paths.insert(path).second)) {
                        LiveGrepHit hit{path, number, content, {}, {}};
                        for (const auto& [unused, text] : context) {
                            (void)unused;
                            hit.contextBefore.push_back(text);
                        }
                        result.hits.push_back(std::move(hit));
                        recent.push_back(result.hits.size() - 1);
                    }
                    if (options.beforeContext != 0)
                        context.emplace_back(number, std::move(content));
                    if (options.resultLimit != 0 && result.hits.size() >= options.resultLimit) {
                        const auto matchedLine = result.hits.back().lineNumber;
                        if (number >= matchedLine && number - matchedLine >= options.afterContext)
                            return false;
                    }
                    return true;
                });
        }
    } catch (const std::system_error& error) {
        if (error.code() == std::errc::no_such_file_or_directory)
            return result;
        throw;
    }
    if (process.exitCode > 1)
        throw std::runtime_error("Live grep failed; check the regex and readable paths (rg exit " +
                                 std::to_string(process.exitCode) + ")");
    result.available = true;
    return result;
#endif
}
} // namespace yams::cli
