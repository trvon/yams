#include <catch2/catch_session.hpp>

#include <string>
#include <string_view>
#include <vector>

namespace {

bool isOption(std::string_view arg) {
    return !arg.empty() && arg.front() == '-';
}

std::vector<std::string> normalizeArgs(int argc, char* argv[]) {
    std::vector<std::string> args;
    args.reserve(static_cast<std::size_t>(argc));

    bool hasOption = false;
    for (int i = 1; i < argc; ++i) {
        if (argv[i] && isOption(argv[i])) {
            hasOption = true;
            break;
        }
    }

    if (argc <= 2 || hasOption) {
        for (int i = 0; i < argc; ++i) {
            args.emplace_back(argv[i] ? argv[i] : "");
        }
        return args;
    }

    // Meson --test-args splits a test name like "Foo bar" into multiple argv entries.
    // Keep Catch2's parser on the single test-spec path; Catch2 3.12 + libc++ ASAN can
    // otherwise trip a container-overflow while collecting multiple positional specs.
    args.emplace_back(argv[0] ? argv[0] : "");
    std::string testSpec;
    for (int i = 1; i < argc; ++i) {
        if (!testSpec.empty()) {
            testSpec.push_back(' ');
        }
        testSpec += argv[i] ? argv[i] : "";
    }
    args.push_back(std::move(testSpec));
    return args;
}

} // namespace

int main(int argc, char* argv[]) {
    auto args = normalizeArgs(argc, argv);
    std::vector<char*> normalizedArgv;
    normalizedArgv.reserve(args.size());
    for (auto& arg : args) {
        normalizedArgv.push_back(arg.data());
    }

    return Catch::Session().run(static_cast<int>(normalizedArgv.size()), normalizedArgv.data());
}
