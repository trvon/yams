#pragma once

#include <vector>

#include "fixture_manager.h"

namespace yams::test {

inline SearchCorpusSpec defaultSearchCorpusSpec() {
    SearchCorpusSpec spec;
    spec.rootDirectory = "corpus";
    spec.bytesPerDocument = 384;
    spec.commonTags = {"search", "corpus"};
    spec.topics = {{.name = "quick",
                    .documentsPerTopic = 2,
                    .extension = ".txt",
                    .keywords = {"quick brown fox", "lazy dog"},
                    .tags = {"stories"}},
                   {.name = "python",
                    .documentsPerTopic = 2,
                    .extension = ".txt",
                    .keywords = {"Python programming language", "data science"},
                    .tags = {"code"}},
                   {.name = "machine",
                    .documentsPerTopic = 2,
                    .extension = ".txt",
                    .keywords = {"machine learning", "artificial intelligence"},
                    .tags = {"ml"}},
                   {.name = "deployment",
                    .documentsPerTopic = 1,
                    .extension = ".txt",
                    .keywords = {"quick deployment process"},
                    .tags = {"ops"}}};
    return spec;
}

inline std::vector<std::string> defaultSearchQueries() {
    return {"quick",         "python",       "machine", "deployment", "qwick",
            "hello_world()", "{\"server\":", "30,New",  "**bold**"};
}

} // namespace yams::test
