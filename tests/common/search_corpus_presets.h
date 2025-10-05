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

inline SearchCorpusSpec mobileSearchCorpusSpec() {
    SearchCorpusSpec spec;
    spec.rootDirectory = "mobile";
    spec.bytesPerDocument = 256;
    spec.commonTags = {"mobile", "fixture"};
    spec.topics = {{.name = "sync",
                    .documentsPerTopic = 2,
                    .extension = ".txt",
                    .keywords = {"offline sync", "delta"},
                    .tags = {"sync", "scenario:offline"}},
                   {.name = "case",
                    .documentsPerTopic = 2,
                    .extension = ".md",
                    .keywords = {"CaseSensitive", "casesensitive"},
                    .tags = {"case", "scenario:case"}},
                   {.name = "paths",
                    .documentsPerTopic = 2,
                    .extension = ".json",
                    .keywords = {"/Users/demo/Documents", "C:/Users/Demo/Documents"},
                    .tags = {"paths", "scenario:path"}},
                   {.name = "semantic",
                    .documentsPerTopic = 1,
                    .extension = ".txt",
                    .keywords = {"neural embedding warm up"},
                    .tags = {"semantic", "scenario:warmup"}}};
    return spec;
}

} // namespace yams::test
