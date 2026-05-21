#include <catch2/catch_test_macros.hpp>

#include <filesystem>

#include <yams/app/services/retrieval_service.h>

using namespace yams::app::services;

TEST_CASE("RetrievalService testing helpers map client config", "[retrieval][service][mapping]") {
    RetrievalOptions opts;
    opts.socketPath = std::filesystem::path{"/tmp/yams.sock"};
    opts.explicitDataDir = std::filesystem::path{"/tmp/yams-data"};
    opts.enableStreaming = false;
    opts.progressiveOutput = true;
    opts.singleUseConnections = true;
    opts.maxChunkSize = 4096;
    opts.headerTimeoutMs = 1234;
    opts.bodyTimeoutMs = 5678;
    opts.requestTimeoutMs = 9012;
    opts.acceptCompressed = true;
    opts.transportMode = yams::daemon::ClientTransportMode::Socket;
    opts.autoStart = false;

    const auto cfg = testing::makeClientConfigForTest(opts);

    CHECK(cfg.socketPath == *opts.socketPath);
    CHECK(cfg.dataDir == *opts.explicitDataDir);
    CHECK_FALSE(cfg.enableChunkedResponses);
    CHECK(cfg.progressiveOutput);
    CHECK(cfg.singleUseConnections);
    CHECK(cfg.maxChunkSize == opts.maxChunkSize);
    CHECK(cfg.headerTimeout == std::chrono::milliseconds(opts.headerTimeoutMs));
    CHECK(cfg.bodyTimeout == std::chrono::milliseconds(opts.bodyTimeoutMs));
    CHECK(cfg.requestTimeout == std::chrono::milliseconds(opts.requestTimeoutMs));
    CHECK(cfg.acceptCompressed);
    CHECK(cfg.transportMode == yams::daemon::ClientTransportMode::Socket);
    CHECK_FALSE(cfg.autoStart);
}

TEST_CASE("RetrievalService testing helpers map Get options", "[retrieval][service][mapping]") {
    RetrievalOptions opts;
    opts.acceptCompressed = true;

    GetOptions in;
    in.hash = "hash";
    in.name = "name.txt";
    in.byName = true;
    in.fileType = "text";
    in.mimeType = "text/plain";
    in.extension = ".txt";
    in.binaryOnly = true;
    in.textOnly = true;
    in.createdAfter = "2024-01-01";
    in.createdBefore = "2024-02-01";
    in.modifiedAfter = "2024-03-01";
    in.modifiedBefore = "2024-04-01";
    in.indexedAfter = "2024-05-01";
    in.indexedBefore = "2024-06-01";
    in.latest = true;
    in.oldest = true;
    in.outputPath = "out.txt";
    in.metadataOnly = true;
    in.maxBytes = 42;
    in.chunkSize = 8192;
    in.raw = true;
    in.extract = true;
    in.acceptCompressed = false;
    in.showGraph = true;
    in.graphDepth = 3;
    in.verbose = true;

    const auto out = testing::makeGetRequestForTest(in, opts);

    CHECK(out.hash == in.hash);
    CHECK(out.name == in.name);
    CHECK(out.byName == in.byName);
    CHECK(out.fileType == in.fileType);
    CHECK(out.mimeType == in.mimeType);
    CHECK(out.extension == in.extension);
    CHECK(out.binaryOnly == in.binaryOnly);
    CHECK(out.textOnly == in.textOnly);
    CHECK(out.createdAfter == in.createdAfter);
    CHECK(out.createdBefore == in.createdBefore);
    CHECK(out.modifiedAfter == in.modifiedAfter);
    CHECK(out.modifiedBefore == in.modifiedBefore);
    CHECK(out.indexedAfter == in.indexedAfter);
    CHECK(out.indexedBefore == in.indexedBefore);
    CHECK(out.latest == in.latest);
    CHECK(out.oldest == in.oldest);
    CHECK(out.outputPath == in.outputPath);
    CHECK(out.metadataOnly == in.metadataOnly);
    CHECK(out.maxBytes == in.maxBytes);
    CHECK(out.chunkSize == in.chunkSize);
    CHECK(out.raw == in.raw);
    CHECK(out.extract == in.extract);
    CHECK(out.acceptCompressed);
    CHECK(out.showGraph == in.showGraph);
    CHECK(out.graphDepth == in.graphDepth);
    CHECK(out.verbose == in.verbose);
}

TEST_CASE("RetrievalService testing helpers map Grep options", "[retrieval][service][mapping]") {
    GrepOptions in;
    in.pattern = "needle";
    in.paths = {"a.txt", "b.txt"};
    in.caseInsensitive = true;
    in.invertMatch = true;
    in.contextLines = 2;
    in.maxMatches = 7;
    in.includePatterns = {"*.cpp"};
    in.recursive = false;
    in.wholeWord = true;
    in.showLineNumbers = true;
    in.showFilename = true;
    in.noFilename = true;
    in.countOnly = true;
    in.filesOnly = true;
    in.filesWithoutMatch = true;
    in.pathsOnly = true;
    in.literalText = true;
    in.regexOnly = true;
    in.semanticLimit = 4;
    in.filterTags = {"tag"};
    in.matchAllTags = true;
    in.colorMode = "never";
    in.beforeContext = 3;
    in.afterContext = 5;
    in.showDiff = true;
    in.useSession = true;
    in.sessionName = "work";

    const auto out = testing::makeGrepRequestForTest(in);

    CHECK(out.pattern == in.pattern);
    CHECK(out.paths == in.paths);
    CHECK(out.caseInsensitive == in.caseInsensitive);
    CHECK(out.invertMatch == in.invertMatch);
    CHECK(out.contextLines == in.contextLines);
    CHECK(out.maxMatches == in.maxMatches);
    CHECK(out.includePatterns == in.includePatterns);
    CHECK(out.recursive == in.recursive);
    CHECK(out.wholeWord == in.wholeWord);
    CHECK(out.showLineNumbers == in.showLineNumbers);
    CHECK(out.showFilename == in.showFilename);
    CHECK(out.noFilename == in.noFilename);
    CHECK(out.countOnly == in.countOnly);
    CHECK(out.filesOnly == in.filesOnly);
    CHECK(out.filesWithoutMatch == in.filesWithoutMatch);
    CHECK(out.pathsOnly == in.pathsOnly);
    CHECK(out.literalText == in.literalText);
    CHECK(out.regexOnly == in.regexOnly);
    CHECK(out.semanticLimit == in.semanticLimit);
    CHECK(out.filterTags == in.filterTags);
    CHECK(out.matchAllTags == in.matchAllTags);
    CHECK(out.colorMode == in.colorMode);
    CHECK(out.beforeContext == in.beforeContext);
    CHECK(out.afterContext == in.afterContext);
    CHECK(out.showDiff == in.showDiff);
    CHECK(out.useSession == in.useSession);
    CHECK(out.sessionName == in.sessionName);
}

TEST_CASE("RetrievalService testing helpers map List options", "[retrieval][service][mapping]") {
    ListOptions in;
    in.limit = 25;
    in.tags = {"one", "two"};
    in.metadataFilters = {{"owner", "opencode"}};
    in.format = "json";
    in.sortBy = "name";
    in.fileType = "code";
    in.mimeType = "text/x-c++src";
    in.extensions = ".cpp,.h";
    in.createdAfter = "ca";
    in.createdBefore = "cb";
    in.modifiedAfter = "ma";
    in.modifiedBefore = "mb";
    in.indexedAfter = "ia";
    in.indexedBefore = "ib";
    in.sinceTime = "1h";
    in.changeWindow = "7d";
    in.filterTags = "one,two";
    in.namePattern = "%.cpp";
    in.offset = 4;
    in.recentCount = 9;
    in.snippetLength = 80;
    in.recent = false;
    in.reverse = true;
    in.verbose = true;
    in.showSnippets = false;
    in.showMetadata = true;
    in.showTags = false;
    in.groupBySession = true;
    in.noSnippets = true;
    in.pathsOnly = true;
    in.binaryOnly = true;
    in.textOnly = true;
    in.showChanges = true;
    in.showDiffTags = true;
    in.showDeleted = true;
    in.matchAllTags = true;
    in.matchAllMetadata = false;

    const auto out = testing::makeListRequestForTest(in);

    CHECK(out.limit == in.limit);
    CHECK(out.tags == in.tags);
    CHECK(out.metadataFilters == in.metadataFilters);
    CHECK(out.format == in.format);
    CHECK(out.sortBy == in.sortBy);
    CHECK(out.fileType == in.fileType);
    CHECK(out.mimeType == in.mimeType);
    CHECK(out.extensions == in.extensions);
    CHECK(out.createdAfter == in.createdAfter);
    CHECK(out.createdBefore == in.createdBefore);
    CHECK(out.modifiedAfter == in.modifiedAfter);
    CHECK(out.modifiedBefore == in.modifiedBefore);
    CHECK(out.indexedAfter == in.indexedAfter);
    CHECK(out.indexedBefore == in.indexedBefore);
    CHECK(out.sinceTime == in.sinceTime);
    CHECK(out.changeWindow == in.changeWindow);
    CHECK(out.filterTags == in.filterTags);
    CHECK(out.namePattern == in.namePattern);
    CHECK(out.offset == in.offset);
    CHECK(out.recentCount == in.recentCount);
    CHECK(out.snippetLength == in.snippetLength);
    CHECK(out.recent == in.recent);
    CHECK(out.reverse == in.reverse);
    CHECK(out.verbose == in.verbose);
    CHECK(out.showSnippets == in.showSnippets);
    CHECK(out.showMetadata == in.showMetadata);
    CHECK(out.showTags == in.showTags);
    CHECK(out.groupBySession == in.groupBySession);
    CHECK(out.noSnippets == in.noSnippets);
    CHECK(out.pathsOnly == in.pathsOnly);
    CHECK(out.binaryOnly == in.binaryOnly);
    CHECK(out.textOnly == in.textOnly);
    CHECK(out.showChanges == in.showChanges);
    CHECK(out.showDiffTags == in.showDiffTags);
    CHECK(out.showDeleted == in.showDeleted);
    CHECK(out.matchAllTags == in.matchAllTags);
    CHECK(out.matchAllMetadata == in.matchAllMetadata);
}

TEST_CASE("RetrievalService testing helpers map GetInit and Search options",
          "[retrieval][service][mapping]") {
    GetInitOptions getInit;
    getInit.hash = "hash";
    getInit.name = "doc.md";
    getInit.byName = true;
    getInit.metadataOnly = true;
    getInit.maxBytes = 1024;
    getInit.chunkSize = 4096;

    const auto getInitReq = testing::makeGetInitRequestForTest(getInit);
    CHECK(getInitReq.hash == getInit.hash);
    CHECK(getInitReq.name == getInit.name);
    CHECK(getInitReq.byName == getInit.byName);
    CHECK(getInitReq.metadataOnly == getInit.metadataOnly);
    CHECK(getInitReq.maxBytes == getInit.maxBytes);
    CHECK(getInitReq.chunkSize == getInit.chunkSize);

    SearchOptions search;
    search.query = "graph search";
    search.limit = 11;
    search.fuzzy = true;
    search.literalText = true;
    search.similarity = 0.42;
    search.searchType = "hybrid";
    search.pathsOnly = true;
    search.showHash = true;
    search.verbose = true;
    search.jsonOutput = true;
    search.showLineNumbers = true;
    search.afterContext = 2;
    search.beforeContext = 3;
    search.context = 4;
    search.hashQuery = "abc123";
    search.pathPattern = "src/%";
    search.pathPatterns = {"src/%", "include/%"};
    search.tags = {"tag"};
    search.matchAllTags = true;
    search.extension = ".cpp";
    search.mimeType = "text/plain";
    search.fileType = "code";
    search.textOnly = true;
    search.binaryOnly = true;
    search.createdAfter = "ca";
    search.createdBefore = "cb";
    search.modifiedAfter = "ma";
    search.modifiedBefore = "mb";
    search.indexedAfter = "ia";
    search.indexedBefore = "ib";
    search.vectorStageTimeoutMs = 10;
    search.keywordStageTimeoutMs = 20;
    search.snippetHydrationTimeoutMs = 30;
    search.useSession = true;
    search.sessionName = "session";
    search.globalSearch = true;
    search.symbolRank = false;

    const auto out = testing::makeSearchRequestForTest(search);
    CHECK(out.query == search.query);
    CHECK(out.limit == search.limit);
    CHECK(out.fuzzy == search.fuzzy);
    CHECK(out.literalText == search.literalText);
    CHECK(out.similarity == search.similarity);
    CHECK(out.searchType == search.searchType);
    CHECK(out.pathsOnly == search.pathsOnly);
    CHECK(out.showHash == search.showHash);
    CHECK(out.verbose == search.verbose);
    CHECK(out.jsonOutput == search.jsonOutput);
    CHECK(out.showLineNumbers == search.showLineNumbers);
    CHECK(out.afterContext == search.afterContext);
    CHECK(out.beforeContext == search.beforeContext);
    CHECK(out.context == search.context);
    CHECK(out.hashQuery == search.hashQuery);
    CHECK(out.pathPattern == search.pathPattern);
    CHECK(out.pathPatterns == search.pathPatterns);
    CHECK(out.tags == search.tags);
    CHECK(out.matchAllTags == search.matchAllTags);
    CHECK(out.extension == search.extension);
    CHECK(out.mimeType == search.mimeType);
    CHECK(out.fileType == search.fileType);
    CHECK(out.textOnly == search.textOnly);
    CHECK(out.binaryOnly == search.binaryOnly);
    CHECK(out.createdAfter == search.createdAfter);
    CHECK(out.createdBefore == search.createdBefore);
    CHECK(out.modifiedAfter == search.modifiedAfter);
    CHECK(out.modifiedBefore == search.modifiedBefore);
    CHECK(out.indexedAfter == search.indexedAfter);
    CHECK(out.indexedBefore == search.indexedBefore);
    CHECK(out.vectorStageTimeoutMs == search.vectorStageTimeoutMs);
    CHECK(out.keywordStageTimeoutMs == search.keywordStageTimeoutMs);
    CHECK(out.snippetHydrationTimeoutMs == search.snippetHydrationTimeoutMs);
    CHECK(out.useSession == search.useSession);
    CHECK(out.sessionName == search.sessionName);
    CHECK(out.globalSearch == search.globalSearch);
    CHECK(out.symbolRank == search.symbolRank);
}
