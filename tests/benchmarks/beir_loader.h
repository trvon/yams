#pragma once

#include <filesystem>
#include <fstream>
#include <map>
#include <sstream>
#include <string>
#include <utility>
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <yams/core/types.h>

namespace yams::bench {

struct BEIRDocument {
    std::string id;
    std::string title;
    std::string text;
};

struct BEIRQuery {
    std::string id;
    std::string text;
};

struct BEIRDataset {
    std::string name;
    std::map<std::string, BEIRDocument> documents;
    std::map<std::string, BEIRQuery> queries;
    std::multimap<std::string, std::pair<std::string, int>> qrels;
    std::filesystem::path basePath;
};

inline yams::Result<BEIRDataset> loadBEIRDataset(const std::string& datasetName,
                                                 const std::filesystem::path& cacheDir) {
    namespace fs = std::filesystem;
    using nlohmann::json;

    static const std::map<std::string, std::string> DATASET_ZIP_NAMES = {
        {"touche-2020", "webis-touche2020"},
        {"dbpedia-entity", "dbpedia-entity"},
    };

    std::string zipName = datasetName;
    auto zipIt = DATASET_ZIP_NAMES.find(datasetName);
    if (zipIt != DATASET_ZIP_NAMES.end()) {
        zipName = zipIt->second;
    }

    fs::path datasetDir = cacheDir;
    if (datasetDir.empty()) {
        const char* home = std::getenv("HOME");
        if (!home) {
            return yams::Error{yams::ErrorCode::InvalidArgument, "HOME not set and no cacheDir"};
        }
        datasetDir = fs::path(home) / ".cache" / "yams" / "benchmarks" / datasetName;
    }

    BEIRDataset dataset;
    dataset.name = datasetName;
    dataset.basePath = datasetDir;

    fs::path corpusFile = datasetDir / "corpus.jsonl";
    fs::path queriesFile = datasetDir / "queries.jsonl";
    fs::path qrelsFile = datasetDir / "qrels" / "test.tsv";

    if (!fs::exists(corpusFile) || !fs::exists(queriesFile) || !fs::exists(qrelsFile)) {
        if (datasetName == "longmemeval_s") {
            return yams::Error{yams::ErrorCode::NotFound,
                               "LongMemEval_S dataset not found. Convert from HuggingFace:\n"
                               "  uv pip install datasets\n"
                               "  python scripts/prepare_longmemeval_s.py\n"
                               "Expected location: ~/.cache/yams/benchmarks/longmemeval_s/"};
        }
        std::ostringstream oss;
        oss << datasetName << " dataset not found. Please download manually:\n"
            << "  mkdir -p ~/.cache/yams/benchmarks && cd ~/.cache/yams/benchmarks\n"
            << "  curl -L -o " << zipName << ".zip "
            << "https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/" << zipName
            << ".zip\n"
            << "  unzip " << zipName << ".zip";
        if (datasetName != zipName) {
            oss << " && mv " << zipName << " " << datasetName;
        }
        return yams::Error{yams::ErrorCode::NotFound, oss.str()};
    }

    std::ifstream corpusIn(corpusFile);
    std::string line;
    while (std::getline(corpusIn, line)) {
        if (line.empty())
            continue;
        try {
            auto j = json::parse(line);
            BEIRDocument doc;
            doc.id = j.value("_id", "");
            if (doc.id.empty())
                doc.id = j.value("id", "");
            doc.title = j.value("title", "");
            doc.text = j.value("text", "");
            if (!doc.id.empty() && !doc.text.empty()) {
                dataset.documents[doc.id] = doc;
            }
        } catch (const json::exception& e) {
            spdlog::warn("BEIR loader: failed to parse corpus line: {}", e.what());
        }
    }
    corpusIn.close();

    std::ifstream queriesIn(queriesFile);
    while (std::getline(queriesIn, line)) {
        if (line.empty())
            continue;
        try {
            auto j = json::parse(line);
            BEIRQuery q;
            q.id = j.value("_id", "");
            if (q.id.empty())
                q.id = j.value("id", "");
            q.text = j.value("text", "");
            if (!q.id.empty() && !q.text.empty()) {
                dataset.queries[q.id] = q;
            }
        } catch (const json::exception& e) {
            spdlog::warn("BEIR loader: failed to parse query line: {}", e.what());
        }
    }
    queriesIn.close();

    std::ifstream qrelsIn(qrelsFile);
    bool firstLine = true;
    while (std::getline(qrelsIn, line)) {
        if (line.empty() || line[0] == '#')
            continue;
        if (firstLine) {
            firstLine = false;
            if (line.find("query-id") != std::string::npos)
                continue;
        }
        std::istringstream iss(line);
        std::string queryId, docId, scoreStr;
        if (std::getline(iss, queryId, '\t') && std::getline(iss, docId, '\t') &&
            std::getline(iss, scoreStr, '\t')) {
            try {
                int score = std::stoi(scoreStr);
                dataset.qrels.emplace(queryId, std::make_pair(docId, score));
            } catch (const std::exception& e) {
                spdlog::warn("BEIR loader: bad qrel score '{}': {}", scoreStr, e.what());
            }
        }
    }
    qrelsIn.close();

    spdlog::info("BEIR loader: {} -> {} docs, {} queries, {} qrels", datasetName,
                 dataset.documents.size(), dataset.queries.size(), dataset.qrels.size());

    return dataset;
}

} // namespace yams::bench
