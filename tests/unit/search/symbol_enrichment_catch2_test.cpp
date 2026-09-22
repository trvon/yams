// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_test_macros.hpp>

#include <nlohmann/json.hpp>
#include <yams/core/uuid.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/migration.h>
#include <yams/search/symbol_enrichment.h>

#include <filesystem>

using yams::metadata::KGNode;
using yams::search::SearchResultItem;
using yams::search::SymbolEnricher;
using yams::search::SymbolInfo;

TEST_CASE("SymbolEnricher: extractSymbolInfoFromNode handles standard and versioned types",
          "[search][symbols][catch2]") {
    SymbolEnricher enricher(nullptr);

    SECTION("Standard symbol types are recognized") {
        KGNode node;
        node.id = 1;
        node.type = "class";
        node.label = "TuneAdvisor";
        nlohmann::json props;
        props["qualified_name"] = "yams::daemon::TuneAdvisor";
        props["file_path"] = "include/yams/daemon/TuneAdvisor.h";
        props["start_line"] = 42;
        node.properties = props.dump();

        auto info = enricher.extractSymbolInfoFromNode(node);
        REQUIRE(info.has_value());
        CHECK(info->name == "TuneAdvisor");
        CHECK(info->qualifiedName == "yams::daemon::TuneAdvisor");
        CHECK(info->kind == "class");
        CHECK(info->definitionFile == "include/yams/daemon/TuneAdvisor.h");
        CHECK(info->definitionLine == 42);
    }

    SECTION("Extended symbol types (struct, interface, enum) are recognized") {
        KGNode structNode;
        structNode.id = 2;
        structNode.type = "struct";
        structNode.label = "SearchConfig";
        auto infoStruct = enricher.extractSymbolInfoFromNode(structNode);
        REQUIRE(infoStruct.has_value());
        CHECK(infoStruct->kind == "struct");
        CHECK(infoStruct->name == "SearchConfig");

        KGNode enumNode;
        enumNode.id = 3;
        enumNode.type = "enum";
        enumNode.label = "QueryType";
        auto infoEnum = enricher.extractSymbolInfoFromNode(enumNode);
        REQUIRE(infoEnum.has_value());
        CHECK(infoEnum->kind == "enum");

        KGNode interfaceNode;
        interfaceNode.id = 4;
        interfaceNode.type = "interface";
        interfaceNode.label = "ISearchBackend";
        auto infoInterface = enricher.extractSymbolInfoFromNode(interfaceNode);
        REQUIRE(infoInterface.has_value());
        CHECK(infoInterface->kind == "interface");
    }

    SECTION("Versioned symbol types (class_version, function_version, etc.) are recognized") {
        KGNode versionNode;
        versionNode.id = 5;
        versionNode.type = "class_version";
        versionNode.label = "PostIngestQueue";
        nlohmann::json props;
        props["qualified_name"] = "yams::daemon::PostIngestQueue";
        props["file_path"] = "include/yams/daemon/PostIngestQueue.h";
        props["start_line"] = 120;
        props["return_type"] = "void";
        versionNode.properties = props.dump();

        auto info = enricher.extractSymbolInfoFromNode(versionNode);
        REQUIRE(info.has_value());
        CHECK(info->name == "PostIngestQueue");
        CHECK(info->qualifiedName == "yams::daemon::PostIngestQueue");
        CHECK(info->kind == "class");
        CHECK(info->definitionLine == 120);
    }

    SECTION("Non-symbol types return nullopt") {
        for (const std::string& nonSymbolType :
             {"file", "directory", "document", "blob", "package", "unknown", "concept"}) {
            KGNode node;
            node.id = 10;
            node.type = nonSymbolType;
            node.label = "not_a_symbol";
            auto info = enricher.extractSymbolInfoFromNode(node);
            CHECK_FALSE(info.has_value());
        }
    }

    SECTION("Natural language entities with nodeKey starting with nl_entity: return nullopt") {
        KGNode nlNode;
        nlNode.id = 11;
        nlNode.nodeKey = "nl_entity:method:pytorch";
        nlNode.type = "method";
        nlNode.label = "pytorch";
        auto info = enricher.extractSymbolInfoFromNode(nlNode);
        CHECK_FALSE(info.has_value());
    }
}

TEST_CASE("SymbolEnricher: enrichResult enriches search results matching doc entities",
          "[search][symbols][enrichment][catch2]") {
    auto dir =
        std::filesystem::temp_directory_path() / yams::core::generateId("symbol-enrichment-test");
    std::filesystem::create_directories(dir);
    std::string dbPath = (dir / "test.db").string();

    struct Cleanup {
        std::filesystem::path path;
        ~Cleanup() {
            std::error_code ec;
            std::filesystem::remove_all(path, ec);
        }
    } cleanup{dir};

    {
        yams::metadata::Database db;
        REQUIRE(db.open(dbPath, yams::metadata::ConnectionMode::Create));
        yams::metadata::MigrationManager migrations(db);
        REQUIRE(migrations.initialize());
        migrations.registerMigrations(yams::metadata::YamsMetadataMigrations::getAllMigrations());
        REQUIRE(migrations.migrate());
        db.close();
    }

    yams::metadata::ConnectionPoolConfig poolConfig;
    poolConfig.minConnections = 1;
    poolConfig.maxConnections = 2;
    auto pool = std::make_shared<yams::metadata::ConnectionPool>(dbPath, poolConfig);
    auto kgResult = yams::metadata::makeSqliteKnowledgeGraphStore(*pool);
    REQUIRE(kgResult.has_value());
    std::shared_ptr<yams::metadata::KnowledgeGraphStore> kg = std::move(kgResult.value());
    auto metadata = std::make_shared<yams::metadata::MetadataRepository>(*pool);

    // Insert document
    yams::metadata::DocumentInfo doc;
    doc.filePath = "include/yams/daemon/TuneAdvisor.h";
    doc.fileName = "TuneAdvisor.h";
    doc.sha256Hash = "hash-tune-advisor-123456";
    auto docIdRes = metadata->insertDocument(doc);
    REQUIRE(docIdRes.has_value());
    int64_t docId = docIdRes.value();

    auto wbRes = kg->beginWriteBatch();
    REQUIRE(wbRes.has_value());
    auto wb = std::move(wbRes.value());

    // Insert doc node
    KGNode docNode;
    docNode.nodeKey = "doc:" + doc.sha256Hash;
    docNode.label = doc.filePath;
    docNode.type = "document";
    nlohmann::json docProps;
    docProps["hash"] = doc.sha256Hash;
    docNode.properties = docProps.dump();
    auto docNodeRes = wb->upsertNode(docNode);
    REQUIRE(docNodeRes.has_value());

    // Insert versioned symbol node
    KGNode symNode;
    symNode.nodeKey = "class:TuneAdvisor@include/yams/daemon/TuneAdvisor.h@snap:" + doc.sha256Hash;
    symNode.label = "TuneAdvisor";
    symNode.type = "class_version";
    nlohmann::json symProps;
    symProps["qualified_name"] = "yams::daemon::TuneAdvisor";
    symProps["simple_name"] = "TuneAdvisor";
    symProps["file_path"] = doc.filePath;
    symProps["language"] = "cpp";
    symProps["start_line"] = 42;
    symProps["end_line"] = 90;
    symNode.properties = symProps.dump();
    auto symNodeRes = wb->upsertNode(symNode);
    REQUIRE(symNodeRes.has_value());
    int64_t symNodeId = symNodeRes.value();

    // Insert short symbol node "Go"
    KGNode goNode;
    goNode.nodeKey = "class:Go@include/yams/daemon/TuneAdvisor.h@snap:" + doc.sha256Hash;
    goNode.label = "Go";
    goNode.type = "class_version";
    nlohmann::json goProps;
    goProps["qualified_name"] = "Go";
    goProps["simple_name"] = "Go";
    goProps["file_path"] = doc.filePath;
    goNode.properties = goProps.dump();
    auto goNodeRes = wb->upsertNode(goNode);
    REQUIRE(goNodeRes.has_value());

    // Insert doc entities
    yams::metadata::DocEntity entity;
    entity.documentId = docId;
    entity.nodeId = symNodeId;
    entity.entityText = "yams::daemon::TuneAdvisor";
    entity.startOffset = 100;
    entity.endOffset = 250;
    entity.confidence = 1.0f;
    entity.extractor = "symbol_extractor_v1";

    yams::metadata::DocEntity goEntity;
    goEntity.documentId = docId;
    goEntity.nodeId = goNodeRes.value();
    goEntity.entityText = "Go";
    goEntity.startOffset = 0;
    goEntity.endOffset = 2;
    goEntity.confidence = 1.0f;
    goEntity.extractor = "symbol_extractor_v1";

    auto addEntRes = wb->addDocEntities({entity, goEntity});
    REQUIRE(addEntRes.has_value());
    REQUIRE(wb->commit().has_value());
    wb.reset();

    SymbolEnricher enricher(kg);

    SECTION("Enrich result for matching query") {
        SearchResultItem item;
        item.path = doc.filePath;
        item.metadata["sha256_hash"] = doc.sha256Hash;

        bool enriched = enricher.enrichResult(item, "TuneAdvisor");
        REQUIRE(enriched);
        REQUIRE(item.symbolContext.has_value());
        CHECK(item.symbolContext->isSymbolQuery);
        CHECK(item.symbolContext->matchType == "definition");
        CHECK(item.symbolContext->definitionScore >= 1.5f);
        REQUIRE_FALSE(item.symbolContext->symbols.empty());
        CHECK(item.symbolContext->symbols.front().name == "TuneAdvisor");
    }

    SECTION("Enrich result for case-insensitive query") {
        SearchResultItem item;
        item.path = doc.filePath;
        item.metadata["sha256_hash"] = doc.sha256Hash;

        bool enriched = enricher.enrichResult(item, "tuneadvisor");
        REQUIRE(enriched);
        REQUIRE(item.symbolContext.has_value());
        CHECK(item.symbolContext->isSymbolQuery);
    }

    SECTION("Non-matching query is not marked as symbol query") {
        SearchResultItem item;
        item.path = doc.filePath;
        item.metadata["sha256_hash"] = doc.sha256Hash;

        bool enriched = enricher.enrichResult(item, "NonExistentClass");
        REQUIRE(enriched);
        REQUIRE(item.symbolContext.has_value());
        CHECK_FALSE(item.symbolContext->isSymbolQuery);
        CHECK(item.symbolContext->symbolScore == 0.0f);
        CHECK(item.symbolContext->matchType.empty());
    }

    SECTION("Short symbol name is not matched by broad queries (symbol-contains-query)") {
        SearchResultItem item;
        item.path = doc.filePath;
        item.metadata["sha256_hash"] = doc.sha256Hash;

        // Query contains "go" as a substring/word, but symbol "Go" does not contain query
        bool enriched = enricher.enrichResult(item, "how to go faster");
        REQUIRE(enriched);
        REQUIRE(item.symbolContext.has_value());
        CHECK_FALSE(item.symbolContext->isSymbolQuery);
        CHECK(item.symbolContext->symbolScore == 0.0f);
        CHECK(item.symbolContext->matchType.empty());

        // Exact match on "Go" should still match
        SearchResultItem exactItem;
        exactItem.path = doc.filePath;
        exactItem.metadata["sha256_hash"] = doc.sha256Hash;
        bool exactEnriched = enricher.enrichResult(exactItem, "Go");
        REQUIRE(exactEnriched);
        REQUIRE(exactItem.symbolContext.has_value());
        CHECK(exactItem.symbolContext->isSymbolQuery);
    }

    pool->shutdown();
}
