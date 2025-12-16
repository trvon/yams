// Catch2 migration of metadata unit tests
// Migration: yams-3s4 / yams-aqc (metadata tests)
// Covers:
//   - concepts_compile_test.cpp
//   - database_statement_time_test.cpp
//   - query_helpers_test.cpp
//   - sql_build_select_test.cpp
//   - metadata_value_variant_test.cpp

#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_approx.hpp>

#include <chrono>
#include <variant>
#include <yams/metadata/database.h>
#include <yams/metadata/document_metadata.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/query_helpers.h>

using namespace std::chrono;
using namespace yams::metadata;

// ============================================================================
// ConceptsCompileTest - verifies static_assert for FullMetadataStore concept
// ============================================================================
TEST_CASE("FullMetadataStore concept is satisfied", "[metadata][concepts]") {
    // This test does no runtime checks; including the header ensures
    // the compile-time static_assert for FullMetadataStore remains enforced.
    SUCCEED();
}

// ============================================================================
// DatabaseStatementChronoTest - chrono bind/getTime roundtrip
// ============================================================================
TEST_CASE("Database statement chrono bind and getTime roundtrip", "[metadata][database]") {
    Database db;
    REQUIRE(db.open(":memory:", ConnectionMode::Memory).has_value());

    REQUIRE(db.execute("CREATE TABLE t(ts INTEGER)").has_value());

    // Use a stable epoch value to avoid flakiness
    sys_seconds t = sys_seconds{seconds{1'234'567}};

    // Insert via chrono bind
    auto ins = db.prepare("INSERT INTO t(ts) VALUES(?)");
    REQUIRE(ins.has_value());
    Statement istmt = std::move(ins).value();
    REQUIRE(istmt.bind(1, t).has_value());
    REQUIRE(istmt.execute().has_value());

    // Read back via getTime
    auto sel = db.prepare("SELECT ts FROM t LIMIT 1");
    REQUIRE(sel.has_value());
    Statement sstmt = std::move(sel).value();
    auto step = sstmt.step();
    REQUIRE(step.has_value());
    REQUIRE(step.value());

    sys_seconds out = sstmt.getTime(0);
    CHECK(out.time_since_epoch().count() == t.time_since_epoch().count());
}

// ============================================================================
// QueryHelpersTest - buildQueryOptionsForSqlLikePattern
// ============================================================================
TEST_CASE("Query helpers build exact path for exact pattern", "[metadata][query_helpers]") {
    auto opts = buildQueryOptionsForSqlLikePattern("/notes/todo.md");
    REQUIRE(opts.exactPath.has_value());
    CHECK(*opts.exactPath == "/notes/todo.md");
    CHECK_FALSE(opts.likePattern.has_value());
}

TEST_CASE("Query helpers set prefix flags for directory pattern", "[metadata][query_helpers]") {
    auto opts = buildQueryOptionsForSqlLikePattern("/notes/%");
    REQUIRE(opts.pathPrefix.has_value());
    CHECK(*opts.pathPrefix == "/notes");
    CHECK(opts.prefixIsDirectory);
    CHECK(opts.includeSubdirectories);
    CHECK_FALSE(opts.likePattern.has_value());
}

TEST_CASE("Query helpers target FTS for contains pattern", "[metadata][query_helpers]") {
    auto opts = buildQueryOptionsForSqlLikePattern("%/todo.md");
    REQUIRE(opts.containsFragment.has_value());
    CHECK(*opts.containsFragment == "todo.md");
    CHECK(opts.containsUsesFts);
}

TEST_CASE("Query helpers set extension filter for extension pattern", "[metadata][query_helpers]") {
    auto opts = buildQueryOptionsForSqlLikePattern("%.md");
    REQUIRE(opts.extension.has_value());
    CHECK(*opts.extension == ".md");
}

TEST_CASE("Query helpers fallback keeps LIKE pattern", "[metadata][query_helpers]") {
    auto opts = buildQueryOptionsForSqlLikePattern("%notes%2025%");
    REQUIRE(opts.likePattern.has_value());
    CHECK(*opts.likePattern == "%notes%2025%");
}

// ============================================================================
// SqlBuildSelectTest - buildSelect function
// ============================================================================
TEST_CASE("SQL build select with conditions and order", "[metadata][sql]") {
    sql::QuerySpec spec;
    spec.table = "documents";
    spec.columns = {"id", "file_name"};
    spec.conditions = {"mime_type = 'text/plain'", "file_extension = '.md'"};
    spec.orderBy = std::optional<std::string>{"indexed_time DESC"};
    spec.limit = 10;
    spec.offset = 5;

    auto sql = sql::buildSelect(spec);
    CHECK(sql == "SELECT id, file_name FROM documents WHERE mime_type = 'text/plain' AND "
                 "file_extension = '.md' ORDER BY indexed_time DESC LIMIT 10 OFFSET 5");
}

TEST_CASE("SQL build select with FROM clause join and GROUP BY", "[metadata][sql]") {
    sql::QuerySpec spec;
    spec.from =
        std::optional<std::string>{"tree_changes tc JOIN tree_diffs td ON tc.diff_id = td.diff_id"};
    spec.table = "tree_changes"; // ignored when from is set
    spec.columns = {"change_type", "old_path"};
    spec.conditions = {"td.base_snapshot_id = ?", "td.target_snapshot_id = ?"};
    spec.groupBy = std::optional<std::string>{"change_type"};
    spec.orderBy = std::optional<std::string>{"tc.change_id"};
    spec.limit = 100;
    spec.offset = 10;

    auto sql = sql::buildSelect(spec);
    CHECK(sql ==
          "SELECT change_type, old_path FROM tree_changes tc JOIN tree_diffs td ON tc.diff_id "
          "= td.diff_id WHERE td.base_snapshot_id = ? AND td.target_snapshot_id = ? GROUP BY "
          "change_type ORDER BY tc.change_id LIMIT 100 OFFSET 10");
}

// ============================================================================
// MetadataValueVariantTest - MetadataValue constructors and variant operations
// ============================================================================
TEST_CASE("MetadataValue constructors and variant types", "[metadata][value]") {
    SECTION("String value") {
        MetadataValue s{"hello"};
        auto vs = s.asVariant();
        REQUIRE(std::holds_alternative<std::string>(vs));
        CHECK(std::get<std::string>(vs) == "hello");
    }

    SECTION("Integer value") {
        MetadataValue i{int64_t{42}};
        auto vi = i.asVariant();
        REQUIRE(std::holds_alternative<int64_t>(vi));
        CHECK(std::get<int64_t>(vi) == 42);
        CHECK(i.asInteger() == 42);
    }

    SECTION("Double value") {
        MetadataValue d{3.14};
        auto vd = d.asVariant();
        REQUIRE(std::holds_alternative<double>(vd));
        CHECK(std::get<double>(vd) == Catch::Approx(3.14));
    }

    SECTION("Boolean value") {
        MetadataValue b{true};
        auto vb = b.asVariant();
        REQUIRE(std::holds_alternative<bool>(vb));
        CHECK(std::get<bool>(vb) == true);
    }

    SECTION("Blob value") {
        std::vector<uint8_t> blob = {1, 2, 3};
        auto mv = MetadataValue::fromBlob(blob);
        auto vv = mv.asVariant();
        REQUIRE(std::holds_alternative<std::vector<uint8_t>>(vv));
        CHECK(std::get<std::vector<uint8_t>>(vv).size() == 3u);
    }
}

TEST_CASE("MetadataValue setVariant syncs legacy fields", "[metadata][value]") {
    MetadataValue v;
    
    SECTION("Integer variant syncs legacy") {
        v.setVariant(int64_t{99});
        CHECK(v.type == MetadataValueType::Integer);
        CHECK(v.asInteger() == 99);
        CHECK(v.value == std::string("99"));
    }

    SECTION("String variant syncs legacy") {
        v.setVariant(std::string{"abc"});
        CHECK(v.type == MetadataValueType::String);
        CHECK(v.asString() == "abc");
    }
}
