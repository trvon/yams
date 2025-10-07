#include <gtest/gtest.h>
#include <yams/metadata/query_helpers.h>

using namespace yams::metadata::sql;

TEST(SqlBuildSelectTest, BasicSelectWithConditionsAndOrder) {
    QuerySpec spec;
    spec.table = "documents";
    spec.columns = {"id", "file_name"};
    spec.conditions = {"mime_type = 'text/plain'", "file_extension = '.md'"};
    spec.orderBy = std::optional<std::string>{"indexed_time DESC"};
    spec.limit = 10;
    spec.offset = 5;

    auto sql = buildSelect(spec);
    EXPECT_EQ(sql, "SELECT id, file_name FROM documents WHERE mime_type = 'text/plain' AND "
                   "file_extension = '.md' ORDER BY indexed_time DESC LIMIT 10 OFFSET 5");
}

TEST(SqlBuildSelectTest, FromClauseWithJoinAndGroupBy) {
    QuerySpec spec;
    spec.from =
        std::optional<std::string>{"tree_changes tc JOIN tree_diffs td ON tc.diff_id = td.diff_id"};
    spec.table = "tree_changes"; // ignored when from is set
    spec.columns = {"change_type", "old_path"};
    spec.conditions = {"td.base_snapshot_id = ?", "td.target_snapshot_id = ?"};
    spec.groupBy = std::optional<std::string>{"change_type"};
    spec.orderBy = std::optional<std::string>{"tc.change_id"};
    spec.limit = 100;
    spec.offset = 10;

    auto sql = buildSelect(spec);
    EXPECT_EQ(sql,
              "SELECT change_type, old_path FROM tree_changes tc JOIN tree_diffs td ON tc.diff_id "
              "= td.diff_id WHERE td.base_snapshot_id = ? AND td.target_snapshot_id = ? GROUP BY "
              "change_type ORDER BY tc.change_id LIMIT 100 OFFSET 10");
}
