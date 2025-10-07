#include <gtest/gtest.h>
#include <yams/metadata/document_metadata.h>

using namespace yams::metadata;

TEST(MetadataValueVariantTest, ConstructorsAndVariant) {
    MetadataValue s{"hello"};
    auto vs = s.asVariant();
    ASSERT_TRUE(std::holds_alternative<std::string>(vs));
    EXPECT_EQ(std::get<std::string>(vs), "hello");

    MetadataValue i{int64_t{42}};
    auto vi = i.asVariant();
    ASSERT_TRUE(std::holds_alternative<int64_t>(vi));
    EXPECT_EQ(std::get<int64_t>(vi), 42);
    EXPECT_EQ(i.asInteger(), 42);

    MetadataValue d{3.14};
    auto vd = d.asVariant();
    ASSERT_TRUE(std::holds_alternative<double>(vd));
    EXPECT_NEAR(std::get<double>(vd), 3.14, 1e-9);

    MetadataValue b{true};
    auto vb = b.asVariant();
    ASSERT_TRUE(std::holds_alternative<bool>(vb));
    EXPECT_TRUE(std::get<bool>(vb));

    std::vector<uint8_t> blob = {1, 2, 3};
    auto mv = MetadataValue::fromBlob(blob);
    auto vv = mv.asVariant();
    ASSERT_TRUE(std::holds_alternative<std::vector<uint8_t>>(vv));
    EXPECT_EQ(std::get<std::vector<uint8_t>>(vv).size(), 3u);
}

TEST(MetadataValueVariantTest, SetVariantSyncsLegacy) {
    MetadataValue v;
    v.setVariant(int64_t{99});
    EXPECT_EQ(v.type, MetadataValueType::Integer);
    EXPECT_EQ(v.asInteger(), 99);
    EXPECT_EQ(v.value, std::string("99"));

    v.setVariant(std::string{"abc"});
    EXPECT_EQ(v.type, MetadataValueType::String);
    EXPECT_EQ(v.asString(), "abc");
}
