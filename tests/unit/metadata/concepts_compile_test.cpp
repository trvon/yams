#include <gtest/gtest.h>
#include <yams/metadata/metadata_repository.h>

// This test intentionally does no runtime checks; including the header ensures
// the compile-time static_assert for FullMetadataStore remains enforced.
TEST(MetadataConceptsCompile, FullMetadataStoreConceptIsSatisfied) {
    SUCCEED();
}
