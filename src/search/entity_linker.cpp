#include <yams/search/entity_linker.h>

// This translation unit is intentionally minimal.
// SimpleHeuristicEntityLinker is implemented header-only for now to keep usage lightweight
// and enable inlining in performance-sensitive paths.

namespace yams::search {
// No out-of-line definitions at this time.
} // namespace yams::search

// Basic compile-time sanity checks to ensure symbols are available.
static_assert(sizeof(yams::search::EntityLinkerConfig) > 0, "EntityLinkerConfig must be defined");
static_assert(sizeof(yams::search::SimpleHeuristicEntityLinker) > 0,
              "SimpleHeuristicEntityLinker must be defined");