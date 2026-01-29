# ADR-0004: MetadataRepository Refactor with Templates and Metaprogramming

- Status: Proposed
- Date: 2026-01-16
- Decision Makers: @trvon
- Links: beads/yams-6rxi

## Context

`metadata_repository.cpp` has grown to ~5700 lines with significant code duplication:

### Current State Analysis
- **220 `Result<>` return types** - each with similar error handling patterns
- **90 `executeQuery` calls** - repetitive lambda boilerplate
- **~40 CRUD methods** - following identical patterns for different entity types
- **Manual SQL construction** - QuerySpec setup repeated everywhere
- **Duplicated bind/extract logic** - same column mapping code copied across methods

### Example of Current Repetition

```cpp
// Pattern repeated 40+ times with minor variations
Result<std::optional<DocumentInfo>> MetadataRepository::getDocument(int64_t id) {
    return executeQuery<std::optional<DocumentInfo>>(
        [&](Database& db) -> Result<std::optional<DocumentInfo>> {
            QuerySpec spec{};
            spec.table = "documents";
            spec.columns = {kDocumentColumnList};
            spec.conditions = {"id = ?"};

            auto stmtResult = db.prepareCached(buildSelect(spec));
            if (!stmtResult) return stmtResult.error();

            auto& stmt = *stmtResult.value();
            auto bindResult = stmt.bind(1, id);
            if (!bindResult) return bindResult.error();

            auto stepResult = stmt.step();
            if (!stepResult) return stepResult.error();

            if (!stepResult.value()) return std::optional<DocumentInfo>{};
            return std::optional<DocumentInfo>{mapDocumentRow(stmt)};
        });
}
```

### Problems
1. **Maintenance burden**: Changes to error handling require updating 90+ locations
2. **Bug risk**: Copy-paste errors in bind indices, column mappings
3. **Readability**: Business logic buried under boilerplate
4. **Testing**: Hard to unit test individual components
5. **Compile times**: Large monolithic file

## Decision

Refactor using modern C++20/23 techniques to reduce boilerplate while maintaining type safety.
Leverage existing `include/yams/core/cpp23_features.hpp` for feature detection and compatibility.

### Foundation: YAMS C++20/23 Feature Detection

Use existing infrastructure from `cpp23_features.hpp`:

```cpp
#include <yams/core/cpp23_features.hpp>

// Available feature flags:
// - YAMS_HAS_CONSTEXPR_CONTAINERS  (C++23 constexpr vector/string)
// - YAMS_HAS_EXPECTED              (std::expected for monadic ops)
// - YAMS_HAS_RANGES                (std::views for iteration)
// - YAMS_HAS_FLAT_MAP              (cache-friendly containers)
// - YAMS_CONSTEXPR_IF_SUPPORTED    (conditional constexpr)
```

### 1. Entity Traits System with C++20 Concepts

Define compile-time metadata using concepts for type safety:

```cpp
#include <concepts>
#include <tuple>
#include <string_view>

// Concept: Types that can be bound to SQL statements
template<typename T>
concept SqlBindable =
    std::same_as<T, int64_t> ||
    std::same_as<T, std::string> ||
    std::same_as<T, double> ||
    std::same_as<T, bool> ||
    std::same_as<T, std::nullptr_t>;

// Concept: Types that have entity traits defined
template<typename T>
concept HasEntityTraits = requires {
    { EntityTraits<T>::table } -> std::convertible_to<std::string_view>;
    { EntityTraits<T>::primary_key } -> std::convertible_to<std::string_view>;
    typename EntityTraits<T>::columns_tuple;
};

// Column descriptor with member pointer
template<SqlBindable T, typename Entity>
struct Column {
    std::string_view name;
    T Entity::* member;  // Pointer to member
    bool nullable = false;
};

template<typename T>
struct EntityTraits;

template<>
struct EntityTraits<DocumentInfo> {
    static constexpr std::string_view table = "documents";
    static constexpr std::string_view primary_key = "id";

    // Column definitions with member pointers for automatic bind/extract
    using columns_tuple = std::tuple<
        Column<int64_t, DocumentInfo>,
        Column<std::string, DocumentInfo>,
        Column<std::string, DocumentInfo>,
        Column<std::string, DocumentInfo>
    >;

    static constexpr columns_tuple columns{
        {"id", &DocumentInfo::id},
        {"file_path", &DocumentInfo::filePath},
        {"file_name", &DocumentInfo::fileName},
        {"sha256_hash", &DocumentInfo::sha256Hash}
        // ... etc
    };

    // Custom extraction for complex fields (timestamps, enums)
    static DocumentInfo extract(Statement& stmt) {
        DocumentInfo info;
        extractColumns(stmt, info);
        // Handle special cases like timestamp conversion
        info.setCreatedTime(stmt.getInt64(idx_created));
        return info;
    }
};
```

### 2. Generic CRUD Operations with Fold Expressions

Template-based CRUD using C++17 fold expressions for parameter binding:

```cpp
// Variadic bind using fold expressions - binds all args in order
template<typename... Args>
Result<void> bindAll(Statement& stmt, Args&&... args) {
    int idx = 1;
    Result<void> result;
    // Fold expression: (bind each arg, short-circuit on error)
    ((result = stmt.bind(idx++, std::forward<Args>(args)), result.has_value()) && ...);
    return result;
}

// Extract columns using fold + index_sequence
template<HasEntityTraits Entity, std::size_t... Is>
void extractColumnsImpl(Statement& stmt, Entity& e, std::index_sequence<Is...>) {
    auto& cols = EntityTraits<Entity>::columns;
    // Fold: extract each column into corresponding member
    ((e.*(std::get<Is>(cols).member) = stmt.get<
        std::remove_reference_t<decltype(e.*(std::get<Is>(cols).member))>
    >(Is)), ...);
}

template<HasEntityTraits Entity>
void extractColumns(Statement& stmt, Entity& e) {
    constexpr auto N = std::tuple_size_v<typename EntityTraits<Entity>::columns_tuple>;
    extractColumnsImpl(stmt, e, std::make_index_sequence<N>{});
}

// Constrained CRUD operations
template<HasEntityTraits Entity>
class CrudOps {
    using Traits = EntityTraits<Entity>;

public:
    Result<std::optional<Entity>> getById(Database& db, int64_t id) {
        static const auto sql = buildSelectSql<Entity>() + " WHERE id = ?";
        auto stmtResult = db.prepareCached(sql);
        if (!stmtResult) return stmtResult.error();

        auto& stmt = *stmtResult.value();
        if (auto r = stmt.bind(1, id); !r) return r.error();

        auto stepResult = stmt.step();
        if (!stepResult) return stepResult.error();
        if (!stepResult.value()) return std::nullopt;

        return Traits::extract(stmt);
    }

    Result<int64_t> insert(Database& db, const Entity& e) {
        static const auto sql = buildInsertSql<Entity>();
        auto stmtResult = db.prepare(sql);
        if (!stmtResult) return stmtResult.error();

        Statement stmt = std::move(stmtResult).value();
        // Use fold expression to bind all columns
        if (auto r = bindEntityColumns(stmt, e); !r) return r.error();
        if (auto r = stmt.execute(); !r) return r.error();

        return db.lastInsertRowId();
    }

private:
    // Bind entity columns using fold expression
    template<std::size_t... Is>
    static Result<void> bindColumnsImpl(Statement& stmt, const Entity& e,
                                         std::index_sequence<Is...>) {
        int idx = 1;
        Result<void> result;
        auto& cols = Traits::columns;
        // Fold: bind each member value
        ((result = stmt.bind(idx++, e.*(std::get<Is>(cols).member)),
          result.has_value()) && ...);
        return result;
    }

    static Result<void> bindEntityColumns(Statement& stmt, const Entity& e) {
        constexpr auto N = std::tuple_size_v<typename Traits::columns_tuple>;
        return bindColumnsImpl(stmt, e, std::make_index_sequence<N>{});
    }
};
```

### 3. Constexpr SQL Generation

Build SQL at compile time where possible:

```cpp
template<typename Entity>
consteval std::string_view buildSelectSql() {
    // Use std::format or string concatenation at compile time
    return "SELECT " + joinColumns<Entity>() + " FROM " + EntityTraits<Entity>::table;
}

// For runtime conditions, use a builder that validates at compile time
template<StringLiteral condition>
constexpr auto where() {
    static_assert(isValidSqlCondition(condition), "Invalid SQL condition");
    return WhereClause<condition>{};
}
```

### 4. Result Handling Macros/Helpers

Reduce error handling boilerplate:

```cpp
// Option A: Macros (controversial but effective)
#define TRY(expr) \
    if (auto _r = (expr); !_r) return _r.error()

#define TRY_ASSIGN(var, expr) \
    auto var##_result = (expr); \
    if (!var##_result) return var##_result.error(); \
    auto var = std::move(var##_result).value()

// Option B: Monadic Result operations (C++23)
Result<Entity> getDocument(int64_t id) {
    return executeQuery([&](auto& db) {
        return db.prepareCached(sql)
            .and_then([&](auto& stmt) { return stmt.bind(1, id); })
            .and_then([&](auto& stmt) { return stmt.step(); })
            .transform([&](auto& stmt) { return mapRow(stmt); });
    });
}
```

### 5. File Organization

Split the monolith:

```
src/metadata/
├── repository/
│   ├── crud_ops.hpp          # Generic CRUD template
│   ├── entity_traits.hpp     # Trait definitions
│   ├── sql_builder.hpp       # Constexpr SQL generation
│   ├── result_helpers.hpp    # TRY macros, monadic ops
│   │
│   ├── document_ops.cpp      # DocumentInfo operations
│   ├── content_ops.cpp       # DocumentContent operations
│   ├── metadata_ops.cpp      # Key-value metadata ops
│   ├── search_ops.cpp        # FTS5/search operations
│   ├── relationship_ops.cpp  # Document relationships
│   └── history_ops.cpp       # Search history, saved queries
│
├── metadata_repository.cpp   # Facade that composes above
└── metadata_repository.h     # Public interface (unchanged)
```

## Migration Plan

### Phase 1: Foundation (Non-breaking)
1. Create `entity_traits.hpp` with traits for DocumentInfo
2. Create `crud_ops.hpp` with template CRUD
3. Create `result_helpers.hpp` with TRY macros
4. Add unit tests for new infrastructure

### Phase 2: Incremental Migration
1. Migrate `getDocument`/`insertDocument` to use new system
2. Validate with existing integration tests
3. Migrate remaining Document operations
4. Migrate Content, Metadata, Relationship operations

### Phase 3: Split Files
1. Extract operations to separate files
2. Update build system
3. Final cleanup of legacy patterns

### Phase 4: Advanced Features
1. Constexpr SQL validation
2. Compile-time column index computation
3. Batch operation optimizations

## Consequences

### Positive
- **50-70% reduction in code volume** (estimated 2000-2500 lines)
- **Single point of change** for error handling, tracing
- **Easier testing** of individual components
- **Better IDE support** with explicit types
- **Faster iteration** on new entity types

### Negative
- **Learning curve** for template metaprogramming patterns
- **Compile time increase** for template-heavy headers (mitigated by explicit instantiation)
- **Debugging complexity** for template errors
- **Risk during migration** - need comprehensive test coverage

### Neutral
- **Public API unchanged** - MetadataRepository interface stays the same
- **Performance neutral** - templates compile to same code as hand-written

## Alternatives Considered

### 1. Code Generation (Rejected)
Generate C++ from schema definitions. Rejected because:
- Adds build complexity
- Harder to debug generated code
- Templates achieve similar result with standard tooling

### 2. ORM Library (Rejected)
Use existing ORM like sqlpp11 or ODB. Rejected because:
- Heavy dependencies
- May not fit existing Result<> pattern
- Loss of fine-grained control

### 3. Keep As-Is (Rejected)
The file is already causing maintenance issues and will only get worse.

## Notes

### YAMS-Specific Integration

Leverage existing YAMS infrastructure:

```cpp
// Use existing feature detection
#include <yams/core/cpp23_features.hpp>

// Conditional monadic ops based on std::expected availability
#if YAMS_HAS_EXPECTED
    // Use std::expected monadic chain
    return stmt.and_then([](auto& s) { return s.step(); })
               .transform([](auto& s) { return extract(s); });
#else
    // Fallback to manual error checking
    if (auto r = stmt.step(); !r) return r.error();
    return extract(stmt);
#endif

// Use YAMS_HAS_RANGES for iteration
#if YAMS_HAS_RANGES
    auto results = rows | std::views::transform(extractRow);
#else
    std::vector<Entity> results;
    for (auto& row : rows) results.push_back(extractRow(row));
#endif
```

### Inspiration
- Rust's Diesel ORM (type-safe query building)
- C++ sqlpp11 (compile-time SQL validation)
- JOOQ (Java type-safe SQL)
- Boost.Hana (compile-time type manipulation)

### References

**C++20 Features Used:**
- [Concepts and Constraints](https://en.cppreference.com/w/cpp/language/constraints) - Type-safe template parameters
- [Fold Expressions](https://en.cppreference.com/w/cpp/language/fold) - Pack expansion without recursion
- [Abbreviated Function Templates](https://en.cppreference.com/w/cpp/language/function_template#Abbreviated_function_template) - `auto` parameters

**C++23 Features (Optional, via YAMS_HAS_*):**
- `std::expected` - Monadic error handling
- Constexpr `std::vector`/`std::string` - Compile-time SQL building
- Deducing `this` - Simplified CRTP patterns

**Key Resources:**
- [C++20 Idioms for Parameter Packs](https://www.scs.stanford.edu/~dm/blog/param-pack.html) - Stanford blog on modern pack patterns
- [C++20 Concepts: The Details](https://www.modernescpp.com/index.php/c-20-concepts-the-details/) - ModernCpp.com
- [Boost.Hana](https://www.boost.org/doc/libs/latest/libs/hana/) - If heavier metaprogramming needed

### Existing YAMS Infrastructure

Files to leverage:
- `include/yams/core/cpp23_features.hpp` - Feature detection macros
- `include/yams/core/types.h` - `Result<T>` type
- `src/metadata/sql/query_helpers.h` - `QuerySpec` and `buildSelect()`
