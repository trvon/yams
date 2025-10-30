# Catch2 Test Patterns for YAMS

**Version:** 1.0  
**Updated:** 2025-10-25  
**Based on:** 5 migrated Catch2 tests (daemon, metadata, services)

This document captures established patterns for writing Catch2 tests in YAMS, extracted from successfully migrated tests.

---

## Table of Contents

1. [Basic Structure](#basic-structure)
2. [Schema Validation Pattern](#schema-validation-pattern)
3. [Isolated Test Harnesses](#isolated-test-harnesses)
4. [BDD-Style Organization](#bdd-style-organization)
5. [Async Operations](#async-operations)
6. [Tag Conventions](#tag-conventions)
7. [Migration Guidelines](#migration-guidelines)

---

## Basic Structure

### Minimal Test File

```cpp
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_session.hpp>

using namespace yams;

TEST_CASE("Component behavior description", "[module][component]") {
    // Setup
    auto component = createTestComponent();
    
    SECTION("specific behavior case") {
        // Arrange
        auto input = createInput();
        
        // Act
        auto result = component.process(input);
        
        // Assert
        REQUIRE(result.has_value());
        REQUIRE(result->expected_field == expected_value);
    }
}
```

### Main Runner (for standalone test files)

```cpp
#define CATCH_CONFIG_MAIN
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_session.hpp>

// Tests follow...
```

---

## Schema Validation Pattern

**Used in:** retrieval_schema_validation_test.cpp, search_metadata_interface_test.cpp, symbol_metadata_schema_test.cpp

### Pattern: Validate API Parameters Match Database Schema

```cpp
#include <catch2/catch_test_macros.hpp>
#include <yams/metadata/database.h>
#include <yams/metadata/metadata_repository.h>

namespace {

class SchemaValidator {
public:
    SchemaValidator() {
        // Create temp database
        auto tempDir = std::filesystem::temp_directory_path() / "yams_test_schema";
        std::filesystem::create_directories(tempDir);
        auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
        dbPath_ = tempDir / (std::string("schema_") + std::to_string(ts) + ".db");
        
        // Initialize with migrations
        pool_ = std::make_shared<ConnectionPool>(dbPath_.string());
        auto initResult = pool_->initialize();
        REQUIRE(initResult.has_value());
        
        pool_->withConnection([](Database& db) -> Result<void> {
            MigrationManager mm(db);
            auto initResult = mm.initialize();
            if (!initResult) return initResult.error();
            
            mm.registerMigrations(YamsMetadataMigrations::getAllMigrations());
            return mm.migrate();
        });
        
        // Cache column info
        pool_->withConnection([this](Database& db) -> Result<void> {
            table_columns_ = getColumnsFromDB(db, "target_table");
            return Result<void>{};
        });
    }
    
    ~SchemaValidator() {
        repo_.reset();
        pool_.reset();
        std::filesystem::remove(dbPath_);
    }
    
    std::set<std::string> getColumnsFromDB(Database& db, const std::string& table) {
        std::set<std::string> columns;
        auto result = db.executeQuery("PRAGMA table_info(" + table + ")");
        if (result && result->hasRows()) {
            while (result->hasRow()) {
                columns.insert(result->getString(1)); // column name
                result->step();
            }
        }
        return columns;
    }
    
    bool hasColumn(const std::string& table, const std::string& column) {
        return table_columns_.count(column) > 0;
    }

private:
    std::filesystem::path dbPath_;
    std::shared_ptr<ConnectionPool> pool_;
    std::unique_ptr<MetadataRepository> repo_;
    std::set<std::string> table_columns_;
};

} // namespace

TEST_CASE("Service parameters map to schema", "[metadata][schema][validation]") {
    SchemaValidator validator;
    
    SECTION("field X maps to documents.column_y") {
        REQUIRE(validator.hasColumn("documents", "column_y"));
    }
    
    SECTION("metadata filter uses valid columns") {
        // Example: validate that MetadataFilter::path_pattern uses "path" column
        REQUIRE(validator.hasColumn("documents", "path"));
    }
}
```

**Benefits:**
- Catches schema drift at compile/test time
- Prevents runtime errors from mismatched field names
- Documents schema dependencies in tests

---

## Isolated Test Harnesses

### Pattern: DaemonHarness (Integration Tests)

**Used in:** daemon_socket_client_test.cpp

```cpp
#include "test_daemon_harness.h"
#include <catch2/catch_test_macros.hpp>

TEST_CASE("Daemon integration test", "[daemon][integration]") {
    DaemonHarness harness; // RAII - creates temp socket/db/cas
    
    SECTION("operation requires running daemon") {
        REQUIRE(harness.start()); // Blocks until Ready state
        
        auto client = createClient(harness.socketPath());
        auto result = yams::cli::run_sync(client.connect(), 3s);
        
        REQUIRE(result.has_value());
        // harness.stop() called automatically on destruction
    }
}
```

**DaemonHarness Features:**
- Creates isolated temp directory per test
- Manages daemon lifecycle (start/stop)
- Provides socket path, db path, CAS path
- Cleans up automatically (RAII)

### Pattern: IsolatedStorage (Unit Tests)

**Proposed** (not yet implemented, but follows established pattern):

```cpp
class IsolatedStorage {
public:
    IsolatedStorage() {
        temp_dir_ = std::filesystem::temp_directory_path() / 
                    ("yams_test_" + generateUniqueId());
        std::filesystem::create_directories(temp_dir_);
        
        db_ = std::make_unique<Database>(temp_dir_ / "test.db");
        cas_ = std::make_unique<CAS>(temp_dir_ / "cas");
        repo_ = std::make_unique<MetadataRepository>(*pool_);
    }
    
    ~IsolatedStorage() {
        repo_.reset();
        cas_.reset();
        db_.reset();
        std::filesystem::remove_all(temp_dir_);
    }
    
    Database& db() { return *db_; }
    CAS& cas() { return *cas_; }
    MetadataRepository& repo() { return *repo_; }

private:
    std::filesystem::path temp_dir_;
    std::unique_ptr<Database> db_;
    std::unique_ptr<CAS> cas_;
    std::unique_ptr<MetadataRepository> repo_;
};

TEST_CASE("Storage operation", "[storage][unit]") {
    IsolatedStorage storage;
    
    SECTION("operation") {
        auto result = storage.repo().insertDocument(...);
        REQUIRE(result.has_value());
    }
}
```

---

## BDD-Style Organization

### Pattern: Behavior-Driven Structure with SECTION

**Used in:** All 5 migrated tests

```cpp
TEST_CASE("Component::method behavior", "[component][method]") {
    // Shared setup
    auto component = createComponent();
    
    SECTION("succeeds with valid input") {
        auto result = component.method(validInput);
        REQUIRE(result.has_value());
        REQUIRE(result->value == expected);
    }
    
    SECTION("fails with invalid input") {
        auto result = component.method(invalidInput);
        REQUIRE(!result.has_value());
        REQUIRE(result.error().code == ErrorCode::kInvalidInput);
    }
    
    SECTION("handles edge case X") {
        auto result = component.method(edgeCaseInput);
        REQUIRE(result.has_value());
        // Edge case specific assertions
    }
}
```

**Benefits:**
- Readable test names (clear intent)
- Shared setup code (runs once per SECTION)
- Easy to add new test cases
- Better test failure messages

### Nested SECTIONS for Complex Scenarios

```cpp
TEST_CASE("Complex component behavior", "[component]") {
    auto component = createComponent();
    
    SECTION("when configured with option A") {
        component.configure(OptionA);
        
        SECTION("processes input X correctly") {
            REQUIRE(component.process(X).has_value());
        }
        
        SECTION("rejects input Y") {
            REQUIRE(!component.process(Y).has_value());
        }
    }
    
    SECTION("when configured with option B") {
        component.configure(OptionB);
        
        SECTION("processes input X differently") {
            auto result = component.process(X);
            REQUIRE(result.has_value());
            REQUIRE(result->differentBehavior == true);
        }
    }
}
```

---

## Async Operations

### Pattern: run_sync for Awaitable Operations

**Used in:** daemon_socket_client_test.cpp

```cpp
#include "test_async_helpers.h"
#include <catch2/catch_test_macros.hpp>

TEST_CASE("Async operation", "[async]") {
    SECTION("completes successfully") {
        auto client = createClient();
        
        // Convert awaitable to sync for testing
        auto result = yams::cli::run_sync(
            client.asyncOperation(),  // Returns awaitable<Result<T>>
            5s                        // Timeout
        );
        
        REQUIRE(result.has_value());
        REQUIRE(result->field == expected);
    }
    
    SECTION("times out appropriately") {
        auto client = createSlowClient();
        
        auto result = yams::cli::run_sync(
            client.slowOperation(),
            100ms  // Short timeout
        );
        
        // May timeout or complete - test both cases
        if (!result.has_value()) {
            // Timeout expected
            REQUIRE(result.error().code == ErrorCode::kTimeout);
        }
    }
}
```

---

## Tag Conventions

**Based on existing tests:**

### Module Tags (Required)
- `[daemon]` - Daemon-related tests
- `[metadata]` - Metadata/database tests
- `[services]` - Service layer tests
- `[search]` - Search engine tests
- `[storage]` - Storage (DB/CAS) tests
- `[plugins]` - Plugin tests
- `[mcp]` - MCP protocol tests

### Component Tags (Recommended)
- `[socket]` - Socket communication
- `[schema]` - Schema validation
- `[client]` - Client implementation
- `[harness]` - Test harness utilities

### Test Type Tags (Recommended)
- `[unit]` - Unit tests
- `[integration]` - Integration tests
- `[validation]` - Validation/schema tests

### Usage Examples
```cpp
TEST_CASE("Daemon client connects", "[daemon][socket][integration]") { ... }
TEST_CASE("Metadata schema valid", "[metadata][schema][validation]") { ... }
TEST_CASE("Storage transaction", "[storage][unit]") { ... }
```

**Running tests by tag:**
```bash
# Run all daemon tests
./test_executable "[daemon]"

# Run integration tests only
./test_executable "[integration]"

# Run daemon integration tests
./test_executable "[daemon][integration]"

# Exclude slow tests
./test_executable "~[slow]"
```

---

## Migration Guidelines

### GTest → Catch2 Conversion

#### 1. Basic Assertion Mapping

| GTest | Catch2 | Notes |
|-------|--------|-------|
| `ASSERT_TRUE(x)` | `REQUIRE(x)` | Aborts test on failure |
| `EXPECT_TRUE(x)` | `CHECK(x)` | Continues test on failure |
| `ASSERT_EQ(a, b)` | `REQUIRE(a == b)` | Better diagnostics in Catch2 |
| `ASSERT_NE(a, b)` | `REQUIRE(a != b)` | |
| `ASSERT_LT(a, b)` | `REQUIRE(a < b)` | |
| `ASSERT_THAT(x, matcher)` | `REQUIRE_THAT(x, matcher)` | Catch2 has built-in matchers |

#### 2. Test Structure Conversion

**GTest:**
```cpp
class ComponentTest : public ::testing::Test {
protected:
    void SetUp() override {
        component_ = std::make_unique<Component>();
    }
    
    void TearDown() override {
        component_.reset();
    }
    
    std::unique_ptr<Component> component_;
};

TEST_F(ComponentTest, ProcessesValidInput) {
    auto result = component_->process(validInput);
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(result.value(), expected);
}

TEST_F(ComponentTest, RejectsInvalidInput) {
    auto result = component_->process(invalidInput);
    ASSERT_FALSE(result.ok());
}
```

**Catch2:**
```cpp
TEST_CASE("Component behavior", "[component]") {
    // Setup runs once, shared by all SECTIONS
    auto component = std::make_unique<Component>();
    
    SECTION("processes valid input") {
        auto result = component->process(validInput);
        REQUIRE(result.ok());
        REQUIRE(result.value() == expected);
    }
    
    SECTION("rejects invalid input") {
        auto result = component->process(invalidInput);
        REQUIRE(!result.ok());
    }
    
    // TearDown automatic via RAII
}
```

#### 3. Parameterized Tests

**GTest:**
```cpp
class ParamTest : public ::testing::TestWithParam<int> {};

TEST_P(ParamTest, HandlesValue) {
    int value = GetParam();
    EXPECT_TRUE(validate(value));
}

INSTANTIATE_TEST_SUITE_P(Values, ParamTest,
    ::testing::Values(1, 2, 3, 4, 5));
```

**Catch2:**
```cpp
#include <catch2/generators/catch_generators.hpp>

TEST_CASE("Parameterized test", "[param]") {
    auto value = GENERATE(1, 2, 3, 4, 5);
    REQUIRE(validate(value));
}

// Or with named generators
TEST_CASE("Range test", "[param]") {
    auto value = GENERATE(range(1, 6)); // 1..5
    REQUIRE(validate(value));
}
```

#### 4. Migration Checklist

- [ ] Update includes: `#include <catch2/catch_test_macros.hpp>`
- [ ] Convert `TEST_F` → `TEST_CASE` with `SECTION`s
- [ ] Convert `ASSERT_*` → `REQUIRE()`
- [ ] Convert `EXPECT_*` → `CHECK()`
- [ ] Add appropriate tags `[module][component][type]`
- [ ] Use BDD-style test names (behavior description)
- [ ] Replace fixtures with RAII or harness classes
- [ ] Update parameterized tests to use `GENERATE`
- [ ] Remove GTest-specific setup (TEST_MAIN, etc.)
- [ ] Add `#define CATCH_CONFIG_MAIN` if standalone

---

## Anti-Patterns to Avoid

### ❌ Don't: Share mutable state across TEST_CASEs
```cpp
static Database* globalDb = nullptr; // Bad!

TEST_CASE("Test 1") {
    globalDb = new Database(...);
    // ...
}

TEST_CASE("Test 2") {
    // Depends on Test 1 having run!
    globalDb->query(...);
}
```

### ✅ Do: Use isolated harnesses or SECTION-scoped state
```cpp
TEST_CASE("Database tests") {
    IsolatedStorage storage; // Fresh per TEST_CASE
    
    SECTION("Test 1") {
        storage.db().query(...);
    }
    
    SECTION("Test 2") {
        storage.db().query(...); // Independent!
    }
}
```

### ❌ Don't: Write overly long TEST_CASEs
```cpp
TEST_CASE("Everything about component") {
    // 500 lines of tests...
}
```

### ✅ Do: Split into focused TEST_CASEs with SECTIONs
```cpp
TEST_CASE("Component initialization", "[component][init]") {
    SECTION("with default config") { ... }
    SECTION("with custom config") { ... }
}

TEST_CASE("Component processing", "[component][process]") {
    SECTION("valid input") { ... }
    SECTION("invalid input") { ... }
}
```

---

## References

- [Catch2 Documentation](https://github.com/catchorg/Catch2/tree/devel/docs)
- [Catch2 Tutorial](https://github.com/catchorg/Catch2/blob/devel/docs/tutorial.md)
- Existing YAMS Catch2 tests (examples):
  - `tests/integration/daemon/daemon_socket_client_test.cpp` - Daemon harness, async
  - `tests/unit/metadata/search_metadata_interface_test.cpp` - Schema validation
  - `tests/unit/app/services/retrieval_schema_validation_test.cpp` - Service validation

---

**Document maintained by:** YAMS Testing Infrastructure  
**Questions/Updates:** Create issue in PBI-068
