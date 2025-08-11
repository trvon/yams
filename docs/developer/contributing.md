# Contributing to YAMS

Thank you for your interest in contributing to YAMS! This guide provides everything you need to know to contribute effectively to the project.

## 🚀 Quick Start for Contributors

1. **[Set up your development environment](#development-environment-setup)**
2. **[Find an issue to work on](#finding-issues-to-work-on)**
3. **[Fork and clone the repository](#forking-and-cloning)**
4. **[Make your changes](#making-changes)**
5. **[Run tests and ensure quality](#testing-and-quality-assurance)**
6. **[Submit a pull request](#submitting-pull-requests)**

### Development Environment Setup

#### Prerequisites
- **C++20 compatible compiler** (GCC 11+, Clang 14+, or MSVC 2022+)
- **CMake 3.20+**
- **Git 2.20+**
- **Platform-specific dependencies** (see [Installation Guide](../user_guide/installation.md))

#### Quick Setup
```bash
# 1. Fork the repository on GitHub
# 2. Clone your fork
git clone https://github.com/YOUR_USERNAME/yams.git
cd yams

# 3. Add upstream remote
git remote add upstream https://github.com/trvon/yams.git

# 4. Install dependencies (macOS example)
brew install cmake openssl@3 protobuf sqlite3 gcovr

# 5. Create development build
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Debug \
      -DYAMS_ENABLE_COVERAGE=ON \
      -DYAMS_ENABLE_SANITIZERS=ON \
      -DYAMS_BUILD_TESTS=ON \
      ..

# 6. Build and test
make -j$(nproc) && ctest --output-on-failure
```

## 🎯 Ways to Contribute

### Code Contributions
- **Bug fixes**: Fix issues reported in [GitHub Issues](https://github.com/trvon/yams/issues)
- **New features**: Implement features from the [roadmap](../ROADMAP.md)
- **Performance improvements**: Optimize critical code paths
- **Test coverage**: Add tests for untested code
- **Code quality**: Refactor and improve existing code

### Documentation Contributions
- **API documentation**: Improve code comments and API docs
- **User guides**: Create tutorials and usage examples
- **Developer docs**: Document architecture and design decisions
- **Translations**: Translate documentation to other languages

### Community Contributions  
- **Issue triage**: Help categorize and reproduce bug reports
- **Code reviews**: Review pull requests from other contributors
- **Community support**: Answer questions in discussions
- **Testing**: Test pre-release versions and report issues

## 📋 Development Workflow

### Finding Issues to Work On

#### Good First Issues
Look for issues labeled with:
- `good first issue` - Beginner-friendly tasks
- `help wanted` - Issues where we need help
- `bug` - Bug fixes (usually well-defined)
- `documentation` - Documentation improvements

#### Feature Development
- Check the [project roadmap](../ROADMAP.md)
- Review [Product Backlog Items](../delivery/backlog.md)
- Discuss new features in [GitHub Discussions](https://github.com/trvon/yams/discussions)

### Forking and Cloning
```bash
# 1. Fork on GitHub (click Fork button)

# 2. Clone your fork
git clone https://github.com/YOUR_USERNAME/yams.git
cd yams

# 3. Add upstream remote
git remote add upstream https://github.com/trvon/yams.git

# 4. Verify remotes
git remote -v
# origin    https://github.com/YOUR_USERNAME/yams.git (fetch)
# origin    https://github.com/YOUR_USERNAME/yams.git (push)
# upstream  https://github.com/trvon/yams.git (fetch)
# upstream  https://github.com/trvon/yams.git (push)
```

### Branch Management
```bash
# Always work on feature branches
git checkout -b feature/your-feature-name

# Keep your main branch up to date
git checkout main
git pull upstream main
git push origin main

# Rebase your feature branch regularly
git checkout feature/your-feature-name
git rebase main
```

### Making Changes

#### Code Style and Standards
- **C++ Standard**: Use C++20 features appropriately
- **Naming Conventions**:
  - Classes: `PascalCase` (e.g., `ContentStore`)
  - Functions/Variables: `camelCase` (e.g., `storeContent`)
  - Constants: `UPPER_SNAKE_CASE` (e.g., `MAX_CHUNK_SIZE`)
  - Files: `snake_case` (e.g., `content_store.h`)
- **Formatting**: Use `.clang-format` configuration
- **Comments**: Document public APIs with Doxygen-style comments

#### Code Quality Rules
```cpp
// Good: Clear, descriptive names
Result<ContentInfo> ContentStore::storeContent(
    const std::filesystem::path& filePath,
    const ContentMetadata& metadata) {
    
    // Good: Use early returns
    if (!std::filesystem::exists(filePath)) {
        return Error{ErrorCode::FileNotFound, "File does not exist"};
    }
    
    // Good: RAII and smart pointers
    auto hasher = crypto::createSHA256Hasher();
    auto hash = hasher->hashFile(filePath);
    
    // Good: Modern C++ features
    return ContentInfo{
        .hash = std::move(hash),
        .size = std::filesystem::file_size(filePath),
        .createdAt = std::chrono::system_clock::now()
    };
}
```

#### Testing Requirements
Every contribution must include appropriate tests:

```cpp
// Unit test example
TEST(ContentStoreTest, StoreValidFile) {
    ContentStore store("/tmp/test_store");
    ContentMetadata metadata{.tags = {"test", "document"}};
    
    auto result = store.storeContent("test_file.txt", metadata);
    
    ASSERT_TRUE(result.has_value());
    EXPECT_FALSE(result.value().hash.empty());
    EXPECT_GT(result.value().size, 0);
}

// Integration test example  
TEST(ContentStoreIntegrationTest, StoreAndRetrieveWorkflow) {
    // Test complete workflow from storage to retrieval
    ContentStore store("/tmp/integration_test");
    
    // Store a file
    auto storeResult = store.storeContent("input.txt", {});
    ASSERT_TRUE(storeResult.has_value());
    
    // Retrieve the file
    auto retrieveResult = store.retrieveContent(
        storeResult.value().hash, "output.txt");
    ASSERT_TRUE(retrieveResult.has_value());
    
    // Verify contents match
    EXPECT_TRUE(filesAreIdentical("input.txt", "output.txt"));
}
```

### Commit Guidelines

#### Commit Message Format
```
type(scope): brief description

Detailed explanation of the change, including:
- What was changed and why
- Any breaking changes
- Related issue numbers

Fixes #123
```

#### Commit Types
- `feat`: New feature
- `fix`: Bug fix  
- `docs`: Documentation changes
- `style`: Code style/formatting changes
- `refactor`: Code refactoring
- `test`: Adding or modifying tests
- `perf`: Performance improvements
- `build`: Build system changes
- `ci`: CI/CD changes

#### Examples
```bash
git commit -m "feat(storage): add compression support for large files

Implement Zstandard compression for files larger than 1MB to reduce
storage requirements. Compression is applied automatically based on
configurable size thresholds.

- Add ZstandardCompressor class
- Integrate compression into storage pipeline  
- Add configuration options for compression settings
- Include comprehensive tests for compression scenarios

Fixes #456"
```

## 🧪 Testing and Quality Assurance

### Running Tests
```bash
# Run all tests
ctest --output-on-failure

# Run specific test categories
ctest -L unit
ctest -L integration  
ctest -L performance

# Run tests with coverage
make coverage

# Run specific test executable
./tests/unit/storage/storage_tests
./tests/integration/full_system_test
```

### Code Coverage
```bash
# Generate coverage report
make coverage-html

# View coverage report
open build/coverage/html/index.html  # macOS
xdg-open build/coverage/html/index.html  # Linux

# Coverage requirements
# - New code: 90%+ line coverage
# - Modified code: No decrease in coverage
# - Critical paths: 100% coverage
```

### Static Analysis
```bash
# Run clang-tidy
clang-tidy src/**/*.cpp -- -I include

# Run cppcheck  
cppcheck --enable=all src/ include/

# Run AddressSanitizer (if enabled)
ASAN_OPTIONS=abort_on_error=1 ./tests/unit/all_tests
```

### Performance Testing
```bash
# Run performance benchmarks
./benchmarks/yams_benchmarks

# Measure specific operations
./benchmarks/storage_benchmark --size=1GB
./benchmarks/search_benchmark --queries=10000

# Performance requirements:
# - No regressions >10% in critical paths
# - Memory usage should not increase significantly
# - Startup time should remain under 1 second
```

## 🔄 Submitting Pull Requests

### Pre-Submission Checklist
- [ ] **Tests pass**: All tests pass locally
- [ ] **Coverage maintained**: Code coverage meets requirements
- [ ] **Documentation updated**: API docs and user guides updated
- [ ] **Changelog entry**: Added entry to CHANGELOG.md
- [ ] **Commit messages**: Follow conventional commit format
- [ ] **Branch up-to-date**: Rebased against latest main
- [ ] **Self-review**: Code has been self-reviewed

### Pull Request Process
```bash
# 1. Push your feature branch
git push origin feature/your-feature-name

# 2. Create PR on GitHub
# - Use descriptive title and description
# - Link related issues
# - Add appropriate labels
# - Request reviews from relevant maintainers

# 3. Address feedback
# - Make changes based on review comments
# - Push additional commits
# - Request re-review when ready

# 4. Merge
# - Maintainer will merge after approval
# - Delete your feature branch after merge
```

### Pull Request Template
```markdown
## Description
Brief description of the changes and their purpose.

## Type of Change
- [ ] Bug fix (non-breaking change fixing an issue)
- [ ] New feature (non-breaking change adding functionality)
- [ ] Breaking change (fix or feature causing existing functionality to change)
- [ ] Documentation update

## Testing
- [ ] Unit tests added/updated
- [ ] Integration tests added/updated
- [ ] All tests pass locally
- [ ] Coverage requirements met

## Checklist
- [ ] Code follows project style guidelines
- [ ] Self-review completed
- [ ] Documentation updated
- [ ] Changelog entry added

## Related Issues
Fixes #(issue_number)
```

## 🏗️ Architecture Guidelines

### Design Principles
- **Modularity**: Components should be loosely coupled
- **Performance**: Optimize for common use cases
- **Safety**: Use RAII, smart pointers, and proper error handling
- **Testability**: Design for easy unit testing
- **Documentation**: Self-documenting code with clear APIs

### Module Structure
```cpp
// Example module structure
namespace yams::storage {
    // Interface (pure virtual)
    class IStorageEngine {
    public:
        virtual ~IStorageEngine() = default;
        virtual Result<Hash> store(ByteSpan data) = 0;
        virtual Result<ByteVector> retrieve(const Hash& hash) = 0;
    };
    
    // Implementation
    class StorageEngine : public IStorageEngine {
    public:
        explicit StorageEngine(Config config);
        
        Result<Hash> store(ByteSpan data) override;
        Result<ByteVector> retrieve(const Hash& hash) override;
        
    private:
        class Impl;  // Pimpl idiom for stable ABI
        std::unique_ptr<Impl> pImpl;
    };
    
    // Factory function
    std::unique_ptr<IStorageEngine> createStorageEngine(Config config);
}
```

### Error Handling
```cpp
// Use Result<T> type for operations that can fail
Result<ContentInfo> storeContent(const std::filesystem::path& path) {
    if (!std::filesystem::exists(path)) {
        return Error{ErrorCode::FileNotFound, "File does not exist"};
    }
    
    try {
        // Actual implementation
        return ContentInfo{/* ... */};
    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError, e.what()};
    }
}

// Usage
auto result = storeContent("file.txt");
if (!result) {
    spdlog::error("Failed to store content: {}", result.error().message);
    return;
}
auto info = result.value();
```

## 📝 Documentation Standards

### Code Documentation
```cpp
/**
 * @brief Stores content with automatic deduplication
 * 
 * This function stores the given content using SHA-256 content addressing.
 * If identical content already exists, no duplicate storage occurs.
 * 
 * @param data The content to store
 * @param metadata Associated metadata for the content  
 * @return Result containing the content hash or error information
 * 
 * @throws std::bad_alloc If memory allocation fails
 * 
 * @example
 * ```cpp
 * ContentStore store("/path/to/storage");
 * auto result = store.storeContent(fileData, {.tags = {"document"}});
 * if (result) {
 *     std::cout << "Stored with hash: " << result.value().hash << std::endl;
 * }
 * ```
 */
Result<ContentInfo> storeContent(ByteSpan data, const ContentMetadata& metadata);
```

### Markdown Documentation
- Use clear headings and sections
- Include code examples for all features
- Add diagrams for complex concepts
- Link between related documents
- Keep language concise but complete

## 🤝 Community Guidelines

### Communication
- **Be respectful**: Treat all community members with respect
- **Be constructive**: Provide helpful feedback and suggestions
- **Be patient**: Remember that contributors have different experience levels
- **Be inclusive**: Welcome contributors from all backgrounds

### Code Review Guidelines
- **Focus on code, not the person**: Comment on the code, not the author
- **Be specific**: Provide clear, actionable feedback
- **Explain reasoning**: Help others learn by explaining your suggestions
- **Approve quickly**: Don't delay approval for minor style issues
- **Test thoroughly**: Verify that changes work as intended

### Issue Reporting
- **Search first**: Check if the issue already exists
- **Use templates**: Follow the provided issue templates
- **Provide details**: Include reproduction steps, system info, and logs
- **Be responsive**: Reply to requests for additional information

## 🎖️ Recognition

### Contributors
All contributors are recognized in:
- [CONTRIBUTORS.md](../CONTRIBUTORS.md) file
- GitHub contributors page
- Release notes for their contributions
- Special recognition for significant contributions

### Maintainer Path
Regular contributors may be invited to become maintainers:
1. **Consistent contributions** over 6+ months
2. **Code quality** and adherence to guidelines  
3. **Community engagement** through reviews and discussions
4. **Domain expertise** in specific areas of the codebase

## 📚 Additional Resources

### Development Resources
- [Development Setup Guide](setup.md)
- [Architecture Documentation](architecture/README.md)
- [Testing Guide](testing/README.md)
- [Build System Documentation](build_system.md)

### External Resources
- [C++20 Reference](https://en.cppreference.com/w/cpp/20)
- [CMake Documentation](https://cmake.org/documentation/)
- [Google Test Guide](https://google.github.io/googletest/)
- [Conventional Commits](https://www.conventionalcommits.org/)

---

**Questions?** 
- Join [GitHub Discussions](https://github.com/trvon/yams/discussions)
- Check the [FAQ](../user_guide/faq.md)
- Review existing [issues and PRs](https://github.com/trvon/yams)

Thank you for contributing to YAMS! 🚀