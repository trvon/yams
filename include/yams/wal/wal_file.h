#pragma once

#include <yams/core/types.h>
#include <yams/wal/wal_entry.h>

#include <filesystem>
#include <memory>
#include <optional>
#include <string>

namespace yams::wal {

class WALFile {
public:
    enum class Mode { Read, Write, ReadWrite };

    class Iterator;

    WALFile(const std::filesystem::path& path, Mode mode);
    ~WALFile();

    WALFile(WALFile&&) noexcept;
    WALFile& operator=(WALFile&&) noexcept;
    WALFile(const WALFile&) = delete;
    WALFile& operator=(const WALFile&) = delete;

    Result<void> open();
    Result<void> close();
    bool isOpen() const;

    Result<size_t> append(const WALEntry& entry);
    Result<void> sync();

    Iterator begin();
    Iterator end();

    size_t getSize() const;
    std::filesystem::path getPath() const;
    bool canWrite() const;

    static constexpr size_t INITIAL_MAP_SIZE = 1024 * 1024; // 1MB
    static constexpr size_t MAP_GROW_SIZE = 1024 * 1024;    // 1MB

    class Iterator {
    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = WALEntry;
        using difference_type = std::ptrdiff_t;
        using pointer = const WALEntry*;
        using reference = const WALEntry&;

        Iterator(WALFile* file, size_t position);
        Iterator() : pImpl(nullptr) {}

        bool operator==(const Iterator& other) const noexcept;
        bool operator!=(const Iterator& other) const noexcept;

        Iterator& operator++();
        Iterator operator++(int);

        std::optional<WALEntry> operator*() const;

    private:
        struct Impl;
        std::shared_ptr<Impl> pImpl;
    };

private:
    struct Impl;
    std::unique_ptr<Impl> pImpl;
};

} // namespace yams::wal
