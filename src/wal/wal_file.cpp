#include <spdlog/spdlog.h>
#include <algorithm>
#include <cstring>
#include <filesystem>
#include <vector>
#include <yams/wal/wal_file.h>

#ifdef _WIN32
#define NOMINMAX
#define _CRT_SECURE_NO_WARNINGS
#include <fcntl.h>
#include <io.h>
#include <windows.h>
#include <sys/stat.h>
#define fstat _fstat64
#define stat _stat64
#else
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#endif

namespace yams::wal {

struct WALFile::Impl {
    std::filesystem::path path;
    int fd = -1;
    size_t fileSize = 0;
    size_t mappedSize = 0;
    void* mappedMemory = nullptr;
    Mode mode = Mode::Read;
    size_t writePos = 0;
#ifdef _WIN32
    HANDLE mappingHandle = NULL;
#endif

    Impl(std::filesystem::path p, Mode m) : path(std::move(p)), mode(m) {}

    ~Impl() {
        if (mappedMemory) {
#ifdef _WIN32
            UnmapViewOfFile(mappedMemory);
            if (mappingHandle)
                CloseHandle(mappingHandle);
#else
            munmap(mappedMemory, mappedSize);
#endif
        }
        if (fd >= 0) {
#ifdef _WIN32
            _close(fd);
#else
            ::close(fd);
#endif
        }
    }

    Result<void> mapFile() {
        // Get current file size
        struct stat st;
        if (fstat(fd, &st) < 0) {
            spdlog::error("fstat failed: {}", strerror(errno));
            return Result<void>(ErrorCode::Unknown);
        }
        fileSize = st.st_size;

        // For write mode, extend file if needed
        if (mode == Mode::Write || mode == Mode::ReadWrite) {
            size_t desiredSize = std::max(fileSize, WALFile::INITIAL_MAP_SIZE);
#ifdef _WIN32
            if (_chsize_s(fd, static_cast<__int64>(desiredSize)) != 0) {
                spdlog::error("_chsize_s failed: {}", strerror(errno));
                return Result<void>(ErrorCode::Unknown);
            }
#else
            if (ftruncate(fd, desiredSize) < 0) {
                spdlog::error("ftruncate failed: {}", strerror(errno));
                return Result<void>(ErrorCode::Unknown);
            }
#endif
            mappedSize = desiredSize;
        } else {
            mappedSize = fileSize;
        }

        // Map the file
#ifdef _WIN32
        HANDLE hFile = (HANDLE)_get_osfhandle(fd);
        if (hFile == INVALID_HANDLE_VALUE) {
            spdlog::error("_get_osfhandle failed");
            return Result<void>(ErrorCode::Unknown);
        }

        DWORD protect = (mode == Mode::Read) ? PAGE_READONLY : PAGE_READWRITE;
        DWORD access = (mode == Mode::Read) ? FILE_MAP_READ : FILE_MAP_WRITE;

        // CreateFileMapping expects size in high/low DWORDs. For < 4GB, high is 0.
        mappingHandle = CreateFileMapping(hFile, NULL, protect, 0, (DWORD)mappedSize, NULL);
        if (!mappingHandle) {
            spdlog::error("CreateFileMapping failed: {}", GetLastError());
            return Result<void>(ErrorCode::Unknown);
        }

        mappedMemory = MapViewOfFile(mappingHandle, access, 0, 0, mappedSize);
        if (!mappedMemory) {
            CloseHandle(mappingHandle);
            mappingHandle = NULL;
            spdlog::error("MapViewOfFile failed: {}", GetLastError());
            return Result<void>(ErrorCode::Unknown);
        }
#else
        int prot = (mode == Mode::Read) ? PROT_READ : (PROT_READ | PROT_WRITE);
        int flags = MAP_SHARED;

        mappedMemory = static_cast<uint8_t*>(mmap(nullptr, mappedSize, prot, flags, fd, 0));

        if (mappedMemory == MAP_FAILED) {
            mappedMemory = nullptr;
            spdlog::error("mmap failed: {}", strerror(errno));
            return Result<void>(ErrorCode::Unknown);
        }
#endif

        // For write mode, position at end of existing data
        if (mode != Mode::Read) {
            writePos = fileSize;
        }

        return Result<void>();
    }

    Result<void> growMapping(size_t requiredSize) {
        if (requiredSize <= mappedSize) {
            return Result<void>();
        }

        // Calculate new size
        size_t newSize = mappedSize;
        while (newSize < requiredSize) {
            newSize += WALFile::MAP_GROW_SIZE;
        }

        // Unmap current mapping
        if (mappedMemory) {
#ifdef _WIN32
            UnmapViewOfFile(mappedMemory);
            if (mappingHandle)
                CloseHandle(mappingHandle);
            mappingHandle = NULL;
#else
            munmap(mappedMemory, mappedSize);
#endif
            mappedMemory = nullptr;
        }

        // Extend file
#ifdef _WIN32
        if (_chsize_s(fd, static_cast<__int64>(newSize)) != 0) {
            return Result<void>(ErrorCode::Unknown);
        }
#else
        if (ftruncate(fd, newSize) < 0) {
            return Result<void>(ErrorCode::Unknown);
        }
#endif

        // Remap with new size
        mappedSize = newSize;
#ifdef _WIN32
        HANDLE hFile = (HANDLE)_get_osfhandle(fd);
        DWORD protect = PAGE_READWRITE;
        mappingHandle = CreateFileMapping(hFile, NULL, protect, 0, (DWORD)mappedSize, NULL);
        if (!mappingHandle) {
            spdlog::error("CreateFileMapping failed: {}", GetLastError());
            return Result<void>(ErrorCode::Unknown);
        }
        mappedMemory = MapViewOfFile(mappingHandle, FILE_MAP_WRITE, 0, 0, mappedSize);
        if (!mappedMemory) {
            CloseHandle(mappingHandle);
            mappingHandle = NULL;
            spdlog::error("MapViewOfFile failed: {}", GetLastError());
            return Result<void>(ErrorCode::Unknown);
        }
#else
        int prot = PROT_READ | PROT_WRITE;
        mappedMemory = static_cast<uint8_t*>(mmap(nullptr, mappedSize, prot, MAP_SHARED, fd, 0));

        if (mappedMemory == MAP_FAILED) {
            mappedMemory = nullptr;
            spdlog::error("mmap failed: {}", strerror(errno));
            return Result<void>(ErrorCode::Unknown);
        }
#endif

        return Result<void>();
    }
};

// WALFile public methods
WALFile::WALFile(const std::filesystem::path& path, Mode mode)
    : pImpl(std::make_unique<Impl>(path, mode)) {}

WALFile::~WALFile() = default;
WALFile::WALFile(WALFile&&) noexcept = default;
WALFile& WALFile::operator=(WALFile&&) noexcept = default;

Result<void> WALFile::open() {
    if (!pImpl) {
        return Result<void>(ErrorCode::InvalidOperation);
    }

    // Determine open flags
    int flags = 0;
#ifdef _WIN32
    switch (pImpl->mode) {
        case Mode::Read:
            flags = _O_RDONLY;
            break;
        case Mode::Write:
            flags = _O_RDWR | _O_CREAT;
            break;
        case Mode::ReadWrite:
            flags = _O_RDWR | _O_CREAT;
            break;
    }
    flags |= _O_BINARY;

    // Open file
    pImpl->fd = _wopen(pImpl->path.c_str(), flags, _S_IREAD | _S_IWRITE);
#else
    switch (pImpl->mode) {
        case Mode::Read:
            flags = O_RDONLY;
            break;
        case Mode::Write:
            flags = O_RDWR | O_CREAT; // Need read-write for mmap, no O_APPEND with mmap
            break;
        case Mode::ReadWrite:
            flags = O_RDWR | O_CREAT;
            break;
    }

    // Open file
    pImpl->fd = ::open(pImpl->path.c_str(), flags, 0644);
#endif

    if (pImpl->fd < 0) {
        spdlog::error("Failed to open WAL file {}: {}", pImpl->path.string(), strerror(errno));
        return Result<void>(ErrorCode::FileNotFound);
    }

    // Map the file
    return pImpl->mapFile();
}

Result<void> WALFile::close() {
    if (pImpl->mappedMemory) {
        // Sync before closing
        if (pImpl->mode != Mode::Read) {
#ifdef _WIN32
            FlushViewOfFile(pImpl->mappedMemory, pImpl->writePos);
#else
            msync(pImpl->mappedMemory, pImpl->writePos, MS_SYNC);
#endif
        }

#ifdef _WIN32
        UnmapViewOfFile(pImpl->mappedMemory);
        if (pImpl->mappingHandle) {
            CloseHandle(pImpl->mappingHandle);
            pImpl->mappingHandle = NULL;
        }
#else
        munmap(pImpl->mappedMemory, pImpl->mappedSize);
#endif
        pImpl->mappedMemory = nullptr;
    }

    if (pImpl->fd >= 0) {
        // Truncate to actual size for write mode
        if (pImpl->mode != Mode::Read && pImpl->writePos < pImpl->mappedSize) {
#ifdef _WIN32
            _chsize_s(pImpl->fd, static_cast<__int64>(pImpl->writePos));
#else
            ftruncate(pImpl->fd, pImpl->writePos);
#endif
        }

#ifdef _WIN32
        _close(pImpl->fd);
#else
        ::close(pImpl->fd);
#endif
        pImpl->fd = -1;
    }

    return Result<void>();
}

bool WALFile::isOpen() const {
    return pImpl && pImpl->fd >= 0 && pImpl->mappedMemory != nullptr;
}

Result<size_t> WALFile::append(const WALEntry& entry) {
    if (!pImpl) {
        return Result<size_t>(ErrorCode::InvalidOperation);
    }

    if (pImpl->mode == Mode::Read) {
        return Result<size_t>(ErrorCode::InvalidOperation);
    }

    if (!isOpen()) {
        return Result<size_t>(ErrorCode::InvalidOperation);
    }

    // Serialize entry
    auto serialized = entry.serialize();

    // Check if we need to grow the mapping
    size_t requiredSize = pImpl->writePos + serialized.size();
    if (requiredSize > pImpl->mappedSize) {
        auto growResult = pImpl->growMapping(requiredSize);
        if (!growResult) {
            return Result<size_t>(growResult.error());
        }
    }

    // Write to mapped memory
    std::memcpy(static_cast<uint8_t*>(pImpl->mappedMemory) + pImpl->writePos, serialized.data(),
                serialized.size());

    pImpl->writePos += serialized.size();
    pImpl->fileSize = pImpl->writePos;

    return Result<size_t>(serialized.size());
}

Result<void> WALFile::sync() {
    if (!pImpl || !isOpen() || pImpl->mode == Mode::Read) {
        return Result<void>(ErrorCode::InvalidOperation);
    }

    // Sync mapped memory (only if we've written something)
    if (pImpl->writePos > 0) {
#ifdef _WIN32
        if (!FlushViewOfFile(pImpl->mappedMemory, pImpl->writePos)) {
            spdlog::error("FlushViewOfFile failed: {}", GetLastError());
            return Result<void>(ErrorCode::Unknown);
        }
#else
        if (msync(pImpl->mappedMemory, pImpl->writePos, MS_SYNC) < 0) {
            spdlog::error("msync failed: {}", strerror(errno));
            return Result<void>(ErrorCode::Unknown);
        }
#endif
    }

    // Also sync file descriptor
#ifdef _WIN32
    if (_commit(pImpl->fd) < 0) {
#else
    if (fsync(pImpl->fd) < 0) {
#endif
        spdlog::error("fsync failed: {}", strerror(errno));
        return Result<void>(ErrorCode::Unknown);
    }

    return Result<void>();
}

// Iterator implementation
struct WALFile::Iterator::Impl {
    WALFile* file;
    size_t position;

    Impl(WALFile* f, size_t pos) : file(f), position(pos) {}

    std::optional<WALEntry> readNext() {
        if (!file || !file->isOpen()) {
            return std::nullopt;
        }

        // Use writePos for write mode files, fileSize for read mode
        size_t dataEnd =
            (file->pImpl->mode != Mode::Read) ? file->pImpl->writePos : file->pImpl->fileSize;

        if (position >= dataEnd) {
            return std::nullopt;
        }

        // Check if we have enough bytes for header
        if (position + sizeof(WALEntry::Header) > dataEnd) {
            return std::nullopt;
        }

        // Read header
        WALEntry::Header header;
        std::memcpy(&header, static_cast<uint8_t*>(file->pImpl->mappedMemory) + position,
                    sizeof(header));

        // Validate header
        if (!header.isValid()) {
            return std::nullopt;
        }

        // Check if we have enough bytes for full entry
        size_t entrySize = sizeof(WALEntry::Header) + header.dataSize;
        if (position + entrySize > dataEnd) {
            return std::nullopt;
        }

        // Deserialize entry
        std::span<const std::byte> entryData(
            reinterpret_cast<const std::byte*>(static_cast<uint8_t*>(file->pImpl->mappedMemory) +
                                               position),
            entrySize);

        auto entry = WALEntry::deserialize(entryData);
        if (entry) {
            position += entrySize;
        }

        return entry;
    }
};

WALFile::Iterator::Iterator(WALFile* file, size_t position)
    : pImpl(std::make_shared<Impl>(file, position)) {}

bool WALFile::Iterator::operator==(const Iterator& other) const noexcept {
    if (!pImpl && !other.pImpl)
        return true;
    if (!pImpl || !other.pImpl)
        return false;
    return pImpl->file == other.pImpl->file && pImpl->position == other.pImpl->position;
}

bool WALFile::Iterator::operator!=(const Iterator& other) const noexcept {
    return !(*this == other);
}

WALFile::Iterator& WALFile::Iterator::operator++() {
    if (pImpl) {
        // Try to read next entry to advance position
        auto entry = pImpl->readNext();
        if (!entry) {
            // If no valid entry, move to end
            size_t dataEnd =
                (pImpl->file && pImpl->file->pImpl && pImpl->file->pImpl->mode != Mode::Read)
                    ? pImpl->file->pImpl->writePos
                    : (pImpl->file && pImpl->file->pImpl ? pImpl->file->pImpl->fileSize : 0);
            pImpl->position = dataEnd;
        }
    }
    return *this;
}

WALFile::Iterator WALFile::Iterator::operator++(int) {
    Iterator tmp = *this;
    ++(*this);
    return tmp;
}

std::optional<WALEntry> WALFile::Iterator::operator*() const {
    if (!pImpl)
        return std::nullopt;

    // Save current position
    size_t savedPos = pImpl->position;

    // Read entry
    auto entry = pImpl->readNext();

    // Restore position (reading is non-destructive for iterator)
    pImpl->position = savedPos;

    return entry;
}

WALFile::Iterator WALFile::begin() {
    return Iterator(this, 0);
}

WALFile::Iterator WALFile::end() {
    // For write mode, use writePos as the actual end of data
    // For read mode, use fileSize
    size_t endPos =
        (pImpl && pImpl->mode != Mode::Read) ? pImpl->writePos : (pImpl ? pImpl->fileSize : 0);
    return Iterator(this, endPos);
}

size_t WALFile::getSize() const {
    return pImpl ? pImpl->fileSize : 0;
}

std::filesystem::path WALFile::getPath() const {
    return pImpl ? pImpl->path : std::filesystem::path();
}

bool WALFile::canWrite() const {
    return pImpl && pImpl->mode != Mode::Read;
}

} // namespace yams::wal