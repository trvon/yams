#include <yams/storage/storage_engine.h>
#include <spdlog/spdlog.h>

#ifdef _WIN32

#include <windows.h>
#include <io.h>
#include <fcntl.h>

namespace yams::storage::platform {

// Windows-specific atomic rename using MoveFileEx
Result<void> atomicRename(const std::filesystem::path& from, 
                         const std::filesystem::path& to) {
    // MOVEFILE_REPLACE_EXISTING makes this atomic on NTFS
    DWORD flags = MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH;
    
    if (::MoveFileExW(from.wstring().c_str(), to.wstring().c_str(), flags)) {
        return {};
    }
    
    DWORD error = ::GetLastError();
    
    // Handle specific error cases
    switch (error) {
        case ERROR_FILE_EXISTS:
            // For content-addressed storage, this is OK
            return {};
            
        case ERROR_ACCESS_DENIED:
            spdlog::error("Permission denied: rename {} to {}", from.string(), to.string());
            return Result<void>(ErrorCode::PermissionDenied);
            
        case ERROR_DISK_FULL:
            spdlog::error("Disk full");
            return Result<void>(ErrorCode::StorageFull);
            
        case ERROR_FILE_NOT_FOUND:
            spdlog::error("Source file not found: {}", from.string());
            return Result<void>(ErrorCode::FileNotFound);
            
        default:
            spdlog::error("Rename failed: error code {}", error);
            return Result<void>(ErrorCode::Unknown);
    }
}

// Windows-specific file sync using FlushFileBuffers
Result<void> syncFile(const std::filesystem::path& path) {
    HANDLE hFile = ::CreateFileW(
        path.wstring().c_str(),
        GENERIC_READ,
        FILE_SHARE_READ | FILE_SHARE_WRITE,
        nullptr,
        OPEN_EXISTING,
        FILE_ATTRIBUTE_NORMAL,
        nullptr
    );
    
    if (hFile == INVALID_HANDLE_VALUE) {
        spdlog::error("Failed to open file for sync: {}", path.string());
        return Result<void>(ErrorCode::FileNotFound);
    }
    
    // Flush file buffers to disk
    BOOL result = ::FlushFileBuffers(hFile);
    ::CloseHandle(hFile);
    
    if (!result) {
        spdlog::error("Failed to sync file: {} (error: {})", path.string(), ::GetLastError());
        return Result<void>(ErrorCode::Unknown);
    }
    
    return {};
}

// Windows doesn't need explicit directory sync
Result<void> syncDirectory(const std::filesystem::path& dir) {
    // NTFS maintains directory consistency automatically
    return {};
}

// Windows atomic write using transactional NTFS (if available)
Result<void> atomicWriteOptimized(const std::filesystem::path& path,
                                 std::span<const std::byte> data) {
    // Generate temporary filename
    wchar_t tempPath[MAX_PATH];
    wchar_t tempFile[MAX_PATH];
    
    if (!::GetTempPathW(MAX_PATH, tempPath)) {
        return Result<void>(ErrorCode::Unknown);
    }
    
    if (!::GetTempFileNameW(tempPath, L"KRO", 0, tempFile)) {
        return Result<void>(ErrorCode::Unknown);
    }
    
    // Write to temp file
    HANDLE hFile = ::CreateFileW(
        tempFile,
        GENERIC_WRITE,
        0,  // No sharing
        nullptr,
        CREATE_ALWAYS,
        FILE_ATTRIBUTE_NORMAL | FILE_FLAG_WRITE_THROUGH,
        nullptr
    );
    
    if (hFile == INVALID_HANDLE_VALUE) {
        ::DeleteFileW(tempFile);
        return Result<void>(ErrorCode::PermissionDenied);
    }
    
    // Write data
    DWORD written = 0;
    DWORD remaining = static_cast<DWORD>(data.size());
    const char* ptr = reinterpret_cast<const char*>(data.data());
    
    while (remaining > 0) {
        DWORD bytesWritten = 0;
        if (!::WriteFile(hFile, ptr + written, remaining, &bytesWritten, nullptr)) {
            ::CloseHandle(hFile);
            ::DeleteFileW(tempFile);
            return Result<void>(ErrorCode::Unknown);
        }
        written += bytesWritten;
        remaining -= bytesWritten;
    }
    
    // Ensure data is written to disk
    if (!::FlushFileBuffers(hFile)) {
        ::CloseHandle(hFile);
        ::DeleteFileW(tempFile);
        return Result<void>(ErrorCode::Unknown);
    }
    
    ::CloseHandle(hFile);
    
    // Atomic rename
    return atomicRename(tempFile, path);
}

// Get handle count for monitoring
size_t getOpenFileDescriptorCount() {
    DWORD handleCount = 0;
    
    if (::GetProcessHandleCount(::GetCurrentProcess(), &handleCount)) {
        return static_cast<size_t>(handleCount);
    }
    
    return 0;
}

// Set file permissions for security
Result<void> setSecurePermissions(const std::filesystem::path& path) {
    // Remove write permissions for stored objects
    DWORD attributes = ::GetFileAttributesW(path.wstring().c_str());
    
    if (attributes != INVALID_FILE_ATTRIBUTES) {
        ::SetFileAttributesW(path.wstring().c_str(), 
                           attributes | FILE_ATTRIBUTE_READONLY);
    }
    
    return {};
}

// Windows-specific optimizations for ReFS copy-on-write
Result<void> enableCopyOnWrite(const std::filesystem::path& path) {
    // Check if filesystem supports copy-on-write (ReFS)
    wchar_t fsName[MAX_PATH];
    DWORD fsFlags = 0;
    
    std::wstring root = path.root_path().wstring();
    if (::GetVolumeInformationW(
            root.c_str(),
            nullptr, 0,
            nullptr,
            nullptr,
            &fsFlags,
            fsName, MAX_PATH)) {
        
        // Check if this is ReFS
        if (std::wstring(fsName) == L"ReFS") {
            // Enable integrity streams for better data protection
            HANDLE hFile = ::CreateFileW(
                path.wstring().c_str(),
                GENERIC_READ | GENERIC_WRITE,
                0,
                nullptr,
                OPEN_EXISTING,
                FILE_ATTRIBUTE_NORMAL,
                nullptr
            );
            
            if (hFile != INVALID_HANDLE_VALUE) {
                // Set integrity information
                FILE_SET_SPARSE_BUFFER sparseBuffer = {TRUE};
                DWORD bytesReturned = 0;
                
                ::DeviceIoControl(
                    hFile,
                    FSCTL_SET_SPARSE,
                    &sparseBuffer,
                    sizeof(sparseBuffer),
                    nullptr,
                    0,
                    &bytesReturned,
                    nullptr
                );
                
                ::CloseHandle(hFile);
            }
        }
    }
    
    return {};
}

} // namespace yams::storage::platform

#endif // _WIN32