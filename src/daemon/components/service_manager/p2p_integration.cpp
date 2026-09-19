#define YAMS_DAEMON_TEST_HOOKS_IMPL 1
#include <yams/daemon/components/ServiceManager.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

#include <yams/common/fs_utils.h>
#include <yams/daemon/components/ResourceGovernor.h>
#include <yams/memory_sync/memory_sync_config.h>
#include <yams/memory_sync/memory_sync_service.h>
#include <yams/storage/storage_runtime_resolver.h>

#ifdef _WIN32
#include <aclapi.h>
#include <io.h>
#include <windows.h>
#define getpid _getpid
#else
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#endif

#undef YAMS_DAEMON_TEST_HOOKS_IMPL

namespace {

std::string systemErrorMessage(int code) {
    return std::error_code(code, std::generic_category()).message();
}

constexpr std::size_t kMaxP2pPrivateKeyBytes = std::size_t{64} * 1024;

#ifdef _WIN32
class WindowsTokenUser {
public:
    ~WindowsTokenUser() {
        if (token_ != nullptr) {
            CloseHandle(token_);
        }
    }

    WindowsTokenUser(const WindowsTokenUser&) = delete;
    WindowsTokenUser& operator=(const WindowsTokenUser&) = delete;
    WindowsTokenUser() = default;

    yams::Result<void> initialize() {
        if (!OpenThreadToken(GetCurrentThread(), TOKEN_QUERY, TRUE, &token_)) {
            if (GetLastError() != ERROR_NO_TOKEN ||
                !OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &token_)) {
                return yams::Error{yams::ErrorCode::IOError,
                                   "cannot open effective token for P2P identity key ACL"};
            }
        }
        DWORD bytes = 0;
        GetTokenInformation(token_, TokenUser, nullptr, 0, &bytes);
        if (bytes == 0 || GetLastError() != ERROR_INSUFFICIENT_BUFFER) {
            return yams::Error{yams::ErrorCode::IOError,
                               "cannot size effective SID for P2P identity key ACL"};
        }
        user_.resize(bytes);
        if (!GetTokenInformation(token_, TokenUser, user_.data(), bytes, &bytes)) {
            return yams::Error{yams::ErrorCode::IOError,
                               "cannot read effective SID for P2P identity key ACL"};
        }
        return {};
    }

    PSID sid() const { return reinterpret_cast<const TOKEN_USER*>(user_.data())->User.Sid; }

private:
    HANDLE token_{nullptr};
    std::vector<std::byte> user_;
};

class WindowsOwnerOnlySecurity {
public:
    ~WindowsOwnerOnlySecurity() {
        if (acl_ != nullptr) {
            LocalFree(acl_);
        }
    }

    WindowsOwnerOnlySecurity(const WindowsOwnerOnlySecurity&) = delete;
    WindowsOwnerOnlySecurity& operator=(const WindowsOwnerOnlySecurity&) = delete;
    WindowsOwnerOnlySecurity() = default;

    yams::Result<SECURITY_ATTRIBUTES*> initialize() {
        if (auto initialized = user_.initialize(); !initialized) {
            return initialized.error();
        }
        EXPLICIT_ACCESSW access{};
        access.grfAccessPermissions = GENERIC_ALL;
        access.grfAccessMode = SET_ACCESS;
        access.grfInheritance = NO_INHERITANCE;
        BuildTrusteeWithSidW(&access.Trustee, user_.sid());
        const auto aclResult = SetEntriesInAclW(1, &access, nullptr, &acl_);
        if (aclResult != ERROR_SUCCESS) {
            return yams::Error{yams::ErrorCode::IOError,
                               "cannot build owner-only P2P identity key ACL"};
        }
        if (!InitializeSecurityDescriptor(&descriptor_, SECURITY_DESCRIPTOR_REVISION) ||
            !SetSecurityDescriptorOwner(&descriptor_, user_.sid(), FALSE) ||
            !SetSecurityDescriptorDacl(&descriptor_, TRUE, acl_, FALSE) ||
            !SetSecurityDescriptorControl(&descriptor_, SE_DACL_PROTECTED, SE_DACL_PROTECTED)) {
            return yams::Error{yams::ErrorCode::IOError,
                               "cannot initialize owner-only P2P identity key ACL"};
        }
        attributes_.nLength = sizeof(attributes_);
        attributes_.lpSecurityDescriptor = &descriptor_;
        attributes_.bInheritHandle = FALSE;
        return &attributes_;
    }

private:
    WindowsTokenUser user_;
    PACL acl_{nullptr};
    SECURITY_DESCRIPTOR descriptor_{};
    SECURITY_ATTRIBUTES attributes_{};
};

class WindowsHandle {
public:
    explicit WindowsHandle(HANDLE handle) : handle_(handle) {}
    ~WindowsHandle() {
        if (handle_ != INVALID_HANDLE_VALUE) {
            CloseHandle(handle_);
        }
    }
    WindowsHandle(const WindowsHandle&) = delete;
    WindowsHandle& operator=(const WindowsHandle&) = delete;
    HANDLE get() const { return handle_; }

private:
    HANDLE handle_{INVALID_HANDLE_VALUE};
};

yams::Result<std::string> readProtectedP2pFile(const std::filesystem::path& keyPath,
                                               bool ownerOnlyRead, std::size_t maxBytes) {
    WindowsHandle file(CreateFileW(keyPath.c_str(), GENERIC_READ | READ_CONTROL, 0, nullptr,
                                   OPEN_EXISTING,
                                   FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OPEN_REPARSE_POINT, nullptr));
    if (file.get() == INVALID_HANDLE_VALUE) {
        return yams::Error{yams::ErrorCode::IOError, "cannot open P2P identity key"};
    }
    FILE_ATTRIBUTE_TAG_INFO attributes{};
    if (!GetFileInformationByHandleEx(file.get(), FileAttributeTagInfo, &attributes,
                                      sizeof(attributes)) ||
        (attributes.FileAttributes & (FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT)) !=
            0 ||
        GetFileType(file.get()) != FILE_TYPE_DISK) {
        return yams::Error{yams::ErrorCode::Unauthorized,
                           "P2P identity key must be a non-reparse regular file"};
    }

    WindowsTokenUser expectedUser;
    if (auto initialized = expectedUser.initialize(); !initialized) {
        return initialized.error();
    }
    PSID owner = nullptr;
    PACL dacl = nullptr;
    PSECURITY_DESCRIPTOR rawDescriptor = nullptr;
    const auto status = GetSecurityInfo(file.get(), SE_FILE_OBJECT,
                                        OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
                                        &owner, nullptr, &dacl, nullptr, &rawDescriptor);
    std::unique_ptr<void, decltype(&LocalFree)> descriptor(rawDescriptor, LocalFree);
    if (status != ERROR_SUCCESS || owner == nullptr || dacl == nullptr ||
        !EqualSid(owner, expectedUser.sid())) {
        return yams::Error{yams::ErrorCode::Unauthorized,
                           "P2P identity key must have an explicit effective-user-only ACL"};
    }
    SECURITY_DESCRIPTOR_CONTROL control{};
    DWORD revision = 0;
    if (!GetSecurityDescriptorControl(rawDescriptor, &control, &revision) ||
        (control & SE_DACL_PROTECTED) == 0) {
        return yams::Error{yams::ErrorCode::Unauthorized,
                           "P2P identity key ACL must be protected from inheritance"};
    }
    ACL_SIZE_INFORMATION info{};
    if (!GetAclInformation(dacl, &info, sizeof(info), AclSizeInformation)) {
        return yams::Error{yams::ErrorCode::IOError, "cannot inspect P2P identity key ACL"};
    }
    bool ownerAllowed = false;
    for (DWORD index = 0; index < info.AceCount; ++index) {
        void* rawAce = nullptr;
        if (!GetAce(dacl, index, &rawAce) || rawAce == nullptr) {
            return yams::Error{yams::ErrorCode::IOError,
                               "cannot inspect P2P identity key ACL entry"};
        }
        const auto* header = static_cast<const ACE_HEADER*>(rawAce);
        if (header->AceType == ACCESS_ALLOWED_ACE_TYPE) {
            constexpr std::size_t sidOffset = offsetof(ACCESS_ALLOWED_ACE, SidStart);
            // nosemgrep: yams.cpp.suspicious-add-sizeof -- byte-count validation
            if (header->AceSize < sidOffset + sizeof(DWORD) ||
                (header->AceFlags & INHERITED_ACE) != 0) {
                return yams::Error{yams::ErrorCode::Unauthorized,
                                   "P2P identity key ACL contains an unsafe allow entry"};
            }
            const auto* ace = static_cast<const ACCESS_ALLOWED_ACE*>(rawAce);
            auto* sid = const_cast<DWORD*>(&ace->SidStart);
            if (!IsValidSid(sid) || GetLengthSid(sid) > header->AceSize - sidOffset) {
                return yams::Error{yams::ErrorCode::Unauthorized,
                                   "P2P trust file ACL contains an invalid SID"};
            }
            const bool ownerSid = EqualSid(expectedUser.sid(), sid);
            constexpr ACCESS_MASK readable =
                GENERIC_ALL | GENERIC_READ | FILE_GENERIC_READ | FILE_READ_DATA | FILE_EXECUTE;
            constexpr ACCESS_MASK writable = GENERIC_ALL | GENERIC_WRITE | FILE_GENERIC_WRITE |
                                             FILE_WRITE_DATA | FILE_APPEND_DATA | FILE_WRITE_EA |
                                             FILE_WRITE_ATTRIBUTES | FILE_DELETE_CHILD | DELETE |
                                             WRITE_DAC | WRITE_OWNER;
            if ((!ownerSid && ownerOnlyRead && ace->Mask != 0) ||
                (!ownerSid && (ace->Mask & writable) != 0)) {
                return yams::Error{yams::ErrorCode::Unauthorized,
                                   "P2P trust file ACL grants unsafe external access"};
            }
            ownerAllowed = ownerAllowed || (ownerSid && (ace->Mask & readable) != 0);
        } else if (header->AceType != ACCESS_DENIED_ACE_TYPE &&
                   header->AceType != ACCESS_DENIED_OBJECT_ACE_TYPE &&
                   header->AceType != ACCESS_DENIED_CALLBACK_ACE_TYPE &&
                   header->AceType != ACCESS_DENIED_CALLBACK_OBJECT_ACE_TYPE) {
            return yams::Error{yams::ErrorCode::Unauthorized,
                               "P2P identity key ACL contains an unsupported entry type"};
        }
    }
    if (!ownerAllowed) {
        return yams::Error{yams::ErrorCode::Unauthorized,
                           "P2P identity key ACL does not grant effective-user read access"};
    }

    LARGE_INTEGER size{};
    if (!GetFileSizeEx(file.get(), &size) || size.QuadPart <= 0 ||
        static_cast<unsigned long long>(size.QuadPart) > maxBytes) {
        return yams::Error{yams::ErrorCode::InvalidArgument,
                           "P2P identity key is empty or oversized"};
    }
    std::string content(static_cast<std::size_t>(size.QuadPart), '\0');
    std::size_t offset = 0;
    while (offset < content.size()) {
        DWORD read = 0;
        const auto remaining =
            std::min<std::size_t>(content.size() - offset, std::numeric_limits<DWORD>::max());
        if (!ReadFile(file.get(), content.data() + offset, static_cast<DWORD>(remaining), &read,
                      nullptr) ||
            read == 0) {
            return yams::Error{yams::ErrorCode::IOError, "cannot read complete P2P identity key"};
        }
        offset += read;
    }
    return content;
}
#else
yams::Result<std::string> readProtectedP2pFile(const std::filesystem::path& keyPath,
                                               bool ownerOnlyRead, std::size_t maxBytes) {
    int flags = O_RDONLY;
#ifdef O_CLOEXEC
    flags |= O_CLOEXEC;
#endif
#ifdef O_NOFOLLOW
    flags |= O_NOFOLLOW;
#endif
    const int file = ::open(keyPath.c_str(), flags);
    if (file < 0) {
        const auto openError = errno;
        return yams::Error{
            openError == ELOOP ? yams::ErrorCode::Unauthorized : yams::ErrorCode::IOError,
            openError == ELOOP ? "P2P identity key must not be a symbolic link"
                               : "cannot open P2P identity key: " + systemErrorMessage(openError)};
    }
    const auto closeFile = [&] { (void)::close(file); };
    struct stat info{};
    if (::fstat(file, &info) != 0) {
        const auto message = systemErrorMessage(errno);
        closeFile();
        return yams::Error{yams::ErrorCode::IOError, "cannot inspect P2P identity key: " + message};
    }
    const mode_t unsafePermissions = ownerOnlyRead ? mode_t{0077} : mode_t{0022};
    if (!S_ISREG(info.st_mode) || info.st_uid != ::geteuid() ||
        (info.st_mode & unsafePermissions) != 0) {
        closeFile();
        return yams::Error{yams::ErrorCode::Unauthorized,
                           "P2P trust file must be an effective-user-owned regular file without "
                           "unsafe permissions"};
    }
    if (info.st_size <= 0 ||
        static_cast<std::uintmax_t>(info.st_size) > static_cast<std::uintmax_t>(maxBytes)) {
        closeFile();
        return yams::Error{yams::ErrorCode::InvalidArgument,
                           "P2P identity key is empty or oversized"};
    }
    std::string content(static_cast<std::size_t>(info.st_size), '\0');
    std::size_t offset = 0;
    while (offset < content.size()) {
        const auto read = ::read(file, content.data() + offset, content.size() - offset);
        if (read < 0 && errno == EINTR) {
            continue;
        }
        if (read <= 0) {
            const auto message = systemErrorMessage(errno);
            closeFile();
            return yams::Error{yams::ErrorCode::IOError,
                               "cannot read complete P2P identity key: " + message};
        }
        offset += static_cast<std::size_t>(read);
    }
    closeFile();
    return content;
}
#endif

} // namespace

namespace yams::daemon::service_manager_detail {

yams::Result<std::string> readProtectedP2pPrivateKey(const std::filesystem::path& keyPath) {
    return readProtectedP2pFile(keyPath, true, kMaxP2pPrivateKeyBytes);
}

yams::Result<std::string> readProtectedP2pTrustFile(const std::filesystem::path& path,
                                                    std::size_t maxBytes) {
    return readProtectedP2pFile(path, false, maxBytes);
}

yams::Result<void> writeExclusiveP2pPrivateKey(const std::filesystem::path& path,
                                               std::string_view contents) {
#ifdef _WIN32
    WindowsOwnerOnlySecurity security;
    auto securityAttributes = security.initialize();
    if (!securityAttributes) {
        return securityAttributes.error();
    }
    HANDLE file = CreateFileW(path.c_str(), GENERIC_WRITE, 0, securityAttributes.value(),
                              CREATE_NEW, FILE_ATTRIBUTE_NORMAL, nullptr);
    if (file == INVALID_HANDLE_VALUE) {
        const auto code = GetLastError();
        return yams::Error{code == ERROR_FILE_EXISTS || code == ERROR_ALREADY_EXISTS
                               ? yams::ErrorCode::ResourceBusy
                               : yams::ErrorCode::IOError,
                           "cannot exclusively create temporary P2P identity key"};
    }
    DWORD written = 0;
    const bool writeOk =
        contents.size() <= std::numeric_limits<DWORD>::max() &&
        WriteFile(file, contents.data(), static_cast<DWORD>(contents.size()), &written, nullptr) &&
        written == contents.size() && FlushFileBuffers(file);
    const bool closeOk = CloseHandle(file);
    if (!writeOk || !closeOk) {
        std::error_code ignored;
        std::filesystem::remove(path, ignored);
        return yams::Error{yams::ErrorCode::WriteError, "cannot write P2P identity key"};
    }
#else
    int flags = O_WRONLY | O_CREAT | O_EXCL;
#ifdef O_CLOEXEC
    flags |= O_CLOEXEC;
#endif
#ifdef O_NOFOLLOW
    flags |= O_NOFOLLOW;
#endif
    const int file = ::open(path.c_str(), flags, S_IRUSR | S_IWUSR);
    if (file < 0) {
        const int openError = errno;
        return yams::Error{openError == EEXIST ? yams::ErrorCode::ResourceBusy
                                               : yams::ErrorCode::IOError,
                           "cannot exclusively create temporary P2P identity key: " +
                               systemErrorMessage(openError)};
    }
    std::size_t offset = 0;
    while (offset < contents.size()) {
        const auto written = ::write(file, contents.data() + offset, contents.size() - offset);
        if (written < 0 && errno == EINTR) {
            continue;
        }
        if (written <= 0) {
            const auto message = systemErrorMessage(errno);
            (void)::close(file);
            std::error_code ignored;
            std::filesystem::remove(path, ignored);
            return yams::Error{yams::ErrorCode::WriteError,
                               "cannot write P2P identity key: " + message};
        }
        offset += static_cast<std::size_t>(written);
    }
    const bool synced = ::fsync(file) == 0;
    const bool closed = ::close(file) == 0;
    if (!synced || !closed) {
        std::error_code ignored;
        std::filesystem::remove(path, ignored);
        return yams::Error{yams::ErrorCode::WriteError, "cannot finalize P2P identity key"};
    }
#endif
    return {};
}

} // namespace yams::daemon::service_manager_detail

namespace {

yams::Result<bool> installP2pPrivateKey(const std::filesystem::path& temporary,
                                        const std::filesystem::path& keyPath) {
#ifdef _WIN32
    if (MoveFileExW(temporary.c_str(), keyPath.c_str(), MOVEFILE_WRITE_THROUGH)) {
        return true;
    }
    const auto code = GetLastError();
    std::error_code ignored;
    std::filesystem::remove(temporary, ignored);
    if (code == ERROR_FILE_EXISTS || code == ERROR_ALREADY_EXISTS) {
        return false;
    }
    return yams::Error{yams::ErrorCode::IOError, "cannot install P2P identity key"};
#else
    if (::link(temporary.c_str(), keyPath.c_str()) == 0) {
        std::error_code ignored;
        std::filesystem::remove(temporary, ignored);
        return true;
    }
    const int installError = errno;
    std::error_code ignored;
    std::filesystem::remove(temporary, ignored);
    if (installError == EEXIST) {
        return false;
    }
    return yams::Error{yams::ErrorCode::IOError,
                       "cannot install P2P identity key: " + systemErrorMessage(installError)};
#endif
}

yams::Result<std::string> loadOrCreateP2pPrivateKey(const std::filesystem::path& keyPath) {
    std::error_code error;
    if (std::filesystem::exists(keyPath, error)) {
        return yams::daemon::service_manager_detail::readProtectedP2pPrivateKey(keyPath);
    }
    if (error) {
        return yams::Error{yams::ErrorCode::IOError,
                           "cannot inspect P2P identity key: " + error.message()};
    }
    if (!yams::common::ensureDirectories(keyPath.parent_path(), error)) {
        return yams::Error{yams::ErrorCode::IOError,
                           "cannot create P2P identity directory: " + error.message()};
    }
    auto generated = yams::memory_sync::generateWriterKeyPair();
    if (!generated) {
        return generated.error();
    }
    std::random_device random;
    for (int attempt = 0; attempt < 8; ++attempt) {
        auto temporary = keyPath;
        temporary += ".tmp." + std::to_string(static_cast<unsigned long long>(::getpid())) + "." +
                     std::to_string(static_cast<unsigned long long>(random())) + "." +
                     std::to_string(static_cast<unsigned long long>(random()));
        auto written = yams::daemon::service_manager_detail::writeExclusiveP2pPrivateKey(
            temporary, generated.value().privateKeyPem);
        if (!written) {
            if (written.error().code == yams::ErrorCode::ResourceBusy) {
                continue;
            }
            return written.error();
        }
        auto installed = installP2pPrivateKey(temporary, keyPath);
        if (!installed) {
            return installed.error();
        }
        if (!installed.value()) {
            return yams::daemon::service_manager_detail::readProtectedP2pPrivateKey(keyPath);
        }
        return std::move(generated.value().privateKeyPem);
    }
    return yams::Error{yams::ErrorCode::ResourceBusy,
                       "cannot allocate a unique temporary P2P identity key"};
}

yams::Result<std::string>
loadP2pPrivateKey(const yams::daemon::DaemonConfig::MemorySyncPolicy& policy,
                  const std::filesystem::path& dataDir) {
    if (!policy.identityKeyPath.empty()) {
        const std::filesystem::path configured(policy.identityKeyPath);
        return loadOrCreateP2pPrivateKey(configured.is_absolute() ? configured
                                                                  : dataDir / configured);
    }
    if (!policy.writerAuthManifestPath.empty()) {
        const std::filesystem::path manifestPath(policy.writerAuthManifestPath);
        auto bytes = yams::daemon::service_manager_detail::readProtectedP2pTrustFile(
            manifestPath, std::size_t{1024} * 1024);
        if (!bytes) {
            return bytes.error();
        }
        try {
            const auto manifest = nlohmann::json::parse(bytes.value());
            if (manifest.at("schema_version").get<std::uint32_t>() != 1 ||
                manifest.at("corpus_id").get<std::string>() != policy.corpusId ||
                manifest.at("corpus_epoch").get<std::uint64_t>() != policy.corpusEpoch) {
                return yams::Error{yams::ErrorCode::InvalidArgument,
                                   "writer authentication manifest corpus or epoch mismatch"};
            }
            const auto& local = manifest.at("local_key");
            if (local.at("writer_id").get<std::string>() != policy.nodeId) {
                return yams::Error{yams::ErrorCode::InvalidArgument,
                                   "writer authentication manifest local writer mismatch"};
            }
            std::filesystem::path keyPath(local.at("private_key_path").get<std::string>());
            if (!keyPath.is_absolute()) {
                keyPath = manifestPath.parent_path() / keyPath;
            }
            return yams::daemon::service_manager_detail::readProtectedP2pPrivateKey(keyPath);
        } catch (const std::exception&) {
            return yams::Error{yams::ErrorCode::InvalidArgument,
                               "writer authentication manifest is malformed"};
        }
    }
    return loadOrCreateP2pPrivateKey(dataDir / "p2p" / "identity.pem");
}

} // namespace

namespace yams::daemon {

using yams::Error;
using yams::ErrorCode;
using yams::Result;

Result<std::string>
ServiceManager::__test_loadOrCreateP2pPrivateKey(const std::filesystem::path& keyPath) {
    return loadOrCreateP2pPrivateKey(keyPath);
}

Result<void>
ServiceManager::__test_writeProtectedP2pPrivateKey(const std::filesystem::path& keyPath,
                                                   std::string_view contents) {
    return service_manager_detail::writeExclusiveP2pPrivateKey(keyPath, contents);
}

Result<p2p::P2pSyncResult> ServiceManager::connectP2p(std::string_view connectionString) {
    if (!p2pManager_) {
        return Error{ErrorCode::InvalidState, "direct P2P transport is not enabled"};
    }
    return p2pManager_->connect(connectionString);
}

Result<void> ServiceManager::disconnectP2p(std::string_view nodeId) {
    if (!p2pManager_) {
        return Error{ErrorCode::InvalidState, "direct P2P transport is not enabled"};
    }
    return p2pManager_->disconnect(nodeId);
}

Result<void> ServiceManager::enrollP2pPeer(std::string_view nodeId, std::string_view spkiPin) {
    if (!p2pManager_) {
        return Error{ErrorCode::InvalidState, "direct P2P transport is not enabled"};
    }
    return p2pManager_->enrollPeer(nodeId, spkiPin);
}

Result<void> ServiceManager::forgetP2pPeer(std::string_view nodeId) {
    if (!p2pManager_) {
        return Error{ErrorCode::InvalidState, "direct P2P transport is not enabled"};
    }
    return p2pManager_->forget(nodeId);
}

Result<p2p::P2pLocalIdentity> ServiceManager::getP2pIdentity() const {
    if (!p2pManager_) {
        return Error{ErrorCode::InvalidState, "direct P2P transport is not enabled"};
    }
    return p2pManager_->localIdentity();
}

Result<std::vector<p2p::PeerRegistryRecord>> ServiceManager::listP2pPeers() const {
    if (!p2pManager_) {
        return Error{ErrorCode::InvalidState, "direct P2P transport is not enabled"};
    }
    return p2pManager_->peers();
}

Result<void> ServiceManager::initializeDirectP2p(const std::filesystem::path& dataDir) {
    const auto& policy = config_.memorySync;
    if (!policy.enabled || policy.transport != "direct") {
        return Result<void>();
    }
    if (!memorySync_) {
        return Error{ErrorCode::InvalidState, "direct P2P requires memory sync service"};
    }
    auto listen = p2p::parseP2pConnectionString(policy.listen);
    if (!listen) {
        return Error{listen.error().code, "invalid memory_sync.listen: " + listen.error().message};
    }
    auto privateKey = loadP2pPrivateKey(policy, dataDir);
    if (!privateKey) {
        return privateKey.error();
    }
    auto synchronized = memorySync_->syncFully();
    if (!synchronized) {
        return Error{synchronized.error().code,
                     "direct P2P local op-store recovery failed: " + synchronized.error().message};
    }
    if (memorySync_->legacyUnauthenticatedHistoryObserved()) {
        return Error{ErrorCode::InvalidState,
                     "direct P2P operation store contains unsigned legacy history; preserve it "
                     "for audit, advance corpus_epoch, and bootstrap a fresh store"};
    }
    if (policy.allowFirstContact) {
        spdlog::warn("[ServiceManager] memory_sync.allow_first_contact=true permits unsolicited "
                     "P2P peer enrollment; use operator-pinned enrollment instead");
    }
    auto manager = p2p::P2pManager::create(
        p2p::P2pManagerOptions{.nodeId = policy.nodeId,
                               .corpusId = policy.corpusId,
                               .corpusEpoch = policy.corpusEpoch,
                               .privateKeyPem = std::move(privateKey.value()),
                               .databasePath = dataDir / "yams.db",
                               .listenHost = listen.value().host,
                               .listenPort = listen.value().port,
                               .allowFirstContact = policy.allowFirstContact,
                               .maxPeers = policy.maxPeers,
                               .reconnectInterval =
                                   std::chrono::milliseconds(policy.syncIntervalMs),
                               .timeout = std::chrono::seconds(10)},
        *memorySync_);
    if (!manager) {
        return manager.error();
    }
    if (auto started = manager.value()->start(); !started) {
        return started.error();
    }
    spdlog::info("[ServiceManager] direct P2P listening on {}:{} with identity {}",
                 listen.value().host, manager.value()->boundPort(), policy.nodeId);
    p2pManager_ = std::move(manager.value());
    return Result<void>();
}

} // namespace yams::daemon
