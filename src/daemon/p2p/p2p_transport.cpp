// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#define YAMS_DAEMON_TEST_HOOKS_IMPL 1
#include <yams/daemon/p2p/p2p_transport.h>
#undef YAMS_DAEMON_TEST_HOOKS_IMPL

// pi-lens-ignore: fatal error
#include <boost/asio/connect.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/write.hpp>

#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/sha.h>
#include <openssl/x509.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cstring>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <thread>
#include <utility>

namespace yams::daemon::p2p {

std::string toHex(std::span<const std::byte> bytes) {
    static constexpr char kHex[] = "0123456789abcdef";
    std::string out;
    out.reserve(bytes.size() * 2);
    for (const std::byte byte : bytes) {
        const auto value = static_cast<unsigned char>(byte);
        out.push_back(kHex[value >> 4]);
        out.push_back(kHex[value & 0xf]);
    }
    return out;
}

namespace {

struct PkeyDeleter {
    void operator()(EVP_PKEY* key) const noexcept { EVP_PKEY_free(key); }
};
struct X509Deleter {
    void operator()(X509* cert) const noexcept { X509_free(cert); }
};
using Pkey = std::shared_ptr<EVP_PKEY>;
using Cert = std::shared_ptr<X509>;
using Tcp = boost::asio::ip::tcp;
using TlsStream = boost::asio::ssl::stream<Tcp::socket>;

std::mutex gAcceptLoopHookMutex;
std::function<void()> gAcceptLoopHook;
std::mutex gHandshakeStartedHookMutex;
std::function<void()> gHandshakeStartedHook;
std::mutex gAfterAcceptJoinHookMutex;
std::function<void()> gAfterAcceptJoinHook;
thread_local const void* gSessionWorkerOwner = nullptr;

void invokeAcceptLoopHook() {
    std::function<void()> hook;
    {
        std::lock_guard<std::mutex> lock(gAcceptLoopHookMutex);
        hook = gAcceptLoopHook;
    }
    if (hook) {
        hook();
    }
}

void invokeHandshakeStartedHook() {
    std::function<void()> hook;
    {
        std::lock_guard<std::mutex> lock(gHandshakeStartedHookMutex);
        hook = gHandshakeStartedHook;
    }
    if (hook) {
        hook();
    }
}

void invokeAfterAcceptJoinHook() {
    std::function<void()> hook;
    {
        std::lock_guard<std::mutex> lock(gAfterAcceptJoinHookMutex);
        hook = gAfterAcceptJoinHook;
    }
    if (hook) {
        hook();
    }
}

bool validOperationTimeout(std::chrono::milliseconds timeout) {
    return timeout > std::chrono::milliseconds::zero() && timeout <= kP2pMaxOperationTimeout;
}

bool validSessionTimeout(std::chrono::milliseconds timeout) {
    return timeout > std::chrono::milliseconds::zero() && timeout <= kP2pMaxSessionTimeout;
}

Result<Pkey> loadPrivateKey(std::string_view pem) {
    auto* bio = BIO_new_mem_buf(pem.data(), static_cast<int>(pem.size()));
    if (bio == nullptr) {
        return Error{ErrorCode::InvalidData, "failed to allocate key PEM buffer"};
    }
    std::unique_ptr<BIO, decltype(&BIO_free)> bioGuard(bio, BIO_free);
    auto* raw = PEM_read_bio_PrivateKey(bio, nullptr, nullptr, nullptr);
    if (raw == nullptr) {
        return Error{ErrorCode::InvalidData, "failed to parse Ed25519 private key PEM"};
    }
    auto key = Pkey(raw, PkeyDeleter{});
    if (EVP_PKEY_base_id(key.get()) != EVP_PKEY_ED25519) {
        return Error{ErrorCode::InvalidData, "p2p TLS identity requires an Ed25519 key"};
    }
    return key;
}

std::string sha256Hex(std::span<const std::byte> bytes) {
    unsigned char digest[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char*>(bytes.data()), bytes.size(), digest);
    return toHex(std::span<const std::byte>(reinterpret_cast<const std::byte*>(digest),
                                            SHA256_DIGEST_LENGTH));
}

Result<std::string> computeSpkiPin(EVP_PKEY* key) {
    unsigned char* der = nullptr;
    const int length = i2d_PUBKEY(key, &der);
    if (length <= 0 || der == nullptr) {
        return Error{ErrorCode::InternalError, "failed to serialize public key"};
    }
    std::vector<std::byte> bytes(static_cast<std::size_t>(length));
    std::memcpy(bytes.data(), der, bytes.size());
    OPENSSL_free(der);
    return sha256Hex(bytes);
}

std::string certificatePin(X509* cert) {
    unsigned char* der = nullptr;
    const int length = i2d_X509_PUBKEY(X509_get_X509_PUBKEY(cert), &der);
    if (length <= 0 || der == nullptr) {
        return {};
    }
    std::vector<std::byte> bytes(static_cast<std::size_t>(length));
    std::memcpy(bytes.data(), der, bytes.size());
    OPENSSL_free(der);
    return sha256Hex(bytes);
}

std::string certificateCommonName(X509* cert) {
    char name[256] = {0};
    const int length = X509_NAME_get_text_by_NID(X509_get_subject_name(cert), NID_commonName, name,
                                                 static_cast<int>(sizeof(name)));
    return length > 0 ? std::string(name, static_cast<std::size_t>(length)) : std::string{};
}

Result<Cert> makeSelfSignedCertificate(EVP_PKEY* key, std::string_view nodeId) {
    auto cert = Cert(X509_new(), X509Deleter{});
    if (!cert) {
        return Error{ErrorCode::InternalError, "failed to allocate X509"};
    }
    if (X509_set_version(cert.get(), 2) != 1 ||
        ASN1_INTEGER_set(X509_get_serialNumber(cert.get()), 1) != 1 ||
        X509_gmtime_adj(X509_get_notBefore(cert.get()), -60) == nullptr ||
        X509_gmtime_adj(X509_get_notAfter(cert.get()), 365L * 24 * 3600) == nullptr ||
        X509_set_pubkey(cert.get(), key) != 1) {
        return Error{ErrorCode::InternalError, "failed to initialize X509 certificate"};
    }
    X509_NAME* name = X509_get_subject_name(cert.get());
    const std::string node(nodeId);
    if (name == nullptr ||
        X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC,
                                   reinterpret_cast<const unsigned char*>(node.c_str()), -1, -1,
                                   0) != 1 ||
        X509_set_issuer_name(cert.get(), name) != 1 || X509_sign(cert.get(), key, nullptr) <= 0) {
        return Error{ErrorCode::InternalError, "failed to self-sign X509 certificate"};
    }
    return cert;
}

std::string certificatePem(X509* cert) {
    auto* bio = BIO_new(BIO_s_mem());
    if (bio == nullptr) {
        return {};
    }
    std::unique_ptr<BIO, decltype(&BIO_free)> bioGuard(bio, BIO_free);
    if (PEM_write_bio_X509(bio, cert) != 1) {
        return {};
    }
    char* data = nullptr;
    const long length = BIO_get_mem_data(bio, &data);
    return length > 0 && data != nullptr ? std::string(data, static_cast<std::size_t>(length))
                                         : std::string{};
}

Result<void> configureMutualTls(boost::asio::ssl::context& context, const TlsIdentity& identity) {
    context.set_options(boost::asio::ssl::context::default_workarounds |
                        boost::asio::ssl::context::no_sslv2 | boost::asio::ssl::context::no_sslv3 |
                        boost::asio::ssl::context::no_tlsv1 |
                        boost::asio::ssl::context::no_tlsv1_1);
    if (SSL_CTX_set_min_proto_version(context.native_handle(), TLS1_3_VERSION) != 1) {
        return Error{ErrorCode::InternalError, "failed to require TLS 1.3"};
    }
    context.set_verify_mode(boost::asio::ssl::verify_peer |
                            boost::asio::ssl::verify_fail_if_no_peer_cert);
    // Certificates are self-signed by design. SPKI pinning is the trust anchor.
    context.set_verify_callback([](bool, boost::asio::ssl::verify_context&) { return true; });

    auto key = loadPrivateKey(identity.privateKeyPemForTransport());
    if (!key) {
        return key.error();
    }
    if (SSL_CTX_use_PrivateKey(context.native_handle(), key.value().get()) != 1) {
        return Error{ErrorCode::InternalError, "failed to load TLS private key"};
    }
    const std::string pem = identity.certPem();
    auto* bio = BIO_new_mem_buf(pem.data(), static_cast<int>(pem.size()));
    if (bio == nullptr) {
        return Error{ErrorCode::InternalError, "failed to allocate certificate PEM buffer"};
    }
    std::unique_ptr<BIO, decltype(&BIO_free)> bioGuard(bio, BIO_free);
    auto* rawCert = PEM_read_bio_X509(bio, nullptr, nullptr, nullptr);
    if (rawCert == nullptr) {
        return Error{ErrorCode::InternalError, "failed to parse TLS certificate"};
    }
    std::unique_ptr<X509, decltype(&X509_free)> cert(rawCert, X509_free);
    if (SSL_CTX_use_certificate(context.native_handle(), cert.get()) != 1 ||
        SSL_CTX_check_private_key(context.native_handle()) != 1) {
        return Error{ErrorCode::InternalError, "TLS certificate does not match private key"};
    }
    return {};
}

struct TimedOperation {
    boost::system::error_code error;
    bool timedOut{false};
};

template <typename StartOperation>
TimedOperation runTimed(boost::asio::io_context& io, Tcp::socket& socket,
                        std::chrono::milliseconds timeout, StartOperation&& startOperation) {
    boost::system::error_code operationError;
    bool done = false;
    bool timedOut = false;
    boost::asio::steady_timer timer(io);
    timer.expires_after(timeout);
    timer.async_wait([&](const boost::system::error_code& timerError) {
        if (!timerError && !done) {
            timedOut = true;
            boost::system::error_code ignored;
            // NOLINTNEXTLINE(bugprone-unused-return-value) -- best-effort timeout cancel.
            socket.cancel(ignored);
        }
    });
    auto completion = [&](const boost::system::error_code& error, auto&&...) {
        operationError = error;
        done = true;
        (void)timer.cancel();
    };
    std::forward<StartOperation>(startOperation)(completion);
    io.restart();
    io.run();
    return TimedOperation{operationError, timedOut};
}

Error transportError(const TimedOperation& operation, std::string_view action,
                     bool locallyClosed = false) {
    if (operation.timedOut) {
        return Error{ErrorCode::Timeout, std::string(action) + " timed out"};
    }
    if (locallyClosed || operation.error == boost::asio::error::operation_aborted) {
        return Error{ErrorCode::OperationCancelled, std::string(action) + " cancelled"};
    }
    if (operation.error == boost::asio::error::eof ||
        operation.error == boost::asio::error::connection_reset ||
        operation.error == boost::asio::error::broken_pipe) {
        return Error{ErrorCode::NotFound, "p2p peer closed the channel"};
    }
    return Error{ErrorCode::NetworkError,
                 std::string(action) + " failed: " + operation.error.message()};
}

std::array<std::byte, 4> encodeLength(std::size_t length) {
    return {std::byte((length >> 24U) & 0xffU), std::byte((length >> 16U) & 0xffU),
            std::byte((length >> 8U) & 0xffU), std::byte(length & 0xffU)};
}

std::size_t decodeLength(const std::array<std::byte, 4>& header) {
    return (std::size_t(static_cast<unsigned char>(header[0])) << 24U) |
           (std::size_t(static_cast<unsigned char>(header[1])) << 16U) |
           (std::size_t(static_cast<unsigned char>(header[2])) << 8U) |
           std::size_t(static_cast<unsigned char>(header[3]));
}

} // namespace

class TlsIdentity::Impl {
public:
    std::string nodeId;
    std::string privateKeyPem;
    std::string certificatePem;
    std::string pin;
};

Result<TlsIdentity> TlsIdentity::fromPrivateKeyPem(std::string nodeId, std::string privateKeyPem) {
    if (nodeId.empty() || nodeId.size() > 255 || nodeId.find('\0') != std::string::npos) {
        return Error{ErrorCode::InvalidArgument,
                     "p2p TLS identity requires a non-empty node id (max 255 bytes)"};
    }
    auto key = loadPrivateKey(privateKeyPem);
    if (!key) {
        return key.error();
    }
    auto pin = computeSpkiPin(key.value().get());
    if (!pin) {
        return pin.error();
    }
    auto cert = makeSelfSignedCertificate(key.value().get(), nodeId);
    if (!cert) {
        return cert.error();
    }
    auto pem = certificatePem(cert.value().get());
    if (pem.empty()) {
        return Error{ErrorCode::InternalError, "failed to serialize TLS certificate"};
    }
    auto impl = std::make_shared<Impl>();
    impl->nodeId = std::move(nodeId);
    impl->privateKeyPem = std::move(privateKeyPem);
    impl->certificatePem = std::move(pem);
    impl->pin = std::move(pin.value());
    return TlsIdentity(std::move(impl));
}

TlsIdentity::TlsIdentity(std::shared_ptr<Impl> impl) : impl_(std::move(impl)) {}
TlsIdentity::TlsIdentity(TlsIdentity&&) noexcept = default;
TlsIdentity& TlsIdentity::operator=(TlsIdentity&&) noexcept = default;
TlsIdentity::~TlsIdentity() = default;
std::string TlsIdentity::nodeId() const {
    return impl_ ? impl_->nodeId : std::string{};
}
std::string TlsIdentity::certPem() const {
    return impl_ ? impl_->certificatePem : std::string{};
}
std::string TlsIdentity::spkiPin() const {
    return impl_ ? impl_->pin : std::string{};
}
std::string_view TlsIdentity::privateKeyPemForTransport() const {
    return impl_ ? std::string_view(impl_->privateKeyPem) : std::string_view{};
}

class P2pConnection::Impl : public std::enable_shared_from_this<P2pConnection::Impl> {
public:
    Impl(boost::asio::ssl::context::method method, std::string nodeId)
        : context(method), localNodeId(std::move(nodeId)) {}

    boost::asio::io_context io;
    boost::asio::ssl::context context;
    std::unique_ptr<TlsStream> stream;
    std::string localNodeId;
    std::string peerPin;
    std::string peerCn;
    std::atomic<bool> closed{false};
    std::mutex operationMutex;
    std::mutex socketStateMutex;
    bool operationActive{false};
    std::optional<std::chrono::steady_clock::time_point> sessionDeadline;

    Result<void> configure(const TlsIdentity& identity) {
        if (auto configured = configureMutualTls(context, identity); !configured) {
            return configured.error();
        }
        stream = std::make_unique<TlsStream>(io, context);
        return {};
    }

    void beginSession(std::chrono::milliseconds timeout) {
        sessionDeadline = std::chrono::steady_clock::now() + timeout;
    }

    Result<std::chrono::milliseconds>
    boundedOperationTimeout(std::chrono::milliseconds requested) const {
        if (!validOperationTimeout(requested)) {
            return Error{ErrorCode::InvalidArgument,
                         "p2p operation timeout must be within supported bounds"};
        }
        if (!sessionDeadline) {
            return requested;
        }
        const auto now = std::chrono::steady_clock::now();
        if (now >= *sessionDeadline) {
            return Error{ErrorCode::Timeout, "p2p aggregate session deadline expired"};
        }
        auto remaining =
            std::chrono::duration_cast<std::chrono::milliseconds>(*sessionDeadline - now);
        if (remaining <= std::chrono::milliseconds::zero()) {
            remaining = std::chrono::milliseconds{1};
        }
        return std::min(requested, remaining);
    }

    Result<std::chrono::milliseconds>
    boundedOperationUntil(std::chrono::steady_clock::time_point operationDeadline) const {
        const auto now = std::chrono::steady_clock::now();
        if (now >= operationDeadline) {
            return Error{ErrorCode::Timeout, "p2p frame operation deadline expired"};
        }
        auto remaining =
            std::chrono::duration_cast<std::chrono::milliseconds>(operationDeadline - now);
        if (remaining <= std::chrono::milliseconds::zero()) {
            remaining = std::chrono::milliseconds{1};
        }
        return boundedOperationTimeout(remaining);
    }

    void closeSocketLocked() noexcept {
        if (!stream) {
            return;
        }
        boost::system::error_code ignored;
        stream->next_layer().cancel(ignored);
        stream->next_layer().close(ignored);
    }

    void finishOperation() noexcept {
        std::lock_guard<std::mutex> lock(socketStateMutex);
        operationActive = false;
        if (closed.load(std::memory_order_acquire)) {
            closeSocketLocked();
        }
    }

    template <typename StartOperation>
    TimedOperation runOperation(std::chrono::milliseconds timeout,
                                StartOperation&& startOperation) {
        {
            std::lock_guard<std::mutex> lock(socketStateMutex);
            if (closed.load(std::memory_order_acquire)) {
                return TimedOperation{boost::asio::error::operation_aborted, false};
            }
            operationActive = true;
        }
        try {
            auto operation = runTimed(io, stream->next_layer(), timeout,
                                      std::forward<StartOperation>(startOperation));
            finishOperation();
            return operation;
        } catch (...) {
            finishOperation();
            throw;
        }
    }

    Result<void> handshake(boost::asio::ssl::stream_base::handshake_type type,
                           std::chrono::milliseconds timeout) {
        auto bounded = boundedOperationTimeout(timeout);
        if (!bounded) {
            return bounded.error();
        }
        const auto operation = runOperation(bounded.value(), [&](auto completion) {
            stream->async_handshake(type, std::move(completion));
            invokeHandshakeStartedHook();
        });
        if (operation.error) {
            return transportError(operation, "p2p TLS handshake",
                                  closed.load(std::memory_order_acquire));
        }
        auto* peer = SSL_get_peer_certificate(stream->native_handle());
        if (peer == nullptr) {
            return Error{ErrorCode::Unauthorized, "p2p peer did not present a certificate"};
        }
        std::unique_ptr<X509, decltype(&X509_free)> peerGuard(peer, X509_free);
        peerPin = certificatePin(peer);
        peerCn = certificateCommonName(peer);
        if (peerPin.empty() || peerCn.empty()) {
            return Error{ErrorCode::Unauthorized, "p2p peer certificate identity is incomplete"};
        }
        return {};
    }

    Result<void> writeFrame(std::span<const std::byte> payload, std::chrono::milliseconds timeout,
                            std::size_t maxFrameBytes) {
        std::lock_guard<std::mutex> lock(operationMutex);
        if (closed.load(std::memory_order_acquire)) {
            return Error{ErrorCode::OperationCancelled, "p2p channel is closed"};
        }
        if (payload.size() > maxFrameBytes || payload.size() > UINT32_MAX) {
            return Error{ErrorCode::InvalidArgument, "p2p frame exceeds configured size limit"};
        }
        auto bounded = boundedOperationTimeout(timeout);
        if (!bounded) {
            return bounded.error();
        }
        const auto header = encodeLength(payload.size());
        std::array<boost::asio::const_buffer, 2> buffers{
            boost::asio::buffer(header), boost::asio::buffer(payload.data(), payload.size())};
        const auto operation = runOperation(bounded.value(), [&](auto completion) {
            boost::asio::async_write(*stream, buffers, std::move(completion));
        });
        if (operation.error) {
            return transportError(operation, "p2p frame write",
                                  closed.load(std::memory_order_acquire));
        }
        return {};
    }

    Result<std::vector<std::byte>> readFrame(std::chrono::milliseconds timeout,
                                             std::size_t maxFrameBytes) {
        std::lock_guard<std::mutex> lock(operationMutex);
        if (closed.load(std::memory_order_acquire)) {
            return Error{ErrorCode::OperationCancelled, "p2p channel is closed"};
        }
        if (!validOperationTimeout(timeout)) {
            return Error{ErrorCode::InvalidArgument,
                         "p2p frame timeout must be within supported bounds"};
        }
        const auto frameDeadline = std::chrono::steady_clock::now() + timeout;
        auto bounded = boundedOperationUntil(frameDeadline);
        if (!bounded) {
            return bounded.error();
        }
        std::array<std::byte, 4> header{};
        auto operation = runOperation(bounded.value(), [&](auto completion) {
            boost::asio::async_read(*stream, boost::asio::buffer(header), std::move(completion));
        });
        if (operation.error) {
            return transportError(operation, "p2p frame header read",
                                  closed.load(std::memory_order_acquire));
        }
        const std::size_t payloadSize = decodeLength(header);
        if (payloadSize > maxFrameBytes) {
            close(); // Framing is desynchronized; never reuse this channel.
            return Error{ErrorCode::InvalidData, "p2p frame exceeds configured size limit"};
        }
        std::vector<std::byte> payload(payloadSize);
        if (payload.empty()) {
            return payload;
        }
        bounded = boundedOperationUntil(frameDeadline);
        if (!bounded) {
            return bounded.error();
        }
        operation = runOperation(bounded.value(), [&](auto completion) {
            boost::asio::async_read(*stream, boost::asio::buffer(payload), std::move(completion));
        });
        if (operation.error) {
            return transportError(operation, "p2p frame payload read",
                                  closed.load(std::memory_order_acquire));
        }
        return payload;
    }

    void close() noexcept {
        if (closed.exchange(true, std::memory_order_acq_rel)) {
            return;
        }
        std::lock_guard<std::mutex> lock(socketStateMutex);
        if (!operationActive) {
            closeSocketLocked();
            return;
        }
        try {
            const std::weak_ptr<Impl> weak = weak_from_this();
            boost::asio::post(io, [weak] {
                const auto self = weak.lock();
                if (!self) {
                    return;
                }
                std::lock_guard<std::mutex> stateLock(self->socketStateMutex);
                self->closeSocketLocked();
            });
        } catch (...) {
            // A failed cancellation enqueue must not escape a noexcept shutdown path. The active
            // operation remains bounded by its operation and aggregate session deadlines.
        }
    }
};

P2pConnection::P2pConnection(std::shared_ptr<Impl> impl) : impl_(std::move(impl)) {}
P2pConnection::P2pConnection(P2pConnection&&) noexcept = default;
P2pConnection& P2pConnection::operator=(P2pConnection&&) noexcept = default;
P2pConnection::~P2pConnection() {
    if (impl_) {
        impl_->close();
    }
}
Result<void> P2pConnection::writeFrame(std::span<const std::byte> payload,
                                       std::chrono::milliseconds timeout,
                                       std::size_t maxFrameBytes) {
    if (!impl_) {
        return Error{ErrorCode::InvalidState, "p2p connection was moved from"};
    }
    return impl_->writeFrame(payload, timeout, maxFrameBytes);
}
Result<std::vector<std::byte>> P2pConnection::readFrame(std::chrono::milliseconds timeout,
                                                        std::size_t maxFrameBytes) {
    if (!impl_) {
        return Error{ErrorCode::InvalidState, "p2p connection was moved from"};
    }
    return impl_->readFrame(timeout, maxFrameBytes);
}
std::string P2pConnection::peerSpkiPin() const {
    return impl_ ? impl_->peerPin : std::string{};
}
std::string P2pConnection::peerCertCn() const {
    return impl_ ? impl_->peerCn : std::string{};
}
std::string P2pConnection::peerNodeId() const {
    return peerCertCn();
}
std::string P2pConnection::localNodeId() const {
    return impl_ ? impl_->localNodeId : std::string{};
}
std::uint16_t P2pConnection::localPort() const {
    if (!impl_) {
        return 0;
    }
    boost::system::error_code error;
    const auto endpoint = impl_->stream->next_layer().local_endpoint(error);
    return error ? 0 : endpoint.port();
}
void P2pConnection::close() noexcept {
    if (impl_) {
        impl_->close();
    }
}

Result<P2pConnection> p2pConnect(P2pClientOptions options) {
    if (!validOperationTimeout(options.connectTimeout) ||
        !validOperationTimeout(options.handshakeTimeout) ||
        !validSessionTimeout(options.sessionTimeout)) {
        return Error{ErrorCode::InvalidArgument, "p2p timeouts exceed supported bounds"};
    }
    if (options.expectedPeerPin.empty() && !options.allowUnpinnedPeer) {
        return Error{ErrorCode::InvalidArgument,
                     "p2p connect requires an expected peer pin or explicit TOFU opt-in"};
    }
    auto impl = std::make_shared<P2pConnection::Impl>(boost::asio::ssl::context::tls_client,
                                                      options.identity.nodeId());
    if (auto configured = impl->configure(options.identity); !configured) {
        return configured.error();
    }
    const auto connectDeadline = std::chrono::steady_clock::now() + options.connectTimeout;
    Tcp::resolver resolver(impl->io);
    boost::asio::steady_timer resolveTimer(impl->io);
    boost::system::error_code resolveError;
    std::optional<Tcp::resolver::results_type> endpoints;
    bool resolveTimedOut = false;
    resolver.async_resolve(
        options.host, std::to_string(options.port),
        [&](const boost::system::error_code& error, Tcp::resolver::results_type results) {
            resolveError = error;
            if (!error) {
                endpoints = std::move(results);
            }
            static_cast<void>(resolveTimer.cancel());
        });
    resolveTimer.expires_at(connectDeadline);
    resolveTimer.async_wait([&](const boost::system::error_code& error) {
        if (!error) {
            resolveTimedOut = true;
            resolver.cancel();
        }
    });
    impl->io.restart();
    impl->io.run();
    if (resolveTimedOut) {
        return Error{ErrorCode::Timeout, "p2p host resolution timed out"};
    }
    if (resolveError || !endpoints) {
        return Error{ErrorCode::NetworkError,
                     std::string("p2p host resolution failed: ") + resolveError.message()};
    }
    const auto now = std::chrono::steady_clock::now();
    if (now >= connectDeadline) {
        return Error{ErrorCode::Timeout, "p2p connection deadline expired after resolution"};
    }
    auto connectRemaining =
        std::chrono::duration_cast<std::chrono::milliseconds>(connectDeadline - now);
    if (connectRemaining <= std::chrono::milliseconds::zero()) {
        connectRemaining = std::chrono::milliseconds{1};
    }
    const auto connection =
        runTimed(impl->io, impl->stream->next_layer(), connectRemaining, [&](auto completion) {
            boost::asio::async_connect(impl->stream->next_layer(), *endpoints,
                                       std::move(completion));
        });
    if (connection.error) {
        return transportError(connection, "p2p connect");
    }
    impl->beginSession(options.sessionTimeout);
    if (auto handshake =
            impl->handshake(boost::asio::ssl::stream_base::client, options.handshakeTimeout);
        !handshake) {
        return handshake.error();
    }
    if (!options.expectedPeerPin.empty() && impl->peerPin != options.expectedPeerPin) {
        impl->close();
        return Error{ErrorCode::Unauthorized, "p2p peer pin does not match the expected TOFU pin"};
    }
    return P2pConnection(std::move(impl));
}

class P2pListener::Impl {
public:
    explicit Impl(Options listenerOptions) : options(std::move(listenerOptions)) {}

    Options options;
    SessionHandler handler;
    boost::asio::io_context io;
    std::unique_ptr<Tcp::acceptor> acceptor;
    struct SessionWorker {
        std::thread thread;
        std::shared_ptr<std::atomic<bool>> completed;
    };

    std::thread acceptThread;
    std::vector<SessionWorker> sessionWorkers;
    std::atomic<std::size_t> retainedSessionCount{0};
    std::atomic<bool> running{false};
    std::atomic<std::uint64_t> accepted{0};
    std::atomic<std::size_t> preAuthCount{0};
    std::atomic<std::size_t> authenticatedCount{0};
    std::atomic<std::uint64_t> generation{0};
    std::mutex lifecycleMutex;
    std::mutex allowedPinsMutex;
    std::mutex sessionsMutex;
    std::set<std::shared_ptr<P2pConnection::Impl>,
             std::owner_less<std::shared_ptr<P2pConnection::Impl>>>
        activeSessions;

    Result<void> start() {
        try {
            return startImpl();
        } catch (const std::exception& error) {
            stop();
            return Error{ErrorCode::InternalError,
                         std::string("p2p listener start threw: ") + error.what()};
        } catch (...) {
            stop();
            return Error{ErrorCode::InternalError, "p2p listener start threw an unknown exception"};
        }
    }

    Result<void> startImpl() {
        std::lock_guard<std::mutex> lock(lifecycleMutex);
        if (running.load(std::memory_order_acquire)) {
            return {};
        }
        if (acceptThread.joinable()) {
            return Error{ErrorCode::InvalidState,
                         "p2p listener must be stopped after an accept-thread failure"};
        }
        if (options.maxConcurrentHandshakes == 0 || options.maxConcurrentSessions == 0 ||
            options.maxConcurrentHandshakes > kP2pMaxConcurrentHandshakes ||
            options.maxConcurrentSessions > kP2pMaxConcurrentSessions) {
            return Error{ErrorCode::InvalidArgument,
                         "p2p listener capacities must be within supported bounds"};
        }
        if (!validOperationTimeout(options.handshakeTimeout) ||
            !validSessionTimeout(options.sessionTimeout)) {
            return Error{ErrorCode::InvalidArgument,
                         "p2p listener timeouts exceed supported bounds"};
        }
        if (!handler) {
            return Error{ErrorCode::InvalidState,
                         "p2p listener requires a session handler before start"};
        }
        boost::asio::ssl::context identityProbe(boost::asio::ssl::context::tls_server);
        if (auto configured = configureMutualTls(identityProbe, options.identity); !configured) {
            return configured.error();
        }
        Tcp::resolver resolver(io);
        boost::system::error_code resolveError;
        const auto endpoints =
            resolver.resolve(options.host, std::to_string(options.port), resolveError);
        if (resolveError) {
            return Error{ErrorCode::NetworkError,
                         std::string("p2p listen address resolution failed: ") +
                             resolveError.message()};
        }
        for (const auto& endpoint : endpoints) {
            auto candidate = std::make_unique<Tcp::acceptor>(io);
            boost::system::error_code error;
            // NOLINTNEXTLINE(bugprone-unused-return-value) -- error is checked immediately.
            candidate->open(endpoint.endpoint().protocol(), error);
            if (error) {
                continue;
            }
            // NOLINTNEXTLINE(bugprone-unused-return-value) -- error is checked immediately.
            candidate->set_option(Tcp::acceptor::reuse_address(true), error);
            if (error) {
                continue;
            }
            // NOLINTNEXTLINE(bugprone-unused-return-value) -- error is checked immediately.
            candidate->bind(endpoint.endpoint(), error);
            if (error) {
                continue;
            }
            // NOLINTNEXTLINE(bugprone-unused-return-value) -- error is checked immediately.
            candidate->listen(boost::asio::socket_base::max_listen_connections, error);
            if (!error) {
                acceptor = std::move(candidate);
                break;
            }
        }
        if (!acceptor) {
            return Error{ErrorCode::NetworkError,
                         "p2p listen failed to bind any resolved endpoint"};
        }
        sessionWorkers.reserve(options.maxConcurrentHandshakes + options.maxConcurrentSessions);
        accepted.store(0, std::memory_order_relaxed);
        preAuthCount.store(0, std::memory_order_relaxed);
        authenticatedCount.store(0, std::memory_order_relaxed);
        generation.fetch_add(1, std::memory_order_acq_rel);
        running.store(true, std::memory_order_release);
        acceptThread = std::thread([this] {
            try {
                acceptLoop();
            } catch (const std::exception& error) {
                spdlog::error("[p2p] listener accept thread failed: {}", error.what());
                running.store(false, std::memory_order_release);
            } catch (...) {
                spdlog::error("[p2p] listener accept thread failed with unknown exception");
                running.store(false, std::memory_order_release);
            }
        });
        return {};
    }

    void reapCompletedSessions() {
        for (auto worker = sessionWorkers.begin(); worker != sessionWorkers.end();) {
            if (!worker->completed->load(std::memory_order_acquire)) {
                ++worker;
                continue;
            }
            if (worker->thread.joinable()) {
                worker->thread.join();
            }
            worker = sessionWorkers.erase(worker);
            retainedSessionCount.fetch_sub(1, std::memory_order_relaxed);
        }
    }

    void acceptLoop() {
        while (running.load(std::memory_order_acquire)) {
            invokeAcceptLoopHook();
            reapCompletedSessions();
            const auto retainedLimit =
                options.maxConcurrentHandshakes + options.maxConcurrentSessions;
            if (sessionWorkers.size() >= retainedLimit ||
                preAuthCount.load(std::memory_order_acquire) >= options.maxConcurrentHandshakes) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                continue;
            }
            auto session = std::make_shared<P2pConnection::Impl>(
                boost::asio::ssl::context::tls_server, options.identity.nodeId());
            if (auto configured = session->configure(options.identity); !configured) {
                spdlog::warn("[p2p] listener identity configuration failed: {}",
                             configured.error().message);
                running.store(false, std::memory_order_release);
                return;
            }
            boost::system::error_code acceptError;
            acceptor->async_accept(
                session->stream->next_layer(),
                [&](const boost::system::error_code& error) { acceptError = error; });
            io.restart();
            io.run();
            if (!running.load(std::memory_order_acquire)) {
                break;
            }
            if (acceptError) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                continue;
            }
            accepted.fetch_add(1, std::memory_order_relaxed);
            preAuthCount.fetch_add(1, std::memory_order_relaxed);
            session->beginSession(options.sessionTimeout);
            {
                std::lock_guard<std::mutex> lock(sessionsMutex);
                activeSessions.insert(session);
            }
            auto completed = std::make_shared<std::atomic<bool>>(false);
            sessionWorkers.push_back(
                SessionWorker{.thread = std::thread([this, session, completed] {
                                  gSessionWorkerOwner = this;
                                  handleSession(session, completed);
                                  gSessionWorkerOwner = nullptr;
                              }),
                              .completed = std::move(completed)});
            retainedSessionCount.fetch_add(1, std::memory_order_relaxed);
        }
    }

    bool tryAcquireAuthenticatedSlot() {
        auto current = authenticatedCount.load(std::memory_order_acquire);
        while (current < options.maxConcurrentSessions) {
            if (authenticatedCount.compare_exchange_weak(
                    current, current + 1, std::memory_order_acq_rel, std::memory_order_acquire)) {
                return true;
            }
        }
        return false;
    }

    void handleSession(const std::shared_ptr<P2pConnection::Impl>& session,
                       const std::shared_ptr<std::atomic<bool>>& completed) noexcept {
        bool inPreAuthPool = true;
        bool inAuthenticatedPool = false;
        try {
            auto handshake =
                session->handshake(boost::asio::ssl::stream_base::server, options.handshakeTimeout);
            if (handshake) {
                bool explicitlyPinned = false;
                {
                    std::lock_guard<std::mutex> lock(allowedPinsMutex);
                    explicitlyPinned =
                        std::find(options.allowedPeerPins.begin(), options.allowedPeerPins.end(),
                                  session->peerPin) != options.allowedPeerPins.end();
                }
                if (!explicitlyPinned && !options.allowUnpinnedPeers) {
                    handshake = Error{ErrorCode::Unauthorized, "p2p peer pin is not allowed"};
                }
            }
            if (handshake && !tryAcquireAuthenticatedSlot()) {
                handshake = Error{ErrorCode::ResourceBusy,
                                  "p2p authenticated session capacity is exhausted"};
            }
            if (handshake) {
                inAuthenticatedPool = true;
                preAuthCount.fetch_sub(1, std::memory_order_acq_rel);
                inPreAuthPool = false;
                if (handler) {
                    handler(P2pConnection(session));
                }
            } else {
                spdlog::debug("[p2p] session rejected: {}", handshake.error().message);
            }
        } catch (const std::exception& error) {
            spdlog::warn("[p2p] session thread contained exception: {}", error.what());
        } catch (...) {
            spdlog::warn("[p2p] session thread contained unknown exception");
        }
        session->close();
        {
            std::lock_guard<std::mutex> lock(sessionsMutex);
            activeSessions.erase(session);
        }
        // Publish completion before releasing either capacity counter. This keeps the number of
        // retained workers within the pre-reserved handshake + authenticated-session bound.
        completed->store(true, std::memory_order_release);
        if (inAuthenticatedPool) {
            authenticatedCount.fetch_sub(1, std::memory_order_acq_rel);
        }
        if (inPreAuthPool) {
            preAuthCount.fetch_sub(1, std::memory_order_acq_rel);
        }
    }

    void requestStop() noexcept {
        running.store(false, std::memory_order_release);
        if (acceptor) {
            const auto stoppedGeneration = generation.load(std::memory_order_acquire);
            try {
                boost::asio::post(io, [this, stoppedGeneration] {
                    if (generation.load(std::memory_order_acquire) != stoppedGeneration ||
                        !acceptor) {
                        return;
                    }
                    boost::system::error_code ignored;
                    acceptor->cancel(ignored);
                    acceptor->close(ignored);
                });
            } catch (...) {
                // The accept loop normally drains this cancellation on its own io_context. If the
                // enqueue itself fails, its next completion/error path remains exception-contained.
            }
        }
        std::lock_guard<std::mutex> lock(sessionsMutex);
        for (const auto& session : activeSessions) {
            session->close();
        }
    }

    void stop() noexcept {
        if (gSessionWorkerOwner == this) {
            // A worker cannot join itself. Request cancellation now; an external stop/destructor
            // performs the join and final cleanup after this handler returns.
            requestStop();
            return;
        }
        std::unique_lock<std::mutex> lifecycleLock(lifecycleMutex);
        requestStop();
        if (acceptThread.joinable()) {
            acceptThread.join();
        }
        try {
            invokeAfterAcceptJoinHook();
        } catch (...) {
            // Test instrumentation must never escape a noexcept lifecycle boundary.
        }
        // Close sessions accepted between the initial cancellation sweep and accept-loop exit.
        {
            std::lock_guard<std::mutex> lock(sessionsMutex);
            for (const auto& session : activeSessions) {
                session->close();
            }
        }
        if (acceptor) {
            boost::system::error_code ignored;
            // The accept thread is joined, so direct teardown is now race-free.
            acceptor->cancel(ignored);
            acceptor->close(ignored);
        }
        for (auto& worker : sessionWorkers) {
            if (worker.thread.joinable()) {
                worker.thread.join();
            }
        }
        sessionWorkers.clear();
        retainedSessionCount.store(0, std::memory_order_relaxed);
        preAuthCount.store(0, std::memory_order_relaxed);
        authenticatedCount.store(0, std::memory_order_relaxed);
        {
            std::lock_guard<std::mutex> lock(sessionsMutex);
            activeSessions.clear();
        }
        acceptor.reset();
    }
};

P2pListener::P2pListener(Options options) : impl_(std::make_shared<Impl>(std::move(options))) {}
P2pListener::P2pListener(std::shared_ptr<Impl> impl) : impl_(std::move(impl)) {}
P2pListener::~P2pListener() {
    stop();
}
Result<void> P2pListener::start() {
    return impl_->start();
}
std::uint16_t P2pListener::boundPort() const {
    if (!impl_ || !impl_->acceptor) {
        return 0;
    }
    boost::system::error_code error;
    const auto endpoint = impl_->acceptor->local_endpoint(error);
    return error ? 0 : endpoint.port();
}
void P2pListener::stop() noexcept {
    if (impl_) {
        impl_->stop();
    }
}
bool P2pListener::started() const noexcept {
    return impl_ && impl_->running.load(std::memory_order_acquire);
}
std::uint64_t P2pListener::acceptedCount() const noexcept {
    return impl_ ? impl_->accepted.load(std::memory_order_relaxed) : 0;
}
// pi-lens-ignore: clang-diagnostic-error
std::size_t P2pListener::retainedSessionCount() const noexcept {
    return impl_ ? impl_->retainedSessionCount.load(std::memory_order_relaxed) : 0;
}
std::size_t P2pListener::preAuthSessionCount() const noexcept {
    return impl_ ? impl_->preAuthCount.load(std::memory_order_acquire) : 0;
}
std::size_t P2pListener::authenticatedSessionCount() const noexcept {
    return impl_ ? impl_->authenticatedCount.load(std::memory_order_acquire) : 0;
}
// pi-lens-ignore: clang-diagnostic-error
void P2pListener::allowPeerPin(std::string spkiPin) {
    if (!impl_) {
        return;
    }
    std::lock_guard<std::mutex> lock(impl_->allowedPinsMutex);
    if (std::find(impl_->options.allowedPeerPins.begin(), impl_->options.allowedPeerPins.end(),
                  spkiPin) == impl_->options.allowedPeerPins.end()) {
        impl_->options.allowedPeerPins.push_back(std::move(spkiPin));
    }
}
void P2pListener::setHandler(SessionHandler handler) {
    if (impl_) {
        impl_->handler = std::move(handler);
    }
}

#if YAMS_DAEMON_TEST_HOOKS_ENABLED
void P2pListener::testing_setAcceptLoopHook(std::function<void()> hook) {
    std::lock_guard<std::mutex> lock(gAcceptLoopHookMutex);
    gAcceptLoopHook = std::move(hook);
}

void P2pListener::testing_setHandshakeStartedHook(std::function<void()> hook) {
    std::lock_guard<std::mutex> lock(gHandshakeStartedHookMutex);
    gHandshakeStartedHook = std::move(hook);
}

void P2pListener::testing_setAfterAcceptJoinHook(std::function<void()> hook) {
    std::lock_guard<std::mutex> lock(gAfterAcceptJoinHookMutex);
    gAfterAcceptJoinHook = std::move(hook);
}
#endif

} // namespace yams::daemon::p2p
