#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/message_serializer.h>

#include <spdlog/spdlog.h>
#include <bit>
#include <concepts>
#include <cstring>
#include <ranges>
#include <stdexcept>
#include <type_traits>
#include <variant>

namespace yams::daemon {

// C++20 Concepts for type-safe serialization
template <typename T>
concept Duration = requires(T t) {
    typename T::rep;
    typename T::period;
    t.count();
};

template <typename T>
concept Trivial = std::is_trivially_copyable_v<T> && !std::is_pointer_v<T> && !Duration<T>;

template <typename T>
concept StringLike = std::same_as<T, std::string> || std::same_as<T, std::string_view>;

template <typename T>
concept ContainerOfStrings = std::ranges::range<T> && StringLike<std::ranges::range_value_t<T>>;

template <typename T>
concept MapLike = requires(T t) {
    typename T::key_type;
    typename T::mapped_type;
    t.begin();
    t.end();
} && StringLike<typename T::key_type> && StringLike<typename T::mapped_type>;

// Modern C++20 Binary Serializer
class BinarySerializer {
private:
    std::vector<std::byte> buffer_;

    // Ensure correct endianness for cross-platform compatibility
    template <Trivial T> void writeValue(const T& value) {
        size_t oldSize = buffer_.size();
        buffer_.resize(oldSize + sizeof(T));

        if constexpr (sizeof(T) == 1) {
            std::memcpy(buffer_.data() + oldSize, &value, sizeof(T));
        } else {
            // Handle endianness for multi-byte types
            if constexpr (std::endian::native == std::endian::little) {
                std::memcpy(buffer_.data() + oldSize, &value, sizeof(T));
            } else {
                // Convert to little endian for wire format
                T littleEndian = value;
                if constexpr (sizeof(T) == 2) {
                    littleEndian = __builtin_bswap16(value);
                } else if constexpr (sizeof(T) == 4) {
                    littleEndian = __builtin_bswap32(value);
                } else if constexpr (sizeof(T) == 8) {
                    littleEndian = __builtin_bswap64(value);
                }
                std::memcpy(buffer_.data() + oldSize, &littleEndian, sizeof(T));
            }
        }
    }

public:
    // Serialize trivial types
    template <Trivial T> BinarySerializer& operator<<(const T& value) {
        writeValue(value);
        return *this;
    }

    // Serialize strings
    template <StringLike T> BinarySerializer& operator<<(const T& str) {
        uint32_t length = static_cast<uint32_t>(str.size());
        *this << length;
        if (length > 0) {
            size_t oldSize = buffer_.size();
            buffer_.resize(oldSize + length);
            std::memcpy(buffer_.data() + oldSize, str.data(), length);
        }
        return *this;
    }

    // Serialize durations
    template <Duration T> BinarySerializer& operator<<(const T& duration) {
        *this << static_cast<uint64_t>(duration.count());
        return *this;
    }

    // Serialize containers of strings
    template <ContainerOfStrings T> BinarySerializer& operator<<(const T& container) {
        uint32_t count = static_cast<uint32_t>(std::ranges::size(container));
        *this << count;
        for (const auto& item : container) {
            *this << item;
        }
        return *this;
    }

    // Serialize map-like containers
    template <MapLike T> BinarySerializer& operator<<(const T& map) {
        uint32_t count = static_cast<uint32_t>(map.size());
        *this << count;
        for (const auto& [key, value] : map) {
            *this << key << value;
        }
        return *this;
    }

    // Get serialized data
    std::vector<std::byte> data() && { return std::move(buffer_); }
    const std::vector<std::byte>& data() const& { return buffer_; }
};

// Modern C++20 Binary Deserializer
class BinaryDeserializer {
private:
    std::span<const std::byte> data_;

    template <Trivial T> Result<T> readValue() {
        if (data_.size() < sizeof(T)) {
            return Error{ErrorCode::InvalidData, "Buffer too small for type"};
        }

        T value;
        if constexpr (sizeof(T) == 1) {
            std::memcpy(&value, data_.data(), sizeof(T));
        } else {
            // Handle endianness
            if constexpr (std::endian::native == std::endian::little) {
                std::memcpy(&value, data_.data(), sizeof(T));
            } else {
                // Convert from little endian wire format
                T littleEndian;
                std::memcpy(&littleEndian, data_.data(), sizeof(T));
                if constexpr (sizeof(T) == 2) {
                    value = __builtin_bswap16(littleEndian);
                } else if constexpr (sizeof(T) == 4) {
                    value = __builtin_bswap32(littleEndian);
                } else if constexpr (sizeof(T) == 8) {
                    value = __builtin_bswap64(littleEndian);
                }
            }
        }

        data_ = data_.subspan(sizeof(T));
        return value;
    }

public:
    explicit BinaryDeserializer(std::span<const std::byte> data) : data_(data) {}

    // Deserialize trivial types
    template <Trivial T> Result<T> read() { return readValue<T>(); }

    // Deserialize strings
    Result<std::string> readString() {
        auto lengthResult = read<uint32_t>();
        if (!lengthResult)
            return lengthResult.error();

        uint32_t length = lengthResult.value();
        if (data_.size() < length) {
            return Error{ErrorCode::InvalidData, "Buffer too small for string"};
        }

        std::string str;
        if (length > 0) {
            str.resize(length);
            std::memcpy(str.data(), data_.data(), length);
            data_ = data_.subspan(length);
        }
        return str;
    }

    // Deserialize durations
    template <Duration T> Result<T> readDuration() {
        auto countResult = read<uint64_t>();
        if (!countResult)
            return countResult.error();
        return T{countResult.value()};
    }

    // Deserialize vector of strings
    Result<std::vector<std::string>> readStringVector() {
        auto countResult = read<uint32_t>();
        if (!countResult)
            return countResult.error();

        std::vector<std::string> vec;
        vec.reserve(countResult.value());

        for (uint32_t i = 0; i < countResult.value(); ++i) {
            auto strResult = readString();
            if (!strResult)
                return strResult.error();
            vec.push_back(std::move(strResult.value()));
        }
        return vec;
    }

    // Deserialize map
    Result<std::map<std::string, std::string>> readStringMap() {
        auto countResult = read<uint32_t>();
        if (!countResult)
            return countResult.error();

        std::map<std::string, std::string> map;
        for (uint32_t i = 0; i < countResult.value(); ++i) {
            auto keyResult = readString();
            if (!keyResult)
                return keyResult.error();
            auto valueResult = readString();
            if (!valueResult)
                return valueResult.error();

            map[std::move(keyResult.value())] = std::move(valueResult.value());
        }
        return map;
    }
};

// Modern C++20 concept-based serialization
template <typename T>
concept Serializable = requires(T t, BinarySerializer& ser) { t.serialize(ser); };

template <typename T>
concept Deserializable = requires(BinaryDeserializer& deser) { T::deserialize(deser); };

// Generic serialization dispatch using C++20 concepts
template <Serializable T> void serialize(BinarySerializer& ser, const T& obj) {
    obj.serialize(ser);
}

// Generic deserialization dispatch using C++20 concepts
template <Deserializable T> Result<T> deserialize(BinaryDeserializer& deser) {
    return T::deserialize(deser);
}

std::vector<std::byte> MessageSerializer::serialize_bytes(const Message& msg) {
    BinarySerializer ser;

    // Write header
    ser << msg.version << msg.requestId;

    // Get message type and write it
    MessageType msgType;
    if (std::holds_alternative<Request>(msg.payload)) {
        msgType = getMessageType(std::get<Request>(msg.payload));
    } else {
        msgType = getMessageType(std::get<Response>(msg.payload));
    }
    ser << static_cast<uint8_t>(msgType);

    // Serialize payload using modern C++20 dispatch
    std::visit(
        [&ser](auto&& payload) {
            using T = std::decay_t<decltype(payload)>;

            if constexpr (std::is_same_v<T, Request>) {
                std::visit([&ser](auto&& req) { req.serialize(ser); }, payload);
            } else if constexpr (std::is_same_v<T, Response>) {
                std::visit([&ser](auto&& res) { res.serialize(ser); }, payload);
            }
        },
        msg.payload);

    // Serialize optional fields
    ser << msg.sessionId.has_value();
    if (msg.sessionId.has_value()) {
        ser << msg.sessionId.value();
    }

    ser << msg.clientVersion.has_value();
    if (msg.clientVersion.has_value()) {
        ser << msg.clientVersion.value();
    }

    // Return serialized payload (length is already tracked by outer framer)
    return std::move(ser).data();
}

Result<Message> MessageSerializer::deserialize(std::span<const std::byte> data) {
    if (data.empty()) {
        return Error{ErrorCode::InvalidData, "Empty message data"};
    }

    BinaryDeserializer deser{data};
    Message msg;

    // Read header
    auto versionResult = deser.read<uint32_t>();
    if (!versionResult)
        return versionResult.error();
    msg.version = versionResult.value();

    auto requestIdResult = deser.read<uint64_t>();
    if (!requestIdResult)
        return requestIdResult.error();
    msg.requestId = requestIdResult.value();

    auto msgTypeResult = deser.read<uint8_t>();
    if (!msgTypeResult)
        return msgTypeResult.error();
    MessageType msgType = static_cast<MessageType>(msgTypeResult.value());
    spdlog::debug("Deserializing message type: {}", static_cast<int>(msgType));

    // Deserialize payload based on message type using template dispatch
    switch (msgType) {
        // Request types
        case MessageType::SearchRequest: {
            auto reqResult = deserialize<SearchRequest>(deser);
            if (!reqResult)
                return reqResult.error();
            msg.payload = Request{std::move(reqResult.value())};
            break;
        }
        case MessageType::AddRequest: {
            auto reqResult = deserialize<AddRequest>(deser);
            if (!reqResult)
                return reqResult.error();
            msg.payload = Request{std::move(reqResult.value())};
            break;
        }
        case MessageType::GetRequest: {
            auto reqResult = deserialize<GetRequest>(deser);
            if (!reqResult)
                return reqResult.error();
            msg.payload = Request{std::move(reqResult.value())};
            break;
        }
        case MessageType::GetInitRequest: {
            auto reqResult = deserialize<GetInitRequest>(deser);
            if (!reqResult)
                return reqResult.error();
            msg.payload = Request{std::move(reqResult.value())};
            break;
        }
        case MessageType::GetChunkRequest: {
            auto reqResult = deserialize<GetChunkRequest>(deser);
            if (!reqResult)
                return reqResult.error();
            msg.payload = Request{std::move(reqResult.value())};
            break;
        }
        case MessageType::GetEndRequest: {
            auto reqResult = deserialize<GetEndRequest>(deser);
            if (!reqResult)
                return reqResult.error();
            msg.payload = Request{std::move(reqResult.value())};
            break;
        }
        case MessageType::DeleteRequest: {
            auto reqResult = deserialize<DeleteRequest>(deser);
            if (!reqResult)
                return reqResult.error();
            msg.payload = Request{std::move(reqResult.value())};
            break;
        }
        case MessageType::ListRequest: {
            auto reqResult = deserialize<ListRequest>(deser);
            if (!reqResult)
                return reqResult.error();
            msg.payload = Request{std::move(reqResult.value())};
            break;
        }
        case MessageType::ShutdownRequest: {
            auto reqResult = deserialize<ShutdownRequest>(deser);
            if (!reqResult)
                return reqResult.error();
            msg.payload = Request{std::move(reqResult.value())};
            break;
        }
        case MessageType::StatusRequest: {
            auto reqResult = deserialize<StatusRequest>(deser);
            if (!reqResult)
                return reqResult.error();
            msg.payload = Request{std::move(reqResult.value())};
            break;
        }
        case MessageType::PingRequest: {
            auto reqResult = deserialize<PingRequest>(deser);
            if (!reqResult)
                return reqResult.error();
            msg.payload = Request{std::move(reqResult.value())};
            break;
        }
        case MessageType::GenerateEmbeddingRequest: {
            auto reqResult = deserialize<GenerateEmbeddingRequest>(deser);
            if (!reqResult)
                return reqResult.error();
            msg.payload = Request{std::move(reqResult.value())};
            break;
        }
        case MessageType::BatchEmbeddingRequest: {
            auto reqResult = deserialize<BatchEmbeddingRequest>(deser);
            if (!reqResult)
                return reqResult.error();
            msg.payload = Request{std::move(reqResult.value())};
            break;
        }
        case MessageType::LoadModelRequest: {
            auto reqResult = deserialize<LoadModelRequest>(deser);
            if (!reqResult)
                return reqResult.error();
            msg.payload = Request{std::move(reqResult.value())};
            break;
        }
        case MessageType::UnloadModelRequest: {
            auto reqResult = deserialize<UnloadModelRequest>(deser);
            if (!reqResult)
                return reqResult.error();
            msg.payload = Request{std::move(reqResult.value())};
            break;
        }
        case MessageType::ModelStatusRequest: {
            auto reqResult = deserialize<ModelStatusRequest>(deser);
            if (!reqResult)
                return reqResult.error();
            msg.payload = Request{std::move(reqResult.value())};
            break;
        }
        case MessageType::DownloadRequest: {
            auto reqResult = deserialize<DownloadRequest>(deser);
            if (!reqResult)
                return reqResult.error();
            msg.payload = Request{std::move(reqResult.value())};
            break;
        }
        case MessageType::AddDocumentRequest: {
            auto reqResult = deserialize<AddDocumentRequest>(deser);
            if (!reqResult)
                return reqResult.error();
            msg.payload = Request{std::move(reqResult.value())};
            break;
        }
        case MessageType::GrepRequest: {
            auto reqResult = deserialize<GrepRequest>(deser);
            if (!reqResult)
                return reqResult.error();
            msg.payload = Request{std::move(reqResult.value())};
            break;
        }
        case MessageType::UpdateDocumentRequest: {
            auto reqResult = deserialize<UpdateDocumentRequest>(deser);
            if (!reqResult)
                return reqResult.error();
            msg.payload = Request{std::move(reqResult.value())};
            break;
        }
        case MessageType::GetStatsRequest: {
            auto reqResult = deserialize<GetStatsRequest>(deser);
            if (!reqResult)
                return reqResult.error();
            msg.payload = Request{std::move(reqResult.value())};
            break;
        }

        // Response types
        case MessageType::SearchResponse: {
            auto resResult = deserialize<SearchResponse>(deser);
            if (!resResult)
                return resResult.error();
            msg.payload =
                Response{std::in_place_type<SearchResponse>, std::move(resResult.value())};
            break;
        }
        case MessageType::AddResponse: {
            auto resResult = deserialize<AddResponse>(deser);
            if (!resResult)
                return resResult.error();
            msg.payload = Response{std::in_place_type<AddResponse>, std::move(resResult.value())};
            break;
        }
        case MessageType::GetResponse: {
            auto resResult = deserialize<GetResponse>(deser);
            if (!resResult)
                return resResult.error();
            msg.payload = Response{std::in_place_type<GetResponse>, std::move(resResult.value())};
            break;
        }
        case MessageType::GetInitResponse: {
            auto resResult = deserialize<GetInitResponse>(deser);
            if (!resResult)
                return resResult.error();
            msg.payload =
                Response{std::in_place_type<GetInitResponse>, std::move(resResult.value())};
            break;
        }
        case MessageType::GetChunkResponse: {
            auto resResult = deserialize<GetChunkResponse>(deser);
            if (!resResult)
                return resResult.error();
            msg.payload =
                Response{std::in_place_type<GetChunkResponse>, std::move(resResult.value())};
            break;
        }
        case MessageType::StatusResponse: {
            auto resResult = deserialize<StatusResponse>(deser);
            if (!resResult)
                return resResult.error();
            msg.payload =
                Response{std::in_place_type<StatusResponse>, std::move(resResult.value())};
            break;
        }
        case MessageType::SuccessResponse: {
            auto resResult = deserialize<SuccessResponse>(deser);
            if (!resResult)
                return resResult.error();
            msg.payload =
                Response{std::in_place_type<SuccessResponse>, std::move(resResult.value())};
            break;
        }
        case MessageType::ErrorResponse: {
            auto resResult = deserialize<ErrorResponse>(deser);
            if (!resResult)
                return resResult.error();
            msg.payload = Response{std::in_place_type<ErrorResponse>, std::move(resResult.value())};
            break;
        }
        case MessageType::PongResponse: {
            auto resResult = deserialize<PongResponse>(deser);
            if (!resResult)
                return resResult.error();
            msg.payload = Response{std::in_place_type<PongResponse>, std::move(resResult.value())};
            break;
        }
        case MessageType::EmbeddingResponse: {
            auto resResult = deserialize<EmbeddingResponse>(deser);
            if (!resResult)
                return resResult.error();
            msg.payload =
                Response{std::in_place_type<EmbeddingResponse>, std::move(resResult.value())};
            break;
        }
        case MessageType::BatchEmbeddingResponse: {
            auto resResult = deserialize<BatchEmbeddingResponse>(deser);
            if (!resResult)
                return resResult.error();
            msg.payload =
                Response{std::in_place_type<BatchEmbeddingResponse>, std::move(resResult.value())};
            break;
        }
        case MessageType::ModelLoadResponse: {
            auto resResult = deserialize<ModelLoadResponse>(deser);
            if (!resResult)
                return resResult.error();
            msg.payload =
                Response{std::in_place_type<ModelLoadResponse>, std::move(resResult.value())};
            break;
        }
        case MessageType::ModelStatusResponse: {
            auto resResult = deserialize<ModelStatusResponse>(deser);
            if (!resResult)
                return resResult.error();
            msg.payload =
                Response{std::in_place_type<ModelStatusResponse>, std::move(resResult.value())};
            break;
        }
        case MessageType::ListResponse: {
            auto resResult = deserialize<ListResponse>(deser);
            if (!resResult)
                return resResult.error();
            msg.payload = Response{std::in_place_type<ListResponse>, std::move(resResult.value())};
            break;
        }
        case MessageType::DownloadResponse: {
            auto resResult = deserialize<DownloadResponse>(deser);
            if (!resResult)
                return resResult.error();
            msg.payload =
                Response{std::in_place_type<DownloadResponse>, std::move(resResult.value())};
            break;
        }
        case MessageType::AddDocumentResponse: {
            auto resResult = deserialize<AddDocumentResponse>(deser);
            if (!resResult)
                return resResult.error();
            msg.payload =
                Response{std::in_place_type<AddDocumentResponse>, std::move(resResult.value())};
            break;
        }
        case MessageType::GrepResponse: {
            auto resResult = deserialize<GrepResponse>(deser);
            if (!resResult)
                return resResult.error();
            msg.payload = Response{std::in_place_type<GrepResponse>, std::move(resResult.value())};
            break;
        }
        case MessageType::UpdateDocumentResponse: {
            auto resResult = deserialize<UpdateDocumentResponse>(deser);
            if (!resResult)
                return resResult.error();
            msg.payload =
                Response{std::in_place_type<UpdateDocumentResponse>, std::move(resResult.value())};
            break;
        }
        case MessageType::GetStatsResponse: {
            auto resResult = deserialize<GetStatsResponse>(deser);
            if (!resResult)
                return resResult.error();
            msg.payload =
                Response{std::in_place_type<GetStatsResponse>, std::move(resResult.value())};
            break;
        }

        default:
            return Error{ErrorCode::InvalidData,
                         "Unsupported message type: " + std::to_string(static_cast<int>(msgType))};
    }

    // Read optional fields
    auto hasSessionIdResult = deser.read<bool>();
    if (!hasSessionIdResult)
        return hasSessionIdResult.error();
    if (hasSessionIdResult.value()) {
        auto sessionIdResult = deser.readString();
        if (!sessionIdResult)
            return sessionIdResult.error();
        msg.sessionId = std::move(sessionIdResult.value());
    }

    auto hasClientVersionResult = deser.read<bool>();
    if (!hasClientVersionResult)
        return hasClientVersionResult.error();
    if (hasClientVersionResult.value()) {
        auto clientVersionResult = deser.readString();
        if (!clientVersionResult)
            return clientVersionResult.error();
        msg.clientVersion = std::move(clientVersionResult.value());
    }

    msg.timestamp = std::chrono::steady_clock::now();

    return msg;
}

Result<size_t> MessageSerializer::getMessageSize(std::span<const std::byte> header) {
    if (header.size() < sizeof(uint32_t)) {
        return Error{ErrorCode::InvalidData, "Header too small for message size"};
    }

    BinaryDeserializer deser{header};
    auto sizeResult = deser.read<uint32_t>();
    if (!sizeResult)
        return sizeResult.error();

    uint32_t size = sizeResult.value();
    if (size > MAX_MESSAGE_SIZE) {
        return Error{ErrorCode::InvalidData,
                     "Message size exceeds maximum: " + std::to_string(size)};
    }

    return static_cast<size_t>(size);
}

// MessageSerializer implementation with PIMPL
class MessageSerializer::Impl {
public:
    BinarySerializer serializer;
};

MessageSerializer::MessageSerializer() : pImpl(std::make_unique<Impl>()) {}
MessageSerializer::~MessageSerializer() = default;

std::vector<uint8_t> MessageSerializer::serialize(const Message& msg) {
    // Convert from static method's std::vector<std::byte> to std::vector<uint8_t>
    auto byteData = MessageSerializer::serialize_bytes(msg);
    std::vector<uint8_t> result;
    result.reserve(byteData.size());
    for (const auto& byte : byteData) {
        result.push_back(static_cast<uint8_t>(byte));
    }
    return result;
}

Result<Message> MessageSerializer::deserialize(const std::vector<uint8_t>& data) {
    // Convert from std::vector<uint8_t> to std::span<const std::byte>
    std::vector<std::byte> byteData;
    byteData.reserve(data.size());
    for (const auto& byte : data) {
        byteData.push_back(static_cast<std::byte>(byte));
    }
    return MessageSerializer::deserialize(std::span<const std::byte>(byteData));
}

MessageSerializer& MessageSerializer::operator<<(const Message& msg) {
    // Serialize using the static method and then extract data
    auto data = MessageSerializer::serialize_bytes(msg);
    pImpl->serializer = BinarySerializer{};
    for (const auto& byte : data) {
        pImpl->serializer << static_cast<uint8_t>(byte);
    }
    return *this;
}

std::vector<uint8_t> MessageSerializer::getData() const {
    auto byteData = pImpl->serializer.data();
    std::vector<uint8_t> result;
    result.reserve(byteData.size());
    for (const auto& byte : byteData) {
        result.push_back(static_cast<uint8_t>(byte));
    }
    return result;
}

// MessageDeserializer implementation with PIMPL
class MessageDeserializer::Impl {
public:
    std::vector<std::byte> data;

    explicit Impl(const std::vector<uint8_t>& inputData) {
        data.reserve(inputData.size());
        for (const auto& byte : inputData) {
            data.push_back(static_cast<std::byte>(byte));
        }
    }
};

MessageDeserializer::MessageDeserializer(const std::vector<uint8_t>& data)
    : pImpl(std::make_unique<Impl>(data)) {}
MessageDeserializer::~MessageDeserializer() = default;

MessageDeserializer& MessageDeserializer::operator>>(Message& msg) {
    auto result = MessageSerializer::deserialize(pImpl->data);
    if (!result) {
        throw std::runtime_error("Failed to deserialize message: " + result.error().message);
    }
    msg = std::move(result.value());
    return *this;
}

} // namespace yams::daemon
