#pragma once

#include <cstddef>
#include <string_view>

// Private to yams_plugins_mgmt; not installed.
namespace yams::plugins::detail {

constexpr char asciiLower(char c) noexcept {
    return (c >= 'A' && c <= 'Z') ? static_cast<char>(c - 'A' + 'a') : c;
}

constexpr bool asciiIEquals(std::string_view a, std::string_view b) noexcept {
    if (a.size() != b.size())
        return false;
    for (std::size_t i = 0; i < a.size(); ++i) {
        if (asciiLower(a[i]) != asciiLower(b[i]))
            return false;
    }
    return true;
}

constexpr bool isAsciiDigits(std::string_view s) noexcept {
    if (s.empty())
        return false;
    for (char c : s) {
        if (c < '0' || c > '9')
            return false;
    }
    return true;
}

/// Strict dotted-decimal IPv4 in 127.0.0.0/8: exactly four octets, no leading
/// zeros, no hex/octal/short forms. Anything looser is treated as a hostname.
constexpr bool isLoopbackIpv4Literal(std::string_view host) noexcept {
    int octets = 0;
    while (true) {
        const auto dot = host.find('.');
        const auto part = host.substr(0, dot);
        if (!isAsciiDigits(part) || part.size() > 3 || (part.size() > 1 && part.front() == '0'))
            return false;
        int value = 0;
        for (char c : part)
            value = value * 10 + (c - '0');
        if (value > 255 || (octets == 0 && value != 127))
            return false;
        ++octets;
        if (dot == std::string_view::npos)
            break;
        host.remove_prefix(dot + 1);
    }
    return octets == 4;
}

/// True only when @p url is an http(s) URL whose *host component* is a loopback
/// literal: "localhost" (case-insensitive), a dotted IPv4 in 127.0.0.0/8, or "[::1]".
///
/// The host is taken from scheme://[userinfo@]host[:port] up to the first '/', '?'
/// or '#'. This gates disabling TLS verification, so anything ambiguous (several
/// '@', a backslash, whitespace or control bytes in the authority, a non-numeric
/// port, an unbracketed extra ':') is rejected rather than guessed at.
constexpr bool isLoopbackOrLocalhostUrl(std::string_view url) noexcept {
    const auto schemeEnd = url.find("://");
    if (schemeEnd == std::string_view::npos)
        return false;
    const auto scheme = url.substr(0, schemeEnd);
    if (!asciiIEquals(scheme, "http") && !asciiIEquals(scheme, "https"))
        return false;

    auto authority = url.substr(schemeEnd + 3);
    authority = authority.substr(0, authority.find_first_of("/?#"));
    for (char c : authority) {
        const auto uc = static_cast<unsigned char>(c);
        if (uc <= 0x20 || uc == 0x7f || c == '\\')
            return false;
    }

    auto hostPort = authority;
    if (const auto at = authority.find('@'); at != std::string_view::npos) {
        if (authority.find('@', at + 1) != std::string_view::npos)
            return false;
        hostPort = authority.substr(at + 1);
    }

    std::string_view host;
    std::string_view afterHost;
    if (!hostPort.empty() && hostPort.front() == '[') {
        const auto close = hostPort.find(']');
        if (close == std::string_view::npos)
            return false;
        host = hostPort.substr(0, close + 1);
        afterHost = hostPort.substr(close + 1);
    } else {
        const auto colon = hostPort.find(':');
        host = hostPort.substr(0, colon);
        if (colon != std::string_view::npos)
            afterHost = hostPort.substr(colon);
    }

    if (!afterHost.empty()) {
        if (afterHost.front() != ':')
            return false;
        const auto port = afterHost.substr(1);
        if (!isAsciiDigits(port) || port.size() > 5)
            return false;
    }

    return asciiIEquals(host, "localhost") || host == "[::1]" || isLoopbackIpv4Literal(host);
}

} // namespace yams::plugins::detail
