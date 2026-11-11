// The predicate gates CURLOPT_SSL_VERIFYPEER/VERIFYHOST=0 in plugin_repo_client.cpp,
// so it must match on the parsed URL host, never on a string prefix.

#include <catch2/catch_test_macros.hpp>

#include "src/plugins/loopback_url.h"

#include <string_view>

namespace yams::plugins::test {

namespace {
struct LoopbackCase {
    std::string_view url;
    bool expected;
};
} // namespace

TEST_CASE("isLoopbackOrLocalhostUrl matches the URL host, not a prefix",
          "[plugins][repo_client][security]") {
    using detail::isLoopbackOrLocalhostUrl;

    static constexpr LoopbackCase kCases[] = {
        // localhost
        {"http://localhost", true},
        {"https://localhost", true},
        {"https://localhost/", true},
        {"https://localhost/api/v1/plugins?q=x", true},
        {"https://LOCALHOST/api", true},
        {"HTTPS://LocalHost/api", true},
        {"http://localhost:8080", true},
        {"https://localhost:8080/api/v1/plugins", true},
        {"https://localhost?q=1", true},
        {"https://localhost#frag", true},
        {"https://localhost.evil.example", false},
        {"https://localhost.evil.example/api", false},
        {"https://localhost.evil.example:443/api", false},
        {"https://localhost-evil.example/", false},
        {"https://localhostevil.example/", false},
        {"https://localhost./", false},
        {"https://notlocalhost/", false},
        {"https://evil.example/localhost", false},
        {"https://evil.example/?u=https://localhost/", false},
        {"https://evil.example#@localhost", false},

        // 127.0.0.0/8
        {"http://127.0.0.1", true},
        {"https://127.0.0.1/api", true},
        {"https://127.1.2.3:9", true},
        {"https://127.1.2.3:9/api", true},
        {"https://127.255.255.255/", true},
        {"https://127.evil.example", false},
        {"http://127.evil.example/api", false},
        {"https://127.0.0.1.evil.example", false},
        {"https://127.0.0.1.evil.example:8443/api", false},
        {"https://127.0.0.1.", false},
        {"https://127.0.0", false},
        {"https://127.1", false},
        {"https://127.0.0.1.1", false},
        {"https://127.0.0.256", false},
        {"https://127.0.0.01", false},
        {"https://127.0.0.x", false},
        {"https://127..0.1", false},
        {"https://128.0.0.1", false},
        {"https://1127.0.0.1", false},
        {"https://0x7f.0.0.1", false},

        // userinfo
        {"https://user@localhost", true},
        {"https://user:secret@localhost:8443/api", true},
        {"https://user@127.0.0.1/api", true},
        {"https://user@[::1]/api", true},
        {"https://localhost@evil.example", false},
        {"https://localhost:443@evil.example/api", false},
        {"https://127.0.0.1@evil.example", false},
        {"https://[::1]@evil.example", false},
        {"https://localhost@evil.example@localhost/", false},

        // bracketed IPv6
        {"http://[::1]", true},
        {"https://[::1]", true},
        {"https://[::1]/api", true},
        {"https://[::1]:443", true},
        {"https://[::1]:443/api", true},
        {"https://[::1].evil.example", false},
        {"https://[::1].evil.example/api", false},
        {"https://[::1]evil.example", false},
        {"https://[::1", false},
        {"https://::1/", false},
        {"https://[::2]/", false},
        {"https://[::1%25eth0]/", false},
        {"https://[2001:db8::1]/", false},

        // ports
        {"https://localhost:/api", false},
        {"https://localhost:80a/api", false},
        {"https://localhost:123456/api", false},
        {"https://localhost:80:90/api", false},
        {"https://[::1]:x/", false},

        // ambiguous authorities
        {"https://localhost\\@evil.example/", false},
        {"https://localhost\\.evil.example/", false},
        {"https://localhost evil.example/", false},
        {"https://localhost\t.evil.example/", false},

        // non-http schemes and malformed input
        {"ftp://localhost", false},
        {"file://localhost/etc/passwd", false},
        {"ws://localhost:8080", false},
        {"wss://127.0.0.1", false},
        {"gopher://[::1]", false},
        {"httpx://localhost", false},
        {"xhttp://localhost", false},
        {"localhost", false},
        {"localhost:8080", false},
        {"//localhost/api", false},
        {"http:/localhost", false},
        {"http:localhost", false},
        {"http://", false},
        {"https:///localhost", false},
        {"", false},
    };

    for (const auto& c : kCases) {
        INFO("url: '" << c.url << "'");
        CHECK(isLoopbackOrLocalhostUrl(c.url) == c.expected);
    }
}

} // namespace yams::plugins::test
