#include <yams/daemon/resource/plugin_trust.h>

#include <cctype>
#include <string>

namespace yams::daemon::plugin_trust {
namespace {

static std::string_view trimAscii(std::string_view s) {
    while (!s.empty() && std::isspace(static_cast<unsigned char>(s.front()))) {
        s.remove_prefix(1);
    }
    while (!s.empty() && std::isspace(static_cast<unsigned char>(s.back()))) {
        s.remove_suffix(1);
    }
    return s;
}

} // namespace

std::set<std::filesystem::path> parseTrustList(std::string_view content) {
    std::set<std::filesystem::path> out;

    while (!content.empty()) {
        size_t nl = content.find('\n');
        std::string_view line = (nl == std::string_view::npos) ? content : content.substr(0, nl);
        content = (nl == std::string_view::npos) ? std::string_view{} : content.substr(nl + 1);

        // Strip a single trailing '\r' for CRLF.
        if (!line.empty() && line.back() == '\r') {
            line.remove_suffix(1);
        }

        line = trimAscii(line);
        if (line.empty()) {
            continue;
        }

        // Comment line if first non-whitespace char is '#'
        if (!line.empty() && line.front() == '#') {
            continue;
        }

        out.insert(std::filesystem::path(std::string(line)));
    }

    return out;
}

bool isPathWithin(const std::filesystem::path& base, const std::filesystem::path& candidate) {
    if (base.empty()) {
        return false;
    }

    auto b = base.begin();
    auto c = candidate.begin();
    for (; b != base.end() && c != candidate.end(); ++b, ++c) {
        if (*b != *c) {
            return false;
        }
    }
    return b == base.end();
}

} // namespace yams::daemon::plugin_trust
