#include <yams/storage/s3_signer.h>

#include <yams/crypto/hasher.h>

#include <openssl/evp.h>
#include <openssl/hmac.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <regex>
#include <sstream>
#include <string_view>

namespace yams::storage {

namespace {

std::string toLower(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(),
                   [](unsigned char c) { return (char)std::tolower(c); });
    return s;
}

std::string hexEncode(const unsigned char* data, std::size_t len) {
    static const char* hex = "0123456789abcdef";
    std::string out;
    out.resize(len * 2);
    for (size_t i = 0; i < len; ++i) {
        out[2 * i] = hex[(data[i] >> 4) & 0xF];
        out[2 * i + 1] = hex[data[i] & 0xF];
    }
    return out;
}

std::string percentEncode(const std::string& s, bool encodeSlash = false) {
    static const char* unreserved =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.~";
    std::string out;
    for (unsigned char c : s) {
        if (std::strchr(unreserved, c) || (!encodeSlash && c == '/')) {
            out.push_back((char)c);
        } else {
            char buf[4];
            std::snprintf(buf, sizeof(buf), "%%%02X", c);
            out.append(buf);
        }
    }
    return out;
}

struct ParsedUrl {
    std::string scheme;
    std::string host;
    std::string path;  // begins with '/'
    std::string query; // without leading '?'
};

ParsedUrl parseUrl(const std::string& url) {
    ParsedUrl pu;
    auto pos = url.find("://");
    if (pos == std::string::npos)
        return pu;
    pu.scheme = url.substr(0, pos);
    auto rest = url.substr(pos + 3);
    auto slash = rest.find('/');
    if (slash == std::string::npos) {
        pu.host = rest;
        pu.path = "/";
    } else {
        pu.host = rest.substr(0, slash);
        auto pathQuery = rest.substr(slash);
        auto qpos = pathQuery.find('?');
        if (qpos == std::string::npos) {
            pu.path = pathQuery;
        } else {
            pu.path = pathQuery.substr(0, qpos);
            pu.query = pathQuery.substr(qpos + 1);
        }
    }
    return pu;
}

std::string formatAmzDate(std::string* outShortDate) {
    using namespace std::chrono;
    auto now = system_clock::now();
    auto t = system_clock::to_time_t(now);
    std::tm gmt{};
#if defined(_WIN32)
    gmtime_s(&gmt, &t);
#else
    gmtime_r(&t, &gmt);
#endif
    char bufTs[32];
    char bufD[16];
    // Use strftime to avoid manual snprintf size calculations (eliminates truncation warnings)
    if (std::strftime(bufTs, sizeof(bufTs), "%Y%m%dT%H%M%SZ", &gmt) == 0) {
        return {};
    }
    if (std::strftime(bufD, sizeof(bufD), "%Y%m%d", &gmt) == 0) {
        if (outShortDate) {
            *outShortDate = {};
        }
    } else if (outShortDate) {
        *outShortDate = bufD;
    }
    return std::string(bufTs);
}

std::array<unsigned char, 32> hmacSha256(std::string_view key, std::string_view data) {
    std::array<unsigned char, 32> out{};
    unsigned int len = 0;
    HMAC(EVP_sha256(), key.data(), (int)key.size(), (const unsigned char*)data.data(),
         (int)data.size(), out.data(), &len);
    return out;
}

std::string sha256Hex(std::string_view data) {
    auto hasher = crypto::createSHA256Hasher();
    hasher->init();
    hasher->update(
        std::span<const std::byte>(reinterpret_cast<const std::byte*>(data.data()), data.size()));
    return hasher->finalize();
}

} // namespace

Result<curl_slist*>
S3Signer::signRequest(CURL* curl, const BackendConfig& config, const std::string& method,
                      const std::string& url, std::span<const std::byte> payload,
                      const std::vector<std::pair<std::string, std::string>>& extraHeaders) {
    (void)curl; // curl is not needed to compute headers, kept for API parity

    // Resolve credentials
    std::string accessKey;
    std::string secretKey;
    std::string sessionToken;

    if (auto it = config.credentials.find("access_key"); it != config.credentials.end()) {
        accessKey = it->second;
    }
    if (auto it = config.credentials.find("secret_key"); it != config.credentials.end()) {
        secretKey = it->second;
    }
    if (auto it = config.credentials.find("session_token"); it != config.credentials.end()) {
        sessionToken = it->second;
    }

    if (accessKey.empty() || secretKey.empty()) {
        const char* ak = std::getenv("AWS_ACCESS_KEY_ID");
        const char* sk = std::getenv("AWS_SECRET_ACCESS_KEY");
        const char* st = std::getenv("AWS_SESSION_TOKEN");
        if (ak)
            accessKey = ak;
        if (sk)
            secretKey = sk;
        if (st)
            sessionToken = st;
    }

    if (accessKey.empty() || secretKey.empty()) {
        return Result<curl_slist*>(Error{ErrorCode::PermissionDenied, "Missing S3 credentials"});
    }

    // Determine region and service
    std::string region = config.region.empty() ? std::string("us-east-1") : config.region;
    ParsedUrl pu = parseUrl(url);
    if (region == "auto" && pu.host.find("r2.cloudflarestorage.com") == std::string::npos) {
        region = "us-east-1"; // fallback
    }
    if (config.region.empty() && pu.host.find("r2.cloudflarestorage.com") != std::string::npos) {
        region = "auto"; // Cloudflare R2 default
    }
    const std::string service = "s3";

    // Hash payload
    std::string payloadHex;
    if (payload.size() == 0) {
        // SHA256 of empty string
        payloadHex = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    } else {
        payloadHex = sha256Hex(
            std::string_view(reinterpret_cast<const char*>(payload.data()), payload.size()));
    }

    // Prepare dates
    std::string ymd;
    std::string amzDate = formatAmzDate(&ymd);

    // Canonical request
    std::string canonicalURI = percentEncode(pu.path, false);
    // Canonical query (assume none or already encoded → sort if present)
    std::string canonicalQuery;
    if (!pu.query.empty()) {
        // Minimal: keep as-is; proper implementation should split, encode and sort.
        canonicalQuery = pu.query;
    }

    // Headers (canonical set)
    std::vector<std::pair<std::string, std::string>> hdrs;
    hdrs.emplace_back("host", toLower(pu.host));
    hdrs.emplace_back("x-amz-content-sha256", payloadHex);
    hdrs.emplace_back("x-amz-date", amzDate);
    if (!sessionToken.empty())
        hdrs.emplace_back("x-amz-security-token", sessionToken);
    // Add caller-provided headers to be signed (lowercased names, trimmed values)
    for (auto [name, value] : extraHeaders) {
        std::string lname = toLower(std::move(name));
        // Trim value
        auto start = value.find_first_not_of(" \t\r\n");
        auto end = value.find_last_not_of(" \t\r\n");
        std::string tval =
            (start == std::string::npos ? std::string() : value.substr(start, end - start + 1));
        hdrs.emplace_back(std::move(lname), std::move(tval));
    }
    std::sort(hdrs.begin(), hdrs.end(), [](auto& a, auto& b) { return a.first < b.first; });

    std::ostringstream canonicalHeaders;
    std::ostringstream signedHeaders;
    for (size_t i = 0; i < hdrs.size(); ++i) {
        canonicalHeaders << hdrs[i].first << ':' << hdrs[i].second << "\n";
        signedHeaders << hdrs[i].first;
        if (i + 1 < hdrs.size())
            signedHeaders << ';';
    }

    std::ostringstream cr;
    cr << method << "\n"
       << canonicalURI << "\n"
       << canonicalQuery << "\n"
       << canonicalHeaders.str() << signedHeaders.str() << "\n"
       << payloadHex;
    std::string canonicalRequest = cr.str();

    std::string canonicalRequestHash = sha256Hex(canonicalRequest);

    // String to sign
    std::ostringstream sts;
    sts << "AWS4-HMAC-SHA256\n"
        << amzDate << "\n"
        << ymd << '/' << region << '/' << service << "/aws4_request\n"
        << canonicalRequestHash;
    std::string stringToSign = sts.str();

    // Derive signing key
    std::string kSecret = "AWS4" + secretKey;
    auto kDate = hmacSha256(kSecret, ymd);
    auto kRegion = hmacSha256(std::string_view((const char*)kDate.data(), kDate.size()), region);
    auto kService =
        hmacSha256(std::string_view((const char*)kRegion.data(), kRegion.size()), service);
    auto kSigning = hmacSha256(std::string_view((const char*)kService.data(), kService.size()),
                               std::string_view("aws4_request"));
    auto sig =
        hmacSha256(std::string_view((const char*)kSigning.data(), kSigning.size()), stringToSign);
    std::string signature = hexEncode(sig.data(), sig.size());

    // Authorization header
    std::ostringstream auth;
    auth << "AWS4-HMAC-SHA256 Credential=" << accessKey << '/' << ymd << '/' << region << '/'
         << service << "/aws4_request, SignedHeaders=" << signedHeaders.str()
         << ", Signature=" << signature;

    // Build header list
    struct curl_slist* headers = nullptr;
    std::string hostHdr = "Host: " + pu.host;
    headers = curl_slist_append(headers, hostHdr.c_str());
    std::string dateHdr = "x-amz-date: " + amzDate;
    headers = curl_slist_append(headers, dateHdr.c_str());
    std::string hashHdr = "x-amz-content-sha256: " + payloadHex;
    headers = curl_slist_append(headers, hashHdr.c_str());
    if (!sessionToken.empty()) {
        std::string tokHdr = "x-amz-security-token: " + sessionToken;
        headers = curl_slist_append(headers, tokHdr.c_str());
    }
    // Append signed extras exactly as provided (assumed canonicalized already)
    for (const auto& kv : extraHeaders) {
        std::string line = kv.first + ": " + kv.second;
        headers = curl_slist_append(headers, line.c_str());
    }
    std::string authHdr = "Authorization: " + auth.str();
    headers = curl_slist_append(headers, authHdr.c_str());

    return headers;
}

} // namespace yams::storage
