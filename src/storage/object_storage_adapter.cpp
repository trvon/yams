// Adapter between C++ IObjectStorageBackend and C ABI yams_object_storage_v1

#include <yams/plugins/object_storage_adapter.hpp>

#include <cstdlib>
#include <cstring>
#include <memory>
#include <optional>
#include <set>
#include <span>
#include <sstream>

namespace yams::plugins::adapter {

namespace {
struct BackendHandle {
    std::shared_ptr<IObjectStorageBackend> impl;
};

static char* dup_cstr(const std::string& s) {
    char* p = static_cast<char*>(std::malloc(s.size() + 1));
    if (!p)
        return nullptr;
    std::memcpy(p, s.data(), s.size());
    p[s.size()] = '\0';
    return p;
}

static std::string escape_json(const std::string& in) {
    std::ostringstream os;
    os << '"';
    for (unsigned char c : in) {
        switch (c) {
            case '"':
                os << "\\\"";
                break;
            case '\\':
                os << "\\\\";
                break;
            case '\n':
                os << "\\n";
                break;
            case '\r':
                os << "\\r";
                break;
            case '\t':
                os << "\\t";
                break;
            default:
                if (c < 0x20) {
                    char buf[7];
                    std::snprintf(buf, sizeof(buf), "\\u%04x", c);
                    os << buf;
                } else {
                    os << static_cast<char>(c);
                }
        }
    }
    os << '"';
    return os.str();
}

static std::optional<std::string> json_get_string(const char* json, const char* key) {
    if (!json)
        return std::nullopt;
    std::string s(json);
    std::string k = std::string("\"") + key + "\"";
    auto pos = s.find(k);
    if (pos == std::string::npos)
        return std::nullopt;
    pos = s.find(':', pos);
    if (pos == std::string::npos)
        return std::nullopt;
    pos = s.find('"', pos);
    if (pos == std::string::npos)
        return std::nullopt;
    auto end = s.find('"', pos + 1);
    if (end == std::string::npos)
        return std::nullopt;
    return s.substr(pos + 1, end - (pos + 1));
}

static std::optional<int> json_get_int(const char* json, const char* key) {
    if (!json)
        return std::nullopt;
    std::string s(json);
    std::string k = std::string("\"") + key + "\"";
    auto pos = s.find(k);
    if (pos == std::string::npos)
        return std::nullopt;
    pos = s.find(':', pos);
    if (pos == std::string::npos)
        return std::nullopt;
    // naive parse until next comma/brace
    auto end = s.find_first_of(",}", pos + 1);
    std::string num = s.substr(pos + 1, (end == std::string::npos ? s.size() : end) - (pos + 1));
    try {
        return std::stoi(num);
    } catch (...) {
        return std::nullopt;
    }
}

// C ABI shims
static int c_create(const char* /*config_json*/, void** out_backend) {
    // Expect the table to have a pre-bound impl in out_backend via our expose helper
    // Nothing to do here; just ensure non-null
    if (!out_backend)
        return -1;
    return 0;
}

static void c_destroy(void* backend) {
    auto* h = static_cast<BackendHandle*>(backend);
    delete h;
}

static int c_put(void* backend, const char* key, const void* buf, size_t len,
                 const char* /*opts_json*/) {
    auto* h = static_cast<BackendHandle*>(backend);
    if (!h || !key || (!buf && len != 0))
        return -1;
    const auto* data_bytes = reinterpret_cast<const std::byte*>(buf);
    auto r = h->impl->put(key, data_bytes, len, PutOptions{});
    return std::holds_alternative<PutResult>(r) ? 0 : -1;
}

static int c_get(void* backend, const char* key, void** out_buf, size_t* out_len,
                 const char* /*opts_json*/) {
    auto* h = static_cast<BackendHandle*>(backend);
    if (!h || !key || !out_buf || !out_len)
        return -1;
    auto r = h->impl->get(key, std::nullopt, GetOptions{});
    if (std::holds_alternative<std::vector<std::uint8_t>>(r)) {
        const auto& v = std::get<std::vector<std::uint8_t>>(r);
        void* p = std::malloc(v.size());
        if (!p)
            return -2;
        std::memcpy(p, v.data(), v.size());
        *out_buf = p;
        *out_len = v.size();
        return 0;
    }
    return -1;
}

static int c_head(void* backend, const char* key, char** out_metadata_json,
                  const char* /*opts_json*/) {
    auto* h = static_cast<BackendHandle*>(backend);
    if (!h || !key || !out_metadata_json)
        return -1;
    auto r = h->impl->head(key, GetOptions{});
    if (std::holds_alternative<ObjectMetadata>(r)) {
        const auto& md = std::get<ObjectMetadata>(r);
        std::ostringstream os;
        os << '{' << "\"size\":" << md.size;
        if (md.etag)
            os << ",\"etag\":" << escape_json(*md.etag);
        os << '}';
        *out_metadata_json = dup_cstr(os.str());
        return *out_metadata_json ? 0 : -2;
    }
    return -1;
}

static int c_del(void* backend, const char* key, const char* /*opts_json*/) {
    auto* h = static_cast<BackendHandle*>(backend);
    if (!h || !key)
        return -1;
    auto r = h->impl->remove(key);
    return r ? 0 : -1;
}

static int c_list(void* backend, const char* prefix, char** out_list_json, const char* opts_json) {
    auto* h = static_cast<BackendHandle*>(backend);
    if (!h || !out_list_json)
        return -1;
    std::optional<std::string> delim = json_get_string(opts_json, "delimiter");
    auto pageToken = json_get_string(opts_json, "pageToken");
    auto maxKeys = json_get_int(opts_json, "maxKeys");
    auto r = h->impl->list(prefix ? prefix : std::string_view{}, delim, pageToken, maxKeys);
    if (std::holds_alternative<Page<ObjectSummary>>(r)) {
        const auto& page = std::get<Page<ObjectSummary>>(r);
        std::ostringstream os;
        os << '{' << "\"items\":[";
        for (size_t i = 0; i < page.items.size(); ++i) {
            if (i)
                os << ',';
            os << escape_json(page.items[i].key);
        }
        os << ']';
        if (page.nextPageToken)
            os << ",\"nextPageToken\":" << escape_json(*page.nextPageToken);
        // Derive common prefixes if delimiter is provided
        os << ",\"prefixes\":";
        if (delim && !delim->empty()) {
            std::set<std::string> prefixes;
            std::string base = prefix ? std::string(prefix) : std::string();
            for (const auto& it : page.items) {
                const std::string& k = it.key;
                if (k.rfind(base, 0) == 0) {
                    std::string_view rest(k.c_str() + base.size(), k.size() - base.size());
                    auto pos = rest.find(delim->c_str());
                    if (pos != std::string_view::npos) {
                        std::string p = base + std::string(rest.substr(0, pos)) + *delim;
                        prefixes.insert(std::move(p));
                    }
                }
            }
            os << '[';
            bool first = true;
            for (const auto& p : prefixes) {
                if (!first)
                    os << ',';
                first = false;
                os << escape_json(p);
            }
            os << ']';
        } else {
            os << "[]";
        }
        os << '}';
        *out_list_json = dup_cstr(os.str());
        return *out_list_json ? 0 : -2;
    }
    return -1;
}

} // namespace

std::shared_ptr<IObjectStorageBackend> wrap_c_abi(yams_object_storage_v1* /*v1_iface*/) {
    // Not implemented in this slice (out of scope); can be added on demand.
    return nullptr;
}

yams_object_storage_v1* expose_as_c_abi(std::shared_ptr<IObjectStorageBackend> impl) {
    if (!impl)
        return nullptr;
    auto* handle = new BackendHandle{std::move(impl)};
    // Allocate and populate the interface table
    auto* v = static_cast<yams_object_storage_v1*>(std::malloc(sizeof(yams_object_storage_v1)));
    if (!v) {
        delete handle;
        return nullptr;
    }
    v->size = sizeof(yams_object_storage_v1);
    v->version = 1;
    v->create = c_create;   // no-op; backend already bound
    v->destroy = c_destroy; // frees the BackendHandle
    v->put = c_put;
    v->get = c_get;
    v->head = c_head;
    v->del = c_del;
    v->list = c_list;
    // The caller should pass 'handle' as the backend pointer to these functions.
    // Ownership: destroy() will delete handle.
    return v;
}

std::pair<yams_object_storage_v1*, void*>
expose_as_c_abi_with_state(std::shared_ptr<IObjectStorageBackend> impl) {
    if (!impl)
        return {nullptr, nullptr};
    auto* handle = new BackendHandle{impl};
    auto* v = expose_as_c_abi(impl);
    if (!v) {
        delete handle;
        return {nullptr, nullptr};
    }
    return {v, static_cast<void*>(handle)};
}

std::string to_json(const PutOptions& /*opts*/) {
    return "{}";
}
std::string to_json(const GetOptions& /*opts*/) {
    return "{}";
}
std::string to_json(const ObjectMetadata& md) {
    std::ostringstream os;
    os << '{' << "\"size\":" << md.size;
    if (md.etag)
        os << ",\"etag\":" << escape_json(*md.etag);
    os << '}';
    return os.str();
}

PutOptions parse_put_options(const char* /*json*/) {
    return PutOptions{};
}
GetOptions parse_get_options(const char* /*json*/) {
    return GetOptions{};
}

} // namespace yams::plugins::adapter
