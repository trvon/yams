#include <yams/app/services/list_input_resolver.hpp>
#include <yams/app/services/services.hpp>

#include <filesystem>

namespace yams::app::services {

ResolvedListInput resolveNameToPatternIfLocalFile(const std::string& name) {
    ResolvedListInput out;
    out.pattern = name;
    if (name.empty())
        return out;

    auto normalized = utils::normalizeLookupPath(name);
    if (normalized.changed) {
        out.pattern = normalized.normalized;
    }
    if (normalized.hasWildcards) {
        out.kind = ResolvedListInputKind::Pattern;
        return out;
    }

    namespace fs = std::filesystem;
    std::error_code ec;
    fs::path p{out.pattern};
    if (!fs::exists(p, ec)) {
        out.kind = ResolvedListInputKind::HistoricalPath;
        return out;
    }

    fs::path abs = fs::weakly_canonical(p, ec);
    if (ec) {
        std::error_code ec2;
        fs::path tmp = fs::absolute(p, ec2);
        abs = ec2 ? p : tmp;
    }
    out.absPath = abs.string();

    if (fs::is_directory(p, ec)) {
        out.kind = ResolvedListInputKind::LocalDirectory;
        out.isLocalDirectory = true;
        out.pattern = abs.string();
        return out;
    }

    if (fs::is_regular_file(p, ec)) {
        out.kind = ResolvedListInputKind::LocalFile;
        out.isLocalFile = true;
        out.pattern = abs.string();
        return out;
    }

    out.kind = ResolvedListInputKind::HistoricalPath;
    out.pattern = abs.string();
    return out;
}

} // namespace yams::app::services
