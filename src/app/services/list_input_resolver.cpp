#include <yams/app/services/list_input_resolver.hpp>

#include <filesystem>

namespace yams::app::services {

ResolvedListInput resolveNameToPatternIfLocalFile(const std::string& name) {
    ResolvedListInput out;
    out.pattern = name;
    if (name.empty())
        return out;
    namespace fs = std::filesystem;
    std::error_code ec;
    fs::path p{name};
    if (!fs::exists(p, ec) || !fs::is_regular_file(p, ec))
        return out;
    fs::path abs = fs::weakly_canonical(p, ec);
    if (ec) {
        std::error_code ec2;
        fs::path tmp = fs::absolute(p, ec2);
        abs = ec2 ? p : tmp;
    }
    out.isLocalFile = true;
    out.absPath = abs.string();
    out.pattern = abs.string(); // exact path preferred for list filters
    return out;
}

} // namespace yams::app::services
