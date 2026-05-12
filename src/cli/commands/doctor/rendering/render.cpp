#include <yams/cli/doctor/rendering/render.h>
#include <yams/cli/ui_helpers.hpp>

namespace yams::cli::doctor {

void DoctorRender::printHeader(std::ostream& os, const std::string& title) {
    os << "\n" << title << "\n";
    for (size_t i = 0; i < title.size(); ++i)
        os << '-';
    os << "\n";
}

void DoctorRender::printStatusLine(std::ostream& os, const std::string& label,
                                   const std::string& value) {
    os << "- " << label << ": " << value << "\n";
}

void DoctorRender::printSummary(std::ostream& os, const std::string& title,
                                const std::vector<StepResult>& steps) {
    printHeader(os, title);
    for (const auto& s : steps) {
        os << "  " << (s.ok ? ui::status_ok(s.name) : ui::status_error(s.name));
        if (!s.message.empty())
            os << " — " << s.message;
        os << "\n";
    }
}

} // namespace yams::cli::doctor
