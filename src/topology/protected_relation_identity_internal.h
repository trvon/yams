#pragma once

#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

namespace yams::topology::detail {

struct ProtectedRelationObservation {
    std::string_view lhs;
    std::string_view rhs;
    float score{0.0F};
};

[[nodiscard]] std::string
protectedRelationIdentityFromObservations(std::vector<ProtectedRelationObservation> observations);

} // namespace yams::topology::detail
