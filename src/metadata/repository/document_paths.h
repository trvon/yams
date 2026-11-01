#pragma once

#include <string>

namespace yams::metadata::repository {

// A read projection, not a second content store. Explicit local path heads
// override canonical document paths; legacy rows remain readable until indexed.
std::string documentPathQueryCte(bool currentOnly);

} // namespace yams::metadata::repository
