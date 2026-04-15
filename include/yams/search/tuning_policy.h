#pragma once

// Thin forwarding header. `ITuningPolicy`, `TuningContext`, and the
// `TuningPolicyPtr` alias are declared in `search_tuner.h` to keep
// inheritance clean without a circular include. Future R2+ work adds
// fields to `TuningContext`; consumers should include this header rather
// than reaching into `search_tuner.h` directly.
#include <yams/search/search_tuner.h>
