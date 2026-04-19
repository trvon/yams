#pragma once

#include <memory>
#include <yams/daemon/resource/model_provider.h>

namespace yams::daemon {

std::unique_ptr<IModelProvider> makeSimeonModelProvider(std::size_t embeddingDim = 384);

// Anchor symbol: referenced from another TU (mock_model_provider.cpp) so the
// static archive linker cannot drop simeon_model_provider.o and skip its
// ModelProviderFactoryRegistration static initializer.
void forceLinkSimeonProvider() noexcept;

} // namespace yams::daemon
