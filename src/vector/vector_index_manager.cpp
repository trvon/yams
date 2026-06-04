#include <utility>
#include <yams/vector/vector_index_manager.h>

namespace yams::vector {

VectorIndexManager::VectorIndexManager(ISearchIndex& searchIndex,
                                       std::function<bool()> isInitialized)
    : index_(&searchIndex), isInitialized_(std::move(isInitialized)) {}

Result<void> VectorIndexManager::buildIndex() {
    if (!index_)
        return Error{ErrorCode::NotInitialized, "Index facade not set"};
    return index_->buildIndex();
}

Result<void> VectorIndexManager::prepareSearchIndex() {
    if (!index_)
        return Error{ErrorCode::NotInitialized, "Index facade not set"};
    return index_->prepareSearchIndex();
}

Result<bool> VectorIndexManager::hasReusablePersistedSearchIndex() {
    if (!index_)
        return Error{ErrorCode::NotInitialized, "Index facade not set"};
    return index_->hasReusablePersistedSearchIndex();
}

Result<void> VectorIndexManager::optimize() {
    if (!index_)
        return Error{ErrorCode::NotInitialized, "Index facade not set"};
    return index_->optimize();
}

Result<void> VectorIndexManager::persistIndex() {
    if (!index_)
        return Error{ErrorCode::NotInitialized, "Index facade not set"};
    return index_->persistIndex();
}

bool VectorIndexManager::isInitialized() const {
    return index_ && (!isInitialized_ || isInitialized_());
}

} // namespace yams::vector
