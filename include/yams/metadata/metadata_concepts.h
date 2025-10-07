#pragma once

#include <concepts>
#include <optional>
#include <span>
#include <string>
#include <vector>
#include <yams/core/types.h>
#include <yams/metadata/document_metadata.h>

namespace yams::metadata {

template <typename T>
concept DocumentStore = requires(T store, DocumentInfo info, int64_t id) {
    { store.insertDocument(info) } -> std::same_as<Result<int64_t>>;
    { store.getDocument(id) } -> std::same_as<Result<std::optional<DocumentInfo>>>;
    { store.updateDocument(info) } -> std::same_as<Result<void>>;
    { store.deleteDocument(id) } -> std::same_as<Result<void>>;
};

template <typename T>
concept ContentStore = requires(T store, DocumentContent content, int64_t id) {
    { store.insertContent(content) } -> std::same_as<Result<void>>;
    { store.getContent(id) } -> std::same_as<Result<std::optional<DocumentContent>>>;
};

template <typename T>
concept SearchStore = requires(T store, std::string query) {
    { store.search(query) } -> std::same_as<Result<SearchResults>>;
};

template <typename T>
concept MetadataKVStore = requires(T store, int64_t id, std::string key, MetadataValue val) {
    { store.setMetadata(id, key, val) } -> std::same_as<Result<void>>;
    { store.getMetadata(id, key) } -> std::same_as<Result<std::optional<MetadataValue>>>;
};

template <typename T>
concept FullMetadataStore =
    DocumentStore<T> && ContentStore<T> && SearchStore<T> && MetadataKVStore<T>;

} // namespace yams::metadata
