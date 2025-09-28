#include <gtest/gtest.h>

#include <filesystem>
#include <memory>
#include <string>

#include <yams/app/services/services.hpp>

namespace yams::app::services::test {

namespace {
class NotFoundStore : public yams::api::IContentStore {
public:
    Result<api::StoreResult> store(const std::filesystem::path& path, const api::ContentMetadata&,
                                   api::ProgressCallback) override {
        return Error{ErrorCode::FileNotFound, std::string("File not found: ") + path.string()};
    }
    Result<api::RetrieveResult> retrieve(const std::string&, const std::filesystem::path&,
                                         api::ProgressCallback) override {
        return ErrorCode::NotImplemented;
    }
    Result<api::StoreResult> storeStream(std::istream&, const api::ContentMetadata&,
                                         api::ProgressCallback) override {
        return ErrorCode::NotImplemented;
    }
    Result<api::RetrieveResult> retrieveStream(const std::string&, std::ostream&,
                                               api::ProgressCallback) override {
        return ErrorCode::NotImplemented;
    }
    Result<api::StoreResult> storeBytes(std::span<const std::byte>,
                                        const api::ContentMetadata&) override {
        return ErrorCode::NotImplemented;
    }
    Result<std::vector<std::byte>> retrieveBytes(const std::string&) override {
        return ErrorCode::NotImplemented;
    }
    Result<bool> exists(const std::string&) const override { return ErrorCode::NotImplemented; }
    Result<bool> remove(const std::string&) override { return ErrorCode::NotImplemented; }
    Result<api::ContentMetadata> getMetadata(const std::string&) const override {
        return ErrorCode::NotImplemented;
    }
    Result<void> updateMetadata(const std::string&, const api::ContentMetadata&) override {
        return ErrorCode::NotImplemented;
    }
    std::vector<Result<api::StoreResult>>
    storeBatch(const std::vector<std::filesystem::path>&,
               const std::vector<api::ContentMetadata>&) override {
        return {};
    }
    std::vector<Result<bool>> removeBatch(const std::vector<std::string>&) override { return {}; }
    api::ContentStoreStats getStats() const override { return {}; }
    api::HealthStatus checkHealth() const override { return {}; }
    Result<void> verify(api::ProgressCallback) override { return ErrorCode::NotImplemented; }
    Result<void> compact(api::ProgressCallback) override { return ErrorCode::NotImplemented; }
    Result<void> garbageCollect(api::ProgressCallback) override {
        return ErrorCode::NotImplemented;
    }
};
} // namespace

TEST(DocumentServiceErrorsTest, EmitsHelpfulMessageOnFileNotFound) {
    AppContext ctx;
    ctx.store = std::make_shared<NotFoundStore>();
    auto svc = makeDocumentService(ctx);

    StoreDocumentRequest req;
    req.path = "a,b,c"; // common user mistake: comma-separated paths
    auto r = svc->store(req);
    ASSERT_FALSE(r);
    EXPECT_EQ(r.error().code, ErrorCode::FileNotFound);
    auto msg = r.error().message;
    EXPECT_NE(msg.find("a,b,c"), std::string::npos);
    EXPECT_NE(msg.find("commas"), std::string::npos);
}

} // namespace yams::app::services::test
