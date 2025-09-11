#include <string>
#include <vector>
#include <gtest/gtest.h>
#include <yams/extraction/text_extractor.h>

using namespace yams::extraction;

TEST(EncodingDetectorTest, Utf16LE_To_Utf8) {
    // BOM + "Hi" (H=0x0048, i=0x0069)
    std::string utf16le;
    utf16le.push_back('\xFF');
    utf16le.push_back('\xFE'); // BOM LE
    // 'H' 0x48 0x00
    utf16le.push_back('\x48');
    utf16le.push_back('\x00');
    // 'i' 0x69 0x00
    utf16le.push_back('\x69');
    utf16le.push_back('\x00');

    auto out = EncodingDetector::convertToUtf8(utf16le, "UTF-16LE");
    ASSERT_TRUE(out);
    EXPECT_EQ(out.value(), std::string("Hi"));
}

TEST(EncodingDetectorTest, Utf16BE_To_Utf8) {
    // BOM + "Hi" (H=0x0048, i=0x0069)
    std::string utf16be;
    utf16be.push_back('\xFE');
    utf16be.push_back('\xFF'); // BOM BE
    // 'H' 0x00 0x48
    utf16be.push_back('\x00');
    utf16be.push_back('\x48');
    // 'i' 0x00 0x69
    utf16be.push_back('\x00');
    utf16be.push_back('\x69');

    auto out = EncodingDetector::convertToUtf8(utf16be, "UTF-16BE");
    ASSERT_TRUE(out);
    EXPECT_EQ(out.value(), std::string("Hi"));
}

TEST(EncodingDetectorTest, Latin1_To_Utf8) {
    // ISO-8859-1: "Caf\xE9" (é=0xE9)
    std::string latin1 = std::string("Caf") + std::string(1, static_cast<char>(0xE9));
    auto out = EncodingDetector::convertToUtf8(latin1, "ISO-8859-1");
    ASSERT_TRUE(out);
    EXPECT_EQ(out.value(), std::string("Café"));
}
