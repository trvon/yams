// SPDX-License-Identifier: GPL-3.0-or-later
// LanguageDetector unit tests: stop-word voting, whole-word matching, ties and confidence.

#include <catch2/catch_test_macros.hpp>
#include <yams/extraction/text_extractor.h>

#include <string>

using yams::extraction::LanguageDetector;

TEST_CASE("LanguageDetector picks the language with the most stop words",
          "[extraction][language]") {
    double confidence = 0.0;
    CHECK(LanguageDetector::detectLanguage(
              "The cat is on the mat, and the dogs are in the yard for most of the day.",
              &confidence) == "en");
    CHECK(confidence == 0.9);

    CHECK(LanguageDetector::detectLanguage(
              "El gato es una criatura que vive con la familia y duerme por la tarde.",
              &confidence) == "es");
    CHECK(confidence == 0.9);

    CHECK(LanguageDetector::detectLanguage(
              "Le chat est dans le jardin avec une balle et il joue pour le plaisir.",
              &confidence) == "fr");
    CHECK(confidence == 0.9);
}

TEST_CASE("LanguageDetector counts whole words only, in any letter case",
          "[extraction][language]") {
    double confidence = 0.0;

    // "the" inside "other", "is" inside "this" and "or" inside "for_or" do not count; the
    // digit and underscore neighbours of "for" and "of" make those whole words.
    CHECK(LanguageDetector::detectLanguage("other this bathe", &confidence) == "en");
    CHECK(confidence == 0.3);

    // Three English stop words: enough for the middle confidence band only.
    CHECK(LanguageDetector::detectLanguage("THE 1for_ Of.", &confidence) == "en");
    CHECK(confidence == 0.7);

    // A repeated stop word votes once.
    CHECK(LanguageDetector::detectLanguage("the the the the the the the", &confidence) == "en");
    CHECK(confidence == 0.3);

    // Bytes outside ASCII end a word, so "et" still counts next to an accented letter.
    CHECK(LanguageDetector::detectLanguage("le \xC3\xA9t\xC3\xA9 et l'hiver", &confidence) == "fr");
}

TEST_CASE("LanguageDetector resolves ties toward English, then Spanish", "[extraction][language]") {
    double confidence = 0.0;
    CHECK(LanguageDetector::detectLanguage("", &confidence) == "en");
    CHECK(confidence == 0.3);
    CHECK(LanguageDetector::detectLanguage("int main() { return 0; }") == "en");

    // One vote each.
    CHECK(LanguageDetector::detectLanguage("the el le") == "en");
    // "de", "un" and "que" vote for Spanish and French alike.
    CHECK(LanguageDetector::detectLanguage("de un que") == "es");
    // One more French-only word breaks that tie.
    CHECK(LanguageDetector::detectLanguage("de un que avec") == "fr");
}

TEST_CASE("LanguageDetector finds stop words anywhere in a long document",
          "[extraction][language]") {
    // The scan stops early once every stop word is seen; it must not stop before that.
    std::string text(200000, 'x');
    text += " le chat est dans le jardin avec une balle et il joue pour le plaisir";
    double confidence = 0.0;
    CHECK(LanguageDetector::detectLanguage(text, &confidence) == "fr");
    CHECK(confidence == 0.9);
}
