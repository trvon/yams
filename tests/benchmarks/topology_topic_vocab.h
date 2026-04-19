#pragma once

#include <array>
#include <string>
#include <vector>

namespace yams::bench {

struct TopicVocabEntry {
    const char* name;
    std::array<const char*, 6> primaryTerms;
    const char* queryPhrase;
};

inline const std::vector<TopicVocabEntry>& extendedTopicVocab() {
    static const std::vector<TopicVocabEntry> kVocab = {
        {"compiler",
         {"parser", "lexer", "ast", "codegen", "optimizer", "linker"},
         "parser ast codegen optimizer"},
        {"machinelearning",
         {"gradient", "tensor", "epoch", "neuron", "regression", "loss"},
         "tensor gradient epoch loss"},
        {"finance",
         {"ledger", "invoice", "asset", "yield", "portfolio", "dividend"},
         "ledger invoice yield portfolio"},
        {"cooking",
         {"oven", "skillet", "marinade", "broth", "garnish", "knead"},
         "skillet marinade broth garnish"},
        {"astronomy",
         {"orbit", "nebula", "telescope", "spectrum", "comet", "magnitude"},
         "orbit nebula telescope spectrum"},
        {"biology",
         {"cell", "enzyme", "protein", "gene", "membrane", "nucleus"},
         "enzyme protein gene membrane"},
        {"music",
         {"chord", "rhythm", "tempo", "melody", "harmony", "octave"},
         "chord rhythm melody harmony"},
        {"sports",
         {"goal", "referee", "tactics", "penalty", "stadium", "tournament"},
         "referee tactics penalty tournament"},
        {"agriculture",
         {"harvest", "irrigation", "soil", "fertilizer", "crop", "tractor"},
         "harvest irrigation soil fertilizer"},
        {"medicine",
         {"diagnosis", "dosage", "antibody", "vaccine", "syringe", "anesthesia"},
         "diagnosis dosage antibody vaccine"},
        {"transportation",
         {"freight", "diesel", "tunnel", "ferry", "junction", "railcar"},
         "freight diesel tunnel ferry"},
        {"architecture",
         {"facade", "vault", "buttress", "atrium", "rebar", "blueprint"},
         "facade vault atrium blueprint"},
        {"meteorology",
         {"barometer", "humidity", "cyclone", "isobar", "stratus", "doppler"},
         "barometer humidity cyclone isobar"},
        {"chemistry",
         {"reagent", "molarity", "isomer", "catalyst", "titration", "solvent"},
         "reagent molarity catalyst titration"},
        {"law",
         {"plaintiff", "tort", "subpoena", "jurisdiction", "litigation", "precedent"},
         "plaintiff tort subpoena litigation"},
        {"linguistics",
         {"morpheme", "syntax", "phoneme", "lexicon", "discourse", "pragmatics"},
         "morpheme syntax phoneme lexicon"},
        {"gaming",
         {"sprite", "shader", "respawn", "loadout", "speedrun", "hitbox"},
         "sprite shader respawn loadout"},
        {"travel",
         {"itinerary", "passport", "boarding", "concierge", "embassy", "hostel"},
         "itinerary passport boarding concierge"},
    };
    return kVocab;
}

} // namespace yams::bench
