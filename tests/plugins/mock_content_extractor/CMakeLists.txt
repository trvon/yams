add_library(yams_mock_content_extractor_plugin SHARED
    mock_content_extractor_plugin.cpp
)

target_include_directories(yams_mock_content_extractor_plugin
    PRIVATE
        ${CMAKE_SOURCE_DIR}/include
)

target_compile_features(yams_mock_content_extractor_plugin PUBLIC cxx_std_20)

set_target_properties(yams_mock_content_extractor_plugin PROPERTIES
    INTERPROCEDURAL_OPTIMIZATION FALSE
    C_VISIBILITY_PRESET default
    CXX_VISIBILITY_PRESET default
    VISIBILITY_INLINES_HIDDEN OFF
)

