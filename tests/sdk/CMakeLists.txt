find_program(PYTHON3_EXEC python3 REQUIRED)

set(REPO_ROOT ${CMAKE_SOURCE_DIR})

add_test(NAME sdk_conformance_test
    COMMAND ${PYTHON3_EXEC} ${REPO_ROOT}/tests/sdk/test_sdk_conformance.py)
set_tests_properties(sdk_conformance_test PROPERTIES
    LABELS "sdk;python"
    TIMEOUT 30
    ENVIRONMENT "PYTHONPATH=${REPO_ROOT}/external/yams-sdk/python;REPO_ROOT=${REPO_ROOT}"
)

add_test(NAME sdk_python_template_plugin_test
    COMMAND ${PYTHON3_EXEC} ${REPO_ROOT}/tests/sdk/test_python_template_plugin.py)
set_tests_properties(sdk_python_template_plugin_test PROPERTIES
    LABELS "sdk;python"
    TIMEOUT 30
    ENVIRONMENT "PYTHONPATH=${REPO_ROOT}/external/yams-sdk/python;REPO_ROOT=${REPO_ROOT}"
)

