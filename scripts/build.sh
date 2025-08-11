#!/bin/bash

# Build script for Kronos

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default values
BUILD_TYPE=${1:-Debug}
BUILD_DIR="build-${BUILD_TYPE,,}"
GENERATOR=${CMAKE_GENERATOR:-"Ninja"}
CORES=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)

echo -e "${GREEN}Building Kronos (${BUILD_TYPE})${NC}"
echo "Build directory: ${BUILD_DIR}"
echo "Generator: ${GENERATOR}"
echo "Parallel jobs: ${CORES}"

# Create build directory
mkdir -p "${BUILD_DIR}"

# Configure
echo -e "${YELLOW}Configuring...${NC}"
cmake -B "${BUILD_DIR}" \
    -G "${GENERATOR}" \
    -DCMAKE_BUILD_TYPE="${BUILD_TYPE}" \
    -DKRONOS_BUILD_TESTS=ON \
    -DKRONOS_BUILD_BENCHMARKS=ON \
    -DKRONOS_BUILD_TOOLS=ON \
    -DKRONOS_ENABLE_SANITIZERS=$([[ "${BUILD_TYPE}" == "Debug" ]] && echo "ON" || echo "OFF") \
    -DCMAKE_EXPORT_COMPILE_COMMANDS=ON

# Build
echo -e "${YELLOW}Building...${NC}"
cmake --build "${BUILD_DIR}" --parallel "${CORES}"

# Run tests if in Debug mode
if [[ "${BUILD_TYPE}" == "Debug" ]]; then
    echo -e "${YELLOW}Running tests...${NC}"
    cd "${BUILD_DIR}"
    ctest --output-on-failure --parallel "${CORES}"
    cd ..
fi

echo -e "${GREEN}Build complete!${NC}"