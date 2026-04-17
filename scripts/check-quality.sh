#!/usr/bin/env bash

# Comprehensive static analysis using cppcheck and other quality tools

set -euo pipefail

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
MAGENTA='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Default values
PROFILE="normal"
OUTPUT_FORMAT="human"
OUTPUT_DIR=""
GIT_ONLY=false
DIFF_BASE=""
PARALLEL=true
VERBOSE=false
FIX_FORMAT=false
BUILD_DIR=""
COMPILE_COMMANDS=""
EXCLUDE_DIRS=""
INCLUDE_DIRS=""
SUPPRESSIONS_FILE=""
BASELINE_FILE=""
GENERATE_BASELINE=false
CPPCHECK_BUILD_DIR=""
CLANG_TIDY_AVAILABLE=false

# Script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Function to print colored output
print_color() {
    local color=$1
    shift
    echo -e "${color}$*${NC}"
}

# Function to print section headers
print_section() {
    print_color "$CYAN" "\n==== $* ===="
}

# Function to print progress
print_progress() {
    print_color "$BLUE" "→ $*"
}

# Function to print success
print_success() {
    print_color "$GREEN" "✓ $*"
}

# Function to print warning
print_warning() {
    print_color "$YELLOW" "⚠ $*"
}

# Function to print error
print_error() {
    print_color "$RED" "✗ $*"
}

# Usage information
usage() {
    cat << EOF
Usage: $(basename "$0") [OPTIONS]

Code Quality Analysis Script for YAMS C++ Project

OPTIONS:
    Quality Profiles:
        -p, --profile PROFILE    Quality profile: strict, normal, quick, ci (default: normal)
        
    Output Options:
        -f, --format FORMAT      Output format: human, xml, json, sarif (default: human)
        -o, --output DIR         Output directory for reports (default: stdout)
        -v, --verbose            Show detailed output and progress
        
    Source Selection:
        -g, --git                Only analyze files changed in git (staged and unstaged)
        --diff-base REV          Only analyze C/C++ files changed since REV and only fail on
                                 findings that land on changed lines
        -d, --build-dir DIR      Build directory for compile_commands.json
        
    Analysis Options:
        --fix-format             Automatically fix formatting issues found
        --suppressions FILE      Use custom suppressions file
        --baseline FILE          Compare against baseline results
        --generate-baseline      Generate baseline file for future comparisons
        
    Performance:
        -j, --parallel           Enable parallel analysis (default: true)
        -s, --serial             Force serial processing
        
    Advanced:
        --exclude DIRS           Additional directories to exclude (comma-separated)
        --include DIRS           Additional include directories (comma-separated)
        --compile-commands FILE  Explicit path to compile_commands.json
        
    Help:
        -h, --help               Show this help message

PROFILES:
    strict      All checks enabled, warnings as errors, strict coding standards
    normal      Standard checks with reasonable suppressions (default)
    quick       Essential checks only for rapid feedback
    ci          Optimized for continuous integration with machine-readable output

OUTPUT FORMATS:
    human       Human-readable colored terminal output (default)
    xml         XML format for IDE integration
    json        JSON format for tool integration
    sarif       SARIF format for GitHub integration

EXAMPLES:
    # Basic quality check
    $(basename "$0")
    
    # Strict analysis with XML output
    $(basename "$0") --profile strict --format xml --output reports/
    
    # Quick check on git changes only
    $(basename "$0") --profile quick --git

    # CI regression check against a base commit
    $(basename "$0") --profile ci --diff-base origin/main
    
    # CI mode with baseline comparison
    $(basename "$0") --profile ci --format json --baseline quality-baseline.json
    
    # Generate new baseline
    $(basename "$0") --generate-baseline --output quality-baseline.json
    
    # Fix formatting issues automatically
    $(basename "$0") --fix-format --git

EOF
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -p|--profile)
            PROFILE="$2"
            shift 2
            ;;
        -f|--format)
            OUTPUT_FORMAT="$2"
            shift 2
            ;;
        -o|--output)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        -g|--git)
            GIT_ONLY=true
            shift
            ;;
        --diff-base)
            DIFF_BASE="$2"
            shift 2
            ;;
        -d|--build-dir)
            BUILD_DIR="$2"
            shift 2
            ;;
        --fix-format)
            FIX_FORMAT=true
            shift
            ;;
        --suppressions)
            SUPPRESSIONS_FILE="$2"
            shift 2
            ;;
        --baseline)
            BASELINE_FILE="$2"
            shift 2
            ;;
        --generate-baseline)
            GENERATE_BASELINE=true
            shift
            ;;
        -j|--parallel)
            PARALLEL=true
            shift
            ;;
        -s|--serial)
            PARALLEL=false
            shift
            ;;
        --exclude)
            EXCLUDE_DIRS="$2"
            shift 2
            ;;
        --include)
            INCLUDE_DIRS="$2"
            shift 2
            ;;
        --compile-commands)
            COMPILE_COMMANDS="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Validate profile
case $PROFILE in
    strict|normal|quick|ci)
        ;;
    *)
        print_error "Invalid profile: $PROFILE"
        print_error "Valid profiles: strict, normal, quick, ci"
        exit 1
        ;;
esac

# Validate output format
case $OUTPUT_FORMAT in
    human|xml|json|sarif)
        ;;
    *)
        print_error "Invalid output format: $OUTPUT_FORMAT"
        print_error "Valid formats: human, xml, json, sarif"
        exit 1
        ;;
esac

if [[ "$GIT_ONLY" == true && -n "$DIFF_BASE" ]]; then
    print_error "--git and --diff-base are mutually exclusive"
    exit 1
fi

if [[ -n "$DIFF_BASE" ]]; then
    if ! git rev-parse --verify "${DIFF_BASE}^{commit}" >/dev/null 2>&1; then
        print_error "Invalid --diff-base revision: $DIFF_BASE"
        exit 1
    fi
fi

# Check for required tools
check_tool() {
    local tool=$1
    local package=$2
    local description=$3
    
    if ! command -v "$tool" &> /dev/null; then
        print_error "$tool not found in PATH"
        print_error "$description"
        print_error "Install with: $package"
        return 1
    fi
    return 0
}

print_section "Checking Required Tools"

MISSING_TOOLS=false

if ! check_tool "cppcheck" "brew install cppcheck" "Static analysis tool"; then
    MISSING_TOOLS=true
fi

# Check for optional tools
if ! command -v "clang-format" &> /dev/null; then
    print_warning "clang-format not found (needed for --fix-format)"
    print_warning "Install with: brew install clang-format"
    if [[ "$FIX_FORMAT" == true ]]; then
        print_error "--fix-format requires clang-format"
        exit 1
    fi
fi

if command -v "clang-tidy" &> /dev/null; then
    CLANG_TIDY_AVAILABLE=true
else
    print_warning "clang-tidy not found (compile database audit only)"
    print_warning "Install with: brew install llvm"
fi

if [[ "$MISSING_TOOLS" == true ]]; then
    exit 1
fi

print_success "All required tools found"

# Change to project root
cd "$PROJECT_ROOT"

# Set up default paths
if [[ -z "$BUILD_DIR" ]]; then
    # Look for common build directories, preferring active Meson trees with compile_commands.json
    for candidate in \
        "builddir-nosan" \
        "builddir" \
        "builddir-asan" \
        "builddir-ubsan" \
        "build/asan" \
        "build/yams-debug" \
        "build/yams-release" \
        "build" \
        "cmake-build-release" \
        "cmake-build-debug"; do
        if [[ -d "$candidate" ]]; then
            BUILD_DIR="$candidate"
            break
        fi
    done
fi

if [[ -z "$COMPILE_COMMANDS" && -n "$BUILD_DIR" ]]; then
    if [[ -f "$BUILD_DIR/compile_commands.json" ]]; then
        COMPILE_COMMANDS="$BUILD_DIR/compile_commands.json"
    fi
fi

if [[ -z "$SUPPRESSIONS_FILE" ]]; then
    SUPPRESSIONS_FILE="$PROJECT_ROOT/.cppcheck-suppressions"
fi

if [[ -z "$CPPCHECK_BUILD_DIR" ]]; then
    if [[ -n "$BUILD_DIR" ]]; then
        CPPCHECK_BUILD_DIR="$BUILD_DIR/.cppcheck"
    else
        CPPCHECK_BUILD_DIR="$PROJECT_ROOT/.cppcheck"
    fi
fi
mkdir -p "$CPPCHECK_BUILD_DIR"

# Create output directory if specified
if [[ -n "$OUTPUT_DIR" ]]; then
    mkdir -p "$OUTPUT_DIR"
fi

print_section "Configuration"
print_progress "Profile: $PROFILE"
print_progress "Output format: $OUTPUT_FORMAT"
[[ -n "$OUTPUT_DIR" ]] && print_progress "Output directory: $OUTPUT_DIR"
[[ -n "$BUILD_DIR" ]] && print_progress "Build directory: $BUILD_DIR"
[[ -n "$COMPILE_COMMANDS" ]] && print_progress "Compile commands: $COMPILE_COMMANDS"
[[ -f "$SUPPRESSIONS_FILE" ]] && print_progress "Suppressions file: $SUPPRESSIONS_FILE"
[[ -n "$CPPCHECK_BUILD_DIR" ]] && print_progress "Cppcheck build dir: $CPPCHECK_BUILD_DIR"
print_progress "Git only: $GIT_ONLY"
[[ -n "$DIFF_BASE" ]] && print_progress "Diff base: $DIFF_BASE"
print_progress "Parallel: $PARALLEL"
print_progress "Fix format: $FIX_FORMAT"

meson_option_value() {
    local build_dir=$1
    local option_name=$2
    local intro="$build_dir/meson-info/intro-buildoptions.json"

    if [[ ! -f "$intro" ]]; then
        return 1
    fi

    python3 - "$intro" "$option_name" <<'PY'
import json
import sys

intro_path = sys.argv[1]
option_name = sys.argv[2]
with open(intro_path, 'r', encoding='utf-8') as fh:
    data = json.load(fh)
for item in data:
    if item.get('name') == option_name:
        value = item.get('value')
        if isinstance(value, list):
            print(','.join(str(v) for v in value))
        else:
            print(value)
        break
PY
}

find_first_build_dir() {
    for candidate in "$@"; do
        if [[ -d "$candidate" ]]; then
            echo "$candidate"
            return 0
        fi
    done
    return 1
}

report_sanitizer_build() {
    local label=$1
    local expected=$2
    shift 2

    local build_dir
    build_dir=$(find_first_build_dir "$@") || {
        print_warning "$label build directory not found"
        return 0
    }

    local intro="$build_dir/meson-info/intro-buildoptions.json"
    local ccdb="$build_dir/compile_commands.json"
    if [[ ! -f "$intro" ]]; then
        print_warning "$label build dir found but Meson intro options missing: $build_dir"
        return 0
    fi

    local sanitize
    sanitize=$(meson_option_value "$build_dir" "b_sanitize" || true)
    local buildtype
    buildtype=$(meson_option_value "$build_dir" "buildtype" || true)
    local lundef
    lundef=$(meson_option_value "$build_dir" "b_lundef" || true)

    if [[ "$sanitize" == *"$expected"* ]]; then
        print_success "$label build ready: $build_dir (buildtype=$buildtype, b_sanitize=$sanitize)"
    else
        print_warning "$label build dir found but sanitizer mismatch: $build_dir (b_sanitize=$sanitize)"
    fi

    if [[ -f "$ccdb" ]]; then
        print_progress "$label compile database: $ccdb"
    else
        print_warning "$label compile database missing: $build_dir/compile_commands.json"
    fi

    if [[ "$expected" == "address" && "$lundef" == "true" ]]; then
        print_warning "$label uses Clang ASAN with b_lundef=true; Meson warns this may fail. Prefer -Db_lundef=false."
    fi
}

print_section "Toolchain Readiness"
if [[ -n "$COMPILE_COMMANDS" && -f "$COMPILE_COMMANDS" ]]; then
    print_success "Compile commands available: $COMPILE_COMMANDS"
else
    print_warning "compile_commands.json not found; clang-tidy and project-aware checks are limited"
fi

if [[ "$CLANG_TIDY_AVAILABLE" == true ]]; then
    print_success "clang-tidy available"
else
    print_warning "clang-tidy unavailable in PATH"
fi

report_sanitizer_build "TSAN" "thread" "builddir" "build/tsan" "builddir-tsan"
report_sanitizer_build "ASAN" "address" "builddir-asan" "build/asan" "builddir-asan"
report_sanitizer_build "UBSAN" "undefined" "builddir-ubsan" "build/ubsan" "builddir-ubsan"

# Get list of files to analyze
print_section "Collecting Source Files"

if [[ -n "$DIFF_BASE" ]]; then
    print_progress "Getting list of files changed since $DIFF_BASE..."

    FILES=$(git diff --name-only --diff-filter=ACMR "$DIFF_BASE"...HEAD 2>/dev/null || true)

    FILES=$(echo "$FILES" | grep -E '\.(cpp|h|hpp|cc|cxx|c)$' | sort -u || true)

    if [[ -z "$FILES" ]]; then
        print_success "No changed C++ files found since $DIFF_BASE"
        exit 0
    fi

    FILE_ARRAY=()
    while IFS= read -r line; do
        [[ -n "$line" ]] && FILE_ARRAY+=("$line")
    done <<< "$FILES"
elif [[ "$GIT_ONLY" == true ]]; then
    print_progress "Getting list of git-modified files..."
    
    # Get both staged and unstaged files
    FILES=$(git diff --name-only --diff-filter=ACMR HEAD 2>/dev/null || true)
    FILES+=$'\n'
    FILES+=$(git diff --cached --name-only --diff-filter=ACMR 2>/dev/null || true)
    
    # Filter for C++ files and remove duplicates
    FILES=$(echo "$FILES" | grep -E '\.(cpp|h|hpp|cc|cxx|c)$' | sort -u || true)
    
    if [[ -z "$FILES" ]]; then
        print_success "No modified C++ files found"
        exit 0
    fi
    
    # Convert to array
    FILE_ARRAY=()
    while IFS= read -r line; do
        [[ -n "$line" ]] && FILE_ARRAY+=("$line")
    done <<< "$FILES"
else
    # Find all C++ source files, excluding build directories and third-party code
    print_progress "Finding all C++ source files..."
    
    FIND_EXCLUDES=(
        -not -path "./build/*"
        -not -path "./builddir/*"
        -not -path "./.cache/*"
        -not -path "./cmake-build-*/*"
        -not -path "./third_party/*"
        -not -path "./external/*"
        -not -path "./vendor/*"
        -not -path "./.git/*"
        -not -path "./examples/plugins/*"
        -not -path "*/generated/*"
        -not -path "*/_deps/*"
        -not -path "*.pb.h"
        -not -path "*.pb.cc"
    )
    
    # Add custom excludes
    if [[ -n "$EXCLUDE_DIRS" ]]; then
        IFS=',' read -ra EXCLUDE_ARRAY <<< "$EXCLUDE_DIRS"
        for exclude in "${EXCLUDE_ARRAY[@]}"; do
            FIND_EXCLUDES+=(-not -path "./${exclude}/*")
        done
    fi
    
    FILE_ARRAY=()
    while IFS= read -r line; do
        FILE_ARRAY+=("$line")
    done < <(find . \
        -type f \
        \( -name "*.cpp" -o -name "*.h" -o -name "*.hpp" -o -name "*.cc" -o -name "*.cxx" -o -name "*.c" \) \
        "${FIND_EXCLUDES[@]}" \
        | sort)
fi

# Check if we found any files
if [[ ${#FILE_ARRAY[@]} -eq 0 ]]; then
    print_warning "No C++ files found to analyze"
    exit 0
fi

print_success "Found ${#FILE_ARRAY[@]} files to analyze"

# Fix formatting if requested
if [[ "$FIX_FORMAT" == true ]]; then
    print_section "Fixing Code Formatting"
    
    if [[ -x "$SCRIPT_DIR/format-code.sh" ]]; then
        if [[ "$GIT_ONLY" == true ]]; then
            "$SCRIPT_DIR/format-code.sh" --git
        else
            "$SCRIPT_DIR/format-code.sh"
        fi
    else
        print_warning "format-code.sh not found or not executable"
    fi
fi

# Configure cppcheck based on profile
print_section "Configuring Static Analysis"

CPPCHECK_ARGS=()

# Base configuration
CPPCHECK_ARGS+=(
    "--std=c++20"
    "--language=c++"
    "--inline-suppr"
    "--quiet"
    "--cppcheck-build-dir=$CPPCHECK_BUILD_DIR"
)

# In this repo/environment, system header resolution is noisy and drowns out
# actionable findings. compile_commands.json still provides project include paths.
CPPCHECK_ARGS+=("--suppress=missingIncludeSystem")
CPPCHECK_ARGS+=("--suppress=unmatchedSuppression")
CPPCHECK_ARGS+=("--suppress=checkersReport")

# Add suppressions file if it exists
if [[ -f "$SUPPRESSIONS_FILE" ]]; then
    CPPCHECK_ARGS+=("--suppressions-list=$SUPPRESSIONS_FILE")
fi

# Add include directories
CPPCHECK_ARGS+=("-I" ".")
CPPCHECK_ARGS+=("-I" "tests")
CPPCHECK_ARGS+=("-I" "include")
CPPCHECK_ARGS+=("-I" "src")
if [[ -n "$INCLUDE_DIRS" ]]; then
    IFS=',' read -ra INCLUDE_ARRAY <<< "$INCLUDE_DIRS"
    for include_dir in "${INCLUDE_ARRAY[@]}"; do
        CPPCHECK_ARGS+=("-I" "$include_dir")
    done
fi

# Add compile commands if available (but only if not using git-scoped modes)
USE_COMPILE_COMMANDS=false
if [[ -f "$COMPILE_COMMANDS" && "$GIT_ONLY" == false && -z "$DIFF_BASE" ]]; then
    CPPCHECK_ARGS+=("--project=$COMPILE_COMMANDS")
    USE_COMPILE_COMMANDS=true
fi

# Profile-specific configuration
case $PROFILE in
    strict)
        CPPCHECK_ARGS+=(
            "--enable=all"
            "--error-exitcode=1"
            "--check-config"
            "--check-library"
            "--inconclusive"
        )
        ;;
    normal)
        CPPCHECK_ARGS+=(
            "--enable=warning,style,performance,portability"
        )
        ;;
    quick)
        CPPCHECK_ARGS+=(
            # cppcheck --enable supports categories like warning/style/performance/etc.
            # "error" is a message severity, not a valid --enable category.
            "--enable=warning,style,performance"
        )
        ;;
    ci)
        CPPCHECK_ARGS+=(
            "--enable=warning,style,performance,portability"
            "--error-exitcode=1"
        )
        ;;
esac

# Output format configuration
case $OUTPUT_FORMAT in
    xml)
        CPPCHECK_ARGS+=("--xml" "--xml-version=2")
        ;;
    json)
        # cppcheck doesn't have native JSON, we'll convert XML
        CPPCHECK_ARGS+=("--xml" "--xml-version=2")
        ;;
    sarif)
        # We'll need to convert XML to SARIF
        CPPCHECK_ARGS+=("--xml" "--xml-version=2")
        ;;
    human)
        CPPCHECK_ARGS+=("--template={file}:{line}:{column}: {severity}: {message} [{id}]")
        ;;
esac

# Add parallel processing
if [[ "$PARALLEL" == true ]]; then
    # Use number of CPU cores
    if command -v nproc &> /dev/null; then
        JOBS=$(nproc)
    elif [[ -f /proc/cpuinfo ]]; then
        JOBS=$(grep -c ^processor /proc/cpuinfo)
    else
        JOBS=4  # Default fallback
    fi
    CPPCHECK_ARGS+=("-j" "$JOBS")
fi

print_success "cppcheck configured with ${#CPPCHECK_ARGS[@]} arguments"

filter_cppcheck_to_changed_lines() {
    local diff_base=$1
    local input_file=$2
    local output_file=$3

    python3 - "$diff_base" "$input_file" "$output_file" <<'PY'
import os
import re
import subprocess
import sys

diff_base, input_path, output_path = sys.argv[1:]

diff = subprocess.run(
    ["git", "diff", "--unified=0", f"{diff_base}...HEAD", "--"],
    check=False,
    capture_output=True,
    text=True,
).stdout.splitlines()

current = None
ranges = {}
path_re = re.compile(r"^\+\+\+ b/(.+)$")
hunk_re = re.compile(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@")

for line in diff:
    path_match = path_re.match(line)
    if path_match:
        current = os.path.normpath(path_match.group(1))
        continue

    hunk_match = hunk_re.match(line)
    if hunk_match and current:
        start = int(hunk_match.group(1))
        count = int(hunk_match.group(2) or "1")
        if count == 0:
            continue
        ranges.setdefault(current, []).append((start, start + count - 1))

finding_re = re.compile(r"^(?P<file>.+?):(?P<line>\d+):(?P<column>\d+):\s+(?P<severity>[^:]+):")

matches = []
with open(input_path, "r", encoding="utf-8", errors="replace") as fh:
    for raw in fh:
        line = raw.rstrip("\n")
        match = finding_re.match(line)
        if not match:
            continue
        path = os.path.normpath(match.group("file").lstrip("./"))
        line_no = int(match.group("line"))
        if any(lo <= line_no <= hi for lo, hi in ranges.get(path, [])):
            matches.append(raw)

with open(output_path, "w", encoding="utf-8") as out:
    out.writelines(matches)

print(len(matches))
PY
}

# Run static analysis
print_section "Running Static Analysis"

# Prepare output redirection
OUTPUT_FILE=""
if [[ -n "$OUTPUT_DIR" ]]; then
    case $OUTPUT_FORMAT in
        xml)
            OUTPUT_FILE="$OUTPUT_DIR/cppcheck-results.xml"
            ;;
        json)
            OUTPUT_FILE="$OUTPUT_DIR/cppcheck-results.json"
            ;;
        sarif)
            OUTPUT_FILE="$OUTPUT_DIR/cppcheck-results.sarif"
            ;;
        human)
            OUTPUT_FILE="$OUTPUT_DIR/cppcheck-results.txt"
            ;;
    esac
fi

# Create file list for cppcheck
TEMP_FILE_LIST=$(mktemp)
TEMP_CPPCHECK_OUTPUT=""
TEMP_FILTERED_OUTPUT=""
trap 'rm -f "$TEMP_FILE_LIST" "$TEMP_CPPCHECK_OUTPUT" "$TEMP_FILTERED_OUTPUT"' EXIT

if [[ -n "$DIFF_BASE" && "$OUTPUT_FORMAT" == "human" ]]; then
    TEMP_CPPCHECK_OUTPUT=$(mktemp)
    TEMP_FILTERED_OUTPUT=$(mktemp)
fi

printf '%s\n' "${FILE_ARRAY[@]}" > "$TEMP_FILE_LIST"

# Run cppcheck
print_progress "Running cppcheck analysis..."

# Choose between project mode and file list mode
if [[ "$USE_COMPILE_COMMANDS" == true ]]; then
    # Use project mode - don't specify individual files
    if [[ "$VERBOSE" == true ]]; then
        print_progress "Command: cppcheck ${CPPCHECK_ARGS[*]} (using compile_commands.json)"
    fi
    
    CPPCHECK_EXIT_CODE=0
    if [[ -n "$TEMP_CPPCHECK_OUTPUT" ]]; then
        if ! cppcheck "${CPPCHECK_ARGS[@]}" > "$TEMP_CPPCHECK_OUTPUT" 2>&1; then
            CPPCHECK_EXIT_CODE=$?
        fi
    elif [[ -n "$OUTPUT_FILE" ]]; then
        # Output to file
        if [[ "$OUTPUT_FORMAT" == "xml" || "$OUTPUT_FORMAT" == "json" || "$OUTPUT_FORMAT" == "sarif" ]]; then
            # XML output goes to stderr
            if ! cppcheck "${CPPCHECK_ARGS[@]}" 2> "$OUTPUT_FILE.tmp"; then
                CPPCHECK_EXIT_CODE=$?
            fi
            mv "$OUTPUT_FILE.tmp" "$OUTPUT_FILE"
        else
            # Human output
            if ! cppcheck "${CPPCHECK_ARGS[@]}" > "$OUTPUT_FILE" 2>&1; then
                CPPCHECK_EXIT_CODE=$?
            fi
        fi
    else
        # Output to stdout/stderr
        if ! cppcheck "${CPPCHECK_ARGS[@]}"; then
            CPPCHECK_EXIT_CODE=$?
        fi
    fi
else
    # Use file list mode
    if [[ "$VERBOSE" == true ]]; then
        print_progress "Command: cppcheck ${CPPCHECK_ARGS[*]} --file-list=$TEMP_FILE_LIST"
    fi
    
    CPPCHECK_EXIT_CODE=0
    if [[ -n "$TEMP_CPPCHECK_OUTPUT" ]]; then
        if ! cppcheck "${CPPCHECK_ARGS[@]}" --file-list="$TEMP_FILE_LIST" > "$TEMP_CPPCHECK_OUTPUT" 2>&1; then
            CPPCHECK_EXIT_CODE=$?
        fi
    elif [[ -n "$OUTPUT_FILE" ]]; then
        # Output to file
        if [[ "$OUTPUT_FORMAT" == "xml" || "$OUTPUT_FORMAT" == "json" || "$OUTPUT_FORMAT" == "sarif" ]]; then
            # XML output goes to stderr
            if ! cppcheck "${CPPCHECK_ARGS[@]}" --file-list="$TEMP_FILE_LIST" 2> "$OUTPUT_FILE.tmp"; then
                CPPCHECK_EXIT_CODE=$?
            fi
            mv "$OUTPUT_FILE.tmp" "$OUTPUT_FILE"
        else
            # Human output
            if ! cppcheck "${CPPCHECK_ARGS[@]}" --file-list="$TEMP_FILE_LIST" > "$OUTPUT_FILE" 2>&1; then
                CPPCHECK_EXIT_CODE=$?
            fi
        fi
    else
        # Output to stdout/stderr
        if ! cppcheck "${CPPCHECK_ARGS[@]}" --file-list="$TEMP_FILE_LIST"; then
            CPPCHECK_EXIT_CODE=$?
        fi
    fi
fi

if [[ -n "$TEMP_CPPCHECK_OUTPUT" ]]; then
    FILTERED_COUNT=$(filter_cppcheck_to_changed_lines "$DIFF_BASE" "$TEMP_CPPCHECK_OUTPUT" "$TEMP_FILTERED_OUTPUT")
    RAW_FINDING_COUNT=$(rg -c '^[^:]+:[0-9]+:[0-9]+:\s+[^:]+:' "$TEMP_CPPCHECK_OUTPUT" || true)

    if [[ -n "$OUTPUT_FILE" ]]; then
        cp "$TEMP_FILTERED_OUTPUT" "$OUTPUT_FILE"
    else
        cat "$TEMP_FILTERED_OUTPUT"
    fi

    if [[ "$FILTERED_COUNT" == "0" ]]; then
        if [[ "${RAW_FINDING_COUNT:-0}" -gt 0 ]]; then
            print_success "No cppcheck findings on changed lines since $DIFF_BASE"
            CPPCHECK_EXIT_CODE=0
        elif [[ $CPPCHECK_EXIT_CODE -ne 0 ]]; then
            cat "$TEMP_CPPCHECK_OUTPUT"
        fi
    fi
fi

# Post-process output if needed
if [[ -n "$OUTPUT_FILE" && -f "$OUTPUT_FILE" ]]; then
    case $OUTPUT_FORMAT in
        json)
            # Convert XML to JSON (basic conversion)
            print_progress "Converting XML output to JSON..."
            # This is a placeholder - you might want to use a proper XML to JSON converter
            print_warning "JSON conversion not yet implemented, XML file saved as $OUTPUT_FILE"
            ;;
        sarif)
            # Convert XML to SARIF
            print_progress "Converting XML output to SARIF..."
            # This is a placeholder - you might want to use a proper XML to SARIF converter
            print_warning "SARIF conversion not yet implemented, XML file saved as $OUTPUT_FILE"
            ;;
    esac
fi

# Handle baseline comparison
if [[ -n "$BASELINE_FILE" && -f "$BASELINE_FILE" ]]; then
    print_section "Baseline Comparison"
    print_progress "Comparing results against baseline: $BASELINE_FILE"
    # This would need to be implemented based on your needs
    print_warning "Baseline comparison not yet implemented"
fi

# Generate baseline if requested
if [[ "$GENERATE_BASELINE" == true ]]; then
    print_section "Generating Baseline"
    BASELINE_OUTPUT="$OUTPUT_DIR/quality-baseline.xml"
    if [[ -z "$OUTPUT_DIR" ]]; then
        BASELINE_OUTPUT="quality-baseline.xml"
    fi
    print_progress "Generating baseline file: $BASELINE_OUTPUT"
    # Copy current results as baseline
    if [[ -n "$OUTPUT_FILE" && -f "$OUTPUT_FILE" ]]; then
        cp "$OUTPUT_FILE" "$BASELINE_OUTPUT"
        print_success "Baseline generated: $BASELINE_OUTPUT"
    fi
fi

# Summary
print_section "Analysis Summary"

if [[ -n "$OUTPUT_FILE" && -f "$OUTPUT_FILE" ]]; then
    # Count issues in output file
    case $OUTPUT_FORMAT in
        xml)
            ERROR_COUNT=$(grep -c '<error ' "$OUTPUT_FILE" 2>/dev/null || echo "0")
            ;;
        human)
            ERROR_COUNT=$(wc -l < "$OUTPUT_FILE" 2>/dev/null || echo "0")
            ;;
        *)
            ERROR_COUNT="unknown"
            ;;
    esac
    
    print_progress "Results saved to: $OUTPUT_FILE"
    if [[ "$ERROR_COUNT" != "unknown" ]]; then
        print_progress "Issues found: $ERROR_COUNT"
    fi
fi

print_progress "Files analyzed: ${#FILE_ARRAY[@]}"
print_progress "Profile: $PROFILE"
print_progress "Format: $OUTPUT_FORMAT"

# Final exit code and summary
if [[ $CPPCHECK_EXIT_CODE -eq 0 ]]; then
    print_success "Code quality analysis completed successfully"
    print_success "No critical issues found"
else
    print_error "Code quality analysis found issues (exit code: $CPPCHECK_EXIT_CODE)"
    if [[ "$PROFILE" == "strict" || "$PROFILE" == "ci" ]]; then
        print_error "Critical issues detected in $PROFILE mode"
    else
        print_warning "Issues found, but not treating as errors in $PROFILE mode"
        CPPCHECK_EXIT_CODE=0  # Don't fail in normal/quick mode
    fi
fi

exit $CPPCHECK_EXIT_CODE
