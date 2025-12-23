#!/usr/bin/env bash

# Formats all C++ source files using clang-format

set -euo pipefail

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
MODE="format"
DRY_RUN=false
GIT_ONLY=false
PARALLEL=true
VERBOSE=false
READ_FROM_STDIN=false

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# clang-format invocation settings
CLANG_FORMAT_ARGS=("--style=file")
DISABLE_INCLUDE_SORT=false
case "${OSTYPE:-}" in
    msys*|cygwin*|win32*|mingw*)
        DISABLE_INCLUDE_SORT=true
        CLANG_FORMAT_ARGS+=("--sort-includes=0")
        ;;
esac
CLANG_FORMAT_ARGS_STRING="${CLANG_FORMAT_ARGS[*]}"
export CLANG_FORMAT_ARGS_STRING

run_clang_format() {
    if [[ -n "${CLANG_FORMAT_ARGS_STRING:-}" ]]; then
        # shellcheck disable=SC2086
        clang-format ${CLANG_FORMAT_ARGS_STRING} "$@"
    else
        clang-format "$@"
    fi
}

# Function to print colored output
print_color() {
    local color=$1
    shift
    echo -e "${color}$*${NC}"
}

# Usage information
usage() {
    cat << EOF
Usage: $(basename "$0") [OPTIONS]

Options:
    -c, --check      Check if files need formatting (exit 1 if changes needed)
    -d, --dry-run    Show which files would be changed without modifying them
    -g, --git        Only format files changed in git (staged and unstaged)
    -s, --serial     Run formatting serially instead of in parallel
        --stdin-files Read newline-separated file list from stdin
    -v, --verbose    Show detailed output
    -h, --help       Show this help message

Examples:
    # Format all files
    $(basename "$0")
    
    # Check if formatting is needed (for CI)
    $(basename "$0") --check
    
    # Format only git-changed files
    $(basename "$0") --git
    
    # Preview changes without applying
    $(basename "$0") --dry-run

EOF
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -c|--check)
            MODE="check"
            shift
            ;;
        -d|--dry-run)
            DRY_RUN=true
            shift
            ;;
        -g|--git)
            GIT_ONLY=true
            shift
            ;;
        -s|--serial)
            PARALLEL=false
            shift
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        --stdin-files)
            READ_FROM_STDIN=true
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            print_color "$RED" "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Check for clang-format
if ! command -v clang-format &> /dev/null; then
    print_color "$RED" "Error: clang-format not found in PATH"
    echo "Please install clang-format:"
    echo "  macOS:  brew install clang-format"
    echo "  Ubuntu: apt-get install clang-format"
    echo "  Arch:   pacman -S clang"
    exit 1
fi

# Check for .clang-format file
if [[ ! -f "${PROJECT_ROOT}/.clang-format" ]]; then
    print_color "$RED" "Error: .clang-format file not found in project root"
    exit 1
fi

# Change to project root
cd "$PROJECT_ROOT"

if [[ "$DISABLE_INCLUDE_SORT" == true ]]; then
    print_color "$YELLOW" "Detected Windows environment; preserving include order (disabling clang-format include sorting)"
fi

# Get list of files to format
FILE_ARRAY=()
if [[ "$READ_FROM_STDIN" == true ]]; then
    print_color "$BLUE" "Reading file list from stdin..."
    while IFS= read -r line; do
        [[ -n "$line" ]] && FILE_ARRAY+=("$line")
    done
elif [[ "$GIT_ONLY" == true ]]; then
    # Get list of modified files (staged and unstaged)
    print_color "$BLUE" "Getting list of git-modified files..."
    
    # Get both staged and unstaged files
    FILES=$(git diff --name-only --diff-filter=ACMR HEAD 2>/dev/null || true)
    FILES+=$'\n'
    FILES+=$(git diff --cached --name-only --diff-filter=ACMR 2>/dev/null || true)
    
    # Filter for C++ files and remove duplicates
    FILES=$(echo "$FILES" | grep -E '\.(cpp|h|hpp|cc|cxx)$' | sort -u || true)
    
    if [[ -z "$FILES" ]]; then
        print_color "$GREEN" "No modified C++ files found"
        exit 0
    fi
    
    # Convert to array (portable method)
    FILE_ARRAY=()
    while IFS= read -r line; do
        [[ -n "$line" ]] && FILE_ARRAY+=("$line")
    done <<< "$FILES"
else
    # Find all C++ source files, excluding build directories and third-party code
    print_color "$BLUE" "Finding all C++ source files..."
    
    # Use find with exclusions (portable method)
    while IFS= read -r line; do
        FILE_ARRAY+=("$line")
    done < <(find . \
        -type f \
        \( -name "*.cpp" -o -name "*.h" -o -name "*.hpp" -o -name "*.cc" -o -name "*.cxx" \) \
        -not -path "./build/*" \
        -not -path "./.cache/*" \
        -not -path "./cmake-build-*/*" \
        -not -path "./third_party/*" \
        -not -path "./external/*" \
        -not -path "./vendor/*" \
        -not -path "./.git/*" \
        -not -path "*/generated/*" \
        -not -path "*.pb.h" \
        -not -path "*.pb.cc" \
        | sort)
fi

# Check if we found any files
if [[ ${#FILE_ARRAY[@]} -eq 0 ]]; then
    print_color "$YELLOW" "No C++ files found to format"
    exit 0
fi

print_color "$BLUE" "Found ${#FILE_ARRAY[@]} files to process"

# Function to format a single file
format_file() {
    local file=$1
    local mode=$2
    local dry_run=$3
    local verbose=$4
    
    if [[ "$mode" == "check" ]]; then
        # Check if file needs formatting
        if ! run_clang_format --dry-run --Werror "$file" &>/dev/null; then
            if [[ "$verbose" == true ]]; then
                print_color "$YELLOW" "Needs formatting: $file"
            fi
            return 1
        else
            if [[ "$verbose" == true ]]; then
                print_color "$GREEN" "Formatted correctly: $file"
            fi
            return 0
        fi
    else
        # Format mode
        if [[ "$dry_run" == true ]]; then
            # Show diff without applying
            local diff
            diff=$(run_clang_format "$file" | diff -u "$file" - || true)
            if [[ -n "$diff" ]]; then
                print_color "$YELLOW" "Would change: $file"
                if [[ "$verbose" == true ]]; then
                    echo "$diff"
                fi
                return 1
            else
                if [[ "$verbose" == true ]]; then
                    print_color "$GREEN" "No changes needed: $file"
                fi
                return 0
            fi
        else
            # Actually format the file
            run_clang_format -i "$file"
            if [[ "$verbose" == true ]]; then
                print_color "$GREEN" "Formatted: $file"
            fi
            return 0
        fi
    fi
}

# Export function for parallel execution
export -f format_file print_color run_clang_format
export RED GREEN YELLOW BLUE NC

# Process files
FAILED_COUNT=0
CHANGED_COUNT=0

if [[ "$PARALLEL" == true ]] && command -v parallel &> /dev/null; then
    # Use GNU parallel if available
    print_color "$BLUE" "Processing files in parallel..."
    
    # Run with parallel and capture exit codes
    if ! printf '%s\n' "${FILE_ARRAY[@]}" | \
        parallel --bar --halt soon,fail=1 \
        "format_file {} '$MODE' '$DRY_RUN' '$VERBOSE' || exit 1" \
        2>/dev/null; then
        FAILED_COUNT=1
    fi
elif [[ "$PARALLEL" == true ]] && [[ $(uname) == "Darwin" ]] && command -v xargs &> /dev/null; then
    # Use xargs on macOS
    print_color "$BLUE" "Processing files with xargs..."
    
    # Create a wrapper function for xargs
    process_with_xargs() {
        for file in "$@"; do
            if ! format_file "$file" "$MODE" "$DRY_RUN" "$VERBOSE"; then
                if [[ "$MODE" == "check" ]]; then
                    FAILED_COUNT=$((FAILED_COUNT + 1))
                else
                    CHANGED_COUNT=$((CHANGED_COUNT + 1))
                fi
            fi
        done
    }
    export -f process_with_xargs
    
    printf '%s\n' "${FILE_ARRAY[@]}" | xargs -n 10 -P 8 bash -c 'process_with_xargs "$@"' _
else
    # Serial processing
    print_color "$BLUE" "Processing files serially..."
    
    for file in "${FILE_ARRAY[@]}"; do
        if [[ "$VERBOSE" == false ]]; then
            # Show progress
            echo -n "."
        fi
        
        if ! format_file "$file" "$MODE" "$DRY_RUN" "$VERBOSE"; then
            if [[ "$MODE" == "check" ]]; then
                FAILED_COUNT=$((FAILED_COUNT + 1))
            else
                CHANGED_COUNT=$((CHANGED_COUNT + 1))
            fi
        fi
    done
    
    if [[ "$VERBOSE" == false ]]; then
        echo # New line after progress dots
    fi
fi

# Print summary
echo
if [[ "$MODE" == "check" ]]; then
    if [[ $FAILED_COUNT -gt 0 ]]; then
        print_color "$RED" "✗ $FAILED_COUNT files need formatting"
        echo "Run '$(basename "$0")' to format them"
        exit 1
    else
        print_color "$GREEN" "✓ All files are properly formatted"
        exit 0
    fi
elif [[ "$DRY_RUN" == true ]]; then
    if [[ $CHANGED_COUNT -gt 0 ]]; then
        print_color "$YELLOW" "Would format $CHANGED_COUNT files"
        echo "Run '$(basename "$0")' without --dry-run to apply changes"
    else
        print_color "$GREEN" "✓ All files are already properly formatted"
    fi
else
    print_color "$GREEN" "✓ Formatting complete"
    if [[ $CHANGED_COUNT -gt 0 ]]; then
        print_color "$YELLOW" "Changed $CHANGED_COUNT files"
    else
        print_color "$GREEN" "All files were already properly formatted"
    fi
fi

exit 0
