#!/bin/bash

################################################################################
# zstash_profile.sh - Profile zstash update performance with synthetic data
#
# This script creates a test directory with synthetic files, archives it with
# zstash create, then profiles zstash update to identify bottlenecks.
#
# Usage:
#   ./zstash_profile.sh [options]
#
# Options:
#   --num-files <N>      Number of files to create (default: 10000)
#   --num-dirs <N>       Number of directories to create (default: 100)
#   --update-files <N>   Number of new files to add for update (default: 1000)
#   --hpss <path>        HPSS path (default: none for local-only)
#   --cache <path>       Cache directory name (default: zstash)
#   --keep-data          Don't delete test data after profiling
#   --skip-create        Skip create step (use existing test data)
#
# Examples:
#   # Quick test with small dataset
#   ./zstash_profile.sh --num-files 1000 --num-dirs 50
#
#   # Larger test to simulate real workload
#   ./zstash_profile.sh --num-files 50000 --num-dirs 500
#
#   # Test with HPSS
#   ./zstash_profile.sh --hpss=test/profiling_archive --num-files 5000
#
################################################################################

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Default parameters
NUM_FILES=10000
NUM_DIRS=100
UPDATE_FILES=1000
HPSS_PATH="none"
CACHE_NAME="zstash"
KEEP_DATA=false
SKIP_CREATE=false
WORK_DIR=""

# Function to print colored messages
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_step() {
    echo -e "${CYAN}[STEP]${NC} $1"
}

# Function to display usage
usage() {
    cat << EOF
Usage: $0 [options]

Create synthetic test data and profile zstash update performance.

Options:
  --num-files <N>      Number of files to create (default: 10000)
  --num-dirs <N>       Number of directories to create (default: 100)
  --update-files <N>   Number of new files to add for update (default: 1000)
  --hpss <path>        HPSS archive path (default: none for local-only)
  --cache <name>       Cache directory name (default: zstash)
  --keep-data          Don't delete test data after profiling
  --skip-create        Skip create step (use existing test_zstash_profile)
  --help               Display this help message

Examples:
  # Small test (fast)
  $0 --num-files 1000 --num-dirs 50 --update-files 100

  # Medium test (realistic for identifying bottlenecks)
  $0 --num-files 10000 --num-dirs 100 --update-files 1000

  # Large test (simulates real simulation data)
  $0 --num-files 50000 --num-dirs 500 --update-files 5000

  # Test with HPSS
  $0 --hpss=test/profiling_archive --num-files 5000

  # Keep test data for further analysis
  $0 --num-files 5000 --keep-data

EOF
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --work-dir)
            WORK_DIR="$2"
            shift 2
            ;;
        --num-files)
            NUM_FILES="$2"
            shift 2
            ;;
        --num-dirs)
            NUM_DIRS="$2"
            shift 2
            ;;
        --update-files)
            UPDATE_FILES="$2"
            shift 2
            ;;
        --hpss)
            HPSS_PATH="$2"
            shift 2
            ;;
        --cache)
            CACHE_NAME="$2"
            shift 2
            ;;
        --keep-data)
            KEEP_DATA=true
            shift
            ;;
        --skip-create)
            SKIP_CREATE=true
            shift
            ;;
        --help)
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

# Check if zstash is available
if ! command -v zstash &> /dev/null; then
    print_error "zstash command not found. Please ensure zstash is installed and in your PATH."
    exit 1
fi

# Get zstash version
ZSTASH_VERSION=$(zstash version 2>/dev/null || echo "unknown")

# Set working directory (default to current directory)
if [ -z "$WORK_DIR" ]; then
    WORK_DIR="$(pwd)"
else
    # Create working directory if it doesn't exist
    mkdir -p "$WORK_DIR"
    # Convert to absolute path
    WORK_DIR="$(cd "$WORK_DIR" && pwd)"
fi

# Setup test directory
TEST_DIR="$WORK_DIR/test_zstash_profile"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_DIR="$WORK_DIR/zstash_profile_logs_${TIMESTAMP}"

print_info "=========================================="
print_info "Zstash Update Performance Profiling"
print_info "=========================================="
print_info "Zstash version: $ZSTASH_VERSION"
print_info "Working directory: $WORK_DIR"
print_info "Test directory: $TEST_DIR"
print_info "Number of files: $NUM_FILES"
print_info "Number of directories: $NUM_DIRS"
print_info "Update files: $UPDATE_FILES"
print_info "HPSS path: $HPSS_PATH"
print_info "Cache name: $CACHE_NAME"
print_info "Log directory: $LOG_DIR"
print_info "=========================================="
echo ""

# Create log directory
mkdir -p "$LOG_DIR"

if [ "$SKIP_CREATE" = false ]; then
    # Clean up any existing test directory
    if [ -d "$TEST_DIR" ]; then
        print_warning "Removing existing test directory: $TEST_DIR"
        rm -rf "$TEST_DIR"
    fi

    # Step 1: Create synthetic test data
    print_step "Step 1/4: Creating synthetic test data..."
    mkdir -p "$TEST_DIR"

    # Create directory structure
    print_info "Creating $NUM_DIRS directories..."
    for i in $(seq 1 $NUM_DIRS); do
        DIR_NAME=$(printf "dir_%04d" $i)
        mkdir -p "$TEST_DIR/$DIR_NAME"
    done

    # Create files distributed across directories
    print_info "Creating $NUM_FILES files..."
    FILES_PER_DIR=$((NUM_FILES / NUM_DIRS))
    REMAINING_FILES=$((NUM_FILES % NUM_DIRS))

    FILE_COUNTER=0
    for i in $(seq 1 $NUM_DIRS); do
        DIR_NAME=$(printf "dir_%04d" $i)

        # Calculate files for this directory
        if [ $i -le $REMAINING_FILES ]; then
            FILES_THIS_DIR=$((FILES_PER_DIR + 1))
        else
            FILES_THIS_DIR=$FILES_PER_DIR
        fi

        for j in $(seq 1 $FILES_THIS_DIR); do
            FILE_COUNTER=$((FILE_COUNTER + 1))
            FILE_NAME=$(printf "file_%08d.txt" $FILE_COUNTER)
            # Create small files with some content (1KB each)
            echo "Test file $FILE_COUNTER - $(date)" > "$TEST_DIR/$DIR_NAME/$FILE_NAME"

            # Progress indicator
            if [ $((FILE_COUNTER % 1000)) -eq 0 ]; then
                echo -ne "  Created $FILE_COUNTER / $NUM_FILES files\r"
            fi
        done
    done
    echo -ne "\n"

    print_success "Created $NUM_FILES files in $NUM_DIRS directories"

    # Calculate total size
    TOTAL_SIZE=$(du -sh "$TEST_DIR" | awk '{print $1}')
    print_info "Total test data size: $TOTAL_SIZE"
    echo ""

    # Step 2: Create initial archive
    print_step "Step 2/4: Creating initial zstash archive..."
    CREATE_LOG="$LOG_DIR/create.log"

    cd "$TEST_DIR"
    CREATE_START=$(date +%s)

    if zstash create --hpss="$HPSS_PATH" --cache="$CACHE_NAME" -v . 2>&1 | tee "$CREATE_LOG"; then
        CREATE_END=$(date +%s)
        CREATE_ELAPSED=$((CREATE_END - CREATE_START))
        print_success "Archive created in ${CREATE_ELAPSED} seconds"
    else
        print_error "Failed to create archive"
        exit 1
    fi

    cd ..
    echo ""
else
    print_step "Skipping create step, using existing $TEST_DIR"

    if [ ! -d "$TEST_DIR" ]; then
        print_error "Test directory $TEST_DIR does not exist. Cannot skip create step."
        exit 1
    fi

    cd "$TEST_DIR"
    if [ ! -d "$CACHE_NAME" ]; then
        print_error "Cache directory $CACHE_NAME does not exist in $TEST_DIR"
        exit 1
    fi
    cd ..
    echo ""
fi

# Step 3: Add new files to simulate update scenario
print_step "Step 3/4: Adding new files for update test..."

cd "$TEST_DIR"

# Create a new directory for update files
UPDATE_DIR="update_files"
mkdir -p "$UPDATE_DIR"

print_info "Creating $UPDATE_FILES new files..."
for i in $(seq 1 $UPDATE_FILES); do
    FILE_NAME=$(printf "new_file_%08d.txt" $i)
    echo "New test file $i - $(date)" > "$UPDATE_DIR/$FILE_NAME"

    if [ $((i % 100)) -eq 0 ]; then
        echo -ne "  Created $i / $UPDATE_FILES new files\r"
    fi
done
echo -ne "\n"

print_success "Added $UPDATE_FILES new files"
echo ""

# Step 4: Profile zstash update
print_step "Step 4/4: Profiling zstash update..."
UPDATE_LOG="$LOG_DIR/update.log"

print_info "Running zstash update with profiling..."
print_info "Command: zstash update --hpss=$HPSS_PATH --cache=$CACHE_NAME --dry-run -v"
echo ""

UPDATE_START=$(date +%s)

if zstash update --hpss="$HPSS_PATH" --cache="$CACHE_NAME" --dry-run -v 2>&1 | tee "$UPDATE_LOG"; then
    UPDATE_END=$(date +%s)
    UPDATE_ELAPSED=$((UPDATE_END - UPDATE_START))
    print_success "Update profiling completed in ${UPDATE_ELAPSED} seconds"
else
    print_error "Update profiling failed"
    cd ..
    exit 1
fi

cd ..
echo ""

# Extract and display performance metrics
print_info "=========================================="
print_info "Performance Analysis"
print_info "=========================================="

# File gathering metrics
if grep -q "PERFORMANCE (get_files_to_archive)" "$UPDATE_LOG"; then
    echo ""
    print_info "File Gathering Breakdown:"
    grep "PERFORMANCE (walk):" "$UPDATE_LOG" | tail -5
    grep "PERFORMANCE (sort):" "$UPDATE_LOG"
    grep "PERFORMANCE (normalize):" "$UPDATE_LOG"
    grep "PERFORMANCE (get_files_to_archive): TOTAL TIME:" "$UPDATE_LOG"
fi

# Database comparison metrics
if grep -q "PERFORMANCE: Database comparison completed" "$UPDATE_LOG"; then
    echo ""
    print_info "Database Comparison Breakdown:"
    grep "PERFORMANCE: Total comparison time:" "$UPDATE_LOG"
    grep "PERFORMANCE: Files checked:" "$UPDATE_LOG"
    grep "PERFORMANCE: New files to archive:" "$UPDATE_LOG"
    grep "PERFORMANCE: Average rate:" "$UPDATE_LOG"
    grep "stat operations:" "$UPDATE_LOG"
    grep "database queries:" "$UPDATE_LOG"
    grep "comparison logic:" "$UPDATE_LOG"
fi

# Overall summary
if grep -q "PERFORMANCE: Update complete - Summary:" "$UPDATE_LOG"; then
    echo ""
    print_info "Overall Time Breakdown:"
    grep "PERFORMANCE: Update complete - Summary:" -A 6 "$UPDATE_LOG" | tail -6
fi

echo ""
print_info "=========================================="
print_info "Analysis Commands"
print_info "=========================================="
print_success "View full logs:"
echo "  cat $LOG_DIR/create.log       # Initial archive creation"
echo "  cat $LOG_DIR/update.log       # Update profiling"
echo ""
print_success "Extract specific metrics:"
echo "  grep 'PERFORMANCE' $UPDATE_LOG | less"
echo "  grep 'PERFORMANCE (walk)' $UPDATE_LOG"
echo "  grep 'database queries' $UPDATE_LOG"
echo ""
print_success "Compare times:"
echo "  grep 'TOTAL TIME' $UPDATE_LOG"
echo ""

# Cleanup
if [ "$KEEP_DATA" = false ]; then
    print_info "=========================================="
    print_warning "Cleaning up test data..."
    rm -rf "$TEST_DIR"
    print_success "Test directory removed"
    print_info "Logs preserved in: $LOG_DIR"
else
    print_info "=========================================="
    print_success "Test data preserved in: $TEST_DIR"
    print_info "Logs saved in: $LOG_DIR"
    echo ""
    print_info "To rerun profiling with this data:"
    echo "  $0 --work-dir \"$WORK_DIR\" --skip-create"
fi

echo ""
print_info "=========================================="
print_success "Profiling Complete!"
print_info "=========================================="
