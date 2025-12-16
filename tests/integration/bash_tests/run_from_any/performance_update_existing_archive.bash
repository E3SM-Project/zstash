# Manually edit parameters here:

# May want to change:
DIR_THAT_WAS_ARCHIVED=/lcrc/group/e3sm/ac.forsyth2/zstash_performance/build/
EXISTING_ARCHIVE=globus://9cd89cfd-6d04-11e5-ba46-22000b92c6ec//home/f/forsyth/zstash_performance_20251216_try2
WORK_DIR=/lcrc/group/e3sm/ac.forsyth2/zstash_performance
UPDATE_FILES=1000

# Unlikely to need to change:
LOG_DIR=${WORK_DIR}/logs
CACHE_DIR=${WORK_DIR}/cache

###############################################################################
set -e
cd ${WORK_DIR}

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Functions to print colored messages
print_step() {
    echo -e "${CYAN}[STEP]${NC} $1"
}

print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Step 1: Add new files to simulate update scenario
print_step "Step 1: Adding new files for update test..."
cd "$DIR_THAT_WAS_ARCHIVED"
# Create a new directory for update files:
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

# Step 2: Profile zstash update
print_step "Step 2: Profiling zstash update..."
UPDATE_LOG="$LOG_DIR/update.log"
print_info "Running zstash update with profiling..."
echo ""
UPDATE_START=$(date +%s)
zstash update --hpss="$EXISTING_ARCHIVE" --cache="$CACHE_DIR" -v 2>&1 | tee "$UPDATE_LOG"
if [ $? -eq 0 ]; then
    UPDATE_END=$(date +%s)
    UPDATE_ELAPSED=$((UPDATE_END - UPDATE_START))
    print_success "Update profiling completed in ${UPDATE_ELAPSED} seconds"
else
    print_error "Update profiling failed"
    exit 1
fi
echo ""

# Step 3: Analyze performance metrics from update log
print_step "Step 3: Analyzing performance metrics from update log..."
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
print_info "=========================================="
print_success "Profiling Complete!"
print_info "=========================================="
