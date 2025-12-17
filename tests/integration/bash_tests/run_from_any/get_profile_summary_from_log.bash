#!/bin/bash

# Manually edit parameters here:

# May want to change:
WORK_DIR=/lcrc/group/e3sm/ac.forsyth2/zstash_performance

# Unlikely to need to change:
LOG_DIR=${WORK_DIR}/logs_update_optimization
UPDATE_LOG="$LOG_DIR/update.log"

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

print_step "Analyzing performance metrics from update log..."
# Extract and display performance metrics
print_info "=========================================="
print_info "Performance Analysis (OPTIMIZED VERSION)"
print_info "=========================================="

# File gathering metrics (OPTIMIZED - now with stats)
if grep -q "PERFORMANCE (get_files_to_archive_with_stats)" "$UPDATE_LOG"; then
    echo ""
    print_info "File Gathering Breakdown (with stats collection):"
    grep "PERFORMANCE (scandir):" "$UPDATE_LOG" | tail -5
    grep "PERFORMANCE (sort):" "$UPDATE_LOG"
    grep "PERFORMANCE (get_files_to_archive_with_stats): TOTAL TIME:" "$UPDATE_LOG"
fi

# Database comparison metrics (OPTIMIZED - no stats)
if grep -q "PERFORMANCE: Database comparison completed" "$UPDATE_LOG"; then
    echo ""
    print_info "Database Comparison Breakdown (OPTIMIZED):"
    grep "PERFORMANCE: Total comparison time:" "$UPDATE_LOG"
    grep "PERFORMANCE: Files checked:" "$UPDATE_LOG"
    grep "PERFORMANCE: New files to archive:" "$UPDATE_LOG"
    grep "PERFORMANCE: Average rate:" "$UPDATE_LOG"
    grep "database load:" "$UPDATE_LOG"
    grep "comparison (in-memory):" "$UPDATE_LOG"
fi

# Optimization impact
if grep -q "PERFORMANCE: Optimization impact:" "$UPDATE_LOG"; then
    echo ""
    print_info "Optimization Impact:"
    grep "stat operations eliminated:" "$UPDATE_LOG"
    grep "All stats performed during initial filesystem walk" "$UPDATE_LOG"
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
echo "  grep 'PERFORMANCE (scandir)' $UPDATE_LOG"
echo "  grep 'database load' $UPDATE_LOG"
echo "  grep 'Optimization impact' $UPDATE_LOG"
echo ""
print_success "Compare times:"
echo "  grep 'TOTAL TIME' $UPDATE_LOG"
echo ""
print_success "View optimization benefits:"
echo "  grep 'stat operations eliminated' $UPDATE_LOG"
echo ""
print_info "=========================================="
print_success "Profiling Complete!"
print_info "=========================================="
