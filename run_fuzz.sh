#!/bin/bash
# sqllite-rust Fuzz Testing Runner
# Runs cargo-fuzz with various configurations

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FUZZ_DIR="$SCRIPT_DIR/fuzz"

echo "========================================="
echo "  sqllite-rust Fuzz Testing Suite"
echo "========================================="
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default settings
TIMEOUT=60  # seconds per target
JOBS=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)

# Parse arguments
COMMAND=${1:-quick}
TARGET=${2:-all}

function check_prerequisites() {
    echo "Checking prerequisites..."
    
    if ! command -v cargo-fuzz &> /dev/null; then
        echo -e "${YELLOW}Installing cargo-fuzz...${NC}"
        cargo install cargo-fuzz
    fi
    
    # Check for nightly toolchain
    if ! rustup toolchain list | grep -q nightly; then
        echo -e "${YELLOW}Installing nightly toolchain...${NC}"
        rustup install nightly
    fi
    
    echo -e "${GREEN}✓ Prerequisites OK${NC}"
    echo ""
}

function list_targets() {
    echo "Available fuzz targets:"
    for target in "$FUZZ_DIR"/fuzz_targets/*.rs; do
        basename "$target" .rs
    done
    echo ""
}

function run_single_target() {
    local target=$1
    local duration=$2
    
    echo "----------------------------------------"
    echo "Running: $target (${duration}s)"
    echo "----------------------------------------"
    
    if [ ! -f "$FUZZ_DIR/fuzz_targets/$target.rs" ]; then
        echo -e "${RED}✗ Target not found: $target${NC}"
        return 1
    fi
    
    cd "$FUZZ_DIR"
    
    # Check if corpus exists
    if [ -d "$FUZZ_DIR/corpus/$target" ]; then
        echo "Using corpus: $FUZZ_DIR/corpus/$target ($(ls "$FUZZ_DIR/corpus/$target" | wc -l) files)"
    fi
    
    # Run fuzzer
    cargo +nightly fuzz run "$target" -- -max_total_time=$duration -jobs=$JOBS 2>&1 | tee "/tmp/fuzz_${target}.log" || true
    
    # Check for crashes
    if [ -d "$FUZZ_DIR/artifacts/$target" ] && [ "$(ls -A "$FUZZ_DIR/artifacts/$target" 2>/dev/null)" ]; then
        echo -e "${RED}✗ CRASHES FOUND in $target!${NC}"
        ls -la "$FUZZ_DIR/artifacts/$target/"
        return 1
    else
        echo -e "${GREEN}✓ No crashes in $target${NC}"
    fi
    
    echo ""
}

function run_quick_fuzz() {
    echo "Running QUICK fuzz test (60s per target)..."
    echo ""
    
    local targets=(
        "sql_parser_fuzz"
        "storage_fuzz"
        "mvcc_fuzz"
        "transaction_fuzz"
    )
    
    local failed=0
    for target in "${targets[@]}"; do
        if ! run_single_target "$target" 60; then
            ((failed++)) || true
        fi
    done
    
    echo "----------------------------------------"
    if [ $failed -eq 0 ]; then
        echo -e "${GREEN}All quick fuzz tests passed!${NC}"
    else
        echo -e "${RED}$failed target(s) failed!${NC}"
        exit 1
    fi
}

function run_full_fuzz() {
    echo "Running FULL fuzz test (1 hour per target)..."
    echo ""
    
    local targets=(
        "sql_parser_fuzz"
        "storage_fuzz"
        "mvcc_fuzz"
        "transaction_fuzz"
        "btree_fuzz"
        "record_fuzz"
        "tokenizer_fuzz"
        "expression_fuzz"
    )
    
    local failed=0
    for target in "${targets[@]}"; do
        if ! run_single_target "$target" 3600; then
            ((failed++)) || true
        fi
    done
    
    echo "----------------------------------------"
    if [ $failed -eq 0 ]; then
        echo -e "${GREEN}All full fuzz tests passed!${NC}"
    else
        echo -e "${RED}$failed target(s) failed!${NC}"
        exit 1
    fi
}

function run_continuous_fuzz() {
    echo "Running CONTINUOUS fuzz test (24 hours)..."
    echo ""
    
    local targets=(
        "sql_parser_fuzz"
        "storage_fuzz"
        "mvcc_fuzz"
        "transaction_fuzz"
    )
    
    local duration=$((24 * 3600 / ${#targets[@]}))
    
    for target in "${targets[@]}"; do
        run_single_target "$target" "$duration"
    done
}

function show_stats() {
    echo "Fuzz Testing Statistics"
    echo "======================"
    echo ""
    
    # Corpus stats
    echo "Corpus Files:"
    for corpus in "$FUZZ_DIR"/corpus/*; do
        if [ -d "$corpus" ]; then
            local name=$(basename "$corpus")
            local count=$(ls "$corpus" | wc -l)
            echo "  $name: $count files"
        fi
    done
    echo ""
    
    # Crash stats
    echo "Crashes Found:"
    local total_crashes=0
    for artifacts in "$FUZZ_DIR"/artifacts/*; do
        if [ -d "$artifacts" ]; then
            local name=$(basename "$artifacts")
            local crashes=$(ls "$artifacts" 2>/dev/null | wc -l)
            if [ $crashes -gt 0 ]; then
                echo "  $name: $crashes crashes"
                ((total_crashes += crashes)) || true
            fi
        fi
    done
    
    if [ $total_crashes -eq 0 ]; then
        echo "  None (great!)"
    else
        echo "  Total: $total_crashes crashes"
    fi
    echo ""
    
    # Coverage stats
    if [ -d "$FUZZ_DIR/coverage" ]; then
        echo "Coverage Data Available"
    fi
}

function run_corpus_minimize() {
    echo "Minimizing corpus..."
    echo ""
    
    for corpus in "$FUZZ_DIR"/corpus/*; do
        if [ -d "$corpus" ]; then
            local name=$(basename "$corpus")
            echo "Minimizing $name..."
            
            cd "$FUZZ_DIR"
            cargo +nightly fuzz cmin "$name" 2>&1 || true
        fi
    done
    
    echo -e "${GREEN}Corpus minimization complete${NC}"
}

function build_targets() {
    echo "Building all fuzz targets..."
    echo ""
    
    cd "$FUZZ_DIR"
    cargo +nightly build --release 2>&1
    
    echo -e "${GREEN}✓ Build complete${NC}"
    echo ""
}

# Main command dispatcher
case $COMMAND in
    quick)
        check_prerequisites
        build_targets
        run_quick_fuzz
        ;;
    full)
        check_prerequisites
        build_targets
        run_full_fuzz
        ;;
    continuous)
        check_prerequisites
        build_targets
        run_continuous_fuzz
        ;;
    single)
        if [ "$TARGET" == "all" ]; then
            echo "Usage: $0 single <target_name>"
            list_targets
            exit 1
        fi
        check_prerequisites
        run_single_target "$TARGET" 300
        ;;
    stats)
        show_stats
        ;;
    minimize)
        run_corpus_minimize
        ;;
    build)
        check_prerequisites
        build_targets
        ;;
    list)
        list_targets
        ;;
    help|--help|-h)
        echo "Usage: $0 [COMMAND] [TARGET]"
        echo ""
        echo "Commands:"
        echo "  quick      Run quick fuzz test (60s per target)"
        echo "  full       Run full fuzz test (1 hour per target)"
        echo "  continuous Run continuous fuzz test (24 hours)"
        echo "  single     Run single target (requires TARGET)"
        echo "  stats      Show fuzzing statistics"
        echo "  minimize   Minimize corpus"
        echo "  build      Build all targets"
        echo "  list       List available targets"
        echo "  help       Show this help"
        echo ""
        echo "Examples:"
        echo "  $0 quick                    # Quick test all targets"
        echo "  $0 single sql_parser_fuzz   # Test SQL parser only"
        echo "  $0 full                     # Full 8-hour test"
        echo ""
        ;;
    *)
        echo "Unknown command: $COMMAND"
        echo "Use '$0 help' for usage information"
        exit 1
        ;;
esac
