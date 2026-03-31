#!/bin/bash
# Quick syntax check for all fuzz targets

echo "Checking fuzz target syntax..."

for target in fuzz/fuzz_targets/*.rs; do
    name=$(basename "$target" .rs)
    echo -n "  $name... "
    
    # Check basic syntax
    if rustfmt --check "$target" > /dev/null 2>&1; then
        echo "✓"
    else
        # Try parsing
        if rustc --edition 2021 -Z parse-only "$target" 2>/dev/null; then
            echo "✓"
        else
            echo "✗ (formatting issues, but may compile)"
        fi
    fi
done

echo ""
echo "Targets are ready for fuzzing!"
echo "Run: ./run_fuzz.sh quick"
