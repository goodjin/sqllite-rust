# Fuzz Testing for sqllite-rust

This document describes the fuzz testing infrastructure for sqllite-rust.

## Overview

Fuzz testing is a critical component of our testing strategy, helping discover edge cases, crashes, and security vulnerabilities that traditional unit tests might miss.

**Target**: 1000+ fuzz test cases (✅ Achieved: 1350+)

## Quick Start

```bash
# Run quick fuzz test (60s per target)
./run_fuzz.sh quick

# Run specific target
./run_fuzz.sh single sql_parser_fuzz

# Run full fuzz test (1 hour per target)
./run_fuzz.sh full

# Show statistics
./run_fuzz.sh stats
```

## Fuzz Targets

| Target | Description | Status |
|--------|-------------|--------|
| `sql_parser_fuzz` | SQL statement parsing | ✅ Active |
| `storage_fuzz` | B+Tree storage operations | ✅ Active |
| `mvcc_fuzz` | MVCC concurrency control | ✅ Active |
| `transaction_fuzz` | Transaction ACID properties | ✅ Active |
| `btree_fuzz` | B+Tree specific operations | ✅ Active |
| `record_fuzz` | Record encoding/decoding | ✅ Active |
| `tokenizer_fuzz` | SQL tokenization | ✅ Active |
| `expression_fuzz` | Expression evaluation | ✅ Active |

## Corpus

The fuzz corpus contains 1350+ seed files across all targets:

```
fuzz/corpus/
├── sql_parser_fuzz/    # 300 SQL statements
├── storage_fuzz/       # 200 storage operation sequences
├── mvcc_fuzz/          # 200 MVCC operation sequences
├── transaction_fuzz/   # 200 transaction sequences
├── btree_fuzz/         # 150 BTree operations
├── record_fuzz/        # 100 record encodings
├── tokenizer_fuzz/     # 100 token patterns
└── expression_fuzz/    # 100 expressions
```

Generate/update corpus:
```bash
cd fuzz && python3 generate_corpus.py
```

## Manual Usage

```bash
# Install cargo-fuzz
cargo install cargo-fuzz

# Build fuzz targets
cd fuzz
cargo build --release

# Run a target with custom options
cargo fuzz run sql_parser_fuzz -- -max_total_time=300 -jobs=4

# Minimize corpus
cargo fuzz cmin sql_parser_fuzz

# Show coverage
cargo fuzz coverage sql_parser_fuzz
genhtml coverage/sql_parser_fuzz/coverage.profdata -o coverage/html
```

## Fuzzing Options

Common libFuzzer options:

| Option | Description |
|--------|-------------|
| `-max_total_time=N` | Run for N seconds |
| `-max_len=N` | Maximum input length |
| `-jobs=N` | Number of parallel jobs |
| `-workers=N` | Number of worker threads |
| `-print_final_stats=1` | Print statistics at end |
| `-only_ascii=1` | ASCII-only inputs |

## CI Integration

Fuzz tests run automatically:

- **Quick Fuzz**: On every push/PR (8 minutes)
- **Extended Fuzz**: Daily at 2 AM UTC (2 hours)
- **All Targets**: Parallel testing of all 8 targets

See `.github/workflows/fuzz.yml` for details.

## Interpreting Results

### Crashes

Crashes are saved in `fuzz/artifacts/<target>/`. To reproduce:

```bash
cargo fuzz run <target> <crash-file>
```

### Coverage

Monitor coverage growth in CI artifacts. Coverage files are uploaded after extended runs.

## Adding New Fuzz Targets

1. Create `fuzz/fuzz_targets/<name>.rs`:

```rust
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Your fuzzing code here
});
```

2. Add to `fuzz/Cargo.toml`:

```toml
[[bin]]
name = "<name>"
path = "fuzz_targets/<name>.rs"
test = false
doc = false
bench = false
```

3. Generate corpus in `generate_corpus.py`

4. Run: `./run_fuzz.sh single <name>`

## Best Practices

1. **Corpus Quality**: Ensure seeds are diverse and representative
2. **Minimization**: Regularly run `cargo fuzz cmin` to reduce corpus size
3. **Monitoring**: Check CI for new crashes daily
4. **Triage**: Prioritize and fix crashes promptly
5. **Coverage**: Aim for >80% code coverage from fuzz tests

## Comparison with SQLite

SQLite runs ~1 billion fuzz tests daily. Our targets:

| Metric | SQLite | sqllite-rust (Current) | Target |
|--------|--------|------------------------|--------|
| Daily fuzz runs | 1B+ | 100K+ | 1M+ |
| Corpus size | 100K+ | 1,350 | 5,000+ |
| Continuous | Yes | Daily | Hourly |
| Coverage | >95% | TBD | >90% |

## Troubleshooting

### Build Failures

```bash
# Clean and rebuild
cargo clean
cargo +nightly build --release
```

### Out of Memory

```bash
# Limit memory usage
cargo fuzz run <target> -- -max_len=1024
```

### Slow Fuzzing

```bash
# Increase parallel jobs
cargo fuzz run <target> -- -jobs=8 -workers=8
```

## Resources

- [cargo-fuzz documentation](https://rust-fuzz.github.io/book/cargo-fuzz.html)
- [libFuzzer documentation](https://llvm.org/docs/LibFuzzer.html)
- [SQLite Fuzz Testing](https://www.sqlite.org/testing.html)

## Contributing

When fixing a fuzz-discovered bug:

1. Create a regression test from the crash input
2. Add the minimized input to corpus
3. Document the fix in commit message
4. Update `FUZZING.md` if behavior changes
