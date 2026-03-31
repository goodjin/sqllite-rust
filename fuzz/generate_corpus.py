#!/usr/bin/env python3
"""
Generate fuzzing corpus seeds for sqllite-rust
Target: 1000+ test cases
"""

import os
import random
import string
from pathlib import Path

# SQL Parser corpus - 200 seeds
SQL_TEMPLATES = [
    "SELECT * FROM {table}",
    "SELECT {col} FROM {table} WHERE {cond}",
    "SELECT * FROM {table1} JOIN {table2} ON {join_cond}",
    "INSERT INTO {table} VALUES ({values})",
    "INSERT INTO {table} ({cols}) VALUES ({values})",
    "UPDATE {table} SET {set_clause} WHERE {cond}",
    "DELETE FROM {table} WHERE {cond}",
    "CREATE TABLE {table} ({schema})",
    "CREATE INDEX {idx} ON {table} ({col})",
    "DROP TABLE {table}",
    "DROP INDEX {idx}",
    "BEGIN TRANSACTION",
    "BEGIN",
    "COMMIT",
    "ROLLBACK",
    "SELECT COUNT(*) FROM {table}",
    "SELECT SUM({col}) FROM {table}",
    "SELECT AVG({col}) FROM {table}",
    "SELECT MIN({col}), MAX({col}) FROM {table}",
    "SELECT * FROM {table} ORDER BY {col} ASC",
    "SELECT * FROM {table} ORDER BY {col} DESC",
    "SELECT * FROM {table} LIMIT {n}",
    "SELECT * FROM {table} LIMIT {n} OFFSET {m}",
    "SELECT DISTINCT {col} FROM {table}",
    "SELECT * FROM {table} WHERE {col} IS NULL",
    "SELECT * FROM {table} WHERE {col} IS NOT NULL",
    "SELECT * FROM {table} WHERE {col} LIKE '{pattern}'",
    "SELECT * FROM {table} WHERE {col} IN ({list})",
    "SELECT * FROM {table} WHERE {col} BETWEEN {n} AND {m}",
    "SELECT * FROM {table} GROUP BY {col}",
    "SELECT * FROM {table} GROUP BY {col} HAVING {cond}",
    "ALTER TABLE {table} ADD COLUMN {col} {type}",
    "ALTER TABLE {table} DROP COLUMN {col}",
    "SELECT {expr} FROM {table}",
]

TABLES = ["users", "orders", "products", "customers", "items", "categories", "logs", "data", "test", "foo"]
COLS = ["id", "name", "email", "price", "total", "user_id", "created_at", "status", "value", "type"]
TYPES = ["INTEGER", "TEXT", "REAL", "BLOB", "NUMERIC", "BOOLEAN", "DATETIME"]

def generate_sql_seed(seed_id):
    """Generate a SQL statement for fuzzing corpus."""
    random.seed(seed_id)
    template = random.choice(SQL_TEMPLATES)
    
    table = random.choice(TABLES)
    col = random.choice(COLS)
    table2 = random.choice([t for t in TABLES if t != table])
    col2 = random.choice([c for c in COLS if c != col])
    
    values = ", ".join([str(random.randint(1, 1000)) if random.random() > 0.5 else f"'{random.randint(1, 100)}'" for _ in range(random.randint(1, 5))])
    cols = ", ".join(random.sample(COLS, random.randint(1, 3)))
    cond = f"{col} = {random.randint(1, 100)}"
    join_cond = f"{table}.id = {table2}.{table}_id"
    set_clause = f"{col} = {random.randint(1, 1000)}"
    schema = ", ".join([f"{c} {random.choice(TYPES)}" for c in random.sample(COLS, random.randint(2, 4))])
    idx = f"idx_{col}"
    expr = f"{col} + {random.randint(1, 100)}"
    pattern = f"%{random.choice(string.ascii_lowercase)}%"
    list_vals = ", ".join([str(random.randint(1, 10)) for _ in range(random.randint(2, 5))])
    n, m = random.randint(1, 100), random.randint(1, 50)
    
    try:
        sql = template.format(
            table=table, table1=table, table2=table2, col=col, col2=col2,
            values=values, cols=cols, cond=cond, join_cond=join_cond,
            set_clause=set_clause, schema=schema, idx=idx, expr=expr,
            pattern=pattern, list=list_vals, n=n, m=m, type=random.choice(TYPES)
        )
    except:
        sql = f"SELECT * FROM {table}"
    
    return sql

def generate_sql_corpus(base_dir, count=300):
    """Generate SQL parser corpus."""
    corpus_dir = Path(base_dir) / "sql_parser_fuzz"
    corpus_dir.mkdir(parents=True, exist_ok=True)
    
    for i in range(count):
        sql = generate_sql_seed(i)
        with open(corpus_dir / f"seed_{i+1:03d}.sql", "w") as f:
            f.write(sql)
    
    print(f"Generated {count} SQL parser corpus files")

# Storage corpus - 200 seeds
def generate_storage_seed(seed_id):
    """Generate storage operation sequences."""
    random.seed(seed_id)
    ops = []
    
    for _ in range(random.randint(5, 50)):
        op_type = random.randint(0, 4)
        key_len = random.randint(1, 32)
        key = bytes([random.randint(0, 255) for _ in range(key_len)])
        
        if op_type == 0:  # Insert
            val_len = random.randint(1, 64)
            value = bytes([random.randint(0, 255) for _ in range(val_len)])
            ops.append(bytes([op_type, key_len]) + key + bytes([val_len]) + value)
        elif op_type == 1:  # Get
            ops.append(bytes([op_type, key_len]) + key)
        elif op_type == 2:  # Delete
            ops.append(bytes([op_type, key_len]) + key)
        elif op_type == 3:  # RangeScan
            end_key = bytes([b ^ 0xFF for b in key])
            ops.append(bytes([op_type, key_len]) + key + end_key)
        else:  # Update
            val_len = random.randint(1, 64)
            value = bytes([random.randint(0, 255) for _ in range(val_len)])
            ops.append(bytes([op_type, key_len]) + key + bytes([val_len]) + value)
    
    return b"".join(ops)

def generate_storage_corpus(base_dir, count=200):
    """Generate storage fuzz corpus."""
    corpus_dir = Path(base_dir) / "storage_fuzz"
    corpus_dir.mkdir(parents=True, exist_ok=True)
    
    for i in range(count):
        data = generate_storage_seed(i)
        with open(corpus_dir / f"seed_{i+1:03d}.bin", "wb") as f:
            f.write(data)
    
    print(f"Generated {count} storage corpus files")

# MVCC corpus - 200 seeds
def generate_mvcc_seed(seed_id):
    """Generate MVCC operation sequences."""
    random.seed(seed_id)
    data = []
    
    num_txns = random.randint(1, 8)
    data.append(num_txns)
    
    tx_id = 1
    ts = 1
    
    for _ in range(random.randint(20, 100)):
        txn_idx = random.randint(0, num_txns - 1)
        op_type = random.randint(0, 4)
        
        data.append(txn_idx)
        data.append(op_type)
        
        if op_type in [1, 2, 3, 4]:  # Read, Write, Commit, Rollback need extra data
            data.append(random.randint(0, 255))  # key
            if op_type == 2:  # Write needs value
                data.append(random.randint(0, 255))  # value
    
    return bytes(data)

def generate_mvcc_corpus(base_dir, count=200):
    """Generate MVCC fuzz corpus."""
    corpus_dir = Path(base_dir) / "mvcc_fuzz"
    corpus_dir.mkdir(parents=True, exist_ok=True)
    
    for i in range(count):
        data = generate_mvcc_seed(i)
        with open(corpus_dir / f"seed_{i+1:03d}.bin", "wb") as f:
            f.write(data)
    
    print(f"Generated {count} MVCC corpus files")

# Transaction corpus - 200 seeds
def generate_transaction_seed(seed_id):
    """Generate transaction sequences."""
    random.seed(seed_id)
    data = []
    
    in_tx = False
    for _ in range(random.randint(10, 100)):
        if not in_tx:
            action = random.choice([0, 7])  # Begin or Checkpoint
        else:
            action = random.randint(1, 7)
        
        data.append(action)
        
        if action in [1, 2, 3, 4]:  # Insert, Update, Delete, Select need key
            data.append(random.randint(0, 255))  # key
            if action in [1, 2]:  # Insert/Update need value
                data.append(random.randint(0, 255))  # value
        
        if action in [5, 6]:  # Commit or Rollback
            in_tx = False
        elif action == 0:  # Begin
            in_tx = True
    
    return bytes(data)

def generate_transaction_corpus(base_dir, count=200):
    """Generate transaction fuzz corpus."""
    corpus_dir = Path(base_dir) / "transaction_fuzz"
    corpus_dir.mkdir(parents=True, exist_ok=True)
    
    for i in range(count):
        data = generate_transaction_seed(i)
        with open(corpus_dir / f"seed_{i+1:03d}.bin", "wb") as f:
            f.write(data)
    
    print(f"Generated {count} transaction corpus files")

# BTree corpus - 150 seeds
def generate_btree_seed(seed_id):
    """Generate BTree operation sequences."""
    random.seed(seed_id)
    data = []
    
    for _ in range(random.randint(10, 100)):
        op_type = random.randint(0, 4)
        key_len = random.randint(1, 32)
        
        data.append(op_type)
        data.append(key_len)
        data.extend([random.randint(0, 255) for _ in range(key_len)])
        
        if op_type == 0:  # Insert needs value
            val_len = random.randint(1, 64)
            data.append(val_len)
            data.extend([random.randint(0, 255) for _ in range(val_len)])
    
    return bytes(data)

def generate_btree_corpus(base_dir, count=150):
    """Generate BTree fuzz corpus."""
    corpus_dir = Path(base_dir) / "btree_fuzz"
    corpus_dir.mkdir(parents=True, exist_ok=True)
    
    for i in range(count):
        data = generate_btree_seed(i)
        with open(corpus_dir / f"seed_{i+1:03d}.bin", "wb") as f:
            f.write(data)
    
    print(f"Generated {count} BTree corpus files")

# Record corpus - 100 seeds
def generate_record_seed(seed_id):
    """Generate record data."""
    random.seed(seed_id)
    data = []
    
    for _ in range(random.randint(5, 50)):
        val_type = random.randint(0, 9)
        data.append(val_type)
        
        if val_type in [8, 9]:  # String/Blob
            length = random.randint(0, 31)
            data.append(length)
            data.extend([random.randint(0, 255) for _ in range(length)])
    
    return bytes(data)

def generate_record_corpus(base_dir, count=100):
    """Generate record fuzz corpus."""
    corpus_dir = Path(base_dir) / "record_fuzz"
    corpus_dir.mkdir(parents=True, exist_ok=True)
    
    for i in range(count):
        data = generate_record_seed(i)
        with open(corpus_dir / f"seed_{i+1:03d}.bin", "wb") as f:
            f.write(data)
    
    print(f"Generated {count} record corpus files")

# Tokenizer corpus - 100 seeds
def generate_tokenizer_corpus(base_dir, count=100):
    """Generate tokenizer fuzz corpus."""
    corpus_dir = Path(base_dir) / "tokenizer_fuzz"
    corpus_dir.mkdir(parents=True, exist_ok=True)
    
    patterns = [
        "SELECT * FROM t",
        "INSERT INTO t VALUES (1, 2, 3)",
        "WHERE x = 1 AND y = 2",
        "ORDER BY id DESC",
        "GROUP BY category HAVING count > 5",
        "JOIN orders ON users.id = orders.user_id",
        "LIMIT 10 OFFSET 5",
        "CREATE TABLE test (id INTEGER PRIMARY KEY)",
        "/* comment */ SELECT 1",
        "-- line comment\nSELECT 2",
        "'string with '' escape'",
        "123.456e-7",
        "0xABCDEF",
        "TRUE FALSE NULL",
        "BEGIN; COMMIT; ROLLBACK",
    ]
    
    random.seed(42)
    for i in range(count):
        # Combine random patterns
        num_patterns = random.randint(1, 4)
        content = " ".join(random.sample(patterns, min(num_patterns, len(patterns))))
        
        # Add some randomness
        if random.random() > 0.5:
            content += f" {random.randint(1, 1000)}"
        
        with open(corpus_dir / f"seed_{i+1:03d}.sql", "w") as f:
            f.write(content)
    
    print(f"Generated {count} tokenizer corpus files")

# Expression corpus - 100 seeds
def generate_expression_corpus(base_dir, count=100):
    """Generate expression fuzz corpus."""
    corpus_dir = Path(base_dir) / "expression_fuzz"
    corpus_dir.mkdir(parents=True, exist_ok=True)
    
    operators = ["+", "-", "*", "/", "=", "!=", "<", ">", "<=", ">=", "AND", "OR"]
    functions = ["ABS", "LENGTH", "UPPER", "LOWER", "COALESCE", "NULLIF", "ROUND"]
    
    random.seed(42)
    for i in range(count):
        depth = random.randint(1, 5)
        expr = generate_expression(depth, operators, functions)
        
        with open(corpus_dir / f"seed_{i+1:03d}.txt", "w") as f:
            f.write(expr)
    
    print(f"Generated {count} expression corpus files")

def generate_expression(depth, operators, functions):
    """Generate a random expression."""
    if depth <= 0:
        return random.choice([str(random.randint(1, 100)), "'text'", "NULL", "TRUE", "FALSE"])
    
    expr_type = random.randint(0, 4)
    
    if expr_type == 0:  # Binary op
        left = generate_expression(depth - 1, operators, functions)
        right = generate_expression(depth - 1, operators, functions)
        op = random.choice(operators)
        return f"{left} {op} {right}"
    elif expr_type == 1:  # Function call
        func = random.choice(functions)
        arg = generate_expression(depth - 1, operators, functions)
        return f"{func}({arg})"
    elif expr_type == 2:  # Parenthesized
        inner = generate_expression(depth - 1, operators, functions)
        return f"({inner})"
    elif expr_type == 3:  # Unary
        inner = generate_expression(depth - 1, operators, functions)
        return f"-{inner}"
    else:  # Literal
        return random.choice([str(random.randint(1, 100)), "'text'", "col_name"])

def main():
    base_dir = Path(__file__).parent / "corpus"
    
    # Generate all corpus files
    generate_sql_corpus(base_dir, 300)
    generate_storage_corpus(base_dir, 200)
    generate_mvcc_corpus(base_dir, 200)
    generate_transaction_corpus(base_dir, 200)
    generate_btree_corpus(base_dir, 150)
    generate_record_corpus(base_dir, 100)
    generate_tokenizer_corpus(base_dir, 100)
    generate_expression_corpus(base_dir, 100)
    
    # Count total
    total = 0
    for corpus_type in ["sql_parser_fuzz", "storage_fuzz", "mvcc_fuzz", "transaction_fuzz", 
                        "btree_fuzz", "record_fuzz", "tokenizer_fuzz", "expression_fuzz"]:
        corpus_dir = base_dir / corpus_type
        if corpus_dir.exists():
            count = len(list(corpus_dir.iterdir()))
            total += count
            print(f"  {corpus_type}: {count} files")
    
    print(f"\nTotal corpus files: {total}")
    print("Target: 1000+ test cases")
    
    if total >= 1000:
        print("✓ Target achieved!")
    else:
        print(f"✗ Need {1000 - total} more files")

if __name__ == "__main__":
    main()
