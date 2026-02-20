# ZNPG v1.3.0 Documentation

A robust, high-level PostgreSQL database abstraction layer built on `psycopg3` with connection pooling, query building, and context manager support.

**Philosophy:**
> Database access should be simple, safe, and Pythonic.

ZNPG was born from frustration with psycopg2's verbosity and SQLAlchemy's complexity. We believe database libraries should:

- Get out of your way with clean, intuitive APIs
- Protect you from yourself with safe defaults
- Scale with your needs from scripts to production apps

Built by a developer who got tired of boilerplate. Used by 250+ developers who felt the same.

## What's New in v1.3.0

- **Fixed transaction support** — all write methods now accept an optional `conn` parameter, making atomic multi-operation transactions actually reliable
- All CRUD methods (`insert`, `update`, `delete`, `bulk_insert`, `select`, `fetch_one`, `query`, `execute`) now support `conn` passthrough

---

## Features
- Zero-boilerplate — from import to query in 3 lines
- Safe by default — no unsafe DELETE/UPDATE without explicit flags
- Built-in pooling — connection pooling that just works
- Full CRUD — high-level operations for 95% of use cases
- Raw SQL access — escape hatch when you need full control
- Type-hinted — modern Python with complete type hints
- Production-ready — connection health checks, stats, and maintenance ops
- JSON native — built-in import/export for data portability
- **True atomic transactions** — pass `conn` into any method to run it inside a transaction

---

## Table of Contents
- [Overview](#overview)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Core Classes](#core-classes)
- [Connection Management](#connection-management)
- [CRUD Operations](#crud-operations)
- [Transaction Support](#transaction-support)
- [DDL Operations](#ddl-operations)
- [Utility Methods](#utility-methods)
- [Error Handling](#error-handling)
- [Best Practices](#best-practices)
- [API Reference](#api-reference-summary)

---

## Overview

ZNPG provides a Pythonic interface to PostgreSQL databases with the following features:

- **Connection Pooling**: Built-in `psycopg_pool` integration for efficient connection management
- **Context Managers**: Safe resource handling with `with` statements
- **CRUD Abstractions**: High-level methods for common database operations
- **Query Builder Integration**: Seamless SQL generation via `QueryBuilder` class
- **Type Safety**: Full type hints and generic support
- **Atomic Transactions**: Pass `conn` into any method to guarantee all-or-nothing execution
- **JSON Export/Import**: Native support for data serialization

---

## Installation

```bash
pip install znpg
```

**Dependencies:**
- `psycopg` (v3.x)
- `psycopg-pool`
- Python 3.8+

---

## Quick Start

```python
from znpg import Database

# Initialize with connection string
db = Database(min_size=2, max_size=10)
db.url_connect("postgresql://user:pass@localhost:5432/mydb")

# Or use manual connection parameters
db.manual_connect(
    username="user",
    password="pass",
    host="localhost",
    port=5432,
    db_name="mydb"
)

# Simple query
users = db.query("SELECT * FROM users WHERE age > %s", [18])

# Using context manager (recommended)
with Database() as db:
    db.url_connect("postgresql://user:pass@localhost/db")
    result = db.select("users", where={"status": "active"})
```

---

## Core Classes

### Database Class

The primary interface for all database operations.

#### Constructor

```python
Database(
    min_size: int = 1,      # Minimum connections in pool
    max_size: int = 10,     # Maximum connections in pool
    timeout: int = 30       # Connection timeout in seconds
)
```

#### Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `pool` | `Optional[ConnectionPool]` | The underlying connection pool instance |
| `min_size` | `int` | Minimum number of connections maintained |
| `max_size` | `int` | Maximum number of connections allowed |
| `is_connected` | `bool` | Connection status flag |
| `timeout` | `int` | Connection acquisition timeout |

---

## Connection Management

#### `url_connect(conn_string: str) -> None`

Connect using a PostgreSQL connection URI.

```python
db = Database()
db.url_connect("postgresql://admin:secret@db.example.com:5432/production")
```

#### `manual_connect(username, host, password, db_name, port) -> None`

Connect using individual parameters.

```python
db.manual_connect(
    username="postgres",
    host="localhost",
    password="secure_pass",
    db_name="myapp",
    port=5432
)
```

#### `get_connection() -> ContextManager`

Context manager for raw connection access.

```python
with db.get_connection() as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT version()")
        print(cur.fetchone())
```

#### `is_healthy() -> bool`

Check database connectivity.

```python
if db.is_healthy():
    print("Database connection is active")
```

#### `stats() -> Dict`

Retrieve connection pool statistics.

```python
stats = db.stats()
# Returns: {"size": 5, "available": 3, "used": 2}
```

#### `close() -> None`

Explicitly close the connection pool.

```python
db.close()  # Called automatically when using context managers
```

---

## CRUD Operations

### The `conn` Parameter

All write methods and most read methods accept an optional `conn` parameter. When provided, the method runs on that connection instead of grabbing one from the pool. This is how you achieve atomic transactions — see [Transaction Support](#transaction-support).

```python
# without conn — grabs its own connection, auto-commits
db.insert("users", {"name": "Alice"})

# with conn — runs inside your transaction, no auto-commit
with db.transaction() as conn:
    db.insert("users", {"name": "Alice"}, conn=conn)
```

---

### Read Operations

#### `query(sql, params, conn) -> List[Dict[str, Any]]`

Execute raw SQL and return results as a list of dicts.

```python
results = db.query(
    "SELECT * FROM orders WHERE status = %s AND amount > %s",
    ["pending", 100.00]
)
# Returns: [{"id": 1, "status": "pending", "amount": 150.00}, ...]
```

#### `fetch_one(sql, params, conn) -> Optional[Dict[str, Any]]`

Fetch a single record or None.

```python
user = db.fetch_one("SELECT * FROM users WHERE email = %s", ["john@example.com"])
if user:
    print(user["name"])
```

#### `select(table, columns, where, order_by, limit, conn) -> List[Dict[str, Any]]`

High-level SELECT with QueryBuilder.

```python
# Basic select
users = db.select("users")

# With filters
active_users = db.select(
    table="users",
    columns=["id", "name", "email"],
    where={"status": "active", "verified": True},
    order_by=["created_at DESC"],
    limit=10
)
```

#### `get_by_id(table, id_name, id) -> List[Dict]`

Fetch a record by primary key.

```python
user = db.get_by_id("users", "user_id", 42)
```

#### `count(table, where) -> Optional[int]`

Count records with optional filtering.

```python
total = db.count("orders")
pending_count = db.count("orders", where={"status": "pending"})
```

#### `exists(table, where) -> Optional[bool]`

Check if any records matching the criteria exist.

```python
has_admin = db.exists("users", {"role": "admin"})
```

---

### Write Operations

#### `execute(sql, params, conn) -> int`

Execute raw SQL (INSERT, UPDATE, DELETE). Returns rowcount.

```python
rows_deleted = db.execute("DELETE FROM logs WHERE created_at < %s", ["2023-01-01"])
```

#### `insert(table, data, conn) -> bool`

Insert a single record.

```python
success = db.insert("users", {
    "name": "Jane Doe",
    "email": "jane@example.com",
    "created_at": "2024-01-15"
})
```

#### `update(table, data, conditions, allow_all, conn) -> int`

Update records with safety checks.

```python
# Safe update with WHERE clause
rows_updated = db.update(
    table="users",
    data={"last_login": "2024-01-15"},
    conditions={"id": 42}
)

# Update all records (requires explicit flag)
db.update("users", {"status": "inactive"}, allow_all=True)
```

#### `delete(table, conditions, allow_deleteall, conn) -> int`

Delete records with safety checks.

```python
# Safe delete
deleted = db.delete("sessions", {"expired": True})

# Delete all (requires explicit flag)
db.delete("logs", allow_deleteall=True)
```

#### `bulk_insert(table, data, on_conflict, conn) -> int`

Efficient batch insertion.

```python
users = [
    {"name": "Alice", "email": "alice@example.com"},
    {"name": "Bob", "email": "bob@example.com"}
]
inserted = db.bulk_insert("users", users, on_conflict="DO NOTHING")
```

---

## Transaction Support

#### `transaction() -> ContextManager`

Yields a connection for atomic multi-operation transactions. Automatically commits on success and rolls back on any exception.

**v1.3.0 fix**: pass the yielded `conn` into any method to guarantee they all run on the same connection — making rollbacks actually work.

```python
# Atomic checkout: sale recorded + inventory updated or neither happens
try:
    with db.transaction() as conn:
        db.insert("sales", {"product_id": "abc", "total": 29.99}, conn=conn)
        db.update("inventory", {"quantity": 11}, {"product_id": "abc"}, conn=conn)
        # both succeed → auto-commit
except Exception as e:
    # either one fails → both roll back
    logger.error(f"Checkout failed, transaction rolled back: {e}")
```

**Without `conn` (old broken behavior):**
```python
# DON'T do this — each call grabs its own connection, rollback won't affect them
with db.transaction() as conn:
    db.insert("sales", {...})      # connection A
    db.update("inventory", {...})  # connection B — not part of the transaction!
```

---

## DDL Operations

#### `create_table(table, columns) -> bool`

```python
db.create_table("products", {
    "id": "SERIAL PRIMARY KEY",
    "name": "VARCHAR(255) NOT NULL",
    "price": "DECIMAL(10,2)",
    "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
})
```

#### `drop_table(table, cascade, allow_action) -> bool`

```python
db.drop_table("temp_table", allow_action=True)
db.drop_table("orders", cascade=True, allow_action=True)
```

#### `truncate(table) -> bool`

```python
db.truncate("logs")
```

#### `table_exists(table) -> Optional[bool]`

```python
if db.table_exists("migrations"):
    print("Already set up")
```

#### `get_table_columns(table) -> Optional[List[str]]`

```python
columns = db.get_table_columns("users")
# Returns: ['id', 'name', 'email', 'created_at']
```

#### `create_index(table, columns, unique) -> int`

```python
db.create_index("users", ["email"], unique=True)
db.create_index("orders", ["user_id", "created_at"])
```

#### `vacuum(table, analyze) -> int`

```python
db.vacuum()                            # full database
db.vacuum("large_table", analyze=True)
```

---

## Utility Methods

#### `export_to_json(file, data, indent) -> bool`

```python
data = db.select("users")
Database.export_to_json("backup.json", data)
```

#### `import_from_json(file) -> Union[dict, bool]`

```python
data = Database.import_from_json("config.json")
db.bulk_insert("settings", data)
```

#### `c_string(username, host, password, db_name, port) -> str`

```python
conn_str = Database.c_string("user", "localhost", "pass", "db", 5432)
# Returns: "postgresql://user:pass@localhost:5432/db"
```

---

## Error Handling

All operations catch `psycopg.Error` and log via the configured logger. Methods return safe defaults on failure:

| Method | Failure Return |
|--------|---------------|
| `select()` | `[]` |
| `insert()` | `False` |
| `update()` | `0` |
| `delete()` | `0` |
| `create_table()` | `False` |
| `drop_table()` | `False` |
| `truncate()` | `False` |
| `table_exists()` | `None` |
| `bulk_insert()` | `0` |
| `get_table_columns()` | `None` |
| `get_by_id()` | `None` |
| `count()` | `None` |
| `exists()` | `None` |

Connection failures are re-raised after logging.

---

## Best Practices

### 1. Always Use Context Managers
```python
# good
with Database() as db:
    db.url_connect(conn_str)
    data = db.select("users")

# risky — pool may not close if an exception occurs
db = Database()
db.url_connect(conn_str)
```

### 2. Use Parameterized Queries
```python
# safe
db.query("SELECT * FROM users WHERE id = %s", [user_id])

# never do this — SQL injection risk
db.query(f"SELECT * FROM users WHERE id = {user_id}")
```

### 3. Always Pass `conn` Inside Transactions
```python
with db.transaction() as conn:
    db.insert("orders", order_data, conn=conn)      # correct
    db.update("inventory", inv_data, conn=conn)     # correct
```

### 4. Handle Connection Failures
```python
try:
    db.url_connect(conn_str)
except Exception as e:
    logger.critical(f"Database connection failed: {e}")
    raise SystemExit(1)
```

### 5. Use Explicit Safety Flags
```python
# raises ValueError — missing WHERE
db.update("users", {"role": "admin"})

# works
db.update("users", {"role": "admin"}, allow_all=True)
```

### 6. Monitor Pool Health
```python
stats = db.stats()
if stats["available"] / stats["size"] < 0.2:
    logger.warning("Pool running low on connections")
```

---

## API Reference Summary

| Method | Returns | Description |
|--------|---------|-------------|
| `url_connect(conn_string)` | `None` | Connect via URI |
| `manual_connect(...)` | `None` | Connect via params |
| `get_connection()` | `ContextManager` | Raw connection access |
| `transaction()` | `ContextManager` | Atomic transaction context |
| `query(sql, params, conn)` | `List[Dict]` | Raw SQL query |
| `execute(sql, params, conn)` | `int` | Raw SQL execution |
| `fetch_one(sql, params, conn)` | `Optional[Dict]` | Single record fetch |
| `select(..., conn)` | `List[Dict]` | High-level SELECT |
| `insert(table, data, conn)` | `bool` | Insert record |
| `update(table, data, conditions, allow_all, conn)` | `int` | Update records |
| `delete(table, conditions, allow_deleteall, conn)` | `int` | Delete records |
| `bulk_insert(table, data, on_conflict, conn)` | `int` | Batch insert |
| `create_table(table, columns)` | `bool` | Create table |
| `drop_table(table, cascade, allow_action)` | `bool` | Drop table |
| `truncate(table)` | `bool` | Truncate table |
| `table_exists(table)` | `Optional[bool]` | Check existence |
| `get_table_columns(table)` | `Optional[List[str]]` | Get columns |
| `get_by_id(table, id_name, id)` | `List[Dict]` | Fetch by PK |
| `count(table, where)` | `Optional[int]` | Count records |
| `exists(table, where)` | `Optional[bool]` | Check existence |
| `create_index(table, columns, unique)` | `int` | Create index |
| `vacuum(table, analyze)` | `int` | Run VACUUM |
| `is_healthy()` | `bool` | Health check |
| `stats()` | `Dict` | Pool statistics |
| `close()` | `None` | Close pool |
| `export_to_json(file, data, indent)` | `bool` | Static: export JSON |
| `import_from_json(file)` | `Union[dict, bool]` | Static: import JSON |
| `c_string(...)` | `str` | Static: build conn string |

---

**Version:** 1.3.0
**License:** MIT
**Python Support:** 3.8+
**PostgreSQL:** 12+
**Author:** [ZN-0X](https://github.com/thezn0x)

For issues and contributions, visit the [repository](https://github.com/thezn0x/znpg).