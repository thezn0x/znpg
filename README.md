# ZNPG v1.2.0 Documentation

A robust, high-level PostgreSQL database abstraction layer built on `psycopg3` with connection pooling, query building, and context manager support.

**Philosophy:**
> Database access should be simple, safe, and Pythonic.

ZNPG was born from frustration with psycopg2's verbosity and SQLAlchemy's complexity. We believe database libraries should:

- Get out of your way with clean, intuitive APIs

- Protect you from yourself with safe defaults

- Scale with your needs from scripts to production apps

Built by a developer who got tired of boilerplate. Used by 250+ developers who felt the same.

## Features
- Zero-boilerplate - From import to query in 3 lines

- Safe by default - No unsafe DELETE/UPDATE without explicit flags

- Built-in pooling - Connection pooling that just works

- Full CRUD - High-level operations for 95% of use cases

- Raw SQL access - Escape hatch when you need full control

- Type-hinted - Modern Python with complete type hints

- Production-ready - Connection health checks, stats, and maintenance ops

- JSON native - Built-in import/export for data portability  

---
## Table of Contents
- [Overview](#overview)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Core Classes](#core-classes)
- [Database](#database-class)
- [Connection Management](#connection-management)
- [CRUD Operations](#crud-operations)
- [Query Builder Integration](#query-builder-integration)
- [Transaction Support](#transaction-support)
- [Utility Methods](#utility-methods)
- [Error Handling](#error-handling)
- [Best Practices](#best-practices)

---

## Overview

ZNPG provides a Pythonic interface to PostgreSQL databases with the following features:

- **Connection Pooling**: Built-in `psycopg_pool` integration for efficient connection management
- **Context Managers**: Safe resource handling with `with` statements
- **CRUD Abstractions**: High-level methods for common database operations
- **Query Builder Integration**: Seamless SQL generation via `QueryBuilder` class
- **Type Safety**: Full type hints and generic support
- **Transaction Support**: Atomic operations with automatic rollback on failure
- **JSON Export/Import**: Native support for data serialization

---

## Installation

```bash
# Optional: Install znpg package
pip install znpg
```

**Dependencies:**
- `psycopg` (v3.x)
- `psycopg-pool`
- `typing` (Python 3.8+)

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

The primary interface for database operations.

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

### Connection Methods

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
db.close()  # Automatically called when using context managers
```

---

## CRUD Operations

### Read Operations

#### `query(sql: str, params: Optional[List[Any]] = None) -> List[Dict[str, Any]]`

Execute raw SQL and return results as list of dictionaries.

```python
# Parameterized query (safe against SQL injection)
results = db.query(
    "SELECT * FROM orders WHERE status = %s AND amount > %s",
    ["pending", 100.00]
)
# Returns: [{"id": 1, "status": "pending", "amount": 150.00}, ...]
```

#### `fetch_one(sql: str, params: Optional[List[Any]] = None) -> Optional[Dict[str, Any]]`

Fetch single record or None.

```python
user = db.fetch_one("SELECT * FROM users WHERE email = %s", ["john@example.com"])
if user:
    print(user["name"])
```

#### `select(table, columns, where, order_by, limit) -> List[Dict[str, Any]]`

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

#### `get_by_id(table: str, id_name: str, id: Union[str, int]) -> List[Dict]`

Fetch record by primary key.

```python
user = db.get_by_id("users", "user_id", 42)
```

#### `count(table: str, where: Optional[Dict] = None) -> Optional[int]`

Count records with optional filtering.

```python
total = db.count("orders")
pending_count = db.count("orders", where={"status": "pending"})
```

#### `exists(table: str, where: Dict[str, Any]) -> Optional[bool]`

Check if records matching criteria exist.

```python
has_admin = db.exists("users", {"role": "admin"})
```

### Write Operations

#### `execute(sql: str, params: Optional[List[Any]] = None) -> int`

Execute non-query SQL (INSERT, UPDATE, DELETE). Returns rowcount.

```python
rows_deleted = db.execute("DELETE FROM logs WHERE created_at < %s", ["2023-01-01"])
```

#### `insert(table: str, data: Dict[str, Any]) -> bool`

Insert single record.

```python
success = db.insert("users", {
    "name": "Jane Doe",
    "email": "jane@example.com",
    "created_at": "2024-01-15"
})
```

#### `update(table: str, data: Dict, conditions: Optional[Dict] = None, allow_all: bool = False) -> int`

Update records with safety checks.

```python
# Safe update with WHERE clause
rows_updated = db.update(
    table="users",
    data={"last_login": "2024-01-15"},
    conditions={"id": 42}
)

# DANGEROUS: Update all records (requires explicit flag)
db.update("users", {"status": "inactive"}, allow_all=True)
```

#### `delete(table: str, conditions: Optional[Dict] = None, allow_deleteall: bool = False) -> int`

Delete records with safety checks.

```python
# Safe delete
deleted = db.delete("sessions", {"expired": True})

# DANGEROUS: Delete all (requires explicit flag)
db.delete("logs", allow_deleteall=True)
```

#### `bulk_insert(table: str, data: List[Dict], on_conflict: Optional[str] = None) -> int`

Efficient batch insertion.

```python
users = [
    {"name": "Alice", "email": "alice@example.com"},
    {"name": "Bob", "email": "bob@example.com"}
]
inserted = db.bulk_insert("users", users, on_conflict="DO NOTHING")
```

---

## DDL Operations

#### `create_table(table: str, columns: Optional[Dict[str, str]] = None) -> bool`

Create new table.

```python
db.create_table("products", {
    "id": "SERIAL PRIMARY KEY",
    "name": "VARCHAR(255) NOT NULL",
    "price": "DECIMAL(10,2)",
    "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
})
```

#### `drop_table(table: str, cascade: bool = False, allow_action: bool = False) -> bool`

Drop table with safety checks.

```python
db.drop_table("temp_table", allow_action=True)
db.drop_table("orders", cascade=True, allow_action=True)  # Cascades to dependencies
```

#### `truncate(table: str) -> bool`

Truncate table (remove all data, keep structure).

```python
db.truncate("logs")
```

#### `table_exists(table: str) -> Optional[bool]`

Check table existence.

```python
if db.table_exists("migrations"):
    print("Migrations table already created")
```

#### `get_table_columns(table: str) -> Optional[List[str]]`

Retrieve column names for a table.

```python
columns = db.get_table_columns("users")
# Returns: ['id', 'name', 'email', 'created_at']
```

#### `create_index(table: str, columns: List[str], unique: bool = False) -> int`

Create database index.

```python
db.create_index("users", ["email"], unique=True)
db.create_index("orders", ["user_id", "created_at"])
```

#### `vacuum(table: Optional[str] = None, analyze: bool = True) -> int`

Run PostgreSQL VACUUM for maintenance.

```python
db.vacuum()  # Full database
db.vacuum("large_table", analyze=True)
```

---

## Transaction Support

#### `transaction() -> ContextManager`

Explicit transaction management with automatic commit/rollback.

```python
try:
    with db.transaction() as conn:
        # All operations use same connection
        db.insert("accounts", {"user_id": 1, "balance": 100})
        db.insert("transactions", {"account_id": 1, "amount": 100})
        # Automatically commits if no exception
except Exception as e:
    # Automatically rolled back on exception
    logger.error(f"Transaction failed: {e}")
```

**Note:** The `transaction()` method yields a connection, but the current implementation doesn't override internal methods to use this connection. For true transactional safety with the high-level methods, extend the class or use `get_connection()` directly:

```python
with db.get_connection() as conn:
    try:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO accounts ...")
            cur.execute("INSERT INTO transactions ...")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
```

---

## Utility Methods

### JSON Serialization

#### `export_to_json(file: str, data: dict, indent: int = 4) -> bool`

Static method to export data to JSON file.

```python
data = db.select("users")
Database.export_to_json("backup.json", data)
```

#### `import_from_json(file: str) -> Union[dict, bool]`

Static method to import data from JSON file.

```python
data = Database.import_from_json("config.json")
db.bulk_insert("settings", data)
```

### Connection String Builder

#### `c_string(username, host, password, db_name, port) -> str`

Static method to generate connection string.

```python
conn_str = Database.c_string("user", "localhost", "pass", "db", 5432)
# Returns: "postgresql://user:pass@localhost:5432/db"
```

---

## Query Builder Integration

The `Database` class relies on a `QueryBuilder` class (not shown) to construct SQL. Expected interface:

| Method | Purpose |
|--------|---------|
| `build_select_query(table, columns, where, order_by, limit)` | Generate SELECT SQL |
| `build_insert_query(table, data)` | Generate INSERT SQL |
| `build_update_query(table, data, conditions)` | Generate UPDATE SQL |
| `build_delete_query(table, conditions)` | Generate DELETE SQL |
| `build_createtable_query(table, columns)` | Generate CREATE TABLE SQL |
| `build_droptable_query(table, cascade, allow)` | Generate DROP TABLE SQL |
| `build_truncate_query(table)` | Generate TRUNCATE SQL |
| `build_findtable_query()` | Generate table existence check |
| `build_bulk_insert(table, data, on_conflict)` | Generate batch INSERT |
| `build_allcolumns_query()` | Generate column metadata query |
| `build_getby_id(table, id_name)` | Generate primary key lookup |
| `build_count_query(table, where)` | Generate COUNT SQL |
| `build_exists_query(table, where)` | Generate EXISTS SQL |
| `build_create_index(table, columns, unique)` | Generate CREATE INDEX |
| `build_vacuum(table, analyze)` | Generate VACUUM SQL |

---

## Error Handling

All database operations catch `psycopg.Error` and log via the configured logger. Methods return safe defaults on failure:

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

**Critical errors** (connection failures) are re-raised after logging.

---

## Best Practices

### 1. Always Use Context Managers
```python
# Good
with Database() as db:
    db.url_connect(conn_str)
    data = db.select("users")

# Avoid
db = Database()
db.url_connect(conn_str)
# If exception occurs here, pool may not close
```

### 2. Use Parameterized Queries
```python
# Safe
db.query("SELECT * FROM users WHERE id = %s", [user_id])

# NEVER do this (SQL Injection risk)
db.query(f"SELECT * FROM users WHERE id = {user_id}")
```

### 3. Handle Connection Failures
```python
try:
    db.url_connect(conn_str)
except Exception as e:
    logger.critical(f"Database connection failed: {e}")
    raise SystemExit(1)
```

### 4. Use Explicit Safety Flags
```python
# This raises ValueError
db.update("users", {"role": "admin"})  # Missing WHERE

# This works
db.update("users", {"role": "admin"}, allow_all=True)
```

### 5. Monitor Pool Health
```python
stats = db.stats()
if stats["available"] / stats["size"] < 0.2:
    logger.warning("Database pool running low on connections")
```

---

## API Reference Summary

### Class: `Database`

| Method | Returns | Description |
|--------|---------|-------------|
| `__init__(min_size, max_size, timeout)` | `Database` | Constructor |
| `url_connect(conn_string)` | `None` | Connect via URI |
| `manual_connect(...)` | `None` | Connect via params |
| `get_connection()` | `ContextManager` | Raw connection access |
| `query(sql, params)` | `List[Dict]` | Raw SQL query |
| `execute(sql, params)` | `int` | Raw SQL execution |
| `fetch_one(sql, params)` | `Optional[Dict]` | Single record fetch |
| `select(...)` | `List[Dict]` | High-level SELECT |
| `insert(table, data)` | `bool` | Insert record |
| `update(table, data, conditions, allow_all)` | `int` | Update records |
| `delete(table, conditions, allow_deleteall)` | `int` | Delete records |
| `bulk_insert(table, data, on_conflict)` | `int` | Batch insert |
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
| `transaction()` | `ContextManager` | Transaction context |
| `close()` | `None` | Close pool |
| `export_to_json(file, data, indent)` | `bool` | Static: Export JSON |
| `import_from_json(file)` | `Union[dict, bool]` | Static: Import JSON |
| `c_string(...)` | `str` | Static: Build conn string |

---

**Version:** 1.2.0  
**License:** MIT  
**Python Support:** 3.8+  
**PostgreSQL:** 12+  
**Author:** [ZN-0X](https://github.com/thezn0x)

For issues and contributions, visit the project [repository](https://github.com/thezn0x/znpg).